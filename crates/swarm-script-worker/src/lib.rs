//! Standalone OS process boundary for admitted ScriptRun operations.
//!
//! This crate owns no Store, cursor, scheduler, manager credential, or effect
//! API. It accepts the kernel's immutable Work receipt, waits for the existing
//! ready/Go protocol, verifies a plan materialized after Go, and launches one
//! interpreter inside the existing script process Group.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use swarm_contracts::error::{Error, Result};
use swarm_process::{Group, private_permissions, write_private_new};
use swarm_scripts::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENTS, MAX_BUNDLE_BYTES, MAX_BUNDLE_FILE_BYTES,
    MAX_CONTROLLER_EFFECTS, MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_INHERITED_ENVIRONMENT, MAX_INVOCATION_BYTES, MAX_RESULT_BYTES, MAX_SCRIPT_DURATION_MS,
    MAX_STDERR_BYTES,
    process::{ProcessPlan, plan_process},
    protocol::{ScriptEffectRequest, ScriptInvocation, validate_invocation_size},
    result::{ProcessExit, ProcessOutcome, project_completion},
    schema::{
        InterpreterKind, MAX_SCHEMA_BYTES, ScriptControllerEffect, ScriptValueSchema,
        validate_bundle_path,
    },
};

const CONTROL_LIMIT: usize = 16 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const START_GATE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_INTERPRETER_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutorPin {
    pub executable: PathBuf,
    pub sha256: String,
    pub artifact_id: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InterpreterIdentity {
    pub kind: InterpreterKind,
    pub canonical_path: PathBuf,
    pub sha256: String,
    pub byte_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ScriptTrust {
    TrustedLocal,
    Isolated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleFile {
    path: String,
    sha256: String,
    byte_length: u64,
    content_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptBundle {
    bundle_version: u32,
    script_id: String,
    interpreter: InterpreterIdentity,
    entrypoint: String,
    argv: Vec<String>,
    trust: ScriptTrust,
    inherit_environment: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    controller_effects: Vec<ScriptControllerEffect>,
    input_schema: ScriptValueSchema,
    result_schema: ScriptValueSchema,
    files: Vec<BundleFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRecord {
    kind: String,
    artifact_id: String,
    relative_path: String,
    byte_length: u64,
    content_digest: String,
    metadata: Value,
}

/// This wire shape is intentionally compatible with `scripts::runner::Work`.
/// `executor` is optional so receipts written before migration continue using
/// the root binary's legacy worker dispatch. New selected pins are immutable
/// per-run and can never silently fall back to the legacy executable.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptWork {
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub data_dir: PathBuf,
    pub bundle_record: ArtifactRecord,
    pub bundle: ScriptBundle,
    pub interpreter: InterpreterIdentity,
    pub environment: std::collections::BTreeMap<String, String>,
    pub environment_sha256: String,
    pub invocation: ScriptInvocation,
    #[serde(default)]
    pub executor: Option<ExecutorPin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReceipt {
    run_id: String,
    work_digest: String,
    data_dir: PathBuf,
}

/// Private post-Go plan. It carries execution inputs only; Store-derived
/// automation attribution and manager/source authorization remain in Store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptWorkerPlan {
    pub schema_version: u8,
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub executor: ExecutorPin,
    pub work_sha256: String,
    pub bundle_sha256: String,
    pub interpreter_sha256: String,
    pub environment_sha256: String,
    pub process: ProcessPlan,
    pub invocation: ScriptInvocation,
    pub result_schema: ScriptValueSchema,
    pub granted_effects: Vec<ScriptControllerEffect>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlanReady {
    schema_version: u8,
    run_id: String,
    operation_id: String,
    token: String,
    work_sha256: String,
    plan_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Completion {
    run_id: String,
    operation_id: String,
    token: String,
    state: String,
    started_at_ms: Option<i64>,
    exit_code: Option<i32>,
    process: Value,
    result: ArtifactRecord,
    stdout: ArtifactRecord,
    stderr: ArtifactRecord,
    result_value: Option<Value>,
    #[serde(default)]
    controller_effects: Vec<ScriptEffectRequest>,
    error_code: Option<String>,
}

#[derive(Debug)]
struct Captured {
    bytes: Vec<u8>,
    overflow: bool,
}

struct CompletionPublication<'a> {
    stdout: &'a [u8],
    stderr: &'a [u8],
    result_value: Option<Value>,
    controller_effects: &'a [ScriptEffectRequest],
    error_code: Option<String>,
    state: &'a str,
    started_at_ms: Option<i64>,
    exit_code: Option<i32>,
    process: &'a Value,
}

/// Materialize the execution plan only after Store has durably acknowledged
/// the ready worker and written the existing Go receipt. The child cannot
/// start the interpreter until it verifies both this plan and its digest.
pub fn materialize_after_go(receipt_path: &Path) -> Result<String> {
    let (work, work_sha256) = read_work_record(receipt_path)?;
    let executor = work.executor.clone().ok_or_else(|| {
        Error::new(
            "SCRIPT_EXECUTOR_UNSELECTED",
            "no standalone executor is pinned for this run",
        )
    })?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    ensure_run_directory(&work.data_dir, &dir)?;
    verify_ready_worker(&work, &dir)?;
    verify_go(&work, &dir)?;
    if receipt_exists(&dir.join("started.json"))?
        || receipt_exists(&dir.join("terminal.json"))?
        || receipt_exists(&dir.join("completion.json"))?
    {
        return Err(Error::new(
            "SCRIPT_RECOVERY_REQUIRED",
            "a script process may already have started; its plan cannot be replaced",
        ));
    }
    let stage = dir.join("bundle");
    verify_staged_bundle(&work.bundle, &stage)?;
    let entrypoint = safe_stage_path(&stage, &work.bundle.entrypoint)?;
    let executable = path_text(&work.interpreter.canonical_path)?;
    let entrypoint = path_text(&entrypoint)?;
    let process = plan_process(
        work.interpreter.kind,
        &executable,
        &entrypoint,
        &work.bundle.argv,
        work.environment.clone(),
    )
    .map_err(script_error)?;
    let plan = ScriptWorkerPlan {
        schema_version: 1,
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        executor,
        work_sha256: work_sha256.clone(),
        bundle_sha256: work.bundle_record.content_digest.clone(),
        interpreter_sha256: work.interpreter.sha256.clone(),
        environment_sha256: work.environment_sha256.clone(),
        process,
        invocation: work.invocation.clone(),
        result_schema: work.bundle.result_schema.clone(),
        granted_effects: work.bundle.controller_effects.clone(),
    };
    validate_plan_against_work(&plan, &work, &work_sha256, &dir)?;
    let plan_bytes = canonical_json(&serde_json::to_value(&plan)?)?.into_bytes();
    if plan_bytes.len() > CONTROL_LIMIT {
        return Err(Error::invalid("script execution plan exceeds 16 MiB"));
    }
    let plan_sha256 = sha256_hex(&plan_bytes);
    write_once(&dir.join("execution-plan.json"), &plan_bytes)?;
    let ready = PlanReady {
        schema_version: 1,
        run_id: work.run_id,
        operation_id: work.operation_id,
        token: work.token,
        work_sha256,
        plan_sha256: plan_sha256.clone(),
    };
    write_once(
        &dir.join("execution-plan-ready.json"),
        &canonical_json(&serde_json::to_value(ready)?)?.into_bytes(),
    )?;
    Ok(plan_sha256)
}

/// Execute one admitted receipt. A prior worker identity or start/completion
/// receipt is a recovery condition, never permission to launch again.
pub fn run_worker(receipt_path: &Path) -> Result<()> {
    let (work, work_sha256) = read_work_record(receipt_path)?;
    let executor = work.executor.as_ref().ok_or_else(|| {
        Error::new(
            "SCRIPT_EXECUTOR_UNSELECTED",
            "standalone worker cannot run a legacy receipt",
        )
    })?;
    verify_executor(executor)?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    ensure_run_directory(&work.data_dir, &dir)?;
    let lock_path = dir.join("worker.lock");
    if fs::symlink_metadata(&lock_path).is_ok_and(|metadata| is_link_or_reparse(&metadata)) {
        return Err(Error::new(
            "SCRIPT_PATH",
            "worker lock is not a regular file",
        ));
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    lock.try_lock().map_err(|_| {
        Error::new(
            "SCRIPT_WORKER_EXISTS",
            "this ScriptRun already has an owning worker",
        )
    })?;
    private_permissions(&lock_path, false)?;
    if receipt_exists(&dir.join("completion.json"))?
        || receipt_exists(&dir.join("terminal.json"))?
        || receipt_exists(&dir.join("worker.json"))?
        || receipt_exists(&dir.join("started.json"))?
    {
        return Err(Error::new(
            "SCRIPT_RECOVERY_REQUIRED",
            "a prior script worker started; code will not be repeated",
        ));
    }
    let group = Group::enter_script(&work.token)?;
    let identity = json!({
        "run_id":work.run_id,
        "operation_id":work.operation_id,
        "token":work.token,
        "ready_at_ms":now_ms()?,
        "control_version":1,
        "process":group.identity
    });
    write_once(
        &dir.join("worker.json"),
        &canonical_json(&identity)?.into_bytes(),
    )?;

    if let Err(error) = stage_bundle(&work, &dir) {
        return finish_worker_error(&work, &group, &identity, error.code, "failed");
    }
    if let Err(error) = wait_for_go(&work, &dir) {
        return finish_worker_error(&work, &group, &identity, error.code, "failed");
    }
    let plan = match wait_for_plan(&work, &work_sha256, &dir) {
        Ok(plan) => plan,
        Err(error) => return finish_worker_error(&work, &group, &identity, error.code, "failed"),
    };
    match execute(&work, &plan, &dir, &group, &identity) {
        Ok(()) => Ok(()),
        Err(error) => {
            let started = receipt_exists(&dir.join("started.json"))?;
            let state = if started || error.code == "SCRIPT_CHILD_STARTED" {
                "incomplete"
            } else {
                "failed"
            };
            finish_worker_error(&work, &group, &identity, error.code, state)
        }
    }
}

fn read_work_record(receipt_path: &Path) -> Result<(ScriptWork, String)> {
    let receipt_bytes = read_bytes(receipt_path, 4096)?;
    let receipt: WorkerReceipt = serde_json::from_slice(&receipt_bytes).map_err(|_| {
        Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt cannot be parsed",
        )
    })?;
    if uuid::Uuid::parse_str(&receipt.run_id).is_err()
        || !valid_sha256(&receipt.work_digest)
        || !receipt.data_dir.is_absolute()
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt identity is invalid",
        ));
    }
    let expected_dir = directory(&receipt.data_dir, &receipt.run_id)?;
    if receipt_path != expected_dir.join("receipt.json") {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script receipt path does not match its run directory",
        ));
    }
    let work_bytes = read_bytes(&expected_dir.join("work.json"), CONTROL_LIMIT as u64)?;
    let digest = sha256_hex(&work_bytes);
    if digest != receipt.work_digest {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "immutable script work digest changed",
        ));
    }
    let work: ScriptWork = serde_json::from_slice(&work_bytes).map_err(|_| {
        Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work document cannot be parsed",
        )
    })?;
    if work.run_id != receipt.run_id || work.data_dir != receipt.data_dir || work.token.is_empty() {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work identity differs from its receipt",
        ));
    }
    validate_work(&work)?;
    Ok((work, digest))
}

fn validate_work(work: &ScriptWork) -> Result<()> {
    if uuid::Uuid::parse_str(&work.run_id).is_err()
        || work.operation_id.trim().is_empty()
        || work.operation_id.len() > 128
        || work.token.trim().is_empty()
        || work.token.len() > 128
        || !work.data_dir.is_absolute()
        || work.bundle_record.kind != "script_bundle"
        || work.bundle_record.artifact_id.len() != 71
        || !work.bundle_record.artifact_id.starts_with("script-")
        || work.bundle_record.relative_path
            != format!("artifacts/{}.bin", work.bundle_record.artifact_id)
        || !valid_sha256(&work.bundle_record.content_digest)
        || work.bundle_record.byte_length > (900 * 1024 + MAX_SCHEMA_BYTES) as u64
        || work.bundle.script_id != work.invocation.script_id
        || work.bundle.interpreter != work.interpreter
        || work.invocation.operation_id != work.operation_id
        || work.invocation.run_id != work.run_id
        || work.invocation.controller_effects != work.bundle.controller_effects
        || work.bundle_record.metadata["script_id"] != work.bundle.script_id
        || work.bundle_record.metadata["revision"] != work.invocation.script_revision
        || work.bundle_record.metadata["bundle_sha256"] != work.bundle_record.content_digest
        || work.bundle_record.metadata["interpreter_sha256"] != work.interpreter.sha256
        || !valid_sha256(&work.environment_sha256)
        || sha256_hex(canonical_json(&serde_json::to_value(&work.environment)?)?.as_bytes())
            != work.environment_sha256
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work identity or grant differs",
        ));
    }
    validate_bundle(&work.bundle)?;
    work.invocation.validate().map_err(script_error)?;
    work.bundle
        .input_schema
        .validate_value(&work.invocation.input)
        .map_err(script_error)?;
    validate_environment(&work.environment)?;
    if let Some(executor) = &work.executor {
        validate_executor_shape(executor)?;
    }
    let bundle = canonical_json(&serde_json::to_value(&work.bundle)?)?;
    if bundle.len() as u64 != work.bundle_record.byte_length
        || sha256_hex(bundle.as_bytes()) != work.bundle_record.content_digest
    {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "bundle artifact digest differs from immutable content",
        ));
    }
    Ok(())
}

fn validate_bundle(bundle: &ScriptBundle) -> Result<()> {
    if bundle.bundle_version != 1
        || bundle.trust != ScriptTrust::TrustedLocal
        || bundle.script_id.is_empty()
        || bundle.script_id.len() > 64
        || !bundle.script_id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
        || bundle.files.is_empty()
        || bundle.files.len() > 64
        || bundle.argv.len() > MAX_ARGUMENTS
        || bundle
            .argv
            .iter()
            .any(|value| value.len() > MAX_ARGUMENT_BYTES || value.contains('\0'))
        || bundle.inherit_environment.len() > MAX_INHERITED_ENVIRONMENT
        || bundle.controller_effects.len() > MAX_CONTROLLER_EFFECTS
        || bundle.interpreter.sha256.len() != 64
        || !valid_sha256(&bundle.interpreter.sha256)
        || bundle.interpreter.byte_length == 0
        || bundle.interpreter.byte_length > MAX_INTERPRETER_BYTES
        || !bundle.interpreter.canonical_path.is_absolute()
    {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "retained bundle metadata is invalid",
        ));
    }
    validate_bundle_path(&bundle.entrypoint).map_err(script_error)?;
    bundle
        .input_schema
        .validate_definition(0)
        .map_err(script_error)?;
    bundle
        .result_schema
        .validate_definition(0)
        .map_err(script_error)?;
    let schema_pair = json!({"input":bundle.input_schema,"result":bundle.result_schema});
    if canonical_json(&schema_pair)?.len() > MAX_SCHEMA_BYTES {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "retained schemas exceed 32 KiB",
        ));
    }
    let mut paths = BTreeSet::new();
    let mut total = 0usize;
    let mut entrypoint_present = false;
    for file in &bundle.files {
        validate_bundle_path(&file.path).map_err(script_error)?;
        if !paths.insert(file.path.to_ascii_lowercase()) {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "bundle paths are not unique",
            ));
        }
        entrypoint_present |= file.path == bundle.entrypoint;
        let bytes = STANDARD
            .decode(&file.content_base64)
            .map_err(|_| Error::new("SCRIPT_BUNDLE_DAMAGED", "bundle source is not base64"))?;
        if bytes.len() > MAX_BUNDLE_FILE_BYTES
            || bytes.len() as u64 != file.byte_length
            || sha256_hex(&bytes) != file.sha256
        {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "bundle file digest or size differs",
            ));
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| Error::invalid("bundle size overflow"))?;
        if total > MAX_BUNDLE_BYTES {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "bundle exceeds 512 KiB",
            ));
        }
    }
    if !entrypoint_present {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "bundle entrypoint is missing",
        ));
    }
    let mut names = BTreeSet::new();
    for name in &bundle.inherit_environment {
        if !valid_environment_name(name) || !names.insert(name.to_ascii_uppercase()) {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "bundle environment names are invalid",
            ));
        }
    }
    swarm_scripts::schema::validate_effect_grants(&bundle.controller_effects)
        .map_err(script_error)?;
    Ok(())
}

fn validate_environment(environment: &std::collections::BTreeMap<String, String>) -> Result<()> {
    if environment.len() > MAX_INHERITED_ENVIRONMENT + 8
        || environment.iter().any(|(name, value)| {
            name.is_empty()
                || name.to_ascii_uppercase().starts_with("SWARM_")
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || value.len() > MAX_ENVIRONMENT_VALUE_BYTES
                || value.contains('\0')
        })
        || canonical_json(&serde_json::to_value(environment)?)?.len() > MAX_ENVIRONMENT_BYTES
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "captured child environment exceeds its bounds",
        ));
    }
    Ok(())
}

fn validate_executor_shape(pin: &ExecutorPin) -> Result<()> {
    if !pin.executable.is_absolute()
        || !valid_sha256(&pin.sha256)
        || !opaque_atom(&pin.artifact_id, true)
        || !opaque_atom(&pin.version, false)
    {
        return Err(Error::invalid("standalone script executor pin is invalid"));
    }
    Ok(())
}

/// Validate the public configuration shape for a selected local executor.
pub fn validate_executor_pin(pin: &ExecutorPin) -> Result<()> {
    validate_executor_shape(pin)
}

/// Verify a selected executor file before the kernel launches it. The path is
/// returned canonicalized, but the configured identity remains in the Work
/// receipt and is checked again by the standalone worker itself.
pub fn verify_pinned_executor_file(pin: &ExecutorPin) -> Result<PathBuf> {
    validate_executor_shape(pin)?;
    let metadata = fs::symlink_metadata(&pin.executable)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        return Err(Error::new(
            "SCRIPT_EXECUTOR_INVALID",
            "selected script executor is not a regular file",
        ));
    }
    let canonical = fs::canonicalize(&pin.executable)?;
    if sha256_file(&canonical, 512 * 1024 * 1024)? != pin.sha256 {
        return Err(Error::new(
            "SCRIPT_EXECUTOR_CHANGED",
            "selected script executor differs from its configured digest",
        ));
    }
    Ok(canonical)
}

fn opaque_atom(value: &str, allow_colon: bool) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || b"._+-".contains(&byte) || (allow_colon && byte == b':')
        })
}

fn verify_executor(pin: &ExecutorPin) -> Result<()> {
    let configured = verify_pinned_executor_file(pin)?;
    let current = fs::canonicalize(std::env::current_exe()?)?;
    if configured != current {
        return Err(Error::new(
            "SCRIPT_EXECUTOR_CHANGED",
            "running script worker differs from the selected executable pin",
        ));
    }
    Ok(())
}

fn directory(data_dir: &Path, run_id: &str) -> Result<PathBuf> {
    if uuid::Uuid::parse_str(run_id).is_err() || !data_dir.is_absolute() {
        return Err(Error::invalid("invalid ScriptRun directory identity"));
    }
    Ok(data_dir.join("script-runs").join(run_id))
}

fn ensure_run_directory(data_dir: &Path, dir: &Path) -> Result<()> {
    let data = fs::canonicalize(data_dir)?;
    let runs = data_dir.join("script-runs");
    let runs_metadata = fs::symlink_metadata(&runs)?;
    if is_link_or_reparse(&runs_metadata) || !runs_metadata.is_dir() {
        return Err(Error::new(
            "SCRIPT_PATH",
            "script run root is not a regular directory",
        ));
    }
    let canonical_runs = fs::canonicalize(&runs)?;
    if canonical_runs.parent() != Some(data.as_path()) {
        return Err(Error::new(
            "SCRIPT_PATH",
            "script run root escaped the state directory",
        ));
    }
    let metadata = fs::symlink_metadata(dir)?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        return Err(Error::new(
            "SCRIPT_PATH",
            "script run directory is not regular",
        ));
    }
    let canonical_dir = fs::canonicalize(dir)?;
    if canonical_dir.parent() != Some(canonical_runs.as_path()) {
        return Err(Error::new(
            "SCRIPT_PATH",
            "script run directory escaped its root",
        ));
    }
    private_permissions(&canonical_runs, true)?;
    private_permissions(&canonical_dir, true)?;
    Ok(())
}

fn verify_ready_worker(work: &ScriptWork, dir: &Path) -> Result<()> {
    let identity = read_json(&dir.join("worker.json"))?;
    if identity["run_id"] != work.run_id
        || identity["operation_id"] != work.operation_id
        || identity["token"] != work.token
        || identity["control_version"] != 1
        || identity["process"]["purpose"] != "script"
    {
        return Err(Error::conflict(
            "ready worker identity differs from the admitted ScriptRun",
        ));
    }
    let lock_path = dir.join("worker.lock");
    let lock_metadata = fs::symlink_metadata(&lock_path)?;
    if is_link_or_reparse(&lock_metadata) || !lock_metadata.is_file() {
        return Err(Error::new(
            "SCRIPT_PATH",
            "worker lock is not a regular file",
        ));
    }
    let lock = OpenOptions::new().read(true).write(true).open(lock_path)?;
    match lock.try_lock() {
        Ok(()) => {
            return Err(Error::new(
                "SCRIPT_WORKER_LOST",
                "worker lock is free before Go",
            ));
        }
        Err(std::fs::TryLockError::WouldBlock) => Ok(()),
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

fn verify_go(work: &ScriptWork, dir: &Path) -> Result<()> {
    let go = read_json(&dir.join("go.json"))?;
    if go["run_id"] != work.run_id
        || go["operation_id"] != work.operation_id
        || go["token"] != work.token
    {
        return Err(Error::new(
            "SCRIPT_START_GATE_INVALID",
            "Go receipt differs from the admitted ScriptRun",
        ));
    }
    Ok(())
}

fn make_plan(work: &ScriptWork, work_sha256: &str, dir: &Path) -> Result<ScriptWorkerPlan> {
    let executor = work.executor.clone().ok_or_else(|| {
        Error::new(
            "SCRIPT_EXECUTOR_UNSELECTED",
            "run has no standalone executor",
        )
    })?;
    let stage = dir.join("bundle");
    let entrypoint = safe_stage_path(&stage, &work.bundle.entrypoint)?;
    let process = plan_process(
        work.interpreter.kind,
        &path_text(&work.interpreter.canonical_path)?,
        &path_text(&entrypoint)?,
        &work.bundle.argv,
        work.environment.clone(),
    )
    .map_err(script_error)?;
    Ok(ScriptWorkerPlan {
        schema_version: 1,
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        executor,
        work_sha256: work_sha256.to_owned(),
        bundle_sha256: work.bundle_record.content_digest.clone(),
        interpreter_sha256: work.interpreter.sha256.clone(),
        environment_sha256: work.environment_sha256.clone(),
        process,
        invocation: work.invocation.clone(),
        result_schema: work.bundle.result_schema.clone(),
        granted_effects: work.bundle.controller_effects.clone(),
    })
}

fn validate_plan_against_work(
    plan: &ScriptWorkerPlan,
    work: &ScriptWork,
    work_sha256: &str,
    dir: &Path,
) -> Result<()> {
    let expected = make_plan(work, work_sha256, dir)?;
    let expected_invocation = canonical_json(&serde_json::to_value(&expected.invocation)?)?;
    let actual_invocation = canonical_json(&serde_json::to_value(&plan.invocation)?)?;
    if plan.schema_version != 1
        || plan.run_id != work.run_id
        || plan.operation_id != work.operation_id
        || plan.token != work.token
        || plan.executor != expected.executor
        || plan.work_sha256 != work_sha256
        || plan.bundle_sha256 != work.bundle_record.content_digest
        || plan.interpreter_sha256 != work.interpreter.sha256
        || plan.environment_sha256 != work.environment_sha256
        || plan.process != expected.process
        || actual_invocation != expected_invocation
        || plan.result_schema != expected.result_schema
        || plan.granted_effects != expected.granted_effects
    {
        return Err(Error::new(
            "SCRIPT_PLAN_INVALID",
            "post-Go plan differs from immutable Work or interpreter profile",
        ));
    }
    Ok(())
}

fn wait_for_go(work: &ScriptWork, dir: &Path) -> Result<()> {
    let gate = dir.join("go.json");
    let denial = dir.join("deny.json");
    let deadline = Instant::now() + START_GATE_TIMEOUT;
    loop {
        if receipt_exists(&gate)? {
            return verify_go(work, dir);
        }
        if receipt_exists(&denial)? {
            let denied = read_json(&denial)?;
            if denied["run_id"] != work.run_id
                || denied["operation_id"] != work.operation_id
                || denied["token"] != work.token
            {
                return Err(Error::new(
                    "SCRIPT_START_DENIAL_INVALID",
                    "denial receipt identity differs",
                ));
            }
            return Err(Error::new(
                denied["error_code"]
                    .as_str()
                    .unwrap_or("SCRIPT_START_DENIED"),
                "script start was denied by Store",
            ));
        }
        if Instant::now() >= deadline {
            return Err(Error::new(
                "SCRIPT_START_GATE_TIMEOUT",
                "Store did not publish Go",
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn wait_for_plan(work: &ScriptWork, work_sha256: &str, dir: &Path) -> Result<ScriptWorkerPlan> {
    let plan_path = dir.join("execution-plan.json");
    let ready_path = dir.join("execution-plan-ready.json");
    let deadline = Instant::now() + START_GATE_TIMEOUT;
    loop {
        if receipt_exists(&plan_path)? && receipt_exists(&ready_path)? {
            let ready: PlanReady =
                serde_json::from_value(read_json(&ready_path)?).map_err(|_| {
                    Error::new("SCRIPT_PLAN_INVALID", "plan-ready receipt cannot be parsed")
                })?;
            let plan_bytes = read_bytes(&plan_path, CONTROL_LIMIT as u64)?;
            if ready.schema_version != 1
                || ready.run_id != work.run_id
                || ready.operation_id != work.operation_id
                || ready.token != work.token
                || ready.work_sha256 != work_sha256
                || !valid_sha256(&ready.plan_sha256)
                || sha256_hex(&plan_bytes) != ready.plan_sha256
            {
                return Err(Error::new(
                    "SCRIPT_PLAN_INVALID",
                    "plan receipt identity or digest differs",
                ));
            }
            let plan: ScriptWorkerPlan = serde_json::from_slice(&plan_bytes).map_err(|_| {
                Error::new(
                    "SCRIPT_PLAN_INVALID",
                    "script execution plan cannot be parsed",
                )
            })?;
            validate_plan_against_work(&plan, work, work_sha256, dir)?;
            let executor = work.executor.as_ref().ok_or_else(|| {
                Error::new(
                    "SCRIPT_EXECUTOR_UNSELECTED",
                    "run has no standalone executor",
                )
            })?;
            verify_executor(executor)?;
            verify_interpreter(&work.interpreter)?;
            verify_staged_bundle(&work.bundle, &dir.join("bundle"))?;
            return Ok(plan);
        }
        if Instant::now() >= deadline {
            return Err(Error::new(
                "SCRIPT_PLAN_TIMEOUT",
                "Store did not materialize the post-Go execution plan",
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn execute(
    work: &ScriptWork,
    plan: &ScriptWorkerPlan,
    dir: &Path,
    group: &Group,
    identity: &Value,
) -> Result<()> {
    let input = validate_invocation_size(&plan.invocation).map_err(script_error)?;
    if input.len() > MAX_INVOCATION_BYTES {
        return Err(Error::invalid("script invocation exceeds 260 KiB"));
    }
    verify_interpreter(&work.interpreter)?;
    let mut command = Command::new(&plan.process.executable);
    command
        .args(&plan.process.arguments)
        .current_dir(dir.join("bundle"))
        .env_clear()
        .envs(&plan.process.environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let started_at_ms = now_ms()?;
    let mut child = command.spawn()?;
    if let Err(error) = write_once(
        &dir.join("started.json"),
        &canonical_json(&json!({"pid":child.id(),"started_at_ms":started_at_ms,"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token}))?.into_bytes(),
    ) {
        let _ = group.cancel_children();
        return Err(Error::new("SCRIPT_CHILD_STARTED", error.to_string()));
    }
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("SCRIPT_PIPE_FAILED", "script stdout pipe is missing"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("SCRIPT_PIPE_FAILED", "script stderr pipe is missing"))?;
    let stdout_flag = Arc::clone(&overflow);
    let stdout_reader = thread::Builder::new()
        .name("script-stdout".into())
        .spawn(move || read_bounded(stdout, MAX_RESULT_BYTES, stdout_flag))?;
    let stderr_flag = Arc::clone(&overflow);
    let stderr_reader = thread::Builder::new()
        .name("script-stderr".into())
        .spawn(move || read_bounded(stderr, MAX_STDERR_BYTES, stderr_flag))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::new("SCRIPT_PIPE_FAILED", "script stdin pipe is missing"))?;
    let input_writer = thread::Builder::new()
        .name("script-stdin".into())
        .spawn(move || stdin.write_all(&input))?;

    let deadline =
        Instant::now() + Duration::from_millis(plan.process.timeout_ms.min(MAX_SCRIPT_DURATION_MS));
    let mut timed_out = false;
    let mut status = None;
    loop {
        if overflow.load(Ordering::Acquire) {
            let _ = group.cancel_children();
            break;
        }
        match child.try_wait() {
            Ok(Some(exit)) => {
                status = Some(exit);
                break;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = group.cancel_children();
                return Err(error.into());
            }
        }
        if Instant::now() >= deadline {
            timed_out = true;
            let _ = group.cancel_children();
            break;
        }
        thread::sleep(POLL_INTERVAL);
    }
    if status.is_none() {
        status = Some(child.wait()?);
    }
    while !group.children_empty()? {
        let _ = group.cancel_children();
        thread::sleep(POLL_INTERVAL);
    }
    group.disarm()?;
    let writer_result = input_writer
        .join()
        .map_err(|_| Error::new("SCRIPT_PIPE_FAILED", "script stdin writer panicked"))?;
    let stdout = stdout_reader
        .join()
        .map_err(|_| Error::new("SCRIPT_PIPE_FAILED", "script stdout reader panicked"))?;
    let stderr = stderr_reader
        .join()
        .map_err(|_| Error::new("SCRIPT_PIPE_FAILED", "script stderr reader panicked"))?;
    let exit_code = status.and_then(|status| status.code());
    let projection = project_completion(
        ProcessOutcome {
            stdout: &stdout.bytes,
            exit: ProcessExit::Observed { exit_code },
            timed_out,
            output_overflow: stdout.overflow || stderr.overflow,
            input_failed: writer_result.is_err(),
        },
        &work.operation_id,
        &work.run_id,
        &plan.result_schema,
        &plan.granted_effects,
    )
    .map_err(script_error)?
    .ok_or_else(|| Error::new("SCRIPT_OUTCOME_UNKNOWN", "child exit was not observed"))?;
    publish_completion(
        work,
        CompletionPublication {
            stdout: &stdout.bytes,
            stderr: &stderr.bytes,
            result_value: projection.result,
            controller_effects: &projection.controller_effects,
            error_code: projection.error_code,
            state: &projection.state,
            started_at_ms: Some(started_at_ms),
            exit_code: projection.exit_code,
            process: &identity["process"],
        },
    )
}

fn finish_worker_error(
    work: &ScriptWork,
    group: &Group,
    identity: &Value,
    error_code: String,
    state: &str,
) -> Result<()> {
    let _ = group.cancel_children();
    while !group.children_empty()? {
        thread::sleep(POLL_INTERVAL);
    }
    group.disarm()?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    let started_at_ms = if receipt_exists(&dir.join("started.json"))? {
        read_json(&dir.join("started.json"))?["started_at_ms"].as_i64()
    } else {
        None
    };
    publish_completion(
        work,
        CompletionPublication {
            stdout: &[],
            stderr: &[],
            result_value: None,
            controller_effects: &[],
            error_code: Some(error_code),
            state,
            started_at_ms,
            exit_code: None,
            process: &identity["process"],
        },
    )
}

fn publish_completion(work: &ScriptWork, publication: CompletionPublication<'_>) -> Result<()> {
    let CompletionPublication {
        stdout: stdout_bytes,
        stderr: stderr_bytes,
        result_value,
        controller_effects,
        error_code,
        state,
        started_at_ms,
        exit_code,
        process,
    } = publication;
    let files = ArtifactFiles::new(&work.data_dir)?;
    let stdout = output_record(work, "stdout", stdout_bytes);
    let stderr = output_record(work, "stderr", stderr_bytes);
    files.publish(&stdout, stdout_bytes)?;
    files.publish(&stderr, stderr_bytes)?;
    let result_document = json!({
        "protocol_version":1,
        "run_id":work.run_id,
        "operation_id":work.operation_id,
        "script_id":work.bundle.script_id,
        "script_revision":work.invocation.script_revision,
        "state":state,
        "started_at_ms":started_at_ms,
        "exit_code":exit_code,
        "error_code":error_code,
        "result":result_value,
        "stdout_ref":stdout.artifact_id,
        "stderr_ref":stderr.artifact_id,
        "controller_effects":controller_effects,
    });
    let result_id = format!("scriptresult-{}", sha256_hex(work.operation_id.as_bytes()));
    let result_bytes = canonical_json(&result_document)?.into_bytes();
    let result = ArtifactRecord {
        kind: "script_result".into(),
        artifact_id: result_id.clone(),
        relative_path: format!("artifacts/{result_id}.bin"),
        byte_length: result_bytes.len() as u64,
        content_digest: sha256_hex(&result_bytes),
        metadata: json!({"run_id":work.run_id,"operation_id":work.operation_id,"script_id":work.bundle.script_id,"state":state}),
    };
    files.publish(&result, &result_bytes)?;
    let completion = Completion {
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: state.into(),
        started_at_ms,
        exit_code,
        process: process.clone(),
        result,
        stdout,
        stderr,
        result_value,
        controller_effects: controller_effects.to_vec(),
        error_code,
    };
    let dir = directory(&work.data_dir, &work.run_id)?;
    let bytes = canonical_json(&serde_json::to_value(completion)?)?.into_bytes();
    write_once(&dir.join("terminal.json"), &bytes)?;
    write_once(&dir.join("completion.json"), &bytes)?;
    Ok(())
}

fn output_record(work: &ScriptWork, stream: &str, bytes: &[u8]) -> ArtifactRecord {
    let mut identity = Vec::with_capacity(work.run_id.len() + stream.len() + 1);
    identity.extend_from_slice(work.run_id.as_bytes());
    identity.push(b':');
    identity.extend_from_slice(stream.as_bytes());
    let id = format!("scriptlog-{}", sha256_hex(&identity));
    ArtifactRecord {
        kind: "script_output".into(),
        artifact_id: id.clone(),
        relative_path: format!("artifacts/{id}.bin"),
        byte_length: bytes.len() as u64,
        content_digest: sha256_hex(bytes),
        metadata: json!({"run_id":work.run_id,"operation_id":work.operation_id,"stream":stream}),
    }
}

fn stage_bundle(work: &ScriptWork, run_dir: &Path) -> Result<()> {
    let bundle_root = run_dir.join("bundle");
    match fs::symlink_metadata(&bundle_root) {
        Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
            return Err(Error::new(
                "SCRIPT_STAGE_INVALID",
                "script stage root is not a regular directory",
            ));
        }
        Ok(_) => {
            return Err(Error::new(
                "SCRIPT_STAGE_EXISTS",
                "bundle stage already exists without a completion receipt",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir(&bundle_root)?;
    private_permissions(&bundle_root, true)?;
    let mut total = 0usize;
    for file in &work.bundle.files {
        let bytes = STANDARD
            .decode(&file.content_base64)
            .map_err(|_| Error::new("SCRIPT_BUNDLE_DAMAGED", "bundle source is not base64"))?;
        if bytes.len() as u64 != file.byte_length || sha256_hex(&bytes) != file.sha256 {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "staged source differs from its immutable digest",
            ));
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| Error::invalid("bundle size overflow"))?;
        if total > MAX_BUNDLE_BYTES {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "bundle exceeds 512 KiB",
            ));
        }
        let target = safe_stage_path(&bundle_root, &file.path)?;
        if let Some(parent) = target.parent() {
            ensure_private_tree(&bundle_root, parent)?;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        output.write_all(&bytes)?;
        output.sync_all()?;
        drop(output);
        private_permissions(&target, false)?;
    }
    Ok(())
}

fn verify_staged_bundle(bundle: &ScriptBundle, root: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(root)?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        return Err(Error::new(
            "SCRIPT_STAGE_INVALID",
            "bundle stage root is not a regular directory",
        ));
    }
    let canonical_root = fs::canonicalize(root)?;
    for file in &bundle.files {
        let path = safe_stage_path(&canonical_root, &file.path)?;
        let bytes = read_bytes(&path, MAX_BUNDLE_FILE_BYTES as u64)?;
        if bytes.len() as u64 != file.byte_length || sha256_hex(&bytes) != file.sha256 {
            return Err(Error::new(
                "SCRIPT_STAGE_INVALID",
                "staged file differs from the immutable bundle",
            ));
        }
    }
    let entrypoint = safe_stage_path(&canonical_root, &bundle.entrypoint)?;
    if !fs::symlink_metadata(entrypoint)?.is_file() {
        return Err(Error::new(
            "SCRIPT_STAGE_INVALID",
            "staged entrypoint is not a regular file",
        ));
    }
    Ok(())
}

fn safe_stage_path(root: &Path, relative: &str) -> Result<PathBuf> {
    validate_bundle_path(relative).map_err(script_error)?;
    let root = fs::canonicalize(root)?;
    let mut target = root.clone();
    for component in relative.split('/') {
        target.push(component);
    }
    if !target.starts_with(&root) {
        return Err(Error::new(
            "SCRIPT_STAGE_INVALID",
            "bundle path escaped its stage",
        ));
    }
    Ok(target)
}

fn ensure_private_tree(root: &Path, destination: &Path) -> Result<()> {
    let root = fs::canonicalize(root)?;
    let relative = destination
        .strip_prefix(&root)
        .map_err(|_| Error::new("SCRIPT_STAGE_INVALID", "bundle directory escaped its stage"))?;
    let mut current = root;
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(Error::new(
                "SCRIPT_STAGE_INVALID",
                "bundle stage path is not normalized",
            ));
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
                return Err(Error::new(
                    "SCRIPT_STAGE_INVALID",
                    "bundle stage contains a link or non-directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                private_permissions(&current, true)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_bounded<R: Read>(mut input: R, limit: usize, overflow: Arc<AtomicBool>) -> Captured {
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0u8; 8192];
    let mut local_overflow = false;
    loop {
        match input.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                let remaining = limit.saturating_sub(bytes.len());
                bytes.extend_from_slice(&buffer[..count.min(remaining)]);
                if count > remaining {
                    local_overflow = true;
                    overflow.store(true, Ordering::Release);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    Captured {
        bytes,
        overflow: local_overflow,
    }
}

struct ArtifactFiles {
    root: PathBuf,
}

impl ArtifactFiles {
    fn new(data_dir: &Path) -> Result<Self> {
        let canonical_data = fs::canonicalize(data_dir)?;
        let root = data_dir.join("artifacts");
        match fs::symlink_metadata(&root) {
            Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
                return Err(Error::new(
                    "ARTIFACT_PATH",
                    "artifact root is not a regular directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&root)?,
            Err(error) => return Err(error.into()),
        }
        let canonical_root = fs::canonicalize(&root)?;
        if canonical_root.parent() != Some(canonical_data.as_path()) {
            return Err(Error::new(
                "ARTIFACT_PATH",
                "artifact root escaped the controller data directory",
            ));
        }
        private_permissions(&canonical_root, true)?;
        Ok(Self {
            root: canonical_root,
        })
    }

    fn path(&self, record: &ArtifactRecord) -> Result<PathBuf> {
        let prefix = match record.kind.as_str() {
            "script_result" => "scriptresult-",
            "script_output" => "scriptlog-",
            _ => {
                return Err(Error::new(
                    "ARTIFACT_KIND",
                    "unsupported ScriptRun artifact kind",
                ));
            }
        };
        let digest = record.artifact_id.strip_prefix(prefix).unwrap_or("");
        if !valid_sha256(digest)
            || record.relative_path != format!("artifacts/{}.bin", record.artifact_id)
        {
            return Err(Error::new(
                "ARTIFACT_PATH",
                "generated ScriptRun artifact identity is invalid",
            ));
        }
        Ok(self.root.join(format!("{}.bin", record.artifact_id)))
    }

    fn publish(&self, record: &ArtifactRecord, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != record.byte_length || sha256_hex(bytes) != record.content_digest {
            return Err(Error::invalid(
                "artifact bytes differ from their recorded digest",
            ));
        }
        let destination = self.path(record)?;
        if receipt_exists(&destination)? {
            return self.verify(record);
        }
        let temp = self
            .root
            .join(format!(".script-{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            write_private_new(&temp, bytes)?;
            match fs::hard_link(&temp, &destination) {
                Ok(()) => private_permissions(&destination, false),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    self.verify(record)
                }
                Err(error) => Err(error.into()),
            }
        })();
        let _ = fs::remove_file(&temp);
        result
    }

    fn verify(&self, record: &ArtifactRecord) -> Result<()> {
        let path = self.path(record)?;
        let bytes = read_bytes(
            &path,
            (MAX_RESULT_BYTES + MAX_STDERR_BYTES + 64 * 1024) as u64,
        )?;
        if bytes.len() as u64 != record.byte_length || sha256_hex(&bytes) != record.content_digest {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "retained ScriptRun artifact differs from its digest",
            ));
        }
        Ok(())
    }
}

fn verify_interpreter(interpreter: &InterpreterIdentity) -> Result<()> {
    if !interpreter.canonical_path.is_absolute() || !valid_sha256(&interpreter.sha256) {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "interpreter identity is invalid",
        ));
    }
    let metadata = fs::symlink_metadata(&interpreter.canonical_path)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "interpreter is not a regular file",
        ));
    }
    let canonical = fs::canonicalize(&interpreter.canonical_path)?;
    let length = metadata.len();
    if canonical != interpreter.canonical_path
        || length != interpreter.byte_length
        || length == 0
        || length > MAX_INTERPRETER_BYTES
        || sha256_file(&canonical, MAX_INTERPRETER_BYTES)? != interpreter.sha256
    {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "interpreter changed after ScriptRun admission",
        ));
    }
    Ok(())
}

fn sha256_file(path: &Path, maximum: u64) -> Result<String> {
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if !before.is_file() || before.len() == 0 || before.len() > maximum {
        return Err(Error::new(
            "SCRIPT_IMAGE_INVALID",
            "executable is outside its bounded file limit",
        ));
    }
    let modified = before.modified()?;
    let mut hash = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or_else(|| Error::invalid("executable size overflow"))?;
        if total > maximum {
            return Err(Error::new(
                "SCRIPT_IMAGE_INVALID",
                "executable exceeds its bounded file limit",
            ));
        }
        hash.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    if total != before.len() || after.len() != before.len() || after.modified()? != modified {
        return Err(Error::new(
            "SCRIPT_IMAGE_CHANGED",
            "executable changed while its digest was computed",
        ));
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn canonical_json(value: &Value) -> Result<String> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, value)| (key.clone(), ordered(value)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(ordered).collect()),
            value => value.clone(),
        }
    }
    serde_json::to_string(&ordered(value)).map_err(Into::into)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("CLOCK_ERROR", "system clock precedes Unix epoch"))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| Error::new("CLOCK_ERROR", "timestamp overflow"))
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<()> {
    if receipt_exists(path)? {
        let existing = read_bytes(path, CONTROL_LIMIT as u64)?;
        if existing == bytes {
            return Ok(());
        }
        return Err(Error::conflict("retained ScriptRun control file differs"));
    }
    let temp = path.with_file_name(format!(".script-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        write_private_new(&temp, bytes)?;
        match fs::hard_link(&temp, path) {
            Ok(()) => private_permissions(path, false),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_bytes(path, CONTROL_LIMIT as u64)? == bytes {
                    Ok(())
                } else {
                    Err(Error::conflict("retained ScriptRun control file differs"))
                }
            }
            Err(error) => Err(error.into()),
        }
    })();
    let _ = fs::remove_file(&temp);
    result
}

fn read_json(path: &Path) -> Result<Value> {
    let bytes = read_bytes(path, CONTROL_LIMIT as u64)?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn read_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() || metadata.len() > maximum {
        return Err(Error::new(
            "SCRIPT_CONTROL_INVALID",
            "receipt is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::new(
            "SCRIPT_CONTROL_INVALID",
            "receipt exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn receipt_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn path_text(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| Error::invalid("ScriptRun path is not Unicode"))?;
    if value.is_empty() || value.contains('\0') {
        return Err(Error::invalid("ScriptRun path is invalid"));
    }
    Ok(value.to_owned())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_environment_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn script_error(error: swarm_scripts::ScriptError) -> Error {
    Error::new(error.code(), error.to_string())
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}
