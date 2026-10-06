//! Zed native `eval-cli` is a one-shot batch executor, not a session service.
//! This unit implements exactly that boundary: describe the configured
//! entrypoint and run one batch to its native terminal state, publishing the
//! files the pinned binary writes (`result.json`, `thread.md`, `thread.json`)
//! as immutable artifacts. There is no persistent control, resume, goal,
//! steer or family surface, and exit 0 means the agent run finished — it is
//! not Task acceptance. The contract basis is the pinned upstream source
//! recorded as ZD-EXEC in `docs/agent_swarm.runtime-sources-v16.json`;
//! the installed binary has not been live-qualified (`installed_runtime_verified`
//! stays false), so result readback is cross-checked against the exit code
//! and the configured model instead of being trusted on its own.

use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord, MAX_PAGE_BYTES},
    error::{Error, Result},
    model,
    runtime::{RuntimeCommand, RuntimeOutcome, batch::prompt_facts},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

pub const ARTIFACT_ID: &str = "eliot-zed.eval-cli.1";
pub const RUNTIME: &str = "zed";
pub const CONTRACT_REVISION: &str = "zed-eval-cli-v1";
/// Pinned upstream basis of the eval-cli contract (ZD-EXEC).
pub const UPSTREAM_BASIS: &str = "7604aa3f19cef0c4d8be2bb3335c24acd788ccb1";

const NATIVE_OUTPUTS: [&str; 3] = ["result.json", "thread.md", "thread.json"];
const MAX_INSTRUCTION_BYTES: usize = 1024 * 1024;
const MAX_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;
/// The native binary enforces its own `--timeout` and exits 2. The host
/// deadline only guards a hung binary that never reaches that code path.
const HOST_GRACE_SECONDS: u64 = 5;
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    /// Stable operator-assigned namespace. A PID/path is not durable identity.
    pub scope_id: String,
    /// Program name resolved through PATH, or an absolute/relative path.
    pub executable: String,
    pub workdir: PathBuf,
    /// Exact `provider/model` string passed to `--model`. The binary's own
    /// built-in default is never accepted implicitly.
    pub model: String,
    pub timeout_seconds: u64,
    /// Names of host environment variables handed to the child (provider API
    /// keys). Values are never read into controller state, persisted or logged.
    #[serde(default)]
    pub env_keys: Vec<String>,
}
impl Options {
    pub fn parse(value: &Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::new("CONFIG_ERROR", "invalid Zed route options"))?;
        let valid_name = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        };
        if !valid_name(&options.scope_id)
            || options.executable.is_empty()
            || options.executable.len() > 512
            || options.executable.bytes().any(|b| b.is_ascii_control())
            || !options.workdir.is_absolute()
            || options.workdir.to_str().is_none()
            || !valid_model(&options.model)
            || !(1..=86_400).contains(&options.timeout_seconds)
            || options.env_keys.len() > 32
            || options.env_keys.iter().any(|k| !valid_env_key(k))
            || options
                .env_keys
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != options.env_keys.len()
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "Zed requires a scope ID, an executable, an absolute workdir, an explicit provider/model, a bounded timeout and unique environment key names",
            ));
        }
        Ok(options)
    }
    pub(crate) fn scope(&self) -> String {
        format!("zed:{}", self.scope_id)
    }
}

fn valid_model(model: &str) -> bool {
    let mut parts = model.split('/');
    let (Some(provider), Some(name), None) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    [provider, name].iter().all(|part| {
        !part.is_empty()
            && part.len() <= 128
            && !part.bytes().any(|b| b.is_ascii_control() || b == b' ')
    })
}

fn valid_env_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && !key.as_bytes()[0].is_ascii_digit()
        && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Resolve a program name through an explicit PATH value, or a path as given.
/// Pure path facts only: nothing is executed here.
pub(crate) fn resolve_program(
    program: &str,
    path_env: Option<&std::ffi::OsStr>,
) -> Result<PathBuf> {
    let candidate = Path::new(program);
    let resolved = if candidate.components().count() > 1 {
        candidate.to_path_buf()
    } else {
        let dirs = path_env
            .map(|p| std::env::split_paths(p).collect::<Vec<_>>())
            .unwrap_or_default();
        dirs.iter()
            .map(|dir| dir.join(program))
            .find(|p| is_executable_file(p))
            .ok_or_else(|| {
                Error::new(
                    "NATIVE_EXECUTABLE_NOT_FOUND",
                    "eval-cli executable is not on PATH",
                )
            })?
    };
    if !is_executable_file(&resolved) {
        return Err(Error::new(
            "NATIVE_EXECUTABLE_NOT_FOUND",
            "eval-cli executable does not exist or is not executable",
        ));
    }
    Ok(resolved)
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Facts about the configured entrypoint. Launching nothing, this is the
/// describe half of the batch boundary: what would run, under which exact
/// model, and which capabilities honestly do not exist for it.
pub fn describe(options: &Options) -> Result<Value> {
    let resolved = resolve_program(&options.executable, std::env::var_os("PATH").as_deref())?;
    if !options.workdir.is_dir() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "Zed workdir does not exist or is not a directory",
        ));
    }
    let env_presence: Value = options
        .env_keys
        .iter()
        .map(|k| (k.clone(), json!(std::env::var_os(k).is_some())))
        .collect::<serde_json::Map<_, _>>()
        .into();
    Ok(json!({
        "runtime": RUNTIME,
        "module_artifact_id": ARTIFACT_ID,
        "entrypoint": "eval_cli_batch",
        "contract_revision": CONTRACT_REVISION,
        "upstream_basis": UPSTREAM_BASIS,
        "installed_runtime_verified": false,
        "scope": options.scope(),
        "executable": {"configured": options.executable, "resolved": resolved},
        "workdir": options.workdir,
        "model": options.model,
        "timeout_seconds": options.timeout_seconds,
        "env_keys_present": env_presence,
        "outputs": NATIVE_OUTPUTS,
        "exit_codes": {"0": "agent_finished", "1": "error", "2": "timeout", "3": "interrupted"},
        "capabilities": {
            "batch_run": true,
            "persistent_control": false,
            "resume": false,
            "goal": false,
            "steer": false,
            "session_family": false
        }
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchDisposition {
    /// Exit 0 with a matching native result: the agent run finished.
    Completed,
    /// Exit 1: model/auth/runtime failure reported by the native binary.
    Error,
    /// Exit 2, or the host deadline had to terminate a hung binary.
    Timeout,
    /// Exit 3: interrupted by SIGTERM/SIGINT; partial outputs may exist.
    Interrupted,
    /// Any other exit/signal shape: an evidence gap, never a success.
    UnexpectedExit,
}
impl BatchDisposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Interrupted => "interrupted",
            Self::UnexpectedExit => "unexpected_exit",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchOutcome {
    pub run_id: String,
    pub disposition: BatchDisposition,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    /// True only when this controller terminated the child at its own host
    /// deadline because the native timeout never fired.
    pub host_terminated: bool,
    /// `native_batch_result_recorded` when a validated `result.json` backs the
    /// disposition; otherwise the exit code is the only native evidence.
    pub completion_condition: &'static str,
    /// Compact projection of the native `result.json`; never the transcript.
    pub native_result: Option<Value>,
    /// Digest of the exact validated `result.json` bytes published as pages.
    pub native_result_sha256: Option<String>,
    pub artifacts: Vec<ArtifactRecord>,
    pub output_dir: PathBuf,
}

/// Durable, non-secret identity written before the native process is spawned.
/// The run directory itself is the no-replay marker after a crash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchIntent {
    pub version: u8,
    pub operation_id: String,
    pub run_id: String,
    pub binding_id: String,
    pub generation: i64,
    pub route_sha256: String,
    pub prompt_sha256: String,
    pub prompt_bytes: usize,
    pub task_snapshot_sha256: String,
}

/// Exact native terminal and immutable page identities retained for restart
/// reconciliation. It contains no prompt, environment values or workspace path.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchReceipt {
    pub version: u8,
    pub intent: BatchIntent,
    pub outcome: RuntimeOutcome,
    pub artifacts: Vec<ArtifactRecord>,
}

/// Frozen runtime inputs required to identify one persisted receipt during
/// reconciliation. Borrowed so the caller cannot mutate the authority while
/// the receipt is being checked.
#[derive(Debug, Clone, Copy)]
pub struct BatchReadContext<'a> {
    pub operation_id: &'a str,
    pub binding_id: &'a str,
    pub generation: i64,
    pub route: &'a Value,
    pub instruction: &'a str,
    pub task_snapshot: &'a Value,
}

#[derive(Debug, Deserialize)]
struct EvalResult {
    status: String,
    #[serde(default)]
    error: Option<String>,
    duration_secs: f64,
    #[serde(default)]
    timeout_secs: Option<u64>,
    model: String,
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    step_count: Option<u64>,
    #[serde(default)]
    tool_call_count: Option<u64>,
    #[serde(default)]
    tool_calls: Option<BTreeMap<String, u64>>,
}

struct NativeResult {
    bytes: Vec<u8>,
    sha256: String,
    projection: Value,
}

impl EvalResult {
    fn validate(&self, options: &Options, exit_code: Option<i32>) -> Result<()> {
        let expected_status = match exit_code {
            Some(0) => "completed",
            Some(1) => "error",
            Some(2) => "timeout",
            Some(3) => "interrupted",
            _ => {
                return Err(Error::new(
                    "NATIVE_RESULT_MISMATCH",
                    "native result exists for an unclassified exit",
                ));
            }
        };
        if self.status != expected_status
            || self.model != options.model
            || !self.duration_secs.is_finite()
            || self.duration_secs < 0.0
            || self
                .timeout_secs
                .is_some_and(|t| t != options.timeout_seconds)
            || (self.status == "error") != self.error.is_some()
        {
            return Err(Error::new(
                "NATIVE_RESULT_MISMATCH",
                "result.json disagrees with the exit code, the configured model or its own error field",
            ));
        }
        Ok(())
    }
    fn projection(&self) -> Value {
        json!({
            "status": self.status,
            "error_reported": self.error.is_some(),
            "duration_secs": self.duration_secs,
            "timeout_secs": self.timeout_secs,
            "model": self.model,
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cache_creation_input_tokens": self.cache_creation_input_tokens,
                "cache_read_input_tokens": self.cache_read_input_tokens,
                "basis": "native_batch_result",
                "scope": "single_batch_run"
            },
            "step_count": self.step_count,
            "tool_call_count": self.tool_call_count,
            "tool_calls": self.tool_calls
        })
    }
}

pub fn run_id(operation_id: &str) -> String {
    format!("zed_{}", model::digest(operation_id.as_bytes()))
}

/// Run one batch to its native terminal state and publish every native
/// output file the run produced. A nonzero exit is a native fact carried in
/// the outcome, not a controller error; only contract violations (no launch,
/// contradictory or oversized native evidence) return `Err`.
pub fn run_batch(
    options: &Options,
    operation_id: &str,
    instruction: &str,
    output_root: &Path,
    artifacts: &ArtifactFiles,
) -> Result<BatchOutcome> {
    run_batch_inner(
        options,
        operation_id,
        instruction,
        output_root,
        artifacts,
        None,
    )
}

pub fn run_batch_command(
    options: &Options,
    command: &RuntimeCommand,
    instruction: &str,
    output_root: &Path,
    artifacts: &ArtifactFiles,
) -> Result<(BatchOutcome, BatchIntent)> {
    let intent = make_intent(command, instruction)?;
    let outcome = run_batch_inner(
        options,
        &command.operation_id,
        instruction,
        output_root,
        artifacts,
        Some(intent.clone()),
    )?;
    Ok((outcome, intent))
}

pub fn make_intent(command: &RuntimeCommand, instruction: &str) -> Result<BatchIntent> {
    if command.method != "task.dispatch" {
        return Err(Error::invalid(
            "Zed batch executor only accepts task.dispatch",
        ));
    }
    let task_snapshot = command
        .input
        .get("task_snapshot")
        .ok_or_else(|| Error::invalid("immutable task snapshot is required"))?;
    let facts = prompt_facts(instruction, task_snapshot)?;
    Ok(BatchIntent {
        version: 1,
        operation_id: command.operation_id.clone(),
        run_id: run_id(&command.operation_id),
        binding_id: command.binding_id.clone(),
        generation: command.generation,
        route_sha256: model::digest(model::canonical(&command.route)?.as_bytes()),
        prompt_sha256: model::text(&facts, "prompt_sha256")?.to_owned(),
        prompt_bytes: facts["prompt_bytes"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| Error::invalid("batch prompt length is out of range"))?,
        task_snapshot_sha256: model::text(&facts, "task_snapshot_sha256")?.to_owned(),
    })
}

fn run_batch_inner(
    options: &Options,
    operation_id: &str,
    instruction: &str,
    output_root: &Path,
    artifacts: &ArtifactFiles,
    intent: Option<BatchIntent>,
) -> Result<BatchOutcome> {
    if instruction.trim().is_empty() || instruction.len() > MAX_INSTRUCTION_BYTES {
        return Err(Error::invalid(
            "batch instruction must be nonempty and within the frame bound",
        ));
    }
    if !options.workdir.is_dir() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "Zed workdir does not exist or is not a directory",
        ));
    }
    let resolved = resolve_program(&options.executable, std::env::var_os("PATH").as_deref())?;
    let run = run_id(operation_id);
    std::fs::create_dir_all(output_root)?;
    validate_output_root(output_root)?;
    let output_dir = output_root.join(&run);
    if std::fs::create_dir(&output_dir).is_err() {
        return Err(Error::conflict(
            "batch run identity already has an output directory; refusing to overwrite native evidence",
        ));
    }
    validate_run_directory(output_root, operation_id)?;
    crate::platform::private_permissions(&output_dir, true)?;
    let intent = intent.unwrap_or_else(|| BatchIntent {
        version: 1,
        operation_id: operation_id.to_owned(),
        run_id: run.clone(),
        binding_id: String::new(),
        generation: 0,
        route_sha256: String::new(),
        prompt_sha256: model::digest(instruction.as_bytes()),
        prompt_bytes: instruction.len(),
        task_snapshot_sha256: String::new(),
    });
    if intent.operation_id != operation_id || intent.run_id != run {
        return Err(Error::invalid("batch intent does not identify this run"));
    }
    write_json_new(&output_dir.join("intent.json"), &json!(intent))?;
    let stdout_path = output_dir.join("stdout.log");
    let stderr_path = output_dir.join("stderr.log");
    let stdout = File::create(&stdout_path)?;
    let stderr = File::create(&stderr_path)?;
    crate::platform::private_permissions(&stdout_path, false)?;
    crate::platform::private_permissions(&stderr_path, false)?;
    let mut command = Command::new(&resolved);
    command
        .arg("--workdir")
        .arg(&options.workdir)
        .arg("--model")
        .arg(&options.model)
        .arg("--instruction")
        .arg(instruction)
        .arg("--timeout")
        .arg(options.timeout_seconds.to_string())
        .arg("--output-dir")
        .arg(&output_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .env_clear();
    // Host facts the native binary needs, plus exactly the key names the
    // route declares. Values pass through the process environment only.
    for key in ["PATH", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for key in &options.env_keys {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|e| {
        Error::new(
            "NATIVE_LAUNCH_FAILED",
            format!("eval-cli did not start: {e}"),
        )
    })?;
    let deadline = Duration::from_secs(options.timeout_seconds + HOST_GRACE_SECONDS);
    let started = Instant::now();
    let (status, host_terminated) = wait_bounded(&mut child, deadline, started)?;
    let exit_code = status.code();
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    let disposition = if host_terminated {
        BatchDisposition::Timeout
    } else {
        match exit_code {
            Some(0) => BatchDisposition::Completed,
            Some(1) => BatchDisposition::Error,
            Some(2) => BatchDisposition::Timeout,
            Some(3) => BatchDisposition::Interrupted,
            _ => BatchDisposition::UnexpectedExit,
        }
    };
    // Exit 0 without a valid result is a contract violation, not a finish:
    // the pinned binary writes result.json on every classified path.
    let native = read_native_result(&output_dir, options, exit_code)?;
    if disposition == BatchDisposition::Completed && native.is_none() {
        return Err(Error::new(
            "NATIVE_RESULT_MISSING",
            "eval-cli exited 0 without result.json",
        ));
    }
    let mut published = Vec::new();
    let output_context = BatchOutputContext {
        operation_id,
        run_id: &run,
        intent: &intent,
        disposition,
    };
    for name in NATIVE_OUTPUTS {
        let path = output_dir.join(name);
        if name == "result.json" {
            if let Some(native) = &native {
                publish_output_bytes(
                    artifacts,
                    &output_context,
                    name,
                    &native.bytes,
                    &mut published,
                )?;
            } else {
                match std::fs::symlink_metadata(&path) {
                    Ok(_) => {
                        return Err(Error::new(
                            "NATIVE_RESULT_CHANGED",
                            "result.json appeared after native result inspection",
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
        } else if path.is_file() {
            publish_output(artifacts, &output_context, name, &path, &mut published)?;
        }
    }
    Ok(BatchOutcome {
        run_id: run,
        disposition,
        exit_code,
        signal,
        host_terminated,
        completion_condition: if native.is_some() {
            "native_batch_result_recorded"
        } else {
            "native_batch_exit_classified"
        },
        native_result: native.as_ref().map(|result| result.projection.clone()),
        native_result_sha256: native.as_ref().map(|result| result.sha256.clone()),
        artifacts: published,
        output_dir,
    })
}

pub fn run_directory(output_root: &Path, operation_id: &str) -> PathBuf {
    output_root.join(run_id(operation_id))
}

fn validate_output_root(output_root: &Path) -> Result<PathBuf> {
    let metadata = std::fs::symlink_metadata(output_root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "BATCH_PATH",
            "batch evidence root must be a regular directory",
        ));
    }
    Ok(std::fs::canonicalize(output_root)?)
}

fn validate_run_directory(output_root: &Path, operation_id: &str) -> Result<PathBuf> {
    let root = validate_output_root(output_root)?;
    let directory = run_directory(output_root, operation_id);
    let metadata = std::fs::symlink_metadata(&directory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "BATCH_PATH",
            "batch run directory must be a regular child directory",
        ));
    }
    let canonical = std::fs::canonicalize(&directory)?;
    if canonical.parent() != Some(root.as_path()) {
        return Err(Error::new(
            "BATCH_PATH",
            "batch run directory escaped the evidence root",
        ));
    }
    Ok(canonical)
}

pub fn persist_receipt(
    output_root: &Path,
    artifacts: &ArtifactFiles,
    route: &Value,
    receipt: &BatchReceipt,
) -> Result<()> {
    validate_receipt_content(receipt, artifacts, route)?;
    let dir = validate_run_directory(output_root, &receipt.intent.operation_id)?;
    let intent_value = read_json_bounded(&dir.join("intent.json"), 65_536)?;
    let saved_intent: BatchIntent = serde_json::from_value(intent_value)
        .map_err(|_| Error::new("BATCH_INTENT_INVALID", "batch intent schema is invalid"))?;
    if saved_intent != receipt.intent
        || receipt.version != 1
        || receipt.outcome.operation_id != saved_intent.operation_id
        || receipt.intent.run_id != run_id(&receipt.intent.operation_id)
    {
        return Err(Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "terminal receipt differs from its durable batch intent",
        ));
    }
    let path = dir.join("terminal.json");
    let value = json!(receipt);
    if path.try_exists()? {
        let existing = read_json_bounded(&path, 4 * 1024 * 1024)?;
        if model::canonical(&existing)? != model::canonical(&value)? {
            return Err(Error::conflict(
                "terminal batch receipt already exists with different evidence",
            ));
        }
        return Ok(());
    }
    write_json_new(&path, &value)
}

pub fn read_receipt(
    output_root: &Path,
    context: BatchReadContext<'_>,
    artifacts: &ArtifactFiles,
) -> Result<Option<BatchReceipt>> {
    validate_output_root(output_root)?;
    let candidate = run_directory(output_root, context.operation_id);
    let dir = match std::fs::symlink_metadata(&candidate) {
        Ok(_) => validate_run_directory(output_root, context.operation_id)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let path = dir.join("terminal.json");
    if !path.try_exists()? {
        return Ok(None);
    }
    let raw = read_json_bounded(&path, 4 * 1024 * 1024)?;
    let receipt: BatchReceipt = serde_json::from_value(raw)
        .map_err(|_| Error::new("BATCH_RECEIPT_INVALID", "saved batch receipt is invalid"))?;
    let facts = prompt_facts(context.instruction, context.task_snapshot)?;
    if receipt.version != 1
        || receipt.intent.operation_id != context.operation_id
        || receipt.outcome.operation_id != context.operation_id
        || receipt.intent.run_id != run_id(context.operation_id)
        || receipt.intent.binding_id != context.binding_id
        || receipt.intent.generation != context.generation
        || receipt.intent.route_sha256 != model::digest(model::canonical(context.route)?.as_bytes())
        || receipt.intent.prompt_sha256 != facts["prompt_sha256"]
        || receipt.intent.prompt_bytes as u64 != facts["prompt_bytes"].as_u64().unwrap_or(u64::MAX)
        || receipt.intent.task_snapshot_sha256 != facts["task_snapshot_sha256"]
    {
        return Err(Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "saved terminal evidence does not match the frozen operation input",
        ));
    }
    validate_receipt_content(&receipt, artifacts, context.route)?;
    Ok(Some(receipt))
}

/// Verify each retained output and bind result.json's exact bytes to its
/// recorded digest and parsed native projection before persistence/readback.
fn validate_receipt_content(
    receipt: &BatchReceipt,
    artifacts: &ArtifactFiles,
    route: &Value,
) -> Result<()> {
    validate_receipt_manifest(receipt)?;
    let mismatch = || {
        Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "terminal result bytes do not match the validated native projection",
        )
    };
    if receipt.intent.route_sha256 != model::digest(model::canonical(route)?.as_bytes()) {
        return Err(mismatch());
    }
    for record in &receipt.artifacts {
        if record.metadata["native_output"] != "result.json" {
            artifacts.verify(record)?;
        }
    }

    let result_records = receipt
        .artifacts
        .iter()
        .filter(|record| record.metadata["native_output"] == "result.json")
        .collect::<Vec<_>>();
    if receipt.outcome.details["native_result"].is_null() {
        if !result_records.is_empty() || !receipt.outcome.details["native_result_sha256"].is_null()
        {
            return Err(mismatch());
        }
        return Ok(());
    }

    let options = Options::parse(&route["native_options"]).map_err(|_| mismatch())?;
    if receipt.outcome.details["requested_model"] != options.model
        || receipt.outcome.details["effective_model"] != options.model
        || receipt.outcome.details["effective_model_status"] != "observed"
    {
        return Err(mismatch());
    }
    let expected_digest = receipt.outcome.details["native_result_sha256"]
        .as_str()
        .ok_or_else(mismatch)?;
    let total = result_records.iter().try_fold(0usize, |total, record| {
        let page_len = usize::try_from(record.byte_length).map_err(|_| mismatch())?;
        total.checked_add(page_len).ok_or_else(mismatch)
    })?;
    if u64::try_from(total).map_err(|_| mismatch())? > MAX_OUTPUT_BYTES || result_records.is_empty()
    {
        return Err(mismatch());
    }
    let mut bytes = Vec::with_capacity(total);
    for record in result_records {
        let page = artifacts.verified_bytes(record)?;
        bytes.extend_from_slice(&page);
    }
    if bytes.len() != total || model::digest(&bytes) != expected_digest {
        return Err(mismatch());
    }
    let result: EvalResult = serde_json::from_slice(&bytes).map_err(|_| mismatch())?;
    let exit_code = receipt.outcome.details["exit_code"]
        .as_i64()
        .and_then(|value| i32::try_from(value).ok());
    result
        .validate(&options, exit_code)
        .map_err(|_| mismatch())?;
    if result.projection() != receipt.outcome.details["native_result"] {
        return Err(mismatch());
    }
    Ok(())
}

/// Bind the receipt's terminal correlation and both artifact indexes to the
/// exact retained page records. A receipt with internally inconsistent refs
/// must stay unresolved during restart reconciliation.
fn validate_receipt_manifest(receipt: &BatchReceipt) -> Result<()> {
    let mismatch = || {
        Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "terminal receipt run identity or artifact manifest is inconsistent",
        )
    };
    if receipt.version != 1
        || receipt.intent.run_id != run_id(&receipt.intent.operation_id)
        || receipt.outcome.operation_id != receipt.intent.operation_id
        || receipt.outcome.details["batch_run_id"] != receipt.intent.run_id
    {
        return Err(mismatch());
    }

    let mut refs = Vec::with_capacity(receipt.artifacts.len());
    let mut outputs = BTreeMap::<String, Vec<String>>::new();
    let mut page_groups = BTreeMap::<String, Vec<(usize, usize)>>::new();
    let mut seen = BTreeSet::new();
    for record in &receipt.artifacts {
        let output = record.metadata["native_output"]
            .as_str()
            .ok_or_else(mismatch)?;
        let page = record.metadata["page"]
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(mismatch)?;
        let page_count = record.metadata["pages"]
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(mismatch)?;
        let identity = format!("{}:zed:{output}:{page}", receipt.intent.operation_id);
        let expected_id = format!("result-{}", model::digest(identity.as_bytes()));
        if record.kind != "native_result_page"
            || !NATIVE_OUTPUTS.contains(&output)
            || page_count == 0
            || page >= page_count
            || record.byte_length > MAX_PAGE_BYTES as u64
            || record.artifact_id != expected_id
            || record.relative_path != format!("artifacts/{expected_id}.bin")
            || record.metadata["operation_id"] != receipt.intent.operation_id
            || record.metadata["run_id"] != receipt.intent.run_id
            || record.metadata["binding_id"] != receipt.intent.binding_id
            || record.metadata["binding_generation"] != receipt.intent.generation
            || !seen.insert(record.artifact_id.as_str())
        {
            return Err(mismatch());
        }
        refs.push(json!(record.artifact_id));
        outputs
            .entry(output.to_owned())
            .or_default()
            .push(record.artifact_id.clone());
        page_groups
            .entry(output.to_owned())
            .or_default()
            .push((page, page_count));
    }
    for group in page_groups.values_mut() {
        group.sort_unstable();
        let Some((_, expected_count)) = group.first().copied() else {
            return Err(mismatch());
        };
        if group.len() != expected_count
            || group.iter().any(|(_, count)| *count != expected_count)
            || group
                .iter()
                .enumerate()
                .any(|(expected_page, (page, _))| *page != expected_page)
        {
            return Err(mismatch());
        }
    }
    if !receipt.outcome.details["native_result"].is_null()
        && !page_groups.contains_key("result.json")
    {
        return Err(mismatch());
    }
    if receipt.outcome.details["artifact_refs"] != json!(refs)
        || receipt.outcome.details["output_artifact_refs"] != json!(outputs)
    {
        return Err(mismatch());
    }
    Ok(())
}

fn write_json_new(path: &Path, value: &Value) -> Result<()> {
    #[cfg(unix)]
    let parent = path
        .parent()
        .ok_or_else(|| Error::invalid("batch control file has no parent"))?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(model::canonical(value)?.as_bytes())?;
    file.sync_all()?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn read_json_bounded(path: &Path, max_bytes: u64) -> Result<Value> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > max_bytes {
        return Err(Error::new(
            "BATCH_CONTROL_FILE_INVALID",
            "batch control file is missing, linked or outside its size bound",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(max_bytes + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(Error::new(
            "BATCH_CONTROL_FILE_INVALID",
            "batch control file exceeds its size bound",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "BATCH_CONTROL_FILE_INVALID",
            "batch control file is invalid JSON",
        )
    })
}

fn wait_bounded(
    child: &mut Child,
    deadline: Duration,
    started: Instant,
) -> Result<(std::process::ExitStatus, bool)> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok((status, false));
        }
        if started.elapsed() >= deadline {
            terminate(child);
            return Ok((child.wait()?, true));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn terminate(child: &mut Child) {
    #[cfg(target_os = "linux")]
    {
        // The child leads its own process group; the batch tree is this
        // controller's own short-lived executor, unlike a native agent family.
        let pgid = child.id() as libc::pid_t;
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
    let _ = child.kill();
}

fn read_native_result(
    output_dir: &Path,
    options: &Options,
    exit_code: Option<i32>,
) -> Result<Option<NativeResult>> {
    let path = output_dir.join("result.json");
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded(&path)?;
    let result: EvalResult = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "NATIVE_RESULT_SCHEMA",
            "result.json does not match the pinned eval-cli result shape",
        )
    })?;
    result.validate(options, exit_code)?;
    Ok(Some(NativeResult {
        sha256: model::digest(&bytes),
        projection: result.projection(),
        bytes,
    }))
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "NATIVE_OUTPUT_INVALID",
            "native output must be a regular file",
        ));
    }
    if metadata.len() > MAX_OUTPUT_BYTES {
        return Err(Error::new(
            "NATIVE_OUTPUT_TOO_LARGE",
            "native output exceeds the publication bound; refusing to truncate evidence",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_OUTPUT_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_OUTPUT_BYTES {
        return Err(Error::new(
            "NATIVE_OUTPUT_TOO_LARGE",
            "native output exceeds the publication bound; refusing to truncate evidence",
        ));
    }
    Ok(bytes)
}

struct BatchOutputContext<'a> {
    operation_id: &'a str,
    run_id: &'a str,
    intent: &'a BatchIntent,
    disposition: BatchDisposition,
}

fn publish_output(
    artifacts: &ArtifactFiles,
    context: &BatchOutputContext<'_>,
    name: &str,
    path: &Path,
    published: &mut Vec<ArtifactRecord>,
) -> Result<()> {
    let bytes = read_bounded(path)?;
    publish_output_bytes(artifacts, context, name, &bytes, published)
}

fn publish_output_bytes(
    artifacts: &ArtifactFiles,
    context: &BatchOutputContext<'_>,
    name: &str,
    bytes: &[u8],
    published: &mut Vec<ArtifactRecord>,
) -> Result<()> {
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![&[]]
    } else {
        bytes.chunks(MAX_PAGE_BYTES).collect()
    };
    let pages = chunks.len();
    for (page, chunk) in chunks.into_iter().enumerate() {
        let record = ArtifactFiles::record(
            &format!("{}:zed:{name}:{page}", context.operation_id),
            chunk,
            json!({
                "operation_id": context.intent.operation_id,
                "binding_id": context.intent.binding_id,
                "binding_generation": context.intent.generation,
                "runtime": RUNTIME,
                "contract_revision": CONTRACT_REVISION,
                "run_id": context.run_id,
                "native_output": name,
                "page": page,
                "pages": pages,
                "disposition": context.disposition.as_str()
            }),
        );
        artifacts.publish(&record, chunk)?;
        published.push(record);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
