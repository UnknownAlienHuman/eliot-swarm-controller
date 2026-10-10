//! Store-side ScriptRun receipt and observation protocol. Execution is owned
//! by the standalone `swarm-script-worker` process.
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model,
    platform::{
        self,
        process_group::{departed_empty, spawned_departed, spawned_identity},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
};

use super::{
    manifest::{self, InterpreterIdentity, MAX_RESULT_BYTES, MAX_STDERR_BYTES, ScriptBundle},
    protocol::{ScriptEffectRequest, ScriptInvocation},
    registry,
};
use swarm_scripts::process::{
    MAX_PROCESS_CONTROL_FAILURE_BYTES, PROCESS_CONTROL_FAILURE_FILE, ProcessControlFailure,
};

const CONTROL_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Work {
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub data_dir: PathBuf,
    pub bundle_record: ArtifactRecord,
    pub bundle: ScriptBundle,
    pub interpreter: InterpreterIdentity,
    pub environment: BTreeMap<String, String>,
    pub environment_sha256: String,
    pub invocation: ScriptInvocation,
    /// `None` is retained only to decode pre-cutover receipts for reconciliation.
    /// New runs require a pin and never fall back to a root-binary executor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<swarm_script_worker::ExecutorPin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerReceipt {
    run_id: String,
    work_digest: String,
    data_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EarlyExitReceipt {
    schema_version: u8,
    state: EarlyExitState,
    run_id: String,
    operation_id: String,
    token: String,
    pid: u32,
    exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum EarlyExitState {
    ExitedBeforeWorkerIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completion {
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub state: String,
    pub started_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub process: Value,
    pub result: ArtifactRecord,
    pub stdout: ArtifactRecord,
    pub stderr: ArtifactRecord,
    pub result_value: Option<Value>,
    #[serde(default)]
    pub controller_effects: Vec<ScriptEffectRequest>,
    pub error_code: Option<String>,
    #[serde(default)]
    pub process_facts: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedOutput {
    pub artifact: ArtifactRecord,
    pub capture: Value,
}

/// Exact standalone-worker fact retained when family, output capture, or
/// input-drain cleanup did not finish within its bounded grace period.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupPending {
    pub schema_version: u8,
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub state: String,
    pub started_at_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub error_code: Option<String>,
    pub process: Value,
    pub process_facts: Value,
    pub stdout: RetainedOutput,
    pub stderr: RetainedOutput,
    #[serde(default)]
    pub controller_effects: Vec<ScriptEffectRequest>,
}

pub fn directory(data_dir: &Path, run_id: &str) -> Result<PathBuf> {
    if uuid::Uuid::parse_str(run_id).is_err() {
        return Err(Error::invalid("invalid ScriptRun ID"));
    }
    Ok(data_dir.join("script-runs").join(run_id))
}

pub fn environment_sha256(environment: &BTreeMap<String, String>) -> Result<String> {
    Ok(model::digest(
        model::canonical(&json!(environment))?.as_bytes(),
    ))
}

/// Capture only the platform's small runtime baseline plus explicitly declared
/// names. These values are kept only in the private work file and child env.
pub fn capture_environment(bundle: &ScriptBundle) -> Result<BTreeMap<String, String>> {
    let mut names = vec!["PATH"];
    #[cfg(windows)]
    names.extend(["SystemRoot", "WINDIR", "TEMP", "TMP"]);
    #[cfg(unix)]
    names.extend(["HOME", "TMPDIR", "LANG", "LC_ALL", "TMP", "TEMP"]);
    names.extend(bundle.inherit_environment.iter().map(String::as_str));
    let mut values = BTreeMap::new();
    for name in names {
        if let Some((key, value)) = std::env::vars().find(|(key, _)| key.eq_ignore_ascii_case(name))
        {
            if value.len() > manifest::MAX_ENVIRONMENT_VALUE_BYTES {
                return Err(Error::invalid("captured environment value exceeds 16 KiB"));
            }
            values.insert(key, value);
        }
    }
    if values.len() > manifest::MAX_INHERITED_ENVIRONMENT + 8
        || model::canonical(&json!(values))?.len() > manifest::MAX_ENVIRONMENT_BYTES
    {
        return Err(Error::invalid(
            "captured script environment exceeds 256 KiB",
        ));
    }
    Ok(values)
}

/// Create both the immutable private work document and the CLI receipt before
/// reserving the DB row. A host restart uses these exact captured values.
pub fn write_work(work: &Work) -> Result<(PathBuf, String)> {
    validate_work(work)?;
    let pin = work.executor.as_ref().ok_or_else(|| {
        Error::new(
            "SCRIPT_EXECUTOR_UNSELECTED",
            "new ScriptRun requires a pinned standalone worker",
        )
    })?;
    swarm_script_worker::verify_pinned_executor_file(pin)?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    ensure_private_directory(&work.data_dir, &dir)?;
    let path = dir.join("work.json");
    let body = model::canonical(&json!(work))?.into_bytes();
    if body.len() > CONTROL_LIMIT {
        return Err(Error::invalid("script run control record exceeds 16 MiB"));
    }
    let digest = model::digest(&body);
    write_once(&path, &body)?;
    let receipt_path = dir.join("receipt.json");
    let receipt = WorkerReceipt {
        run_id: work.run_id.clone(),
        work_digest: digest.clone(),
        data_dir: work.data_dir.clone(),
    };
    write_once(
        &receipt_path,
        &model::canonical(&json!(receipt))?.into_bytes(),
    )?;
    Ok((receipt_path, digest))
}

pub fn read_work(path: &Path) -> Result<Work> {
    read_work_record(path).map(|(work, _)| work)
}

pub fn read_work_record(path: &Path) -> Result<(Work, String)> {
    let receipt_metadata = fs::symlink_metadata(path)?;
    if receipt_metadata.file_type().is_symlink()
        || !receipt_metadata.is_file()
        || receipt_metadata.len() > 4096
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt is not a small regular file",
        ));
    }
    let receipt: WorkerReceipt = serde_json::from_slice(&fs::read(path)?).map_err(|_| {
        Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt cannot be parsed",
        )
    })?;
    if uuid::Uuid::parse_str(&receipt.run_id).is_err()
        || receipt.work_digest.len() != 64
        || !receipt
            .work_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !receipt.data_dir.is_absolute()
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt identity is invalid",
        ));
    }
    let expected_dir = directory(&receipt.data_dir, &receipt.run_id)?;
    let expected_receipt = expected_dir.join("receipt.json");
    if path != expected_receipt {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script worker receipt path does not match its run directory",
        ));
    }
    let path = expected_dir.join("work.json");
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > CONTROL_LIMIT as u64
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work receipt is not a bounded regular file",
        ));
    }
    let bytes = fs::read(path)?;
    if model::digest(&bytes) != receipt.work_digest {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work receipt digest changed",
        ));
    }
    let digest = model::digest(&bytes);
    let work: Work = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work receipt cannot be parsed",
        )
    })?;
    if work.run_id != receipt.run_id || work.data_dir != receipt.data_dir || work.token.is_empty() {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work receipt identity differs",
        ));
    }
    validate_work(&work)?;
    Ok((work, digest))
}

pub fn prepare_and_spawn(work: &Work, work_digest: &str) -> Result<Value> {
    validate_work(work)?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    ensure_private_directory(&work.data_dir, &dir)?;
    let work_path = dir.join("work.json");
    let bytes = fs::read(&work_path)?;
    if model::digest(&bytes) != work_digest {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work receipt digest differs from Store",
        ));
    }
    if dir.join("launch.json").try_exists()? {
        return read_json(&dir.join("launch.json"));
    }
    if dir.join("worker.json").try_exists()? || dir.join("completion.json").try_exists()? {
        return Err(Error::new(
            "SCRIPT_WORKER_EXISTS",
            "script run already has a worker identity",
        ));
    }
    let current =
        manifest::capture_interpreter(&work.interpreter.canonical_path, work.interpreter.kind)?;
    if current.sha256 != work.interpreter.sha256
        || current.canonical_path != work.interpreter.canonical_path
    {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "configured interpreter changed after admission",
        ));
    }
    let pin = work.executor.as_ref().ok_or_else(|| {
        Error::new(
            "SCRIPT_EXECUTOR_UNSELECTED",
            "new ScriptRun requires a pinned standalone worker",
        )
    })?;
    let executable = swarm_script_worker::verify_pinned_executor_file(pin)?;
    let mut command = Command::new(executable);
    command.arg("--file").arg(dir.join("receipt.json"));
    command
        .env_clear()
        .envs(standalone_worker_environment())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = command.spawn()?;
    let pid = child.id();
    let (process, early_exit) = match spawned_identity(pid) {
        Ok(identity) => (Some(identity), None),
        Err(error) if error.code == "PROCESS_GONE" => {
            let status = child.try_wait().map_err(|_| {
                Error::new(
                    "SCRIPT_LAUNCH_UNKNOWN",
                    "worker process vanished before identity capture and its owned exit could not be confirmed",
                )
            })?.ok_or_else(|| {
                Error::new(
                    "SCRIPT_LAUNCH_UNKNOWN",
                    "worker process identity could not be captured and its owned exit was not confirmed",
                )
            })?;
            let dir = directory(&work.data_dir, &work.run_id)?;
            if receipt_exists(&dir.join("worker.json"))?
                || receipt_exists(&dir.join("start-decision.json"))?
                || receipt_exists(&dir.join("go.json"))?
                || receipt_exists(&dir.join("started.json"))?
                || receipt_exists(&dir.join("completion.json"))?
                || receipt_exists(&dir.join("terminal.json"))?
            {
                return Err(Error::new(
                    "SCRIPT_LAUNCH_UNKNOWN",
                    "worker exited before launch identity capture but run receipts prevent an early-exit proof",
                ));
            }
            (
                None,
                Some(EarlyExitReceipt {
                    schema_version: 1,
                    state: EarlyExitState::ExitedBeforeWorkerIdentity,
                    run_id: work.run_id.clone(),
                    operation_id: work.operation_id.clone(),
                    token: work.token.clone(),
                    pid,
                    exit_code: status.code(),
                }),
            )
        }
        Err(error) => {
            return Err(Error::new(
                "SCRIPT_LAUNCH_UNKNOWN",
                format!("worker started without a pinned launch identity: {error}"),
            ));
        }
    };
    let exited_before_identity = early_exit.is_some();
    let launch = json!({"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token,"spawned_at_ms":model::now_ms()? ,"process":process,"early_exit":early_exit});
    write_once(
        &dir.join("launch.json"),
        &model::canonical(&launch)?.into_bytes(),
    )?;
    if !exited_before_identity {
        thread::Builder::new()
            .name("script-worker-reaper".into())
            .spawn(move || {
                let _ = child.wait();
            })
            .map_err(|error| {
                Error::new(
                    "SCRIPT_LAUNCH_UNKNOWN",
                    format!("worker started but cannot be reaped: {error}"),
                )
            })?;
    }
    Ok(launch)
}

pub fn launch_record(work: &Work) -> Result<Option<Value>> {
    let path = directory(&work.data_dir, &work.run_id)?.join("launch.json");
    if path.try_exists()? {
        read_json(&path).map(Some)
    } else {
        Ok(None)
    }
}

pub fn ready(work: &Work) -> Result<Option<Value>> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let identity_path = dir.join("worker.json");
    if !identity_path.try_exists()? {
        return Ok(None);
    }
    let lock_path = dir.join("worker.lock");
    let lock = OpenOptions::new().read(true).write(true).open(lock_path)?;
    match lock.try_lock() {
        Ok(()) => {
            return Err(Error::new(
                "SCRIPT_WORKER_LOST",
                "worker lock is free without a completion receipt",
            ));
        }
        Err(std::fs::TryLockError::WouldBlock) => {}
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }
    let identity = read_json(&identity_path)?;
    if identity["run_id"] != work.run_id
        || identity["operation_id"] != work.operation_id
        || identity["token"] != work.token
        || identity["process"]["purpose"] != "script"
    {
        return Err(Error::conflict(
            "script worker identity differs from its Operation",
        ));
    }
    Ok(Some(identity))
}

/// Read the worker's single bounded cancellation-control diagnostic. This is
/// advisory evidence and never proves process departure or completion.
pub fn process_control_failure(work: &Work) -> Result<Option<ProcessControlFailure>> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let path = dir.join(PROCESS_CONTROL_FAILURE_FILE);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_PROCESS_CONTROL_FAILURE_BYTES as u64
    {
        return Err(Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "script cancellation diagnostic is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(MAX_PROCESS_CONTROL_FAILURE_BYTES);
    OpenOptions::new()
        .read(true)
        .open(&path)?
        .take((MAX_PROCESS_CONTROL_FAILURE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_PROCESS_CONTROL_FAILURE_BYTES {
        return Err(Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "script cancellation diagnostic exceeds its size limit",
        ));
    }
    let failure: ProcessControlFailure = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "script cancellation diagnostic cannot be parsed",
        )
    })?;
    let worker = read_json(&dir.join("worker.json"))?;
    if worker["run_id"] != work.run_id
        || worker["operation_id"] != work.operation_id
        || worker["token"] != work.token
        || failure
            .validate_for(
                &work.run_id,
                &work.operation_id,
                &work.token,
                &worker["process"],
            )
            .is_err()
    {
        return Err(Error::new(
            "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            "script cancellation diagnostic differs from its exact worker",
        ));
    }
    Ok(Some(failure))
}

pub fn allow(work: &Work) -> Result<()> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let decision = swarm_script_worker::ScriptStartDecision::Allow {
        schema_version: 1,
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        decided_at_ms: model::now_ms()?,
    };
    match swarm_script_worker::publish_start_decision(&dir, decision)? {
        swarm_script_worker::StartDecisionPublish::Retained(retained)
            if start_decision_allows_start(&retained)? == Some(true) => {}
        _ => {
            return Err(Error::conflict(
                "script start decision was denied or conflicted before Allow",
            ));
        }
    }
    if work.executor.is_some() {
        let receipt = directory(&work.data_dir, &work.run_id)?.join("receipt.json");
        swarm_script_worker::materialize_after_go(&receipt)?;
    }
    Ok(())
}

pub fn deny(work: &Work, error_code: &str) -> Result<()> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let decision = swarm_script_worker::ScriptStartDecision::Deny {
        schema_version: 1,
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        error_code: error_code.to_owned(),
        decided_at_ms: model::now_ms()?,
    };
    match swarm_script_worker::publish_start_decision(&dir, decision)? {
        swarm_script_worker::StartDecisionPublish::Retained(retained)
            if start_decision_allows_start(&retained)? == Some(false) =>
        {
            Ok(())
        }
        _ => Err(Error::conflict(
            "script start decision was allowed or conflicted before Deny",
        )),
    }
}

/// Deny a worker that has been spawned but has not passed the start gate when
/// the Store can no longer decode its immutable work receipt.
pub fn deny_unstarted(
    data_dir: &Path,
    run_id: &str,
    operation_id: &str,
    error_code: &str,
) -> Result<bool> {
    let dir = directory(data_dir, run_id)?;
    let launch_path = dir.join("launch.json");
    if !launch_path.try_exists()? {
        return Ok(true);
    }
    let launch = read_json(&launch_path)?;
    let token = launch["token"]
        .as_str()
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::new("SCRIPT_LAUNCH_DAMAGED", "launch receipt has no run token"))?;
    if launch["run_id"] != run_id || launch["operation_id"] != operation_id {
        return Err(Error::new(
            "SCRIPT_LAUNCH_DAMAGED",
            "launch receipt identifies another run",
        ));
    }
    let decision = swarm_script_worker::ScriptStartDecision::Deny {
        schema_version: 1,
        run_id: run_id.to_owned(),
        operation_id: operation_id.to_owned(),
        token: token.to_owned(),
        error_code: error_code.to_owned(),
        decided_at_ms: model::now_ms()?,
    };
    let published = swarm_script_worker::publish_start_decision(&dir, decision)?;
    match published {
        swarm_script_worker::StartDecisionPublish::Retained(
            swarm_script_worker::StartDecisionRead::Current(
                swarm_script_worker::ScriptStartDecision::Deny {
                    error_code: retained_code,
                    ..
                },
            ),
        )
        | swarm_script_worker::StartDecisionPublish::Retained(
            swarm_script_worker::StartDecisionRead::LegacyDeny {
                error_code: retained_code,
            },
        ) if retained_code == error_code => Ok(true),
        swarm_script_worker::StartDecisionPublish::Conflict(retained) => {
            match start_decision_allows_start(&retained)? {
                Some(true) => Err(Error::conflict(
                    "script start was already allowed before the unstarted denial",
                )),
                Some(false) => Err(Error::conflict(
                    "a different script denial is already retained",
                )),
                None => Err(Error::conflict(
                    "script denial conflicted without a retained decision",
                )),
            }
        }
        _ => Err(Error::conflict(
            "script denial did not retain the requested immutable decision",
        )),
    }
}

pub fn start_decision(work: &Work) -> Result<swarm_script_worker::StartDecisionRead> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    Ok(swarm_script_worker::read_start_decision(
        &dir,
        &work.run_id,
        &work.operation_id,
        &work.token,
    )?)
}

pub fn start_decision_allows_start(
    decision: &swarm_script_worker::StartDecisionRead,
) -> Result<Option<bool>> {
    match decision {
        swarm_script_worker::StartDecisionRead::Current(
            swarm_script_worker::ScriptStartDecision::Allow { .. },
        )
        | swarm_script_worker::StartDecisionRead::LegacyAllow => Ok(Some(true)),
        swarm_script_worker::StartDecisionRead::Current(
            swarm_script_worker::ScriptStartDecision::Deny { .. },
        )
        | swarm_script_worker::StartDecisionRead::LegacyDeny { .. } => Ok(Some(false)),
        swarm_script_worker::StartDecisionRead::Pending => Ok(None),
        swarm_script_worker::StartDecisionRead::Ambiguous => Err(Error::new(
            "SCRIPT_START_DECISION_AMBIGUOUS",
            "multiple start decisions are retained for this ScriptRun",
        )),
    }
}

pub fn has_start_gate(work: &Work) -> Result<bool> {
    Ok(start_decision_allows_start(&start_decision(work)?)? == Some(true))
}

pub fn terminal_receipt_exists(data_dir: &Path, run_id: &str) -> Result<bool> {
    let dir = directory(data_dir, run_id)?;
    Ok(
        receipt_exists(&dir.join("terminal.json"))?
            || receipt_exists(&dir.join("completion.json"))?,
    )
}

pub fn started_receipt_exists(data_dir: &Path, run_id: &str) -> Result<bool> {
    receipt_exists(&directory(data_dir, run_id)?.join("started.json"))
}

fn receipt_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub fn completion(work: &Work, files: &ArtifactFiles) -> Result<Option<Completion>> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let terminal_path = dir.join("terminal.json");
    let completion_path = dir.join("completion.json");
    let path = if terminal_path.try_exists()? {
        terminal_path
    } else if completion_path.try_exists()? {
        completion_path
    } else {
        return Ok(None);
    };
    let value = read_json(&path)?;
    let completion: Completion = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script completion receipt cannot be parsed",
        )
    })?;
    let worker = read_json(&directory(&work.data_dir, &work.run_id)?.join("worker.json"))?;
    if completion.run_id != work.run_id
        || completion.operation_id != work.operation_id
        || completion.token != work.token
        || completion.process != worker["process"]
        || !matches!(
            completion.state.as_str(),
            "completed" | "failed" | "incomplete"
        )
        || completion.result.kind != "script_result"
        || completion.stdout.kind != "script_output"
        || completion.stderr.kind != "script_output"
        || (completion.state == "completed"
            && (completion.exit_code != Some(0)
                || completion.error_code.is_some()
                || completion.result_value.is_none()
                || completion.started_at_ms.is_none()))
        || (completion.state != "completed"
            && (completion.error_code.is_none()
                || completion.result_value.is_some()
                || !completion.controller_effects.is_empty()))
        || completion.controller_effects.len() > manifest::MAX_CONTROLLER_EFFECTS
        || completion.controller_effects.iter().any(|effect| {
            !work.invocation.controller_effects.contains(&effect.effect)
                || effect.validate().is_err()
        })
        || completion.result.byte_length > (MAX_RESULT_BYTES + 16 * 1024) as u64
        || completion.stdout.byte_length > MAX_RESULT_BYTES as u64
        || completion.stderr.byte_length > MAX_STDERR_BYTES as u64
    {
        return Err(Error::conflict(
            "script completion identity or artifact types differ",
        ));
    }
    if let Some(process_facts) = completion.process_facts.as_ref() {
        validate_process_facts(process_facts, &worker["process"], false)?;
        if !capture_fact_matches_length(
            &process_facts["stdout_capture"],
            completion.stdout.byte_length,
        ) || !capture_fact_matches_length(
            &process_facts["stderr_capture"],
            completion.stderr.byte_length,
        ) {
            return Err(Error::conflict(
                "script completion capture facts differ from retained output sizes",
            ));
        }
    }
    files.verify(&completion.result)?;
    files.verify(&completion.stdout)?;
    files.verify(&completion.stderr)?;
    let expected_result_id = format!(
        "scriptresult-{}",
        model::digest(work.operation_id.as_bytes())
    );
    if completion.result.artifact_id != expected_result_id
        || completion.result.metadata
            != json!({
                "run_id":work.run_id,
                "operation_id":work.operation_id,
                "script_id":work.bundle.script_id,
                "state":completion.state,
            })
        || !output_matches(work, "stdout", &completion.stdout)
        || !output_matches(work, "stderr", &completion.stderr)
    {
        return Err(Error::conflict(
            "script completion artifact linkage differs from its run",
        ));
    }
    let result_value: Value = serde_json::from_slice(&files.document_bytes(&completion.result)?)
        .map_err(|_| {
            Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "script result document cannot be parsed",
            )
        })?;
    if result_value["protocol_version"] != 1
        || result_value["run_id"] != work.run_id
        || result_value["operation_id"] != work.operation_id
        || result_value["script_id"] != work.bundle.script_id
        || result_value["script_revision"] != work.invocation.script_revision
        || result_value["state"] != completion.state
        || result_value["started_at_ms"] != json!(completion.started_at_ms)
        || result_value["exit_code"] != json!(completion.exit_code)
        || result_value["error_code"] != json!(completion.error_code)
        || result_value["result"] != json!(completion.result_value)
        || result_value["stdout_ref"] != completion.stdout.artifact_id
        || result_value["stderr_ref"] != completion.stderr.artifact_id
        || result_value["controller_effects"] != json!(completion.controller_effects)
        || completion
            .process_facts
            .as_ref()
            .is_some_and(|facts| result_value["process_facts"] != *facts)
    {
        return Err(Error::conflict(
            "script result document differs from its completion receipt",
        ));
    }
    if let Some(value) = completion.result_value.as_ref() {
        work.bundle.result_schema.validate_value(value)?;
    }
    Ok(Some(completion))
}

/// Read and validate the standalone worker's retained cleanup fact. It keeps
/// partial output and its capture facts available while family ownership is
/// unresolved; this receipt never proves resource release.
pub fn cleanup_pending(work: &Work, files: &ArtifactFiles) -> Result<Option<CleanupPending>> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let path = dir.join("cleanup-pending.json");
    if !receipt_exists(&path)? {
        return Ok(None);
    }
    let pending: CleanupPending = serde_json::from_value(read_json(&path)?).map_err(|_| {
        Error::new(
            "SCRIPT_CLEANUP_PENDING_DAMAGED",
            "script cleanup-pending receipt cannot be parsed",
        )
    })?;
    let worker = read_json(&dir.join("worker.json"))?;
    if pending.schema_version != 1
        || pending.run_id != work.run_id
        || pending.operation_id != work.operation_id
        || pending.token != work.token
        || pending.state != "cleanup_pending"
        || pending.process != worker["process"]
        || !pending.controller_effects.is_empty()
        || pending.stdout.capture != pending.process_facts["stdout_capture"]
        || pending.stderr.capture != pending.process_facts["stderr_capture"]
    {
        return Err(Error::conflict(
            "script cleanup-pending identity or capture facts differ",
        ));
    }
    validate_process_facts(&pending.process_facts, &worker["process"], true)?;
    if !capture_fact_matches_length(&pending.stdout.capture, pending.stdout.artifact.byte_length)
        || !capture_fact_matches_length(
            &pending.stderr.capture,
            pending.stderr.artifact.byte_length,
        )
        || !output_matches(work, "stdout", &pending.stdout.artifact)
        || !output_matches(work, "stderr", &pending.stderr.artifact)
    {
        return Err(Error::conflict(
            "script cleanup-pending output facts differ from retained artifacts",
        ));
    }
    files.verify(&pending.stdout.artifact)?;
    files.verify(&pending.stderr.artifact)?;
    Ok(Some(pending))
}

/// Same-execution recovery after positive family departure. Captured bytes
/// remain exact and no script or controller effect is replayed.
pub fn recover_cleanup_pending(
    work: &Work,
    files: &ArtifactFiles,
    pending: &CleanupPending,
) -> Result<Option<Completion>> {
    if !departed_empty(&pending.process, &work.token)? {
        return Ok(None);
    }
    let dir = directory(&work.data_dir, &work.run_id)?;
    let proof_path = dir.join("cleanup-release.json");
    if !receipt_exists(&proof_path)? {
        let proof = json!({"run_id":work.run_id,"operation_id":work.operation_id,
            "token":work.token,"process":pending.process,"method":"departed_empty",
            "observed_at_ms":model::now_ms()?});
        write_once(&proof_path, &model::canonical(&proof)?.into_bytes())?;
    }
    let proof = read_json(&proof_path)?;
    if proof["run_id"] != work.run_id
        || proof["operation_id"] != work.operation_id
        || proof["token"] != work.token
        || proof["process"] != pending.process
        || proof["method"] != "departed_empty"
        || proof["observed_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
    {
        return Err(Error::conflict(
            "script cleanup release proof differs from its exact execution",
        ));
    }
    let mut facts = pending.process_facts.clone();
    facts["resource_released"] = json!(true);
    facts["cleanup_pending"] = json!(false);
    facts["family_departure"]["state"] = json!("confirmed");
    validate_process_facts(&facts, &pending.process, false)?;
    let error_code = pending
        .error_code
        .clone()
        .unwrap_or_else(|| "SCRIPT_CAPTURE_INCOMPLETE".to_owned());
    let report = json!({"protocol_version":1,"run_id":work.run_id,
        "operation_id":work.operation_id,"script_id":work.bundle.script_id,
        "script_revision":work.invocation.script_revision,"state":"incomplete",
        "started_at_ms":pending.started_at_ms,"exit_code":pending.exit_code,
        "error_code":error_code,"result":null,"stdout_ref":pending.stdout.artifact.artifact_id,
        "stderr_ref":pending.stderr.artifact.artifact_id,"controller_effects":[],
        "process_facts":facts,"execution_process_facts":pending.process_facts,
        "release_evidence":proof,"recovery":{"command_replayed":false,"effects_applied":false}});
    let id = format!(
        "scriptresult-{}",
        model::digest(format!("cleanup-release:{}", work.operation_id).as_bytes())
    );
    let (result, bytes) = ArtifactFiles::document(
        "script_result",
        &id,
        &report,
        json!({"run_id":work.run_id,"operation_id":work.operation_id,
            "script_id":work.bundle.script_id,"state":"incomplete"}),
    )?;
    files.publish(&result, &bytes)?;
    Ok(Some(Completion {
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: "incomplete".to_owned(),
        started_at_ms: pending.started_at_ms,
        exit_code: pending.exit_code,
        process: pending.process.clone(),
        result,
        stdout: pending.stdout.artifact.clone(),
        stderr: pending.stderr.artifact.clone(),
        result_value: None,
        controller_effects: Vec::new(),
        error_code: Some(error_code),
        process_facts: Some(facts),
    }))
}

fn validate_process_facts(facts: &Value, process: &Value, cleanup_pending: bool) -> Result<()> {
    let family = &facts["family_departure"];
    let family_state = family["state"].as_str();
    let direct_state = facts["direct_exit"]["state"].as_str();
    let family_valid = match (cleanup_pending, facts["resource_released"].as_bool()) {
        (false, Some(true)) => family_state == Some("confirmed"),
        (true, Some(true)) => family_state == Some("confirmed"),
        (true, Some(false)) => matches!(
            family_state,
            Some("confirmed" | "cleanup_pending" | "observation_unknown")
        ),
        _ => false,
    };
    if facts["schema_version"] != 1
        || facts["cleanup_pending"] != cleanup_pending
        || !family_valid
        || family["process"] != *process
        || !matches!(
            direct_state,
            Some("observed" | "observation_unknown" | "not_started")
        )
        || !capture_fact_matches_length(
            &facts["stdout_capture"],
            facts["stdout_capture"]["bytes_retained"]
                .as_u64()
                .unwrap_or(u64::MAX),
        )
        || !capture_fact_matches_length(
            &facts["stderr_capture"],
            facts["stderr_capture"]["bytes_retained"]
                .as_u64()
                .unwrap_or(u64::MAX),
        )
    {
        return Err(Error::conflict(
            "script process facts do not prove the declared cleanup state",
        ));
    }
    Ok(())
}

fn capture_fact_matches_length(fact: &Value, byte_length: u64) -> bool {
    let bytes_retained = fact["bytes_retained"].as_u64();
    let bytes_observed = fact["bytes_observed"].as_u64();
    let capture_complete = fact["capture_complete"].as_bool();
    let state = fact["state"].as_str();
    let error_valid = fact["capture_error"].is_null() || fact["capture_error"].is_string();
    matches!(state, Some("complete" | "incomplete" | "not_started"))
        && bytes_retained == Some(byte_length)
        && bytes_observed.is_some_and(|observed| observed >= byte_length)
        && fact["truncated"].as_bool().is_some()
        && capture_complete.is_some_and(|complete| {
            if state == Some("complete") {
                complete
            } else {
                !complete
            }
        })
        && error_valid
}

/// A validated terminal receipt is written only after interpreter output has
/// been drained and the worker has disarmed its owned Group. Wait for that
/// exact worker Group to leave before Store applies any declared effects.
pub fn completion_family_departed(work: &Work, completion: &Completion) -> Result<bool> {
    let dir = directory(&work.data_dir, &work.run_id)?;
    let worker = read_json(&dir.join("worker.json"))?;
    if worker["run_id"] != work.run_id
        || worker["operation_id"] != work.operation_id
        || worker["token"] != work.token
        || worker["process"] != completion.process
        || completion.run_id != work.run_id
        || completion.operation_id != work.operation_id
        || completion.token != work.token
        || completion.process["purpose"] != "script"
    {
        return Err(Error::new(
            "SCRIPT_WORKER_DAMAGED",
            "terminal ScriptRun does not retain its exact worker process identity",
        ));
    }
    departed_empty(&completion.process, &work.token)
}

fn output_matches(work: &Work, stream: &str, artifact: &ArtifactRecord) -> bool {
    let mut identity = Vec::with_capacity(work.run_id.len() + stream.len() + 1);
    identity.extend_from_slice(work.run_id.as_bytes());
    identity.push(b':');
    identity.extend_from_slice(stream.as_bytes());
    let expected_id = format!("scriptlog-{}", model::digest(&identity));
    artifact.artifact_id == expected_id
        && artifact.relative_path == format!("artifacts/{expected_id}.bin")
        && artifact.metadata
            == json!({"run_id":work.run_id,"operation_id":work.operation_id,"stream":stream})
        && artifact.byte_length
            <= if stream == "stdout" {
                MAX_RESULT_BYTES as u64
            } else {
                MAX_STDERR_BYTES as u64
            }
}

pub fn worker_departed(work: &Work, launch: &Value) -> Result<bool> {
    if launch["run_id"] != work.run_id
        || launch["token"] != work.token
        || launch
            .get("operation_id")
            .is_some_and(|operation_id| operation_id != &json!(work.operation_id))
    {
        return Err(Error::new(
            "SCRIPT_LAUNCH_DAMAGED",
            "launch receipt identifies another script run",
        ));
    }
    if let Some(receipt) = launch.get("early_exit").filter(|value| !value.is_null()) {
        let receipt: EarlyExitReceipt = serde_json::from_value(receipt.clone()).map_err(|_| {
            Error::new(
                "SCRIPT_LAUNCH_DAMAGED",
                "early-exit receipt cannot be parsed",
            )
        })?;
        if receipt.schema_version != 1
            || receipt.state != EarlyExitState::ExitedBeforeWorkerIdentity
            || receipt.run_id != work.run_id
            || receipt.operation_id != work.operation_id
            || receipt.token != work.token
            || receipt.pid == 0
            || launch["process"] != Value::Null
            || launch["operation_id"].as_str() != Some(work.operation_id.as_str())
        {
            return Err(Error::new(
                "SCRIPT_LAUNCH_DAMAGED",
                "early-exit receipt differs from the admitted run",
            ));
        }
        let dir = directory(&work.data_dir, &work.run_id)?;
        if receipt_exists(&dir.join("worker.json"))?
            || receipt_exists(&dir.join("start-decision.json"))?
            || receipt_exists(&dir.join("go.json"))?
            || receipt_exists(&dir.join("started.json"))?
            || receipt_exists(&dir.join("completion.json"))?
            || receipt_exists(&dir.join("terminal.json"))?
            || receipt_exists(&dir.join("cleanup-pending.json"))?
        {
            return Ok(false);
        }
        return Ok(true);
    }
    let process = &launch["process"];
    if process["scope"] == "launcher_spawned_process" {
        if process["purpose"] != "check" {
            return Err(Error::invalid(
                "launcher-spawned script identity has an unsupported purpose",
            ));
        }
        return spawned_departed(process, &work.token);
    }
    if process["purpose"] != "script" {
        return Err(Error::invalid("launch receipt is not a script process"));
    }
    departed_empty(process, &work.token)
}

/// Check departure from the process identity retained by Store at the ready
/// acknowledgement. This path is used only when the mutable Work/receipt files
/// can no longer be decoded; the identity, Operation, and run IDs come from
/// the durable ScriptRun row, not those files.
pub fn worker_family_departed_from_identity(
    data_dir: &Path,
    run_id: &str,
    operation_id: &str,
    worker_identity: &Value,
) -> Result<bool> {
    let _ = directory(data_dir, run_id)?;
    let token = worker_identity["token"]
        .as_str()
        .filter(|token| !token.is_empty() && token.len() <= 128)
        .ok_or_else(|| {
            Error::new(
                "SCRIPT_WORKER_IDENTITY_DAMAGED",
                "retained ScriptRun worker identity has no valid token",
            )
        })?;
    let process = &worker_identity["process"];
    if run_id.is_empty()
        || operation_id.is_empty()
        || worker_identity["run_id"] != run_id
        || worker_identity["operation_id"] != operation_id
        || worker_identity["control_version"] != 1
        || worker_identity["ready_at_ms"]
            .as_i64()
            .is_none_or(|time| time <= 0)
        || process["purpose"] != "script"
        || !matches!(
            process["scope"].as_str(),
            Some("windows_job" | "linux_process_group")
        )
    {
        return Err(Error::new(
            "SCRIPT_WORKER_IDENTITY_DAMAGED",
            "retained ScriptRun worker identity does not match its exact run and Operation",
        ));
    }
    departed_empty(process, token)
}

/// Prove that a pre-Allow worker is gone before Store settles its run. This
/// path requires the exact launch receipt, a retained non-Allow decision or no
/// decision, and no interpreter-start or plan markers. A missing launch receipt
/// is not proof that the launcher failed before creating a process.
pub fn prestart_worker_family_departed(
    data_dir: &Path,
    run_id: &str,
    operation_id: &str,
) -> Result<bool> {
    let dir = directory(data_dir, run_id)?;
    for marker in [
        "started.json",
        "execution-plan.json",
        "execution-plan-ready.json",
        "cleanup-pending.json",
    ] {
        if receipt_exists(&dir.join(marker))? {
            return Ok(false);
        }
    }
    let launch_path = dir.join("launch.json");
    if !receipt_exists(&launch_path)? {
        return Ok(false);
    }
    let launch = read_json(&launch_path)?;
    let token = launch["token"]
        .as_str()
        .filter(|token| !token.is_empty() && token.len() <= 128)
        .ok_or_else(|| Error::new("SCRIPT_LAUNCH_DAMAGED", "launch has no valid token"))?;
    if launch["run_id"] != run_id || launch["operation_id"] != operation_id {
        return Err(Error::new(
            "SCRIPT_LAUNCH_DAMAGED",
            "pre-start launch receipt identifies another run or Operation",
        ));
    }
    if start_decision_allows_start(&swarm_script_worker::read_start_decision(
        &dir,
        run_id,
        operation_id,
        token,
    )?)? == Some(true)
    {
        return Ok(false);
    }
    if let Some(receipt) = launch.get("early_exit").filter(|value| !value.is_null()) {
        let receipt: EarlyExitReceipt = serde_json::from_value(receipt.clone()).map_err(|_| {
            Error::new(
                "SCRIPT_LAUNCH_DAMAGED",
                "early-exit receipt cannot be parsed",
            )
        })?;
        if receipt.schema_version != 1
            || receipt.state != EarlyExitState::ExitedBeforeWorkerIdentity
            || receipt.run_id != run_id
            || receipt.operation_id != operation_id
            || receipt.token != token
            || receipt.pid == 0
            || launch["process"] != Value::Null
        {
            return Err(Error::new(
                "SCRIPT_LAUNCH_DAMAGED",
                "pre-start early-exit receipt differs from its run",
            ));
        }
        if receipt_exists(&dir.join("worker.json"))?
            || receipt_exists(&dir.join("completion.json"))?
            || receipt_exists(&dir.join("terminal.json"))?
        {
            return Ok(false);
        }
        return Ok(true);
    }
    let process = &launch["process"];
    if process["scope"] == "launcher_spawned_process" {
        if process["purpose"] != "check" {
            return Err(Error::new(
                "SCRIPT_LAUNCH_DAMAGED",
                "pre-start launch process has an unsupported purpose",
            ));
        }
        return spawned_departed(process, token);
    }
    if process["purpose"] != "script" {
        return Err(Error::new(
            "SCRIPT_LAUNCH_DAMAGED",
            "pre-start launch process is not an owned script worker",
        ));
    }
    departed_empty(process, token)
}

/// Compatibility entry point for the hidden host CLI. The worker implementation
/// lives only in the standalone worker crate.
pub fn run_worker(receipt_path: &Path) -> Result<()> {
    swarm_script_worker::run_worker(receipt_path).map_err(Error::from)
}

fn ensure_private_directory(root: &Path, dir: &Path) -> Result<()> {
    fs::create_dir_all(root.join("script-runs"))?;
    let runs = root.join("script-runs");
    if fs::symlink_metadata(&runs)?.file_type().is_symlink()
        || !fs::canonicalize(&runs)?.starts_with(root)
    {
        return Err(Error::new(
            "SCRIPT_PATH",
            "script run root must be a private state directory",
        ));
    }
    platform::private_permissions(&runs, true)?;
    match fs::symlink_metadata(dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "SCRIPT_PATH",
                "script run directory is not regular",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(dir)?;
        }
        Err(error) => return Err(error.into()),
    }
    platform::private_permissions(dir, true)?;
    Ok(())
}

fn write_once(path: &Path, bytes: &[u8]) -> Result<()> {
    match swarm_process::write_private_new(path, bytes) {
        Ok(()) => Ok(()),
        Err(error) if error.code == "PRIVATE_FILE_ALREADY_EXISTS" => {
            if read_control_bytes(path)? == bytes {
                Ok(())
            } else {
                Err(Error::conflict("retained script control file differs"))
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&read_control_bytes(path)?)?)
}

fn read_control_bytes(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(Error::invalid("script control file is a reparse point"));
        }
    }
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > CONTROL_LIMIT as u64
    {
        return Err(Error::invalid(
            "script control file must be a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(CONTROL_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > CONTROL_LIMIT {
        return Err(Error::invalid("script control file exceeds 16 MiB"));
    }
    Ok(bytes)
}

fn validate_work(work: &Work) -> Result<()> {
    let bundle_bytes = model::canonical(&json!(work.bundle))?;
    if work.bundle_record.kind != registry::BUNDLE_KIND
        || work.bundle_record.artifact_id.len() != 71
        || !work.bundle_record.artifact_id.starts_with("script-")
        || work.bundle_record.relative_path
            != format!("artifacts/{}.bin", work.bundle_record.artifact_id)
        || work.bundle_record.byte_length != bundle_bytes.len() as u64
        || work.bundle_record.content_digest != model::digest(bundle_bytes.as_bytes())
        || work.bundle_record.metadata["script_id"] != work.bundle.script_id
        || work.bundle_record.metadata["revision"] != work.invocation.script_revision
        || work.bundle_record.metadata["bundle_sha256"] != work.bundle_record.content_digest
        || work.bundle_record.metadata["interpreter_sha256"] != work.interpreter.sha256
        || work.bundle.script_id != work.invocation.script_id
        || work.bundle.interpreter != work.interpreter
        || work.invocation.protocol_version != 1
        || work.invocation.operation_id != work.operation_id
        || work.invocation.run_id != work.run_id
        || work.invocation.controller_effects.len() > manifest::MAX_CONTROLLER_EFFECTS
        || work
            .invocation
            .controller_effects
            .iter()
            .enumerate()
            .any(|(index, effect)| {
                !work.bundle.controller_effects.contains(effect)
                    || work.invocation.controller_effects[..index].contains(effect)
            })
        || environment_sha256(&work.environment)? != work.environment_sha256
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work identity or capability set differs",
        ));
    }
    work.bundle.validate()?;
    if let Some(executor) = &work.executor {
        swarm_script_worker::validate_executor_pin(executor)?;
    }
    if work.bundle_record.byte_length
        > (manifest::MAX_BUNDLE_REQUEST_BYTES + manifest::MAX_SCHEMA_BYTES) as u64
        || !work.bundle_record.artifact_id.starts_with("script-")
        || work.bundle_record.artifact_id.len() != 71
        || work.environment.len() > manifest::MAX_INHERITED_ENVIRONMENT + 8
        || work.environment.iter().any(|(name, value)| {
            value.len() > manifest::MAX_ENVIRONMENT_VALUE_BYTES
                || name.to_ascii_uppercase().starts_with("SWARM_")
                || name.is_empty()
                || name
                    .bytes()
                    .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
        })
        || model::canonical(&json!(work.environment))?.len() > manifest::MAX_ENVIRONMENT_BYTES
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script bundle or environment exceeds its stored bounds",
        ));
    }
    work.bundle
        .input_schema
        .validate_value(&work.invocation.input)?;
    Ok(())
}

fn standalone_worker_environment() -> BTreeMap<String, String> {
    #[cfg(windows)]
    let names = ["SystemRoot", "WINDIR", "ComSpec", "TEMP", "TMP"];
    #[cfg(not(windows))]
    let names: [&str; 0] = [];
    std::env::vars()
        .filter(|(key, _)| names.iter().any(|name| key.eq_ignore_ascii_case(name)))
        .collect()
}
