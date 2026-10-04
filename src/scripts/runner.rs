//! One bounded worker per admitted direct script run. The worker receives no
//! Store connection, manager credential, or controller API capability.
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model,
    platform::{
        self,
        process_group::{Group, departed_empty, spawned_departed, spawned_identity},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use super::{
    manifest::{
        self, InterpreterIdentity, MAX_INVOCATION_BYTES, MAX_RESULT_BYTES, MAX_SCRIPT_DURATION_MS,
        MAX_STDERR_BYTES, ScriptBundle,
    },
    protocol::{ScriptEffectRequest, ScriptInvocation, ScriptResult},
    registry,
};

const CONTROL_LIMIT: usize = 16 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(25);
const START_GATE_TIMEOUT: Duration = Duration::from_secs(120);

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
    let executable = std::env::current_exe()?;
    let mut command = Command::new(executable);
    command
        .arg("script-worker")
        .arg("--file")
        .arg(dir.join("receipt.json"))
        .env_clear()
        .envs(worker_environment())
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

pub fn allow(work: &Work) -> Result<()> {
    if directory(&work.data_dir, &work.run_id)?
        .join("deny.json")
        .try_exists()?
    {
        return Err(Error::conflict(
            "script run was denied before its start gate",
        ));
    }
    write_once(
        &directory(&work.data_dir, &work.run_id)?.join("go.json"),
        &model::canonical(
            &json!({"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token}),
        )?
        .into_bytes(),
    )
}

pub fn deny(work: &Work, error_code: &str) -> Result<()> {
    write_once(
        &directory(&work.data_dir, &work.run_id)?.join("deny.json"),
        &model::canonical(&json!({"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token,"error_code":error_code}))?.into_bytes(),
    )
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
    if dir.join("go.json").try_exists()? {
        return Ok(false);
    }
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
    write_once(
        &dir.join("deny.json"),
        &model::canonical(&json!({"run_id":run_id,"operation_id":operation_id,"token":token,"error_code":error_code}))?.into_bytes(),
    )?;
    Ok(!dir.join("go.json").try_exists()?)
}

pub fn has_start_gate(work: &Work) -> Result<bool> {
    directory(&work.data_dir, &work.run_id)?
        .join("go.json")
        .try_exists()
        .map_err(Into::into)
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
            !work.bundle.controller_effects.contains(&effect.effect) || effect.validate().is_err()
        })
        || completion.result.byte_length > (MAX_RESULT_BYTES + 16 * 1024) as u64
        || completion.stdout.byte_length > MAX_RESULT_BYTES as u64
        || completion.stderr.byte_length > MAX_STDERR_BYTES as u64
    {
        return Err(Error::conflict(
            "script completion identity or artifact types differ",
        ));
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
            || receipt_exists(&dir.join("go.json"))?
            || receipt_exists(&dir.join("started.json"))?
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

pub fn run_worker(receipt_path: &Path) -> Result<()> {
    let work = read_work(receipt_path)?;
    let dir = directory(&work.data_dir, &work.run_id)?;
    let lock_path = dir.join("worker.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    lock.try_lock().map_err(|_| {
        Error::new(
            "SCRIPT_WORKER_EXISTS",
            "this script already has an owning worker",
        )
    })?;
    if dir.join("completion.json").exists() || dir.join("worker.json").exists() {
        return Err(Error::new(
            "SCRIPT_RECOVERY_REQUIRED",
            "a previous script worker started; code will not be repeated",
        ));
    }
    let group = Group::enter_script(&work.token)?;
    let identity = json!({"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token,"ready_at_ms":model::now_ms()? ,"control_version":1,"process":group_identity(&group)?});
    write_once(
        &dir.join("worker.json"),
        &model::canonical(&identity)?.into_bytes(),
    )?;
    if let Err(error) = stage_bundle(&work, &dir) {
        return finish_without_spawn(&work, &group, &identity, error.code);
    }
    let gate = dir.join("go.json");
    let gate_deadline = Instant::now() + START_GATE_TIMEOUT;
    loop {
        if gate.try_exists()? {
            let go = match read_json(&gate) {
                Ok(go) => go,
                Err(_) => {
                    return finish_without_spawn(
                        &work,
                        &group,
                        &identity,
                        "SCRIPT_START_GATE_INVALID".into(),
                    );
                }
            };
            if go["run_id"] != work.run_id
                || go["operation_id"] != work.operation_id
                || go["token"] != work.token
            {
                return finish_without_spawn(
                    &work,
                    &group,
                    &identity,
                    "SCRIPT_START_GATE_INVALID".into(),
                );
            }
            break;
        }
        let denied = dir.join("deny.json");
        if denied.try_exists()? {
            let denial = match read_json(&denied) {
                Ok(denial) => denial,
                Err(_) => {
                    return finish_without_spawn(
                        &work,
                        &group,
                        &identity,
                        "SCRIPT_START_DENIAL_INVALID".into(),
                    );
                }
            };
            if denial["run_id"] != work.run_id
                || denial["operation_id"] != work.operation_id
                || denial["token"] != work.token
            {
                return finish_without_spawn(
                    &work,
                    &group,
                    &identity,
                    "SCRIPT_START_DENIAL_INVALID".into(),
                );
            }
            let code = denial["error_code"]
                .as_str()
                .unwrap_or("SCRIPT_START_DENIED");
            return finish_without_spawn(&work, &group, &identity, code.to_owned());
        }
        if Instant::now() >= gate_deadline {
            return finish_without_spawn(
                &work,
                &group,
                &identity,
                "SCRIPT_START_GATE_TIMEOUT".into(),
            );
        }
        thread::sleep(POLL_INTERVAL);
    }
    let current = match manifest::capture_interpreter(
        &work.interpreter.canonical_path,
        work.interpreter.kind,
    ) {
        Ok(current) => current,
        Err(error) => return finish_without_spawn(&work, &group, &identity, error.code),
    };
    if current.sha256 != work.interpreter.sha256
        || current.canonical_path != work.interpreter.canonical_path
    {
        return finish_without_spawn(
            &work,
            &group,
            &identity,
            "SCRIPT_INTERPRETER_CHANGED".into(),
        );
    }
    if !environment_sha256(&work.environment).is_ok_and(|digest| digest == work.environment_sha256)
    {
        return finish_without_spawn(
            &work,
            &group,
            &identity,
            "SCRIPT_ENVIRONMENT_CHANGED".into(),
        );
    }
    match execute(&work, &dir, &group, &identity) {
        Ok(()) => Ok(()),
        Err(error) => {
            let started = dir.join("started.json").try_exists()?;
            let state = if started || error.code == "SCRIPT_CHILD_STARTED" {
                "incomplete"
            } else {
                "failed"
            };
            finish_worker_error(&work, &group, &identity, error.code, state)
        }
    }
}

fn execute(work: &Work, dir: &Path, group: &Group, identity: &Value) -> Result<()> {
    let entrypoint = safe_stage_path(&dir.join("bundle"), &work.bundle.entrypoint)?;
    let args = command_arguments(&work.bundle, &entrypoint);
    let input = model::canonical(&json!(work.invocation))?.into_bytes();
    if input.len() > MAX_INVOCATION_BYTES {
        return Err(Error::invalid("script invocation exceeds 260 KiB"));
    }
    let mut command = Command::new(&work.interpreter.canonical_path);
    command
        .args(args)
        .current_dir(dir.join("bundle"))
        .env_clear()
        .envs(&work.environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let started_at_ms = model::now_ms()?;
    let mut child = command.spawn()?;
    if let Err(error) = write_once(
        &dir.join("started.json"),
        &model::canonical(&json!({"pid":child.id(),"started_at_ms":started_at_ms,"run_id":work.run_id,"operation_id":work.operation_id,"token":work.token}))?.into_bytes(),
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

    let deadline = Instant::now() + Duration::from_millis(MAX_SCRIPT_DURATION_MS);
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
    let mut error_code = if timed_out {
        Some("SCRIPT_TIMEOUT".to_owned())
    } else if stdout.overflow || stderr.overflow {
        Some("SCRIPT_OUTPUT_LIMIT".to_owned())
    } else if writer_result.is_err() {
        Some("SCRIPT_INPUT_FAILED".to_owned())
    } else if exit_code != Some(0) {
        Some("SCRIPT_EXIT_NONZERO".to_owned())
    } else {
        None
    };
    let parsed = if error_code.is_none() {
        match parse_script_result(&stdout.bytes, work) {
            Ok(value) => Some(value),
            Err(error) => {
                error_code = Some(error.code);
                None
            }
        }
    } else {
        None
    };
    let result_value = parsed.as_ref().map(|parsed| parsed.result.clone());
    let controller_effects = parsed.map(|parsed| parsed.effects).unwrap_or_default();
    let state = if error_code.is_none() {
        "completed"
    } else {
        "failed"
    };
    publish_completion(
        work,
        CompletionPublication {
            stdout: &stdout.bytes,
            stderr: &stderr.bytes,
            result_value,
            controller_effects: &controller_effects,
            error_code,
            state,
            started_at_ms: Some(started_at_ms),
            exit_code,
            process: &identity["process"],
        },
    )
}

struct ParsedScriptResult {
    result: Value,
    effects: Vec<ScriptEffectRequest>,
}

fn parse_script_result(bytes: &[u8], work: &Work) -> Result<ParsedScriptResult> {
    if bytes.is_empty() || bytes.len() > MAX_RESULT_BYTES {
        return Err(Error::new(
            "SCRIPT_RESULT_INVALID",
            "script stdout must contain one bounded JSON result",
        ));
    }
    let result: ScriptResult = serde_json::from_slice(bytes).map_err(|_| {
        Error::new(
            "SCRIPT_RESULT_INVALID",
            "script stdout is not a valid ScriptResult",
        )
    })?;
    if result.protocol_version != 1
        || result.operation_id != work.operation_id
        || result.run_id != work.run_id
    {
        return Err(Error::new(
            "SCRIPT_RESULT_INVALID",
            "script result identity differs from the admitted run",
        ));
    }
    work.bundle.result_schema.validate_value(&result.result)?;
    if result.effects.len() > manifest::MAX_CONTROLLER_EFFECTS {
        return Err(Error::new(
            "SCRIPT_EFFECTS_INVALID",
            "script requested more controller effects than the invocation permits",
        ));
    }
    for effect in &result.effects {
        if !work.invocation.controller_effects.contains(&effect.effect) {
            return Err(Error::new(
                "SCRIPT_EFFECTS_UNGRANTED",
                "script requested a controller effect outside this invocation grant",
            ));
        }
        effect.validate().map_err(|_| {
            Error::new(
                "SCRIPT_EFFECTS_INVALID",
                "script controller effect payload is invalid",
            )
        })?;
    }
    Ok(ParsedScriptResult {
        result: result.result,
        effects: result.effects,
    })
}

fn publish_completion(work: &Work, publication: CompletionPublication<'_>) -> Result<()> {
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
    let stdout = output_record(work, "stdout", stdout_bytes)?;
    let stderr = output_record(work, "stderr", stderr_bytes)?;
    files.publish(&stdout.0, &stdout.1)?;
    files.publish(&stderr.0, &stderr.1)?;
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
        "stdout_ref":stdout.0.artifact_id,
        "stderr_ref":stderr.0.artifact_id,
        "controller_effects":controller_effects,
    });
    let result_id = format!(
        "scriptresult-{}",
        model::digest(work.operation_id.as_bytes())
    );
    let (result, result_bytes) = ArtifactFiles::document(
        "script_result",
        &result_id,
        &result_document,
        json!({"run_id":work.run_id,"operation_id":work.operation_id,"script_id":work.bundle.script_id,"state":state}),
    )?;
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
        stdout: stdout.0,
        stderr: stderr.0,
        result_value,
        controller_effects: controller_effects.to_vec(),
        error_code,
    };
    let dir = directory(&work.data_dir, &work.run_id)?;
    write_once(
        &dir.join("terminal.json"),
        &model::canonical(&json!(completion))?.into_bytes(),
    )?;
    write_once(
        &dir.join("completion.json"),
        &model::canonical(&json!(completion))?.into_bytes(),
    )?;
    Ok(())
}

fn output_record(work: &Work, stream: &str, bytes: &[u8]) -> Result<(ArtifactRecord, Vec<u8>)> {
    let mut identity = Vec::with_capacity(work.run_id.len() + stream.len() + 1);
    identity.extend_from_slice(work.run_id.as_bytes());
    identity.push(b':');
    identity.extend_from_slice(stream.as_bytes());
    let id = format!("scriptlog-{}", model::digest(&identity));
    let record = ArtifactRecord {
        kind: "script_output".into(),
        artifact_id: id.clone(),
        relative_path: format!("artifacts/{id}.bin"),
        byte_length: bytes.len() as u64,
        content_digest: model::digest(bytes),
        metadata: json!({"run_id":work.run_id,"operation_id":work.operation_id,"stream":stream}),
    };
    Ok((record, bytes.to_vec()))
}

fn finish_without_spawn(
    work: &Work,
    group: &Group,
    identity: &Value,
    error_code: String,
) -> Result<()> {
    finish_worker_error(work, group, identity, error_code, "failed")
}

fn finish_worker_error(
    work: &Work,
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
    let started_at_ms = directory(&work.data_dir, &work.run_id)
        .ok()
        .and_then(|dir| read_json(&dir.join("started.json")).ok())
        .and_then(|value| value["started_at_ms"].as_i64());
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

fn stage_bundle(work: &Work, run_dir: &Path) -> Result<()> {
    let bundle_root = run_dir.join("bundle");
    if bundle_root.exists() {
        if fs::symlink_metadata(&bundle_root)?.file_type().is_symlink() {
            return Err(Error::new(
                "SCRIPT_STAGE_INVALID",
                "script stage root is a symlink",
            ));
        }
        return Err(Error::new(
            "SCRIPT_STAGE_EXISTS",
            "script stage already exists without a prior worker receipt",
        ));
    }
    fs::create_dir_all(&bundle_root)?;
    platform::private_permissions(&bundle_root, true)?;
    let mut total = 0usize;
    for file in &work.bundle.files {
        manifest::validate_bundle_path(&file.path)?;
        let bytes = manifest::decode_file(&file.content_base64)?;
        if bytes.len() as u64 != file.byte_length || model::digest(&bytes) != file.sha256 {
            return Err(Error::new(
                "SCRIPT_BUNDLE_DAMAGED",
                "staged script file differs from immutable digest",
            ));
        }
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| Error::invalid("script bundle size overflow"))?;
        if total > manifest::MAX_BUNDLE_BYTES {
            return Err(Error::invalid("script bundle exceeds 512 KiB"));
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
        platform::private_permissions(&target, false)?;
    }
    Ok(())
}

fn safe_stage_path(root: &Path, relative: &str) -> Result<PathBuf> {
    manifest::validate_bundle_path(relative)?;
    let root = fs::canonicalize(root)?;
    let mut target = root.clone();
    for component in relative.split('/') {
        target.push(component);
    }
    if !target.starts_with(&root) {
        return Err(Error::new(
            "SCRIPT_STAGE_INVALID",
            "script file escaped the stage",
        ));
    }
    Ok(target)
}

fn ensure_private_tree(root: &Path, destination: &Path) -> Result<()> {
    let root = fs::canonicalize(root)?;
    let relative = destination
        .strip_prefix(&root)
        .map_err(|_| Error::new("SCRIPT_STAGE_INVALID", "script directory escaped its stage"))?;
    let mut current = root;
    for component in relative.components() {
        let std::path::Component::Normal(part) = component else {
            return Err(Error::new(
                "SCRIPT_STAGE_INVALID",
                "script stage path is not normalized",
            ));
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(Error::new(
                    "SCRIPT_STAGE_INVALID",
                    "script stage contains a link or non-directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                platform::private_permissions(&current, true)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn command_arguments(bundle: &ScriptBundle, entrypoint: &Path) -> Vec<String> {
    let mut args = match bundle.interpreter.kind {
        manifest::InterpreterKind::Python => {
            vec!["-I".to_owned(), entrypoint.to_string_lossy().into_owned()]
        }
        manifest::InterpreterKind::Powershell => vec![
            "-NoLogo".into(),
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-File".into(),
            entrypoint.to_string_lossy().into_owned(),
        ],
    };
    args.extend(bundle.argv.iter().cloned());
    args
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
    if path.exists() {
        if fs::read(path)? == bytes {
            return Ok(());
        }
        return Err(Error::conflict("retained script control file differs"));
    }
    let temp = path.with_file_name(format!(".{}.tmp", model::new_id()));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match fs::hard_link(&temp, path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if fs::read(path)? == bytes {
                    Ok(())
                } else {
                    Err(Error::conflict("retained script control file differs"))
                }
            }
            Err(error) => Err(error.into()),
        }
    })();
    let _ = fs::remove_file(&temp);
    if result.is_ok() {
        platform::private_permissions(path, false)?;
    }
    result
}

fn read_json(path: &Path) -> Result<Value> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > CONTROL_LIMIT as u64
    {
        return Err(Error::invalid(
            "script control file must be a bounded regular file",
        ));
    }
    let bytes = fs::read(path)?;
    if bytes.len() > CONTROL_LIMIT {
        return Err(Error::invalid("script control file exceeds 16 MiB"));
    }
    Ok(serde_json::from_slice(&bytes)?)
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
        || work.invocation.controller_effects != work.bundle.controller_effects
        || environment_sha256(&work.environment)? != work.environment_sha256
    {
        return Err(Error::new(
            "SCRIPT_WORK_DAMAGED",
            "script work identity or capability set differs",
        ));
    }
    work.bundle.validate()?;
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

fn worker_environment() -> BTreeMap<String, String> {
    let mut names = vec!["PATH"];
    #[cfg(windows)]
    names.extend(["SystemRoot", "WINDIR", "TEMP", "TMP"]);
    #[cfg(unix)]
    names.extend(["HOME", "TMPDIR", "LANG", "LC_ALL", "TMP", "TEMP"]);
    std::env::vars()
        .filter(|(key, _)| names.iter().any(|name| key.eq_ignore_ascii_case(name)))
        .collect()
}

fn group_identity(group: &Group) -> Result<Value> {
    Ok(group.identity.clone())
}
