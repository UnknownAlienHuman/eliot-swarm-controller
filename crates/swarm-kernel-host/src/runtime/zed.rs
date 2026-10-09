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
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use swarm_contracts::{
    runtime::TaskDispatchContext,
    task_prompt::{TASK_PROMPT_SCHEMA_ID, TASK_PROMPT_SCHEMA_VERSION, TaskPromptEnvelopeV1},
};

/// Current built-in route. The artifact identity is immutable across prompt
/// contract changes so existing `.1` operations retain their legacy reader.
pub const ARTIFACT_ID: &str = "eliot-zed.eval-cli.2";
pub const LEGACY_ARTIFACT_ID: &str = "eliot-zed.eval-cli.1";
pub const RUNTIME: &str = "zed";
pub const CONTRACT_REVISION: &str = "zed-eval-cli-v2";
pub const LEGACY_CONTRACT_REVISION: &str = "zed-eval-cli-v1";
/// Pinned upstream basis of the eval-cli contract (ZD-EXEC).
pub const UPSTREAM_BASIS: &str = "7604aa3f19cef0c4d8be2bb3335c24acd788ccb1";

const NATIVE_OUTPUTS: [&str; 3] = ["result.json", "thread.md", "thread.json"];
const MAX_INSTRUCTION_BYTES: usize = 1024 * 1024;
const MAX_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;
/// The native binary enforces its own `--timeout` and exits 2. The host
/// deadline only guards a hung binary that never reaches that code path.
const HOST_GRACE_SECONDS: u64 = 5;
/// A bounded family-drain period after direct exit, followed by cancellation.
const CLEANUP_GRACE_SECONDS: u64 = 5;
const POLL_INTERVAL: Duration = Duration::from_millis(20);
const WORKER_START_GATE_SECONDS: u64 = 30;
const WORKER_CAPTURE_DRAIN_SECONDS: u64 = 5;
const WORKER_TERMINATION_RETRY: Duration = Duration::from_millis(250);
const WORKER_MAX_TERMINATION_ATTEMPTS: u8 = 3;

pub const BATCH_WORKER_COMMAND: &str = "zed-batch-worker";
pub const BATCH_WORKER_FILE_FLAG: &str = "--file";

/// Route identity used by existing reads and recovery. New binding admission
/// is deliberately narrower and must use [`is_current_route`].
pub fn is_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME
        && matches!(
            route["module_artifact_id"].as_str(),
            Some(ARTIFACT_ID | LEGACY_ARTIFACT_ID)
        )
}

pub fn is_current_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME && route["module_artifact_id"] == ARTIFACT_ID
}

pub fn is_legacy_route(route: &Value) -> bool {
    route["runtime"] == RUNTIME && route["module_artifact_id"] == LEGACY_ARTIFACT_ID
}

#[path = "zed_worker.rs"]
pub mod worker;
pub use worker::run_worker;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Present only for the TaskPrompt v1 route. The exact prompt bytes live
    /// in the private worker plan, whose digest is retained by the process
    /// owner record; this proof binds them to Store's frozen Task context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_prompt_proof: Option<TaskPromptBatchProof>,
}

/// Durable identity for the exact TaskPrompt envelope and Store dispatch
/// context used by a v2 Zed run. Prompt text itself remains in the private
/// worker plan and is bound by its owner-record digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPromptBatchProof {
    pub schema_id: String,
    pub schema_version: u16,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub task_snapshot_sha256: String,
    pub prompt_sha256: String,
    pub prompt_bytes: u64,
    pub task_dispatch_context: TaskDispatchContext,
}

/// Validated v2 request, returned so Store can use the exact envelope bytes
/// and the matching durable proof without composing a second prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPromptDispatch {
    pub envelope: TaskPromptEnvelopeV1,
    pub task_dispatch_context: TaskDispatchContext,
    pub intent: BatchIntent,
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

/// Exact private one-shot plan consumed by the isolated host-binary worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchWorkerPlan {
    version: u8,
    operation_id: String,
    run_id: String,
    intent: BatchIntent,
    executable: PathBuf,
    workdir: PathBuf,
    model: String,
    timeout_seconds: u64,
    env_keys: Vec<String>,
    instruction: String,
    output_dir: PathBuf,
}

/// Durable parent and worker facts for one immutable run. The worker Group
/// identity is recorded before the go gate can authorize the native request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchProcessOwnerRecord {
    pub version: u8,
    pub operation_id: String,
    pub run_id: String,
    pub plan_sha256: String,
    pub launch_state: String,
    pub launch_error: Option<String>,
    pub worker_pid: Option<u32>,
    pub identity_capture: String,
    pub identity_error: Option<String>,
    pub worker_process_identity: Option<Value>,
    pub worker_group_identity: Option<Value>,
    pub worker_exit: String,
    pub worker_exit_code: Option<i32>,
    pub worker_exit_error: Option<String>,
    pub direct_exit: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub direct_exit_error: Option<String>,
    pub family_departure: String,
    pub family_error: Option<String>,
    pub capture: String,
    pub capture_error: Option<String>,
    pub stdout: Option<Value>,
    pub stderr: Option<Value>,
    pub cleanup: String,
    pub host_termination_requested: bool,
    pub family_termination_requested: bool,
    #[serde(default)]
    pub parent_termination_requested: bool,
    pub worker_failure: Option<Value>,
}

impl BatchProcessOwnerRecord {
    fn pending(operation_id: &str, run_id: &str, plan_sha256: String) -> Self {
        Self {
            version: 1,
            operation_id: operation_id.to_owned(),
            run_id: run_id.to_owned(),
            plan_sha256,
            launch_state: "pending".to_owned(),
            launch_error: None,
            worker_pid: None,
            identity_capture: "pending".to_owned(),
            identity_error: None,
            worker_process_identity: None,
            worker_group_identity: None,
            worker_exit: "pending".to_owned(),
            worker_exit_code: None,
            worker_exit_error: None,
            direct_exit: "pending".to_owned(),
            exit_code: None,
            signal: None,
            direct_exit_error: None,
            family_departure: "not_observed".to_owned(),
            family_error: None,
            capture: "not_started".to_owned(),
            capture_error: None,
            stdout: None,
            stderr: None,
            cleanup: "not_started".to_owned(),
            host_termination_requested: false,
            family_termination_requested: false,
            parent_termination_requested: false,
            worker_failure: None,
        }
    }

    fn value(&self) -> Result<Value> {
        serde_json::to_value(self)
            .map_err(|error| Error::new("BATCH_OWNER_INVALID", error.to_string()))
    }
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

/// Exact Store-owned TaskPrompt inputs needed to reconcile a v2 run. The
/// source text and context are retained so recovery checks the original
/// admission identity without reconstructing a legacy Task snapshot prompt.
#[derive(Debug, Clone, Copy)]
pub struct TaskPromptReadContext<'a> {
    pub operation_id: &'a str,
    pub binding_id: &'a str,
    pub generation: i64,
    pub route: &'a Value,
    pub source_text: &'a str,
    pub envelope: &'a TaskPromptEnvelopeV1,
    pub task_dispatch_context: &'a TaskDispatchContext,
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
    if is_current_route(&command.route) {
        let dispatch = validate_task_prompt_dispatch(command)?;
        if instruction != dispatch.envelope.prompt {
            return Err(Error::new(
                "TASK_PROMPT_INVALID",
                "Zed instruction differs from the exact Store TaskPrompt bytes",
            ));
        }
        return Ok(dispatch.intent);
    }
    if !is_legacy_route(&command.route) {
        return Err(Error::new(
            "UNSUPPORTED_RUNTIME",
            "Zed batch intent requires the current or historical artifact identity",
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
        task_prompt_proof: None,
    })
}

/// Validate and retain the exact v2 TaskPrompt envelope and Store context.
/// The returned instruction is the envelope's original UTF-8 string; this
/// path never reconstructs or appends a Task snapshot.
pub fn validate_task_prompt_dispatch(command: &RuntimeCommand) -> Result<TaskPromptDispatch> {
    if command.method != "task.dispatch" || !is_current_route(&command.route) {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "TaskPrompt dispatch requires the current Zed artifact and task.dispatch",
        ));
    }
    if command.input.get("task_snapshot").is_some()
        || command.input.get("task_snapshot_canonical").is_some()
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "TaskPrompt dispatch must not carry legacy Task snapshot fields",
        ));
    }
    let envelope: TaskPromptEnvelopeV1 =
        serde_json::from_value(command.input["task_prompt"].clone()).map_err(|_| {
            Error::new(
                "TASK_PROMPT_INVALID",
                "Store TaskPrompt v1 envelope is missing or malformed",
            )
        })?;
    let context: TaskDispatchContext =
        serde_json::from_value(command.input["task_dispatch_context"].clone()).map_err(|_| {
            Error::new(
                "TASK_DISPATCH_CONTEXT_INVALID",
                "Store task dispatch context is missing or malformed",
            )
        })?;
    let source_text = model::text(&command.input, "text")?;
    validate_task_prompt_parts(
        &command.operation_id,
        &command.binding_id,
        command.generation,
        &command.route,
        source_text,
        &envelope,
        &context,
    )?;
    let proof = task_prompt_batch_proof(&envelope, &context);
    let prompt_bytes = usize::try_from(envelope.prompt_bytes)
        .map_err(|_| Error::invalid("TaskPrompt byte count is out of range"))?;
    let intent = BatchIntent {
        version: 2,
        operation_id: command.operation_id.clone(),
        run_id: run_id(&command.operation_id),
        binding_id: command.binding_id.clone(),
        generation: command.generation,
        route_sha256: model::digest(model::canonical(&command.route)?.as_bytes()),
        prompt_sha256: envelope.prompt_sha256.clone(),
        prompt_bytes,
        task_snapshot_sha256: envelope.task_snapshot_sha256.clone(),
        task_prompt_proof: Some(proof),
    };
    Ok(TaskPromptDispatch {
        envelope,
        task_dispatch_context: context,
        intent,
    })
}

fn validate_task_prompt_parts(
    operation_id: &str,
    binding_id: &str,
    generation: i64,
    route: &Value,
    source_text: &str,
    envelope: &TaskPromptEnvelopeV1,
    context: &TaskDispatchContext,
) -> Result<()> {
    if !is_current_route(route)
        || envelope.validate_shape().is_err()
        || envelope.schema_id != TASK_PROMPT_SCHEMA_ID
        || envelope.schema_version != TASK_PROMPT_SCHEMA_VERSION
        || envelope.prompt_sha256 != model::digest(envelope.prompt.as_bytes())
        || context.validate().is_err()
        || context.operation_id != operation_id
        || context.binding_id != binding_id
        || context.binding_generation != generation
        || context.attempt_id != envelope.attempt_id
        || context.task_id != envelope.task_id
        || context.task_revision != envelope.task_revision
        || context.task_snapshot_sha256 != envelope.task_snapshot_sha256
        || context.source_text_sha256 != model::digest(source_text.as_bytes())
        || context.source_text_bytes != u64::try_from(source_text.len()).unwrap_or(u64::MAX)
    {
        return Err(Error::new(
            "TASK_PROMPT_INVALID",
            "TaskPrompt bytes or Store dispatch context differ from this Zed Operation",
        ));
    }
    Ok(())
}

fn task_prompt_batch_proof(
    envelope: &TaskPromptEnvelopeV1,
    context: &TaskDispatchContext,
) -> TaskPromptBatchProof {
    TaskPromptBatchProof {
        schema_id: envelope.schema_id.clone(),
        schema_version: envelope.schema_version,
        task_id: envelope.task_id.clone(),
        task_revision: envelope.task_revision,
        attempt_id: envelope.attempt_id.clone(),
        task_snapshot_sha256: envelope.task_snapshot_sha256.clone(),
        prompt_sha256: envelope.prompt_sha256.clone(),
        prompt_bytes: envelope.prompt_bytes,
        task_dispatch_context: context.clone(),
    }
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
    let output_root = validate_output_root(output_root)?;
    let output_dir = output_root.join(&run);
    if std::fs::create_dir(&output_dir).is_err() {
        return Err(Error::conflict(
            "batch run identity already has an output directory; refusing to overwrite native evidence",
        ));
    }
    validate_run_directory(&output_root, operation_id)?;
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
        task_prompt_proof: None,
    });
    if intent.operation_id != operation_id || intent.run_id != run {
        return Err(Error::invalid("batch intent does not identify this run"));
    }
    write_json_new(&output_dir.join("intent.json"), &json!(intent))?;
    let stdout_path = output_dir.join("stdout.log");
    let stderr_path = output_dir.join("stderr.log");
    File::create(&stdout_path)?;
    File::create(&stderr_path)?;
    crate::platform::private_permissions(&stdout_path, false)?;
    crate::platform::private_permissions(&stderr_path, false)?;

    let plan = BatchWorkerPlan {
        version: 1,
        operation_id: operation_id.to_owned(),
        run_id: run.clone(),
        intent: intent.clone(),
        executable: resolved,
        workdir: options.workdir.clone(),
        model: options.model.clone(),
        timeout_seconds: options.timeout_seconds,
        env_keys: options.env_keys.clone(),
        instruction: instruction.to_owned(),
        output_dir: output_dir.clone(),
    };
    let plan_value = serde_json::to_value(&plan)
        .map_err(|error| Error::new("BATCH_WORKER_PLAN_INVALID", error.to_string()))?;
    let plan_sha256 = model::digest(model::canonical(&plan_value)?.as_bytes());
    let plan_path = output_dir.join("worker-plan.json");
    write_json_new(&plan_path, &plan_value)?;
    let owner_path = output_dir.join("process-owner.json");
    let mut owner_record =
        BatchProcessOwnerRecord::pending(operation_id, &run, plan_sha256.clone());
    write_json_new(&owner_path, &owner_record.value()?)?;
    execute_batch_worker(&plan_path, &owner_path, &plan, &mut owner_record)?;
    let exit_code = owner_record.exit_code;
    let signal = owner_record.signal;
    let host_terminated = owner_record.host_termination_requested;
    let disposition = if host_terminated {
        BatchDisposition::Timeout
    } else if owner_record.family_termination_requested {
        BatchDisposition::UnexpectedExit
    } else {
        match exit_code {
            Some(0) => BatchDisposition::Completed,
            Some(1) => BatchDisposition::Error,
            Some(2) => BatchDisposition::Timeout,
            Some(3) => BatchDisposition::Interrupted,
            _ => BatchDisposition::UnexpectedExit,
        }
    };
    owner_record.capture = "publishing".to_owned();
    persist_process_owner(&owner_path, &owner_record)?;
    let capture_result = (|| -> Result<(Option<NativeResult>, Vec<ArtifactRecord>)> {
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
        Ok((native, published))
    })();
    let (native, published) = match capture_result {
        Ok(captured) => captured,
        Err(error) => {
            owner_record.capture = "incomplete".to_owned();
            owner_record.capture_error = Some(format!("{}: {}", error.code, error.message));
            let _ = persist_process_owner(&owner_path, &owner_record);
            return Err(error);
        }
    };
    owner_record.capture = "complete".to_owned();
    owner_record.capture_error = None;
    persist_process_owner(&owner_path, &owner_record)?;
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

fn execute_batch_worker(
    plan_path: &Path,
    owner_path: &Path,
    plan: &BatchWorkerPlan,
    owner: &mut BatchProcessOwnerRecord,
) -> Result<()> {
    let executable = std::env::current_exe().map_err(|error| {
        owner.launch_state = "not_started".to_owned();
        owner.launch_error = Some(format!("worker executable lookup failed: {error}"));
        owner.cleanup = "not_needed".to_owned();
        let _ = persist_process_owner(owner_path, owner);
        Error::new(
            "BATCH_WORKER_EXECUTABLE_UNKNOWN",
            "host worker executable is unavailable",
        )
    })?;
    let mut command = Command::new(executable);
    command
        .arg(BATCH_WORKER_COMMAND)
        .arg(BATCH_WORKER_FILE_FLAG)
        .arg(plan_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            owner.launch_state = "not_started".to_owned();
            owner.launch_error = Some(error.to_string());
            owner.cleanup = "not_needed".to_owned();
            persist_process_owner(owner_path, owner)?;
            return Err(Error::new(
                "BATCH_WORKER_LAUNCH_FAILED",
                format!("could not start the isolated Zed worker: {error}"),
            ));
        }
    };

    let worker_pid = child.id();
    owner.launch_state = "spawned_waiting_ready".to_owned();
    owner.worker_pid = Some(worker_pid);
    match swarm_process::spawned_identity(worker_pid) {
        Ok(identity) => {
            owner.worker_process_identity = Some(identity);
            owner.identity_capture = "captured".to_owned();
            owner.identity_error = None;
        }
        Err(error) => {
            owner.identity_capture = "unknown".to_owned();
            owner.identity_error = Some(format!("{}: {}", error.code, error.message));
        }
    }
    persist_process_owner(owner_path, owner)?;

    let ready_path = plan.output_dir.join("worker-ready.json");
    let start_deadline = Instant::now() + Duration::from_secs(WORKER_START_GATE_SECONDS);
    let mut go_written = false;
    let mut ready_identity = None;
    while Instant::now() < start_deadline {
        if ready_path.try_exists()? {
            let ready = read_json_bounded(&ready_path, 8 * 1024 * 1024)?;
            if validate_worker_ready(&ready, plan, worker_pid).is_err() {
                owner.launch_state = "ready_identity_mismatch".to_owned();
                owner.launch_error = Some(
                    "worker ready receipt failed exact run or process identity validation"
                        .to_owned(),
                );
                break;
            }
            let identity = ready["worker_identity"].clone();
            owner.worker_group_identity = Some(identity.clone());
            ready_identity = Some(identity.clone());
            if owner
                .worker_process_identity
                .as_ref()
                .is_some_and(|process| same_process_incarnation(process, &identity))
            {
                owner.launch_state = "ready".to_owned();
                persist_process_owner(owner_path, owner)?;
                let gate = worker_gate(plan, worker_pid, identity, "go");
                write_json_new(&plan.output_dir.join("worker-go.json"), &gate)?;
                go_written = true;
            } else {
                owner.launch_state = "denied_worker_identity_unverified".to_owned();
                owner.launch_error =
                    Some("parent could not verify the helper birth identity".to_owned());
                let gate = worker_gate(plan, worker_pid, identity, "deny");
                write_json_new(&plan.output_dir.join("worker-deny.json"), &gate)?;
            }
            persist_process_owner(owner_path, owner)?;
            break;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                owner.worker_exit = "observed".to_owned();
                owner.worker_exit_code = status.code();
                owner.worker_exit_error =
                    (!status.success()).then(|| format!("worker exited before ready: {status}"));
                owner.launch_state = "worker_exited_before_ready".to_owned();
                break;
            }
            Ok(None) => {}
            Err(error) => {
                owner.worker_exit = "observation_unknown".to_owned();
                owner.worker_exit_error = Some(error.to_string());
                owner.launch_state = "worker_ready_unknown".to_owned();
                break;
            }
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    if !go_written {
        if let Some(identity) = ready_identity {
            let deny_path = plan.output_dir.join("worker-deny.json");
            if !deny_path.try_exists()? {
                write_json_new(&deny_path, &worker_gate(plan, worker_pid, identity, "deny"))?;
            }
        } else if child.try_wait()?.is_none() {
            // No go gate exists, so the helper has not started native work.
            let _ = child.kill();
        }
        if owner.worker_exit != "observed" {
            if let Some(status) = wait_child_until(&mut child, CLEANUP_GRACE_SECONDS) {
                owner.worker_exit = "observed".to_owned();
                owner.worker_exit_code = status.code();
                owner.worker_exit_error = (!status.success())
                    .then(|| format!("worker denied before native start: {status}"));
            } else {
                owner.worker_exit = "observation_unknown".to_owned();
                owner.cleanup = "cleanup_pending".to_owned();
            }
        }
        harvest_worker_files(plan, owner)?;
        persist_process_owner(owner_path, owner)?;
        let _ = read_process_owner(
            plan.output_dir.parent().unwrap_or(&plan.output_dir),
            &plan.operation_id,
        )?;
        return Err(Error::new(
            "BATCH_WORKER_NOT_AUTHORIZED",
            owner.launch_error.clone().unwrap_or_else(|| {
                "one-shot worker did not pass the exact ready/go gate".to_owned()
            }),
        ));
    }

    let worker_deadline = Instant::now()
        + Duration::from_secs(
            plan.timeout_seconds
                .saturating_add(HOST_GRACE_SECONDS)
                .saturating_add(CLEANUP_GRACE_SECONDS.saturating_mul(2))
                .saturating_add(WORKER_CAPTURE_DRAIN_SECONDS)
                .saturating_add(WORKER_START_GATE_SECONDS),
        );
    let mut cancel_written = false;
    let mut cancel_deadline = None;
    loop {
        harvest_worker_files(plan, owner)?;
        match child.try_wait() {
            Ok(Some(status)) => {
                owner.worker_exit = "observed".to_owned();
                owner.worker_exit_code = status.code();
                owner.worker_exit_error =
                    (!status.success()).then(|| format!("worker exited unsuccessfully: {status}"));
                break;
            }
            Ok(None) => {}
            Err(error) => {
                owner.worker_exit = "observation_unknown".to_owned();
                owner.worker_exit_error = Some(error.to_string());
            }
        }
        if !cancel_written && Instant::now() >= worker_deadline {
            cancel_written = true;
            cancel_deadline = Some(Instant::now() + Duration::from_secs(CLEANUP_GRACE_SECONDS));
            owner.parent_termination_requested = true;
            if let Some(identity) = owner.worker_group_identity.clone() {
                let cancel = worker_gate(plan, worker_pid, identity, "cancel");
                write_json_new(&plan.output_dir.join("worker-cancel.json"), &cancel)?;
            }
            owner.cleanup = "parent_cleanup_pending".to_owned();
            persist_process_owner(owner_path, owner)?;
        } else if cancel_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            owner.worker_exit = "observation_unknown".to_owned();
            owner.cleanup = "cleanup_pending".to_owned();
            harvest_worker_files(plan, owner)?;
            persist_process_owner(owner_path, owner)?;
            let _ = read_process_owner(
                plan.output_dir.parent().unwrap_or(&plan.output_dir),
                &plan.operation_id,
            )?;
            return Err(Error::new(
                "BATCH_WORKER_CLEANUP_PENDING",
                "bounded parent observation ended with the exact worker identity retained for same-run recovery",
            ));
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    harvest_worker_files(plan, owner)?;
    persist_process_owner(owner_path, owner)?;
    let recovered = read_process_owner(
        plan.output_dir.parent().unwrap_or(&plan.output_dir),
        &plan.operation_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "BATCH_OWNER_READBACK_MISSING",
            "same-run process owner readback is missing",
        )
    })?;
    *owner = recovered;
    if owner.worker_exit != "observed"
        || owner.worker_exit_code != Some(0)
        || owner.direct_exit != "observed"
        || owner.family_departure != "confirmed"
        || !matches!(owner.cleanup.as_str(), "complete" | "recovered_empty")
        || owner.capture != "complete"
    {
        return Err(Error::new(
            "BATCH_WORKER_RESULT_UNKNOWN",
            owner.worker_failure.as_ref()
                .and_then(|failure| failure["message"].as_str())
                .unwrap_or("worker result, cleanup, or output capture did not reach a verified terminal state"),
        ));
    }
    Ok(())
}

fn worker_gate(plan: &BatchWorkerPlan, worker_pid: u32, identity: Value, decision: &str) -> Value {
    json!({
        "version": 1,
        "decision": decision,
        "operation_id": plan.operation_id,
        "run_id": plan.run_id,
        "plan_sha256": plan_digest(plan),
        "worker_pid": worker_pid,
        "worker_identity": identity
    })
}

fn plan_digest(plan: &BatchWorkerPlan) -> String {
    serde_json::to_value(plan)
        .ok()
        .and_then(|value| model::canonical(&value).ok())
        .map(|bytes| model::digest(bytes.as_bytes()))
        .unwrap_or_default()
}

fn validate_worker_ready(ready: &Value, plan: &BatchWorkerPlan, worker_pid: u32) -> Result<()> {
    let identity = &ready["worker_identity"];
    if ready["version"] != 1
        || ready["operation_id"] != plan.operation_id
        || ready["run_id"] != plan.run_id
        || ready["plan_sha256"] != plan_digest(plan)
        || ready["worker_pid"].as_u64() != Some(u64::from(worker_pid))
        || identity["pid"].as_u64() != Some(u64::from(worker_pid))
        || identity["purpose"] != "check"
    {
        return Err(Error::new(
            "BATCH_WORKER_READY_INVALID",
            "worker ready receipt is not bound to this run",
        ));
    }
    #[cfg(windows)]
    let supported = {
        identity["scope"] == "windows_job"
            && identity["job_name"] == format!("Global\\EliotSwarmCheck-{}", plan.run_id)
            && identity["disposition_source"] == "job_accounting"
    };
    #[cfg(target_os = "linux")]
    let supported = {
        identity["scope"] == "linux_process_group"
            && identity["pgid"].as_i64() == Some(i64::from(worker_pid))
            && identity["disposition_source"] == "proc_group_members"
    };
    #[cfg(not(any(windows, target_os = "linux")))]
    let supported = false;
    if !supported {
        return Err(Error::new(
            "BATCH_WORKER_READY_INVALID",
            "worker Group identity is unsupported",
        ));
    }
    Ok(())
}

fn same_process_incarnation(process: &Value, group: &Value) -> bool {
    if process["pid"] != group["pid"] {
        return false;
    }
    #[cfg(windows)]
    {
        process["creation_filetime"] == group["creation_filetime"]
    }
    #[cfg(target_os = "linux")]
    {
        process["boot_id"] == group["boot_id"] && process["start_ticks"] == group["start_ticks"]
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        false
    }
}

fn wait_child_until(child: &mut Child, seconds: u64) -> Option<ExitStatus> {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn harvest_worker_files(plan: &BatchWorkerPlan, owner: &mut BatchProcessOwnerRecord) -> Result<()> {
    let ready_path = plan.output_dir.join("worker-ready.json");
    if ready_path.try_exists()? {
        let ready = read_json_bounded(&ready_path, 8 * 1024 * 1024)?;
        let ready_pid = ready["worker_pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or_else(|| {
                Error::new("BATCH_WORKER_READY_INVALID", "worker ready PID is invalid")
            })?;
        if owner.worker_pid.is_some_and(|pid| pid != ready_pid) {
            return Err(Error::new(
                "BATCH_WORKER_IDENTITY_CHANGED",
                "worker PID differs from its durable owner",
            ));
        }
        validate_worker_ready(&ready, plan, ready_pid)?;
        if owner.worker_pid.is_none() {
            owner.worker_pid = Some(ready_pid);
            owner.identity_capture = "worker_ready_only".to_owned();
            owner.identity_error = Some(
                "parent spawn identity was not durably recorded; retained helper ready identity only"
                    .to_owned(),
            );
        }
        let identity = ready["worker_identity"].clone();
        if owner
            .worker_group_identity
            .as_ref()
            .is_some_and(|previous| previous != &identity)
        {
            return Err(Error::new(
                "BATCH_WORKER_IDENTITY_CHANGED",
                "worker Group identity changed",
            ));
        }
        owner.worker_group_identity = Some(identity);
    }
    let result_path = plan.output_dir.join("worker-result.json");
    let state_path = plan.output_dir.join("worker-state.json");
    let snapshot_path = if result_path.try_exists()? {
        Some(result_path)
    } else if state_path.try_exists()? {
        Some(state_path)
    } else {
        None
    };
    if let Some(path) = snapshot_path {
        let snapshot = read_json_bounded(&path, 8 * 1024 * 1024)?;
        validate_worker_snapshot(&snapshot, plan, owner)?;
        owner.launch_state = snapshot["launch_state"]
            .as_str()
            .unwrap_or("unknown")
            .to_owned();
        owner.direct_exit = snapshot["direct_exit"]
            .as_str()
            .unwrap_or("observation_unknown")
            .to_owned();
        owner.exit_code = snapshot["exit_code"]
            .as_i64()
            .and_then(|code| i32::try_from(code).ok());
        owner.signal = snapshot["signal"]
            .as_i64()
            .and_then(|signal| i32::try_from(signal).ok());
        owner.direct_exit_error = snapshot["direct_exit_error"].as_str().map(str::to_owned);
        owner.family_departure = snapshot["family_departure"]
            .as_str()
            .unwrap_or("observation_unknown")
            .to_owned();
        owner.family_error = snapshot["family_error"].as_str().map(str::to_owned);
        owner.capture = snapshot["capture"]
            .as_str()
            .unwrap_or("incomplete")
            .to_owned();
        owner.capture_error = snapshot["capture_error"].as_str().map(str::to_owned);
        owner.stdout = snapshot
            .get("stdout")
            .cloned()
            .filter(|value| !value.is_null());
        owner.stderr = snapshot
            .get("stderr")
            .cloned()
            .filter(|value| !value.is_null());
        owner.cleanup = snapshot["cleanup"]
            .as_str()
            .unwrap_or("cleanup_pending")
            .to_owned();
        owner.host_termination_requested = snapshot["host_termination_requested"]
            .as_bool()
            .unwrap_or(false);
        owner.family_termination_requested = snapshot["family_termination_requested"]
            .as_bool()
            .unwrap_or(false);
        owner.parent_termination_requested |= snapshot["parent_termination_requested"]
            .as_bool()
            .unwrap_or(false);
        owner.worker_failure = snapshot
            .get("failure")
            .cloned()
            .filter(|value| !value.is_null());
    }
    Ok(())
}

fn validate_worker_snapshot(
    snapshot: &Value,
    plan: &BatchWorkerPlan,
    owner: &BatchProcessOwnerRecord,
) -> Result<()> {
    if snapshot["version"] != 1
        || snapshot["operation_id"] != plan.operation_id
        || snapshot["run_id"] != plan.run_id
        || snapshot["plan_sha256"] != owner.plan_sha256
        || snapshot["worker_pid"].as_u64() != owner.worker_pid.map(u64::from)
        || owner
            .worker_group_identity
            .as_ref()
            .is_none_or(|identity| snapshot["worker_identity"] != *identity)
    {
        return Err(Error::new(
            "BATCH_WORKER_RESULT_MISMATCH",
            "worker evidence targets another run or owner",
        ));
    }
    Ok(())
}

fn persist_process_owner(path: &Path, owner: &BatchProcessOwnerRecord) -> Result<()> {
    replace_json_durable(path, &owner.value()?)
}

/// Reconcile the exact process owner for one existing run without replaying it.
/// This is the Store's read-only recovery seam for a cleanup-pending batch.
pub fn read_process_owner(
    output_root: &Path,
    operation_id: &str,
) -> Result<Option<BatchProcessOwnerRecord>> {
    validate_output_root(output_root)?;
    let candidate = run_directory(output_root, operation_id);
    let dir = match std::fs::symlink_metadata(&candidate) {
        Ok(_) => validate_run_directory(output_root, operation_id)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let owner_path = dir.join("process-owner.json");
    if !owner_path.try_exists()? {
        return Ok(None);
    }
    let mut owner: BatchProcessOwnerRecord =
        serde_json::from_value(read_json_bounded(&owner_path, 8 * 1024 * 1024)?).map_err(|_| {
            Error::new(
                "BATCH_OWNER_INVALID",
                "saved process owner schema is invalid",
            )
        })?;
    if owner.version != 1
        || owner.operation_id != operation_id
        || owner.run_id != run_id(operation_id)
    {
        return Err(Error::new(
            "BATCH_OWNER_MISMATCH",
            "saved process owner is not bound to this run",
        ));
    }
    let plan_value = read_json_bounded(&dir.join("worker-plan.json"), 8 * 1024 * 1024)?;
    if model::digest(model::canonical(&plan_value)?.as_bytes()) != owner.plan_sha256 {
        return Err(Error::new(
            "BATCH_WORKER_PLAN_MISMATCH",
            "saved worker plan differs from its owner record",
        ));
    }
    let plan: BatchWorkerPlan = serde_json::from_value(plan_value.clone()).map_err(|_| {
        Error::new(
            "BATCH_WORKER_PLAN_INVALID",
            "saved worker plan schema is invalid",
        )
    })?;
    worker::validate_plan(&dir.join("worker-plan.json"), &plan, &plan_value)?;
    harvest_worker_files(&plan, &mut owner)?;
    if let Some(identity) = owner.worker_group_identity.as_ref() {
        match swarm_process::departed_empty(identity, &owner.run_id) {
            Ok(true) => {
                owner.family_departure = "confirmed".to_owned();
                owner.family_error = None;
                if owner.cleanup == "cleanup_pending" || owner.cleanup == "parent_cleanup_pending" {
                    owner.cleanup = "recovered_empty".to_owned();
                }
            }
            Ok(false) => {
                owner.family_departure = "observed_active".to_owned();
                owner.family_error = Some(
                    "same-run readback found a live member of the retained worker Group".to_owned(),
                );
            }
            Err(error) => {
                owner.family_departure = "observation_unknown".to_owned();
                owner.family_error = Some(format!("{}: {}", error.code, error.message));
            }
        }
    }
    persist_process_owner(&owner_path, &owner)?;
    Ok(Some(owner))
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
    if let Some(owner) = read_process_owner(output_root, &receipt.intent.operation_id)? {
        validate_terminal_owner(&owner)?;
    } else {
        return Err(Error::new(
            "BATCH_OWNER_MISSING",
            "terminal receipt cannot be persisted without same-run process owner evidence",
        ));
    }
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
    if !is_legacy_route(context.route) {
        return Err(Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "legacy snapshot receipt reader only accepts the historical Zed artifact",
        ));
    }
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
    let owner = read_process_owner(output_root, context.operation_id)?.ok_or_else(|| {
        Error::new(
            "BATCH_OWNER_MISSING",
            "terminal receipt has no same-run process owner evidence",
        )
    })?;
    validate_terminal_owner(&owner)?;
    let raw = read_json_bounded(&path, 4 * 1024 * 1024)?;
    let receipt: BatchReceipt = serde_json::from_value(raw)
        .map_err(|_| Error::new("BATCH_RECEIPT_INVALID", "saved batch receipt is invalid"))?;
    let facts = prompt_facts(context.instruction, context.task_snapshot)?;
    if receipt.version != 1
        || receipt.intent.version != 1
        || receipt.intent.task_prompt_proof.is_some()
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

/// Read a v2 terminal receipt using the exact retained TaskPrompt and Store
/// context. The worker plan and owner record bind the original prompt bytes;
/// this readback binds the saved proof to the current immutable Operation.
pub fn read_task_prompt_receipt(
    output_root: &Path,
    context: TaskPromptReadContext<'_>,
    artifacts: &ArtifactFiles,
) -> Result<Option<BatchReceipt>> {
    validate_task_prompt_parts(
        context.operation_id,
        context.binding_id,
        context.generation,
        context.route,
        context.source_text,
        context.envelope,
        context.task_dispatch_context,
    )?;
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
    let owner = read_process_owner(output_root, context.operation_id)?.ok_or_else(|| {
        Error::new(
            "BATCH_OWNER_MISSING",
            "terminal receipt has no same-run process owner evidence",
        )
    })?;
    validate_terminal_owner(&owner)?;
    let raw = read_json_bounded(&path, 4 * 1024 * 1024)?;
    let receipt: BatchReceipt = serde_json::from_value(raw)
        .map_err(|_| Error::new("BATCH_RECEIPT_INVALID", "saved batch receipt is invalid"))?;
    let expected_proof = task_prompt_batch_proof(context.envelope, context.task_dispatch_context);
    let expected_intent = BatchIntent {
        version: 2,
        operation_id: context.operation_id.to_owned(),
        run_id: run_id(context.operation_id),
        binding_id: context.binding_id.to_owned(),
        generation: context.generation,
        route_sha256: model::digest(model::canonical(context.route)?.as_bytes()),
        prompt_sha256: context.envelope.prompt_sha256.clone(),
        prompt_bytes: usize::try_from(context.envelope.prompt_bytes)
            .map_err(|_| Error::invalid("TaskPrompt byte count is out of range"))?,
        task_snapshot_sha256: context.envelope.task_snapshot_sha256.clone(),
        task_prompt_proof: Some(expected_proof),
    };
    if receipt.version != 1
        || receipt.intent != expected_intent
        || receipt.outcome.operation_id != context.operation_id
    {
        return Err(Error::new(
            "BATCH_RECEIPT_MISMATCH",
            "saved v2 terminal evidence differs from the retained TaskPrompt and context",
        ));
    }
    validate_receipt_content(&receipt, artifacts, context.route)?;
    Ok(Some(receipt))
}

fn validate_terminal_owner(owner: &BatchProcessOwnerRecord) -> Result<()> {
    let capture_is_complete = |fact: &Option<Value>| {
        fact.as_ref().is_some_and(|fact| {
            fact["complete"].as_bool() == Some(true)
                && fact["truncated"].as_bool() == Some(false)
                && fact["error"].is_null()
                && fact["bytes_total"].as_u64().is_some()
                && fact["bytes_hashed"].as_u64() == fact["bytes_total"].as_u64()
                && fact["sha256"]
                    .as_str()
                    .is_some_and(|digest| !digest.is_empty())
        })
    };
    let exact_worker_identity = owner.worker_pid.is_some_and(|pid| {
        owner
            .worker_process_identity
            .as_ref()
            .is_some_and(|process| {
                process["pid"].as_u64() == Some(u64::from(pid))
                    && owner.worker_group_identity.as_ref().is_some_and(|group| {
                        group["pid"].as_u64() == Some(u64::from(pid))
                            && same_process_incarnation(process, group)
                    })
            })
    });
    if owner.version != 1
        || owner.identity_capture != "captured"
        || !exact_worker_identity
        || owner.worker_exit != "observed"
        || owner.worker_exit_code != Some(0)
        || owner.worker_exit_error.is_some()
        || owner.direct_exit != "observed"
        || (owner.exit_code.is_none() && owner.signal.is_none())
        || owner.direct_exit_error.is_some()
        || owner.family_departure != "confirmed"
        || owner.family_error.is_some()
        || !matches!(owner.cleanup.as_str(), "complete" | "recovered_empty")
        || owner.capture != "complete"
        || owner.capture_error.is_some()
        || !capture_is_complete(&owner.stdout)
        || !capture_is_complete(&owner.stderr)
        || owner.worker_failure.is_some()
    {
        return Err(Error::new(
            "BATCH_OWNER_NOT_TERMINAL",
            "same-run process owner lacks verified worker exit, native exit, family departure, cleanup, or complete capture evidence",
        ));
    }
    Ok(())
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
    let route_proof_matches = if is_current_route(route) {
        receipt.intent.version == 2 && receipt.intent.task_prompt_proof.is_some()
    } else if is_legacy_route(route) {
        receipt.intent.version == 1 && receipt.intent.task_prompt_proof.is_none()
    } else {
        false
    };
    if !route_proof_matches
        || receipt.intent.route_sha256 != model::digest(model::canonical(route)?.as_bytes())
    {
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
    let intent_proof_valid = match (
        receipt.intent.version,
        receipt.intent.task_prompt_proof.as_ref(),
    ) {
        (1, None) => true,
        (2, Some(proof)) => {
            proof.schema_id == TASK_PROMPT_SCHEMA_ID
                && proof.schema_version == TASK_PROMPT_SCHEMA_VERSION
                && proof.task_id == proof.task_dispatch_context.task_id
                && proof.task_revision == proof.task_dispatch_context.task_revision
                && proof.attempt_id == proof.task_dispatch_context.attempt_id
                && proof.task_snapshot_sha256 == proof.task_dispatch_context.task_snapshot_sha256
                && proof.prompt_sha256 == receipt.intent.prompt_sha256
                && proof.task_snapshot_sha256 == receipt.intent.task_snapshot_sha256
                && u64::try_from(receipt.intent.prompt_bytes).ok() == Some(proof.prompt_bytes)
                && proof.task_dispatch_context.validate().is_ok()
                && proof.task_dispatch_context.operation_id == receipt.intent.operation_id
                && proof.task_dispatch_context.binding_id == receipt.intent.binding_id
                && proof.task_dispatch_context.binding_generation == receipt.intent.generation
        }
        _ => false,
    };
    if receipt.version != 1
        || !intent_proof_valid
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
    let bytes = model::canonical(value)?;
    swarm_process::write_private_new(path, bytes.as_bytes())?;
    Ok(())
}

fn replace_json_durable(path: &Path, value: &Value) -> Result<()> {
    let bytes = model::canonical(value)?;
    swarm_process::replace_private_durable(path, bytes.as_bytes())?;
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
