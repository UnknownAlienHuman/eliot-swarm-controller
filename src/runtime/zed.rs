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
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs::File,
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

#[derive(Debug)]
pub struct BatchOutcome {
    pub run_id: String,
    pub disposition: BatchDisposition,
    pub exit_code: Option<i32>,
    /// True only when this controller terminated the child at its own host
    /// deadline because the native timeout never fired.
    pub host_terminated: bool,
    /// `native_batch_result_recorded` when a validated `result.json` backs the
    /// disposition; otherwise the exit code is the only native evidence.
    pub completion_condition: &'static str,
    /// Compact projection of the native `result.json`; never the transcript.
    pub native_result: Option<Value>,
    pub artifacts: Vec<ArtifactRecord>,
    pub output_dir: PathBuf,
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
            "error": self.error,
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
    let output_dir = output_root.join(&run);
    if std::fs::create_dir(&output_dir).is_err() {
        return Err(Error::conflict(
            "batch run identity already has an output directory; refusing to overwrite native evidence",
        ));
    }
    let stdout = File::create(output_dir.join("stdout.log"))?;
    let stderr = File::create(output_dir.join("stderr.log"))?;
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
    for name in NATIVE_OUTPUTS {
        let path = output_dir.join(name);
        if path.is_file() {
            publish_output(
                artifacts,
                operation_id,
                &run,
                name,
                &path,
                disposition,
                &mut published,
            )?;
        }
    }
    Ok(BatchOutcome {
        run_id: run,
        disposition,
        exit_code,
        host_terminated,
        completion_condition: if native.is_some() {
            "native_batch_result_recorded"
        } else {
            "native_batch_exit_classified"
        },
        native_result: native.map(|r| r.projection()),
        artifacts: published,
        output_dir,
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
) -> Result<Option<EvalResult>> {
    let path = output_dir.join("result.json");
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = read_bounded(&path)?;
    let result: EvalResult = serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "NATIVE_RESULT_SCHEMA",
            "result.json does not match the pinned eval-cli result shape",
        )
    })?;
    result.validate(options, exit_code)?;
    Ok(Some(result))
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    if std::fs::metadata(path)?.len() > MAX_OUTPUT_BYTES {
        return Err(Error::new(
            "NATIVE_OUTPUT_TOO_LARGE",
            "native output exceeds the publication bound; refusing to truncate evidence",
        ));
    }
    Ok(std::fs::read(path)?)
}

fn publish_output(
    artifacts: &ArtifactFiles,
    operation_id: &str,
    run: &str,
    name: &str,
    path: &Path,
    disposition: BatchDisposition,
    published: &mut Vec<ArtifactRecord>,
) -> Result<()> {
    let bytes = read_bounded(path)?;
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![&[]]
    } else {
        bytes.chunks(MAX_PAGE_BYTES).collect()
    };
    let pages = chunks.len();
    for (page, chunk) in chunks.into_iter().enumerate() {
        let record = ArtifactFiles::record(
            &format!("{operation_id}:zed:{name}:{page}"),
            chunk,
            json!({
                "runtime": RUNTIME,
                "contract_revision": CONTRACT_REVISION,
                "run_id": run,
                "native_output": name,
                "page": page,
                "pages": pages,
                "disposition": disposition.as_str()
            }),
        );
        artifacts.publish(&record, chunk)?;
        published.push(record);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
