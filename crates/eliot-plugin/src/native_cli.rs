//! Shared mechanics for bounded one-shot native CLI children and the
//! Agent MCP identity preflight. Pure translation helpers: each harness
//! supplies its exact argv, environment, output cap and static message
//! labels; the spawn/read/wait/kill discipline lives here once.
//!
//! donor: the six former one-shot copies in the OpenCode and Antigravity
//! adapters (`native/models.rs`, `native/usage.rs`, `native/auth.rs`),
//! themselves derived from the installed CLIs' read-only surfaces.

use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot};
#[expect(
    clippy::disallowed_types,
    reason = "The bounded native CLI adapter accepts an explicitly owned process command."
)]
use tokio::process::Command;
use tokio::{io::AsyncReadExt, time::Instant};

use crate::{harness_diagnostic, native_lifecycle::kill_and_reap, spawn_stderr};

/// Static message labels for one bounded one-shot read. Every harness keeps
/// its exact pinned strings; only these labels differ between call sites.
pub struct BoundedReadLabels {
    /// `native_spawn`: spawn failed or the piped stdout was missing.
    pub start: &'static str,
    /// `native_spawn`: the read or the exit wait outlived the deadline.
    pub deadline: &'static str,
    /// `native_readback`: unreadable, over-cap or non-UTF-8 output.
    pub unreadable: &'static str,
    /// `native_readback`: the child exited without success.
    pub status: &'static str,
}

/// Run one pre-configured one-shot child to completion and return its raw
/// stdout bytes. The caller builds the command with its exact argv and
/// environment; this helper forces stdin null, piped stdout, discarded
/// stderr and kill-on-drop, then reads at most `cap + 1` bytes under
/// `deadline`, refuses an over-cap read, waits for the exit under the same
/// deadline and requires a successful status. Every failure after spawn
/// kills and reaps the child; `kill_on_drop` stays the backstop.
#[expect(
    clippy::disallowed_types,
    reason = "This helper bounds output and custody for a native CLI child."
)]
pub async fn run_bounded(
    command: Command,
    cap: usize,
    deadline: Instant,
    labels: &BoundedReadLabels,
) -> Result<Vec<u8>> {
    read_bounded_stdout(command, cap, deadline, false)
        .await
        .map_err(|error| read_error(error, labels))
}

/// Run a bounded one-shot child and attach its redacted stderr tail on failure.
#[expect(
    clippy::disallowed_types,
    reason = "This helper bounds output and custody for a native CLI child."
)]
pub async fn run_bounded_with_stderr(
    command: Command,
    cap: usize,
    deadline: Instant,
    labels: &BoundedReadLabels,
    policy: &SecretPolicy,
) -> Result<Vec<u8>> {
    let (result, stderr) = read_bounded_output(command, cap, deadline, false, Some(policy)).await;
    result.map_err(|error| harness_diagnostic(read_error(error, labels), policy, stderr))
}

fn read_error(error: BoundedReadError, labels: &BoundedReadLabels) -> Error {
    match error {
        BoundedReadError::Spawn(_) => Error::new("native_spawn", labels.start),
        BoundedReadError::Deadline => Error::new("native_spawn", labels.deadline),
        BoundedReadError::Unreadable => Error::new("native_readback", labels.unreadable),
        BoundedReadError::Status => Error::new("native_readback", labels.status),
    }
}

#[derive(Debug)]
pub enum BoundedReadError {
    Spawn(std::io::Error),
    Deadline,
    Unreadable,
    Status,
}

/// Read one owned CLI's bounded output. Some read-only native queries encode
/// their authoritative status in stdout even when the process exits nonzero.
#[expect(
    clippy::disallowed_types,
    reason = "This helper reads bounded stdout from a native query child."
)]
pub async fn read_bounded_stdout(
    command: Command,
    cap: usize,
    deadline: Instant,
    accept_non_success: bool,
) -> std::result::Result<Vec<u8>, BoundedReadError> {
    read_bounded_output(command, cap, deadline, accept_non_success, None)
        .await
        .0
}

#[expect(
    clippy::disallowed_types,
    reason = "This helper owns the bounded native query process and its pipes."
)]
async fn read_bounded_output(
    mut command: Command,
    cap: usize,
    deadline: Instant,
    accept_non_success: bool,
    policy: Option<&SecretPolicy>,
) -> (
    std::result::Result<Vec<u8>, BoundedReadError>,
    Option<StderrSnapshot>,
) {
    let stderr = if policy.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return (Err(BoundedReadError::Spawn(error)), None),
    };
    let stderr = policy.and_then(|policy| {
        child
            .stderr
            .take()
            .map(|pipe| spawn_stderr(pipe, Arc::new(policy.clone())))
    });
    let result = read_child_stdout(&mut child, cap, deadline, accept_non_success).await;
    let stderr = match stderr {
        Some(stderr) if result.is_err() => Some(stderr.finish(deadline).await),
        _ => None,
    };
    (result, stderr)
}

async fn read_child_stdout(
    child: &mut tokio::process::Child,
    cap: usize,
    deadline: Instant,
    accept_non_success: bool,
) -> std::result::Result<Vec<u8>, BoundedReadError> {
    let Some(mut stdout) = child.stdout.take() else {
        kill_and_reap(child).await;
        return Err(BoundedReadError::Unreadable);
    };
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((cap + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match tokio::time::timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(child).await;
            return Err(BoundedReadError::Deadline);
        }
        Ok(Err(_)) => {
            kill_and_reap(child).await;
            return Err(BoundedReadError::Unreadable);
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > cap {
        kill_and_reap(child).await;
        return Err(BoundedReadError::Unreadable);
    }
    let status = match tokio::time::timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(child).await;
            return Err(BoundedReadError::Deadline);
        }
        Ok(Err(_)) => {
            kill_and_reap(child).await;
            return Err(BoundedReadError::Unreadable);
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() && !accept_non_success {
        return Err(BoundedReadError::Status);
    }
    Ok(buffer)
}

/// Run one bounded one-shot child and decode its stdout as UTF-8, mapping
/// invalid bytes to the same unreadable readback error as an over-cap read.
#[expect(
    clippy::disallowed_types,
    reason = "This helper decodes the bounded output of a one-shot native CLI child."
)]
pub async fn run_bounded_utf8(
    command: Command,
    cap: usize,
    deadline: Instant,
    labels: &BoundedReadLabels,
) -> Result<String> {
    let bytes = run_bounded(command, cap, deadline, labels).await?;
    String::from_utf8(bytes).map_err(|_| Error::new("native_readback", labels.unreadable))
}

/// Validate one model id line from a native catalogue: non-empty, at most
/// [`MAX_MODEL_ID_BYTES`] bytes, no controls and no whitespace. Pure
/// predicate; the caller owns its harness-specific error string.
pub fn valid_model_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_MODEL_ID_BYTES
        && !id.chars().any(char::is_control)
        && !id.chars().any(char::is_whitespace)
}

/// Maximum model id bytes, mirroring `MAX_HARNESS_MODEL_ID_BYTES` in Core.
pub const MAX_MODEL_ID_BYTES: usize = eliot_core::api::harness::MAX_HARNESS_MODEL_ID_BYTES;

/// One environment variable's value only when the name appears exactly
/// once (case-insensitive); duplicated names are ambiguous and read as
/// absent.
pub fn unique_environment_value<'a>(
    environment: &'a [(OsString, OsString)],
    name: &str,
) -> Option<&'a OsStr> {
    let mut matches = environment
        .iter()
        .filter(|(key, _)| key.to_string_lossy().eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_os_str());
    let value = matches.next()?;
    matches.next().is_none().then_some(value)
}

/// Resolve the Agent-bound Eliot data root from the launch environment for
/// one harness's worker MCP server. The MCP server itself still enforces
/// the identity with `--require-agent`; this preflight never falls back to
/// Owner auth. `identity_message` and `data_root_message` are the
/// harness's exact pinned `not_allowed` strings.
pub fn require_agent_mcp_root(
    environment: &[(OsString, OsString)],
    agent_id: &str,
    generation: i64,
    identity_message: &'static str,
    data_root_message: &'static str,
) -> Result<PathBuf> {
    let expected_generation = generation.to_string();
    let matches_agent = unique_environment_value(environment, "ELIOT_AGENT_ID")
        .and_then(OsStr::to_str)
        == Some(agent_id);
    let matches_generation = unique_environment_value(environment, "ELIOT_AGENT_GENERATION")
        .and_then(OsStr::to_str)
        == Some(expected_generation.as_str());
    let has_token = matches!(
        unique_environment_value(environment, "ELIOT_AGENT_TOKEN"),
        Some(token) if !token.is_empty()
    );
    if agent_id.trim().is_empty()
        || generation <= 0
        || !matches_agent
        || !matches_generation
        || !has_token
    {
        return Err(Error::new("not_allowed", identity_message));
    }
    unique_environment_value(environment, "ELIOT_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute() && path.is_dir())
        .ok_or_else(|| Error::new("not_allowed", data_root_message))
}

/// Name of the Agent-scoped worker Swarm MCP server: the per-session
/// `swarm mcp --require-agent` entry every harness offers its worker.
/// Distinct from the owner-side reserved names
/// (`eliot_core::SWARM_RESERVED_SERVER_NAMES`): this entry carries the
/// Agent identity, never the owner's.
pub const WORKER_SWARM_MCP_SERVER_NAME: &str = "eliot-swarm";

/// Name of the daemon-resolved MCP file in an Antigravity plugin instance.
pub const ANTIGRAVITY_MCP_CONFIG_FILE: &str = "mcp_config.json";

/// The Agent-scoped worker MCP argv tail after the executable:
/// `--data-dir <root> mcp --require-agent`. `--require-agent` fails closed
/// without the inherited Agent identity and never falls back to Owner auth.
pub fn worker_swarm_mcp_args(data_dir: &str) -> [String; 4] {
    [
        "--data-dir".to_owned(),
        data_dir.to_owned(),
        "mcp".to_owned(),
        "--require-agent".to_owned(),
    ]
}
