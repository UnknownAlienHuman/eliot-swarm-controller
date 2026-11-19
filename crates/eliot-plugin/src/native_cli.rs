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
};

use eliot_core::{Error, Result};
use tokio::{io::AsyncReadExt, process::Command, time::Instant};

use crate::native_lifecycle::kill_and_reap;

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
pub async fn run_bounded(
    mut command: Command,
    cap: usize,
    deadline: Instant,
    labels: &BoundedReadLabels,
) -> Result<Vec<u8>> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", labels.start))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", labels.start))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((cap + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match tokio::time::timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new("native_spawn", labels.deadline));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new("native_readback", labels.unreadable));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > cap {
        kill_and_reap(&mut child).await;
        return Err(Error::new("native_readback", labels.unreadable));
    }
    let status = match tokio::time::timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new("native_spawn", labels.deadline));
        }
        Ok(Err(_)) => {
            return Err(Error::new("native_readback", labels.unreadable));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new("native_readback", labels.status));
    }
    Ok(buffer)
}

/// Run one bounded one-shot child and decode its stdout as UTF-8, mapping
/// invalid bytes to the same unreadable readback error as an over-cap read.
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
