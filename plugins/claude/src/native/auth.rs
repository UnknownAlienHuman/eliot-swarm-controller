//! Read the selected CLI's authentication state before starting a session.

use eliot_core::{Error, Result};
use eliot_plugin::native_cli::{BoundedReadError, read_bounded_stdout};
use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    path::Path,
    time::Duration,
};
#[expect(
    clippy::disallowed_types,
    reason = "Claude auth status uses an explicitly owned native CLI command."
)]
use tokio::process::Command;
use tokio::time::Instant;

const STATUS_DEADLINE: Duration = Duration::from_secs(20);

/// Output cap for the auth-status read: the installed JSON status is a few
/// hundred bytes, so 64 KiB bounds a misbehaving child with ample headroom.
const MAX_AUTH_STATUS_BYTES: usize = 64 * 1024;

pub(crate) async fn require_auth(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<()> {
    check_auth_status(&auth_status(program, cwd, environment).await?)
}

fn check_auth_status(status: &Value) -> Result<()> {
    match status.get("loggedIn").and_then(Value::as_bool) {
        Some(true) => Ok(()),
        Some(false) => Err(Error::new("auth_required", "Claude CLI is signed out.")),
        None => Err(Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not report a boolean loggedIn field",
        )),
    }
}

/// The native CLI emits JSON and exits with status 1 when signed out.
/// The shared bounded runner would refuse that exit before the body could
/// be read, so this reader keeps the same bounded discipline inline
/// (capped read, deadline, kill-and-reap) and lets the JSON body decide.
pub(crate) async fn auth_status(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<Value> {
    let mut command = cli(program, cwd, environment);
    command.args(["auth", "status", "--json"]);
    let bytes = read_bounded_stdout(
        command,
        MAX_AUTH_STATUS_BYTES,
        Instant::now() + STATUS_DEADLINE,
        true,
    )
    .await
    .map_err(|error| match error {
        BoundedReadError::Spawn(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // The profile's `cmd` names no executable: say so, rather than
            // blaming authentication for a file that does not exist.
            Error::new(
                "native_spawn_refused",
                format!(
                    "Claude CLI was not found at {}; set cmd in the profile to an installed Claude Code executable",
                    program.to_string_lossy()
                ),
            )
        }
        BoundedReadError::Spawn(_) => Error::new(
            "native_not_dispatched",
            "Claude CLI auth status could not run",
        ),
        BoundedReadError::Deadline => Error::new(
            "native_not_dispatched",
            "Claude CLI auth status timed out",
        ),
        BoundedReadError::Unreadable | BoundedReadError::Status => Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not print its JSON status",
        ),
    })?;
    serde_json::from_slice(&bytes).map_err(|_| {
        Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not print its JSON status",
        )
    })
}

#[expect(
    clippy::disallowed_types,
    reason = "This helper constructs the Claude auth-status child command."
)]
fn cli(program: &OsStr, cwd: &Path, environment: &[(OsString, OsString)]) -> Command {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs a process owned by this module."
    )]
    let mut command = Command::new(program);
    command
        .current_dir(cwd)
        .envs(environment.iter().map(|(name, value)| (name, value)));
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    command
}
