//! Read the selected CLI's authentication state before starting a session.

use eliot_core::{Error, Result};
use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

const STATUS_DEADLINE: Duration = Duration::from_secs(20);

pub(crate) async fn require_auth(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<()> {
    check_auth_status(program, &auth_status(program, cwd, environment).await?)
}

fn check_auth_status(program: &OsStr, status: &Value) -> Result<()> {
    match status.get("loggedIn").and_then(Value::as_bool) {
        Some(true) => Ok(()),
        Some(false) => {
            let program = program.to_string_lossy();
            let command = if cfg!(windows) {
                format!("& '{}' auth login", program.replace('\'', "''"))
            } else {
                format!("'{}' auth login", program.replace('\'', "'\\''"))
            };
            Err(Error::new(
                "auth_required",
                format!("Claude CLI is signed out. Run {command} in a terminal, then start again."),
            ))
        }
        None => Err(Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not report a boolean loggedIn field",
        )),
    }
}

/// The native CLI emits JSON and exits with status 1 when signed out.
pub(crate) async fn auth_status(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<Value> {
    let output = tokio::time::timeout(
        STATUS_DEADLINE,
        cli(program, cwd, environment)
            .args(["auth", "status", "--json"])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| Error::new("native_not_dispatched", "Claude CLI auth status timed out"))?
    .map_err(|error| {
        Error::new(
            "native_not_dispatched",
            format!("Claude CLI auth status could not run: {error}"),
        )
    })?;
    serde_json::from_slice(&output.stdout).map_err(|_| {
        Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not print its JSON status",
        )
    })
}

fn cli(program: &OsStr, cwd: &Path, environment: &[(OsString, OsString)]) -> Command {
    let mut command = Command::new(program);
    command
        .current_dir(cwd)
        .envs(environment.iter().map(|(name, value)| (name, value)));
    #[cfg(windows)]
    command.creation_flags(0x0800_0000);
    command
}
