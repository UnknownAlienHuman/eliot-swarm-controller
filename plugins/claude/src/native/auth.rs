//! Read the selected CLI's authentication state before starting a session.

use eliot_core::{Error, Result};
use eliot_plugin::native_lifecycle::kill_and_reap;
use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, time::timeout_at};

const STATUS_DEADLINE: Duration = Duration::from_secs(20);

/// Output cap for the auth-status read: the installed JSON status is a few
/// hundred bytes, so 64 KiB bounds a misbehaving child with ample headroom.
const MAX_AUTH_STATUS_BYTES: usize = 64 * 1024;

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
/// The shared bounded runner would refuse that exit before the body could
/// be read, so this reader keeps the same bounded discipline inline
/// (capped read, deadline, kill-and-reap) and lets the JSON body decide.
pub(crate) async fn auth_status(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<Value> {
    let mut command = cli(program, cwd, environment);
    command
        .args(["auth", "status", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let deadline = tokio::time::Instant::now() + STATUS_DEADLINE;
    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "native_not_dispatched",
            "Claude CLI auth status could not run",
        )
    })?;
    let mut stdout = child.stdout.take().ok_or_else(|| {
        Error::new(
            "native_not_dispatched",
            "Claude CLI auth status could not run",
        )
    })?;
    let mut bytes = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_AUTH_STATUS_BYTES + 1) as u64);
        limited.read_to_end(&mut bytes).await
    };
    let read_result = timeout_at(deadline, read).await;
    let over_cap = bytes.len() > MAX_AUTH_STATUS_BYTES;
    // The child is done or refused either way: always kill and reap so no
    // auth-status child outlives this read, including on timeout.
    kill_and_reap(&mut child).await;
    if read_result.is_err() {
        return Err(Error::new(
            "native_not_dispatched",
            "Claude CLI auth status timed out",
        ));
    }
    if over_cap || read_result.is_ok_and(|read| read.is_err()) {
        return Err(Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not print its JSON status",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|_| {
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
