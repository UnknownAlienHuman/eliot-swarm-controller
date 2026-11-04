//! The `claude` CLI keeps its own login: the desktop app's session is not
//! shared with other processes. Before a launch the plugin asks
//! `claude auth status`; when the CLI is logged out it starts the CLI's own
//! browser login and waits a bounded time for the owner to finish it there.
//! The plugin never sees or stores credentials.

use eliot_core::{Error, Result};
use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    path::Path,
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, time::Instant};

const STATUS_DEADLINE: Duration = Duration::from_secs(20);
const LOGIN_DEADLINE: Duration = Duration::from_secs(300);
const LOGIN_POLL: Duration = Duration::from_secs(2);

pub(crate) async fn ensure_logged_in(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<()> {
    if logged_in(program, cwd, environment).await? {
        return Ok(());
    }
    let mut login = cli(program, cwd, environment)
        .args(["auth", "login", "--claudeai"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            Error::new(
                "native_not_dispatched",
                format!("Claude CLI login could not start: {error}"),
            )
        })?;
    let deadline = Instant::now() + LOGIN_DEADLINE;
    while Instant::now() < deadline {
        let exited = matches!(login.try_wait(), Ok(Some(_)));
        if logged_in(program, cwd, environment).await? {
            return Ok(());
        }
        if exited {
            break;
        }
        tokio::time::sleep(LOGIN_POLL).await;
    }
    let _ = login.kill().await;
    Err(Error::new(
        "native_not_dispatched",
        "Claude CLI is not logged in and its browser login was not completed; \
         finish `claude auth login`, then launch again",
    ))
}

/// `claude auth status` prints JSON and exits nonzero when logged out.
async fn logged_in(
    program: &OsStr,
    cwd: &Path,
    environment: &[(OsString, OsString)],
) -> Result<bool> {
    let output = tokio::time::timeout(
        STATUS_DEADLINE,
        cli(program, cwd, environment)
            .args(["auth", "status"])
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
    let status: Value = serde_json::from_slice(&output.stdout).map_err(|_| {
        Error::new(
            "native_not_dispatched",
            "Claude CLI auth status did not print its JSON status",
        )
    })?;
    Ok(logged_in_status(&status))
}

fn logged_in_status(status: &Value) -> bool {
    status.get("loggedIn").and_then(Value::as_bool) == Some(true)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_an_explicit_logged_in_true_counts() {
        assert!(logged_in_status(
            &json!({"loggedIn": true, "authMethod": "claude.ai"})
        ));
        for status in [
            json!({"loggedIn": false, "authMethod": "none"}),
            json!({"loggedIn": "true"}),
            json!({"authMethod": "claude.ai"}),
            json!(null),
        ] {
            assert!(!logged_in_status(&status), "{status}");
        }
    }
}
