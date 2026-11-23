//! Shared CLI/desktop startup using the same private daemon and readiness RPC.

use std::path::Path;
use std::time::Duration;

use eliot_core::api::{DaemonStatusRequest, DaemonStatusResponse};
use eliot_core::{Error, FailureOrigin, Result};

use crate::platform::DataDir;
pub use crate::platform::data_dir::{check_acl_repair_target, repairable_data_dir_acl};

/// Failure location for the daemon's validation of its startup config.
pub const CONFIG_STARTUP_LOCATION: &str = "daemon.start.config";
/// The refusal of a second daemon for a root another daemon already holds.
pub const ALREADY_RUNNING: &str = "daemon_already_running";

/// Codes of a start that may succeed when tried again: nothing answered yet,
/// the platform could not reach or spawn it, or another start holds the root.
/// Configuration, identity and safety refusals are not among them.
pub fn retryable(code: &str) -> bool {
    matches!(
        code,
        "unavailable" | "platform_io" | "data_dir_unavailable" | ALREADY_RUNNING
    )
}

/// Start the supplied Swarm executable only when no daemon is reachable.
/// Returns whether startup observed the daemon restore the root ACL.
pub async fn ensure_running(data_dir: &Path, swarm_executable: &Path) -> Result<bool> {
    // daemon.status is Owner-only. Use the same target-bound identity parser
    // as RPC calls so scoped Agent identities remain Agents and malformed role
    // context fails closed before this preflight.
    if !crate::daemon::client_is_owner(data_dir)? {
        return Ok(false);
    }

    let mut acl_problem = if cfg!(windows) {
        DataDir::open_existing(data_dir)
            .err()
            .filter(repairable_data_dir_acl)
    } else {
        None
    };
    match reachable(data_dir).await {
        Ok(true) => return Ok(acl_problem.is_some()),
        Ok(false) => {}
        Err(error) if repairable_data_dir_acl(&error) => acl_problem = Some(error),
        Err(error) => return Err(error),
    }
    #[cfg(windows)]
    let _startup = if let Some(error) = acl_problem.as_ref() {
        // The operator event is protected by the current OS user and lives
        // outside the root. Leave validation and mutation to the daemon.
        check_acl_repair_target(data_dir, error.clone())?;
        crate::platform::windows::acquire_operator_event(data_dir)?
    } else {
        DataDir::open(data_dir)?.acquire_operator_lock(false)?
    };
    #[cfg(not(windows))]
    let _startup = DataDir::open(data_dir)?.acquire_operator_lock(false)?;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis());
    #[cfg(windows)]
    crate::platform::windows::start_background_daemon_executable(swarm_executable, data_dir)?;
    #[cfg(not(windows))]
    crate::operator::start_detached_daemon(swarm_executable, data_dir)
        .map_err(|_| Error::new("platform_io", "cannot start Swarm daemon"))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if daemon_ready(data_dir).await {
            return Ok(acl_problem.is_some());
        }
        // Another start won this root (its lock or endpoint refused ours):
        // that daemon is the one to wait for, not a failure.
        if let Some(failure) = crate::daemon_failure_since(data_dir, started)
            .filter(|failure| failure.error_code != ALREADY_RUNNING)
        {
            // A concurrent successful start wins over a failure record read
            // at the same boundary.
            if daemon_ready(data_dir).await {
                return Ok(acl_problem.is_some());
            }
            if let Some(message) = failure.config_diagnostic {
                return Err(Error::new(failure.error_code, message)
                    .with_where(CONFIG_STARTUP_LOCATION)
                    .with_origin(
                        FailureOrigin::Swarm,
                        "daemon configuration validation failed",
                    ));
            }
            return Err(Error::new(
                failure.error_code,
                "daemon startup failed; read private daemon diagnostics",
            ));
        }

        if tokio::time::Instant::now() >= deadline {
            if daemon_ready(data_dir).await {
                return Ok(acl_problem.is_some());
            }
            if acl_problem.is_some()
                && let Err(current) = DataDir::open_existing(data_dir)
            {
                return Err(current);
            }
            return Err(Error::new(
                "unavailable",
                "daemon did not become ready within 30 seconds",
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "Owner startup polls daemon readiness within its 30-second deadline."
        )]
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait only when the root is owned by this user and has a repairable ACL
/// deviation. No credential, endpoint, or request is opened during the wait.
pub(crate) async fn open_client_data_dir(data_dir: &Path) -> Result<DataDir> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match DataDir::open_existing(data_dir) {
            Err(mut error) if repairable_data_dir_acl(&error) => {
                if tokio::time::Instant::now() >= deadline {
                    // Windows startup hints use PowerShell single-quoted paths.
                    let command_path = data_dir.to_string_lossy().replace('\'', "''");
                    error.message.push_str(&format!(
                        ". The Owner can run `swarm --data-dir '{command_path}' daemon` to retry startup ACL repair"
                    ));
                    return Err(error);
                }
                #[expect(
                    clippy::disallowed_methods,
                    reason = "Local IPC waits up to two seconds for the daemon to repair a checked root before opening credentials or the endpoint."
                )]
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => return result,
        }
    }
}

/// Whether this data directory's daemon answers the Owner-only
/// `daemon.status`. `Ok(false)` means no daemon is reachable there (nothing
/// listens, or the root has no daemon yet): the case [`ensure_running`]
/// starts one for. Any other failure, such as a refused identity, is
/// returned as it is.
pub async fn reachable(data_dir: &Path) -> Result<bool> {
    match crate::client_call_typed::<_, DaemonStatusResponse>(
        data_dir,
        "daemon.status",
        &DaemonStatusRequest::default(),
    )
    .await
    {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.code.as_str(),
                "unavailable" | "platform_io" | "data_dir_unavailable"
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

async fn daemon_ready(data_dir: &Path) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            crate::client_call_typed::<_, DaemonStatusResponse>(
                data_dir,
                "daemon.status",
                &DaemonStatusRequest::default(),
            ),
        )
        .await,
        Ok(Ok(_))
    )
}
