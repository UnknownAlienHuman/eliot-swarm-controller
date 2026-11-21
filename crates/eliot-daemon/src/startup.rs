//! Shared CLI/desktop startup using the same private daemon and readiness RPC.
use std::path::Path;
use std::time::Duration;

use eliot_core::api::{DaemonStatusRequest, DaemonStatusResponse};
use eliot_core::{Error, FailureOrigin, Result};

/// Failure location for the daemon's validation of its startup config.
pub const CONFIG_STARTUP_LOCATION: &str = "daemon.start.config";

/// Start the supplied Swarm executable only when no daemon is reachable.
/// An authentication or safety failure is returned without changing the root.
pub async fn ensure_running(data_dir: &Path, swarm_executable: &Path) -> Result<()> {
    // daemon.status is Owner-only. Use the same target-bound identity parser
    // as RPC calls so scoped Agent identities remain Agents and malformed role
    // context fails closed before this preflight.
    if !crate::daemon::client_is_owner(data_dir)? {
        return Ok(());
    }

    if reachable(data_dir).await? {
        return Ok(());
    }
    crate::platform::DataDir::open(data_dir)?;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| time.as_millis());
    #[cfg(windows)]
    crate::platform::windows::start_background_daemon_executable(swarm_executable, data_dir)?;
    #[cfg(not(windows))]
    {
        std::process::Command::new(swarm_executable)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("daemon")
            .arg("--foreground")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|_| Error::new("platform_io", "cannot start Swarm daemon"))?;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if daemon_ready(data_dir).await {
            return Ok(());
        }
        if let Some(failure) = crate::daemon_failure_since(data_dir, started) {
            // A concurrent successful start wins over a failure record read
            // at the same boundary.
            if daemon_ready(data_dir).await {
                return Ok(());
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
                return Ok(());
            }
            return Err(Error::new(
                "unavailable",
                "daemon did not become ready within 30 seconds",
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
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
