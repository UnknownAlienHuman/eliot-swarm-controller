//! Daemon lifecycle and serving commands: daemon, status, mcp.
use super::args::{DaemonAction, DaemonArgs, MaintenanceArgs, MaintenanceCommand, McpArgs};
use super::support::{Failure, mutation_id, print_value, read_and_print, response_exit_code};
use crate::mcp;
use eliot_core::api::{
    DaemonShutdownRequest, DaemonShutdownResponse, DaemonStatusRequest, DaemonStatusResponse,
    MaintenanceRunRequest, MaintenanceRunResponse, ShutdownMode,
};
use serde_json::Value;
use serde_json::json;
#[cfg(not(windows))]
use std::process::{Command as ProcessCommand, Stdio};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::{Instant, sleep};

pub(crate) async fn run_daemon(
    args: DaemonArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let foreground = args.foreground;
    match args.action {
        Some(DaemonAction::Stop(args)) => {
            if args.drain == args.force {
                return Err(Failure::new(
                    2,
                    "daemon stop requires exactly one of --drain or --force",
                ));
            }
            if foreground {
                return Err(Failure::new(
                    2,
                    "--foreground is only valid when starting the daemon",
                ));
            }
            let mode = if args.drain {
                ShutdownMode::Drain
            } else {
                ShutdownMode::Force
            };
            let request = DaemonShutdownRequest::new(mutation_id(id)?, mode);
            let result = shutdown_daemon(&data_dir, &request).await?;
            let value = serde_json::to_value(&result)
                .map_err(|_| Failure::new(4, "daemon.shutdown response is invalid"))?;
            print_value(&value, json_output);
            Ok(response_exit_code(result.state))
        }
        None if foreground => {
            eliot_daemon::run(data_dir)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
            Ok(0)
        }
        None => {
            let exit_code = start_daemon(&data_dir, json_output).await?;
            Ok(exit_code)
        }
    }
}

pub(crate) async fn run_mcp(
    McpArgs {
        http,
        require_agent,
        port,
        public_url,
        managed_child,
    }: McpArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    if require_agent && http {
        return Err(Failure::new(
            2,
            "--require-agent is only valid with MCP stdio",
        ));
    }
    if json_output {
        return Err(Failure::new(
            2,
            "--json is not valid with mcp; stdout carries protocol/ready output only",
        ));
    }
    if !http {
        if port != 0 || managed_child || public_url.is_some() {
            return Err(Failure::new(
                2,
                "--port, --public-url and --managed-child require --http",
            ));
        }
        mcp::run_stdio(data_dir, require_agent)
            .await
            .map_err(Failure::new_from_message)?;
        return Ok(0);
    }
    if managed_child && public_url.is_none() {
        return Err(Failure::new(2, "--managed-child requires --public-url"));
    }
    mcp::run_http(data_dir, port, managed_child, public_url)
        .await
        .map_err(Failure::new_from_message)?;
    Ok(0)
}

pub(crate) async fn run_maintenance(
    MaintenanceArgs { command }: MaintenanceArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    match command {
        MaintenanceCommand::Run => {
            read_and_print!(
                &data_dir,
                json_output,
                "maintenance.run",
                MaintenanceRunRequest::default(),
                MaintenanceRunResponse
            )
        }
    }
}

pub(crate) async fn run_status(data_dir: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let params = DaemonStatusRequest::default();
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: DaemonStatusResponse =
        eliot_daemon::client_call_typed(&data_dir, "daemon.status", &params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    params
        .validate_response(&response)
        .map_err(|error| Failure::from_core(error, false))?;
    let mut value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "daemon.status response is invalid"))?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "data_dir".to_owned(),
            Value::String(data_dir.to_string_lossy().into_owned()),
        );
    }
    print_value(&value, json_output);
    Ok(0)
}

pub(crate) async fn start_daemon(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    let start_ms = daemon_now_ms();
    #[cfg(windows)]
    eliot_daemon::start_background_daemon(data_dir)
        .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    #[cfg(not(windows))]
    {
        let executable = std::env::current_exe()
            .map_err(|error| Failure::new(1, format!("cannot locate Eliot executable: {error}")))?;
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(data_dir)
            .arg("daemon")
            .arg("--foreground")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            // The daemon opens its authoritative private logs/daemon.log only
            // after validating the root and holding its single-owner lock.
            .stderr(Stdio::null());
        command
            .spawn()
            .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    }
    let log_path = data_dir.join("logs").join("daemon.log");
    let data_dir_value = Value::String(data_dir.to_string_lossy().into_owned());
    if json_output {
        print_value(
            &json!({
                "state": "starting",
                "data_dir": data_dir_value,
                "log": log_path.to_string_lossy(),
            }),
            true,
        );
    } else {
        println!(
            "daemon starting; data_dir: {}; diagnostics: {}",
            data_dir.display(),
            log_path.display()
        );
    }
    wait_for_daemon_ready(data_dir, &log_path, start_ms, json_output).await
}

pub(crate) fn daemon_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

pub(crate) const STARTUP_WAIT: Duration = Duration::from_secs(30);
const STARTUP_POLL: Duration = Duration::from_millis(200);
const STARTUP_RPC_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn wait_for_daemon_ready(
    data_dir: &Path,
    log_path: &Path,
    start_ms: u128,
    json_output: bool,
) -> Result<u8, Failure> {
    let deadline = Instant::now() + STARTUP_WAIT;
    let mut last_probe: Option<String>;
    loop {
        if let Some(failure) = eliot_daemon::daemon_failure_since(data_dir, start_ms) {
            return Err(startup_failure(failure, data_dir, log_path));
        }
        let status = tokio::time::timeout(
            STARTUP_RPC_TIMEOUT,
            eliot_daemon::client_call_typed::<DaemonStatusRequest, DaemonStatusResponse>(
                data_dir,
                "daemon.status",
                &DaemonStatusRequest::default(),
            ),
        )
        .await;
        match status {
            Ok(Ok(_)) => {
                let boot_id = read_daemon_boot_id(data_dir);
                if json_output {
                    print_value(
                        &json!({
                            "state": "ready",
                            "data_dir": data_dir.to_string_lossy(),
                            "boot_id": boot_id.unwrap_or_default(),
                            "log": log_path.to_string_lossy(),
                        }),
                        true,
                    );
                } else if let Some(boot_id) = boot_id {
                    println!(
                        "daemon ready; data_dir: {}; boot_id: {}",
                        data_dir.display(),
                        boot_id
                    );
                } else {
                    println!("daemon ready; data_dir: {}", data_dir.display());
                }
                return Ok(0);
            }
            Ok(Err(error)) if probe_rejection_is_final(&error.code) => {
                return Err(probe_rejection(
                    &error.code,
                    &error.message,
                    data_dir,
                    log_path,
                    agent_credentials_inherited(),
                ));
            }
            Ok(Err(error)) => {
                last_probe = Some(probe_summary(&error.code, &error.message));
            }
            Err(_) => {
                last_probe = Some(format!(
                    "probe timed out after {}s",
                    STARTUP_RPC_TIMEOUT.as_secs()
                ));
            }
        }
        if Instant::now() >= deadline {
            break;
        }
        sleep(STARTUP_POLL.min(deadline.saturating_duration_since(Instant::now()))).await;
        if Instant::now() >= deadline {
            break;
        }
    }
    if let Some(failure) = eliot_daemon::daemon_failure_since(data_dir, start_ms) {
        return Err(startup_failure(failure, data_dir, log_path));
    }
    Err(readiness_timeout(data_dir, log_path, last_probe.as_deref()))
}

/// Probe rejections that waiting cannot resolve. `daemon.status` requires an
/// Owner caller, so an agent-credentialed or malformed probe is rejected
/// deterministically: retrying it for the full startup window only floods the
/// daemon log and ends in the same misleading timeout.
fn probe_rejection_is_final(code: &str) -> bool {
    matches!(
        code,
        "not_allowed" | "stale_generation" | "invalid" | "unsupported"
    )
}

/// Whether this process inherited a managed Agent identity. Such calls can
/// never satisfy the Owner-only readiness probe, no matter how long the wait.
fn agent_credentials_inherited() -> bool {
    std::env::var_os("ELIOT_AGENT_ID").is_some()
        || std::env::var_os("ELIOT_AGENT_GENERATION").is_some()
        || std::env::var_os("ELIOT_AGENT_TOKEN").is_some()
}

/// Bounded one-line summary of a readiness probe for timeout diagnostics.
fn probe_summary(code: &str, message: &str) -> String {
    const LIMIT: usize = 200;
    let mut summary = format!("{code}: {}", message.replace('\n', " "));
    if summary.len() > LIMIT {
        let mut end = LIMIT;
        while !summary.is_char_boundary(end) {
            end -= 1;
        }
        summary.truncate(end);
    }
    summary
}

fn probe_rejection(
    code: &str,
    message: &str,
    data_dir: &Path,
    log_path: &Path,
    agent_hint: bool,
) -> Failure {
    let mut text = format!(
        "daemon rejected the readiness probe: {}; data_dir: {}; diagnostics: {}",
        probe_summary(code, message),
        data_dir.display(),
        log_path.display()
    );
    if let Some(phase) = eliot_daemon::last_daemon_phase(data_dir) {
        text.push_str("; daemon log phase: ");
        text.push_str(&phase);
    }
    if agent_hint {
        text.push_str(
            "; daemon.status requires an Owner caller: unset ELIOT_AGENT_ID, ELIOT_AGENT_GENERATION and ELIOT_AGENT_TOKEN",
        );
    }
    let mut failure = Failure::new(1, text);
    failure.code = Some(code.to_owned());
    failure
}

fn readiness_timeout(data_dir: &Path, log_path: &Path, last_probe: Option<&str>) -> Failure {
    let mut text = format!(
        "daemon did not become ready within 30s; data_dir: {}; diagnostics: {}",
        data_dir.display(),
        log_path.display()
    );
    if let Some(phase) = eliot_daemon::last_daemon_phase(data_dir) {
        text.push_str("; daemon log phase: ");
        text.push_str(&phase);
    }
    if let Some(probe) = last_probe {
        text.push_str("; last probe: ");
        text.push_str(probe);
    }
    Failure::new(1, text)
}

pub(crate) fn read_daemon_boot_id(data_dir: &Path) -> Option<String> {
    let bytes = std::fs::read(data_dir.join("daemon.json")).ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("boot_id")
        .and_then(Value::as_str)
        .filter(|boot_id| !boot_id.is_empty() && boot_id.len() <= 256)
        .map(str::to_owned)
}

pub(crate) fn startup_failure(
    failure: eliot_daemon::DaemonStartupFailure,
    data_dir: &Path,
    log_path: &Path,
) -> Failure {
    let mut result = Failure::new(
        1,
        format!(
            "daemon failed during startup: {}; data_dir: {}; diagnostics: {}",
            failure.error_code,
            data_dir.display(),
            log_path.display()
        ),
    );
    result.diagnostics = failure.diagnostics;
    eliot_core::bound_stderr_diagnostics(
        &mut result.diagnostics,
        eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
    );
    result
}

pub(crate) async fn shutdown_daemon(
    data_dir: &Path,
    params: &DaemonShutdownRequest,
) -> Result<DaemonShutdownResponse, Failure> {
    params
        .validate()
        .map_err(|error| Failure::from_core(error, true))?;
    let response: DaemonShutdownResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.shutdown", params)
            .await
            .map_err(|error| Failure::from_core(error, true))?;
    params
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "daemon.shutdown response is invalid"))?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn missing_dir(name: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("eliot readiness probe {name}"));
        (root.join("data"), root.join("logs").join("daemon.log"))
    }

    #[test]
    fn deterministic_probe_rejections_end_the_wait() {
        for code in ["not_allowed", "stale_generation", "invalid", "unsupported"] {
            assert!(probe_rejection_is_final(code), "{code}");
        }
    }

    #[test]
    fn transient_probe_failures_keep_polling() {
        for code in [
            "unavailable",
            "unknown",
            "busy",
            "platform_io",
            "data_dir_unavailable",
            "internal",
            "not_found",
        ] {
            assert!(!probe_rejection_is_final(code), "{code}");
        }
    }

    #[test]
    fn probe_summary_stays_single_line_and_bounded() {
        let summary = probe_summary("stale_generation", "line one\nline two");
        assert!(!summary.contains('\n'), "{summary}");
        let long = "x".repeat(500);
        let summary = probe_summary("unavailable", &long);
        assert!(summary.len() <= 200, "{summary}");
        assert!(summary.starts_with("unavailable: "), "{summary}");
    }

    #[test]
    fn probe_rejection_names_code_and_owner_hint() {
        let (data_dir, log_path) = missing_dir("rejection");
        let failure = probe_rejection(
            "stale_generation",
            "Agent generation has no live plugin owner",
            &data_dir,
            &log_path,
            true,
        );
        assert_eq!(failure.exit_code, 1);
        assert_eq!(failure.code.as_deref(), Some("stale_generation"));
        assert!(
            failure.message.starts_with(
                "daemon rejected the readiness probe: stale_generation: Agent generation has no live plugin owner"
            ),
            "{}",
            failure.message
        );
        assert!(
            failure.message.contains("unset ELIOT_AGENT_ID"),
            "{}",
            failure.message
        );
        let without_hint = probe_rejection(
            "not_allowed",
            "owner credential is invalid",
            &data_dir,
            &log_path,
            false,
        );
        assert!(
            !without_hint.message.contains("unset ELIOT_AGENT_ID"),
            "{}",
            without_hint.message
        );
    }

    #[test]
    fn readiness_timeout_keeps_legacy_prefix_and_reports_last_probe() {
        let (data_dir, log_path) = missing_dir("timeout");
        let bare = readiness_timeout(&data_dir, &log_path, None);
        assert_eq!(bare.exit_code, 1);
        assert!(
            bare.message
                .starts_with("daemon did not become ready within 30s;"),
            "{}",
            bare.message
        );
        assert!(!bare.message.contains("last probe"), "{}", bare.message);
        let with_probe = readiness_timeout(
            &data_dir,
            &log_path,
            Some("unavailable: daemon is not running"),
        );
        assert!(
            with_probe
                .message
                .contains("last probe: unavailable: daemon is not running"),
            "{}",
            with_probe.message
        );
    }
}
