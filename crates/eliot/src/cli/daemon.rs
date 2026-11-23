//! Daemon lifecycle and serving commands: daemon, status, mcp.

use super::args::{
    DaemonAction, DaemonArgs, MaintenanceArgs, MaintenanceCommand, McpArgs,
    PurgeLegacyAttentionAdvisoriesArgs,
};
use super::support::{Failure, mutation_id, print_value, read_and_print, response_exit_code};
use crate::mcp;
use eliot_core::ProviderHold;
use eliot_core::api::{
    AutomaticCleanupPolicy, DaemonShutdownRequest, DaemonShutdownResponse, DaemonStatusRequest,
    DaemonStatusResponse, MaintenancePurgeLegacyAttentionRequest,
    MaintenancePurgeLegacyAttentionResponse, MaintenanceRunRequest, MaintenanceRunResponse,
    ProviderHoldsRequest, ProviderHoldsResponse, ShutdownMode, StatusBlocker,
};
use serde_json::Value;
use serde_json::json;
#[cfg(not(windows))]
#[expect(
    clippy::disallowed_types,
    reason = "Unix daemon lifecycle owns the process command used for detached startup."
)]
use std::process::Command as ProcessCommand;
#[cfg(not(windows))]
use std::process::Stdio;
use std::{
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};
use tokio::time::{Instant, sleep};

pub(crate) async fn run_daemon(
    args: DaemonArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let foreground = args.foreground;
    if matches!(
        &args.action,
        Some(DaemonAction::Install | DaemonAction::Uninstall)
    ) && (agent_credentials_inherited() || std::env::var_os("ELIOT_AGENT_DATA_DIR").is_some())
    {
        return Err(Failure::for_mutation(eliot_core::Error::new(
            "not_allowed",
            "daemon autostart requires an Owner CLI outside an Agent session",
        )));
    }
    if foreground && args.action.is_some() {
        return Err(Failure::new(
            2,
            "--foreground is only valid when starting the daemon",
        ));
    }
    match args.action {
        Some(DaemonAction::Update {
            release,
            wait_seconds,
        }) => {
            if id.is_some() {
                return Err(Failure::new(2, "--id is not valid with daemon update"));
            }
            super::update::run(release, data_dir, wait_seconds, json_output).await
        }
        Some(DaemonAction::Stop(args)) => {
            if args.drain == args.force {
                return Err(Failure::new(
                    2,
                    "daemon stop requires exactly one of --drain or --force",
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
        Some(DaemonAction::Install) => {
            if foreground {
                return Err(Failure::new(
                    2,
                    "--foreground is only valid when starting the daemon",
                ));
            }
            super::autostart::install(&data_dir, json_output).await
        }
        Some(DaemonAction::Uninstall) => {
            if foreground {
                return Err(Failure::new(
                    2,
                    "--foreground is only valid when starting the daemon",
                ));
            }
            super::autostart::uninstall(&data_dir, json_output).await
        }
        None if foreground => {
            let result = match args.upgrade_attempt.as_deref() {
                Some(attempt) => eliot_daemon::operator::run_for_update(data_dir, attempt).await,
                None => eliot_daemon::run(data_dir).await,
            };
            result.map_err(Failure::for_read)?;
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
        MaintenanceCommand::PurgeLegacyAttentionAdvisories(
            PurgeLegacyAttentionAdvisoriesArgs {
                apply,
                expected_count,
            },
        ) => {
            let request = MaintenancePurgeLegacyAttentionRequest {
                expected_message_count: if apply { expected_count } else { None },
            };
            read_and_print!(
                &data_dir,
                json_output,
                "maintenance.purge_legacy_attention_advisories",
                request,
                MaintenancePurgeLegacyAttentionResponse
            )
        }
    }
}

pub(crate) async fn run_status(data_dir: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let params = DaemonStatusRequest::default();
    params.validate().map_err(Failure::for_read)?;
    #[cfg(windows)]
    let acl_repair_was_pending = eliot_daemon::DataDir::open_existing(&data_dir)
        .is_err_and(|error| eliot_daemon::startup::repairable_data_dir_acl(&error));
    let response: DaemonStatusResponse =
        eliot_daemon::client_call_typed(&data_dir, "daemon.status", &params)
            .await
            .map_err(Failure::for_read)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    let provider_holds = read_all_provider_holds(&data_dir).await?;
    let status_time_ms = status_now_ms();
    #[cfg(windows)]
    if acl_repair_was_pending {
        eprintln!("data directory ACL repaired");
    }
    let build_line = response.build_identity.as_ref().map(|identity| {
        format!(
            "Build identity | version={} | executable_directory={} | git_sha={} | executable_sha256={}",
            identity.version,
            identity.executable_directory.as_deref().unwrap_or("unavailable"),
            identity.git_sha.as_deref().unwrap_or("unavailable"),
            identity.executable_sha256.as_deref().unwrap_or("unavailable"),
        )
    });
    let cleanup_line = response.automatic_cleanup.as_ref().map(|cleanup| {
        let policy = if let Some(code) = &cleanup.config_error {
            format!("unavailable ({code})")
        } else {
            match cleanup.policy {
                AutomaticCleanupPolicy::Disabled => "disabled".to_owned(),
                AutomaticCleanupPolicy::PlanOnly => "plan-only".to_owned(),
                AutomaticCleanupPolicy::Enabled if cleanup.startup_plan_only_pending => format!(
                    "enabled after {} (initial plan-only sweep pending)",
                    cleanup.after.as_deref().unwrap_or("the configured window")
                ),
                AutomaticCleanupPolicy::Enabled => format!(
                    "enabled after {}",
                    cleanup.after.as_deref().unwrap_or("the configured window")
                ),
            }
        };
        match &cleanup.last_sweep {
            Some(sweep) => match &sweep.error_code {
                Some(code) => format!("Automatic cleanup {policy} | last sweep failed ({code})"),
                None => format!(
                    "Automatic cleanup {policy} | last sweep would remove {}, kept {}, removed {}, unknown {}{}",
                    sweep.would_remove,
                    sweep.kept,
                    sweep.removed,
                    sweep.unknown,
                    if sweep.plan_only { " (plan-only)" } else { "" },
                ),
            },
            None => format!("Automatic cleanup {policy}"),
        }
    });
    if json_output {
        let mut value = serde_json::to_value(&response)
            .map_err(|_| Failure::new(4, "daemon.status response is invalid"))?;
        value["data_dir"] = Value::String(data_dir.to_string_lossy().into_owned());
        value["provider_holds"] = provider_hold_values(&provider_holds, status_time_ms)?;
        print_value(&value, true);
    } else {
        print_status_summary(&response, &data_dir);
        print_provider_holds(&provider_holds, status_time_ms);
        if let Some(line) = build_line {
            println!("{line}");
        }
        if let Some(line) = cleanup_line {
            println!("{line}");
        }
    }
    Ok(0)
}

async fn read_all_provider_holds(data_dir: &Path) -> Result<Vec<ProviderHold>, Failure> {
    let mut after = None;
    let mut holds = Vec::new();
    loop {
        let request = ProviderHoldsRequest {
            after: after.clone(),
            limit: None,
        };
        request.validate().map_err(Failure::for_read)?;
        let response: ProviderHoldsResponse =
            eliot_daemon::client_call_typed(data_dir, "provider.holds", &request)
                .await
                .map_err(Failure::for_read)?;
        request
            .validate_response(&response)
            .map_err(Failure::invalid_response)?;
        holds.extend(response.holds);
        let Some(next) = response.next else {
            break;
        };
        after = Some(next);
    }
    Ok(holds)
}

fn provider_hold_values(holds: &[ProviderHold], now_ms: i64) -> Result<Value, Failure> {
    let mut values = serde_json::to_value(holds)
        .map_err(|_| Failure::new(4, "provider.holds response is invalid"))?;
    let Some(rows) = values.as_array_mut() else {
        return Err(Failure::new(4, "provider.holds response is invalid"));
    };
    for (row, hold) in rows.iter_mut().zip(holds) {
        row["age_ms"] = if hold.held_since > 0 {
            json!(now_ms.saturating_sub(hold.held_since).max(0))
        } else {
            Value::Null
        };
    }
    Ok(values)
}

fn print_provider_holds(holds: &[ProviderHold], now_ms: i64) {
    if holds.is_empty() {
        println!("Provider holds: none");
        return;
    }
    println!("Provider holds ({}):", holds.len());
    for hold in holds {
        let age = if hold.held_since > 0 {
            let age_ms = now_ms.saturating_sub(hold.held_since).max(0);
            let age_ms = u64::try_from(age_ms).unwrap_or(u64::MAX);
            humantime::format_duration(Duration::from_millis(age_ms)).to_string()
        } else {
            "unknown".to_owned()
        };
        println!(
            "  {} · {} {} · held {} · {}",
            hold.scope,
            hold.kind.as_str(),
            hold.state.as_str(),
            age,
            provider_hold_schedule(hold, now_ms),
        );
    }
}

fn provider_hold_schedule(hold: &ProviderHold, now_ms: i64) -> String {
    let (action, at) = match (hold.retry_after, hold.next_probe_at) {
        (Some(at), _) => ("retry", Some(at)),
        (None, Some(at)) => ("probe", Some(at)),
        (None, None) => ("retry/probe", None),
    };
    let Some(at) = at else {
        return "no retry/probe scheduled".to_owned();
    };
    let timestamp = provider_timestamp(at);
    if at <= now_ms {
        format!("{action} due since {timestamp}")
    } else {
        format!("next {action} {timestamp}")
    }
}

fn provider_timestamp(millis: i64) -> String {
    u64::try_from(millis)
        .ok()
        .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
        .map_or_else(
            || format!("{millis} ms since Unix epoch"),
            |time| humantime::format_rfc3339_millis(time).to_string(),
        )
}

fn status_now_ms() -> i64 {
    i64::try_from(daemon_now_ms()).unwrap_or(i64::MAX)
}

fn print_status_summary(response: &DaemonStatusResponse, data_dir: &Path) {
    println!(
        "Daemon: {} · admission {}",
        response.state, response.admission
    );
    println!(
        "Agents: {} · owned plugins: {} · observers: {} · discovery readers: {}",
        response.agents,
        response.owned_plugins,
        response.observer_readers,
        response.discovery_readers
    );
    println!(
        "Store writes: {}",
        if response.store_write_faulted {
            "faulted"
        } else {
            "healthy"
        }
    );
    println!(
        "Commands: {} unknown · {} unsettled",
        response.unknown_commands_count, response.unsettled_commands_count
    );
    let liveness = &response.liveness;
    println!(
        "Liveness: {} moving · {} asked · {} parked · {} stuck · {} long",
        liveness.moving, liveness.asked, liveness.parked, liveness.stuck, liveness.long
    );
    if response.blockers.is_empty() {
        println!("Blocking work: none");
    } else {
        println!("Blocking work:");
        for blocker in &response.blockers {
            match blocker {
                StatusBlocker::WorkspaceProcesses => println!("  owned workspace processes"),
                StatusBlocker::DiscoveryReaders { count } => {
                    println!("  {count} discovery reader(s)")
                }
                StatusBlocker::AgentLive {
                    agent,
                    generation,
                    state,
                    active_turn,
                } => println!(
                    "  Agent {agent} generation {generation}: {state:?}{}",
                    active_turn
                        .as_deref()
                        .map(|turn| format!(" (turn {turn})"))
                        .unwrap_or_default()
                ),
                StatusBlocker::CommandUnsettled { command_id, state } => {
                    println!("  command {command_id}: {state:?}")
                }
                StatusBlocker::CommandUnsettledMore { count } => {
                    println!("  {count} more unsettled command(s)")
                }
                StatusBlocker::ScriptRunActive { run_id } => {
                    println!("  script run {run_id} is still active")
                }
                StatusBlocker::StoreWriteFaulted => println!("  Store writes are faulted"),
                StatusBlocker::CommandUnknown { command_id } => {
                    println!("  command {command_id} has an unknown outcome")
                }
                StatusBlocker::CommandUnknownMore { count } => {
                    println!("  {count} more command outcome(s) are unknown")
                }
            }
        }
    }
    if let Some(error) = response.workspace_recovery_error.as_deref() {
        println!("Workspace recovery: {error}");
    }
    if let Some(crash) = response.previous_crash.as_ref() {
        println!("Previous crash: {}", crash.summary());
    }
    if !response.plugin_crashes.is_empty() {
        let latest = response
            .plugin_crashes
            .last()
            .map(eliot_core::api::CrashMarker::summary)
            .unwrap_or_default();
        let suffix = if response.plugin_crashes_truncated {
            " (truncated)"
        } else {
            ""
        };
        println!(
            "Plugin crashes: {}{} (latest: {latest})",
            response.plugin_crashes.len(),
            suffix
        );
    }
    println!("Data directory: {}", data_dir.display());
}

pub(crate) async fn start_daemon(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    let start_ms = daemon_now_ms();
    #[cfg(windows)]
    let acl_problem = eliot_daemon::DataDir::open_existing(data_dir)
        .err()
        .filter(eliot_daemon::startup::repairable_data_dir_acl);
    #[cfg(windows)]
    if let Some(error) = acl_problem.as_ref() {
        eliot_daemon::startup::check_acl_repair_target(data_dir, error.clone())
            .map_err(Failure::for_read)?;
    }
    #[cfg(windows)]
    eliot_daemon::start_background_daemon(data_dir)
        .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    #[cfg(not(windows))]
    {
        let executable = std::env::current_exe()
            .map_err(|error| Failure::new(1, format!("cannot locate Eliot executable: {error}")))?;
        #[expect(
            clippy::disallowed_methods,
            reason = "This statement constructs a process owned by this module."
        )]
        #[expect(
            clippy::disallowed_types,
            reason = "This starts the detached daemon executable from the Unix CLI."
        )]
        let mut command = ProcessCommand::new(executable);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
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
    let outcome = wait_for_daemon_ready(data_dir, &log_path, start_ms, json_output).await?;
    #[cfg(windows)]
    if acl_problem.is_some() && outcome == 0 {
        eprintln!("data directory ACL repaired");
    }
    Ok(outcome)
}

pub(crate) fn daemon_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

pub(crate) const STARTUP_WAIT: Duration = Duration::from_secs(30);

/// Route the human previous-crash note for a ready daemon: the note plus a
/// flag that is true when it belongs on stderr. JSON stdout must stay pure
/// machine-readable output (the structured `previous_crash` field already
/// carries the fact), so JSON mode returns true; human mode keeps the note on
/// stdout. `None` means there is no crash to report.
pub(crate) fn crash_note_stream(
    json_output: bool,
    crash_note: Option<&str>,
) -> Option<(&str, bool)> {
    crash_note.map(|note| (note, json_output))
}
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
            Ok(Ok(status)) => {
                let boot_id = read_daemon_boot_id(data_dir);
                let crash_note = status
                    .previous_crash
                    .as_ref()
                    .map(|crash| format!("previous run crashed: {}", crash.summary()));
                if json_output {
                    print_value(
                        &json!({
                            "state": "ready",
                            "data_dir": data_dir.to_string_lossy(),
                            "boot_id": boot_id.unwrap_or_default(),
                            "log": log_path.to_string_lossy(),
                            "previous_crash": status.previous_crash,
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
                // The structured `previous_crash` field already carries the
                // fact in machine-readable stdout; the human note must not
                // join it there, so JSON mode sends the note to stderr.
                if let Some((note, to_stderr)) =
                    crash_note_stream(json_output, crash_note.as_deref())
                {
                    if to_stderr {
                        eprintln!("{note}");
                    } else {
                        println!("{note}");
                    }
                }
                return Ok(0);
            }
            Ok(Err(error))
                if probe_rejection_is_final(&error.code)
                    || eliot_daemon::startup::repairable_data_dir_acl(&error) =>
            {
                let repairable = eliot_daemon::startup::repairable_data_dir_acl(&error);
                let failure = probe_rejection(
                    &error.code,
                    &error.message,
                    data_dir,
                    log_path,
                    agent_credentials_inherited(),
                );
                return Err(append_acl_report(failure, data_dir, repairable));
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
        #[expect(
            clippy::disallowed_methods,
            reason = "CLI daemon startup polls readiness within its caller's deadline"
        )]
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

fn append_acl_report(mut failure: Failure, data_dir: &Path, repairable: bool) -> Failure {
    if !repairable {
        return failure;
    }
    match eliot_daemon::data_dir_acl_report(data_dir) {
        Ok(entries) if !entries.is_empty() => {
            failure.message.push('\n');
            failure
                .message
                .push_str(&super::reads::format_foreign_data_dir_aces(&entries));
        }
        Err(error) => {
            failure.message.push_str(&format!(
                "\nData directory ACL report unavailable: {}",
                error.message
            ));
        }
        Ok(_) => {}
    }
    failure
}

pub(crate) async fn shutdown_daemon(
    data_dir: &Path,
    params: &DaemonShutdownRequest,
) -> Result<DaemonShutdownResponse, Failure> {
    params.validate().map_err(Failure::for_mutation)?;
    let response: DaemonShutdownResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.shutdown", params)
            .await
            .map_err(Failure::for_mutation)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}
