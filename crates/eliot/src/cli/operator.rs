//! The shared stop, drain and verified startup sequence for local operators.

use super::daemon::{STARTUP_WAIT, daemon_now_ms, startup_failure};
use super::support::{Failure, command_state_result, mutation_id, submit_request};
use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentListRequest, AgentListResponse, AgentSummary,
    ArchivedFilter, CommandGetRequest, CommandSubmitResponse, DaemonShutdownRequest,
    DaemonShutdownResponse, DaemonStatusRequest, DaemonStatusResponse, MAX_SWARM_WAIT_MS,
    ShutdownMode, SwarmWaitRequest, SwarmWaitResponse,
};
use eliot_core::{Command as CoreCommand, CommandState, Error as CoreError, SubmitCommand};
use eliot_daemon::operator::{DaemonInfo, daemon_alive, daemon_info};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

const STOP_WAIT: Duration = Duration::from_secs(90);
const CUSTODY_SETTLE_WAIT: Duration = Duration::from_secs(10);
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

async fn stand_owner_call_typed<P: Serialize + ?Sized, R: DeserializeOwned>(
    data_dir: &Path,
    method: &str,
    params: &P,
) -> eliot_core::Result<R> {
    let payload = serde_json::to_value(params)
        .map_err(|_| CoreError::new("invalid", "daemon RPC parameters could not be serialized"))?;
    let allowed = match method {
        "daemon.status" | "agent.list" | "agent.get" | "swarm.wait" | "command.get"
        | "daemon.shutdown" => true,
        "command.submit" => payload.get("verb").and_then(serde_json::Value::as_str) == Some("stop"),
        _ => false,
    };
    if !allowed {
        return Err(CoreError::new(
            "not_allowed",
            "stand owner helper only permits stand lifecycle and readback RPC methods",
        ));
    }
    let absolute = std::path::absolute(data_dir)
        .map_err(|error| CoreError::new("unavailable", format!("resolve stand root: {error}")))?;
    eliot_daemon::operator::check_directory_chain(&absolute)?;
    // Never resolve the root component itself: a link there must not redirect.
    let canonical = eliot_daemon::operator::stand_key_root(&absolute)?;
    eliot_daemon::operator::check_directory_chain(&canonical)?;
    if eliot_daemon::operator::target_is_protected_from_stand_owner_auth(&absolute) {
        return Err(CoreError::new(
            "not_allowed",
            "a stand operation cannot target a daemon root this process resolves as its own",
        ));
    }
    let located = eliot_daemon::operator::locate_stand_registration(&canonical)?;
    eliot_daemon::operator::ensure_registry_is_external(&canonical, &located.registry)?;
    let registration = located.record;
    eliot_daemon::operator::verify_stand_marker(&canonical, &registration)?;
    let token = eliot_daemon::operator::verify_stand_target(&canonical, &registration)?
        .ok_or_else(|| CoreError::new("not_allowed", "stand has no live owner binding"))?;
    let value =
        eliot_daemon::client_call_with_owner_credential(&canonical, method, payload, &token)
            .await?;
    if !value.is_object() {
        return Err(CoreError::new(
            "unknown",
            "daemon returned a malformed typed result",
        ));
    }
    serde_json::from_value(value)
        .map_err(|_| CoreError::new("unknown", "daemon returned a malformed typed result"))
}

pub(super) async fn stop_and_drain_stand(data_dir: &Path) -> Result<(), Failure> {
    let (daemon, initial_status) = require_running_inner(data_dir, true).await?;
    if initial_status.store_write_faulted || initial_status.drain.is_none() {
        return Err(Failure::new(1, "the daemon cannot prove a clean stop"));
    }
    let mut stops = Vec::new();
    let result = async {
        stop_stand_agents(data_dir, &mut stops).await?;
        wait_for_custody_to_settle(data_dir, &daemon).await?;
        let request = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Drain);
        stand_shutdown(data_dir, &request).await?;
        wait_for_exit(&daemon, STARTUP_WAIT).await?;
        check_same_daemon(data_dir, &daemon)?;
        if eliot_daemon::last_daemon_phase(data_dir).as_deref() != Some("daemon.stopped") {
            return Err(Failure::new(
                4,
                format!(
                    "daemon PID {} exited without a clean shutdown record",
                    daemon.pid
                ),
            ));
        }
        Ok(())
    }
    .await;
    result.map_err(|mut error| {
        error.message = format!("{}; stand stop requests: {:?}", error.message, stops);
        error
    })
}

async fn stop_stand_agents(data_dir: &Path, stops: &mut Vec<String>) -> Result<(), Failure> {
    for summary in agents_inner(data_dir, true)
        .await?
        .into_values()
        .filter(|agent| !agent.state.is_terminal())
    {
        let agent = read_stand_agent(data_dir, &summary.id).await?.agent;
        if agent.generation != summary.generation {
            return Err(Failure::new(
                1,
                format!("{} changed generation during shutdown", agent.id),
            ));
        }
        if agent.observe_only || agent.is_terminal() {
            continue;
        }
        let deadline = Instant::now() + STOP_WAIT;
        let mut stop = submit_request(
            None,
            Some(agent.id.clone()),
            "stop",
            json!({"force": false}),
        )?;
        stop.expected_generation = Some(agent.generation);
        stops.push(format!("{}#{} ({})", agent.id, agent.generation, stop.id));
        timeout_at(deadline, stand_submit_command(data_dir, &stop))
            .await
            .map_err(|_| Failure::timeout())??;
        stand_wait_for_command(data_dir, &stop.id, deadline).await?;
    }
    Ok(())
}

pub(super) async fn wait_for_exit(daemon: &DaemonInfo, wait: Duration) -> Result<(), Failure> {
    let deadline = Instant::now() + wait;
    while daemon_alive(daemon).map_err(Failure::for_read)? {
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                format!("daemon PID {} did not exit", daemon.pid),
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The local operator polls exact daemon process exit within its deadline"
        )]
        sleep(PROBE_INTERVAL).await;
    }
    Ok(())
}

async fn wait_for_custody_to_settle(data_dir: &Path, daemon: &DaemonInfo) -> Result<(), Failure> {
    // Stop commits before the last process-observation projection can settle.
    let deadline = Instant::now() + CUSTODY_SETTLE_WAIT;
    loop {
        let status = status_inner(data_dir, true).await?;
        check_same_daemon(data_dir, daemon)?;
        let report = status
            .drain
            .ok_or_else(|| Failure::new(1, "daemon status omitted its drain report"))?;
        if report.running_turns_count == 0 && report.owned_processes_count == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Failure::new(
                1,
                format!(
                    "cannot drain: {} running turn(s), {} owned process(es); inspect swarm status",
                    report.running_turns_count, report.owned_processes_count
                ),
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The local operator waits briefly for stopped process custody to settle"
        )]
        sleep(PROBE_INTERVAL).await;
    }
}

pub(super) async fn require_running(
    data_dir: &Path,
) -> Result<(DaemonInfo, DaemonStatusResponse), Failure> {
    require_running_inner(data_dir, false).await
}

async fn require_running_inner(
    data_dir: &Path,
    use_stand_owner: bool,
) -> Result<(DaemonInfo, DaemonStatusResponse), Failure> {
    let daemon = daemon_info(data_dir)
        .map_err(Failure::for_read)?
        .ok_or_else(|| Failure::new(1, "the data directory has no daemon process record"))?;
    let status = status_inner(data_dir, use_stand_owner).await?;
    check_same_daemon(data_dir, &daemon)?;
    if !daemon_alive(&daemon).map_err(Failure::for_read)? {
        return Err(Failure::new(1, "the recorded daemon process has exited"));
    }
    Ok((daemon, status))
}

pub(super) fn check_same_daemon(data_dir: &Path, expected: &DaemonInfo) -> Result<(), Failure> {
    let daemon = daemon_info(data_dir).map_err(Failure::for_read)?;
    if daemon.as_ref() != Some(expected) {
        return Err(Failure::new(
            4,
            "daemon process identity changed during the operation",
        ));
    }
    Ok(())
}

pub(super) async fn status(data_dir: &Path) -> Result<DaemonStatusResponse, Failure> {
    status_inner(data_dir, false).await
}

async fn status_inner(
    data_dir: &Path,
    use_stand_owner: bool,
) -> Result<DaemonStatusResponse, Failure> {
    let request = DaemonStatusRequest::default();
    let call = async {
        if use_stand_owner {
            stand_owner_call_typed(data_dir, "daemon.status", &request).await
        } else {
            eliot_daemon::client_call_typed(data_dir, "daemon.status", &request).await
        }
    };
    let response: DaemonStatusResponse = timeout_at(Instant::now() + Duration::from_secs(3), call)
        .await
        .map_err(|_| Failure::new(5, "daemon status timed out"))?
        .map_err(Failure::for_read)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

pub(super) async fn agents(data_dir: &Path) -> Result<BTreeMap<String, AgentSummary>, Failure> {
    agents_inner(data_dir, false).await
}

async fn agents_inner(
    data_dir: &Path,
    use_stand_owner: bool,
) -> Result<BTreeMap<String, AgentSummary>, Failure> {
    let mut request = AgentListRequest {
        archived: ArchivedFilter::Include,
        ..AgentListRequest::default()
    };
    let mut agents = BTreeMap::new();
    loop {
        let page: AgentListResponse = if use_stand_owner {
            stand_owner_call_typed(data_dir, "agent.list", &request).await
        } else {
            eliot_daemon::client_call_typed(data_dir, "agent.list", &request).await
        }
        .map_err(Failure::for_read)?;
        request
            .validate_response(&page)
            .map_err(Failure::invalid_response)?;
        agents.extend(
            page.agents
                .into_iter()
                .map(|row| (row.summary.id.clone(), row.summary)),
        );
        let Some(next) = page.next else {
            return Ok(agents);
        };
        request.after = Some(next);
    }
}

async fn read_stand_agent(data_dir: &Path, id: &str) -> Result<AgentGetResponse, Failure> {
    let request = AgentGetRequest::new(id);
    let response: AgentGetResponse = stand_owner_call_typed(data_dir, "agent.get", &request)
        .await
        .map_err(Failure::for_read)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

async fn stand_submit_command(
    data_dir: &Path,
    params: &SubmitCommand,
) -> Result<CommandSubmitResponse, Failure> {
    params.validate().map_err(Failure::for_mutation)?;
    let response: CommandSubmitResponse =
        stand_owner_call_typed(data_dir, "command.submit", params)
            .await
            .map_err(Failure::for_mutation)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

async fn stand_wait_for_command(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<CoreCommand, Failure> {
    let request = CommandGetRequest::new(command_id);
    request.validate().map_err(Failure::readback)?;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Failure::timeout());
        }
        let wait_ms = u64::try_from(remaining.as_millis())
            .unwrap_or(u64::MAX)
            .min(MAX_SWARM_WAIT_MS);
        let wait_req =
            SwarmWaitRequest::new(vec![command_id.to_owned()], Vec::new(), "all", wait_ms);
        wait_req.validate().map_err(Failure::readback)?;
        let reply: SwarmWaitResponse = timeout_at(
            deadline,
            stand_owner_call_typed(data_dir, "swarm.wait", &wait_req),
        )
        .await
        .map_err(|_| Failure::timeout())?
        .map_err(Failure::readback)?;
        wait_req
            .validate_response(&reply)
            .map_err(Failure::readback)?;
        if !reply.completed {
            continue;
        }
        let command: CoreCommand = timeout_at(
            deadline,
            stand_owner_call_typed(data_dir, "command.get", &request),
        )
        .await
        .map_err(|_| Failure::timeout())?
        .map_err(Failure::readback)?;
        request
            .validate_response(&command)
            .map_err(Failure::readback)?;
        if !matches!(
            command.state,
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
        ) {
            command_state_result(&command)?;
            return Ok(command);
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The local stand operator waits briefly for a settled command readback."
        )]
        sleep(PROBE_INTERVAL.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
}

async fn stand_shutdown(
    data_dir: &Path,
    params: &DaemonShutdownRequest,
) -> Result<DaemonShutdownResponse, Failure> {
    params.validate().map_err(Failure::for_mutation)?;
    let response: DaemonShutdownResponse =
        stand_owner_call_typed(data_dir, "daemon.shutdown", params)
            .await
            .map_err(Failure::for_mutation)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

pub(super) async fn start_owned(data_dir: &Path, binary: &Path) -> Result<u32, Failure> {
    start_owned_inner(data_dir, binary, None, false).await
}

pub(super) async fn start_owned_stand(data_dir: &Path, binary: &Path) -> Result<u32, Failure> {
    start_owned_inner(data_dir, binary, None, true).await
}

pub(super) async fn start_update(
    data_dir: &Path,
    binary: &Path,
    attempt: &str,
) -> Result<u32, Failure> {
    start_owned_inner(data_dir, binary, Some(attempt), false).await
}

async fn start_owned_inner(
    data_dir: &Path,
    binary: &Path,
    attempt: Option<&str>,
    use_stand_owner: bool,
) -> Result<u32, Failure> {
    let start_ms = daemon_now_ms();
    let previous = daemon_info(data_dir).map_err(Failure::for_read)?;
    if let Some(info) = &previous
        && daemon_alive(info).map_err(Failure::for_read)?
    {
        return Err(Failure::new(
            1,
            format!("daemon PID {} is already running", info.pid),
        ));
    }
    let mut args = vec![
        std::ffi::OsStr::new("--data-dir"),
        data_dir.as_os_str(),
        std::ffi::OsStr::new("daemon"),
        std::ffi::OsStr::new("--foreground"),
    ];
    if let Some(attempt) = attempt {
        args.extend([
            std::ffi::OsStr::new("--upgrade-attempt"),
            std::ffi::OsStr::new(attempt),
        ]);
    }
    #[cfg(windows)]
    let (pid, birth) = {
        let pid = eliot_daemon::operator::start_detached(binary, &args, true, None)
            .map_err(Failure::for_read)?;
        let birth = match eliot_daemon::operator::candidate_birth(pid) {
            Ok(birth) => birth,
            Err(error) if error.code == "process_gone" => {
                return Err(Failure::new(
                    1,
                    format!("replacement PID {pid} exited during startup"),
                ));
            }
            Err(error) => return Err(Failure::for_read(error)),
        };
        (pid, birth)
    };
    #[cfg(unix)]
    let (pid, mut child) = {
        use std::process::Stdio;
        #[expect(
            clippy::disallowed_methods,
            reason = "This statement constructs a process owned by this module."
        )]
        #[expect(
            clippy::disallowed_types,
            reason = "This launches the Unix operator helper under its scoped process owner."
        )]
        let mut command = tokio::process::Command::new(binary);
        command
            .args(args)
            .current_dir(
                binary
                    .parent()
                    .ok_or_else(|| Failure::new(2, "invalid release path"))?,
            )
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("ELIOT_") {
                command.env_remove(name);
            }
        }
        let child = command
            .spawn()
            .map_err(|error| Failure::new(1, format!("start release daemon: {error}")))?;
        let pid = child
            .id()
            .ok_or_else(|| Failure::new(1, "release daemon did not return a process ID"))?;
        (pid, child)
    };
    #[cfg(not(any(windows, unix)))]
    let pid: u32 = {
        let _ = (binary, args);
        Err(Failure::new(
            2,
            "detached daemon startup is unsupported on this platform",
        ))?
    };

    let result = wait_for_ready(data_dir, pid, previous.as_ref(), start_ms, use_stand_owner).await;
    if let Err(mut failure) = result {
        let _publication = if let Some(attempt) = attempt {
            match eliot_daemon::operator::lock_update_publication(data_dir, attempt) {
                Ok(guard) if daemon_info(data_dir).is_ok_and(|info| info == previous) => {
                    Some(guard)
                }
                // The candidate may already be admitting writes. Leave it to
                // the operator rather than terminating it on a timeout.
                _ => return Err(failure),
            }
        } else {
            None
        };
        #[cfg(windows)]
        let cleanup = eliot_daemon::operator::stop_candidate(pid, birth).map_err(Failure::for_read);
        #[cfg(unix)]
        let cleanup = child
            .kill()
            .await
            .map_err(|error| Failure::new(4, error.to_string()));
        #[cfg(not(any(windows, unix)))]
        let cleanup: Result<(), Failure> = Ok(());
        if let Err(cleanup) = cleanup {
            failure.message = format!(
                "{}; replacement custody could not be closed: {}",
                failure.message, cleanup.message
            );
        }
        return Err(failure);
    }
    Ok(pid)
}

async fn wait_for_ready(
    data_dir: &Path,
    pid: u32,
    previous: Option<&DaemonInfo>,
    start_ms: u128,
    use_stand_owner: bool,
) -> Result<(), Failure> {
    let deadline = Instant::now() + STARTUP_WAIT;
    let log_path = data_dir.join("logs").join("daemon.log");
    loop {
        if let Some(current) = daemon_info(data_dir).map_err(Failure::for_read)?
            && Some(&current) != previous
        {
            if current.pid != pid {
                return Err(Failure::new(
                    4,
                    format!(
                        "daemon PID {} took the data directory while starting PID {pid}",
                        current.pid
                    ),
                ));
            }
            // `stand up` binds its registration to the owner token the
            // daemon it just started minted, before any owner call.
            if use_stand_owner
                && !eliot_daemon::operator::bind_stand_owner_token(data_dir)
                    .map_err(Failure::for_read)?
            {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "The stand operator waits for its new daemon to mint the owner token"
                )]
                sleep(PROBE_INTERVAL).await;
                continue;
            }
            if daemon_alive(&current).map_err(Failure::for_read)?
                && status_inner(data_dir, use_stand_owner)
                    .await
                    .is_ok_and(|status| status.admission == "open" && !status.store_write_faulted)
            {
                check_same_daemon(data_dir, &current)?;
                return Ok(());
            }
        }
        if let Some(failure) = eliot_daemon::daemon_failure_since(data_dir, start_ms) {
            return Err(startup_failure(failure, data_dir, &log_path));
        }
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                format!(
                    "release daemon PID {pid} did not become ready; read {}",
                    log_path.display()
                ),
            ));
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The local operator waits for the requested daemon to become ready"
        )]
        sleep(PROBE_INTERVAL).await;
    }
}
