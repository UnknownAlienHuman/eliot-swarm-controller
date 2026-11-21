//! The shared stop, drain and verified startup sequence for local operators.

use super::daemon::{STARTUP_WAIT, daemon_now_ms, shutdown_daemon, startup_failure};
use super::support::{Failure, mutation_id, submit_command, submit_request, wait_for_command};
use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentListRequest, AgentListResponse, AgentSummary,
    ArchivedFilter, DaemonShutdownRequest, DaemonStatusRequest, DaemonStatusResponse, ShutdownMode,
};
use eliot_core::{AgentState, CommandState, TurnOutcome};
use eliot_daemon::operator::{DaemonInfo, daemon_alive, daemon_info};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

const STOP_WAIT: Duration = Duration::from_secs(90);
const CUSTODY_SETTLE_WAIT: Duration = Duration::from_secs(10);
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

pub(super) struct StoppedAgent {
    pub(super) id: String,
    pub(super) generation: i64,
    pub(super) skip_reason: Option<String>,
}

pub(super) async fn stop_and_drain(data_dir: &Path) -> Result<Vec<StoppedAgent>, Failure> {
    let (daemon, initial_status) = require_running(data_dir).await?;
    if initial_status.store_write_faulted {
        return Err(Failure::new(
            1,
            "the daemon Store cannot commit a clean stop",
        ));
    }
    if initial_status.drain.is_none() {
        return Err(Failure::new(
            1,
            "the running daemon cannot report turn and process custody",
        ));
    }
    let initial = agents(data_dir).await?;
    let mut stopped = Vec::new();
    for summary in initial.values().filter(|agent| !agent.state.is_terminal()) {
        let agent = read_agent(data_dir, &summary.id).await?.agent;
        if agent.generation != summary.generation {
            return Err(Failure::new(
                1,
                format!("{} changed generation during shutdown", agent.id),
            ));
        }
        if agent.observe_only || agent.is_terminal() {
            continue;
        }
        let ready_to_resume = summary.state == AgentState::Idle
            && agent.state == AgentState::Idle
            && agent.active_turn.is_none()
            && agent.pending.is_empty()
            && agent.native_ref.is_some()
            && summary.last_outcome != Some(TurnOutcome::Unknown)
            && !summary.archived;
        let deadline = Instant::now() + STOP_WAIT;
        let mut stop = submit_request(
            None,
            Some(agent.id.clone()),
            "stop",
            json!({ "force": false }),
        )?;
        stop.expected_generation = Some(agent.generation);
        let submitted = timeout_at(deadline, submit_command(data_dir, &stop))
            .await
            .map_err(|_| Failure::timeout())??;
        let outcome = if submitted.state == CommandState::Unknown {
            Err(Failure::new(4, "stop outcome is unknown"))
        } else {
            wait_for_command(data_dir, &stop.id, deadline).await
        };
        let skip_reason = match outcome {
            Ok(command)
                if command
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome["already_terminal"].as_bool() == Some(true)) =>
            {
                Some("Agent had already stopped".to_owned())
            }
            Ok(_) if !ready_to_resume => Some(
                if summary.active_turn.is_some() || agent.active_turn.is_some() {
                    "turn was active when shutdown began".to_owned()
                } else if summary.last_outcome == Some(TurnOutcome::Unknown) {
                    "last turn outcome is unknown".to_owned()
                } else {
                    "Agent had no confirmed idle, resumable session".to_owned()
                },
            ),
            Ok(_) => None,
            Err(error) => Some(format!("stop {}: {}", stop.id, error.message)),
        };
        stopped.push(StoppedAgent {
            id: agent.id,
            generation: agent.generation,
            skip_reason,
        });
    }

    wait_for_custody_to_settle(data_dir, &daemon).await?;

    // Read the committed post-stop projection. An ordinary stop does not bind
    // an idle turn, so it can race a newly admitted input.
    let final_agents = agents(data_dir).await?;
    for stopped_agent in stopped
        .iter_mut()
        .filter(|agent| agent.skip_reason.is_none())
    {
        let before = initial.get(&stopped_agent.id);
        let after = final_agents.get(&stopped_agent.id);
        if before.zip(after).is_none_or(|(before, after)| {
            after.generation != stopped_agent.generation
                || after.state != AgentState::Stopped
                || after.active_turn.is_some()
                || after.last_turn_id != before.last_turn_id
                || after.last_outcome != before.last_outcome
        }) {
            stopped_agent.skip_reason = Some("Agent or turn changed during stop".to_owned());
        }
    }

    let deadline = Instant::now() + STARTUP_WAIT;
    let request = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Drain);
    let response = timeout_at(deadline, shutdown_daemon(data_dir, &request))
        .await
        .map_err(|_| Failure::timeout())??;
    while daemon_alive(&daemon).map_err(Failure::for_read)? {
        if Instant::now() >= deadline {
            return Err(Failure::new(
                5,
                format!(
                    "daemon PID {} did not exit after drain; initial blockers: {:?}; inspect swarm status",
                    daemon.pid,
                    response.blockers.unwrap_or_default()
                ),
            ));
        }
        sleep(PROBE_INTERVAL).await;
    }
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
    Ok(stopped)
}

async fn wait_for_custody_to_settle(data_dir: &Path, daemon: &DaemonInfo) -> Result<(), Failure> {
    // Stop commits before the last process-observation projection can settle.
    let deadline = Instant::now() + CUSTODY_SETTLE_WAIT;
    loop {
        let status = status(data_dir).await?;
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
        sleep(PROBE_INTERVAL).await;
    }
}

pub(super) async fn require_running(
    data_dir: &Path,
) -> Result<(DaemonInfo, DaemonStatusResponse), Failure> {
    let daemon = daemon_info(data_dir)
        .map_err(Failure::for_read)?
        .ok_or_else(|| Failure::new(1, "the data directory has no daemon process record"))?;
    let status = status(data_dir).await?;
    check_same_daemon(data_dir, &daemon)?;
    if !daemon_alive(&daemon).map_err(Failure::for_read)? {
        return Err(Failure::new(1, "the recorded daemon process has exited"));
    }
    Ok((daemon, status))
}

fn check_same_daemon(data_dir: &Path, expected: &DaemonInfo) -> Result<(), Failure> {
    let daemon = daemon_info(data_dir).map_err(Failure::for_read)?;
    if daemon.as_ref() != Some(expected) {
        return Err(Failure::new(
            4,
            "daemon process identity changed during the operation",
        ));
    }
    Ok(())
}

async fn status(data_dir: &Path) -> Result<DaemonStatusResponse, Failure> {
    let request = DaemonStatusRequest::default();
    let response = timeout_at(
        Instant::now() + Duration::from_secs(3),
        eliot_daemon::client_call_typed(data_dir, "daemon.status", &request),
    )
    .await
    .map_err(|_| Failure::new(5, "daemon status timed out"))?
    .map_err(Failure::for_read)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

async fn agents(data_dir: &Path) -> Result<BTreeMap<String, AgentSummary>, Failure> {
    let mut request = AgentListRequest {
        archived: ArchivedFilter::Include,
        ..AgentListRequest::default()
    };
    let mut agents = BTreeMap::new();
    loop {
        let page: AgentListResponse =
            eliot_daemon::client_call_typed(data_dir, "agent.list", &request)
                .await
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

pub(super) async fn read_agent(data_dir: &Path, id: &str) -> Result<AgentGetResponse, Failure> {
    let request = AgentGetRequest::new(id);
    let response = eliot_daemon::client_call_typed(data_dir, "agent.get", &request)
        .await
        .map_err(Failure::for_read)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

pub(super) async fn start_owned(data_dir: &Path, binary: &Path) -> Result<u32, Failure> {
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
    let args = [
        std::ffi::OsStr::new("--data-dir"),
        data_dir.as_os_str(),
        std::ffi::OsStr::new("daemon"),
        std::ffi::OsStr::new("--foreground"),
    ];
    #[cfg(windows)]
    let pid = eliot_daemon::operator::start_detached(binary, &args, true, None)
        .map_err(Failure::for_read)?;
    #[cfg(unix)]
    let pid = {
        use std::process::Stdio;
        tokio::process::Command::new(binary)
            .args(args)
            .current_dir(
                binary
                    .parent()
                    .ok_or_else(|| Failure::new(2, "invalid release path"))?,
            )
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| Failure::new(1, format!("start release daemon: {error}")))?
            .id()
            .ok_or_else(|| Failure::new(1, "release daemon did not return a process ID"))?
    };
    #[cfg(not(any(windows, unix)))]
    let pid = {
        let _ = (binary, args);
        Err(Failure::new(
            2,
            "detached daemon startup is unsupported on this platform",
        ))?
    };

    let deadline = Instant::now() + STARTUP_WAIT;
    let log_path = data_dir.join("logs").join("daemon.log");
    loop {
        if let Some(current) = daemon_info(data_dir).map_err(Failure::for_read)?
            && Some(&current) != previous.as_ref()
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
            if daemon_alive(&current).map_err(Failure::for_read)? && status(data_dir).await.is_ok()
            {
                check_same_daemon(data_dir, &current)?;
                return Ok(pid);
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
        sleep(PROBE_INTERVAL).await;
    }
}
