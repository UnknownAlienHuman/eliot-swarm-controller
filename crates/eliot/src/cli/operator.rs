//! The shared stop, drain and verified startup sequence for local operators.

use super::daemon::{STARTUP_WAIT, daemon_now_ms, shutdown_daemon, startup_failure};
use super::support::{Failure, mutation_id, submit_command, submit_request, wait_for_command};
use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentListRequest, AgentListResponse, AgentSummary,
    ArchivedFilter, DaemonShutdownRequest, DaemonStatusRequest, DaemonStatusResponse, ShutdownMode,
};
use eliot_daemon::operator::{DaemonInfo, daemon_alive, daemon_info};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

const STOP_WAIT: Duration = Duration::from_secs(90);
const CUSTODY_SETTLE_WAIT: Duration = Duration::from_secs(10);
const PROBE_INTERVAL: Duration = Duration::from_millis(200);

pub(super) async fn stop_and_drain(data_dir: &Path) -> Result<(), Failure> {
    let (daemon, initial_status) = require_running(data_dir).await?;
    if initial_status.store_write_faulted || initial_status.drain.is_none() {
        return Err(Failure::new(1, "the daemon cannot prove a clean stop"));
    }
    let mut stops = Vec::new();
    let result = async {
        stop_stand_agents(data_dir, &mut stops).await?;
        wait_for_custody_to_settle(data_dir, &daemon).await?;
        let request = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Drain);
        shutdown_daemon(data_dir, &request).await?;
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
    for summary in agents(data_dir)
        .await?
        .into_values()
        .filter(|agent| !agent.state.is_terminal())
    {
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
        let deadline = Instant::now() + STOP_WAIT;
        let mut stop = submit_request(
            None,
            Some(agent.id.clone()),
            "stop",
            json!({"force": false}),
        )?;
        stop.expected_generation = Some(agent.generation);
        stops.push(format!("{}#{} ({})", agent.id, agent.generation, stop.id));
        timeout_at(deadline, submit_command(data_dir, &stop))
            .await
            .map_err(|_| Failure::timeout())??;
        wait_for_command(data_dir, &stop.id, deadline).await?;
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

pub(super) async fn agents(data_dir: &Path) -> Result<BTreeMap<String, AgentSummary>, Failure> {
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
    start_owned_inner(data_dir, binary, None).await
}

pub(super) async fn start_update(
    data_dir: &Path,
    binary: &Path,
    attempt: &str,
) -> Result<u32, Failure> {
    start_owned_inner(data_dir, binary, Some(attempt)).await
}

async fn start_owned_inner(
    data_dir: &Path,
    binary: &Path,
    attempt: Option<&str>,
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

    let result = wait_for_ready(data_dir, pid, previous.as_ref(), start_ms).await;
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
            if daemon_alive(&current).map_err(Failure::for_read)?
                && status(data_dir)
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
