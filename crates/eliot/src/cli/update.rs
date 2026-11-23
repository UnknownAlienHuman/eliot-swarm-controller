//! Replace a running daemon without replaying an Agent input.

use super::daemon::{STARTUP_WAIT, shutdown_daemon};
use super::operator::{
    agents, check_same_daemon, require_running, start_owned, start_update, status, wait_for_exit,
};
use super::support::{
    Failure, mutation_id, print_value_or_human, submit_command, submit_request, wait_for_command,
};
use eliot_core::api::discovery::{IdentityRequest, IdentityResponse};
use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentSummary, DaemonShutdownRequest, HarnessCapabilities,
    ShutdownMode, TurnGetRequest,
};
use eliot_core::{Agent, AgentState, CommandState};
use eliot_daemon::operator::{
    DaemonInfo, binary_sha256, continue_after_restart, copy_store_for_update_check, daemon_alive,
    discard_update_backup, lock_offline, lock_transition, replacement_data_dir,
    restore_update_backup, running_binary, update_backup_path,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout, timeout_at};

const INTERRUPT_SETTLE_WAIT: Duration = Duration::from_secs(25);
const RESUME_WAIT: Duration = Duration::from_secs(120);

fn print_update_value(value: &Value, json_output: bool) {
    print_value_or_human(value, json_output, || {
        println!(
            "Daemon update: {}",
            value["state"].as_str().unwrap_or("outcome unavailable")
        );
        for (key, label) in [
            ("previous_pid", "Previous daemon PID"),
            ("pid", "Daemon PID"),
            ("candidate_pid", "Published candidate PID"),
            ("admission", "Admission"),
            ("deadline_policy", "Drain deadline policy"),
            ("version", "Release"),
            ("binary", "Binary"),
            ("sha256", "Binary SHA-256"),
            ("backup_cleanup_error", "Backup cleanup error"),
            ("error", "Error"),
            ("update_error", "Update error"),
            ("rollback_error", "Rollback error"),
            ("message", "Note"),
            ("continuation", "Next"),
        ] {
            if let Some(field) = value.get(key) {
                if let Some(text) = field.as_str() {
                    println!("{label}: {text}");
                } else if let Some(number) = field.as_u64() {
                    println!("{label}: {number}");
                }
            }
        }
        if let Some(backup) = value.get("backup") {
            println!("Store backup retained: {}", backup.as_str().unwrap_or("no"));
        }
        for key in [
            "commit_reply_uncertain",
            "store_restored",
            "backup_retained",
        ] {
            if let Some(flag) = value.get(key).and_then(Value::as_bool) {
                println!("{}: {flag}", key.replace('_', " "));
            }
        }
        if let Some(agents) = value.get("agents_before").and_then(Value::as_array) {
            println!("Agents before restart: {}", agents.len());
            print_update_agents(agents);
        }
        if let Some(interruptions) = value.get("interrupted_agents").and_then(Value::as_array) {
            for interruption in interruptions {
                println!(
                    "  {} · stopped turn {} · {}",
                    interruption["agent"].as_str().unwrap_or("?"),
                    interruption["turn"].as_str().unwrap_or("?"),
                    interruption["continuation"].as_str().unwrap_or("unknown"),
                );
            }
        }
        if let Some(agents) = value.get("agents_after").and_then(Value::as_array) {
            println!("Agents observed after restart: {}", agents.len());
            print_update_agents(agents);
        } else if let Some(error) = value
            .get("agents_after")
            .and_then(|after| after.get("read_error"))
            .and_then(Value::as_str)
        {
            println!("Agent readback unavailable: {error}");
        }
        if let Some(desktop) = value.get("desktop") {
            if let Some(pid) = desktop.get("pid").and_then(Value::as_u64) {
                println!("Desktop companion PID: {pid}");
            }
            if let Some(hash) = desktop.get("sha256").and_then(Value::as_str) {
                println!("Desktop companion SHA-256: {hash}");
            }
            if let Some(reason) = desktop.get("reason").and_then(Value::as_str) {
                println!("Desktop companion: {reason}");
            }
        }
    });
}

fn print_update_agents(agents: &[Value]) {
    for agent in agents {
        let id = agent.get("agent").and_then(Value::as_str).unwrap_or("?");
        let generation = agent
            .get("generation")
            .map(Value::to_string)
            .unwrap_or_else(|| "?".to_owned());
        let state = agent
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        println!("  {id} · generation {generation} · state {state}");
    }
}

pub(crate) async fn run(
    source: PathBuf,
    data_dir: PathBuf,
    wait_seconds: u64,
    json_output: bool,
) -> Result<u8, Failure> {
    Box::pin(run_update(
        source,
        data_dir,
        wait_seconds,
        false,
        json_output,
    ))
    .await
}

pub(super) async fn run_interrupting(
    source: PathBuf,
    data_dir: PathBuf,
    wait_seconds: u64,
    json_output: bool,
) -> Result<u8, Failure> {
    Box::pin(run_update(
        source,
        data_dir,
        wait_seconds,
        true,
        json_output,
    ))
    .await
}

async fn run_update(
    source: PathBuf,
    data_dir: PathBuf,
    wait_seconds: u64,
    interrupt: bool,
    json_output: bool,
) -> Result<u8, Failure> {
    let (old, _) = require_running(&data_dir).await?;
    let identity: IdentityResponse =
        eliot_daemon::client_call_typed(&data_dir, "api.identity", &IdentityRequest::default())
            .await
            .map_err(Failure::for_read)?;
    if identity.principal != "owner" {
        return Err(Failure::new(3, "daemon update requires the local Owner"));
    }
    let previous_binary = running_binary(&old).map_err(Failure::for_read)?;
    let previous_hash = binary_sha256(&previous_binary).map_err(Failure::for_read)?;
    let binary = release_binary(&source)?;
    let previous = release_info(&previous_binary).await?;
    let candidate = release_info(&binary).await?;
    if candidate.store_schema < previous.store_schema {
        return Err(Failure::new(
            2,
            format!(
                "Store downgrade refused: replacement schema {} is older than current schema {}",
                candidate.store_schema, previous.store_schema
            ),
        ));
    }
    if !previous.backup_update || !candidate.backup_update {
        return Err(Failure::new(
            2,
            "both releases must support updates with a Store backup",
        ));
    }
    let sha256 = binary_sha256(&binary).map_err(Failure::for_read)?;
    #[cfg(windows)]
    let desktop = desktop_binary(&binary)?;
    let _transition = lock_transition(&data_dir).map_err(Failure::for_read)?;
    check_same_daemon(&data_dir, &old)?;
    let startup_path = replacement_data_dir(&data_dir, &old).map_err(Failure::for_read)?;
    preflight_store(&startup_path, &binary).await?;
    if binary_sha256(&binary).map_err(Failure::for_read)? != sha256 {
        return Err(Failure::new(
            2,
            "replacement binary changed during preflight",
        ));
    }
    check_same_daemon(&data_dir, &old)?;
    let before = agents(&data_dir).await?.into_values()
        .filter(|agent| !agent.state.is_terminal())
        .map(|agent| json!({"agent": agent.id, "generation": agent.generation, "state": agent.state}))
        .collect::<Vec<_>>();
    let prepare = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::PrepareRestart);
    let mut commit = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Restart);
    commit.restart_id = Some(prepare.id.clone());
    commit.backup = true;
    let backup = StoreBackup {
        id: &commit.id,
        path: update_backup_path(&startup_path, &commit.id).map_err(Failure::for_read)?,
    };
    let previous = PreviousRelease {
        binary: &previous_binary,
        sha256: &previous_hash,
        daemon: &old,
        store_schema: previous.store_schema,
    };
    let mut cancel = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::CancelRestart);
    cancel.restart_id = Some(prepare.id.clone());

    let mut stopped = Vec::new();
    let mode = if interrupt {
        "interrupt_on_deadline"
    } else {
        "cancel_on_deadline"
    };
    let committed = Box::pin(quiesce(
        &data_dir,
        &old,
        &prepare,
        &commit,
        Duration::from_secs(wait_seconds),
        interrupt,
        &mut stopped,
    ))
    .await;
    let commit_reply_uncertain = committed.is_err();
    if let Err(error) = committed {
        match reconcile_preparation(&data_dir, &old, &cancel).await {
            result @ (Preparation::Open | Preparation::Unconfirmed) => {
                let recovered = matches!(result, Preparation::Open);
                let backup_path = backup.retained();
                let backup_cleanup_error = if recovered && backup_path.is_some() {
                    discard_update_backup(&startup_path, backup.id).err()
                } else {
                    None
                };
                let (interruptions, resume_failed) = if recovered {
                    Box::pin(resume_interrupted(&data_dir, &stopped)).await
                } else {
                    hold_interrupted(&stopped, "daemon admission unconfirmed")
                };
                print_update_value(
                    &json!({
                        "state": if recovered { "update_cancelled" } else { "update_cancel_unconfirmed" },
                        "pid": old.pid, "error": error.message,
                        "admission": if recovered { Some("open") } else { None },
                        "deadline_policy": mode, "interrupted_agents": interruptions,
                        "agents_before": before,
                        "backup": backup_path,
                        "backup_retained": backup.retained().is_some(),
                        "backup_cleanup_error": backup_cleanup_error.as_ref().map(ToString::to_string),
                    }),
                    json_output,
                );
                return Ok(
                    if recovered && backup_cleanup_error.is_none() && !resume_failed {
                        error.exit_code
                    } else {
                        4
                    },
                );
            }
            Preparation::Exited => {
                return Ok(rollback(
                    &startup_path,
                    &previous,
                    &backup,
                    RollbackStore::Current,
                    error.message,
                    UpdateAgents {
                        before,
                        stopped,
                        mode,
                    },
                    json_output,
                )
                .await);
            }
            Preparation::Restarting => {} // The old process confirmed commit despite the RPC error.
        }
    }

    if let Err(error) = finish_committed_restart(&startup_path, &old).await {
        print_update_value(
            &json!({"state":"restart_exit_unknown","previous_pid":old.pid,
            "error":error.message,"agents_before":before,"backup":backup.retained(),
            "admission":"restarting",
            "continuation":"confirm the previous daemon has exited before starting a release on this Store",
            "deadline_policy":mode,
            "interrupted_agents":hold_interrupted(&stopped, "old daemon exit unconfirmed").0}),
            json_output,
        );
        return Ok(error.exit_code);
    }
    let replacement = match start_update(&startup_path, &binary, backup.id).await {
        Ok(info) => info,
        Err(error) => {
            return Ok(rollback(
                &startup_path,
                &previous,
                &backup,
                RollbackStore::Unpublished,
                error.message,
                UpdateAgents {
                    before,
                    stopped,
                    mode,
                },
                json_output,
            )
            .await);
        }
    };
    if let Err(error) = check_health(&startup_path, &replacement).await {
        return Ok(rollback(
            &startup_path,
            &previous,
            &backup,
            RollbackStore::Published(&replacement, candidate.store_schema),
            error.message,
            UpdateAgents {
                before,
                stopped,
                mode,
            },
            json_output,
        )
        .await);
    }
    let backup_cleanup = discard_update_backup(&startup_path, backup.id).err();
    let (interruptions, resume_failed) = Box::pin(resume_interrupted(&data_dir, &stopped)).await;

    #[cfg(windows)]
    let (desktop, desktop_failed) = start_companion(&data_dir, desktop);
    #[cfg(not(windows))]
    let (desktop, desktop_failed) = (None::<Value>, false);
    print_update_value(
        &json!({
            "state": if desktop_failed || backup_cleanup.is_some() || resume_failed { "partial" } else { "ready" },
            "previous_pid": old.pid, "pid": replacement.pid,
            "deadline_policy": mode, "interrupted_agents": interruptions,
            "commit_reply_uncertain": commit_reply_uncertain,
            "binary": eliot_core::display_path(&binary.to_string_lossy()), "version": candidate.version,
            "sha256": sha256, "desktop": desktop,
            "backup": backup.retained(), "backup_cleanup_error": backup_cleanup.as_ref().map(ToString::to_string),
            "agents_before": before, "agents_after": observe_agents(&data_dir, &before).await,
            "continuation": "boot recovery applies the configured on_restart policy",
        }),
        json_output,
    );
    Ok(u8::from(
        desktop_failed || backup_cleanup.is_some() || resume_failed,
    ))
}

struct PreviousRelease<'a> {
    binary: &'a Path,
    sha256: &'a str,
    daemon: &'a DaemonInfo,
    store_schema: i64,
}

enum RollbackStore<'a> {
    Current,
    Unpublished,
    Published(&'a DaemonInfo, i64),
}

struct UpdateAgents {
    before: Vec<Value>,
    stopped: Vec<StoppedAgent>,
    mode: &'static str,
}

struct StoppedAgent {
    agent: Agent,
    turn: String,
    stop_id: String,
}

struct StoreBackup<'a> {
    id: &'a str,
    path: PathBuf,
}

impl StoreBackup<'_> {
    fn retained(&self) -> Option<String> {
        self.path
            .exists()
            .then(|| eliot_core::display_path(&self.path.to_string_lossy()))
    }
}

async fn check_health(path: &Path, replacement: &DaemonInfo) -> Result<(), Failure> {
    let readiness_deadline = Instant::now() + STARTUP_WAIT;
    let mut healthy_since = None;
    loop {
        check_same_daemon(path, replacement)?;
        if !daemon_alive(replacement).map_err(Failure::for_read)? {
            return Err(Failure::new(
                4,
                "replacement exited during the health check",
            ));
        }
        match status(path).await {
            Ok(status) if status.admission == "open" && !status.store_write_faulted => {
                let since = healthy_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(3) {
                    check_same_daemon(path, replacement)?;
                    return Ok(());
                }
            }
            Ok(status) => {
                return Err(Failure::new(
                    4,
                    format!(
                        "replacement health check observed admission {} and Store write fault {}",
                        status.admission, status.store_write_faulted
                    ),
                ));
            }
            Err(error) if healthy_since.is_some() || Instant::now() >= readiness_deadline => {
                return Err(error);
            }
            Err(_) => {}
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The update command polls its authenticated restart status within a deadline"
        )]
        sleep(Duration::from_millis(250)).await;
    }
}

async fn finish_committed_restart(path: &Path, old: &DaemonInfo) -> Result<(), Failure> {
    #[cfg(not(windows))]
    let _ = path;
    let exit = wait_for_exit(old, Duration::from_secs(120)).await;
    #[cfg(windows)]
    if exit.as_ref().is_err_and(|error| error.exit_code == 5) {
        // The restart committed and the Store was sealed. Verify the exact
        // old daemon before ending its Job custody and starting a replacement.
        check_same_daemon(path, old)?;
        eliot_daemon::operator::stop_published_candidate(old).map_err(Failure::for_read)?;
        return wait_for_exit(old, Duration::from_secs(5)).await;
    }
    exit
}

async fn retire_failed_candidate(path: &Path, candidate: &DaemonInfo) -> Result<(), Failure> {
    check_same_daemon(path, candidate)?;
    if !daemon_alive(candidate).map_err(Failure::for_read)? {
        return Ok(());
    }
    let shutdown = DaemonShutdownRequest::new(mutation_id(None)?, ShutdownMode::Force);
    let requested = call_shutdown(path, &shutdown).await;
    if wait_for_exit(candidate, Duration::from_secs(12))
        .await
        .is_ok()
    {
        return Ok(());
    }
    #[cfg(windows)]
    {
        // A hung candidate cannot answer the force RPC. Verify the recorded
        // process birth before closing the daemon and its kill-on-close Jobs.
        check_same_daemon(path, candidate)?;
        eliot_daemon::operator::stop_published_candidate(candidate).map_err(|error| {
            Failure::new(
                4,
                format!(
                    "candidate force RPC and process retirement did not settle: {}; {error}",
                    requested
                        .err()
                        .map_or_else(|| "exit timed out".to_owned(), |failure| failure.message)
                ),
            )
        })?;
        wait_for_exit(candidate, Duration::from_secs(5)).await
    }
    #[cfg(not(windows))]
    Err(requested
        .err()
        .unwrap_or_else(|| Failure::new(4, "replacement exit is unconfirmed")))
}

#[derive(Clone, Copy)]
enum Preparation {
    Open,
    Restarting,
    Exited,
    Unconfirmed,
}

async fn reconcile_status(path: &Path, old: &DaemonInfo) -> Preparation {
    match daemon_alive(old) {
        Ok(false) => return Preparation::Exited,
        Ok(true) if check_same_daemon(path, old).is_ok() => {}
        _ => return Preparation::Unconfirmed,
    }
    match status(path).await {
        Ok(status) if status.admission == "open" && !status.store_write_faulted => {
            Preparation::Open
        }
        Ok(status) if status.admission == "restarting" => Preparation::Restarting,
        _ => Preparation::Unconfirmed,
    }
}

async fn reconcile_preparation(
    path: &Path,
    old: &DaemonInfo,
    cancel: &DaemonShutdownRequest,
) -> Preparation {
    // An absent reply cannot distinguish a late restart commit from an uncommitted
    // preparation. Follow the same epoch through cancellation or actual exit.
    let deadline = Instant::now() + Duration::from_secs(125);
    loop {
        if check_same_daemon(path, old).is_ok() && call_shutdown(path, cancel).await.is_ok() {
            return Preparation::Open;
        }
        match reconcile_status(path, old).await {
            Preparation::Unconfirmed if Instant::now() < deadline => {}
            state => return state,
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The update command polls its existing restart preparation until settlement"
        )]
        sleep(Duration::from_millis(250)).await;
    }
}

async fn quiesce(
    path: &Path,
    old: &DaemonInfo,
    prepare: &DaemonShutdownRequest,
    commit: &DaemonShutdownRequest,
    wait: Duration,
    mut interrupt_on_deadline: bool,
    stopped: &mut Vec<StoppedAgent>,
) -> Result<(), Failure> {
    call_shutdown(path, prepare).await?;
    // Stop through the native protocol while terminal events can still commit.
    // A forced process exit can leave the retained session marked busy.
    Box::pin(stop_active_turns(
        path,
        old,
        Instant::now() + INTERRUPT_SETTLE_WAIT,
        stopped,
        |agent| HarnessCapabilities::of(&agent.harness).restart_requires_idle,
    ))
    .await?;
    let mut deadline = Instant::now() + wait;
    loop {
        check_same_daemon(path, old)?;
        match call_shutdown(path, commit).await {
            Ok(()) => return Ok(()),
            Err(error) if error.code.as_deref() == Some("restart_busy") => {
                if Instant::now() >= deadline {
                    if !std::mem::take(&mut interrupt_on_deadline) {
                        return Err(Failure::new(
                            5,
                            "admitted work did not finish before the update deadline",
                        ));
                    }
                    deadline = Instant::now() + INTERRUPT_SETTLE_WAIT;
                    Box::pin(stop_active_turns(path, old, deadline, stopped, |_| true)).await?;
                }
            }
            Err(error) => return Err(error),
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The update command waits for the replacement daemon's readiness"
        )]
        sleep(Duration::from_millis(200)).await;
    }
}

async fn call_shutdown(path: &Path, request: &DaemonShutdownRequest) -> Result<(), Failure> {
    let wait = Duration::from_secs(if request.backup { 155 } else { 5 });
    timeout(wait, shutdown_daemon(path, request))
        .await
        .map_err(|_| Failure::timeout())??;
    Ok(())
}

async fn stop_active_turns(
    path: &Path,
    old: &DaemonInfo,
    deadline: Instant,
    stopped: &mut Vec<StoppedAgent>,
    include: fn(&AgentSummary) -> bool,
) -> Result<(), Failure> {
    let summaries = timeout_at(deadline, agents(path))
        .await
        .map_err(|_| Failure::timeout())??;
    let mut active: Vec<_> = summaries
        .into_values()
        .filter(|agent| agent.active_turn.is_some() && include(agent))
        .collect();
    active.sort_by_key(|agent| agent.parent.is_some());
    let mut submitted = Vec::new();
    let mut failure = None;
    for summary in active {
        let target = async {
            let request = AgentGetRequest::new(&summary.id);
            let details: AgentGetResponse =
                eliot_daemon::client_call_typed(path, "agent.get", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&details)
                .map_err(Failure::invalid_response)?;
            let agent = details.agent;
            let Some(turn) = agent.active_turn.clone().filter(|turn| {
                !agent.observe_only
                    && agent.generation == summary.generation
                    && Some(turn) == summary.active_turn.as_ref()
            }) else {
                return Ok(None);
            };
            check_same_daemon(path, old)?;
            let mut request = submit_request(
                None,
                Some(agent.id.clone()),
                "stop",
                json!({"force": false}),
            )?;
            request.expected_generation = Some(agent.generation);
            request.expected_turn_id = Some(turn.clone());
            Ok(Some((
                StoppedAgent {
                    agent,
                    turn,
                    stop_id: request.id.clone(),
                },
                request,
            )))
        };
        let (candidate, request) = match timeout_at(deadline, target).await {
            Ok(Ok(Some(target))) => target,
            Ok(Ok(None)) => continue,
            Ok(Err(error)) => {
                failure = Some(error);
                break;
            }
            Err(_) => {
                failure = Some(Failure::timeout());
                break;
            }
        };
        match timeout_at(deadline, submit_command(path, &request)).await {
            Ok(Err(error)) if error.code.as_deref() == Some("stale_generation") => continue,
            Ok(Err(error)) if error.exit_code != 4 => {
                failure = Some(error);
                break;
            }
            _ => submitted.push(candidate),
        }
    }
    for candidate in submitted {
        match wait_for_command(path, &candidate.stop_id, deadline).await {
            Ok(command)
                if command.state == CommandState::Applied
                    && command.target.as_deref() == Some(candidate.agent.id.as_str())
                    && command.generation == Some(candidate.agent.generation)
                    && command.outcome.as_ref().is_some_and(|outcome| {
                        outcome["process_custody_exit_proven"] == true
                            && outcome["native_source_drain_proven"] == true
                    }) =>
            {
                let terminal =
                    if HarnessCapabilities::of(&candidate.agent.harness).restart_requires_idle {
                        let request = TurnGetRequest::new(
                            &candidate.agent.id,
                            candidate.agent.generation,
                            &candidate.turn,
                        );
                        match timeout_at(
                            deadline,
                            eliot_daemon::client_call_typed(path, "turn.get", &request),
                        )
                        .await
                        {
                            Ok(Ok(turn)) => {
                                request.validate_response(&turn).is_ok()
                                    && turn.state == eliot_core::TurnState::Completed
                            }
                            _ => false,
                        }
                    } else {
                        true
                    };
                if !terminal {
                    failure = Some(Failure::new(
                        4,
                        format!(
                            "Stop did not prove a terminal for {}#{} turn {} before replacement",
                            candidate.agent.id, candidate.agent.generation, candidate.turn,
                        ),
                    ));
                }
                stopped.push(candidate)
            }
            Ok(_) => {
                failure = Some(Failure::new(
                    4,
                    format!(
                        "stop {} did not prove exit and source drain for {}#{}",
                        candidate.stop_id, candidate.agent.id, candidate.agent.generation
                    ),
                ))
            }
            Err(error) if error.code.as_deref() == Some("stale_generation") => {}
            Err(error) => {
                failure = Some(Failure::new(
                    error.exit_code,
                    format!(
                        "stop {} for {}#{} did not settle: {}",
                        candidate.stop_id,
                        candidate.agent.id,
                        candidate.agent.generation,
                        error.message
                    ),
                ));
            }
        }
    }
    stopped.sort_by_key(|candidate| candidate.agent.depth);
    failure.map_or(Ok(()), Err)
}

fn stopped_result(stopped: &StoppedAgent, continuation: &str, error: Option<String>) -> Value {
    json!({"agent": stopped.agent.id, "generation": stopped.agent.generation,
        "turn": stopped.turn, "stop_id": stopped.stop_id, "continuation": continuation,
        "error": error})
}

fn hold_interrupted(stopped: &[StoppedAgent], reason: &str) -> (Vec<Value>, bool) {
    (
        stopped
            .iter()
            .map(|agent| stopped_result(agent, reason, None))
            .collect(),
        false,
    )
}

async fn resume_interrupted(path: &Path, stopped: &[StoppedAgent]) -> (Vec<Value>, bool) {
    if stopped.is_empty() {
        return (Vec::new(), false);
    }
    match continue_after_restart(path) {
        Ok(true) => {}
        Ok(false) => return hold_interrupted(stopped, "held by on_restart"),
        Err(error) => {
            return (
                stopped
                    .iter()
                    .map(|agent| stopped_result(agent, "hold", Some(error.to_string())))
                    .collect(),
                true,
            );
        }
    }
    let deadline = Instant::now() + RESUME_WAIT;
    let mut failed = BTreeSet::new();
    let mut results = Vec::new();
    for candidate in stopped {
        let resumed = if candidate
            .agent
            .parent
            .as_ref()
            .is_some_and(|parent| failed.contains(parent))
        {
            Err(Failure::new(1, "the interrupted parent did not resume"))
        } else {
            match timeout_at(deadline, resume_stopped(path, candidate, deadline)).await {
                Ok(result) => result,
                Err(_) => Err(Failure::timeout()),
            }
        };
        match resumed {
            Ok(()) => results.push(stopped_result(candidate, "resumed", None)),
            Err(error) => {
                failed.insert(candidate.agent.id.clone());
                results.push(stopped_result(
                    candidate,
                    "resume_unconfirmed",
                    Some(error.message),
                ));
            }
        }
    }
    (results, !failed.is_empty())
}

async fn resume_stopped(
    path: &Path,
    candidate: &StoppedAgent,
    deadline: Instant,
) -> Result<(), Failure> {
    let agent = &candidate.agent;
    // The applied Stop proves custody closed. A pre-restart native stop
    // carries a fresh continuation note for its captured turn.
    let request = AgentGetRequest::new(&agent.id);
    let details: AgentGetResponse = eliot_daemon::client_call_typed(path, "agent.get", &request)
        .await
        .map_err(Failure::for_read)?;
    request
        .validate_response(&details)
        .map_err(Failure::invalid_response)?;
    if details.agent.generation != agent.generation || details.agent.state != AgentState::Stopped {
        return Err(Failure::new(
            4,
            "the stopped generation changed before resume",
        ));
    }
    let args = if HarnessCapabilities::of(&agent.harness).restart_requires_idle {
        let body = format!(
            "The daemon update stopped {} while turn {} of generation {} was active. +             Read its retained outcome and reconcile the worktree (git status and git log on +             the task branch, plus the task card), then continue the task from the files as they are.",
            agent.id, candidate.turn, agent.generation,
        );
        json!({"text": eliot_core::swarm_note(&body, eliot_core::RESTART_PRINCIPAL)})
    } else {
        json!({})
    };
    let mut resume = submit_request(None, Some(agent.id.clone()), "resume", args)?;
    resume.expected_generation = Some(agent.generation);
    match submit_command(path, &resume).await {
        Err(error) if error.exit_code != 4 => return Err(error),
        _ => {}
    }
    wait_for_command(path, &resume.id, deadline).await?;
    Ok(())
}

async fn rollback(
    path: &Path,
    previous: &PreviousRelease<'_>,
    backup: &StoreBackup<'_>,
    store: RollbackStore<'_>,
    update_error: String,
    agents_at_update: UpdateAgents,
    json_output: bool,
) -> u8 {
    let mut store_restored = false;
    let retained_candidate = match store {
        RollbackStore::Published(candidate, schema) if schema != previous.store_schema => {
            Some(candidate.pid)
        }
        _ => None,
    };
    let restored = async {
        if binary_sha256(previous.binary).map_err(Failure::for_read)? != previous.sha256 {
            return Err(Failure::new(
                4,
                "the previous release binary changed on disk",
            ));
        }
        if let RollbackStore::Published(current, schema) = store {
            if schema != previous.store_schema {
                return Err(Failure::new(4, "the candidate published a newer Store schema and may still be serving; the current Store and backup are retained"));
            }
            retire_failed_candidate(path, current).await?;
        }
        // Do not race a surviving candidate or open a second writer.
        let offline = lock_offline(path).map_err(Failure::for_read)?;
        if matches!(store, RollbackStore::Unpublished) {
            restore_update_backup(path, backup.id, previous.daemon, &offline)
                .map_err(Failure::for_read)?;
            store_restored = true;
        }
        drop(offline);
        start_owned(path, previous.binary).await
    }
    .await;
    let (state, pid, rollback_error) = match restored {
        Ok(pid) => ("rolled_back", Some(pid), None),
        Err(error) => ("rollback_failed", None, Some(error.message)),
    };
    let cleanup = if pid.is_some() && !matches!(store, RollbackStore::Unpublished) {
        discard_update_backup(path, backup.id)
            .err()
            .map(|error| error.to_string())
    } else {
        None
    };
    let (interruptions, _) = if pid.is_some() {
        Box::pin(resume_interrupted(path, &agents_at_update.stopped)).await
    } else {
        hold_interrupted(&agents_at_update.stopped, "rollback unavailable")
    };
    print_update_value(
        &json!({
            "state": state, "previous_pid": previous.daemon.pid, "pid": pid, "update_error": update_error,
            "rollback_error": rollback_error, "binary": eliot_core::display_path(&previous.binary.to_string_lossy()),
            "sha256": previous.sha256, "agents_before": agents_at_update.before,
            "deadline_policy": agents_at_update.mode, "interrupted_agents": interruptions,
            "store_restored": store_restored, "backup": backup.retained(), "backup_cleanup_error": cleanup,
            "candidate_pid": retained_candidate,
            "agents_after": if pid.is_some() { observe_agents(path, &agents_at_update.before).await } else { Value::Null },
            "continuation": if retained_candidate.is_some() {
                "run swarm daemon update with a healthy release that supports the current Store schema"
            } else { "boot recovery applies the configured on_restart policy" },
        }),
        json_output,
    );
    if pid.is_some() { 1 } else { 4 }
}

async fn observe_agents(path: &Path, before: &[Value]) -> Value {
    match agents(path).await {
        Ok(current) => json!(before.iter().filter_map(|entry| {
            let observed = current.get(entry["agent"].as_str()?)?;
            Some(json!({"agent": observed.id, "generation": observed.generation,
                "state": observed.state, "last_turn": observed.last_turn_id, "last_outcome": observed.last_outcome}))
        }).collect::<Vec<_>>()),
        Err(error) => json!({"read_error": error.message}),
    }
}

fn release_binary(source: &Path) -> Result<PathBuf, Failure> {
    let source = if source.is_dir() {
        source.join(format!("swarm{}", std::env::consts::EXE_SUFFIX))
    } else {
        source.to_owned()
    };
    let path = source.canonicalize().map_err(|error| {
        Failure::new(
            2,
            format!("resolve release binary {}: {error}", source.display()),
        )
    })?;
    if !path.is_file() {
        return Err(Failure::new(
            2,
            format!("release binary is not a regular file: {}", path.display()),
        ));
    }
    Ok(path)
}

#[derive(serde::Deserialize)]
struct ReleaseInfo {
    version: String,
    store_schema: i64,
    #[serde(default)]
    backup_update: bool,
}

async fn preflight_store(root: &Path, binary: &Path) -> Result<(), Failure> {
    let id = mutation_id(None)?;
    let snapshot_root = root.to_owned();
    let snapshot_id = id.clone();
    let snapshot = tokio::task::spawn_blocking(move || {
        copy_store_for_update_check(&snapshot_root, &snapshot_id)
    })
    .await
    .map_err(|error| Failure::new(4, format!("Store copy worker failed: {error}")))?
    .map_err(Failure::for_read)?;
    let checked = run_store_check(binary, &snapshot).await;
    match (checked, discard_update_backup(root, &id)) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (result, Err(cleanup)) => Err(Failure::new(
            4,
            format!(
                "{} temporary Store copy cleanup failed: {cleanup}",
                result
                    .err()
                    .map_or_else(String::new, |error| format!("{};", error.message)),
            ),
        )),
    }
}

async fn run_store_check(binary: &Path, snapshot: &Path) -> Result<(), Failure> {
    let mut child = release_command(binary)
        .arg("--operator-check-store")
        .arg(snapshot)
        .stdout(Stdio::null())
        .spawn()
        .map_err(|error| {
            Failure::new(2, format!("start candidate Store migration check: {error}"))
        })?;
    match timeout(Duration::from_secs(120), child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(Failure::new(
            2,
            format!("candidate Store migration or readback failed ({status})"),
        )),
        Ok(Err(error)) => Err(Failure::new(
            2,
            format!("wait for candidate Store migration check: {error}"),
        )),
        Err(_) => {
            child.kill().await.map_err(|error| {
                Failure::new(
                    4,
                    format!("candidate Store check timed out and could not be stopped: {error}"),
                )
            })?;
            Err(Failure::new(
                5,
                "candidate Store migration check exceeded 120 seconds",
            ))
        }
    }
}

#[expect(
    clippy::disallowed_types,
    reason = "This helper owns the bounded release inspection subprocess."
)]
fn release_command(binary: &Path) -> tokio::process::Command {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs the owned release inspection process."
    )]
    let mut command = tokio::process::Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("ELIOT_") {
            command.env_remove(name);
        }
    }
    command.stdin(Stdio::null()).kill_on_drop(true);
    command
}

async fn release_info(binary: &Path) -> Result<ReleaseInfo, Failure> {
    let mut command = release_command(binary);
    command.arg("--operator-release-info");
    let output = timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| {
            Failure::new(
                2,
                "release binary did not report its update metadata within 5 seconds",
            )
        })?
        .map_err(|error| Failure::new(2, format!("read replacement binary version: {error}")))?;
    if !output.status.success() {
        return Err(Failure::new(
            2,
            format!(
                "release {} does not support reversible daemon updates",
                binary.display()
            ),
        ));
    }
    let info: ReleaseInfo = serde_json::from_slice(&output.stdout)
        .map_err(|_| Failure::new(2, "release metadata is invalid"))?;
    if info.store_schema <= 0
        || !info.version.starts_with("swarm ")
        || info.version.len() > 128
        || info.version.chars().any(char::is_control)
    {
        return Err(Failure::new(2, "release metadata is invalid"));
    }
    Ok(info)
}

#[cfg(windows)]
fn desktop_binary(binary: &Path) -> Result<Option<(PathBuf, String)>, Failure> {
    let desktop = binary.with_file_name("swarm-desktop.exe");
    if !desktop.is_file() {
        return Ok(None);
    }
    let hash = binary_sha256(&desktop).map_err(Failure::for_read)?;
    Ok(Some((desktop, hash)))
}

#[cfg(windows)]
fn start_companion(data_dir: &Path, desktop: Option<(PathBuf, String)>) -> (Option<Value>, bool) {
    let Some((binary, sha256)) = desktop else {
        return (
            Some(json!({"reason": "release contains no desktop companion"})),
            false,
        );
    };
    match eliot_daemon::operator::start_desktop(data_dir, &binary, sha256.clone()) {
        Ok(pid) => (Some(json!({"pid": pid, "sha256": sha256})), false),
        Err(error) => (Some(json!({"reason": error.to_string()})), true),
    }
}
