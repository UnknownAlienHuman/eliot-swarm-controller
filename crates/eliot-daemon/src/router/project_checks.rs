//! Debounced project formatting and fast checks for committed Git work facts.
//!
//! The project configuration is read from the owner's settings. Commands run
//! with the same minimal environment and owned-process custody as Goal checks.
//! Only a definite nonzero exit is delivered, bound to the exact generation
//! and turn that was active when the edit settled.

use super::{Principal, Router};
use crate::{
    common::now_ms,
    git_monitor::GitProbe,
    scripts::process::{
        ProcessCapture, ProcessFailure, ProcessOutcome, local_process_command, run_owned_process,
    },
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Agent, Error, Event, InjectArgs, InjectPoint, Result, SecretPolicy, SubmitCommand,
    api::{DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalDefinition},
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, sleep_until},
};

const EDIT_DEBOUNCE: Duration = Duration::from_millis(750);
const MAX_PENDING_CHECKS: usize = 256;
const MAX_CONCURRENT_CHECKS: usize = 4;
const MAX_REMEMBERED_CHECKS: usize = 512;
const MAX_FAILURE_CONTEXT_BYTES: usize = 6 * 1024;
const INJECT_EXPIRY_MS: i64 = 10 * 60 * 1000;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WorkKey {
    agent: String,
    generation: i64,
}

struct PendingEdit {
    seq: i64,
    deadline: Instant,
    workspace: String,
    git_only: bool,
}

#[derive(Default)]
struct CheckState {
    checked_seq: i64,
    fingerprint: Option<String>,
    uncertain: bool,
}

struct WorkCompletion {
    key: WorkKey,
    workspace: String,
    event_seq: i64,
    uncertain: bool,
    fingerprint: Option<String>,
}

struct CheckDriver {
    pending: HashMap<WorkKey, PendingEdit>,
    in_flight: HashSet<WorkKey>,
    in_flight_workspaces: HashSet<String>,
    check_states: HashMap<WorkKey, CheckState>,
    state_order: VecDeque<WorkKey>,
    fenced_all: bool,
    tasks: JoinSet<WorkCompletion>,
}

struct CheckDisposition {
    uncertain: bool,
    fingerprint: Option<String>,
}

struct ProjectWork {
    agent: Agent,
    config: crate::config::ProjectRegistrationConfig,
    check: Vec<String>,
    workspace: String,
}

/// Add the configured project check as a launch-time Goal for assigned and
/// role-routed work. An assignment's explicit Goal wins and manual goal.set
/// can replace this default through the ordinary Goal API.
pub(super) async fn attach_default_goal(
    router: &Router,
    request: &mut SubmitCommand,
    manager_assignment: bool,
) -> Result<()> {
    if request.verb != "launch"
        || request
            .args
            .get(eliot_core::PROJECT_CHECK_GOAL_ARG)
            .is_some()
        || request
            .args
            .get("assignment")
            .and_then(|assignment| assignment.get("goal"))
            .is_some_and(|goal| !goal.is_null())
    {
        return Ok(());
    }
    let Some(project_id) = request
        .args
        .get("registered_project")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return Ok(());
    };

    let registration = {
        let store = router.inner.store.lock().await;
        store.get_project_registration("owner", project_id)?
    };
    let Some(registration) = registration else {
        return Ok(());
    };
    let assigned_or_routed = manager_assignment
        || request.args.get("assignment").is_some()
        || request.args.get("profile_choice").is_some();
    if !assigned_or_routed
        && !registration
            .actions
            .iter()
            .any(|action| action == "git.publish")
    {
        return Ok(());
    }
    crate::automation::verify_project_registration(&router.inner.data_root, &registration).await?;
    let config = crate::config::load_project_registration_config(&router.inner.data_root)?
        .into_iter()
        .find(|project| project.id == project_id)
        .ok_or_else(|| Error::new("not_allowed", "project registration is no longer approved"))?;
    if config.check.is_empty() {
        return Ok(());
    }
    let max_rounds = config
        .max_rounds
        .ok_or_else(|| Error::new("source_integrity", "project check has no round bound"))?;
    let goal = GoalDefinition {
        text: "Complete the assigned project work and satisfy the configured project check."
            .to_owned(),
        check: config.check,
        check_cwd: None,
        check_timeout_ms: DEFAULT_GOAL_CHECK_TIMEOUT_MS,
        max_turns: None,
        max_rounds: Some(max_rounds),
        max_without_progress: None,
        cadence_ms: None,
        scope: Vec::new(),
        check_inputs: Vec::new(),
        max_cost: None,
        escalation: None,
    };
    goal.validate()?;
    goal.require_auto_bound()?;
    request.args[eliot_core::PROJECT_CHECK_GOAL_ARG] = serde_json::to_value(goal)
        .map_err(|_| Error::new("internal", "project check Goal could not be encoded"))?;
    Ok(())
}

/// Consume Git file-change hints and dirty-work snapshots, debounce per Agent generation,
/// and run at most four worktree check sequences at once.
pub(crate) async fn run_project_checks(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    router.note_worker_task_running("project_checks");
    let mut events = router.subscribe_rule_events();
    let mut driver = CheckDriver {
        pending: HashMap::new(),
        in_flight: HashSet::new(),
        in_flight_workspaces: HashSet::new(),
        check_states: HashMap::new(),
        state_order: VecDeque::new(),
        fenced_all: false,
        tasks: JoinSet::new(),
    };

    if *shutdown.borrow() {
        return Ok(());
    }
    loop {
        let deadline = if driver.tasks.len() >= MAX_CONCURRENT_CHECKS {
            Instant::now() + Duration::from_secs(24 * 60 * 60)
        } else {
            driver
                .pending
                .values()
                .map(|entry| entry.deadline)
                .min()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(24 * 60 * 60))
        };
        let timer = sleep_until(deadline);
        tokio::pin!(timer);

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    while driver.tasks.join_next().await.is_some() {}
                    return Ok(());
                }
            }
            received = events.recv() => match received {
                Ok(event) => schedule_edit(&router, event, &mut driver),
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    log_worker(&router, "daemon.project_check_lagged", None, &skipped.to_string());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("unavailable", "project-check event bus is closed"));
                }
            },
            Some(joined) = driver.tasks.join_next(), if !driver.tasks.is_empty() => match joined {
                Ok(completion) => {
                    driver.in_flight.remove(&completion.key);
                    driver.in_flight_workspaces.remove(&completion.workspace);
                    let state = driver.check_states.entry(completion.key.clone()).or_default();
                    state.checked_seq = state.checked_seq.max(completion.event_seq);
                    state.uncertain = completion.uncertain;
                    state.fingerprint = completion.fingerprint;
                    if completion.uncertain {
                        driver.pending.remove(&completion.key);
                    }
                    touch_state(&completion.key, &mut driver.state_order);
                    if driver.check_states.len() > MAX_REMEMBERED_CHECKS {
                        let overflow = trim_check_states(
                            &mut driver.check_states,
                            &mut driver.state_order,
                            &driver.pending,
                            &driver.in_flight,
                        );
                        if overflow && !driver.fenced_all {
                            log_worker(
                                &router,
                                "daemon.project_check_state_full",
                                Some(&completion.key),
                                "project-check state is full; new checks are fenced",
                            );
                        }
                        driver.fenced_all |= overflow;
                    }
                }
                Err(_) => {
                    return Err(Error::new("unknown", "project-check task ended unexpectedly"));
                }
            },
            _ = &mut timer, if driver.tasks.len() < MAX_CONCURRENT_CHECKS => {
                start_due_checks(&router, &shutdown, &mut driver);
            }
        }
    }
}

fn schedule_edit(router: &Router, event: Event, driver: &mut CheckDriver) {
    if event.source != "git" || !matches!(event.kind.as_str(), "file.changed" | "git.changed") {
        return;
    }
    let git_only = event.kind == "git.changed";
    if git_only
        && (!event
            .data
            .get("dirty")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|dirty| !dirty.is_empty())
            || event
                .data
                .get("view_digest")
                .and_then(serde_json::Value::as_str)
                .is_none_or(str::is_empty))
    {
        return;
    }
    let Some(workspace) = event
        .data
        .get("workspace")
        .and_then(serde_json::Value::as_str)
    else {
        return;
    };
    if event
        .subject
        .as_ref()
        .and_then(|subject| subject.workspace.as_deref())
        .is_some_and(|subject_workspace| subject_workspace != workspace)
        || driver.fenced_all
    {
        return;
    }
    let (Some(agent), Some(generation)) = (event.agent, event.generation) else {
        return;
    };
    let key = WorkKey { agent, generation };
    if driver
        .check_states
        .get(&key)
        .is_some_and(|state| state.uncertain)
    {
        return;
    }
    if !driver.pending.contains_key(&key) && driver.pending.len() >= MAX_PENDING_CHECKS {
        log_worker(
            router,
            "daemon.project_check_queue_full",
            Some(&key),
            "pending edit cap reached",
        );
        return;
    }
    let deadline = Instant::now() + EDIT_DEBOUNCE;
    driver
        .pending
        .entry(key)
        .and_modify(|existing| {
            existing.seq = existing.seq.max(event.seq);
            existing.deadline = deadline;
            existing.workspace = workspace.to_owned();
            existing.git_only &= git_only;
        })
        .or_insert(PendingEdit {
            seq: event.seq,
            deadline,
            workspace: workspace.to_owned(),
            git_only,
        });
}

fn touch_state(key: &WorkKey, order: &mut VecDeque<WorkKey>) {
    if let Some(index) = order.iter().position(|candidate| candidate == key) {
        order.remove(index);
    }
    order.push_back(key.clone());
}

fn trim_check_states(
    states: &mut HashMap<WorkKey, CheckState>,
    order: &mut VecDeque<WorkKey>,
    pending: &HashMap<WorkKey, PendingEdit>,
    in_flight: &HashSet<WorkKey>,
) -> bool {
    while states.len() > MAX_REMEMBERED_CHECKS {
        let Some(index) = order.iter().position(|key| {
            !pending.contains_key(key)
                && !in_flight.contains(key)
                && !states.get(key).is_some_and(|state| state.uncertain)
        }) else {
            return true;
        };
        if let Some(key) = order.remove(index) {
            states.remove(&key);
        }
    }
    false
}

fn start_due_checks(router: &Router, shutdown: &watch::Receiver<bool>, driver: &mut CheckDriver) {
    let now = Instant::now();
    let mut due = driver
        .pending
        .iter()
        .filter(|(_, entry)| entry.deadline <= now)
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    due.sort_by(|left, right| {
        (&left.agent, left.generation).cmp(&(&right.agent, right.generation))
    });

    for key in due {
        if driver.tasks.len() >= MAX_CONCURRENT_CHECKS {
            break;
        }
        if driver.fenced_all
            || driver.check_states.get(&key).is_some_and(|state| {
                state.uncertain
                    || driver
                        .pending
                        .get(&key)
                        .is_some_and(|edit| edit.seq <= state.checked_seq)
            })
        {
            driver.pending.remove(&key);
            continue;
        }
        let workspace_busy = driver
            .pending
            .get(&key)
            .is_some_and(|edit| driver.in_flight_workspaces.contains(&edit.workspace));
        if driver.in_flight.contains(&key) || workspace_busy {
            if let Some(edit) = driver.pending.get_mut(&key) {
                edit.deadline = now + EDIT_DEBOUNCE;
            }
            continue;
        }
        let Some(edit) = driver.pending.remove(&key) else {
            continue;
        };
        let prior_fingerprint = if edit.git_only {
            driver
                .check_states
                .get(&key)
                .and_then(|state| state.fingerprint.clone())
        } else {
            None
        };
        let task_router = router.clone();
        let task_shutdown = shutdown.clone();
        let task_key = key.clone();
        let task_workspace = edit.workspace.clone();
        driver.in_flight.insert(key);
        driver.in_flight_workspaces.insert(task_workspace.clone());
        driver.tasks.spawn(async move {
            let disposition = check_after_edit(
                &task_router,
                &task_key,
                edit.seq,
                &edit.workspace,
                edit.git_only,
                prior_fingerprint.as_deref(),
                task_shutdown,
            )
            .await;
            WorkCompletion {
                key: task_key,
                workspace: task_workspace,
                event_seq: edit.seq,
                uncertain: disposition.uncertain,
                fingerprint: disposition.fingerprint,
            }
        });
    }
}

async fn check_after_edit(
    router: &Router,
    key: &WorkKey,
    event_seq: i64,
    expected_workspace: &str,
    git_only: bool,
    prior_fingerprint: Option<&str>,
    shutdown: watch::Receiver<bool>,
) -> CheckDisposition {
    let work = match load_project_work(router, key, expected_workspace).await {
        Ok(Some(work)) => work,
        Ok(None) => {
            return CheckDisposition {
                uncertain: false,
                fingerprint: None,
            };
        }
        Err(error) => {
            log_error(
                router,
                "daemon.project_check_workspace_refused",
                key,
                event_seq,
                &error,
            );
            return CheckDisposition {
                uncertain: false,
                fingerprint: None,
            };
        }
    };
    let before = match work_fingerprint(&work.workspace).await {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            log_error(
                router,
                "daemon.project_check_fingerprint_failed",
                key,
                event_seq,
                &error,
            );
            return CheckDisposition {
                uncertain: false,
                fingerprint: None,
            };
        }
    };
    if git_only && prior_fingerprint == Some(before.as_str()) {
        return CheckDisposition {
            uncertain: false,
            fingerprint: Some(before),
        };
    }

    let policy = router.secret_policy_snapshot();
    let active_turn = work.agent.active_turn.clone();
    let mut failure = None;
    let mut checked_fingerprint = None;
    let mut ran_command = false;
    if !work.config.format.is_empty() {
        ran_command = true;
        match run_argv(
            &work.config.format,
            &work.workspace,
            Arc::clone(&policy),
            shutdown.clone(),
        )
        .await
        {
            Ok(CheckOutcome::Passed) => {
                checked_fingerprint = work_fingerprint(&work.workspace).await.ok();
            }
            Ok(CheckOutcome::Failed { exit_code, output }) => {
                failure = Some(format_failure("formatter", exit_code, &output, &policy));
                checked_fingerprint = work_fingerprint(&work.workspace).await.ok();
            }
            Ok(CheckOutcome::Unknown(reason)) => {
                log_worker(
                    router,
                    "daemon.project_formatter_unknown",
                    Some(key),
                    &reason,
                );
                return CheckDisposition {
                    uncertain: true,
                    fingerprint: None,
                };
            }
            Err(error) => {
                log_error(
                    router,
                    "daemon.project_formatter_unavailable",
                    key,
                    event_seq,
                    &error,
                );
                return CheckDisposition {
                    uncertain: false,
                    fingerprint: None,
                };
            }
        }
    }
    if failure.is_none() && !work.check.is_empty() {
        let check_input = work_fingerprint(&work.workspace).await.ok();
        ran_command = true;
        match run_argv(&work.check, &work.workspace, Arc::clone(&policy), shutdown).await {
            Ok(CheckOutcome::Passed) => {
                let after = work_fingerprint(&work.workspace).await.ok();
                checked_fingerprint = (check_input == after).then_some(after).flatten();
            }
            Ok(CheckOutcome::Failed { exit_code, output }) => {
                failure = Some(format_failure("fast check", exit_code, &output, &policy));
                let after = work_fingerprint(&work.workspace).await.ok();
                checked_fingerprint = (check_input == after).then_some(after).flatten();
            }
            Ok(CheckOutcome::Unknown(reason)) => {
                log_worker(router, "daemon.project_check_unknown", Some(key), &reason);
                return CheckDisposition {
                    uncertain: true,
                    fingerprint: None,
                };
            }
            Err(error) => {
                log_error(
                    router,
                    "daemon.project_check_unavailable",
                    key,
                    event_seq,
                    &error,
                );
                return CheckDisposition {
                    uncertain: false,
                    fingerprint: checked_fingerprint,
                };
            }
        }
    }
    if let Some(failure) = failure {
        if let Some(turn_id) = active_turn.as_deref() {
            if let Err(error) =
                deliver_failure(router, key, turn_id, &work.agent.harness, failure).await
            {
                log_error(
                    router,
                    "daemon.project_check_delivery_failed",
                    key,
                    event_seq,
                    &error,
                );
            }
        } else {
            log_worker(
                router,
                "daemon.project_check_failure_no_active_turn",
                Some(key),
                "failure awaits the next Goal continuation or an exact active turn",
            );
        }
    }

    if ran_command && checked_fingerprint.is_none() {
        log_worker(
            router,
            "daemon.project_check_fingerprint_unknown",
            Some(key),
            "the checked worktree fingerprint could not be confirmed",
        );
        return CheckDisposition {
            uncertain: true,
            fingerprint: None,
        };
    }
    CheckDisposition {
        uncertain: false,
        fingerprint: checked_fingerprint,
    }
}

async fn work_fingerprint(workspace: &str) -> Result<String> {
    GitProbe::new()?.work_fingerprint(workspace).await
}

async fn load_project_work(
    router: &Router,
    key: &WorkKey,
    expected_workspace: &str,
) -> Result<Option<ProjectWork>> {
    let (agent, registration) = {
        let store = router.inner.store.lock().await;
        let Some(agent) = store.get_agent(&key.agent)? else {
            return Ok(None);
        };
        if agent.generation != key.generation
            || agent.is_terminal()
            || agent.observe_only
            || agent.branch.is_none()
            || agent.parent.is_some()
            || agent.workspace.as_deref() != Some(expected_workspace)
        {
            return Ok(None);
        }
        let Some(registration) = store.get_project_registration("owner", &agent.project)? else {
            return Ok(None);
        };
        (agent, registration)
    };
    let config = crate::config::load_project_registration_config(&router.inner.data_root)?
        .into_iter()
        .find(|project| project.id == registration.id)
        .ok_or_else(|| Error::new("not_allowed", "project registration is no longer approved"))?;
    crate::automation::verify_project_registration(&router.inner.data_root, &registration).await?;
    let configured_argv = serde_json::json!([&config.format, &config.check]);
    if router
        .secret_policy_snapshot()
        .contains_value(&configured_argv)
    {
        return Err(Error::new(
            "secret_in_config",
            "project check argv contains a known credential",
        ));
    }
    let check = if config.check_at_turn_end {
        Vec::new()
    } else {
        config.check.clone()
    };
    if config.format.is_empty() && check.is_empty() {
        return Ok(None);
    }
    let Some(workspace) = agent.workspace.as_deref() else {
        return Ok(None);
    };
    crate::git_monitor::reject_links(Path::new(workspace))?;
    let canonical = std::fs::canonicalize(workspace)
        .map_err(|_| Error::new("unavailable", "Agent worktree is unavailable"))?;
    let canonical = canonical
        .to_str()
        .ok_or_else(|| Error::new("invalid", "Agent worktree path is not valid UTF-8"))?
        .to_owned();
    let (checkout, common_dir) = WorkspaceGit::new()?.repository_identity(&canonical).await?;
    if checkout != canonical || common_dir != registration.repo_common_dir {
        return Err(Error::new(
            "not_allowed",
            "Agent worktree does not match the registered project",
        ));
    }
    Ok(Some(ProjectWork {
        agent,
        config,
        check,
        workspace: canonical,
    }))
}

enum CheckOutcome {
    Passed,
    Failed { exit_code: i32, output: String },
    Unknown(String),
}

async fn run_argv(
    argv: &[String],
    workspace: &str,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Result<CheckOutcome> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| Error::new("source_integrity", "project command argv is empty"))?;
    let mut command = local_process_command(program)?;
    command.args(args).current_dir(workspace);
    let outcome = run_owned_process(
        command,
        Vec::new(),
        Duration::from_millis(DEFAULT_GOAL_CHECK_TIMEOUT_MS as u64),
        policy.clone(),
        cancellation,
    )
    .await;
    Ok(classify_outcome(outcome, &policy))
}

fn classify_outcome(outcome: ProcessOutcome, policy: &SecretPolicy) -> CheckOutcome {
    match outcome {
        ProcessOutcome::Completed(capture) => classify_capture(capture, policy),
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        } => classify_capture(capture, policy),
        ProcessOutcome::Failed { reason, .. } => {
            CheckOutcome::Unknown(format!("process ended without a proven exit: {reason:?}"))
        }
        ProcessOutcome::Unknown { reason, .. } => {
            CheckOutcome::Unknown(format!("process outcome is unknown: {reason:?}"))
        }
    }
}

fn classify_capture(capture: ProcessCapture, policy: &SecretPolicy) -> CheckOutcome {
    if !custody_proven_for_capture(&capture) {
        return CheckOutcome::Unknown("process custody is unproven".to_owned());
    }
    match capture.status.code() {
        Some(0) if capture.status.success() => CheckOutcome::Passed,
        Some(exit_code) => CheckOutcome::Failed {
            exit_code,
            output: captured_output(&capture, policy),
        },
        None => CheckOutcome::Unknown("process exited without a code".to_owned()),
    }
}

fn captured_output(capture: &ProcessCapture, policy: &SecretPolicy) -> String {
    let stdout = policy.redact_text(&String::from_utf8_lossy(&capture.stdout));
    let stderr = policy.redact_text(&capture.stderr.text);
    let content = match (stdout.trim().is_empty(), stderr.trim().is_empty()) {
        (true, true) => "No diagnostic output was captured.".to_owned(),
        (false, true) => format!("[stdout]\n{stdout}"),
        (true, false) => format!("[stderr]\n{stderr}"),
        (false, false) => format!("[stdout]\n{stdout}\n[stderr]\n{stderr}"),
    };
    let (bounded, _) = eliot_core::api::truncate_bytes(&content, MAX_FAILURE_CONTEXT_BYTES);
    bounded
}

fn format_failure(kind: &str, exit_code: i32, output: &str, policy: &SecretPolicy) -> String {
    let message = policy.redact_text(&format!(
        "Project {kind} failed with exit code {exit_code}.\n{output}"
    ));
    let (message, _) = eliot_core::api::truncate_bytes(&message, MAX_FAILURE_CONTEXT_BYTES);
    message
}

async fn deliver_failure(
    router: &Router,
    key: &WorkKey,
    turn_id: &str,
    harness: &str,
    text: String,
) -> Result<()> {
    let still_current = {
        let store = router.inner.store.lock().await;
        store.get_agent(&key.agent)?.is_some_and(|agent| {
            agent.generation == key.generation && agent.active_turn.as_deref() == Some(turn_id)
        })
    };
    if !still_current {
        return Err(Error::new(
            "stale_generation",
            "project check failure no longer matches the active turn",
        ));
    }
    let point = match harness {
        "claude" | "opencode" | "antigravity" => InjectPoint::TurnSafePoint,
        _ => InjectPoint::ActiveTurn,
    };
    let request = SubmitCommand {
        id: ulid::Ulid::new().to_string(),
        target: Some(key.agent.clone()),
        selector: None,
        verb: "inject".to_owned(),
        args: serde_json::to_value(InjectArgs {
            text,
            point,
            expires_at_ms: Some(now_ms().saturating_add(INJECT_EXPIRY_MS)),
        })
        .map_err(|_| Error::new("internal", "project check context could not be encoded"))?,
        expected_generation: Some(key.generation),
        expected_turn_id: Some(turn_id.to_owned()),
        request_id: None,
        probe: false,
    };
    router
        .submit(
            &serde_json::to_value(request).map_err(|_| {
                Error::new("internal", "project check command could not be encoded")
            })?,
            &Principal::Owner,
        )
        .await?;
    Ok(())
}

fn log_worker(router: &Router, code: &str, key: Option<&WorkKey>, detail: &str) {
    let policy = router.secret_policy_snapshot();
    let agent = key.map_or("", |key| key.agent.as_str());
    let generation = key.map_or_else(String::new, |key| key.generation.to_string());
    crate::logging::record(
        &policy,
        code,
        &[("agent", agent), ("generation", generation.as_str())],
        Some(&Error::new("unavailable", detail)),
    );
}

fn log_error(router: &Router, code: &str, key: &WorkKey, event_seq: i64, error: &Error) {
    let policy = router.secret_policy_snapshot();
    let generation = key.generation.to_string();
    let event_seq = event_seq.to_string();
    crate::logging::record(
        &policy,
        code,
        &[
            ("agent", key.agent.as_str()),
            ("generation", generation.as_str()),
            ("event_seq", event_seq.as_str()),
        ],
        Some(error),
    );
}

#[cfg(windows)]
fn custody_proven_for_capture(_: &ProcessCapture) -> bool {
    true
}

#[cfg(unix)]
fn custody_proven_for_capture(capture: &ProcessCapture) -> bool {
    !matches!(capture.custody, crate::platform::CustodyGuarantee::Unknown)
}
