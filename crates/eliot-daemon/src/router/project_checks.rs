//! Debounced project formatting and fast checks for committed Git work facts.
//!
//! The project configuration is read from the owner's settings. Commands run
//! with the same minimal environment and owned-process custody as Goal checks.
//! Only a definite nonzero exit is delivered, bound to the exact generation
//! and the context consumer available when the check finishes.

use super::Router;
use crate::{
    common::now_ms,
    git_monitor::GitProbe,
    scripts::process::{
        ProcessCapture, ProcessFailure, ProcessOutcome, local_process_command, run_owned_process,
    },
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Error, Event, Result, SecretPolicy, SubmitCommand,
    api::{DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalDefinition},
};
use eliot_plugin::sync::LockExt;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    task::JoinSet,
    time::{Instant, sleep_until},
};

const EDIT_DEBOUNCE: Duration = Duration::from_millis(750);
const MAX_PENDING_CHECKS: usize = 256;
const MAX_CONCURRENT_CHECKS: usize = 4;
const MAX_FAILURE_CONTEXT_BYTES: usize = 6 * 1024;
const REAP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WorkKey {
    agent: String,
    generation: i64,
}

struct PendingEdit {
    seq: i64,
    deadline: Instant,
    workspace: String,
}

#[derive(Default)]
struct CheckState {
    checked_seq: i64,
    fingerprint: Option<String>,
    uncertain: bool,
}

struct WorkCompletion {
    key: WorkKey,
    event_seq: i64,
    uncertain: bool,
    fingerprint: Option<String>,
}

struct CheckDriver {
    pending: HashMap<WorkKey, PendingEdit>,
    in_flight: HashSet<WorkKey>,
    check_states: HashMap<WorkKey, CheckState>,
    tasks: JoinSet<WorkCompletion>,
}

struct CheckDisposition {
    uncertain: bool,
    fingerprint: Option<String>,
}

struct ProjectWork {
    config: crate::config::ProjectRegistrationConfig,
    check: Vec<String>,
    workspace: String,
}

impl Router {
    pub(super) fn workspace_check_lock(&self, workspace: &str) -> Arc<Mutex<()>> {
        let path = eliot_core::display_path(workspace);
        let key = if cfg!(windows) {
            path.replace('/', "\\").to_ascii_lowercase()
        } else {
            path.to_string()
        };
        let key = crate::daemon::normalize_identity_data_dir(Path::new(&key));
        let mut locks = self.inner.workspace_check_locks.lock_recover();
        if let Some(lock) = locks.get(&key).and_then(std::sync::Weak::upgrade) {
            return lock;
        }
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key, Arc::downgrade(&lock));
        lock
    }
}

pub(super) async fn wait_for_check_shutdown(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stopped| *stopped).await;
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
        check_states: HashMap::new(),
        tasks: JoinSet::new(),
    };
    let mut reap = tokio::time::interval(REAP_INTERVAL);
    reap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                    if !is_live_generation(&*router.inner.store.lock().await, &completion.key) {
                        driver.pending.remove(&completion.key);
                        continue;
                    }
                    let state = driver.check_states.entry(completion.key.clone()).or_default();
                    state.checked_seq = state.checked_seq.max(completion.event_seq);
                    state.uncertain = completion.uncertain;
                    state.fingerprint = completion.fingerprint;
                    if completion.uncertain {
                        driver.pending.remove(&completion.key);
                    }
                }
                Err(_) => {
                    return Err(Error::new("unknown", "project-check task ended unexpectedly"));
                }
            },
            _ = reap.tick() => {
                let store = router.inner.store.lock().await;
                driver.check_states.retain(|key, _| is_live_generation(&store, key));
            },
            _ = &mut timer, if driver.tasks.len() < MAX_CONCURRENT_CHECKS => {
                start_due_checks(&router, &shutdown, &mut driver);
            }
        }
    }
}

fn schedule_edit(router: &Router, event: Event, driver: &mut CheckDriver) {
    if let (Some(agent), Some(generation), Some(subject)) =
        (&event.agent, event.generation, &event.subject)
        && (subject.state.is_terminal() || event.kind == "session.ready")
    {
        let retained = |key: &WorkKey| {
            &key.agent != agent
                || key.generation > generation
                || (key.generation == generation && !subject.state.is_terminal())
        };
        driver.check_states.retain(|key, _| retained(key));
        driver.pending.retain(|key, _| retained(key));
    }
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
        })
        .or_insert(PendingEdit {
            seq: event.seq,
            deadline,
            workspace: workspace.to_owned(),
        });
}

fn is_live_generation(store: &eliot_store::Store, key: &WorkKey) -> bool {
    match store.get_agent(&key.agent) {
        Ok(agent) => {
            agent.is_some_and(|agent| agent.generation == key.generation && !agent.is_terminal())
        }
        Err(_) => true,
    }
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
        if driver.check_states.get(&key).is_some_and(|state| {
            state.uncertain
                || driver
                    .pending
                    .get(&key)
                    .is_some_and(|edit| edit.seq <= state.checked_seq)
        }) {
            driver.pending.remove(&key);
            continue;
        }
        if driver.in_flight.contains(&key) {
            if let Some(edit) = driver.pending.get_mut(&key) {
                edit.deadline = now + EDIT_DEBOUNCE;
            }
            continue;
        }
        let Some(edit) = driver.pending.remove(&key) else {
            continue;
        };
        let prior_fingerprint = driver
            .check_states
            .get(&key)
            .and_then(|state| state.fingerprint.clone());
        let task_router = router.clone();
        let task_shutdown = shutdown.clone();
        let task_key = key.clone();
        driver.in_flight.insert(key);
        driver.tasks.spawn(async move {
            let disposition = check_after_edit(
                &task_router,
                &task_key,
                edit.seq,
                &edit.workspace,
                prior_fingerprint.as_deref(),
                task_shutdown,
            )
            .await;
            WorkCompletion {
                key: task_key,
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
    prior_fingerprint: Option<&str>,
    mut shutdown: watch::Receiver<bool>,
) -> CheckDisposition {
    let lock = router.workspace_check_lock(expected_workspace);
    let _permit = tokio::select! {
        biased;
        _ = wait_for_check_shutdown(&mut shutdown) => {
            return CheckDisposition { uncertain: false, fingerprint: None };
        }
        permit = lock.lock_owned() => permit,
    };
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
    if prior_fingerprint == Some(before.as_str()) {
        return CheckDisposition {
            uncertain: false,
            fingerprint: Some(before),
        };
    }

    let policy = router.secret_policy_snapshot();
    let mut failure = None;
    let mut checked_fingerprint = None;
    if !work.config.format.is_empty() {
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
        match run_argv(&work.check, &work.workspace, Arc::clone(&policy), shutdown).await {
            Ok(outcome @ (CheckOutcome::Passed | CheckOutcome::Failed { .. })) => {
                // Remember the worktree left by this invocation, including
                // any reports the checker wrote. Its own Git hints can then
                // be suppressed just as formatter hints are.
                checked_fingerprint = work_fingerprint(&work.workspace).await.ok();
                if let CheckOutcome::Failed { exit_code, output } = outcome {
                    failure = Some(format_failure("fast check", exit_code, &output, &policy));
                }
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
    if let Err(error) = update_failure_context(router, key, failure).await {
        log_error(
            router,
            "daemon.project_check_delivery_failed",
            key,
            event_seq,
            &error,
        );
    }

    let uncertain = checked_fingerprint.is_none();
    if uncertain {
        log_worker(
            router,
            "daemon.project_check_fingerprint_unknown",
            Some(key),
            "the worktree after the project commands could not be verified",
        );
    }
    CheckDisposition {
        uncertain,
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
    classify_outcome(outcome, &policy)
}

fn classify_outcome(outcome: ProcessOutcome, policy: &SecretPolicy) -> Result<CheckOutcome> {
    Ok(match outcome {
        ProcessOutcome::NotSpawned(error) | ProcessOutcome::ProgramUnrunnable(error) => {
            return Err(error);
        }
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
    })
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

async fn update_failure_context(
    router: &Router,
    key: &WorkKey,
    failure: Option<String>,
) -> Result<()> {
    router.check_command_admission_mode("inject")?;
    let mut store = router.inner.store.lock().await;
    let change = store.project_check_context(&key.agent, key.generation, failure, now_ms())?;
    router.publish_locked(&change.events);
    drop(store);
    if let Some(command_id) = change.value {
        router.schedule_command(command_id, None);
    }
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
