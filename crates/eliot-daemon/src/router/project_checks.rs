//! Debounced project formatting and fast checks for committed Git work facts.
//!
//! The project configuration is read from the owner's settings. Commands run
//! with the same minimal environment and owned-process custody as Goal checks.
//! Only a definite nonzero exit is delivered, bound to the exact generation
//! and the context consumer available when the check finishes.

use super::{Principal, Router};
use crate::{
    common::now_ms,
    git_monitor::GitProbe,
    scripts::process::{
        ProcessCapture, ProcessFailure, ProcessOutcome, TASK_BASE_ENV, local_process_command,
        run_owned_process,
    },
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Error, Event, Result, SecretPolicy, SubmitCommand,
    api::{DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalCostLimit, GoalDefinition, GoalStatus},
};
use eliot_plugin::sync::LockExt;
use serde_json::{Map, Value, json};
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
    review: Option<Event>,
}

#[derive(Default)]
struct CheckState {
    checked_seq: i64,
    fingerprint: Option<String>,
    uncertain: bool,
    passed: Option<ProjectCheckPass>,
}

struct WorkCompletion {
    key: WorkKey,
    event_seq: i64,
    uncertain: bool,
    fingerprint: Option<String>,
    passed: Option<ProjectCheckPass>,
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
    passed: Option<ProjectCheckPass>,
}

struct ProjectWork {
    config: crate::config::ProjectRegistrationConfig,
    check: Option<ProjectWorkCheck>,
    workspace: String,
    task_base: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
struct ProjectWorkCheck {
    argv: Vec<String>,
    cwd: Option<String>,
    deadline_ms: i64,
}

#[derive(Clone)]
struct ProjectCheckPass {
    check: ProjectWorkCheck,
    fingerprint: String,
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

/// Resolve policy after project selection and before transactional launch admission.
/// Keep the caller's fields intact for replay and retain the resolved Goal beside them.
pub(super) async fn freeze_launch_goal(
    router: &Router,
    request: &mut SubmitCommand,
    principal: &Principal,
) -> Result<()> {
    let owner = matches!(principal, Principal::Owner);
    validate_launch_inputs(&request.args, owner)?;
    let explicit = request
        .args
        .get("goal")
        .or_else(|| request.args.pointer("/assignment/goal"))
        .and_then(Value::as_object);
    let mut defaults = Map::new();
    let mut retry_approval_identity: Option<(Vec<String>, Option<String>)> = None;
    let project_id = request
        .args
        .get("registered_project")
        .filter(|_| request.args.get("chat") != Some(&Value::Bool(true)))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let opted_out = request.args.get("no_policy").is_some();
    if opted_out && project_id.is_none() {
        return Err(Error::new(
            "invalid",
            "project policy opt-out requires a registered project",
        ));
    }
    if !opted_out && let Some(project_id) = project_id {
        let registration = router
            .inner
            .store
            .lock()
            .await
            .get_project_registration("owner", project_id)?;
        if let Some(registration) = registration {
            let config = crate::config::load_project_registration_config(&router.inner.data_root)?
                .into_iter()
                .find(|project| project.id == project_id)
                .ok_or_else(|| {
                    Error::new("not_allowed", "project registration is no longer approved")
                })?;
            if let Some(policy) = config.policy
                && let Some(check) = policy.check
            {
                crate::automation::verify_project_registration(
                    &router.inner.data_root,
                    &registration,
                )
                .await?;
                let approved_argv = check.argv.clone();
                defaults.insert("check".to_owned(), json!(check.argv));
                if let Some(deadline_ms) = check.deadline_ms {
                    defaults.insert("check_timeout_ms".to_owned(), json!(deadline_ms));
                }
                if check.check_timeout_retry_safe {
                    defaults.insert("check_timeout_retry_safe".to_owned(), json!(true));
                    retry_approval_identity = Some((approved_argv, None));
                }
                if let Some(max_rounds) = policy.budget.max_rounds {
                    defaults.insert("max_rounds".to_owned(), json!(max_rounds));
                }
                if let Some(max_cost) = policy.budget.max_cost {
                    defaults.insert("max_cost".to_owned(), json!(max_cost));
                }
                if !policy.scope.is_empty() {
                    defaults.insert("scope".to_owned(), json!(policy.scope));
                }
                if !policy.check_inputs.is_empty() {
                    defaults.insert("check_inputs".to_owned(), json!(policy.check_inputs));
                }
            }
        }
    }

    if defaults.is_empty() && explicit.is_none() {
        return Ok(());
    }
    let policy_applied = defaults
        .keys()
        .any(|field| explicit.is_none_or(|goal| !goal.contains_key(field)));
    if let Some(explicit) = explicit {
        if owner || defaults.is_empty() {
            defaults.extend(explicit.clone());
        } else {
            merge_tighter_goal_fields(&mut defaults, explicit)?;
        }
    }
    if retry_approval_identity.is_some_and(|(approved_argv, approved_cwd)| {
        defaults.get("check") != Some(&json!(approved_argv))
            || defaults.get("check_cwd").and_then(Value::as_str) != approved_cwd.as_deref()
    }) && !(owner
        && explicit.is_some_and(|goal| {
            goal.get("check_timeout_retry_safe")
                .and_then(Value::as_bool)
                == Some(true)
        }))
    {
        defaults.remove("check_timeout_retry_safe");
    }
    defaults
        .entry("text")
        .or_insert_with(|| json!("Complete the assigned project work and satisfy the Goal check."));
    let goal: GoalDefinition = serde_json::from_value(Value::Object(defaults))
        .map_err(|error| Error::new("invalid", format!("invalid launch Goal: {error}")))?;
    goal.validate()?;
    goal.require_auto_bound()?;
    request.args[eliot_core::PROJECT_CHECK_GOAL_ARG] = serde_json::to_value(goal)
        .map_err(|_| Error::new("internal", "launch Goal could not be encoded"))?;
    if policy_applied {
        request.args[eliot_core::PROJECT_POLICY_APPLIED_ARG] = Value::Bool(true);
    }
    Ok(())
}

fn merge_tighter_goal_fields(
    defaults: &mut Map<String, Value>,
    explicit: &Map<String, Value>,
) -> Result<()> {
    for (field, value) in explicit {
        let refusal = || {
            Error::new(
                "not_allowed",
                format!(
                    "launch Goal {field} must tighten project policy; use no_policy with a reason to deviate"
                ),
            )
        };
        match field.as_str() {
            "max_rounds" | "check_timeout_ms" => {
                let requested = value
                    .as_i64()
                    .filter(|limit| *limit > 0)
                    .ok_or_else(refusal)?;
                let baseline = defaults
                    .get(field)
                    .and_then(Value::as_i64)
                    .or((field == "check_timeout_ms").then_some(DEFAULT_GOAL_CHECK_TIMEOUT_MS));
                if baseline.is_some_and(|limit| requested > limit) {
                    return Err(refusal());
                }
            }
            "check" | "check_cwd" if defaults.get(field).unwrap_or(&Value::Null) != value => {
                return Err(refusal());
            }
            "check_timeout_retry_safe" => {
                let requested = value.as_bool().ok_or_else(refusal)?;
                if requested && defaults.get(field).and_then(Value::as_bool) != Some(true) {
                    return Err(refusal());
                }
            }
            "max_cost" => {
                let requested: GoalCostLimit =
                    serde_json::from_value(value.clone()).map_err(|_| refusal())?;
                requested.validate()?;
                if let Some(inherited) = defaults.get(field) {
                    let inherited: GoalCostLimit = serde_json::from_value(inherited.clone())
                        .map_err(|_| Error::new("internal", "invalid project policy cost limit"))?;
                    if requested.unit != inherited.unit
                        || requested.amount_micros > inherited.amount_micros
                    {
                        return Err(refusal());
                    }
                }
            }
            "scope" | "check_inputs" => {
                let requested = value
                    .as_array()
                    .filter(|globs| !globs.is_empty())
                    .ok_or_else(refusal)?;
                if let Some(inherited) = defaults.remove(field) {
                    if field == "scope" {
                        // Each scope is a union of globs. Require membership in both
                        // unions so adding broad globs cannot expand the allowed paths.
                        defaults.insert("policy_scope".to_owned(), inherited);
                    } else if let Value::Array(mut globs) = inherited {
                        for glob in requested {
                            if !globs.contains(glob) {
                                globs.push(glob.clone());
                            }
                        }
                        defaults.insert(field.clone(), Value::Array(globs));
                        continue;
                    }
                }
            }
            "cadence_ms" if !value.is_null() => {
                // Cadenced Goals only run on explicit slots and lose idle driving.
                return Err(refusal());
            }
            _ => {}
        }
        defaults.insert(field.clone(), value.clone());
    }
    Ok(())
}

pub(super) fn validate_launch_inputs(args: &Value, owner: bool) -> Result<()> {
    if let Some(reason) = args.get("no_policy")
        && !reason
            .as_str()
            .is_some_and(eliot_core::api::valid_project_policy_opt_out_reason)
    {
        return Err(Error::invalid_field(
            "args.no_policy",
            format!(
                "nonblank reason of at most {} bytes without control or invisible format characters",
                eliot_core::api::PROJECT_POLICY_OPT_OUT_REASON_MAX_BYTES
            ),
        ));
    }
    if args.get("no_policy").is_some() && args.get("chat") == Some(&Value::Bool(true)) {
        return Err(Error::new(
            "invalid",
            "private chats have no project policy to opt out of",
        ));
    }
    if !owner
        && args
            .get("goal")
            .or_else(|| args.pointer("/assignment/goal"))
            .and_then(Value::as_object)
            .and_then(|goal| goal.get("check_timeout_retry_safe"))
            .and_then(Value::as_bool)
            == Some(true)
    {
        return Err(Error::new(
            "not_allowed",
            "only the Owner can approve a Goal check timeout retry",
        ));
    }
    if let Some(goal) = args.get("goal")
        && (!goal.is_object() || args.pointer("/assignment/goal").is_some())
    {
        return Err(Error::invalid_field(
            "args.goal",
            "Goal object, supplied at only one location",
        ));
    }
    if !owner
        && let Some(goal) = args
            .get("goal")
            .or_else(|| args.pointer("/assignment/goal"))
            .and_then(Value::as_object)
    {
        if goal.contains_key("policy_scope") {
            return Err(Error::new(
                "invalid",
                "launch Goal policy_scope is resolved by the daemon",
            ));
        }
        if ["check", "check_cwd", "escalation"]
            .iter()
            .any(|field| goal.contains_key(*field))
        {
            return Err(Error::new(
                "not_allowed",
                "only the Owner can set launch Goal check, check_cwd or escalation",
            ));
        }
    }
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
                Ok(event) => schedule_edit(&router, event, &mut driver).await,
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
                    state.passed = completion.passed;
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

async fn schedule_edit(router: &Router, event: Event, driver: &mut CheckDriver) {
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
    let review = wants_checked_review(router, &event).await;
    if !review
        && (event.source != "git" || !matches!(event.kind.as_str(), "file.changed" | "git.changed"))
    {
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
    let workspace = if review {
        event
            .subject
            .as_ref()
            .and_then(|subject| subject.workspace.as_deref())
    } else {
        event.data["workspace"].as_str()
    };
    let Some(workspace) = workspace.map(str::to_owned) else {
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
    let (Some(agent), Some(generation)) = (event.agent.clone(), event.generation) else {
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
    let pending = driver.pending.entry(key).or_insert_with(|| PendingEdit {
        seq: event.seq,
        deadline,
        workspace: workspace.clone(),
        review: None,
    });
    pending.seq = pending.seq.max(event.seq);
    pending.deadline = deadline;
    pending.workspace = workspace;
    if review {
        pending.review = Some(event);
    }
}

async fn wants_checked_review(router: &Router, event: &Event) -> bool {
    if event.kind != "turn.completed"
        || event.data["outcome"] != "success"
        || event.data["project_check"] != "required"
        || !event.data["worktree"].is_object()
    {
        return false;
    }
    let Some(agent) = event.agent.as_deref() else {
        return false;
    };
    let Ok(Some(agent)) = router.inner.store.lock().await.get_agent(agent) else {
        return false;
    };
    if event.generation != Some(agent.generation) || agent.policy_opt_out_reason.is_some() {
        return false;
    }
    let project_principal = super::rule_principal(Some(&agent.project));
    router.inner.rule_definitions.lock_recover().iter().any(
        |((configured_principal, _), (_, rule))| {
            let rule_principal = match configured_principal.as_str() {
                "owner" => "owner",
                scoped if scoped == project_principal => project_principal.as_str(),
                _ => return false,
            };
            rule.matches_project_review(event, &agent.project, rule_principal)
        },
    )
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
        let prior_pass = driver
            .check_states
            .get(&key)
            .and_then(|state| state.passed.clone());
        let task_router = router.clone();
        let task_shutdown = shutdown.clone();
        let task_key = key.clone();
        driver.in_flight.insert(key);
        driver.tasks.spawn(async move {
            let disposition = check_after_edit(
                &task_router,
                &task_key,
                &edit,
                prior_fingerprint.as_deref(),
                prior_pass,
                task_shutdown,
            )
            .await;
            WorkCompletion {
                key: task_key,
                event_seq: edit.seq,
                uncertain: disposition.uncertain,
                fingerprint: disposition.fingerprint,
                passed: disposition.passed,
            }
        });
    }
}

async fn check_after_edit(
    router: &Router,
    key: &WorkKey,
    edit: &PendingEdit,
    prior_fingerprint: Option<&str>,
    prior_pass: Option<ProjectCheckPass>,
    mut shutdown: watch::Receiver<bool>,
) -> CheckDisposition {
    let event_seq = edit.seq;
    let expected_workspace = &edit.workspace;
    let lock = router.workspace_check_lock(expected_workspace);
    let _permit = tokio::select! {
        biased;
        _ = wait_for_check_shutdown(&mut shutdown) => {
            return CheckDisposition { uncertain: false, fingerprint: None, passed: None };
        }
        permit = lock.lock_owned() => permit,
    };
    let work = match load_project_work(router, key, expected_workspace, edit.review.as_ref()).await
    {
        Ok(Some(work)) => work,
        Ok(None) => {
            return CheckDisposition {
                uncertain: false,
                fingerprint: None,
                passed: None,
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
                passed: None,
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
                passed: None,
            };
        }
    };
    let review = edit
        .review
        .as_ref()
        .filter(|event| event.data["worktree"]["fingerprint"] == before && work.check.is_some());
    let prior_pass = prior_pass
        .filter(|pass| pass.fingerprint == before && work.check.as_ref() == Some(&pass.check));
    if prior_pass.is_some() || (review.is_none() && prior_fingerprint == Some(before.as_str())) {
        if let Some(event) = &edit.review
            && prior_pass.is_some()
        {
            let changed =
                (event.data["worktree"]["fingerprint"] != before).then_some(before.as_str());
            record_worktree_check_outcome(router, key, event, &work.config.id, changed).await;
        }
        return CheckDisposition {
            uncertain: false,
            fingerprint: Some(before),
            passed: prior_pass,
        };
    }

    let policy = router.secret_policy_snapshot();
    let mut failure = None;
    let mut checked_fingerprint = None;
    let mut fast_check_exit = None;
    let mut passed = None;
    let mut check_changed_worktree = false;
    if !work.config.format.is_empty() {
        match run_argv(
            &work.config.format,
            &work.workspace,
            DEFAULT_GOAL_CHECK_TIMEOUT_MS,
            Arc::clone(&policy),
            shutdown.clone(),
            work.task_base.as_deref(),
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
                    passed: None,
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
                    passed: None,
                };
            }
        }
    }
    if failure.is_none()
        && let Some(check) = &work.check
    {
        let before_check = checked_fingerprint
            .clone()
            .or_else(|| work.config.format.is_empty().then(|| before.clone()));
        match run_argv(
            &check.argv,
            check.cwd.as_deref().unwrap_or(&work.workspace),
            check.deadline_ms,
            Arc::clone(&policy),
            shutdown,
            work.task_base.as_deref(),
        )
        .await
        {
            Ok(outcome @ (CheckOutcome::Passed | CheckOutcome::Failed { .. })) => {
                // Remember the worktree left by this invocation, including
                // any reports the checker wrote. Its own Git hints can then
                // be suppressed just as formatter hints are.
                checked_fingerprint = work_fingerprint(&work.workspace).await.ok();
                if let CheckOutcome::Failed { exit_code, output } = outcome {
                    fast_check_exit = Some(exit_code);
                    failure = Some(format_failure("fast check", exit_code, &output, &policy));
                } else {
                    fast_check_exit = Some(0);
                    if let (Some(fingerprint), Some(observed)) =
                        (before_check, checked_fingerprint.as_ref())
                    {
                        check_changed_worktree = &fingerprint != observed;
                        if !check_changed_worktree {
                            passed = Some(ProjectCheckPass {
                                check: check.clone(),
                                fingerprint,
                            });
                        }
                    }
                }
            }
            Ok(CheckOutcome::Unknown(reason)) => {
                log_worker(router, "daemon.project_check_unknown", Some(key), &reason);
                return CheckDisposition {
                    uncertain: true,
                    fingerprint: None,
                    passed: None,
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
                    passed: None,
                };
            }
        }
    }
    let fast_check = fast_check_exit.map(|exit_code| (event_seq, exit_code));
    if let Err(error) = update_failure_context(router, key, failure, fast_check).await {
        log_error(
            router,
            "daemon.project_check_delivery_failed",
            key,
            event_seq,
            &error,
        );
    }
    if let Some(event) = &edit.review
        && fast_check_exit == Some(0)
        && let Some(observed) = checked_fingerprint.as_deref()
    {
        if check_changed_worktree
            || event.data["worktree"]["fingerprint"] != before
            || event.data["worktree"]["fingerprint"] != observed
        {
            record_worktree_check_outcome(router, key, event, &work.config.id, Some(observed))
                .await;
        } else if passed.is_some() {
            record_worktree_check_outcome(router, key, event, &work.config.id, None).await;
        }
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
        passed,
    }
}

async fn work_fingerprint(workspace: &str) -> Result<String> {
    GitProbe::new()?.work_fingerprint(workspace).await
}

async fn record_worktree_check_outcome(
    router: &Router,
    key: &WorkKey,
    event: &Event,
    project: &str,
    changed_fingerprint: Option<&str>,
) {
    let mut store = router.inner.store.lock().await;
    match store.record_project_worktree_check_outcome(event, project, changed_fingerprint, now_ms())
    {
        Ok(change) => router.publish_locked(&change.events),
        Err(error) => log_error(
            router,
            "daemon.project_worktree_check_receipt_failed",
            key,
            event.seq,
            &error,
        ),
    }
}

pub(super) async fn publish_goal_worktree_pass(
    router: &Router,
    seq: i64,
    fingerprint: &str,
    definition: &GoalDefinition,
) {
    let Ok(events) = router.read_rule_events(seq.saturating_sub(1), seq, 1).await else {
        return;
    };
    let Some(event) = events.first() else {
        return;
    };
    if event.data["worktree"]["fingerprint"] != fingerprint
        || !wants_checked_review(router, event).await
    {
        return;
    }
    let (Some(agent), Some(generation), Some(workspace)) = (
        event.agent.as_deref(),
        event.generation,
        event
            .subject
            .as_ref()
            .and_then(|subject| subject.workspace.as_deref()),
    ) else {
        return;
    };
    let Ok(Some(agent)) = router.inner.store.lock().await.get_agent(agent) else {
        return;
    };
    let Ok(projects) = crate::config::load_project_registration_config(&router.inner.data_root)
    else {
        return;
    };
    let matches_policy = projects.into_iter().any(|project| {
        project.id == agent.project
            && project
                .policy
                .and_then(|policy| policy.check)
                .is_some_and(|check| {
                    check.at_turn_end == Some(true)
                        && check.argv == definition.check
                        && definition
                            .check_cwd
                            .as_deref()
                            .is_none_or(|cwd| cwd == workspace)
                })
    });
    if matches_policy && let Ok(observed) = work_fingerprint(workspace).await {
        let key = WorkKey {
            agent: agent.id,
            generation,
        };
        let changed = (observed != fingerprint).then_some(observed.as_str());
        record_worktree_check_outcome(router, &key, event, &agent.project, changed).await;
    }
}

async fn load_project_work(
    router: &Router,
    key: &WorkKey,
    expected_workspace: &str,
    review: Option<&Event>,
) -> Result<Option<ProjectWork>> {
    let (agent, registration, opted_out, launch_goal) = {
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
        let opted_out = agent.policy_opt_out_reason.is_some();
        let launch_goal = store.launch_goal_definition(&agent.id, key.generation)?;
        (agent, registration, opted_out, launch_goal)
    };
    let config = crate::config::load_project_registration_config(&router.inner.data_root)?
        .into_iter()
        .find(|project| project.id == registration.id)
        .ok_or_else(|| Error::new("not_allowed", "project registration is no longer approved"))?;
    crate::automation::verify_project_registration(&router.inner.data_root, &registration).await?;
    let project_check = config
        .policy
        .as_ref()
        .and_then(|policy| policy.check.as_ref());
    let review_after_goal = if let Some(event) = review
        && project_check.is_some_and(|check| check.at_turn_end == Some(true))
    {
        let store = router.inner.store.lock().await;
        store.goal_get(&key.agent, "eliot")?.is_some_and(|goal| {
            goal.generation == key.generation
                && goal.status == GoalStatus::Completed
                && goal.round_marker.is_none()
        }) && !store.has_project_worktree_check_outcome(event)?
    } else {
        false
    };
    let check = project_check
        .filter(|check| !opted_out && (review_after_goal || check.at_turn_end != Some(true)))
        .map(|check| match launch_goal {
            Some(goal) => ProjectWorkCheck {
                argv: goal.check,
                cwd: goal.check_cwd,
                deadline_ms: goal
                    .check_timeout_ms
                    .unwrap_or(eliot_core::api::DEFAULT_GOAL_CHECK_TIMEOUT_MS),
            },
            None => ProjectWorkCheck {
                argv: check.argv.clone(),
                cwd: None,
                deadline_ms: check.fixed_deadline_ms(),
            },
        });
    let configured_argv = json!([
        &config.format,
        check.as_ref().map(|check| (&check.argv, &check.cwd))
    ]);
    if router
        .secret_policy_snapshot()
        .contains_value(&configured_argv)
    {
        return Err(Error::new(
            "secret_in_config",
            "project check argv contains a known credential",
        ));
    }
    if config.format.is_empty() && check.is_none() {
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
        task_base: agent.base_oid,
    }))
}

pub(crate) enum CheckOutcome {
    Passed,
    Failed { exit_code: i32, output: String },
    Unknown(String),
}

async fn run_argv(
    argv: &[String],
    workspace: &str,
    deadline_ms: i64,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
    task_base: Option<&str>,
) -> Result<CheckOutcome> {
    run_argv_with_index(
        argv,
        workspace,
        deadline_ms,
        policy,
        cancellation,
        task_base,
        None,
    )
    .await
}

pub(crate) async fn run_argv_with_index(
    argv: &[String],
    workspace: &str,
    deadline_ms: i64,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
    task_base: Option<&str>,
    private: Option<(&Path, crate::git_integration::IntegrationLease)>,
) -> Result<CheckOutcome> {
    let (program, args) = argv
        .split_first()
        .ok_or_else(|| Error::new("source_integrity", "project command argv is empty"))?;
    let mut command = local_process_command(program)?;
    command.args(args).current_dir(workspace);
    if let Some(base) = task_base {
        command.env(TASK_BASE_ENV, base);
    }
    let timeout = Duration::from_millis(deadline_ms as u64);
    let outcome = if let Some((index_file, lease)) = private {
        command.env(
            "GIT_INDEX_FILE",
            crate::git_monitor::git_path_argument(index_file)?,
        );
        crate::scripts::process::run_integration_process(
            command,
            timeout,
            policy.clone(),
            cancellation,
            lease,
        )
        .await
    } else {
        run_owned_process(command, Vec::new(), timeout, policy.clone(), cancellation).await
    };
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
    fast_check: Option<(i64, i32)>,
) -> Result<()> {
    router.check_command_admission_mode("inject")?;
    let mut store = router.inner.store.lock().await;
    let change =
        store.project_check_context(&key.agent, key.generation, failure, fast_check, now_ms())?;
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
