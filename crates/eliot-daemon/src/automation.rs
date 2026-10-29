mod definition;
mod projects;
mod reload;
mod schedule;
pub(crate) use definition::RuleDefinition;

use crate::{common::now_ms, logging, router::Router};
use eliot_core::{
    Error, Event, Result, SecretPolicy, SubmitCommand,
    api::{CatchUp, ProjectRegistrationView, RuleStateView, TimerSchedule, TimerStateView},
    canonical_json_sha256,
};
use eliot_store::{
    RuleCursorOutcome, RuleDefinitionInput, RuleProcessingState, TimerDefinitionInput,
    TimerSlotCommit,
};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, watch};

pub(crate) const RULE_EVENT_PAGE_SIZE: usize = 128;
const TIMER_PAGE_SIZE: usize = 128;
const RULE_CHILD_RECOVERY_PAGE_SIZE: usize = 1_000;
const RULE_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReloadNoticeKey {
    Snapshot([u8; 32]),
    Rejection([u8; 32]),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ReloadNoticeContext {
    deferred: Option<ReloadNoticeKey>,
    rejected: Option<ReloadNoticeKey>,
}

struct ReloadReporter<'a> {
    policy: &'a SecretPolicy,
    notices: &'a mut ReloadNoticeContext,
}

enum TimerDecision {
    NotDue,
    Fire {
        scheduled_at_ms: i64,
        next_due_at_ms: Option<i64>,
        live_armed: bool,
    },
    Skip {
        scheduled_at_ms: i64,
        next_due_at_ms: Option<i64>,
        code: String,
    },
}

pub(crate) struct ActiveRule {
    principal: String,
    project: Option<ProjectRegistrationView>,
    definition: RuleDefinition,
    state: RuleStateView,
    retiring: Option<RetiringRule>,
    timer: Option<ActiveTimer>,
}

pub(crate) struct ProjectRuleNamespace {
    principal: String,
    registration: projects::RegistrationSnapshot,
    registration_view: ProjectRegistrationView,
    registration_current: bool,
    rules: Vec<ActiveRule>,
    notices: ReloadNoticeContext,
}

pub(crate) struct RuleWorkerStart {
    pub global_rules: Vec<ActiveRule>,
    pub project_rules: Vec<ProjectRuleNamespace>,
    pub max_event_backlog: i64,
    pub data_root: PathBuf,
    pub policy: Arc<SecretPolicy>,
    pub initial_notices: ReloadNoticeContext,
}

struct RetiringRule {
    definition: RuleDefinition,
    revision: i64,
    cursor: i64,
    through: i64,
}

struct ActiveTimer {
    state: TimerStateView,
    catch_up: CatchUp,
    armed_due_at_ms: Option<i64>,
    blocked: bool,
}

pub(crate) async fn initialize_global_rules(
    router: &Router,
    data_root: &Path,
    policy: &SecretPolicy,
) -> Result<(Vec<ActiveRule>, ReloadNoticeContext)> {
    let mut notices = ReloadNoticeContext::default();
    let prior = router.rule_processing("owner").await?;
    let registration_now = now_ms();
    let (snapshot, from_file) = match reload::load(data_root) {
        Ok(snapshot) => (snapshot, true),
        Err(error) if !prior.is_empty() => {
            report_rejected_source(policy, &error, &mut notices)?;
            (reload::from_processing(&prior)?, false)
        }
        Err(error) => return Err(error),
    };
    let (rules, candidate_from_file) =
        match apply_global_rule_snapshot(router, "owner", None, &snapshot, None, registration_now)
            .await
        {
            Ok(rules) => (rules, from_file),
            Err(error)
                if !prior.is_empty()
                    && matches!(
                        error.code.as_str(),
                        "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
                    ) =>
            {
                report_rejected_reload(policy, &snapshot.inputs, &error, &mut notices)?;
                let saved = reload::from_processing(&prior)?;
                (
                    apply_global_rule_snapshot(
                        router,
                        "owner",
                        None,
                        &saved,
                        None,
                        registration_now,
                    )
                    .await?,
                    false,
                )
            }
            Err(error) => return Err(error),
        };
    if candidate_from_file
        && rules.iter().any(|rule| rule.retiring.is_some())
        && !snapshot.matches(
            &rules
                .iter()
                .map(|rule| rule.definition.clone())
                .collect::<Vec<_>>(),
        )?
    {
        report_deferred_reload(policy, &snapshot.inputs, &mut notices)?;
    }
    Ok((rules, notices))
}

pub(crate) async fn initialize_project_rules(
    router: &Router,
    data_root: &Path,
    policy: &SecretPolicy,
) -> Result<Vec<ProjectRuleNamespace>> {
    let registration_result = projects::load_registrations(data_root).await;
    let registrations = match registration_result {
        Ok(registrations) => registrations,
        Err(error) => {
            logging::record(policy, "daemon.rule_reload_rejected", &[], Some(&error));
            return Ok(Vec::new());
        }
    };
    let inputs = registrations
        .iter()
        .map(project_registration_input)
        .collect::<Vec<_>>();
    let views = router.reconcile_project_registrations(&inputs).await?;
    let mut namespaces = Vec::with_capacity(views.len());
    for view in views {
        let Some(registration) = registrations
            .iter()
            .find(|item| item.id == view.id)
            .cloned()
        else {
            return Err(Error::new(
                "source_integrity",
                "Store returned an unconfigured project registration",
            ));
        };
        let initialized =
            initialize_project_namespace(router, data_root, policy, registration, view).await;
        match initialized {
            Ok(namespace) => namespaces.push(namespace),
            Err(error) => logging::record(policy, "daemon.rule_reload_rejected", &[], Some(&error)),
        }
    }
    Ok(namespaces)
}

async fn initialize_project_namespace(
    router: &Router,
    data_root: &Path,
    policy: &SecretPolicy,
    registration: projects::RegistrationSnapshot,
    registration_view: ProjectRegistrationView,
) -> Result<ProjectRuleNamespace> {
    let principal = format!("project:{}", registration.id);
    registration_view.validate()?;
    if project_registration_snapshot(&registration_view) != registration {
        return Err(Error::new(
            "source_integrity",
            "Store project registration differs from the owner configuration",
        ));
    }
    let prior = router.rule_processing(&principal).await?;
    let mut notices = ReloadNoticeContext::default();
    let registration_now = now_ms();
    let loaded_result = projects::load_project_rules(data_root, registration.clone()).await;
    let loaded = match loaded_result {
        Ok(loaded) => loaded,
        Err(error) if !prior.is_empty() => {
            report_rejected_source(policy, &error, &mut notices)?;
            let snapshot = reload::from_processing(&prior)?;
            let rules = apply_global_rule_snapshot(
                router,
                &principal,
                Some(&registration_view),
                &snapshot,
                None,
                registration_now,
            )
            .await?;
            return Ok(ProjectRuleNamespace {
                principal,
                registration,
                registration_view,
                registration_current: true,
                rules,
                notices,
            });
        }
        Err(error) => return Err(error),
    };
    if loaded.principal != principal || loaded.registration != registration {
        return Err(Error::new(
            "source_integrity",
            "project rule loader returned a different authority scope",
        ));
    }
    let snapshot = loaded.snapshot;
    let rules_result = apply_global_rule_snapshot(
        router,
        &principal,
        Some(&registration_view),
        &snapshot,
        None,
        registration_now,
    )
    .await;
    let rules = match rules_result {
        Ok(rules) => rules,
        Err(error)
            if !prior.is_empty()
                && matches!(
                    error.code.as_str(),
                    "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
                ) =>
        {
            report_rejected_reload(policy, &snapshot.inputs, &error, &mut notices)?;
            let saved = reload::from_processing(&prior)?;
            apply_global_rule_snapshot(
                router,
                &principal,
                Some(&registration_view),
                &saved,
                None,
                registration_now,
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    let current = rules
        .iter()
        .map(|rule| rule.definition.clone())
        .collect::<Vec<_>>();
    let deferred = rules
        .iter()
        .any(|rule| rule.state.retiring_revision.is_some())
        && !snapshot.matches(&current)?;
    if deferred {
        report_deferred_reload(policy, &snapshot.inputs, &mut notices)?;
    }
    Ok(ProjectRuleNamespace {
        principal,
        registration: loaded.registration,
        registration_view,
        registration_current: true,
        rules,
        notices,
    })
}

fn project_registration_input(
    registration: &projects::RegistrationSnapshot,
) -> eliot_store::ProjectRegistrationInput {
    eliot_store::ProjectRegistrationInput {
        id: registration.id.clone(),
        checkout: registration.checkout.clone(),
        repo_common_dir: registration.repo_common_dir.clone(),
        profiles: registration.profiles.clone(),
        actions: registration.actions.clone(),
    }
}

fn project_registration_snapshot(
    registration: &ProjectRegistrationView,
) -> projects::RegistrationSnapshot {
    projects::RegistrationSnapshot {
        id: registration.id.clone(),
        checkout: registration.checkout.clone(),
        repo_common_dir: registration.repo_common_dir.clone(),
        profiles: registration.profiles.clone(),
        actions: registration.actions.clone(),
    }
}

pub(crate) async fn verify_project_registration(
    data_root: &Path,
    registration: &ProjectRegistrationView,
) -> Result<()> {
    registration.validate()?;
    projects::verify_registration(data_root, &project_registration_snapshot(registration)).await
}

pub(crate) async fn verify_managed_project_workspace(
    registration: &ProjectRegistrationView,
    workspace: &eliot_core::WorkspacePreparation,
    target_workspace: &str,
    expected_generation: i64,
) -> Result<()> {
    registration.validate()?;
    projects::verify_managed_workspace_identity(
        &project_registration_snapshot(registration),
        workspace,
        target_workspace,
        expected_generation,
    )
    .await
}

async fn refresh_project_namespaces(
    router: &Router,
    data_root: &Path,
    policy: &SecretPolicy,
    namespaces: &mut Vec<ProjectRuleNamespace>,
    owner_config_notices: &mut ReloadNoticeContext,
) -> Result<bool> {
    let registrations = match projects::load_registrations(data_root).await {
        Ok(registrations) => registrations,
        Err(error) => {
            report_rejected_source(policy, &error, owner_config_notices)?;
            for namespace in namespaces.iter_mut() {
                namespace.registration_current = false;
            }
            return Ok(false);
        }
    };
    let inputs = registrations
        .iter()
        .map(project_registration_input)
        .collect::<Vec<_>>();
    let views = match router.reconcile_project_registrations(&inputs).await {
        Ok(views) => views,
        Err(error) => {
            report_rejected_source(policy, &error, owner_config_notices)?;
            for namespace in namespaces.iter_mut() {
                namespace.registration_current = false;
            }
            return Ok(false);
        }
    };
    let current_ids: HashSet<_> = views.iter().map(|view| view.id.clone()).collect();
    namespaces.retain(|namespace| current_ids.contains(&namespace.registration.id));

    for view in views {
        let Some(registration) = registrations
            .iter()
            .find(|item| item.id == view.id)
            .cloned()
        else {
            return Err(Error::new(
                "source_integrity",
                "Store returned an unconfigured project registration",
            ));
        };
        let Some(index) = namespaces
            .iter()
            .position(|namespace| namespace.registration.id == view.id)
        else {
            match initialize_project_namespace(router, data_root, policy, registration, view).await
            {
                Ok(namespace) => namespaces.push(namespace),
                Err(error) => {
                    logging::record(policy, "daemon.rule_reload_rejected", &[], Some(&error));
                }
            }
            continue;
        };

        let namespace = &mut namespaces[index];
        namespace.registration_current = true;
        namespace.registration = registration.clone();
        namespace.registration_view = view.clone();
        for rule in &mut namespace.rules {
            rule.project = Some(view.clone());
        }

        let loaded = match projects::load_project_rules(data_root, registration.clone()).await {
            Ok(loaded) => loaded,
            Err(error) => {
                report_rejected_source(policy, &error, &mut namespace.notices)?;
                continue;
            }
        };
        if loaded.principal != namespace.principal || loaded.registration != namespace.registration
        {
            namespace.registration_current = false;
            let error = Error::new(
                "source_integrity",
                "project rule loader returned a different authority scope",
            );
            report_rejected_source(policy, &error, &mut namespace.notices)?;
            continue;
        }
        let snapshot = loaded.snapshot;
        let current = namespace
            .rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if snapshot.matches(&current)? {
            namespace.notices.deferred = None;
            namespace.notices.rejected = None;
            continue;
        }
        let key = reload_notice_key(&snapshot.inputs)?;
        if namespace.notices.rejected.as_ref() == Some(&key)
            || (namespace
                .rules
                .iter()
                .any(|rule| rule.state.retiring_revision.is_some())
                && namespace.notices.deferred.as_ref() == Some(&key))
        {
            continue;
        }
        let update = {
            let mut reporter = ReloadReporter {
                policy,
                notices: &mut namespace.notices,
            };
            attempt_reload_snapshot(
                router,
                &namespace.principal,
                Some(&namespace.registration_view),
                &snapshot,
                &namespace.rules,
                now_ms(),
                &mut reporter,
            )
            .await?
        };
        if let Some((updated, _deferred)) = update {
            namespace.rules = updated;
        }
    }
    Ok(true)
}

fn reload_notice_key(inputs: &[RuleDefinitionInput]) -> Result<ReloadNoticeKey> {
    let mut ordered = inputs.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.id.cmp(&right.id));
    let value = json!({
        "rules": ordered
            .iter()
            .map(|input| json!({
                "id": input.id,
                "digest": input.definition_digest,
                "enabled": input.enabled,
            }))
            .collect::<Vec<_>>(),
    });
    canonical_json_sha256(&value)
        .map(ReloadNoticeKey::Snapshot)
        .map_err(|_| Error::new("internal", "reload notice could not be canonicalized"))
}

fn claim_notice(key: ReloadNoticeKey, last_notice: &mut Option<ReloadNoticeKey>) -> bool {
    if last_notice.as_ref() == Some(&key) {
        return false;
    }
    *last_notice = Some(key);
    true
}

fn report_deferred_reload(
    policy: &SecretPolicy,
    inputs: &[RuleDefinitionInput],
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    if claim_deferred_reload(inputs, notices)? {
        logging::record(policy, "daemon.rule_reload_deferred", &[], None);
    }
    Ok(())
}

fn claim_deferred_reload(
    inputs: &[RuleDefinitionInput],
    notices: &mut ReloadNoticeContext,
) -> Result<bool> {
    let key = reload_notice_key(inputs)?;
    if notices.rejected.as_ref() == Some(&key) {
        return Ok(false);
    }
    Ok(claim_notice(key, &mut notices.deferred))
}

fn report_rejected_reload(
    policy: &SecretPolicy,
    inputs: &[RuleDefinitionInput],
    error: &Error,
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    if claim_notice(reload_notice_key(inputs)?, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", &[], Some(error));
    }
    Ok(())
}

fn report_rejected_source(
    policy: &SecretPolicy,
    error: &Error,
    notices: &mut ReloadNoticeContext,
) -> Result<()> {
    let details = json!({"code": error.code, "message": error.message});
    let key = ReloadNoticeKey::Rejection(
        canonical_json_sha256(&details)
            .map_err(|_| Error::new("internal", "reload notice could not be canonicalized"))?,
    );
    if claim_notice(key, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", &[], Some(error));
    }
    Ok(())
}

async fn apply_global_rule_snapshot(
    router: &Router,
    principal: &str,
    project: Option<&ProjectRegistrationView>,
    snapshot: &reload::RuleSnapshot,
    previous: Option<&[ActiveRule]>,
    registration_now: i64,
) -> Result<Vec<ActiveRule>> {
    router
        .validate_rule_definitions(principal, &snapshot.inputs)
        .await?;
    let prior = router.rule_processing(principal).await?;
    // Preflight schedule arithmetic against the complete incoming snapshot
    // before a retiring revision can replace a deferred definition below.
    let _candidate_timers =
        timer_inputs(router, principal, snapshot, &prior, registration_now).await?;
    let effective = defer_retiring_edits(snapshot, &prior)?;
    let timers = timer_inputs(router, principal, &effective, &prior, registration_now).await?;
    let states = router
        .initialize_rules(principal, &effective.inputs, &timers, registration_now)
        .await?;
    let active = active_rules(
        router,
        principal,
        project,
        states,
        previous,
        registration_now,
    )
    .await?;
    let cached = active
        .iter()
        .map(|rule| (rule.state.revision, rule.definition.clone()))
        .collect::<Vec<_>>();
    router.install_rule_definitions(principal, &cached);
    Ok(active)
}

async fn attempt_reload_snapshot(
    router: &Router,
    principal: &str,
    project: Option<&ProjectRegistrationView>,
    snapshot: &reload::RuleSnapshot,
    previous: &[ActiveRule],
    now: i64,
    reporter: &mut ReloadReporter<'_>,
) -> Result<Option<(Vec<ActiveRule>, bool)>> {
    match apply_global_rule_snapshot(router, principal, project, snapshot, Some(previous), now)
        .await
    {
        Ok(updated) => {
            let definitions = updated
                .iter()
                .map(|rule| rule.definition.clone())
                .collect::<Vec<_>>();
            let deferred = updated
                .iter()
                .any(|rule| rule.state.retiring_revision.is_some())
                && !snapshot.matches(&definitions)?;
            if deferred {
                report_deferred_reload(reporter.policy, &snapshot.inputs, reporter.notices)?;
            } else {
                reporter.notices.deferred = None;
                reporter.notices.rejected = None;
            }
            Ok(Some((updated, deferred)))
        }
        Err(error)
            if matches!(
                error.code.as_str(),
                "frame_too_large" | "invalid" | "secret_in_payload" | "unsupported"
            ) =>
        {
            report_rejected_reload(reporter.policy, &snapshot.inputs, &error, reporter.notices)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn defer_retiring_edits(
    snapshot: &reload::RuleSnapshot,
    prior: &[RuleProcessingState],
) -> Result<reload::RuleSnapshot> {
    let mut definitions: HashMap<String, RuleDefinition> = snapshot
        .definitions
        .iter()
        .cloned()
        .map(|definition| (definition.id.clone(), definition))
        .collect();
    let mut inputs: HashMap<String, RuleDefinitionInput> = snapshot
        .inputs
        .iter()
        .cloned()
        .map(|input| (input.id.clone(), input))
        .collect();

    for state in prior
        .iter()
        .filter(|state| state.view.retiring_revision.is_some())
    {
        let stored = reload::from_canonical(&state.current_definition)?;
        let changed = definitions
            .get(&state.view.id)
            .map(|candidate| {
                Ok(candidate.enabled != stored.enabled
                    || candidate.semantic_digest()? != stored.semantic_digest()?)
            })
            .transpose()?
            .unwrap_or(true);
        if !changed {
            continue;
        }
        let definition_digest = stored.semantic_digest()?;
        inputs.insert(
            state.view.id.clone(),
            RuleDefinitionInput {
                id: state.view.id.clone(),
                canonical_definition: state.current_definition.clone(),
                definition_digest,
                enabled: stored.enabled,
            },
        );
        definitions.insert(state.view.id.clone(), stored);
    }

    let mut ids = definitions.keys().cloned().collect::<Vec<_>>();
    ids.sort();
    let definitions = ids.iter().filter_map(|id| definitions.remove(id)).collect();
    let inputs = ids.iter().filter_map(|id| inputs.remove(id)).collect();
    Ok(reload::RuleSnapshot {
        definitions,
        inputs,
    })
}

async fn timer_inputs(
    router: &Router,
    principal: &str,
    snapshot: &reload::RuleSnapshot,
    prior: &[RuleProcessingState],
    registration_now: i64,
) -> Result<Vec<TimerDefinitionInput>> {
    let prior_by_id: HashMap<_, _> = prior
        .iter()
        .map(|state| (state.view.id.as_str(), state))
        .collect();
    let mut inputs = Vec::new();
    for definition in &snapshot.definitions {
        let Some(schedule) = definition.schedule() else {
            continue;
        };
        let old_definition = prior_by_id
            .get(definition.id.as_str())
            .map(|state| reload::from_canonical(&state.current_definition))
            .transpose()?;
        let schedule_changed = old_definition
            .as_ref()
            .map(RuleDefinition::schedule_digest)
            .transpose()?
            != Some(definition.schedule_digest()?);
        let existing = router.get_rule_timer(principal, &definition.id).await?;
        let schedule_revision =
            match &existing {
                Some(timer) if schedule_changed => timer
                    .schedule_revision
                    .checked_add(1)
                    .ok_or_else(|| Error::new("invalid", "timer schedule revision overflows"))?,
                Some(timer) => timer.schedule_revision,
                None => prior_by_id
                    .get(definition.id.as_str())
                    .map_or(Ok(1), |state| {
                        state.view.revision.checked_add(1).ok_or_else(|| {
                            Error::new("invalid", "timer schedule revision overflows")
                        })
                    })?,
            };
        let initial_next_due_at = match (&existing, schedule_changed) {
            (Some(timer), false) => timer.next_due_at,
            _ => initial_next_due_at(schedule, registration_now)?,
        };
        inputs.push(TimerDefinitionInput {
            id: definition.id.clone(),
            schedule_revision,
            schedule: schedule.clone(),
            enabled: definition.enabled,
            initial_next_due_at,
        });
    }
    Ok(inputs)
}

async fn active_rules(
    router: &Router,
    principal: &str,
    project: Option<&ProjectRegistrationView>,
    states: Vec<RuleProcessingState>,
    previous: Option<&[ActiveRule]>,
    armed_at: i64,
) -> Result<Vec<ActiveRule>> {
    let mut active = Vec::with_capacity(states.len());
    for processing in states {
        let definition = reload::from_canonical(&processing.current_definition)?;
        if definition.id != processing.view.id {
            return Err(Error::new(
                "source_integrity",
                "stored rule definition does not match its state ID",
            ));
        }
        let retiring = match processing.retiring_definition {
            Some(canonical) => {
                let definition = reload::from_canonical(&canonical)?;
                let (Some(revision), Some(cursor), Some(through)) = (
                    processing.view.retiring_revision,
                    processing.view.retiring_cursor,
                    processing.view.retire_through,
                ) else {
                    return Err(Error::new(
                        "source_integrity",
                        "retiring rule definition has incomplete cursor state",
                    ));
                };
                Some(RetiringRule {
                    definition,
                    revision,
                    cursor,
                    through,
                })
            }
            None => {
                if processing.view.retiring_revision.is_some()
                    || processing.view.retiring_cursor.is_some()
                    || processing.view.retire_through.is_some()
                {
                    return Err(Error::new(
                        "source_integrity",
                        "retiring rule cursor has no stored definition",
                    ));
                }
                None
            }
        };
        let timer = if definition.schedule().is_some() {
            let state = router
                .get_rule_timer(principal, &definition.id)
                .await?
                .ok_or_else(|| Error::new("source_integrity", "timer rule has no timer state"))?;
            let old_timer = previous
                .and_then(|rules| {
                    rules
                        .iter()
                        .find(|rule| rule.definition.id == definition.id)
                })
                .and_then(|rule| rule.timer.as_ref());
            let unchanged_schedule = old_timer.is_some_and(|old| {
                old.state.schedule_revision == state.schedule_revision
                    && old.state.schedule == state.schedule
            });
            Some(ActiveTimer {
                armed_due_at_ms: if unchanged_schedule {
                    old_timer.and_then(|old| old.armed_due_at_ms)
                } else {
                    state.next_due_at.filter(|due| *due >= armed_at)
                },
                state,
                catch_up: *definition.catch_up(),
                blocked: unchanged_schedule && old_timer.is_some_and(|old| old.blocked),
            })
        } else {
            None
        };
        active.push(ActiveRule {
            principal: principal.to_owned(),
            project: project.cloned(),
            definition,
            state: processing.view,
            retiring,
            timer,
        });
    }
    Ok(active)
}

pub(crate) async fn run_global_rules(
    router: Router,
    start: RuleWorkerStart,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut rules = start.global_rules;
    let mut project_rules = start.project_rules;
    let data_root = start.data_root;
    let policy = start.policy;
    let initial_notices = start.initial_notices;
    let initial_max_event_backlog = start.max_event_backlog;
    // Bus messages are wake hints; durable Store pages remain authoritative.
    // Register before the first high-water read so no event can fall between them.
    let mut events = router.subscribe_rule_events();
    let mut progress = router.progress_receiver();
    let mut watcher = match reload::RuleWatcher::watch(&data_root) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            logging::record(&policy, "daemon.rule_watch_unavailable", &[], Some(&error));
            None
        }
    };
    let mut project_watchers = Vec::new();
    for namespace in &project_rules {
        match reload::RuleWatcher::watch_project(Path::new(&namespace.registration.checkout)) {
            Ok(watcher) => project_watchers.push(watcher),
            Err(error) => {
                logging::record(&policy, "daemon.rule_watch_unavailable", &[], Some(&error));
            }
        }
    }
    let mut reconcile = tokio::time::interval(RULE_RECONCILE_INTERVAL);
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut notices = initial_notices;
    let mut owner_config_notices = ReloadNoticeContext::default();
    let mut max_event_backlog = initial_max_event_backlog;
    let mut last_applied_inputs = None;
    let mut recover_rule_children = true;
    match crate::config::load_automation_config(&data_root) {
        Ok(config) => {
            max_event_backlog = config.max_event_backlog;
            let _ = refresh_project_namespaces(
                &router,
                &data_root,
                &policy,
                &mut project_rules,
                &mut owner_config_notices,
            )
            .await?;
        }
        Err(error) => {
            report_rejected_source(&policy, &error, &mut owner_config_notices)?;
            for namespace in &mut project_rules {
                namespace.registration_current = false;
            }
        }
    }
    if let Ok(snapshot) = reload::load(&data_root) {
        let current = rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if snapshot.matches(&current).unwrap_or(false) {
            last_applied_inputs = Some(snapshot.inputs);
            notices.deferred = None;
            notices.rejected = None;
        } else {
            let key = reload_notice_key(&snapshot.inputs)?;
            let already_rejected = notices.rejected.as_ref() == Some(&key);
            let already_deferred = rules
                .iter()
                .any(|rule| rule.state.retiring_revision.is_some())
                && notices.deferred.as_ref() == Some(&key);
            if !already_rejected && !already_deferred {
                let update = {
                    let mut reporter = ReloadReporter {
                        policy: &policy,
                        notices: &mut notices,
                    };
                    attempt_reload_snapshot(
                        &router,
                        "owner",
                        None,
                        &snapshot,
                        &rules,
                        now_ms(),
                        &mut reporter,
                    )
                    .await?
                };
                if let Some((updated, deferred)) = update {
                    rules = updated;
                    if !deferred {
                        last_applied_inputs = Some(snapshot.inputs);
                    }
                }
            }
        }
    }
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        if recover_rule_children {
            router
                .recover_rule_children(RULE_CHILD_RECOVERY_PAGE_SIZE)
                .await?;
            recover_rule_children = false;
        }
        let firing_open = router.rule_firings_open();
        if firing_open {
            let high_water = router.rule_high_water().await?;
            if !reconcile_event_rules(&router, &mut rules, high_water, now_ms(), max_event_backlog)
                .await?
            {
                return Ok(());
            }
            for namespace in &mut project_rules {
                if namespace.registration_current
                    && !reconcile_event_rules(
                        &router,
                        &mut namespace.rules,
                        high_water,
                        now_ms(),
                        max_event_backlog,
                    )
                    .await?
                {
                    return Ok(());
                }
            }

            if !reconcile_due_timers(&router, &mut rules, now_ms()).await? {
                return Ok(());
            }
            for namespace in &mut project_rules {
                if namespace.registration_current
                    && !reconcile_due_timers(&router, &mut namespace.rules, now_ms()).await?
                {
                    return Ok(());
                }
            }
        }

        let now = now_ms();
        let timer_wait = if firing_open {
            std::iter::once(next_timer_wait(&rules, now))
                .chain(
                    project_rules
                        .iter()
                        .filter(|namespace| namespace.registration_current)
                        .map(|namespace| next_timer_wait(&namespace.rules, now)),
                )
                .flatten()
                .min()
        } else {
            None
        };
        let mut reload_requested = false;
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            changed = progress.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                recover_rule_children = true;
            }
            received = events.recv() => match received {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {
                    recover_rule_children = true;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
            },
            _ = reconcile.tick() => reload_requested = true,
            _ = async {
                if let Some(watcher) = &mut watcher {
                    let _ = watcher.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => reload_requested = true,
            _ = wait_project_rule_watchers(&mut project_watchers) => reload_requested = true,
            _ = async {
                if let Some(wait) = timer_wait {
                    tokio::time::sleep(wait).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
        if recover_rule_children {
            router
                .recover_rule_children(RULE_CHILD_RECOVERY_PAGE_SIZE)
                .await?;
            recover_rule_children = false;
        }
        for rule in &mut rules {
            if let Some(timer) = &mut rule.timer {
                timer.blocked = false;
            }
        }
        for namespace in &mut project_rules {
            for rule in &mut namespace.rules {
                if let Some(timer) = &mut rule.timer {
                    timer.blocked = false;
                }
            }
        }
        if !reload_requested {
            continue;
        }

        match crate::config::load_automation_config(&data_root) {
            Ok(config) => {
                max_event_backlog = config.max_event_backlog;
                let registrations_valid = refresh_project_namespaces(
                    &router,
                    &data_root,
                    &policy,
                    &mut project_rules,
                    &mut owner_config_notices,
                )
                .await?;
                if registrations_valid {
                    owner_config_notices.deferred = None;
                    owner_config_notices.rejected = None;
                }
            }
            Err(error) => {
                report_rejected_source(&policy, &error, &mut owner_config_notices)?;
            }
        }
        project_watchers.clear();
        for namespace in &project_rules {
            if namespace.registration_current
                && let Ok(watcher) =
                    reload::RuleWatcher::watch_project(Path::new(&namespace.registration.checkout))
            {
                project_watchers.push(watcher);
            }
        }

        let snapshot = match reload::load(&data_root) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                report_rejected_source(&policy, &error, &mut notices)?;
                continue;
            }
        };
        let snapshot_key = reload_notice_key(&snapshot.inputs)?;
        if notices.rejected.as_ref() == Some(&snapshot_key) {
            continue;
        }
        if last_applied_inputs.as_ref() == Some(&snapshot.inputs) {
            notices.deferred = None;
            notices.rejected = None;
            continue;
        }
        let current_definitions = rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if snapshot.matches(&current_definitions)? {
            last_applied_inputs = Some(snapshot.inputs.clone());
            notices.deferred = None;
            notices.rejected = None;
            continue;
        }
        let retirement_pending = rules
            .iter()
            .any(|rule| rule.state.retiring_revision.is_some());
        if retirement_pending && notices.deferred.as_ref() == Some(&snapshot_key) {
            continue;
        }
        let update = {
            let mut reporter = ReloadReporter {
                policy: &policy,
                notices: &mut notices,
            };
            attempt_reload_snapshot(
                &router,
                "owner",
                None,
                &snapshot,
                &rules,
                now_ms(),
                &mut reporter,
            )
            .await?
        };
        if let Some((updated, deferred)) = update {
            rules = updated;
            if !deferred {
                last_applied_inputs = Some(snapshot.inputs);
            }
        }
    }
}

async fn wait_project_rule_watchers(watchers: &mut [reload::RuleWatcher]) {
    if watchers.is_empty() {
        std::future::pending::<()>().await;
        return;
    }
    let waits = watchers
        .iter_mut()
        .map(|watcher| {
            Box::pin(async move {
                let _ = watcher.changed().await;
            })
        })
        .collect::<Vec<_>>();
    let _ = futures_util::future::select_all(waits).await;
}

async fn reconcile_event_rules(
    router: &Router,
    rules: &mut [ActiveRule],
    high_water: i64,
    now: i64,
    max_event_backlog: i64,
) -> Result<bool> {
    for rule in rules {
        if rule.state.paused_reason.as_deref() == Some("backlog_exceeded") {
            continue;
        }
        if rule.state.current_cursor > high_water
            || rule
                .state
                .retiring_cursor
                .is_some_and(|cursor| cursor > high_water)
        {
            return Err(Error::new(
                "source_integrity",
                "rule cursor is above the event high-water mark",
            ));
        }
        refresh_retiring(rule)?;
        let oldest_needed = [
            rule.state.enabled.then_some(rule.state.current_cursor),
            rule.retiring
                .as_ref()
                .filter(|retiring| retiring.definition.enabled)
                .map(|retiring| retiring.cursor),
        ]
        .into_iter()
        .flatten()
        .min();
        if oldest_needed.is_some_and(|cursor| high_water.saturating_sub(cursor) > max_event_backlog)
        {
            let processing = router
                .settle_rule_backlog(
                    &rule.principal,
                    &rule.state,
                    high_water,
                    max_event_backlog,
                    now,
                )
                .await?;
            rule.state = processing.view;
            refresh_retiring(rule)?;
            if rule.state.paused_reason.as_deref() == Some("backlog_exceeded") {
                continue;
            }
        }

        if let Some(retiring) = &rule.retiring {
            if retiring.definition.enabled && rule.state.paused_reason.is_none() {
                let through = retiring.through.min(high_water);
                if retiring.cursor < through
                    && !process_rule_cursor(router, rule, true, through).await?
                {
                    return Ok(false);
                }
            } else if retiring.cursor < retiring.through.min(high_water) {
                advance_rule_cursor(
                    router,
                    rule,
                    true,
                    retiring.through.min(high_water),
                    RuleCursorOutcome::NoMatch,
                )
                .await?;
            }
        }

        if rule.state.current_cursor < high_water {
            if rule.state.paused_reason.is_some() {
                continue;
            }
            if !rule.state.enabled || !rule.definition.enabled {
                advance_rule_cursor(router, rule, false, high_water, RuleCursorOutcome::Disabled)
                    .await?;
            } else if !process_rule_cursor(router, rule, false, high_water).await? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

async fn process_rule_cursor(
    router: &Router,
    rule: &mut ActiveRule,
    retiring: bool,
    through: i64,
) -> Result<bool> {
    let definition = if retiring {
        rule.retiring
            .as_ref()
            .ok_or_else(|| Error::new("source_integrity", "retiring definition is absent"))?
            .definition
            .clone()
    } else {
        rule.definition.clone()
    };
    while let Some(cursor) = rule_cursor(rule, retiring) {
        if cursor >= through {
            break;
        }
        let page = router
            .read_rule_events(cursor, through, RULE_EVENT_PAGE_SIZE)
            .await?;
        if page.is_empty() {
            return Err(Error::new(
                "history_gap",
                "rule cursor is behind unavailable event history",
            ));
        }
        let mut no_match_through = None;
        for event in page {
            if event.seq <= rule_cursor(rule, retiring).unwrap_or(through) {
                continue;
            }
            let stop = match definition::evaluate(&definition, &event) {
                Ok(Some(stop)) => stop,
                Ok(None) => {
                    no_match_through = Some(event.seq);
                    continue;
                }
                Err(error) => {
                    flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        RuleCursorOutcome::Rejected { code: error.code },
                    )
                    .await?;
                    continue;
                }
            };

            flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
            if !router.rule_firings_open() {
                return Ok(false);
            }
            let prepared_result = match rule.project.as_ref() {
                Some(project) => {
                    router
                        .prepare_project_rule_submission(stop_command(stop), project)
                        .await
                }
                None => router.prepare_owner_submission(stop_command(stop)).await,
            };
            let prepared = match prepared_result {
                Ok(prepared) => prepared,
                Err(_) if !router.rule_firings_open() => return Ok(false),
                Err(error) if deterministic_rejection(&error.code) => {
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        RuleCursorOutcome::Rejected { code: error.code },
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let Some(admission) = router
                .admit_rule_stop(
                    &rule.principal,
                    rule.project.as_ref(),
                    &rule.state,
                    event.seq,
                    prepared,
                )
                .await?
            else {
                return Ok(false);
            };
            rule.state = admission.state;
            refresh_retiring(rule)?;
            // Router schedules only a new child after Store commit and lock release.
        }
        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
    }
    Ok(true)
}

fn rule_cursor(rule: &ActiveRule, retiring: bool) -> Option<i64> {
    if retiring {
        rule.retiring.as_ref().map(|state| state.cursor)
    } else {
        Some(rule.state.current_cursor)
    }
}

async fn advance_rule_cursor(
    router: &Router,
    rule: &mut ActiveRule,
    _retiring: bool,
    through: i64,
    outcome: RuleCursorOutcome,
) -> Result<()> {
    rule.state = router
        .advance_rule_cursor(&rule.principal, &rule.state, through, outcome)
        .await?;
    refresh_retiring(rule)
}

async fn flush_rule_no_match(
    router: &Router,
    rule: &mut ActiveRule,
    retiring: bool,
    through: &mut Option<i64>,
) -> Result<()> {
    if let Some(through) = through.take()
        && through > rule_cursor(rule, retiring).unwrap_or(through)
    {
        advance_rule_cursor(router, rule, retiring, through, RuleCursorOutcome::NoMatch).await?;
    }
    Ok(())
}

fn refresh_retiring(rule: &mut ActiveRule) -> Result<()> {
    match (
        rule.state.retiring_revision,
        rule.state.retiring_cursor,
        rule.state.retire_through,
        rule.retiring.as_mut(),
    ) {
        (None, None, None, None) => Ok(()),
        (None, None, None, Some(_)) => {
            rule.retiring = None;
            Ok(())
        }
        (Some(revision), Some(cursor), Some(through), Some(retiring))
            if revision == retiring.revision =>
        {
            retiring.cursor = cursor;
            retiring.through = through;
            Ok(())
        }
        _ => Err(Error::new(
            "source_integrity",
            "retiring definition and cursor state do not match",
        )),
    }
}

fn initial_next_due_at(schedule: &TimerSchedule, registration_now: i64) -> Result<Option<i64>> {
    match schedule {
        TimerSchedule::Interval { anchor, .. } => {
            let anchor = anchor.unwrap_or(registration_now);
            // The explicit anchor sets the phase; creation starts at the first
            // slot after registration, so an old phase never replays history.
            schedule::next_after(schedule, anchor, registration_now)
        }
        TimerSchedule::Once { .. } | TimerSchedule::Cron { .. } => {
            schedule::next_after(schedule, 0, registration_now)
        }
    }
}

async fn reconcile_due_timers(router: &Router, rules: &mut [ActiveRule], now: i64) -> Result<bool> {
    let mut after_id = None;
    loop {
        let principal = rules
            .first()
            .map_or_else(|| "owner".to_owned(), |rule| rule.principal.clone());
        let page = router
            .list_due_rule_timers(&principal, after_id.as_deref(), now, TIMER_PAGE_SIZE)
            .await?;
        let page_len = page.len();
        if page_len == 0 {
            return Ok(true);
        }

        for state in page {
            after_id = Some(state.id.clone());
            let rule = rules
                .iter_mut()
                .find(|rule| {
                    rule.timer
                        .as_ref()
                        .is_some_and(|timer| timer.state.id == state.id)
                        && rule.principal == principal
                })
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "stored timer has no loaded rule definition",
                    )
                })?;
            let Some(timer) = rule.timer.as_mut() else {
                return Err(Error::new(
                    "source_integrity",
                    "stored timer has no loaded rule definition",
                ));
            };
            if timer.state.next_due_at != state.next_due_at {
                timer.armed_due_at_ms = None;
            }
            timer.state = state;
            if timer.blocked {
                continue;
            }

            let (scheduled_at_ms, next_due_at_ms, action, skip_code, armed_slot) =
                match timer_decision(timer, now)? {
                    TimerDecision::NotDue => continue,
                    TimerDecision::Skip {
                        scheduled_at_ms,
                        next_due_at_ms,
                        code,
                    } => (scheduled_at_ms, next_due_at_ms, None, Some(code), None),
                    TimerDecision::Fire {
                        scheduled_at_ms,
                        next_due_at_ms,
                        live_armed,
                    } => {
                        let prepared = match rule.definition.evaluate_timer() {
                            Ok(stop) => match rule.project.as_ref() {
                                Some(project) => {
                                    router
                                        .prepare_project_rule_submission(
                                            stop_command(stop),
                                            project,
                                        )
                                        .await
                                }
                                None => router.prepare_owner_submission(stop_command(stop)).await,
                            },
                            Err(error) if deterministic_rejection(&error.code) => Err(error),
                            Err(error) => return Err(error),
                        };
                        match prepared {
                            Ok(prepared) => (
                                scheduled_at_ms,
                                next_due_at_ms,
                                Some(prepared.request),
                                None,
                                live_armed.then_some(scheduled_at_ms),
                            ),
                            Err(error) if deterministic_rejection(&error.code) => (
                                scheduled_at_ms,
                                next_due_at_ms,
                                None,
                                Some(error.code),
                                None,
                            ),
                            Err(_error) if !router.rule_firings_open() => return Ok(false),
                            Err(error) => return Err(error),
                        }
                    }
                };

            let slot = TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms,
                action,
                skip_code,
            };
            let Some(admission) = router
                .commit_rule_timer_slot(
                    &rule.principal,
                    rule.project.as_ref(),
                    &timer.state,
                    slot,
                    now,
                )
                .await?
            else {
                return Ok(false);
            };
            timer.state = admission.state;
            timer.blocked = admission.blocked;
            timer.armed_due_at_ms = if timer.blocked {
                armed_slot
                    .or(timer.armed_due_at_ms)
                    .filter(|due| timer.state.next_due_at == Some(*due))
            } else {
                timer.state.next_due_at.filter(|due| *due > now)
            };
        }

        if page_len < TIMER_PAGE_SIZE {
            return Ok(true);
        }
    }
}

fn timer_decision(timer: &ActiveTimer, now: i64) -> Result<TimerDecision> {
    let Some(due) = timer.state.next_due_at else {
        return Ok(TimerDecision::NotDue);
    };
    if due > now {
        return Ok(TimerDecision::NotDue);
    }

    let anchor = interval_anchor(&timer.state);
    if timer.armed_due_at_ms == Some(due) || due == now {
        return Ok(TimerDecision::Fire {
            scheduled_at_ms: due,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, now)?,
            live_armed: true,
        });
    }

    let after = timer
        .state
        .last_key
        .unwrap_or_else(|| due.saturating_sub(1));
    let scheduled_at_ms = schedule::latest_due(&timer.state.schedule, anchor, after, now)?
        .ok_or_else(|| Error::new("source_integrity", "due timer has no due schedule slot"))?;
    match timer.catch_up {
        CatchUp::None => Ok(TimerDecision::Skip {
            scheduled_at_ms,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, now)?,
            code: "missed".to_owned(),
        }),
        CatchUp::Latest => Ok(TimerDecision::Fire {
            scheduled_at_ms,
            next_due_at_ms: schedule::next_after(&timer.state.schedule, anchor, scheduled_at_ms)?,
            live_armed: false,
        }),
    }
}

fn interval_anchor(state: &TimerStateView) -> i64 {
    match &state.schedule {
        TimerSchedule::Interval { .. } => state.anchor,
        // Cron and once derive occurrences from their UTC schedule and cursor;
        // their persisted Store anchor is intentionally unused.
        TimerSchedule::Cron { .. } | TimerSchedule::Once { .. } => 0,
    }
}

fn next_timer_wait(rules: &[ActiveRule], now: i64) -> Option<Duration> {
    rules
        .iter()
        .filter_map(|rule| rule.timer.as_ref())
        .filter_map(|timer| timer_wake_delay(timer, now))
        .min()
}

fn timer_wake_delay(timer: &ActiveTimer, now: i64) -> Option<Duration> {
    if !timer.state.enabled || timer.blocked {
        return None;
    }
    let next_due = timer.state.next_due_at?;
    let delay_ms = (i128::from(next_due) - i128::from(now)).max(0);
    Some(Duration::from_millis(
        u64::try_from(delay_ms).unwrap_or(u64::MAX),
    ))
}

fn stop_command(stop: definition::StopAction) -> SubmitCommand {
    // This unbound request ID is used only by Router preparation. The atomic
    // Store rule admission assigns the deterministic rule/event child ID.
    SubmitCommand {
        id: String::new(),
        target: Some(stop.target),
        verb: "stop".to_owned(),
        args: json!({}),
        expected_generation: Some(stop.expected_generation),
        expected_turn_id: None,
        request_id: None,
        probe: false,
    }
}

pub(crate) async fn prepare_rule_catch_up_actions(
    router: &Router,
    definition: &RuleDefinition,
    events: &[Event],
    project: Option<&ProjectRegistrationView>,
) -> Result<Vec<eliot_store::RuleCatchUpAction>> {
    let mut rendered = Vec::new();
    for event in events {
        if let Some(stop) = definition::evaluate(definition, event)? {
            rendered.push((event.seq, stop_command(stop)));
        }
    }
    if let Some(project) = project {
        let requests = rendered
            .iter()
            .map(|(_, request)| request.clone())
            .collect();
        let prepared = router
            .prepare_project_rule_submissions(requests, project)
            .await?;
        return Ok(rendered
            .into_iter()
            .zip(prepared)
            .map(
                |((event_seq, _), prepared)| eliot_store::RuleCatchUpAction {
                    event_seq,
                    request: prepared.request,
                },
            )
            .collect());
    }

    let mut actions = Vec::with_capacity(rendered.len());
    for (event_seq, request) in rendered {
        let prepared = router.prepare_owner_submission(request).await?;
        actions.push(eliot_store::RuleCatchUpAction {
            event_seq,
            request: prepared.request,
        });
    }
    Ok(actions)
}

fn deterministic_rejection(code: &str) -> bool {
    matches!(
        code,
        "bad_state"
            | "frame_too_large"
            | "invalid"
            | "not_allowed"
            | "not_found"
            | "secret_in_payload"
            | "stale_generation"
            | "unsupported"
    )
}

#[cfg(test)]
mod timer_tests {
    use super::*;

    fn timer() -> ActiveTimer {
        ActiveTimer {
            state: TimerStateView {
                id: "interval".to_owned(),
                project: None,
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms: 10,
                    anchor: None,
                },
                anchor: 100,
                last_key: None,
                next_due_at: Some(110),
                enabled: true,
            },
            catch_up: CatchUp::None,
            armed_due_at_ms: Some(110),
            blocked: false,
        }
    }

    #[test]
    fn startup_rejected_snapshot_is_not_deferred_and_new_pending_edit_reports_once() {
        let input = |digest| RuleDefinitionInput {
            id: "rule-a".to_owned(),
            canonical_definition: "{}".to_owned(),
            definition_digest: [digest; 32],
            enabled: true,
        };
        let first = input(1);
        let changed = input(2);
        let mut notices = ReloadNoticeContext::default();
        assert!(claim_notice(
            reload_notice_key(std::slice::from_ref(&first)).unwrap(),
            &mut notices.rejected,
        ));
        assert!(!claim_deferred_reload(std::slice::from_ref(&first), &mut notices).unwrap());
        assert!(claim_deferred_reload(std::slice::from_ref(&changed), &mut notices).unwrap());
        assert!(!claim_deferred_reload(std::slice::from_ref(&changed), &mut notices).unwrap());
    }

    #[tokio::test]
    async fn configured_nondefault_backlog_settles_global_and_project_rules() {
        use std::io::Cursor;

        let root =
            std::env::temp_dir().join(format!("eliot-rule-backlog-config-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("config.toml"),
            "[automation]\nmax_event_backlog = 1\n",
        )
        .unwrap();
        let max_event_backlog = crate::config::load_automation_config(&root)
            .unwrap()
            .max_event_backlog;
        let store = Arc::new(tokio::sync::Mutex::new(
            eliot_store::Store::open(Path::new(":memory:")).unwrap(),
        ));
        let router = Router::new(
            root.clone(),
            "backlog-fixture-owner".to_owned(),
            Arc::clone(&store),
            crate::bus::Bus::new(16),
        );
        let definition = definition::load(Cursor::new(
            r#"
[[rule]]
id = "same-rule-id"
on = { event = "turn.completed" }
do = [{ action = "stop", target = "a1", expected_generation = 1 }]
"#,
        ))
        .unwrap()
        .remove(0);
        let snapshot = reload::from_definitions(vec![definition]).unwrap();
        let now = now_ms();
        router
            .initialize_rules("owner", &snapshot.inputs, &[], now)
            .await
            .unwrap();
        let checkout = root
            .join("registered-project")
            .to_string_lossy()
            .into_owned();
        let registration = router
            .reconcile_project_registrations(&[eliot_store::ProjectRegistrationInput {
                id: "backlog-project".to_owned(),
                checkout: checkout.clone(),
                repo_common_dir: format!("{checkout}/.git"),
                profiles: vec!["fake".to_owned()],
                actions: vec!["stop".to_owned()],
            }])
            .await
            .unwrap()
            .remove(0);
        let project_principal = format!("project:{}", registration.id);
        router
            .initialize_rules(&project_principal, &snapshot.inputs, &[], now)
            .await
            .unwrap();
        let mut global_rules = active_rules(
            &router,
            "owner",
            None,
            router.rule_processing("owner").await.unwrap(),
            None,
            now,
        )
        .await
        .unwrap();
        let mut project_rules = active_rules(
            &router,
            &project_principal,
            Some(&registration),
            router.rule_processing(&project_principal).await.unwrap(),
            None,
            now,
        )
        .await
        .unwrap();

        for index in 0..2 {
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": format!("backlog-event-{index}"),
                "workspace": format!("backlog-workspace-{index}"),
            });
            launch.expected_generation = Some(0);
            store
                .lock()
                .await
                .submit("owner", launch, now_ms())
                .unwrap();
        }
        let high_water = router.rule_high_water().await.unwrap();
        assert!(high_water > max_event_backlog);
        assert!(
            reconcile_event_rules(
                &router,
                &mut global_rules,
                high_water,
                now_ms(),
                max_event_backlog,
            )
            .await
            .unwrap()
        );
        assert!(
            reconcile_event_rules(
                &router,
                &mut project_rules,
                high_water,
                now_ms(),
                max_event_backlog,
            )
            .await
            .unwrap()
        );

        let store = store.lock().await;
        let global = store
            .get_rule_state("owner", "same-rule-id")
            .unwrap()
            .unwrap();
        let project = store
            .get_rule_state(&project_principal, "same-rule-id")
            .unwrap()
            .unwrap();
        assert_eq!(global.paused_reason.as_deref(), Some("backlog_exceeded"));
        assert_eq!(project.paused_reason.as_deref(), Some("backlog_exceeded"));
        assert_eq!(global.current_cursor, high_water);
        assert_eq!(project.current_cursor, high_water);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn store_invalid_snapshot_is_rejected_before_retirement_deferral() {
        let root = std::env::temp_dir().join(format!(
            "eliot-rule-reload-store-validation-{}",
            ulid::Ulid::new()
        ));
        let rules_dir = root.join("rules");
        std::fs::create_dir_all(&rules_dir).unwrap();
        let rule_path = rules_dir.join("reload.toml");
        let store = Arc::new(tokio::sync::Mutex::new(
            eliot_store::Store::open(Path::new(":memory:")).unwrap(),
        ));
        let router = Router::new(
            root.clone(),
            "reload-fixture-owner".to_owned(),
            Arc::clone(&store),
            crate::bus::Bus::new(16),
        );
        let policy = SecretPolicy::default();
        let render_rule = |match_value: Option<&str>| {
            let matcher = match_value.map_or_else(String::new, |value| {
                format!(", match = {{ \"subject.harness\" = '{value}' }}")
            });
            format!(
                "[[rule]]\nid = \"reload-test\"\nenabled = true\non = {{ event = \"turn.completed\"{matcher} }}\ndo = [{{ action = \"stop\", target = \"a1\", expected_generation = 1 }}]\n"
            )
        };
        let submit_launch = |project: &str| {
            let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            request.args = json!({
                "project": project,
                "workspace": format!("reload-{project}"),
            });
            request.expected_generation = Some(0);
            request
        };

        {
            let mut store = store.lock().await;
            store
                .submit("owner", submit_launch("before-seed"), now_ms())
                .unwrap();
        }
        std::fs::write(&rule_path, render_rule(None)).unwrap();
        let (initial, _) = initialize_global_rules(&router, &root, &policy)
            .await
            .unwrap();
        assert_eq!(initial.len(), 1);

        {
            let mut store = store.lock().await;
            store
                .submit("owner", submit_launch("after-seed"), now_ms())
                .unwrap();
        }
        std::fs::write(&rule_path, render_rule(Some("first-edit"))).unwrap();
        let (retiring_rules, _) = initialize_global_rules(&router, &root, &policy)
            .await
            .unwrap();
        assert!(retiring_rules[0].state.retiring_revision.is_some());
        let before = router.rule_processing("owner").await.unwrap();
        let high_water_before = store.lock().await.high_water().unwrap();

        let oversized_match = "\"".repeat(66 * 1024);
        let oversized_source = render_rule(Some(&oversized_match));
        assert!(oversized_source.len() < 128 * 1024);
        std::fs::write(&rule_path, oversized_source).unwrap();
        let invalid_snapshot = reload::load(&root).unwrap();
        assert!(invalid_snapshot.inputs[0].canonical_definition.len() > 128 * 1024);

        let mut notices = ReloadNoticeContext::default();
        let invalid_attempt = {
            let mut reporter = ReloadReporter {
                policy: &policy,
                notices: &mut notices,
            };
            attempt_reload_snapshot(
                &router,
                "owner",
                None,
                &invalid_snapshot,
                &retiring_rules,
                now_ms(),
                &mut reporter,
            )
            .await
            .unwrap()
        };
        assert!(invalid_attempt.is_none());
        let invalid_key = reload_notice_key(&invalid_snapshot.inputs).unwrap();
        assert_eq!(notices.rejected, Some(invalid_key));
        assert!(notices.deferred.is_none());

        let after_invalid = router.rule_processing("owner").await.unwrap();
        assert_eq!(after_invalid.len(), before.len());
        assert_eq!(after_invalid[0].view, before[0].view);
        assert_eq!(
            after_invalid[0].current_definition,
            before[0].current_definition
        );
        assert_eq!(
            after_invalid[0].retiring_definition,
            before[0].retiring_definition
        );
        assert_eq!(store.lock().await.high_water().unwrap(), high_water_before);

        std::fs::write(&rule_path, render_rule(Some("second-pending-edit"))).unwrap();
        let valid_snapshot = reload::load(&root).unwrap();
        let (updated_rules, deferred) = {
            let mut reporter = ReloadReporter {
                policy: &policy,
                notices: &mut notices,
            };
            attempt_reload_snapshot(
                &router,
                "owner",
                None,
                &valid_snapshot,
                &retiring_rules,
                now_ms(),
                &mut reporter,
            )
            .await
            .unwrap()
        }
        .expect("valid pending edit should apply through the shared attempt path");
        assert!(deferred);
        let valid_key = reload_notice_key(&valid_snapshot.inputs).unwrap();
        assert_eq!(notices.deferred, Some(valid_key));

        let (_, deferred_again) = {
            let mut reporter = ReloadReporter {
                policy: &policy,
                notices: &mut notices,
            };
            attempt_reload_snapshot(
                &router,
                "owner",
                None,
                &valid_snapshot,
                &updated_rules,
                now_ms(),
                &mut reporter,
            )
            .await
            .unwrap()
        }
        .expect("repeat attempt remains a valid deferred edit");
        assert!(deferred_again);
        assert!(!claim_deferred_reload(&valid_snapshot.inputs, &mut notices).unwrap());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn new_once_timer_has_only_a_due_slot_strictly_after_registration() {
        let past = TimerSchedule::Once { at: 99 };
        assert_eq!(initial_next_due_at(&past, 100).unwrap(), None);

        let future = TimerSchedule::Once { at: 101 };
        assert_eq!(initial_next_due_at(&future, 100).unwrap(), Some(101));

        let at_boundary = TimerSchedule::Once { at: 100 };
        assert_eq!(initial_next_due_at(&at_boundary, 100).unwrap(), None);
    }

    #[test]
    fn live_armed_slot_fires_while_restart_catch_up_coalesces_and_blocked_waits() {
        let mut timer = timer();
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Fire {
                scheduled_at_ms: 110,
                next_due_at_ms: Some(140),
                live_armed: true,
            }
        ));

        timer.armed_due_at_ms = None;
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Skip {
                scheduled_at_ms: 130,
                next_due_at_ms: Some(140),
                ..
            }
        ));
        timer.catch_up = CatchUp::Latest;
        assert!(matches!(
            timer_decision(&timer, 135).unwrap(),
            TimerDecision::Fire {
                scheduled_at_ms: 130,
                next_due_at_ms: Some(140),
                live_armed: false,
            }
        ));

        timer.blocked = true;
        assert_eq!(timer_wake_delay(&timer, 135), None);
    }
}
