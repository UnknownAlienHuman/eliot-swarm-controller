pub(crate) mod assignment;
mod definition;
mod projects;
mod reload;
pub(crate) mod rule_edit;
mod schedule;
use definition::RuleEvaluation;
pub(crate) use definition::{CompletionFields, CompletionRuleInterest, RuleDefinition};
pub(crate) use schedule::parse_positive_duration_ms;

use crate::{common::now_ms, logging, router::Router};
use eliot_core::{
    Error, Event, FrozenScriptContext, Result, ScriptRuleContext, ScriptTimerContext,
    ScriptTimerInvocation, SecretPolicy,
    api::{
        CatchUp, ProjectGitView, ProjectRegistrationView, RuleStateView, TimerSchedule,
        TimerStateView,
    },
    canonical_json_sha256, is_deterministic_pre_admission_rejection,
};

pub(crate) fn git_fetch_timeout_ms(timeout: Option<&str>) -> Result<i64> {
    match timeout {
        Some(timeout) => schedule::parse_positive_duration_ms(timeout),
        None => Ok(eliot_core::api::DEFAULT_GIT_FETCH_TIMEOUT_MS),
    }
}

pub(crate) fn git_integrate_timeout_ms(timeout: Option<&str>) -> Result<i64> {
    let value = match timeout {
        Some(timeout) => schedule::parse_positive_duration_ms(timeout)?,
        None => eliot_core::api::DEFAULT_GIT_INTEGRATE_TIMEOUT_MS,
    };
    if !(1_000..=eliot_core::api::DEFAULT_GIT_INTEGRATE_TIMEOUT_MS).contains(&value) {
        return Err(Error::new(
            "invalid",
            "git.integrate timeout must be between 1000 and 600000 ms",
        ));
    }
    Ok(value)
}
pub(crate) fn git_publish_timeout_ms(timeout: Option<&str>) -> Result<i64> {
    match timeout {
        Some(timeout) => schedule::parse_positive_duration_ms(timeout),
        None => Ok(eliot_core::api::DEFAULT_GIT_PUBLISH_TIMEOUT_MS),
    }
}
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
const MAX_ARBITRATION_RETRIES: usize = 8;
// Temporary priority changes can reorder fresh heads. Older heads use a
// stable project ring so repeated priority changes cannot keep overtaking them.
const PRIORITY_WAIT_LIMIT_MS: i64 = 30_000;

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
    source_present: bool,
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

/// Reason the rule worker left, propagated to the daemon supervisor so a
/// normal drain is not misreported as an unexpected exit.
/// AdmissionClosed covers every Ok(false) site in this module: the
/// matched-event admission checks, prepared-submission and timer-slot
/// closes, and admission-returns-None sites, all of which fire only
/// when router admission is closed. Shutdown covers the supervisor's
/// own shutdown watch. Any other Ok(()) is an unexpected exit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RuleWorkerExit {
    Shutdown,
    AdmissionClosed,
    Exited,
}

fn rule_cursor_rejection(router: &Router, error: &Error) -> RuleCursorOutcome {
    let error = error.clone().sanitized(&router.secret_policy_snapshot());
    if error.code == "capacity_unavailable"
        && error.failure_where.as_deref() == Some("store.admission.capacity")
    {
        RuleCursorOutcome::RejectedWithCapacity {
            code: error.code.clone(),
            message: error.message.to_string(),
            failure_where: "store.admission.capacity".to_owned(),
        }
    } else {
        RuleCursorOutcome::Rejected {
            code: error.code.clone(),
            message: error.message.to_string(),
        }
    }
}

fn rule_catch_up_rejection(
    router: &Router,
    event_seq: i64,
    error: &Error,
) -> eliot_store::RuleCatchUpAction {
    let error = error.clone().sanitized(&router.secret_policy_snapshot());
    if error.code == "capacity_unavailable"
        && error.failure_where.as_deref() == Some("store.admission.capacity")
    {
        eliot_store::RuleCatchUpAction::rejected_with_capacity(event_seq, &error)
    } else {
        eliot_store::RuleCatchUpAction::rejected_with_message(
            event_seq,
            error.code,
            error.message.as_str(),
        )
    }
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
    router.refresh_assignment_pauses().await?;
    let mut notices = ReloadNoticeContext::default();
    let prior = router.rule_processing("owner").await?;
    let registration_now = now_ms();
    let (snapshot, from_file) = match reload::load(data_root) {
        Ok(snapshot) => (snapshot, true),
        Err(error) if !prior.is_empty() => {
            let log_ids = [
                ("producer", "automation.rule_reload"),
                ("method", "rules.reload"),
                ("path", "rules/*.toml or config.toml"),
            ];
            report_rejected_source(policy, &error, &mut notices, &log_ids)?;
            (reload::from_processing(&prior)?, false)
        }
        Err(error) => return Err(error),
    };
    let (rules, candidate_from_file) =
        match apply_global_rule_snapshot(router, "owner", None, &snapshot, None, registration_now)
            .await
        {
            Ok(rules) => (rules, from_file),
            Err(error) if !prior.is_empty() && rejected_reload(&error) => {
                let log_ids = [
                    ("producer", "automation.rule_reload"),
                    ("method", "rules.reload"),
                    ("path", "rules/*.toml"),
                ];
                report_rejected_reload(policy, &snapshot.inputs, &error, &mut notices, &log_ids)?;
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
    let (revision, registrations) = match registration_result {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let log_ids = [
                ("producer", "automation.project_registration"),
                ("method", "project.registrations.load"),
                ("path", "config.toml"),
            ];
            logging::record(
                policy,
                "daemon.rule_reload_rejected",
                &log_ids,
                Some(&error),
            );
            return Ok(Vec::new());
        }
    };
    for registration in &registrations {
        validate_project_git_endpoints(registration.git.as_ref())?;
    }
    let inputs = registrations
        .iter()
        .map(project_registration_input)
        .collect::<Vec<_>>();
    let views = router
        .reconcile_configured_projects(&inputs, revision.as_deref())
        .await?;
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
        let project_id = registration.id.clone();
        let initialized = Box::pin(initialize_project_namespace(
            router,
            data_root,
            policy,
            registration,
            view,
        ))
        .await;
        match initialized {
            Ok(namespace) => namespaces.push(namespace),
            Err(error) => {
                let log_ids = [
                    ("producer", "automation.project_rules"),
                    ("method", "project.rules.load"),
                    ("path", ".eliot/rules/*.toml"),
                    ("project", project_id.as_str()),
                ];
                logging::record(
                    policy,
                    "daemon.rule_reload_rejected",
                    &log_ids,
                    Some(&error),
                );
            }
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
            let log_ids = [
                ("producer", "automation.project_rules"),
                ("method", "project.rules.reload"),
                ("path", ".eliot/rules/*.toml"),
                ("project", registration.id.as_str()),
            ];
            report_rejected_source(policy, &error, &mut notices, &log_ids)?;
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
        Err(error) if !prior.is_empty() && rejected_reload(&error) => {
            let log_ids = [
                ("producer", "automation.project_rules"),
                ("method", "project.rules.reload"),
                ("path", ".eliot/rules/*.toml"),
                ("project", registration.id.as_str()),
            ];
            report_rejected_reload(policy, &snapshot.inputs, &error, &mut notices, &log_ids)?;
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
        git: registration.git.clone(),
        profiles: registration.profiles.clone(),
        actions: registration.actions.clone(),
        interpreters: registration.interpreters.clone(),
        run_env: registration.run_env.clone(),
    }
}

/// Load the canonical project registrations represented by `projects.*` in
/// config.toml for a settings.save reconciliation. Keep this on the same
/// resolver path used by daemon startup and automation reloads. It runs
/// before anything is written, so a candidate that cannot be resolved (a
/// missing folder, no Git) is a definite `invalid`, never `unavailable`:
/// clients read that code as an unknown outcome.
pub(crate) async fn project_registration_inputs(
    configured: Vec<crate::config::ProjectRegistrationConfig>,
) -> Result<Vec<eliot_store::ProjectRegistrationInput>> {
    let registrations =
        projects::resolve_registrations(configured)
            .await
            .map_err(|mut error| {
                if error.code == "unavailable" {
                    error.code = "invalid".to_owned();
                    *error.message = format!("project checkout cannot be used: {}", error.message);
                }
                error
            })?;
    for registration in &registrations {
        validate_project_git_endpoints(registration.git.as_ref())?;
    }
    Ok(registrations
        .iter()
        .map(project_registration_input)
        .collect())
}

fn project_registration_snapshot(
    registration: &ProjectRegistrationView,
) -> projects::RegistrationSnapshot {
    projects::RegistrationSnapshot {
        id: registration.id.clone(),
        checkout: registration.checkout.clone(),
        repo_common_dir: registration.repo_common_dir.clone(),
        git: registration.git.clone(),
        profiles: registration.profiles.clone(),
        actions: registration.actions.clone(),
        interpreters: registration.interpreters.clone(),
        run_env: registration.run_env.clone(),
    }
}

pub(crate) async fn verify_project_registration(
    data_root: &Path,
    registration: &ProjectRegistrationView,
) -> Result<()> {
    registration.validate()?;
    validate_project_git_endpoints(registration.git.as_ref())?;
    projects::verify_registration(data_root, &project_registration_snapshot(registration)).await
}

fn validate_project_git_endpoints(git: Option<&ProjectGitView>) -> Result<()> {
    if let Some(git) = git {
        for endpoint in git.fetch_remotes.values() {
            crate::git_actions::validate_approved_endpoint(endpoint)?;
        }
    }
    Ok(())
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

pub(crate) async fn verify_managed_project_resume_workspace(
    registration: &ProjectRegistrationView,
    workspace: &eliot_core::WorkspacePreparation,
    agent: &eliot_core::Agent,
    command: &eliot_core::Command,
) -> Result<()> {
    registration.validate()?;
    projects::verify_managed_project_resume_workspace(
        &project_registration_snapshot(registration),
        workspace,
        agent,
        command,
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
    let (revision, registrations) = match projects::load_registrations(data_root).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let log_ids = [
                ("producer", "automation.project_registration"),
                ("method", "project.registrations.reload"),
                ("path", "config.toml"),
            ];
            report_rejected_source(policy, &error, owner_config_notices, &log_ids)?;
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
    let views = match router
        .reconcile_configured_projects(&inputs, revision.as_deref())
        .await
    {
        Ok(views) => views,
        Err(error) => {
            let log_ids = [
                ("producer", "automation.project_registration"),
                ("method", "project.registrations.reload"),
                ("path", "config.toml"),
            ];
            report_rejected_source(policy, &error, owner_config_notices, &log_ids)?;
            for namespace in namespaces.iter_mut() {
                namespace.registration_current = false;
            }
            return Ok(false);
        }
    };
    let current_ids: HashSet<_> = views.iter().map(|view| view.id.clone()).collect();
    namespaces.retain(|namespace| {
        let current = current_ids.contains(&namespace.registration.id);
        if !current {
            router.install_rule_source_files(&namespace.principal, &HashMap::new());
            router.install_rule_definitions(&namespace.principal, &[], Vec::new());
        }
        current
    });

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
            let project_id = registration.id.clone();
            match initialize_project_namespace(router, data_root, policy, registration, view).await
            {
                Ok(namespace) => namespaces.push(namespace),
                Err(error) => {
                    let log_ids = [
                        ("producer", "automation.project_rules"),
                        ("method", "project.rules.load"),
                        ("path", ".eliot/rules/*.toml"),
                        ("project", project_id.as_str()),
                    ];
                    logging::record(
                        policy,
                        "daemon.rule_reload_rejected",
                        &log_ids,
                        Some(&error),
                    );
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
                let log_ids = [
                    ("producer", "automation.project_rules"),
                    ("method", "project.rules.reload"),
                    ("path", ".eliot/rules/*.toml"),
                    ("project", namespace.registration.id.as_str()),
                ];
                report_rejected_source(policy, &error, &mut namespace.notices, &log_ids)?;
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
            let log_ids = [
                ("producer", "automation.project_rules"),
                ("method", "project.rules.reload"),
                ("path", ".eliot/rules/*.toml"),
                ("project", namespace.registration.id.as_str()),
            ];
            report_rejected_source(policy, &error, &mut namespace.notices, &log_ids)?;
            continue;
        }
        let snapshot = loaded.snapshot;
        let current = namespace
            .rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if namespace.rules.iter().all(|rule| rule.source_present) && snapshot.matches(&current)? {
            router.install_rule_source_files(&namespace.principal, &snapshot.source_files);
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
    ids: &[(&str, &str)],
) -> Result<()> {
    if claim_notice(reload_notice_key(inputs)?, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", ids, Some(error));
    }
    Ok(())
}

fn report_rejected_source(
    policy: &SecretPolicy,
    error: &Error,
    notices: &mut ReloadNoticeContext,
    ids: &[(&str, &str)],
) -> Result<()> {
    let details = json!({"code": error.code, "message": error.message});
    let key = ReloadNoticeKey::Rejection(
        canonical_json_sha256(&details)
            .map_err(|_| Error::new("internal", "reload notice could not be canonicalized"))?,
    );
    if claim_notice(key, &mut notices.rejected) {
        logging::record(policy, "daemon.rule_reload_rejected", ids, Some(error));
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
    let source_ids: HashSet<_> = snapshot
        .inputs
        .iter()
        .map(|input| input.id.as_str())
        .collect();
    let absent_from_source: Vec<_> = effective
        .inputs
        .iter()
        .map(|input| input.id.as_str())
        .filter(|id| !source_ids.contains(id))
        .collect();
    let states = router
        .initialize_rules(
            principal,
            &effective.inputs,
            &timers,
            registration_now,
            &absent_from_source,
            &snapshot.source_files,
        )
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
    let completion_interests = active
        .iter()
        .filter(|rule| {
            rule.source_present && rule.state.enabled && rule.state.paused_reason.is_none()
        })
        .filter_map(|rule| rule.definition.completion_interest())
        .collect();
    router.install_rule_definitions(principal, &cached, completion_interests);
    Ok(active)
}

async fn reload_owner_snapshot(
    router: &Router,
    policy: &SecretPolicy,
    notices: &mut ReloadNoticeContext,
    snapshot: &reload::RuleSnapshot,
    rules: &[ActiveRule],
) -> Result<Option<(Vec<ActiveRule>, bool)>> {
    let mut reporter = ReloadReporter { policy, notices };
    attempt_reload_snapshot(
        router,
        "owner",
        None,
        snapshot,
        rules,
        now_ms(),
        &mut reporter,
    )
    .await
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
        Err(error) if rejected_reload(&error) => {
            let mut log_ids = vec![
                ("producer", "automation.rule_reload"),
                ("method", "rules.reload"),
                (
                    "path",
                    if project.is_some() {
                        ".eliot/rules/*.toml"
                    } else {
                        "rules/*.toml"
                    },
                ),
            ];
            if let Some(project) = project {
                log_ids.push(("project", project.id.as_str()));
            }
            report_rejected_reload(
                reporter.policy,
                &snapshot.inputs,
                &error,
                reporter.notices,
                &log_ids,
            )?;
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
        source_files: snapshot.source_files.clone(),
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
    for processing in states
        .into_iter()
        .filter(|state| state.source_present || state.view.retiring_revision.is_some())
    {
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
            source_present: processing.source_present,
            state: processing.view,
            retiring,
            timer,
        });
    }
    Ok(active)
}

async fn join_cleanup_task(task: &mut Option<tokio::task::JoinHandle<Option<String>>>) {
    if let Some(task) = task.take() {
        let _ = task.await;
    }
}

pub(crate) async fn run_global_rules(
    router: Router,
    start: RuleWorkerStart,
    shutdown: watch::Receiver<bool>,
) -> Result<RuleWorkerExit> {
    let mut cleanup_task = None;
    let result = run_global_rules_inner(router, start, shutdown, &mut cleanup_task).await;
    join_cleanup_task(&mut cleanup_task).await;
    result
}

#[expect(
    clippy::disallowed_methods,
    reason = "The rule worker owns this bounded arbitration wait."
)]
async fn run_global_rules_inner(
    router: Router,
    start: RuleWorkerStart,
    mut shutdown: watch::Receiver<bool>,
    cleanup_task: &mut Option<tokio::task::JoinHandle<Option<String>>>,
) -> Result<RuleWorkerExit> {
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
    let mut arbitration = router.launch_arbitration_receiver();
    router.note_worker_task_running("rule_worker");
    let mut watcher = match reload::RuleWatcher::watch(&data_root) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            let log_ids = [
                ("producer", "automation.rule_watcher"),
                ("method", "rules.watch"),
                ("path", "rules/"),
            ];
            logging::record(
                &policy,
                "daemon.rule_watch_unavailable",
                &log_ids,
                Some(&error),
            );
            None
        }
    };
    let mut project_watchers = Vec::new();
    for namespace in &project_rules {
        match reload::RuleWatcher::watch_project(Path::new(&namespace.registration.checkout)) {
            Ok(watcher) => project_watchers.push(watcher),
            Err(error) => {
                let log_ids = [
                    ("producer", "automation.rule_watcher"),
                    ("method", "project.rules.watch"),
                    ("path", ".eliot/rules/"),
                    ("project", namespace.registration.id.as_str()),
                ];
                logging::record(
                    &policy,
                    "daemon.rule_watch_unavailable",
                    &log_ids,
                    Some(&error),
                );
            }
        }
    }
    let mut reconcile = tokio::time::interval(RULE_RECONCILE_INTERVAL);
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cleanup_after = None;
    let mut last_cleanup_at: Option<std::time::Instant> = None;
    let mut cleanup_enabled = false;
    let mut notices = initial_notices;
    let mut owner_config_notices = ReloadNoticeContext::default();
    let mut max_event_backlog = initial_max_event_backlog;
    let mut last_applied_inputs = None;
    let mut recover_rule_children = true;
    let mut arbitrate_launches = true;
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
            let log_ids = [
                ("producer", "automation.configuration"),
                ("method", "automation.config.reload"),
                ("path", "config.toml"),
            ];
            report_rejected_source(&policy, &error, &mut owner_config_notices, &log_ids)?;
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
        if rules.iter().all(|rule| rule.source_present)
            && snapshot.matches(&current).unwrap_or(false)
        {
            router.install_rule_source_files("owner", &snapshot.source_files);
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
                let update =
                    reload_owner_snapshot(&router, &policy, &mut notices, &snapshot, &rules)
                        .await?;
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
            join_cleanup_task(cleanup_task).await;
            return Ok(RuleWorkerExit::Shutdown);
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
                join_cleanup_task(cleanup_task).await;
                return Ok(RuleWorkerExit::AdmissionClosed);
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
                    join_cleanup_task(cleanup_task).await;
                    return Ok(RuleWorkerExit::AdmissionClosed);
                }
            }

            let arbitrate_now = std::mem::take(&mut arbitrate_launches);
            if !reconcile_due_timers_with_arbitration(
                &router,
                "owner",
                &mut rules,
                now_ms(),
                arbitrate_now,
            )
            .await?
            {
                join_cleanup_task(cleanup_task).await;
                return Ok(RuleWorkerExit::AdmissionClosed);
            }
            for namespace in &mut project_rules {
                if namespace.registration_current
                    && !reconcile_due_timers(
                        &router,
                        &namespace.principal,
                        &mut namespace.rules,
                        now_ms(),
                    )
                    .await?
                {
                    join_cleanup_task(cleanup_task).await;
                    return Ok(RuleWorkerExit::AdmissionClosed);
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
        let mut cleanup_requested = false;
        let mut shutdown_requested = false;
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    shutdown_requested = true;
                }
            }
            changed = progress.changed() => {
                if changed.is_err() {
                    join_cleanup_task(cleanup_task).await;
                    return Ok(RuleWorkerExit::Exited);
                }
                recover_rule_children = true;
            }
            changed = arbitration.changed() => {
                if changed.is_err() {
                    join_cleanup_task(cleanup_task).await;
                    return Ok(RuleWorkerExit::Exited);
                }
                arbitrate_launches = true;
            }
            received = events.recv() => match received {
                Ok(event) => {
                    router.note_unknown(std::slice::from_ref(&event));
                    if let Err(error) = router.deliver_project_guard(&event).await {
                        logging::record(
                            &router.secret_policy_snapshot(),
                            "daemon.project_guard_delivery_failed",
                            &[("event_seq", &event.seq.to_string())],
                            Some(&error),
                        );
                    }
                    recover_rule_children = true;
                    if event_wakes_launch_arbitration(&event) {
                        arbitrate_launches = true;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    router.clear_terminal_unknown_cache().await;
                    recover_rule_children = true;
                    arbitrate_launches = true;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    join_cleanup_task(cleanup_task).await;
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
            },
            _ = reconcile.tick() => {
                reload_requested = true;
                cleanup_requested = true;
                arbitrate_launches = true;
            },
            _ = async {
                if let Some(watcher) = &mut watcher {
                    let _ = watcher.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                reload_requested = true;
                arbitrate_launches = true;
            },
            _ = wait_project_rule_watchers(&mut project_watchers) => {
                reload_requested = true;
                arbitrate_launches = true;
            },
            _ = async {
                if let Some(wait) = timer_wait {
                    tokio::time::sleep(wait).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                arbitrate_launches = true;
            },
            completed = async {
                match cleanup_task.as_mut() {
                    Some(task) => Some(task.await),
                    None => std::future::pending::<Option<std::result::Result<Option<String>, tokio::task::JoinError>>>().await,
                }
            } => {
                *cleanup_task = None;
                cleanup_after = if cleanup_enabled {
                    completed
                        .and_then(std::result::Result::ok)
                        .unwrap_or(None)
                } else {
                    None
                };
            }
        }
        if shutdown_requested {
            join_cleanup_task(cleanup_task).await;
            return Ok(RuleWorkerExit::Shutdown);
        }
        if let Some(watcher) = &mut watcher {
            // Re-arm the rules-directory watch after creation or
            // replacement; the periodic reconciliation below reloads from
            // disk regardless.
            watcher.ensure_watches();
        }
        if recover_rule_children {
            router
                .recover_rule_children(RULE_CHILD_RECOVERY_PAGE_SIZE)
                .await?;
            recover_rule_children = false;
        }
        for rule in &mut rules {
            if let Some(timer) = &mut rule.timer
                && (arbitrate_launches
                    || reload_requested
                    || !is_assignment_timer_rule(&rule.definition))
            {
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
                cleanup_enabled = config.auto_cleanup != Some(false);
                if !cleanup_enabled {
                    cleanup_after = None;
                    last_cleanup_at = None;
                } else if cleanup_requested
                    && cleanup_task.is_none()
                    && last_cleanup_at.is_none_or(|last| last.elapsed() >= Duration::from_secs(60))
                {
                    last_cleanup_at = Some(std::time::Instant::now());
                    let mut after = cleanup_after.take();
                    let cleanup_router = router.clone();
                    *cleanup_task = Some(tokio::spawn(async move {
                        cleanup_router.auto_cleanup_worktrees(&mut after).await;
                        after
                    }));
                }
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
                cleanup_enabled = false;
                cleanup_after = None;
                last_cleanup_at = None;
                let log_ids = [
                    ("producer", "automation.configuration"),
                    ("method", "automation.config.reload"),
                    ("path", "config.toml"),
                ];
                report_rejected_source(&policy, &error, &mut owner_config_notices, &log_ids)?;
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

        router.refresh_assignment_pauses().await?;
        let snapshot = match reload::load(&data_root) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let log_ids = [
                    ("producer", "automation.rule_reload"),
                    ("method", "rules.reload"),
                    ("path", "rules/*.toml or config.toml"),
                ];
                report_rejected_source(&policy, &error, &mut notices, &log_ids)?;
                continue;
            }
        };
        let snapshot_key = reload_notice_key(&snapshot.inputs)?;
        if notices.rejected.as_ref() == Some(&snapshot_key) {
            continue;
        }
        if last_applied_inputs.as_ref() == Some(&snapshot.inputs) {
            router.install_rule_source_files("owner", &snapshot.source_files);
            notices.deferred = None;
            notices.rejected = None;
            continue;
        }
        let current_definitions = rules
            .iter()
            .map(|rule| rule.definition.clone())
            .collect::<Vec<_>>();
        if rules.iter().all(|rule| rule.source_present) && snapshot.matches(&current_definitions)? {
            router.install_rule_source_files("owner", &snapshot.source_files);
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
        let update =
            reload_owner_snapshot(&router, &policy, &mut notices, &snapshot, &rules).await?;
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
                router.remove_completion_interest(&rule.principal, &rule.state.id);
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

fn assignment_cleanup_key<'a>(
    principal: &str,
    project_scoped: bool,
    rule_id: &'a str,
    cause_rule: Option<&str>,
) -> Option<&'a str> {
    if principal != "owner" || project_scoped {
        return None;
    }
    let key = rule_id
        .strip_prefix("assignment-")?
        .strip_suffix(".complete")?;
    let expected_cause = format!("assignment-{key}");
    if key.is_empty()
        || key.len() > 128
        || !key
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        || cause_rule != Some(expected_cause.as_str())
    {
        return None;
    }
    Some(key)
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
            let revision = if retiring {
                rule.retiring
                    .as_ref()
                    .map(|retiring| retiring.revision)
                    .ok_or_else(|| Error::new("source_integrity", "retiring rule is absent"))?
            } else {
                rule.state.revision
            };
            if let Some(admission) = router
                .replay_rule_event_action(&rule.principal, &rule.state, revision, event.seq)
                .await?
            {
                rule.state = admission.state;
                refresh_retiring(rule)?;
                no_match_through = None;
                continue;
            }
            if matches!(event.kind.as_str(), "turn.completed" | "goal.changed")
                && let Some(key) = assignment_cleanup_key(
                    &rule.principal,
                    rule.project.is_some(),
                    &definition.id,
                    event.cause.as_ref().and_then(|cause| cause.rule.as_deref()),
                )
                && !router.is_assignment_event(&event, key).await?
            {
                no_match_through = Some(event.seq);
                continue;
            }
            let (action, once_per_key, rate_key) =
                match evaluate_event_action(router, &rule.principal, &definition, &event).await {
                    Ok(RuleEvaluation::Command {
                        action,
                        once_per,
                        rate,
                    }) => (action, once_per, rate),
                    Ok(RuleEvaluation::NoMatch) => {
                        no_match_through = Some(event.seq);
                        continue;
                    }
                    Ok(RuleEvaluation::CauseDepth) => {
                        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
                        advance_rule_cursor(
                            router,
                            rule,
                            retiring,
                            event.seq,
                            RuleCursorOutcome::SkippedCauseDepth,
                        )
                        .await?;
                        continue;
                    }
                    Ok(RuleEvaluation::IncompleteKey) => {
                        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
                        advance_rule_cursor(
                            router,
                            rule,
                            retiring,
                            event.seq,
                            RuleCursorOutcome::Suppressed,
                        )
                        .await?;
                        continue;
                    }
                    Ok(RuleEvaluation::LaunchResultInsertion { .. }) => {
                        unreachable!("insertion was resolved")
                    }
                    Err(error) => {
                        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
                        advance_rule_cursor(
                            router,
                            rule,
                            retiring,
                            event.seq,
                            rule_cursor_rejection(router, &error),
                        )
                        .await?;
                        continue;
                    }
                };

            flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
            if !router.rule_firings_open() {
                return Ok(false);
            }
            let prepared_result = if action.verb == "run" {
                prepare_event_run_submission(router, rule, retiring, &definition, &event, action)
                    .await
            } else {
                match rule.project.as_ref() {
                    Some(project) => {
                        router
                            .prepare_project_rule_submission(action, project)
                            .await
                    }
                    None => router.prepare_owner_submission(action).await,
                }
            };
            let mut prepared = match prepared_result {
                Ok(prepared) => prepared,
                Err(_) if !router.rule_firings_open() => return Ok(false),
                Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        rule_cursor_rejection(router, &error),
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if definition.guards_launch_worktree() {
                prepared.request.args[crate::router::RULE_WORKTREE_GUARD_ARG] = json!(true);
            }
            let admission = match router
                .admit_rule_action(
                    &rule.principal,
                    rule.project.as_ref(),
                    &rule.state,
                    revision,
                    eliot_store::RuleEventInput {
                        seq: event.seq,
                        once_per_key,
                        rate_key,
                    },
                    prepared,
                )
                .await
            {
                Ok(Some(admission)) => admission,
                Ok(None) => return Ok(false),
                Err(_) if !router.rule_firings_open() => return Ok(false),
                Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                    advance_rule_cursor(
                        router,
                        rule,
                        retiring,
                        event.seq,
                        rule_cursor_rejection(router, &error),
                    )
                    .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            rule.state = admission.state;
            refresh_retiring(rule)?;
            // Router schedules only a new child after Store commit and lock release.
        }
        flush_rule_no_match(router, rule, retiring, &mut no_match_through).await?;
    }
    Ok(true)
}

async fn prepare_event_run_submission(
    router: &Router,
    rule: &ActiveRule,
    retiring: bool,
    definition: &RuleDefinition,
    event: &Event,
    action: eliot_core::SubmitCommand,
) -> Result<crate::router::PreparedSubmission> {
    let revision = if retiring {
        rule.retiring
            .as_ref()
            .map(|retiring| retiring.revision)
            .ok_or_else(|| Error::new("source_integrity", "retiring run revision is absent"))?
    } else {
        rule.state.revision
    };
    match rule.project.as_ref() {
        Some(project) => {
            let action =
                freeze_project_run_action(router, project, definition, event, revision, action)?;
            router
                .prepare_project_rule_submission(action, project)
                .await
        }
        None if rule.principal == "owner" => {
            router
                .prepare_owner_rule_run_submission(
                    action,
                    frozen_script_context(definition, event, revision),
                )
                .await
        }
        None => Err(Error::new(
            "not_allowed",
            "global event Run requires the Owner rule principal",
        )),
    }
}

fn frozen_script_context(
    definition: &RuleDefinition,
    event: &Event,
    revision: i64,
) -> eliot_core::FrozenScriptContext {
    FrozenScriptContext::Event {
        event: Box::new(event.clone()),
        subject: event.subject.clone(),
        rule: ScriptRuleContext {
            id: definition.id.clone(),
            revision,
        },
    }
}

pub(crate) fn timer_script_context(
    rule_id: &str,
    revision: i64,
    state: &TimerStateView,
    invocation: ScriptTimerInvocation,
) -> FrozenScriptContext {
    FrozenScriptContext::Timer {
        timer: ScriptTimerContext {
            schedule_revision: state.schedule_revision,
            schedule: state.schedule.clone(),
            anchor: state.anchor,
            invocation,
        },
        rule: ScriptRuleContext {
            id: rule_id.to_owned(),
            revision,
        },
    }
}

fn freeze_project_run_action(
    router: &Router,
    project: &ProjectRegistrationView,
    definition: &RuleDefinition,
    event: &Event,
    revision: i64,
    mut action: eliot_core::SubmitCommand,
) -> Result<eliot_core::SubmitCommand> {
    let raw_definition: eliot_core::RunDefinitionArgs = serde_json::from_value(action.args.clone())
        .map_err(|_| Error::new("invalid", "run definition is malformed"))?;
    let context = frozen_script_context(definition, event, revision);
    let policy = router.secret_policy_snapshot();
    let frozen =
        crate::scripts::freeze_project_run(raw_definition, project, context, policy.as_ref())?;
    action.args = serde_json::to_value(frozen)
        .map_err(|_| Error::new("invalid", "frozen run inputs cannot be encoded"))?;
    Ok(action)
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

fn is_assignment_timer_rule(definition: &RuleDefinition) -> bool {
    definition
        .evaluate_timer()
        .is_ok_and(|action| action.args.get("assignment").is_some())
}

fn event_wakes_launch_arbitration(event: &Event) -> bool {
    matches!(
        event.kind.as_str(),
        "capacity.released" | "provider.hold_cleared" | "project.transferred"
    )
}

async fn reconcile_due_timers(
    router: &Router,
    principal: &str,
    rules: &mut [ActiveRule],
    now: i64,
) -> Result<bool> {
    reconcile_due_timers_with_arbitration(router, principal, rules, now, true).await
}

async fn reconcile_due_timers_with_arbitration(
    router: &Router,
    principal: &str,
    rules: &mut [ActiveRule],
    now: i64,
    arbitrate_launches: bool,
) -> Result<bool> {
    struct TimerHead {
        index: usize,
        state: TimerStateView,
        route: Option<String>,
        retained_wait: Option<eliot_store::TimerWait>,
    }

    enum Head {
        Timer(Box<TimerHead>),
        Interactive(Box<eliot_store::PendingLaunch>),
    }

    for attempt in 0..MAX_ARBITRATION_RETRIES {
        let mut after = None;
        let mut deferred_timers = Vec::new();
        let mut heads = std::collections::BTreeMap::<String, (i64, Head)>::new();
        let (cursor, priorities, intents) = if principal == "owner" && arbitrate_launches {
            router.launch_arbitration().await?
        } else {
            Default::default()
        };
        // Store returns intents in commit order within each millisecond.
        for intent in intents {
            heads
                .entry(intent.view.project.clone())
                .or_insert_with(|| (intent.created_at, Head::Interactive(Box::new(intent))));
        }
        loop {
            let page = router
                .list_due_rule_timers(principal, after.as_deref(), now, TIMER_PAGE_SIZE)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.id.clone());
            for state in page {
                let index = rules
                    .iter()
                    .position(|rule| {
                        rule.principal == principal
                            && rule
                                .timer
                                .as_ref()
                                .is_some_and(|timer| timer.state.id == state.id)
                    })
                    .ok_or_else(|| {
                        Error::new("source_integrity", "stored timer has no loaded definition")
                    })?;
                if !state.enabled
                    || rules[index]
                        .timer
                        .as_ref()
                        .is_some_and(|timer| timer.blocked)
                {
                    continue;
                }
                let assignment = (principal == "owner")
                    .then(|| rules[index].definition.evaluate_timer())
                    .and_then(std::result::Result::ok)
                    .filter(|action| action.args.get("assignment").is_some());
                if let Some(action) = assignment {
                    if !arbitrate_launches {
                        continue;
                    }
                    if router.rule_has_open_actions(principal, &state.id).await? {
                        deferred_timers.push(index);
                        continue;
                    }
                    let retained = router.arbitration_timer_wait(&state).await?;
                    let retained_wait = retained.as_ref().map(|(_, wait)| wait);
                    let route = retained_wait.and_then(|wait| wait.route.clone());
                    let wait_copy = retained_wait.cloned();
                    let project = match router.queued_launch_project(&action).await {
                        Ok(project) => project,
                        Err(error) => {
                            if error.code != "workspace_unavailable" {
                                crate::logging::record(
                                    &router.secret_policy_snapshot(),
                                    "assignment.project_wait",
                                    &[
                                        ("producer", "automation.assignment"),
                                        ("method", "assignment.project_wait"),
                                        ("rule", state.id.as_str()),
                                    ],
                                    Some(&error),
                                );
                            }
                            retained.map(|(project, _)| project).unwrap_or_else(|| {
                                action.args["workspace"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned()
                            })
                        }
                    };
                    let due = state.next_due_at.unwrap_or(now);
                    let timer_head = || {
                        Head::Timer(Box::new(TimerHead {
                            index,
                            state,
                            route,
                            retained_wait: wait_copy,
                        }))
                    };
                    match heads.entry(project) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert((due, timer_head()));
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            if due < entry.get().0 {
                                if let (_, Head::Timer(previous)) =
                                    entry.insert((due, timer_head()))
                                {
                                    deferred_timers.push(previous.index);
                                }
                            } else {
                                deferred_timers.push(index);
                            }
                        }
                    }
                } else if attempt == 0 {
                    match reconcile_timer(router, &mut rules[index], state, now, None).await? {
                        TimerReconcileOutcome::AdmissionClosed => return Ok(false),
                        TimerReconcileOutcome::Continue
                        | TimerReconcileOutcome::CapacityWait(_)
                        | TimerReconcileOutcome::RetryPass => {}
                    }
                }
            }
        }
        for &index in &deferred_timers {
            if let Some(timer) = rules[index].timer.as_mut() {
                timer.blocked = true;
            }
        }
        if heads.is_empty() {
            return Ok(true);
        }
        let mut heads = heads.into_iter().collect::<Vec<_>>();
        heads.sort_by_cached_key(|(project, (due, _))| {
            arbitration_order(project, *due, now, cursor.as_deref(), &priorities)
        });
        let mut blocked_scopes = std::collections::HashSet::new();
        let mut attempted_assignments = Vec::new();
        let mut retry_pass = false;
        for (project, (_, head)) in heads {
            match head {
                Head::Interactive(intent) => {
                    if intent.view.reason.as_ref().is_some_and(|reason| {
                        intent.route.is_some()
                            && reason.code == "capacity_unavailable"
                            && blocked_scopes.contains(&reason.scope)
                    }) {
                        continue;
                    }
                    match router
                        .try_launch_intent_in_pass(&intent, &blocked_scopes)
                        .await?
                    {
                        crate::router::launch_queue::LaunchArbitrationAttempt::CapacityWait(
                            scope,
                        ) => {
                            blocked_scopes.insert(scope);
                        }
                        crate::router::launch_queue::LaunchArbitrationAttempt::RetryPass => {
                            retry_pass = true;
                            break;
                        }
                        crate::router::launch_queue::LaunchArbitrationAttempt::Admitted
                        | crate::router::launch_queue::LaunchArbitrationAttempt::Other => {}
                    }
                }
                Head::Timer(timer_head) => {
                    let TimerHead {
                        index,
                        state,
                        route,
                        retained_wait,
                    } = *timer_head;
                    attempted_assignments.push(index);
                    if retained_wait.as_ref().is_some_and(|wait| {
                        wait.route.is_some()
                            && wait.reason.code == "capacity_unavailable"
                            && blocked_scopes.contains(&wait.reason.scope)
                    }) {
                        if let Some(timer) = rules[index].timer.as_mut() {
                            timer.blocked = true;
                        }
                        continue;
                    }
                    let arb = eliot_store::ArbitratedTimer {
                        project,
                        route,
                        owner_automation: retained_wait
                            .as_ref()
                            .is_none_or(|wait| wait.owner_automation),
                        wait: None,
                        blocked_scopes: blocked_scopes.clone(),
                    };
                    match reconcile_timer(router, &mut rules[index], state, now, Some(arb)).await? {
                        TimerReconcileOutcome::AdmissionClosed => return Ok(false),
                        TimerReconcileOutcome::CapacityWait(scope) => {
                            blocked_scopes.insert(scope);
                        }
                        TimerReconcileOutcome::RetryPass => {
                            retry_pass = true;
                            break;
                        }
                        TimerReconcileOutcome::Continue => {}
                    }
                }
            }
        }
        if !retry_pass {
            return Ok(true);
        }
        for index in attempted_assignments.into_iter().chain(deferred_timers) {
            if let Some(timer) = rules[index].timer.as_mut() {
                timer.blocked = false;
            }
        }
    }
    router.signal_launch_arbitration();
    Ok(true)
}

fn arbitration_order(
    project: &str,
    ready_at: i64,
    now: i64,
    cursor: Option<&str>,
    priorities: &std::collections::HashMap<String, u8>,
) -> (bool, std::cmp::Reverse<u8>, bool, String) {
    let protected = priorities.values().any(|&priority| priority > 0)
        && now.saturating_sub(ready_at) >= PRIORITY_WAIT_LIMIT_MS;
    let priority = if protected {
        0
    } else {
        priorities.get(project).copied().unwrap_or(0)
    };
    let wrapped = cursor.is_some_and(|cursor| project <= cursor);
    (
        !protected,
        std::cmp::Reverse(priority),
        wrapped,
        project.to_owned(),
    )
}

enum TimerReconcileOutcome {
    Continue,
    CapacityWait(String),
    RetryPass,
    AdmissionClosed,
}

async fn reconcile_timer(
    router: &Router,
    rule: &mut ActiveRule,
    state: TimerStateView,
    now: i64,
    mut arbitration: Option<eliot_store::ArbitratedTimer>,
) -> Result<TimerReconcileOutcome> {
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
    if arbitration.is_some() {
        // The due timer is the durable waiting head. Do not turn a wait
        // into a missed schedule on restart or a later reconciliation.
        timer.armed_due_at_ms = timer.state.next_due_at;
    }
    if !timer.state.enabled || timer.blocked {
        return Ok(TimerReconcileOutcome::Continue);
    }

    let (scheduled_at_ms, next_due_at_ms, action, skip_code, skip_capacity_message, armed_slot) =
        match timer_decision(timer, now)? {
            TimerDecision::NotDue => return Ok(TimerReconcileOutcome::Continue),
            TimerDecision::Skip {
                scheduled_at_ms,
                next_due_at_ms,
                code,
            } => (
                scheduled_at_ms,
                next_due_at_ms,
                None,
                Some(code),
                None,
                None,
            ),
            TimerDecision::Fire {
                scheduled_at_ms,
                next_due_at_ms,
                live_armed,
            } => {
                if let Some(admission) = router
                    .replay_rule_timer_slot(&rule.principal, &timer.state, scheduled_at_ms)
                    .await?
                {
                    timer.state = admission.state;
                    timer.blocked = admission.blocked;
                    timer.armed_due_at_ms = timer.state.next_due_at.filter(|due| *due > now);
                    return Ok(TimerReconcileOutcome::Continue);
                }

                // A per-assignment briefs failure (for example a missing
                // briefs folder, `unavailable`) fails only this slot, so
                // the worker keeps running every other automation. The
                // code is recorded as the slot skip reason below.
                let mut assignment_unavailable: Option<String> = None;
                let prepared = match rule.definition.evaluate_timer() {
                    Ok(action) if action.verb == "run" => {
                        router
                            .prepare_timer_rule_run_submission(
                                action,
                                rule.project.as_ref(),
                                timer_script_context(
                                    &rule.definition.id,
                                    rule.state.revision,
                                    &timer.state,
                                    ScriptTimerInvocation::Scheduled { scheduled_at_ms },
                                ),
                            )
                            .await
                    }
                    Ok(action) => {
                        if action.args.get("assignment").is_some() && rule.project.is_some() {
                            Err(Error::new(
                                "not_allowed",
                                "assignments require an Owner timer",
                            ))
                        } else {
                            match assignment::prepare(router, action, None).await {
                                Ok(action) => match rule.project.as_ref() {
                                    Some(project) => {
                                        router
                                            .prepare_project_rule_submission(action, project)
                                            .await
                                    }
                                    None if let Some(arb) = &arbitration => {
                                        router
                                            .prepare_assignment_launch(action, arb.owner_automation)
                                            .await
                                    }
                                    None => router.prepare_owner_submission(action).await,
                                },
                                Err(error) if error.code == "unavailable" => {
                                    assignment_unavailable = Some(error.code.clone());
                                    Err(error)
                                }
                                Err(error) => Err(error),
                            }
                        }
                    }
                    Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                        Err(error)
                    }
                    Err(error) => return Err(error),
                };
                let prepared = match (prepared, arbitration.as_mut()) {
                    (Ok(prepared), Some(arb))
                        if prepared.request.args["registered_project"]
                            .as_str()
                            .or_else(|| prepared.request.args["project"].as_str())
                            != Some(arb.project.as_str()) =>
                    {
                        Err(Error::new(
                            "workspace_unavailable",
                            "assignment project changed during route preparation",
                        ))
                    }
                    (Ok(prepared), Some(arb)) => {
                        match crate::router::launch_queue::checked_route(
                            &prepared.request,
                            arb.route.as_deref(),
                            arb.owner_automation,
                        ) {
                            Ok(route) => {
                                arb.route = route;
                                Ok(prepared)
                            }
                            Err(error) => Err(error),
                        }
                    }
                    (result, _) => result,
                };
                if let Some(arb) = arbitration.as_mut()
                    && let Err(error) = &prepared
                    && assignment_unavailable.is_none()
                {
                    arb.wait = eliot_store::TimerWait::from_error(
                        error,
                        arb.route.clone(),
                        arb.owner_automation,
                    )
                    .map(|wait| wait.reason)
                    .or_else(|| {
                        (!is_deterministic_pre_admission_rejection(&error.code)
                            && router.rule_firings_open())
                        .then(|| eliot_core::api::LaunchWaitReason {
                            code: error.code.clone(),
                            scope: "assignment".into(),
                            changed_by: "owner".into(),
                        })
                    });
                }
                if let Some(code) = assignment_unavailable {
                    if !router.rule_firings_open() {
                        return Ok(TimerReconcileOutcome::AdmissionClosed);
                    }
                    (
                        scheduled_at_ms,
                        next_due_at_ms,
                        None,
                        Some(code),
                        None,
                        None,
                    )
                } else {
                    match prepared {
                        Ok(prepared) => (
                            scheduled_at_ms,
                            next_due_at_ms,
                            Some(prepared.request),
                            None,
                            None,
                            live_armed.then_some(scheduled_at_ms),
                        ),
                        Err(error)
                            if arbitration.as_ref().is_some_and(|arb| arb.wait.is_some())
                                || is_deterministic_pre_admission_rejection(&error.code) =>
                        {
                            (
                                scheduled_at_ms,
                                next_due_at_ms,
                                None,
                                Some(error.code.clone()),
                                (error.code == "capacity_unavailable"
                                    && error.failure_where.as_deref()
                                        == Some("store.admission.capacity"))
                                .then_some(*error.message),
                                None,
                            )
                        }
                        Err(_error) if !router.rule_firings_open() => {
                            return Ok(TimerReconcileOutcome::AdmissionClosed);
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        };

    let slot = TimerSlotCommit {
        scheduled_at_ms,
        next_due_at_ms,
        action,
        skip_code,
        skip_capacity_message,
    };
    let was_arbitrated = arbitration.is_some();
    let prepared_capacity_scope = arbitration
        .as_ref()
        .and_then(|arb| arb.wait.as_ref())
        .filter(|wait| wait.code == "capacity_unavailable")
        .map(|wait| wait.scope.clone());
    let admission = match router
        .commit_rule_timer_slot(
            &rule.principal,
            rule.project.as_ref(),
            &timer.state,
            slot,
            arbitration,
            now,
        )
        .await
    {
        Ok(Some(admission)) => admission,
        Ok(None) => return Ok(TimerReconcileOutcome::AdmissionClosed),
        Err(error) if error.code == "arbitration_retry" => {
            return Ok(TimerReconcileOutcome::RetryPass);
        }
        Err(error) => return Err(error),
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
    if was_arbitrated && timer.blocked {
        let capacity_scope = match prepared_capacity_scope {
            Some(scope) => Some(scope),
            None => router
                .arbitration_timer_wait(&timer.state)
                .await?
                .map(|(_, wait)| wait.reason)
                .filter(|reason| reason.code == "capacity_unavailable")
                .map(|reason| reason.scope),
        };
        if let Some(scope) = capacity_scope {
            return Ok(TimerReconcileOutcome::CapacityWait(scope));
        }
    }
    Ok(TimerReconcileOutcome::Continue)
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

/// Catch-up preparation must verify the frozen Agent target and generation
/// before admission exactly for the Agent-bound project verbs. This is the
/// single closed project-rule verb classification enforced by
/// `authorize_command` (router/auth.rs): targetless coordination actions
/// (`run`, `launch`, `notify`, `emit`, `mail.send`, `var.set`) and project
/// Git actions (`git.fetch`) skip Agent scope checks here exactly as they
/// do on the live path. Unknown verbs stay fail-closed at ordinary
/// admission.
fn project_rule_catch_up_binds_agent(verb: &str) -> bool {
    crate::router::project_rule_action_binds_agent_target(verb)
}

async fn evaluate_event_action(
    router: &Router,
    principal: &str,
    definition: &RuleDefinition,
    event: &Event,
) -> Result<RuleEvaluation> {
    match definition::evaluate(definition, event, principal)? {
        RuleEvaluation::Command { .. }
            if definition.guards_launch_worktree()
                && !router.rule_worktree_is_current(principal, event).await? =>
        {
            Ok(RuleEvaluation::NoMatch)
        }
        RuleEvaluation::LaunchResultInsertion { insertion, keys } => {
            match router
                .rule_launch_result_insertion(
                    principal,
                    &insertion.from_launch,
                    insertion.to.as_deref(),
                    event,
                )
                .await?
            {
                Some(request) => Ok(keys.command(request)),
                None => Ok(RuleEvaluation::NoMatch),
            }
        }
        evaluation => Ok(evaluation),
    }
}

pub(crate) async fn prepare_rule_catch_up_actions(
    router: &Router,
    definition: &RuleDefinition,
    rule_revision: i64,
    events: &[Event],
    project: Option<&ProjectRegistrationView>,
) -> Result<Vec<eliot_store::RuleCatchUpAction>> {
    if definition.guards_launch_worktree() || definition.inserts_launch_result() {
        return Err(Error::new(
            "unsupported",
            "worktree-bound launches and result insertions require live event processing",
        ));
    }
    if definition.has_keyed_firing() {
        return Err(Error::new(
            "unsupported",
            "once_per and key-scoped limits do not support historical catch-up",
        ));
    }
    let mut actions = Vec::new();
    let mut rendered = Vec::new();
    let rule_principal = project.map_or_else(
        || "owner".to_owned(),
        |project| format!("project:{}", project.id),
    );
    for event in events {
        let request = match definition::evaluate(definition, event, &rule_principal) {
            Ok(RuleEvaluation::Command { action, .. }) => action,
            Ok(RuleEvaluation::NoMatch) => continue,
            Ok(RuleEvaluation::CauseDepth) => {
                actions.push(eliot_store::RuleCatchUpAction::skipped_cause_depth(
                    event.seq,
                ));
                continue;
            }
            Ok(RuleEvaluation::IncompleteKey | RuleEvaluation::LaunchResultInsertion { .. }) => {
                return Err(Error::new(
                    "unsupported",
                    "keyed rules do not support historical catch-up",
                ));
            }
            Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                actions.push(rule_catch_up_rejection(router, event.seq, &error));
                continue;
            }
            Err(error) => return Err(error),
        };
        rendered.push((event.clone(), request));
    }

    if let Some(project) = project {
        let requests = rendered
            .iter()
            .map(|(_, request)| request.clone())
            .collect::<Vec<_>>();
        if !requests.is_empty() {
            router
                .verify_project_rule_submission_authority(&requests, project)
                .await?;
        }
    }

    for (event, request) in rendered {
        let event_seq = event.seq;
        if let Some(project) = project
            && project_rule_catch_up_binds_agent(request.verb.as_str())
        {
            let target = request
                .target
                .as_deref()
                .ok_or_else(|| Error::new("invalid", "project action has no Agent target"))?;
            let generation = request.expected_generation.ok_or_else(|| {
                Error::new("invalid", "project action has no expected generation")
            })?;
            if !router
                .verify_project_rule_target_scope(project, target, generation)
                .await?
            {
                actions.push(eliot_store::RuleCatchUpAction::rejected_with_message(
                    event_seq,
                    "stale_generation",
                    "captured project Agent is no longer in scope",
                ));
                continue;
            }
            match router
                .verify_project_rule_submission_target_generation(&request)
                .await
            {
                Ok(()) => {}
                Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                    actions.push(rule_catch_up_rejection(router, event_seq, &error));
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        let prepared = if request.verb == "run" {
            match project {
                Some(project) => match freeze_project_run_action(
                    router,
                    project,
                    definition,
                    &event,
                    rule_revision,
                    request,
                ) {
                    Ok(action) => {
                        router
                            .prepare_project_rule_submission_after_authority(action, project)
                            .await
                    }
                    Err(error) => Err(error),
                },
                None => {
                    router
                        .prepare_owner_rule_run_submission(
                            request,
                            frozen_script_context(definition, &event, rule_revision),
                        )
                        .await
                }
            }
        } else {
            match project {
                Some(project) => {
                    router
                        .prepare_project_rule_submission_after_authority(request, project)
                        .await
                }
                None => router.prepare_owner_submission(request).await,
            }
        };
        match prepared {
            Ok(prepared) => actions.push(eliot_store::RuleCatchUpAction::command(
                event_seq,
                prepared.request,
            )),
            Err(error) if is_deterministic_pre_admission_rejection(&error.code) => {
                actions.push(rule_catch_up_rejection(router, event_seq, &error));
            }
            Err(error) => return Err(error),
        }
    }
    actions.sort_by_key(|action| action.event_seq);
    Ok(actions)
}

/// A definite refusal of a rules-file edit: the reload is rejected and the
/// prior rules keep running. Any other error is a real reload failure.
fn rejected_reload(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "frame_too_large" | "invalid" | "not_allowed" | "secret_in_payload" | "unsupported"
    )
}
