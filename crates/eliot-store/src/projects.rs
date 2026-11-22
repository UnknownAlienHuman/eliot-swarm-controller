//! Owner-registered project rule authority and dispatch fences.

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const PROJECT_PRINCIPAL_PREFIX: &str = "project:";
const PROJECT_POLICY_USAGE_WINDOW_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRegistrationInput {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub git: Option<core::api::ProjectGitView>,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    pub run_env: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectCommandBinding {
    registration_revision: i64,
    profile: Option<String>,
    workspace: Option<String>,
    repo_path: Option<String>,
    repo_common_dir: Option<String>,
    workspace_phase: Option<WorkspacePhase>,
    workspace_target_generation: Option<i64>,
}

impl Store {
    /// Add or replace one owner-approved project registration. A changed
    /// registration receives a fresh monotonic revision so admitted commands
    /// from the prior policy cannot cross dispatch.
    pub fn upsert_project_registration(
        &mut self,
        principal: &str,
        input: &ProjectRegistrationInput,
        _now: i64,
    ) -> Result<Change<core::api::ProjectRegistrationView>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_project_owner(principal)?;
            let input = normalize_registration(input)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let view = upsert_project_registration_tx(&tx, &input)?;
            ensure_project_registration_bytes(std::slice::from_ref(&view))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: view,
                events: Vec::new(),
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Reconcile the complete owner-configured registration set in one writer
    /// transaction. Removed projects immediately lose new admission authority.
    pub fn reconcile_project_registrations(
        &mut self,
        principal: &str,
        inputs: &[ProjectRegistrationInput],
        _now: i64,
    ) -> Result<Change<Vec<core::api::ProjectRegistrationView>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_project_owner(principal)?;
            if inputs.len() > MAX_PAGE_ENTRIES {
                return Err(Error::new(
                    "frame_too_large",
                    "project registration count exceeds its bound",
                ));
            }
            let inputs = inputs
                .iter()
                .map(normalize_registration)
                .collect::<Result<Vec<_>>>()?;
            let mut ids = HashSet::with_capacity(inputs.len());
            if inputs.iter().any(|input| !ids.insert(input.id.as_str())) {
                return Err(Error::new(
                    "invalid",
                    "project registrations contain duplicate IDs",
                ));
            }

            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let existing = project_registration_ids_tx(&tx)?;
            for id in existing {
                if !ids.contains(id.as_str()) {
                    tx.execute("DELETE FROM project_registrations WHERE id = ?1", [&id])
                        .map_err(sql_error)?;
                }
            }
            for input in &inputs {
                upsert_project_registration_tx(&tx, input)?;
            }
            let registrations = list_project_registrations_tx(&tx, None, MAX_PAGE_ENTRIES)?;
            ensure_project_registration_bytes(&registrations)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: registrations,
                events: Vec::new(),
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Revoke only the settings entries removed by an Owner cleanup. Agent
    /// rows retain their own project label and native history independently.
    pub fn remove_project_registrations(&mut self, ids: &[String]) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            for id in ids {
                tx.execute("DELETE FROM project_registrations WHERE id = ?1", [id])
                    .map_err(sql_error)?;
            }
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn get_project_registration(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<core::api::ProjectRegistrationView>> {
        require_project_owner(principal)?;
        require_nonempty(id, "project id")?;
        load_project_registration(&self.connection, id)
    }

    pub fn list_project_registrations(
        &self,
        principal: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<core::api::ProjectRegistrationView>> {
        require_project_owner(principal)?;
        validate_limit(limit)?;
        if let Some(after_id) = after_id {
            require_nonempty(after_id, "project cursor")?;
        }
        let registrations = list_project_registrations_tx(&self.connection, after_id, limit)?;
        ensure_project_registration_bytes(&registrations)?;
        Ok(registrations)
    }

    /// Read launch adoption over the trailing day independently of journal retention.
    pub fn project_policy_usage(
        &self,
        project: &str,
        now: i64,
    ) -> Result<core::api::ProjectPolicyUsageView> {
        self.project_policy_usage_for_projects(&[project], now)?
            .pop()
            .ok_or_else(|| Error::new("internal", "project policy usage was not returned"))
    }

    /// Read usage in the caller's project order using two bounded page queries.
    pub fn project_policy_usage_for_projects(
        &self,
        projects: &[&str],
        now: i64,
    ) -> Result<Vec<core::api::ProjectPolicyUsageView>> {
        self.ensure_committed_reads()?;
        if projects.is_empty() {
            return Ok(Vec::new());
        }
        validate_limit(projects.len())?;
        let indexes: HashMap<_, _> = projects
            .iter()
            .copied()
            .enumerate()
            .map(|(index, project)| (project, index))
            .collect();
        if indexes.len() != projects.len() {
            return Err(Error::new(
                "invalid",
                "project policy page contains duplicate IDs",
            ));
        }

        let page = encode_json(&projects)?;
        let cutoff = now.saturating_sub(PROJECT_POLICY_USAGE_WINDOW_MS);
        let mut usage = vec![core::api::ProjectPolicyUsageView::default(); projects.len()];
        let index = |project: &str| {
            indexes.get(project).copied().ok_or_else(|| {
                Error::new("source_integrity", "project policy query escaped its page")
            })
        };
        let count = |value: i64| {
            u64::try_from(value)
                .map_err(|_| Error::new("source_integrity", "negative project policy count"))
        };
        {
            let mut statement = self
                .connection
                .prepare_cached(
                    "SELECT p.value, COUNT(*), COALESCE(SUM(a.policy_applied), 0),
                        COALESCE(SUM(a.policy_opt_out_reason IS NOT NULL), 0)
                 FROM json_each(?1) AS p CROSS JOIN agents AS a
                 WHERE a.project = p.value AND a.origin = 'eliot'
                   AND a.created_at BETWEEN ?2 AND ?3
                 GROUP BY p.value",
                )
                .map_err(sql_error)?;
            let rows = statement
                .query_map(params![page, cutoff, now], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (project, total, applied, opted_out) = row.map_err(sql_error)?;
                let view = &mut usage[index(&project)?];
                view.applied = count(applied)?;
                view.total = count(total)?;
                view.opted_out = count(opted_out)?;
            }
        }
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT p.value, a.id, a.policy_opt_out_reason, a.created_at
             FROM json_each(?1) AS p CROSS JOIN agents AS a
             WHERE a.id IN (
                 SELECT id FROM agents
                 WHERE project = p.value AND origin = 'eliot'
                   AND created_at BETWEEN ?2 AND ?3 AND policy_opt_out_reason IS NOT NULL
                 ORDER BY created_at DESC, id DESC LIMIT ?4
             )
             ORDER BY p.value, a.created_at DESC, a.id DESC",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    page,
                    cutoff,
                    now,
                    core::api::PROJECT_POLICY_OPTOUTS_MAX as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        core::api::ProjectPolicyOptOutView {
                            agent: format_agent_id(row.get(1)?),
                            reason: core::api::sanitize_project_policy_opt_out_reason(row.get(2)?),
                            created_at: row.get(3)?,
                        },
                    ))
                },
            )
            .map_err(sql_error)?;
        for row in rows {
            let (project, opt_out) = row.map_err(sql_error)?;
            usage[index(&project)?].opt_outs.push(opt_out);
        }
        for project_usage in &usage {
            project_usage.validate()?;
        }
        Ok(usage)
    }
}

pub(super) fn project_registration_tx(
    connection: &Connection,
    id: &str,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    require_nonempty(id, "project id")?;
    load_project_registration(connection, id)
}

/// Store-side rule authorization used by the rule transaction before it
/// admits an action child. Owner rules have no project authority to check.
pub(super) fn validate_project_rule_admission_tx(
    tx: &Transaction<'_>,
    principal: &str,
    target_agent: &str,
    generation: i64,
    action: &str,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    let Some(project_id) = project_id_from_principal(principal)? else {
        return Ok(None);
    };
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?;
    project_binding_for_agent_tx(tx, &registration, target_agent, generation, action)?;
    Ok(Some(registration))
}

pub(super) fn validate_project_rule_launch_action_tx(
    tx: &Transaction<'_>,
    principal: &str,
    args: &Value,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    let Some(project_id) = project_id_from_principal(principal)? else {
        return Ok(None);
    };
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?;
    validate_project_launch_args(&registration, args)?;
    Ok(Some(registration))
}

pub(super) fn validate_project_resume_action_tx(
    tx: &Transaction<'_>,
    principal: &str,
    target_agent: &str,
    generation: i64,
    expected_generation: Option<i64>,
    has_linked_input: bool,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    let Some(project_id) = project_id_from_principal(principal)? else {
        return Ok(None);
    };
    if expected_generation.is_some_and(|expected| expected != generation) {
        return Err(Error::new(
            "stale_generation",
            "project resume expected generation does not match the current Agent",
        ));
    }
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?;
    if has_linked_input {
        require_project_registration_action(&registration, "send")?;
    }
    project_binding_for_agent_tx(tx, &registration, target_agent, generation, "resume")?;
    Ok(Some(registration))
}

pub(super) fn decode_frozen_git_fetch(args: &Value) -> Result<core::api::FrozenGitFetch> {
    let frozen = serde_json::from_value::<core::api::FrozenGitFetch>(args.clone())
        .map_err(|_| Error::new("invalid", "git.fetch args must be a frozen intent"))?;
    frozen.validate()?;
    Ok(frozen)
}

pub(super) fn decode_frozen_git_publish(args: &Value) -> Result<core::api::FrozenGitPublish> {
    let frozen = serde_json::from_value::<core::api::FrozenGitPublish>(args.clone())
        .map_err(|_| Error::new("invalid", "git.publish args must be a frozen intent"))?;
    frozen.validate()?;
    Ok(frozen)
}

pub(super) fn validate_git_publish_intent_tx(
    tx: &Transaction<'_>,
    principal: &str,
    frozen: &core::api::FrozenGitPublish,
) -> Result<core::api::ProjectRegistrationView> {
    let project_id = project_id_from_principal(principal)?
        .ok_or_else(|| Error::new("not_allowed", "git.publish requires project authority"))?;
    if project_id != frozen.repository {
        return Err(Error::new(
            "not_allowed",
            "Project git.publish repository must match its saved principal",
        ));
    }
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "git.publish project is not registered"))?;
    registration
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored project registration is invalid"))?;
    if registration.id != frozen.repository
        || registration.revision != frozen.registration_revision
        || registration.checkout != frozen.canonical_checkout
        || registration.repo_common_dir != frozen.common_dir
        || !registration
            .actions
            .iter()
            .any(|action| action == "git.publish")
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish registration or action approval is stale",
        ));
    }
    let agent_num = parse_agent_id(&frozen.agent)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_allowed", "git.publish Agent is not retained"))?;
    let workspace = workspace::load_workspace(tx, agent_num)?
        .ok_or_else(|| Error::new("not_allowed", "git.publish managed workspace is absent"))?;
    if agent.id != frozen.agent
        || agent.generation != frozen.generation
        || agent.active_turn.is_some()
        || agent.workspace.as_deref() != Some(frozen.workspace.as_str())
        || workspace.agent != frozen.agent
        || workspace.target_generation != frozen.generation
        || workspace.phase != WorkspacePhase::Ready
        || workspace.worktree_path != frozen.workspace
        || workspace.worktree_path == registration.checkout
        || workspace.project != registration.checkout
        || workspace.repo_path != registration.checkout
        || workspace.repo_common_dir != registration.repo_common_dir
        || workspace.base_oid != frozen.base_oid
        || workspace.requested_branch != frozen.branch
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish Agent or managed worktree state changed",
        ));
    }
    let publication = registration
        .git
        .as_ref()
        .and_then(|git| git.publication.as_ref());
    match (&frozen.remote, &frozen.endpoint) {
        (Some(remote), Some(endpoint))
            if publication.and_then(|publication| publication.remotes.get(remote))
                == Some(endpoint) => {}
        (None, None) if !frozen.create_pr => {}
        _ => {
            return Err(Error::new(
                "not_allowed",
                "git.publish endpoint does not match Owner approval",
            ));
        }
    }
    let registered_gh = publication.and_then(|publication| publication.github_cli.as_ref());
    if frozen.create_pr && frozen.github_cli.as_ref() != registered_gh {
        return Err(Error::new(
            "not_allowed",
            "git.publish GitHub integration does not match Owner approval",
        ));
    }
    let helper_approved = frozen.remote.is_some()
        && registration
            .git
            .as_ref()
            .and_then(|git| git.credential_helper)
            == Some(core::api::GitCredentialHelper::Manager);
    if helper_approved != frozen.credential_helper.is_some() {
        return Err(Error::new(
            "not_allowed",
            "git.publish helper receipt does not match Owner approval",
        ));
    }
    let (kind, agent, generation, encoded_data, encoded_subject): (
        String,
        Option<i64>,
        Option<i64>,
        String,
        String,
    ) = tx
        .query_row(
            "SELECT kind, agent, generation, data, subject FROM events WHERE seq = ?1",
            [frozen.source_event_seq],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
        .ok_or_else(|| Error::new("not_found", "git.publish check event was not retained"))?;
    let data: Value = serde_json::from_str(&encoded_data)
        .map_err(|_| Error::new("source_integrity", "project check event data is invalid"))?;
    let check = serde_json::from_value::<core::api::ProjectCheckPassed>(data)
        .map_err(|_| Error::new("source_integrity", "project check event data is invalid"))?;
    let subject: core::EventSubject = serde_json::from_str(&encoded_subject)
        .map_err(|_| Error::new("source_integrity", "project check event subject is invalid"))?;
    check
        .validate()
        .map_err(|_| Error::new("source_integrity", "project check event data is invalid"))?;
    if kind != "project.check.passed"
        || check != frozen.check
        || check.project != frozen.repository
        || agent.map(format_agent_id).as_deref() != Some(frozen.agent.as_str())
        || generation != Some(frozen.generation)
        || subject.workspace.as_deref() != Some(frozen.workspace.as_str())
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish does not match its successful project check event",
        ));
    }
    Ok(registration)
}

pub(super) fn validate_git_fetch_intent_tx(
    tx: &Transaction<'_>,
    principal: &str,
    frozen: &core::api::FrozenGitFetch,
) -> Result<core::api::ProjectRegistrationView> {
    // Syntax was validated at decode; this checks live registration state only.
    let project_id = if principal == "owner" {
        None
    } else {
        Some(
            project_id_from_principal(principal)?
                .ok_or_else(|| Error::new("not_allowed", "git.fetch principal is unauthorized"))?,
        )
    };
    if project_id
        .as_deref()
        .is_some_and(|project_id| project_id != frozen.repository)
    {
        return Err(Error::new(
            "not_allowed",
            "Project git.fetch repository must match its saved principal",
        ));
    }
    let registration = load_project_registration(tx, &frozen.repository)?
        .ok_or_else(|| Error::new("not_allowed", "git.fetch repository is not registered"))?;
    registration
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored project registration is invalid"))?;
    if registration.id != frozen.repository
        || registration.revision != frozen.registration_revision
        || registration.checkout != frozen.canonical_checkout
        || registration.repo_common_dir != frozen.common_dir
    {
        return Err(Error::new(
            "not_allowed",
            "git.fetch frozen registration binding is stale",
        ));
    }
    let git = registration
        .git
        .as_ref()
        .ok_or_else(|| Error::new("not_allowed", "repository has no Git fetch approval"))?;
    if git.fetch_remotes.get(&frozen.remote) != Some(&frozen.endpoint) {
        return Err(Error::new(
            "not_allowed",
            "git.fetch endpoint does not match the approved direct remote",
        ));
    }
    let helper_approved = git.credential_helper == Some(core::api::GitCredentialHelper::Manager);
    if helper_approved != frozen.credential_helper.is_some() {
        return Err(Error::new(
            "not_allowed",
            "git.fetch helper receipt does not match the registered helper policy",
        ));
    }
    if project_id.is_some() {
        require_project_registration_action(&registration, "git.fetch")?;
    }
    Ok(registration)
}

pub(super) fn capture_project_command_binding_tx(
    tx: &Transaction<'_>,
    command: &CommandInsert<'_>,
) -> Result<Option<String>> {
    let Some(project_id) = project_id_from_principal(command.principal)? else {
        return Ok(None);
    };
    if command.link_kind == Some("stop_interrupt") {
        // The Store creates this child in the same transaction that verifies
        // the dispatched stop and its exact generation/turn binding.
        let binding: Option<String> = tx
            .query_row(
                "SELECT project_binding FROM commands WHERE id = ?1",
                [command.parent_command],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .flatten();
        return binding.map(Some).ok_or_else(|| {
            Error::new(
                "source_integrity",
                "stop interrupt has no parent project binding",
            )
        });
    }
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?;
    let binding = if let Some(target) = command.target {
        if command.verb == "launch" {
            validate_project_launch_args(&registration, command.args)?;
        }
        if command.verb == "send" && command.link_kind == Some("send") {
            project_start_input_binding_tx(tx, &registration, command, target)?
        } else {
            let generation = command.generation.ok_or_else(|| {
                Error::new("not_allowed", "project command has no frozen generation")
            })?;
            project_binding_for_agent_tx(
                tx,
                &registration,
                &format_agent_id(target),
                generation,
                command.verb,
            )?
        }
    } else if command.verb == "git.fetch" {
        let frozen = decode_frozen_git_fetch(command.args)?;
        validate_git_fetch_intent_tx(tx, command.principal, &frozen)?;
        targetless_project_command_binding(registration.revision)
    } else if command.verb == "git.publish" {
        let frozen = decode_frozen_git_publish(command.args)?;
        validate_git_publish_intent_tx(tx, command.principal, &frozen)?;
        targetless_project_command_binding(registration.revision)
    } else if command.verb == "run" {
        require_project_registration_action(&registration, "run")?;
        targetless_project_command_binding(registration.revision)
    } else if matches!(command.verb, "notify" | "emit" | "mail.send" | "var.set") {
        require_project_registration_action(&registration, command.verb)?;
        targetless_project_command_binding(registration.revision)
    } else {
        if !matches!(command.verb, "rule" | "timer.run" | "rule.catch_up") {
            return Err(Error::new(
                "not_allowed",
                "project principals may only create rule and catch-up parents",
            ));
        }
        targetless_project_command_binding(registration.revision)
    };
    encode_json(&binding).map(Some)
}

pub(super) fn validate_project_command_dispatch_tx(
    tx: &Transaction<'_>,
    command: &CommandRecord,
) -> Result<()> {
    let Some(project_id) = project_id_from_principal(&command.command.principal)? else {
        return Ok(());
    };
    let encoded: Option<String> = tx
        .query_row(
            "SELECT project_binding FROM commands WHERE id = ?1",
            [&command.command.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    let binding = encoded
        .as_deref()
        .and_then(|encoded| serde_json::from_str::<ProjectCommandBinding>(encoded).ok())
        .ok_or_else(|| Error::new("not_allowed", "project command has no valid frozen binding"))?;
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?;
    if binding.registration_revision != registration.revision {
        return Err(Error::new(
            "not_allowed",
            "project registration changed after command admission",
        ));
    }
    if command.command.verb == "launch" {
        validate_project_launch_args(&registration, &command.command.args)?;
    }
    if command.command.verb == "resume" {
        let target = command
            .target
            .ok_or_else(|| Error::new("not_allowed", "project resume has no Agent target"))?;
        let generation = command
            .command
            .expected_generation
            .or(command.command.generation)
            .ok_or_else(|| Error::new("not_allowed", "project resume has no old generation"))?;
        let next_generation = generation
            .checked_add(1)
            .ok_or_else(|| Error::new("bad_state", "Agent generation cannot be incremented"))?;
        let current = project_resume_binding_for_dispatch_tx(
            tx,
            &registration,
            target,
            &command.command.id,
            generation,
            next_generation,
        )?;
        if current != binding {
            return Err(Error::new(
                "not_allowed",
                "project resume Agent, workspace or repository binding changed",
            ));
        }
        return Ok(());
    }
    if command.target.is_none() && command.command.verb == "git.fetch" {
        let frozen = decode_frozen_git_fetch(&command.command.args)?;
        validate_git_fetch_intent_tx(tx, &command.command.principal, &frozen)?;
        require_project_registration_action(&registration, "git.fetch")?;
        if binding != targetless_project_command_binding(registration.revision) {
            return Err(Error::new(
                "not_allowed",
                "Project git.fetch binding does not match its saved registration",
            ));
        }
        return Ok(());
    }
    if command.target.is_none() && command.command.verb == "git.publish" {
        let frozen = decode_frozen_git_publish(&command.command.args)?;
        validate_git_publish_intent_tx(tx, &command.command.principal, &frozen)?;
        if binding != targetless_project_command_binding(registration.revision) {
            return Err(Error::new(
                "not_allowed",
                "Project git.publish binding does not match its saved registration",
            ));
        }
        return Ok(());
    }
    if command.target.is_none() && command.command.verb == "run" {
        require_project_registration_action(&registration, "run")?;
        if binding != targetless_project_command_binding(registration.revision) {
            return Err(Error::new(
                "not_allowed",
                "project run binding does not match its saved registration",
            ));
        }
        return Ok(());
    }
    if command.target.is_none()
        && matches!(
            command.command.verb.as_str(),
            "notify" | "emit" | "mail.send" | "var.set"
        )
    {
        require_project_registration_action(&registration, &command.command.verb)?;
        if binding != targetless_project_command_binding(registration.revision) {
            return Err(Error::new(
                "not_allowed",
                "project targetless action binding does not match its saved registration",
            ));
        }
        return Ok(());
    }
    let target = command
        .target
        .ok_or_else(|| Error::new("not_allowed", "project command has no Agent target"))?;
    let generation = command
        .command
        .generation
        .ok_or_else(|| Error::new("not_allowed", "project command has no generation"))?;
    let current = project_binding_for_agent_tx(
        tx,
        &registration,
        &format_agent_id(target),
        generation,
        &command.command.verb,
    )?;
    if current != binding {
        return Err(Error::new(
            "not_allowed",
            "project Agent, workspace, profile or repository binding changed",
        ));
    }
    Ok(())
}

fn project_resume_binding_for_dispatch_tx(
    tx: &Transaction<'_>,
    registration: &core::api::ProjectRegistrationView,
    target: i64,
    command_id: &str,
    old_generation: i64,
    next_generation: i64,
) -> Result<ProjectCommandBinding> {
    require_project_registration_action(registration, "resume")?;
    let agent = load_agent_by_num(tx, target)?
        .ok_or_else(|| Error::new("not_found", "project resume Agent was not found"))?;
    if agent.generation != old_generation || !agent.is_terminal() || agent.active_turn.is_some() {
        return Err(Error::new(
            "not_allowed",
            "project resume no longer targets its retained terminal generation",
        ));
    }
    if !registration
        .profiles
        .iter()
        .any(|approved| approved == &agent.profile)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve the retained profile",
        ));
    }
    let workspace_path = agent
        .workspace
        .as_deref()
        .ok_or_else(|| Error::new("not_allowed", "project Agent has no frozen workspace"))?;
    let (repo_path, repo_common_dir, workspace_phase, workspace_target_generation) =
        match workspace::load_workspace(tx, target)? {
            Some(workspace)
                if workspace.project == registration.checkout
                    && workspace.repo_path == registration.checkout
                    && workspace.repo_common_dir == registration.repo_common_dir
                    && workspace.worktree_path == workspace_path
                    && workspace.start_command_id == command_id
                    && workspace.phase == WorkspacePhase::Ready
                    && workspace.target_generation == next_generation
                    && workspace.setup_index == workspace.frozen_setup.len() =>
            {
                (
                    workspace.repo_path,
                    workspace.repo_common_dir,
                    Some(WorkspacePhase::Ready),
                    Some(old_generation),
                )
            }
            Some(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "managed resume workspace is not ready at its exact next generation",
                ));
            }
            None => registered_checkout_binding(registration, workspace_path)?,
        };
    Ok(ProjectCommandBinding {
        registration_revision: registration.revision,
        profile: Some(agent.profile),
        workspace: Some(workspace_path.to_owned()),
        repo_path: Some(repo_path),
        repo_common_dir: Some(repo_common_dir),
        workspace_phase,
        workspace_target_generation,
    })
}

/// Bind a registered checkout directly when no managed workspace row exists.
fn registered_checkout_binding(
    registration: &core::api::ProjectRegistrationView,
    workspace_path: &str,
) -> Result<(String, String, Option<WorkspacePhase>, Option<i64>)> {
    if workspace_path != registration.checkout {
        return Err(Error::new(
            "not_allowed",
            "project Agent workspace is not the registered checkout",
        ));
    }
    Ok((
        registration.checkout.clone(),
        registration.repo_common_dir.clone(),
        None,
        None,
    ))
}

fn targetless_project_command_binding(registration_revision: i64) -> ProjectCommandBinding {
    ProjectCommandBinding {
        registration_revision,
        profile: None,
        workspace: None,
        repo_path: None,
        repo_common_dir: None,
        workspace_phase: None,
        workspace_target_generation: None,
    }
}

fn require_project_registration_action(
    registration: &core::api::ProjectRegistrationView,
    action: &str,
) -> Result<()> {
    if !is_supported_project_registration_action(action) {
        return Err(Error::new(
            "unsupported",
            "project registration action is not implemented",
        ));
    }
    if !registration
        .actions
        .iter()
        .any(|approved| approved == action)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve this action",
        ));
    }
    Ok(())
}

fn validate_project_launch_args(
    registration: &core::api::ProjectRegistrationView,
    args: &Value,
) -> Result<()> {
    require_project_registration_action(registration, "launch")?;
    let profile = required_string(args, "profile")?;
    if !registration
        .profiles
        .iter()
        .any(|approved| approved == &profile)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve the launch profile",
        ));
    }
    let workspace = required_string(args, "workspace")?;
    if workspace != registration.checkout {
        return Err(Error::new(
            "not_allowed",
            "project launch workspace is not the registered checkout",
        ));
    }
    if args
        .get("worktree")
        .is_some_and(|worktree| !worktree.is_null())
    {
        return Err(Error::new(
            "unsupported",
            "project rule launch does not admit managed workspace preparation",
        ));
    }
    if args.get("text").is_some_and(|text| !text.is_null()) {
        require_project_registration_action(registration, "send")?;
    }
    Ok(())
}

fn project_start_input_binding_tx(
    tx: &Transaction<'_>,
    registration: &core::api::ProjectRegistrationView,
    command: &CommandInsert<'_>,
    target: i64,
) -> Result<ProjectCommandBinding> {
    let parent_id = command.parent_command.ok_or_else(|| {
        Error::new(
            "not_allowed",
            "project launch input must be linked to its admitted launch",
        )
    })?;
    let parent = load_command_record(tx, parent_id)?
        .ok_or_else(|| Error::new("source_integrity", "project launch input parent is missing"))?;
    let is_launch = parent.command.verb == "launch";
    let is_resume = parent.command.verb == "resume";
    if !is_launch && !is_resume {
        return Err(Error::new(
            "source_integrity",
            "project linked input parent is not a launch or resume",
        ));
    }
    let old_generation = if is_launch {
        0
    } else {
        parent
            .command
            .expected_generation
            .or(parent.command.generation)
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "project resume has no retained old generation",
                )
            })?
    };
    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "project start generation overflow"))?;
    let target_id = format_agent_id(target);
    let linked_text = command.args.get("text").and_then(Value::as_str);
    let launch_text = parent.command.args.get("text").and_then(Value::as_str);
    if command.link_kind != Some("send")
        || command.ordinal != Some(0)
        || command.id != format!("{parent_id}/send")
        || command.target != Some(target)
        || command.expected_generation != Some(next_generation)
        || command.generation != Some(next_generation)
        || command.expected_turn_id.is_some()
        || command.turn_id.is_some()
        || command.request_id.is_some()
        || command.probe
        || command.state != CommandState::Queued
        || !command
            .args
            .as_object()
            .is_some_and(|args| args.len() == 1 && args.get("text").is_some_and(Value::is_string))
        || parent.command.method != command.method
        || parent.command.principal != command.principal
        || parent.command.target.as_deref() != Some(target_id.as_str())
        || parent.state != CommandState::Accepted
        || parent.target != Some(target)
        || parent.command.generation != Some(old_generation)
        || (is_launch
            && parent
                .command
                .expected_generation
                .is_some_and(|generation| generation != old_generation))
        || (is_resume
            && parent
                .command
                .expected_generation
                .is_some_and(|generation| generation != old_generation))
    {
        return Err(Error::new(
            "source_integrity",
            "project linked input does not match its exact queued start and next generation",
        ));
    }
    if is_launch {
        if launch_text.is_none() || linked_text != launch_text {
            return Err(Error::new(
                "source_integrity",
                "project launch input differs from its retained launch text",
            ));
        }
        validate_project_launch_args(registration, &parent.command.args)?;
    } else {
        if !super::resume_parent_payload_matches(tx, &parent.command, command.args)? {
            return Err(Error::new(
                "source_integrity",
                "project resume input differs from its retained resume request",
            ));
        }
        require_project_registration_action(registration, "resume")?;
    }
    require_project_registration_action(registration, "send")?;
    let agent = load_agent_by_num(tx, target)?
        .ok_or_else(|| Error::new("not_found", "project start Agent was not found"))?;
    let target_is_ready = if is_launch {
        agent.generation == 0 && agent.state == AgentState::Created
    } else {
        agent.generation == old_generation
            && agent.is_terminal()
            && agent.active_turn.is_none()
            && core::api::is_supported_harness(&agent.harness)
            && agent
                .native_scope
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && agent
                .native_ref
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    };
    if !target_is_ready {
        return Err(Error::new(
            "not_allowed",
            "project linked input is not bound to the next generation of its retained Agent",
        ));
    }
    let action = if is_launch { "launch" } else { "resume" };
    let mut binding =
        project_binding_for_agent_tx(tx, registration, &target_id, old_generation, action)?;
    let encoded: Option<String> = tx
        .query_row(
            "SELECT project_binding FROM commands WHERE id = ?1",
            [parent_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    let saved = encoded
        .as_deref()
        .and_then(|value| serde_json::from_str::<ProjectCommandBinding>(value).ok())
        .ok_or_else(|| Error::new("source_integrity", "project launch has no saved binding"))?;
    if saved != binding {
        return Err(Error::new(
            "source_integrity",
            "project linked input differs from its start registration binding",
        ));
    }
    if is_resume && binding.workspace_phase == Some(WorkspacePhase::Ready) {
        binding.workspace_target_generation = Some(next_generation);
    }
    Ok(binding)
}

fn project_binding_for_agent_tx(
    tx: &Transaction<'_>,
    registration: &core::api::ProjectRegistrationView,
    target_agent: &str,
    generation: i64,
    action: &str,
) -> Result<ProjectCommandBinding> {
    if !is_supported_project_action(action) {
        return Err(Error::new(
            "unsupported",
            "project action is not implemented",
        ));
    }
    if !registration
        .actions
        .iter()
        .any(|approved| approved == action)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve this action",
        ));
    }
    let agent_num = parse_agent_id(target_agent)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "project action target Agent was not found"))?;
    if agent.generation != generation {
        return Err(Error::new(
            "not_allowed",
            "project action target generation does not match",
        ));
    }
    if !registration
        .profiles
        .iter()
        .any(|approved| approved == &agent.profile)
    {
        return Err(Error::new(
            "not_allowed",
            "project registration does not approve the target profile",
        ));
    }
    let workspace_path = agent
        .workspace
        .as_deref()
        .ok_or_else(|| Error::new("not_allowed", "project Agent has no frozen workspace"))?;
    // L3 stores the canonical checkout path in WorkspacePreparation.project,
    // independently of the Agent.project label.
    let (repo_path, repo_common_dir, workspace_phase, workspace_target_generation) =
        match workspace::load_workspace(tx, agent_num)? {
            Some(workspace)
                if workspace.project == registration.checkout
                    && workspace.repo_path == registration.checkout
                    && workspace.repo_common_dir == registration.repo_common_dir
                    && workspace.worktree_path == workspace_path
                    && workspace.phase == WorkspacePhase::Ready
                    && workspace.target_generation == generation =>
            {
                (
                    workspace.repo_path,
                    workspace.repo_common_dir,
                    Some(workspace.phase),
                    Some(workspace.target_generation),
                )
            }
            Some(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "managed Agent workspace is not ready at this generation or its repository identity changed",
                ));
            }
            None => registered_checkout_binding(registration, workspace_path)?,
        };
    Ok(ProjectCommandBinding {
        registration_revision: registration.revision,
        profile: Some(agent.profile),
        workspace: Some(workspace_path.to_owned()),
        repo_path: Some(repo_path),
        repo_common_dir: Some(repo_common_dir),
        workspace_phase,
        workspace_target_generation,
    })
}

fn project_id_from_principal(principal: &str) -> Result<Option<String>> {
    let Some(project_id) = principal.strip_prefix(PROJECT_PRINCIPAL_PREFIX) else {
        return Ok(None);
    };
    if project_id.is_empty() || project_id.trim() != project_id {
        return Err(Error::new("not_allowed", "project principal is invalid"));
    }
    Ok(Some(project_id.to_owned()))
}

fn require_project_owner(principal: &str) -> Result<()> {
    require_nonempty(principal, "principal")?;
    if principal != "owner" {
        return Err(Error::new(
            "not_allowed",
            "project registration requires the owner principal",
        ));
    }
    Ok(())
}

fn normalize_registration(input: &ProjectRegistrationInput) -> Result<ProjectRegistrationInput> {
    let mut profiles = input.profiles.clone();
    let mut actions = input.actions.clone();
    let git = input.git.clone();
    let interpreters = input.interpreters.clone();
    let mut run_env = input.run_env.clone();
    normalize_allowset(&mut profiles, "project profiles")?;
    normalize_allowset(&mut actions, "project actions")?;
    normalize_allowset(&mut run_env, "project run_env")?;
    for (reference, interpreter) in &interpreters {
        require_nonempty(reference, "project interpreter reference")?;
        if reference.trim() != reference || reference.contains('\0') {
            return Err(Error::new(
                "invalid",
                "project interpreter reference is malformed",
            ));
        }
        interpreter.validate()?;
    }
    if actions
        .iter()
        .any(|action| !is_supported_project_registration_action(action))
    {
        return Err(Error::new(
            "unsupported",
            "project registration contains an unimplemented action",
        ));
    }
    let view = core::api::ProjectRegistrationView {
        id: input.id.clone(),
        checkout: input.checkout.clone(),
        repo_common_dir: input.repo_common_dir.clone(),
        git: git.clone(),
        profiles: profiles.clone(),
        actions: actions.clone(),
        interpreters: interpreters.clone(),
        run_env: run_env.clone(),
        revision: 1,
        policy_usage: core::api::ProjectPolicyUsageView::default(),
        origin: None,
        checkouts: Vec::new(),
    };
    view.validate()?;
    Ok(ProjectRegistrationInput {
        id: input.id.clone(),
        checkout: input.checkout.clone(),
        repo_common_dir: input.repo_common_dir.clone(),
        git,
        profiles,
        actions,
        interpreters,
        run_env,
    })
}

fn is_supported_project_action(action: &str) -> bool {
    matches!(
        action,
        "launch"
            | "resume"
            | "stop"
            | "send"
            | "interrupt"
            | "steer"
            | "inject"
            | "approve"
            | "deny"
            | "answer"
            | "git.fetch"
            | "git.publish"
            | "notify"
            | "emit"
            | "mail.send"
            | "var.set"
    )
}

fn is_supported_project_registration_action(action: &str) -> bool {
    is_supported_project_action(action)
        || matches!(action, "run" | core::api::PROJECT_ASSIGNMENT_CLAIM_ACTION)
}

fn normalize_allowset(values: &mut [String], name: &str) -> Result<()> {
    let mut seen = HashSet::with_capacity(values.len());
    for value in values.iter() {
        require_nonempty(value, name)?;
        if value.trim() != value || !seen.insert(value.as_str()) {
            return Err(Error::new(
                "invalid",
                format!("{name} are invalid or duplicated"),
            ));
        }
    }
    values.sort();
    Ok(())
}

fn upsert_project_registration_tx(
    tx: &Transaction<'_>,
    input: &ProjectRegistrationInput,
) -> Result<core::api::ProjectRegistrationView> {
    if let Some(existing) = load_project_registration(tx, &input.id)? {
        if existing.checkout == input.checkout
            && existing.repo_common_dir == input.repo_common_dir
            && existing.git == input.git
            && existing.profiles == input.profiles
            && existing.actions == input.actions
            && existing.interpreters == input.interpreters
            && existing.run_env == input.run_env
        {
            return Ok(existing);
        }
        let changed = tx
            .execute(
                "DELETE FROM project_registrations WHERE id = ?1 AND revision = ?2",
                params![input.id, existing.revision],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(Error::new(
                "bad_state",
                "project registration changed before its update",
            ));
        }
    }
    let profiles = encode_json(&input.profiles)?;
    let actions = encode_json(&input.actions)?;
    let git = input.git.as_ref().map(encode_json).transpose()?;
    let interpreters = encode_json(&input.interpreters)?;
    let run_env = encode_json(&input.run_env)?;
    tx.execute(
        "INSERT INTO project_registrations(
             id, checkout, repo_common_dir, profiles, actions, interpreters, run_env, git
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            input.id,
            input.checkout,
            input.repo_common_dir,
            profiles,
            actions,
            interpreters,
            run_env,
            git,
        ],
    )
    .map_err(sql_error)?;
    load_project_registration(tx, &input.id)?
        .ok_or_else(|| Error::new("internal", "upserted project registration disappeared"))
}

fn project_registration_ids_tx(connection: &Connection) -> Result<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT id FROM project_registrations ORDER BY id")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)
}

fn list_project_registrations_tx(
    connection: &Connection,
    after_id: Option<&str>,
    limit: usize,
) -> Result<Vec<core::api::ProjectRegistrationView>> {
    let mut statement = connection
        .prepare(
            "SELECT revision, id, checkout, repo_common_dir, profiles, actions, interpreters, run_env, git
             FROM project_registrations
             WHERE ?1 IS NULL OR id > ?1
             ORDER BY id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![after_id, limit as i64],
            project_registration_from_row,
        )
        .map_err(sql_error)?;
    let mut registrations = Vec::new();
    for row in rows {
        registrations.push(row.map_err(sql_error)?);
    }
    Ok(registrations)
}

fn load_project_registration(
    connection: &Connection,
    id: &str,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    connection
        .query_row(
            "SELECT revision, id, checkout, repo_common_dir, profiles, actions, interpreters, run_env, git
             FROM project_registrations WHERE id = ?1",
            [id],
            project_registration_from_row,
        )
        .optional()
        .map_err(sql_error)
}

fn ensure_project_registration_bytes(
    registrations: &[core::api::ProjectRegistrationView],
) -> Result<()> {
    if encode_json(&registrations)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "project registration result exceeds 900 KiB",
        ));
    }
    Ok(())
}

fn project_registration_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<core::api::ProjectRegistrationView> {
    let revision: i64 = row.get(0)?;
    let id: String = row.get(1)?;
    let checkout: String = row.get(2)?;
    let repo_common_dir: String = row.get(3)?;
    let profiles_text: String = row.get(4)?;
    let actions_text: String = row.get(5)?;
    let interpreters_text: String = row.get(6)?;
    let run_env_text: String = row.get(7)?;
    let git_text: Option<String> = row.get(8)?;
    let profiles = serde_json::from_str(&profiles_text).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid project profiles",
            )),
        )
    })?;
    let actions = serde_json::from_str(&actions_text).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            5,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid project actions",
            )),
        )
    })?;
    let interpreters = serde_json::from_str(&interpreters_text).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            6,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid project interpreters",
            )),
        )
    })?;
    let run_env = serde_json::from_str(&run_env_text).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            7,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid project run environment",
            )),
        )
    })?;
    let git = git_text
        .map(|text| {
            serde_json::from_str::<core::api::ProjectGitView>(&text).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    8,
                    rusqlite::types::Type::Text,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid project Git registration",
                    )),
                )
            })
        })
        .transpose()?;
    Ok(core::api::ProjectRegistrationView {
        id,
        checkout,
        repo_common_dir,
        git,
        profiles,
        actions,
        interpreters,
        run_env,
        revision,
        policy_usage: core::api::ProjectPolicyUsageView::default(),
        origin: None,
        checkouts: Vec::new(),
    })
}
