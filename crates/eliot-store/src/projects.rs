//! Owner-registered project rule authority and dispatch fences.

use super::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

const PROJECT_PRINCIPAL_PREFIX: &str = "project:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRegistrationInput {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
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

struct ProjectRegistrationRow {
    view: core::api::ProjectRegistrationView,
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

    pub fn get_project_registration(
        &self,
        principal: &str,
        id: &str,
    ) -> Result<Option<core::api::ProjectRegistrationView>> {
        require_project_owner(principal)?;
        require_nonempty(id, "project id")?;
        Ok(load_project_registration(&self.connection, id)?.map(|row| row.view))
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
}

pub(super) fn project_registration_tx(
    connection: &Connection,
    id: &str,
) -> Result<Option<core::api::ProjectRegistrationView>> {
    require_nonempty(id, "project id")?;
    Ok(load_project_registration(connection, id)?.map(|row| row.view))
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
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?
        .view;
    project_binding_for_agent_tx(tx, &registration, target_agent, generation, action)?;
    Ok(Some(registration))
}

pub(super) fn capture_project_command_binding_tx(
    tx: &Transaction<'_>,
    command: &CommandInsert<'_>,
) -> Result<Option<String>> {
    let Some(project_id) = project_id_from_principal(command.principal)? else {
        return Ok(None);
    };
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration is absent"))?
        .view;
    let binding = if let Some(target) = command.target {
        let generation = command
            .generation
            .ok_or_else(|| Error::new("not_allowed", "project command has no frozen generation"))?;
        project_binding_for_agent_tx(
            tx,
            &registration,
            &format_agent_id(target),
            generation,
            command.verb,
        )?
    } else if command.verb == "run" {
        require_project_registration_action(&registration, "run")?;
        targetless_project_run_binding(registration.revision)
    } else {
        if !matches!(command.verb, "rule" | "timer.run" | "rule.catch_up") {
            return Err(Error::new(
                "not_allowed",
                "project principals may only create rule and catch-up parents",
            ));
        }
        targetless_project_run_binding(registration.revision)
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
    if binding.registration_revision
        != load_project_registration(tx, &project_id)?
            .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?
            .view
            .revision
    {
        return Err(Error::new(
            "not_allowed",
            "project registration changed after command admission",
        ));
    }
    let registration = load_project_registration(tx, &project_id)?
        .ok_or_else(|| Error::new("not_allowed", "project registration was removed"))?
        .view;
    if command.target.is_none() && command.command.verb == "run" {
        require_project_registration_action(&registration, "run")?;
        if binding != targetless_project_run_binding(registration.revision) {
            return Err(Error::new(
                "not_allowed",
                "project run binding does not match its saved registration",
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

fn targetless_project_run_binding(registration_revision: i64) -> ProjectCommandBinding {
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
        match load_workspace(tx, agent_num)? {
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
            None if workspace_path == registration.checkout => (
                registration.checkout.clone(),
                registration.repo_common_dir.clone(),
                None,
                None,
            ),
            None => {
                return Err(Error::new(
                    "not_allowed",
                    "project Agent workspace is not the registered checkout",
                ));
            }
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
        profiles: profiles.clone(),
        actions: actions.clone(),
        interpreters: interpreters.clone(),
        run_env: run_env.clone(),
        revision: 1,
    };
    view.validate()?;
    Ok(ProjectRegistrationInput {
        id: input.id.clone(),
        checkout: input.checkout.clone(),
        repo_common_dir: input.repo_common_dir.clone(),
        profiles,
        actions,
        interpreters,
        run_env,
    })
}

fn is_supported_project_action(action: &str) -> bool {
    matches!(action, "stop" | "send" | "interrupt")
}

fn is_supported_project_registration_action(action: &str) -> bool {
    is_supported_project_action(action) || action == "run"
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
        if existing.view.checkout == input.checkout
            && existing.view.repo_common_dir == input.repo_common_dir
            && existing.view.profiles == input.profiles
            && existing.view.actions == input.actions
            && existing.view.interpreters == input.interpreters
            && existing.view.run_env == input.run_env
        {
            return Ok(existing.view);
        }
        let changed = tx
            .execute(
                "DELETE FROM project_registrations WHERE id = ?1 AND revision = ?2",
                params![input.id, existing.view.revision],
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
    let interpreters = encode_json(&input.interpreters)?;
    let run_env = encode_json(&input.run_env)?;
    tx.execute(
        "INSERT INTO project_registrations(
             id, checkout, repo_common_dir, profiles, actions, interpreters, run_env
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            input.id,
            input.checkout,
            input.repo_common_dir,
            profiles,
            actions,
            interpreters,
            run_env,
        ],
    )
    .map_err(sql_error)?;
    load_project_registration(tx, &input.id)?
        .map(|row| row.view)
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
            "SELECT revision, id, checkout, repo_common_dir, profiles, actions, interpreters, run_env
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
        registrations.push(row.map_err(sql_error)?.view);
    }
    Ok(registrations)
}

fn load_project_registration(
    connection: &Connection,
    id: &str,
) -> Result<Option<ProjectRegistrationRow>> {
    connection
        .query_row(
            "SELECT revision, id, checkout, repo_common_dir, profiles, actions, interpreters, run_env
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
) -> rusqlite::Result<ProjectRegistrationRow> {
    let revision: i64 = row.get(0)?;
    let id: String = row.get(1)?;
    let checkout: String = row.get(2)?;
    let repo_common_dir: String = row.get(3)?;
    let profiles_text: String = row.get(4)?;
    let actions_text: String = row.get(5)?;
    let interpreters_text: String = row.get(6)?;
    let run_env_text: String = row.get(7)?;
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
    Ok(ProjectRegistrationRow {
        view: core::api::ProjectRegistrationView {
            id,
            checkout,
            repo_common_dir,
            profiles,
            actions,
            interpreters,
            run_env,
            revision,
        },
    })
}
