//! Owner-registered project authority and project-scoped rule loading.
//!
//! Project definitions are read only from the canonical owner-registered checkout.
//! Full project configuration overlays remain unsupported.

use crate::{
    config::{self, ProjectRegistrationConfig},
    git_monitor::reject_links,
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Error, Result,
    api::{ProjectGitView, ScriptInterpreter},
};
use serde_json::Value;
use std::{collections::BTreeMap, fs, io::ErrorKind, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RegistrationSnapshot {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub git: Option<ProjectGitView>,
    pub interpreters: BTreeMap<String, ScriptInterpreter>,
    pub run_env: Vec<String>,
}

pub(super) struct LoadedProjectRules {
    pub principal: String,
    pub registration: RegistrationSnapshot,
    pub snapshot: super::reload::RuleSnapshot,
}

/// Resolve owner-configured registrations to canonical checkout and Git identity.
/// Call before Store admission; no Store lock is acquired during filesystem or Git work.
pub(super) async fn load_registrations(
    data_root: &Path,
) -> Result<(Option<String>, Vec<RegistrationSnapshot>)> {
    let (revision, configured) = config::settings::project_configs_snapshot(data_root)?;
    Ok((revision, resolve_registrations(configured).await?))
}

pub(super) async fn resolve_registrations(
    configured: Vec<ProjectRegistrationConfig>,
) -> Result<Vec<RegistrationSnapshot>> {
    if configured.is_empty() {
        return Ok(Vec::new());
    }

    let git = WorkspaceGit::new()?;
    let mut registrations = Vec::with_capacity(configured.len());
    for project in configured {
        let registration = resolve_registration(&git, project).await?;
        registrations.push(registration);
    }
    Ok(registrations)
}

/// Load one project's bounded rule files from its registered checkout and preserve
/// the originating authority principal beside the parsed definitions.
pub(super) async fn load_project_rules(
    data_root: &Path,
    registration: RegistrationSnapshot,
) -> Result<LoadedProjectRules> {
    verify_registration(data_root, &registration).await?;
    let checkout = Path::new(&registration.checkout);
    reject_links(checkout)?;
    reject_unsupported_project_overlay(checkout)?;
    reject_links(&checkout.join(".eliot").join("rules"))?;
    let snapshot = super::reload::load_project(checkout)?;
    verify_registration(data_root, &registration).await?;
    validate_project_rule_actions(&snapshot.inputs, &registration)?;

    Ok(LoadedProjectRules {
        principal: format!("project:{}", registration.id),
        registration,
        snapshot,
    })
}

fn validate_project_rule_actions(
    inputs: &[eliot_store::RuleDefinitionInput],
    registration: &RegistrationSnapshot,
) -> Result<()> {
    // The shared reload parser validates rule and trigger semantics. This
    // boundary checks only that every parsed action is owner-approved.
    for input in inputs {
        let definition: Value = serde_json::from_str(&input.canonical_definition)
            .map_err(|_| Error::new("source_integrity", "project rule definition is invalid"))?;
        let actions = definition
            .get("do")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("source_integrity", "project rule actions are missing"))?;
        for action in actions {
            let action_name = action
                .get("action")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::new("source_integrity", "project rule action kind is missing")
                })?;
            if !registration
                .actions
                .iter()
                .any(|approved| approved == action_name)
            {
                return Err(Error::new(
                    "not_allowed",
                    "project rule action is not approved by its registration",
                ));
            }
        }
    }
    Ok(())
}

/// Re-read owner policy and Git identity before registration-dependent dispatch.
/// A changed or invalid registration fails closed; callers compare this snapshot
/// with the Store readback/revision before allowing an effect.
pub(super) async fn verify_registration(
    data_root: &Path,
    expected: &RegistrationSnapshot,
) -> Result<()> {
    let configured = config::load_project_registration_config(data_root)?;
    let project = configured
        .into_iter()
        .find(|project| project.id == expected.id)
        .ok_or_else(|| Error::new("not_allowed", "project registration is no longer approved"))?;
    let git = WorkspaceGit::new()?;
    let current = resolve_registration(&git, project).await?;
    if current != *expected {
        return Err(Error::new(
            "not_allowed",
            "project registration or approved authority changed",
        ));
    }
    Ok(())
}

/// Verify a managed Agent workspace against both its frozen Store snapshot and
/// the owner-registered repository. Call outside Store locks, before dispatch.
pub(super) async fn verify_managed_workspace_identity(
    registration: &RegistrationSnapshot,
    workspace: &eliot_core::WorkspacePreparation,
    target_workspace: &str,
    expected_generation: i64,
) -> Result<()> {
    if workspace.phase != eliot_core::WorkspacePhase::Ready
        || workspace.target_generation != expected_generation
    {
        return Err(Error::new(
            "not_allowed",
            "managed project workspace snapshot differs from its registration",
        ));
    }

    verify_managed_workspace_git_identity(registration, workspace, target_workspace).await
}

pub(super) async fn verify_managed_project_resume_workspace(
    registration: &RegistrationSnapshot,
    workspace: &eliot_core::WorkspacePreparation,
    agent: &eliot_core::Agent,
    command: &eliot_core::Command,
) -> Result<()> {
    let next_generation = agent.generation.checked_add(1).ok_or_else(|| {
        Error::new(
            "not_allowed",
            "managed project resume generation is out of range",
        )
    })?;
    if command.verb != "resume"
        || !matches!(
            command.state,
            eliot_core::CommandState::Accepted | eliot_core::CommandState::Queued
        )
        || command.target.as_deref() != Some(agent.id.as_str())
        || command.expected_generation != Some(agent.generation)
        || command.generation != Some(agent.generation)
        || workspace.agent != agent.id
        || workspace.start_command_id != command.id
        || workspace.target_generation != next_generation
        || !matches!(
            workspace.phase,
            eliot_core::WorkspacePhase::Creating | eliot_core::WorkspacePhase::Ready
        )
        || workspace.setup_index != workspace.frozen_setup.len()
        || agent.workspace.as_deref() != Some(workspace.worktree_path.as_str())
    {
        return Err(Error::new(
            "not_allowed",
            "managed project resume workspace snapshot does not match its accepted command",
        ));
    }

    verify_managed_workspace_git_identity(registration, workspace, &workspace.worktree_path).await
}

async fn verify_managed_workspace_git_identity(
    registration: &RegistrationSnapshot,
    workspace: &eliot_core::WorkspacePreparation,
    target_workspace: &str,
) -> Result<()> {
    if workspace.project != registration.checkout
        || workspace.repo_path != registration.checkout
        || workspace.repo_common_dir != registration.repo_common_dir
        || workspace.worktree_path != target_workspace
    {
        return Err(Error::new(
            "not_allowed",
            "managed project workspace snapshot differs from its registration",
        ));
    }

    let (canonical_root, repo_common_dir) = WorkspaceGit::new()?
        .repository_identity(target_workspace)
        .await?;
    if canonical_root != workspace.worktree_path
        || repo_common_dir != workspace.repo_common_dir
        || repo_common_dir != registration.repo_common_dir
    {
        return Err(Error::new(
            "not_allowed",
            "managed project workspace Git identity changed",
        ));
    }
    Ok(())
}

async fn resolve_registration(
    git: &WorkspaceGit,
    project: ProjectRegistrationConfig,
) -> Result<RegistrationSnapshot> {
    let (checkout, repo_common_dir) = git.repository_identity(&project.path).await?;
    Ok(RegistrationSnapshot {
        id: project.id,
        checkout,
        repo_common_dir,
        profiles: project.profiles,
        actions: project.actions,
        git: project.git,
        interpreters: project.interpreters,
        run_env: project.run_env,
    })
}

fn reject_unsupported_project_overlay(checkout: &Path) -> Result<()> {
    let config_path = checkout.join(".eliot").join("config.toml");
    reject_links(&config_path)?;
    match fs::symlink_metadata(&config_path) {
        Ok(_) => Err(Error::new(
            "unsupported",
            "project configuration overlays are not implemented",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::new("unavailable", "cannot inspect project metadata")),
    }
}
