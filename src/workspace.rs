//! Trusted repository registrations and exact manager-owned workspace leases.
//!
//! Repository identity comes from the local ForgeConfig mapping. The launcher
//! supplies Task/Attempt scope, never a repository path, cwd, or Git options.

use crate::{
    error::{Error, Result},
    forge::{self, ForgeConfig, ForgeProject},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

const MAX_WORKSPACE_PROJECTS: usize = 128;
const MAX_WORKSPACE_ROOTS: usize = 16;
const MAX_ALLOWED_PATHS: usize = 128;
const MAX_ALLOWED_SYMBOLS: usize = 128;
const MAX_SCOPE_TEXT_BYTES: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceConfig {
    /// Explicit operator-owned roots for manager-created worktrees, keyed by
    /// the same project IDs used by Tasks and the trusted Forge mapping.
    pub projects: BTreeMap<String, WorkspaceProjectConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceProjectConfig {
    pub allowed_roots: Vec<PathBuf>,
}

impl WorkspaceConfig {
    /// Resolve configured roots relative to the config file and reject paths
    /// whose existing components traverse a symlink or reparse point. Roots
    /// are validated, never created or rewritten here.
    pub fn resolve_paths(&mut self, config_dir: &Path) -> Result<()> {
        if self.projects.len() > MAX_WORKSPACE_PROJECTS {
            return Err(workspace_config_error(
                "too many workspace project mappings",
            ));
        }
        for (project_id, project) in &mut self.projects {
            validate_project_id(project_id)?;
            if project.allowed_roots.is_empty() || project.allowed_roots.len() > MAX_WORKSPACE_ROOTS
            {
                return Err(workspace_config_error(
                    "each workspace project requires 1..=16 allowed_roots",
                ));
            }
            let mut seen = BTreeSet::new();
            let mut resolved = Vec::with_capacity(project.allowed_roots.len());
            for configured in &project.allowed_roots {
                let candidate = if configured.is_absolute() {
                    configured.clone()
                } else {
                    config_dir.join(configured)
                };
                reject_reparse_components(&candidate)?;
                let canonical = fs::canonicalize(&candidate).map_err(|_| {
                    workspace_config_error("allowed workspace root could not be resolved")
                })?;
                let metadata = fs::symlink_metadata(&canonical).map_err(|_| {
                    workspace_config_error("allowed workspace root metadata is unavailable")
                })?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(workspace_config_error(
                        "allowed workspace root must be an existing regular directory",
                    ));
                }
                validate_directory_security(&canonical)?;
                let key = path_key(&canonical);
                if !seen.insert(key) {
                    return Err(workspace_config_error(
                        "allowed workspace roots must be unique after canonicalization",
                    ));
                }
                resolved.push(canonical);
            }
            project.allowed_roots = resolved;
        }
        Ok(())
    }

    pub fn validate(&self, forge_config: &ForgeConfig) -> Result<()> {
        if self.projects.len() > MAX_WORKSPACE_PROJECTS {
            return Err(workspace_config_error(
                "too many workspace project mappings",
            ));
        }
        for (project_id, workspace) in &self.projects {
            validate_project_id(project_id)?;
            let repository = forge_config.projects.get(project_id).ok_or_else(|| {
                workspace_config_error("workspace project has no trusted Forge repository mapping")
            })?;
            if workspace.allowed_roots.is_empty()
                || workspace.allowed_roots.len() > MAX_WORKSPACE_ROOTS
            {
                return Err(workspace_config_error(
                    "each workspace project requires 1..=16 allowed_roots",
                ));
            }
            if !repository.repository_path.is_absolute()
                || !repository
                    .repository_path
                    .metadata()
                    .is_ok_and(|metadata| metadata.is_dir())
                || forge::canonical_repository(&repository.canonical_repository).is_err()
            {
                return Err(workspace_config_error(
                    "workspace project does not have a valid trusted repository",
                ));
            }
            reject_reparse_components(&repository.repository_path)?;
            let canonical_repository = fs::canonicalize(&repository.repository_path)?;
            let mut seen = BTreeSet::new();
            for root in &workspace.allowed_roots {
                if !root.is_absolute()
                    || fs::canonicalize(root).ok().as_deref() != Some(root.as_path())
                    || !root.metadata().is_ok_and(|metadata| metadata.is_dir())
                {
                    return Err(workspace_config_error(
                        "allowed workspace roots must be canonical existing directories",
                    ));
                }
                reject_reparse_components(root)?;
                validate_directory_security(root)?;
                if paths_overlap(root, &canonical_repository) {
                    return Err(workspace_config_error(
                        "allowed workspace roots must be separate from the trusted checkout",
                    ));
                }
                if !seen.insert(path_key(root)) {
                    return Err(workspace_config_error(
                        "allowed workspace roots must be unique after canonicalization",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Database-backed registration facts combined with the already-resolved
/// trusted configuration. Absolute paths are host-private and this type is
/// never serialized. Filesystem revalidation belongs to the async host phase.
#[derive(Debug, Clone)]
pub struct WorkspaceRegistration {
    pub registration_id: String,
    pub project_id: String,
    pub trusted_repository: String,
    pub repository_path: PathBuf,
    pub remote_name: String,
    pub allowed_roots: Vec<PathBuf>,
    pub registration_digest: String,
    pub generation: i64,
}

impl WorkspaceRegistration {
    pub fn from_config(
        project_id: &str,
        workspace: &WorkspaceConfig,
        forge_config: &ForgeConfig,
        registration_id: String,
        generation: i64,
    ) -> Result<Self> {
        if generation <= 0 || registration_id.trim().is_empty() {
            return Err(Error::invalid("invalid workspace registration identity"));
        }
        let workspace_project = workspace.projects.get(project_id).ok_or_else(|| {
            Error::new(
                "WORKSPACE_UNREGISTERED",
                "project has no allowed workspace roots",
            )
        })?;
        let forge_project = forge_config.projects.get(project_id).ok_or_else(|| {
            Error::new(
                "WORKSPACE_UNREGISTERED",
                "project has no trusted repository mapping",
            )
        })?;
        let trusted_repository = forge::canonical_repository(&forge_project.canonical_repository)?;
        let repository_path = forge_project.repository_path.clone();
        if !repository_path.is_absolute()
            || workspace_project
                .allowed_roots
                .iter()
                .any(|root| !root.is_absolute())
        {
            return Err(Error::new(
                "WORKSPACE_CONFIG",
                "workspace paths must be resolved before registration sync",
            ));
        }
        let allowed_roots = workspace_project.allowed_roots.clone();
        let registration_digest = registration_digest(
            project_id,
            &trusted_repository,
            &repository_path,
            &forge_project.remote_name,
            &allowed_roots,
        )?;
        Ok(Self {
            registration_id,
            project_id: project_id.to_owned(),
            trusted_repository,
            repository_path,
            remote_name: forge_project.remote_name.clone(),
            allowed_roots,
            registration_digest,
            generation,
        })
    }

    /// Opaque project-local identity safe for a manager-facing receipt.
    pub fn repository_handle(&self) -> String {
        format!("repo-{}", &self.registration_digest[..16])
    }
}

/// Exact Task-scoped intent. `expected_baseline_commit` is an optional pinned
/// Task fact; when absent, the host captures the registered checkout's exact
/// HEAD only after reserving the preparing lease.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceLeasePlan {
    pub project_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub operation_id: String,
    pub plan_digest: String,
    pub owner_client_id: String,
    pub attempt_id: Option<String>,
    pub allowed_paths: Vec<String>,
    pub allowed_symbols: Vec<String>,
    pub expected_baseline_commit: Option<String>,
}

impl WorkspaceLeasePlan {
    pub fn validate(&self) -> Result<()> {
        validate_project_id(&self.project_id)?;
        for (field, value, max) in [
            ("task_id", self.task_id.as_str(), 512),
            ("operation_id", self.operation_id.as_str(), 128),
            ("owner_client_id", self.owner_client_id.as_str(), 128),
        ] {
            validate_bounded_identifier(field, value, max)?;
        }
        if self.task_revision <= 0 {
            return Err(Error::invalid("task_revision must be positive"));
        }
        validate_sha256_claim(&self.plan_digest, "plan_digest")?;
        if let Some(attempt_id) = &self.attempt_id {
            validate_bounded_identifier("attempt_id", attempt_id, 128)?;
        }
        if let Some(commit) = &self.expected_baseline_commit
            && !forge::valid_object_id(commit)
        {
            return Err(Error::invalid(
                "expected_baseline_commit must be an exact Git object ID",
            ));
        }
        if self.allowed_paths.len() > MAX_ALLOWED_PATHS
            || self.allowed_symbols.len() > MAX_ALLOWED_SYMBOLS
            || self.allowed_paths.is_empty() && self.allowed_symbols.is_empty()
        {
            return Err(Error::invalid(
                "workspace scope must contain bounded allowed paths or symbols",
            ));
        }
        let mut paths = BTreeSet::new();
        for path in &self.allowed_paths {
            validate_allowed_path(path)?;
            if !paths.insert(path.clone()) {
                return Err(Error::invalid("allowed_paths contains a duplicate"));
            }
        }
        let mut symbols = BTreeSet::new();
        for symbol in &self.allowed_symbols {
            if symbol.trim().is_empty()
                || symbol.len() > MAX_SCOPE_TEXT_BYTES
                || symbol.chars().any(char::is_control)
                || !symbols.insert(symbol.clone())
            {
                return Err(Error::invalid("allowed_symbols contains an invalid entry"));
            }
        }
        Ok(())
    }
}

/// Opaque reservation returned from the first Store transaction. It includes
/// a host-private exact path but cannot be serialized into a launch receipt.
#[derive(Debug, Clone)]
pub struct LeaseReservation {
    pub lease_id: String,
    pub registration_id: String,
    pub registration_generation: i64,
    pub generation: i64,
    pub intent_digest: String,
    pub project_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub operation_id: String,
    pub plan_digest: String,
    pub owner_client_id: String,
    pub attempt_id: Option<String>,
    pub allowed_paths: Vec<String>,
    pub allowed_symbols: Vec<String>,
    pub expected_baseline_commit: Option<String>,
    pub branch_ref: String,
    pub worktree_handle: String,
    workspace_path: PathBuf,
}

impl LeaseReservation {
    pub(crate) fn workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    // This initializer combines independently verified Store, plan, and host
    // path facts; retaining those inputs explicitly avoids hiding provenance
    // in a one-use parameter bundle.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_store(
        lease_id: String,
        registration_id: String,
        registration_generation: i64,
        generation: i64,
        intent_digest: String,
        plan: &WorkspaceLeasePlan,
        expected_baseline_commit: Option<String>,
        branch_ref: String,
        worktree_handle: String,
        workspace_path: PathBuf,
    ) -> Self {
        Self {
            lease_id,
            registration_id,
            registration_generation,
            generation,
            intent_digest,
            project_id: plan.project_id.clone(),
            task_id: plan.task_id.clone(),
            task_revision: plan.task_revision,
            operation_id: plan.operation_id.clone(),
            plan_digest: plan.plan_digest.clone(),
            owner_client_id: plan.owner_client_id.clone(),
            attempt_id: plan.attempt_id.clone(),
            allowed_paths: plan.allowed_paths.clone(),
            allowed_symbols: plan.allowed_symbols.clone(),
            expected_baseline_commit,
            branch_ref,
            worktree_handle,
            workspace_path,
        }
    }
}

/// Final host-only evidence. `workspace_path` is persisted privately but is
/// intentionally absent from `LeaseAuthorityRef` and all receipt projections.
#[derive(Debug, Clone)]
pub struct LeaseEvidence {
    pub lease_id: String,
    pub registration_id: String,
    pub registration_generation: i64,
    pub registration_digest: String,
    pub generation: i64,
    pub intent_digest: String,
    pub project_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub operation_id: String,
    pub plan_digest: String,
    pub owner_client_id: String,
    pub attempt_id: Option<String>,
    pub allowed_paths: Vec<String>,
    pub allowed_symbols: Vec<String>,
    pub baseline_commit: String,
    pub branch_ref: String,
    pub worktree_handle: String,
    pub clean_state: Value,
    pub(crate) workspace_path: PathBuf,
}

/// Path-free durable authority reference returned to the launcher.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseAuthorityRef {
    pub lease_id: String,
    pub registration_id: String,
    pub registration_generation: i64,
    pub project_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub operation_id: String,
    pub plan_digest: String,
    pub owner_client_id: String,
    pub attempt_id: Option<String>,
    pub generation: i64,
    pub baseline_commit: String,
    pub branch_ref: String,
    pub worktree_handle: String,
    pub binding_digest: String,
    pub state: String,
}

/// A prepared workspace keeps its local path in a non-serializable object.
#[derive(Debug, Clone)]
pub struct PreparedWorkspace {
    evidence: LeaseEvidence,
}

impl PreparedWorkspace {
    pub(crate) fn evidence(&self) -> &LeaseEvidence {
        &self.evidence
    }

    pub fn public_facts(&self) -> Value {
        json!({
            "repository_handle": format!("repo-{}", &self.evidence.registration_digest[..16]),
            "baseline_commit": self.evidence.baseline_commit,
            "worktree_handle": self.evidence.worktree_handle,
            "branch": self.evidence.branch_ref,
            "write_lease_generation": self.evidence.generation,
            "allowed_paths": self.evidence.allowed_paths,
            "allowed_symbols": self.evidence.allowed_symbols,
            "dirty": false,
            "clean_state": self.evidence.clean_state,
        })
    }
}

/// Create/verify the exact worktree outside the Store transaction. Any Git or
/// filesystem failure after reservation is an unknown external effect; callers
/// must reconcile this exact reservation and must never choose a replacement.
pub fn prepare_lease(
    forge_config: &ForgeConfig,
    forge_project: &ForgeProject,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
) -> Result<PreparedWorkspace> {
    plan.validate()?;
    verify_registration_binding(registration, plan.project_id.as_str())?;
    verify_reservation_binding(registration, reservation, plan)?;
    verify_trusted_repository(forge_config, forge_project, registration)?;

    let source_head_before = current_head(forge_config, forge_project)?;
    require_clean_checkout(forge_config, forge_project)?;
    let baseline_commit = if let Some(expected) = &plan.expected_baseline_commit {
        resolve_exact_commit(forge_config, forge_project, expected)?
    } else {
        source_head_before.clone()
    };
    let workspace_path = reservation.workspace_path();
    ensure_path_absent(workspace_path)?;
    let parent = workspace_path
        .parent()
        .ok_or_else(|| Error::new("WORKSPACE_PATH", "workspace path has no parent"))?;
    if !registration.allowed_roots.iter().any(|root| parent == root) {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "reserved worktree path escaped the registered workspace roots",
        ));
    }
    // Override repository-local hooks for this checkout. The path is an
    // uncreated, lease-unique sibling beneath the ACL-checked registered root.
    let disabled_hooks = parent.join(format!(".eliot-hooks-disabled-{}", reservation.lease_id));
    ensure_path_absent(&disabled_hooks)?;
    let branch_short = reservation
        .branch_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| Error::new("WORKSPACE_BRANCH", "invalid reserved branch ref"))?;
    git_text(
        forge_config,
        forge_project,
        &[
            "check-ref-format".into(),
            "--branch".into(),
            branch_short.into(),
        ],
    )?;
    git_text(
        forge_config,
        forge_project,
        &[
            "-c".into(),
            format!("core.hooksPath={}", disabled_hooks.to_string_lossy()),
            "worktree".into(),
            "add".into(),
            "-b".into(),
            branch_short.into(),
            "--".into(),
            workspace_path.to_string_lossy().into_owned(),
            baseline_commit.clone(),
        ],
    )?;
    // Git creates the target directory as part of `worktree add`; harden it
    // immediately before inspecting or accepting any worktree contents.
    crate::platform::private_permissions(workspace_path, true)?;
    reject_reparse_components(workspace_path)?;
    validate_directory_security(workspace_path)?;

    verify_trusted_repository(forge_config, forge_project, registration)?;
    let source_head_after = current_head(forge_config, forge_project)?;
    require_clean_checkout(forge_config, forge_project)?;
    if source_head_after != source_head_before {
        return Err(Error::new(
            "WORKSPACE_BASELINE_CHANGED",
            "trusted checkout HEAD changed while preparing the worktree",
        ));
    }
    verify_worktree(
        forge_config,
        forge_project,
        registration,
        reservation,
        Some(baseline_commit.as_str()),
        true,
    )?;
    let clean_state = json!({
        "status":"verified_clean",
        "source_head_before":source_head_before,
        "source_head_after":source_head_after,
        "source_head_rechecked":true,
        "tracked_and_untracked":true,
        "worktree_head":baseline_commit,
    });
    Ok(PreparedWorkspace {
        evidence: LeaseEvidence {
            lease_id: reservation.lease_id.clone(),
            registration_id: registration.registration_id.clone(),
            registration_generation: registration.generation,
            registration_digest: registration.registration_digest.clone(),
            generation: reservation.generation,
            intent_digest: reservation.intent_digest.clone(),
            project_id: plan.project_id.clone(),
            task_id: plan.task_id.clone(),
            task_revision: plan.task_revision,
            operation_id: plan.operation_id.clone(),
            plan_digest: plan.plan_digest.clone(),
            owner_client_id: plan.owner_client_id.clone(),
            attempt_id: plan.attempt_id.clone(),
            allowed_paths: plan.allowed_paths.clone(),
            allowed_symbols: plan.allowed_symbols.clone(),
            baseline_commit,
            branch_ref: reservation.branch_ref.clone(),
            worktree_handle: reservation.worktree_handle.clone(),
            clean_state,
            workspace_path: workspace_path.to_path_buf(),
        },
    })
}

/// Read-only restart reconciliation for one previously reserved path/ref.
/// It never creates, repairs, deletes, or substitutes a workspace.
pub fn reconcile_workspace(
    forge_config: &ForgeConfig,
    forge_project: &ForgeProject,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
) -> Result<LeaseEvidence> {
    plan.validate()?;
    verify_registration_binding(registration, plan.project_id.as_str())?;
    verify_reservation_binding(registration, reservation, plan)?;
    verify_trusted_repository(forge_config, forge_project, registration)?;
    let source_head_before = current_head(forge_config, forge_project)?;
    require_clean_checkout(forge_config, forge_project)?;
    let baseline_commit = verify_worktree(
        forge_config,
        forge_project,
        registration,
        reservation,
        plan.expected_baseline_commit.as_deref(),
        false,
    )?;
    if plan.expected_baseline_commit.is_none() && source_head_before != baseline_commit {
        return Err(Error::new(
            "WORKSPACE_BASELINE_CHANGED",
            "trusted checkout no longer matches the interrupted worktree baseline",
        ));
    }
    require_clean_checkout(forge_config, forge_project)?;
    let source_head_after = current_head(forge_config, forge_project)?;
    if source_head_before != source_head_after {
        return Err(Error::new(
            "WORKSPACE_BASELINE_CHANGED",
            "trusted checkout HEAD changed during restart reconciliation",
        ));
    }
    let clean_state = json!({
        "status":"reconciled_clean",
        "tracked_and_untracked":true,
        "source_head_before":source_head_before,
        "source_head_after":source_head_after,
        "worktree_head":baseline_commit,
        "source_head_rechecked":true,
        "reconciled":true,
    });
    Ok(LeaseEvidence {
        lease_id: reservation.lease_id.clone(),
        registration_id: registration.registration_id.clone(),
        registration_generation: registration.generation,
        registration_digest: registration.registration_digest.clone(),
        generation: reservation.generation,
        intent_digest: reservation.intent_digest.clone(),
        project_id: plan.project_id.clone(),
        task_id: plan.task_id.clone(),
        task_revision: plan.task_revision,
        operation_id: plan.operation_id.clone(),
        plan_digest: plan.plan_digest.clone(),
        owner_client_id: plan.owner_client_id.clone(),
        attempt_id: plan.attempt_id.clone(),
        allowed_paths: plan.allowed_paths.clone(),
        allowed_symbols: plan.allowed_symbols.clone(),
        baseline_commit,
        branch_ref: reservation.branch_ref.clone(),
        worktree_handle: reservation.worktree_handle.clone(),
        clean_state,
        workspace_path: reservation.workspace_path().to_path_buf(),
    })
}

pub(crate) fn final_binding_digest(evidence: &LeaseEvidence) -> Result<String> {
    let canonical = model::canonical(&json!({
        "lease_id":evidence.lease_id,
        "registration_id":evidence.registration_id,
        "registration_generation":evidence.registration_generation,
        "registration_digest":evidence.registration_digest,
        "generation":evidence.generation,
        "intent_digest":evidence.intent_digest,
        "project_id":evidence.project_id,
        "task_id":evidence.task_id,
        "task_revision":evidence.task_revision,
        "operation_id":evidence.operation_id,
        "plan_digest":evidence.plan_digest,
        "owner_client_id":evidence.owner_client_id,
        "attempt_id":evidence.attempt_id,
        "allowed_paths":evidence.allowed_paths,
        "allowed_symbols":evidence.allowed_symbols,
        "baseline_commit":evidence.baseline_commit,
        "branch_ref":evidence.branch_ref,
        "worktree_handle":evidence.worktree_handle,
        "workspace_path":path_string(&evidence.workspace_path)?,
        "clean_state":evidence.clean_state,
    }))?;
    Ok(model::digest(canonical.as_bytes()))
}

pub(crate) fn evidence_matches_reservation(
    evidence: &LeaseEvidence,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
    registration: &WorkspaceRegistration,
) -> Result<()> {
    verify_reservation_binding(registration, reservation, plan)?;
    if evidence.lease_id != reservation.lease_id
        || evidence.registration_id != registration.registration_id
        || evidence.registration_generation != registration.generation
        || evidence.registration_digest != registration.registration_digest
        || evidence.generation != reservation.generation
        || evidence.intent_digest != reservation.intent_digest
        || evidence.project_id != plan.project_id
        || evidence.task_id != plan.task_id
        || evidence.task_revision != plan.task_revision
        || evidence.operation_id != plan.operation_id
        || evidence.plan_digest != plan.plan_digest
        || evidence.owner_client_id != plan.owner_client_id
        || evidence.attempt_id != plan.attempt_id
        || evidence.allowed_paths != plan.allowed_paths
        || evidence.allowed_symbols != plan.allowed_symbols
        || evidence.branch_ref != reservation.branch_ref
        || evidence.worktree_handle != reservation.worktree_handle
        || evidence.workspace_path != reservation.workspace_path
        || !forge::valid_object_id(&evidence.baseline_commit)
        || evidence.clean_state["worktree_head"] != evidence.baseline_commit
        || evidence.clean_state["source_head_rechecked"] != true
        || !forge::valid_object_id(
            evidence.clean_state["source_head_before"]
                .as_str()
                .unwrap_or_default(),
        )
        || evidence.clean_state["source_head_before"] != evidence.clean_state["source_head_after"]
        || (plan.expected_baseline_commit.is_none()
            && evidence.clean_state["source_head_before"] != evidence.baseline_commit)
        || plan
            .expected_baseline_commit
            .as_deref()
            .is_some_and(|expected| !evidence.baseline_commit.eq_ignore_ascii_case(expected))
        || evidence.clean_state["tracked_and_untracked"] != true
        || !matches!(
            evidence.clean_state["status"].as_str(),
            Some("verified_clean" | "reconciled_clean")
        )
    {
        return Err(Error::new(
            "WORKSPACE_EVIDENCE_MISMATCH",
            "workspace evidence does not match the exact reserved lease",
        ));
    }
    Ok(())
}

fn verify_registration_binding(
    registration: &WorkspaceRegistration,
    project_id: &str,
) -> Result<()> {
    if registration.project_id != project_id
        || registration.generation <= 0
        || registration.allowed_roots.is_empty()
        || forge::canonical_repository(&registration.trusted_repository)?
            != registration.trusted_repository
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "workspace registration does not match the exact Task project",
        ));
    }
    for root in &registration.allowed_roots {
        reject_reparse_components(root)?;
        let canonical = fs::canonicalize(root).map_err(|_| {
            Error::new("WORKSPACE_ROOT", "registered workspace root is unavailable")
        })?;
        if canonical != *root {
            return Err(Error::new(
                "WORKSPACE_ROOT",
                "registered workspace root changed after configuration validation",
            ));
        }
        validate_directory_security(root)?;
    }
    if registration_digest(
        &registration.project_id,
        &registration.trusted_repository,
        &registration.repository_path,
        &registration.remote_name,
        &registration.allowed_roots,
    )? != registration.registration_digest
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "workspace registration digest does not match its configured facts",
        ));
    }
    Ok(())
}

fn verify_reservation_binding(
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    plan: &WorkspaceLeasePlan,
) -> Result<()> {
    if reservation.registration_id != registration.registration_id
        || reservation.registration_generation != registration.generation
        || reservation.project_id != plan.project_id
        || reservation.task_id != plan.task_id
        || reservation.task_revision != plan.task_revision
        || reservation.operation_id != plan.operation_id
        || reservation.plan_digest != plan.plan_digest
        || reservation.owner_client_id != plan.owner_client_id
        || reservation.attempt_id != plan.attempt_id
        || reservation.allowed_paths != plan.allowed_paths
        || reservation.allowed_symbols != plan.allowed_symbols
        || reservation.expected_baseline_commit != plan.expected_baseline_commit
        || reservation.generation <= 0
        || reservation.lease_id.trim().is_empty()
        || reservation.worktree_handle != format!("wt-{}", reservation.lease_id)
        || reservation.branch_ref != format!("refs/heads/codex/swarm/{}", reservation.lease_id)
        || !registration
            .allowed_roots
            .iter()
            .any(|root| reservation.workspace_path.parent() == Some(root.as_path()))
    {
        return Err(Error::new(
            "WORKSPACE_RESERVATION_MISMATCH",
            "workspace reservation does not match the exact Task launch intent",
        ));
    }
    Ok(())
}

fn verify_trusted_repository(
    forge_config: &ForgeConfig,
    forge_project: &ForgeProject,
    registration: &WorkspaceRegistration,
) -> Result<()> {
    if forge::canonical_repository(&forge_project.canonical_repository)?
        != registration.trusted_repository
        || fs::canonicalize(&forge_project.repository_path)? != registration.repository_path
        || forge_project.remote_name != registration.remote_name
    {
        return Err(Error::new(
            "WORKSPACE_REGISTRATION_CHANGED",
            "current Forge mapping differs from the registered repository",
        ));
    }
    reject_reparse_components(&registration.repository_path)?;
    validate_directory_security(&registration.repository_path)?;
    let top = git_text(
        forge_config,
        forge_project,
        &["rev-parse".into(), "--show-toplevel".into()],
    )?;
    let top = fs::canonicalize(Path::new(top.trim())).map_err(|_| {
        Error::new(
            "WORKSPACE_REPOSITORY",
            "Git root could not be canonicalized",
        )
    })?;
    if top != registration.repository_path {
        return Err(Error::new(
            "WORKSPACE_REPOSITORY",
            "configured path is not the registered Git root",
        ));
    }
    let remote = git_text(
        forge_config,
        forge_project,
        &[
            "remote".into(),
            "get-url".into(),
            registration.remote_name.clone(),
        ],
    )?;
    if forge::repository_from_remote_url(remote.trim())? != registration.trusted_repository {
        return Err(Error::new(
            "WORKSPACE_REPOSITORY_IDENTITY",
            "configured Git remote does not match the trusted repository identity",
        ));
    }
    Ok(())
}

fn current_head(forge_config: &ForgeConfig, project: &ForgeProject) -> Result<String> {
    let head = git_text(
        forge_config,
        project,
        &[
            "rev-parse".into(),
            "--verify".into(),
            "--end-of-options".into(),
            "HEAD^{commit}".into(),
        ],
    )?;
    let head = head.trim();
    if !forge::valid_object_id(head) {
        return Err(Error::new(
            "WORKSPACE_BASELINE_UNKNOWN",
            "trusted checkout did not return an exact full commit ID",
        ));
    }
    Ok(head.to_ascii_lowercase())
}

fn resolve_exact_commit(
    forge_config: &ForgeConfig,
    project: &ForgeProject,
    expected: &str,
) -> Result<String> {
    if !forge::valid_object_id(expected) {
        return Err(Error::invalid(
            "expected baseline is not a full Git object ID",
        ));
    }
    let expression = format!("{expected}^{{commit}}");
    let resolved = git_text(
        forge_config,
        project,
        &[
            "rev-parse".into(),
            "--verify".into(),
            "--end-of-options".into(),
            expression,
        ],
    )?;
    let resolved = resolved.trim();
    if !forge::valid_object_id(resolved) || !resolved.eq_ignore_ascii_case(expected) {
        return Err(Error::new(
            "WORKSPACE_BASELINE_MISMATCH",
            "trusted repository does not contain the exact pinned commit",
        ));
    }
    Ok(resolved.to_ascii_lowercase())
}

fn require_clean_checkout(forge_config: &ForgeConfig, project: &ForgeProject) -> Result<()> {
    let status = git_text(
        forge_config,
        project,
        &[
            "status".into(),
            "--porcelain=v1".into(),
            "--untracked-files=all".into(),
            "--ignore-submodules=none".into(),
        ],
    )?;
    if !status.is_empty() {
        return Err(Error::new(
            "WORKSPACE_DIRTY",
            "trusted checkout has tracked or untracked changes",
        ));
    }
    Ok(())
}

fn verify_worktree(
    forge_config: &ForgeConfig,
    forge_project: &ForgeProject,
    registration: &WorkspaceRegistration,
    reservation: &LeaseReservation,
    expected_commit: Option<&str>,
    newly_created: bool,
) -> Result<String> {
    let workspace_path = reservation.workspace_path();
    reject_reparse_components(workspace_path)?;
    let canonical = fs::canonicalize(workspace_path)
        .map_err(|_| Error::new("WORKSPACE_UNKNOWN", "reserved worktree path is not present"))?;
    if canonical != workspace_path {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "reserved worktree path changed after registration",
        ));
    }
    validate_directory_security(workspace_path)?;
    let args = |tail: &[&str]| {
        let mut result = vec![
            "-C".to_owned(),
            workspace_path.to_string_lossy().into_owned(),
        ];
        result.extend(tail.iter().map(|value| (*value).to_owned()));
        result
    };
    let head = git_text(
        forge_config,
        forge_project,
        &args(&["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"]),
    )?;
    let head = head.trim();
    if !forge::valid_object_id(head)
        || expected_commit.is_some_and(|expected| !head.eq_ignore_ascii_case(expected))
    {
        return Err(Error::new(
            "WORKSPACE_BASELINE_MISMATCH",
            "worktree HEAD does not match the exact reserved baseline",
        ));
    }
    let branch = git_text(
        forge_config,
        forge_project,
        &args(&["symbolic-ref", "--quiet", "HEAD"]),
    )?;
    if branch.trim() != reservation.branch_ref {
        return Err(Error::new(
            "WORKSPACE_BRANCH_MISMATCH",
            "worktree is not attached to its exact lease branch",
        ));
    }
    let status = git_text(
        forge_config,
        forge_project,
        &args(&[
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ]),
    )?;
    if !status.is_empty() {
        return Err(Error::new(
            "WORKSPACE_DIRTY",
            "reserved worktree contains tracked or untracked changes",
        ));
    }
    let worktrees = git_text(
        forge_config,
        forge_project,
        &["worktree".into(), "list".into(), "--porcelain".into()],
    )?;
    let expected_branch = format!("branch {}", reservation.branch_ref);
    let exact_record = worktrees.split("\n\n").any(|record| {
        record.lines().any(|line| {
            line.strip_prefix("worktree ")
                .is_some_and(|path| path_key(Path::new(path)) == path_key(workspace_path))
        }) && record.lines().any(|line| line == expected_branch)
    });
    if !exact_record {
        return Err(Error::new(
            "WORKSPACE_NOT_REGISTERED",
            "Git does not report the exact reserved worktree and branch",
        ));
    }
    if newly_created
        && !registration
            .allowed_roots
            .iter()
            .any(|root| workspace_path.parent() == Some(root.as_path()))
    {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "prepared worktree escaped its registered root",
        ));
    }
    Ok(head.to_ascii_lowercase())
}

fn ensure_path_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(Error::new(
            "WORKSPACE_PATH_CONFLICT",
            "reserved worktree path already exists; it will not be adopted or removed",
        )),
        Err(_) => Err(Error::new(
            "WORKSPACE_PATH_UNKNOWN",
            "reserved worktree path could not be inspected",
        )),
    }
}

fn git_text(config: &ForgeConfig, project: &ForgeProject, args: &[String]) -> Result<String> {
    let output = forge::run_git(config, project, args)?;
    if output.timed_out || output.stdout_truncated || !output.status.success() {
        return Err(Error::new(
            "WORKSPACE_GIT_UNKNOWN",
            "bounded Git inspection did not complete with an exact result",
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| Error::new("WORKSPACE_GIT_UNKNOWN", "Git returned non-UTF8 metadata"))
}

fn registration_digest(
    project_id: &str,
    trusted_repository: &str,
    repository_path: &Path,
    remote_name: &str,
    allowed_roots: &[PathBuf],
) -> Result<String> {
    let canonical = model::canonical(&json!({
        "version":1,
        "project_id":project_id,
        "trusted_repository":trusted_repository,
        "repository_path":path_string(repository_path)?,
        "remote_name":remote_name,
        "allowed_roots":allowed_roots.iter().map(|path| path_string(path)).collect::<Result<Vec<_>>>()?,
    }))?;
    Ok(model::digest(canonical.as_bytes()))
}

fn path_string(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| Error::new("WORKSPACE_PATH", "workspace path is not UTF-8"))?;
    if value.chars().any(char::is_control) {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "workspace path contains control characters",
        ));
    }
    Ok(value.to_owned())
}

fn path_key(path: &Path) -> String {
    let value = path.to_string_lossy().replace('\\', "/");
    if cfg!(windows) {
        value.to_ascii_lowercase()
    } else {
        value
    }
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    let left = path_key(left);
    let right = path_key(right);
    left == right
        || left
            .strip_prefix(&right)
            .is_some_and(|tail| tail.starts_with('/'))
        || right
            .strip_prefix(&left)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn reject_reparse_components(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "workspace paths must be absolute",
        ));
    }
    let mut current = PathBuf::new();
    let mut anchored = false;
    for component in path.components() {
        let inspect = match component {
            // A Windows prefix (for example `C:` or `\\?\C:`) is not yet
            // an absolute path. Wait until RootDir has been appended so the
            // first metadata query addresses the assembled volume root.
            Component::Prefix(_) => {
                current.push(component.as_os_str());
                false
            }
            Component::RootDir => {
                current.push(component.as_os_str());
                anchored = current.is_absolute();
                if !anchored {
                    return Err(Error::new(
                        "WORKSPACE_PATH",
                        "workspace path root could not be assembled",
                    ));
                }
                true
            }
            Component::CurDir => false,
            Component::ParentDir => {
                return Err(Error::new(
                    "WORKSPACE_PATH",
                    "workspace paths cannot traverse parent components",
                ));
            }
            Component::Normal(part) => {
                if !anchored {
                    return Err(Error::new(
                        "WORKSPACE_PATH",
                        "workspace path component preceded its absolute root",
                    ));
                }
                current.push(part);
                true
            }
        };
        if inspect {
            let metadata = fs::symlink_metadata(&current).map_err(|_| {
                Error::new("WORKSPACE_PATH", "workspace path component is unavailable")
            })?;
            if is_reparse_or_symlink(&metadata) {
                return Err(Error::new(
                    "WORKSPACE_PATH_REPARSE",
                    "workspace paths cannot traverse symlinks or reparse points",
                ));
            }
        }
    }
    if !anchored {
        return Err(Error::new(
            "WORKSPACE_PATH",
            "workspace path root could not be assembled",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_root_is_assembled_before_component_metadata_checks() {
        let cwd = std::env::current_dir().expect("test working directory is available");
        let mut root = PathBuf::new();
        for component in cwd.components() {
            match component {
                Component::Prefix(_) | Component::RootDir => {
                    root.push(component.as_os_str());
                }
                _ => break,
            }
        }
        assert!(root.is_absolute());
        reject_reparse_components(&root).expect("assembled absolute root is available");

        let traversing = root.join("..");
        let error =
            reject_reparse_components(&traversing).expect_err("parent traversal remains forbidden");
        assert_eq!(error.code, "WORKSPACE_PATH");
    }
}

fn is_reparse_or_symlink(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return true;
        }
    }
    false
}

fn validate_directory_security(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(path)?;
        let mode = metadata.mode();
        // A configured root must be owned by this host user and not writable
        // by another local principal. A sticky shared ancestor such as /tmp is
        // allowed because it cannot rename the operator-owned root entry.
        let uid = unsafe { libc::geteuid() };
        if metadata.uid() != uid || mode & 0o022 != 0 {
            return Err(Error::new(
                "WORKSPACE_ACL",
                "workspace root must be user-owned and not group/world writable",
            ));
        }
        for ancestor in path.ancestors().skip(1) {
            let ancestor = fs::symlink_metadata(ancestor)?;
            if ancestor.mode() & 0o022 != 0 && ancestor.mode() & 0o1000 == 0 {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "workspace root has a writable non-sticky ancestor",
                ));
            }
        }
    }
    #[cfg(windows)]
    validate_windows_directory_acl(path)?;
    Ok(())
}

#[cfg(windows)]
fn validate_windows_directory_acl(path: &Path) -> Result<()> {
    // New lease directories receive a protected current-user DACL. For the
    // configured root, require the current user as owner and reject writable
    // allow ACEs for any ordinary local principal; operator-owned ACLs are
    // inspected but never rewritten.
    use std::{mem::size_of, os::windows::ffi::OsStrExt, ptr};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, LocalFree},
        Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT},
        Security::{
            ACCESS_ALLOWED_ACE, ACL_SIZE_INFORMATION, AclSizeInformation, CreateWellKnownSid,
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
            GetSecurityDescriptorDacl, GetTokenInformation, OWNER_SECURITY_INFORMATION,
            PSECURITY_DESCRIPTOR, PSID, TOKEN_QUERY, TOKEN_USER, TokenUser,
            WinBuiltinAdministratorsSid, WinLocalSystemSid,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut owner: PSID = ptr::null_mut();
    let mut dacl = ptr::null_mut();
    let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();
    // SAFETY: the path is NUL-terminated and every output pointer is initialized.
    let status = unsafe {
        GetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION,
            &mut owner,
            ptr::null_mut(),
            &mut dacl,
            ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 || owner.is_null() || descriptor.is_null() {
        if !descriptor.is_null() {
            unsafe { LocalFree(descriptor.cast()) };
        }
        return Err(Error::new(
            "WORKSPACE_ACL",
            "workspace root owner or DACL could not be read",
        ));
    }
    let result = (|| {
        let mut token = ptr::null_mut();
        // SAFETY: process handle is pseudo-handle; token output is initialized.
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(Error::new(
                "WORKSPACE_ACL",
                "current user token is unavailable",
            ));
        }
        let mut required = 0u32;
        // The first query intentionally obtains the required bounded buffer size.
        unsafe {
            GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut required);
        }
        let token_result = (|| {
            if required == 0 || required > 64 * 1024 {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "current user SID is unavailable",
                ));
            }
            let mut token_buffer = vec![0usize; (required as usize).div_ceil(size_of::<usize>())];
            // SAFETY: the aligned buffer has the size requested by the OS.
            if unsafe {
                GetTokenInformation(
                    token,
                    TokenUser,
                    token_buffer.as_mut_ptr().cast(),
                    required,
                    &mut required,
                )
            } == 0
            {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "current user SID could not be read",
                ));
            }
            // SAFETY: TokenUser populated a TOKEN_USER in the aligned buffer.
            let current_sid = unsafe { (*token_buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
            if current_sid.is_null() || unsafe { EqualSid(owner, current_sid) } == 0 {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "workspace root must be owned by the current user",
                ));
            }
            let mut present = 0;
            let mut defaulted = 0;
            let mut acl = ptr::null_mut();
            // SAFETY: descriptor was returned by GetNamedSecurityInfoW.
            if unsafe {
                GetSecurityDescriptorDacl(descriptor, &mut present, &mut acl, &mut defaulted)
            } == 0
                || present == 0
                || acl.is_null()
            {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "workspace root requires an explicit non-null DACL",
                ));
            }
            let mut size_info = ACL_SIZE_INFORMATION::default();
            // SAFETY: ACL comes from the validated descriptor and output is initialized.
            if unsafe {
                GetAclInformation(
                    acl,
                    (&mut size_info as *mut ACL_SIZE_INFORMATION).cast(),
                    size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            } == 0
            {
                return Err(Error::new(
                    "WORKSPACE_ACL",
                    "workspace DACL could not be inspected",
                ));
            }
            let mut trusted_sid_storage = Vec::new();
            for sid_type in [WinLocalSystemSid, WinBuiltinAdministratorsSid] {
                let mut storage = vec![0usize; 16];
                let mut sid_bytes = (storage.len() * size_of::<usize>()) as u32;
                // SAFETY: aligned output storage is large enough for a well-known SID.
                if unsafe {
                    CreateWellKnownSid(
                        sid_type,
                        ptr::null_mut(),
                        storage.as_mut_ptr().cast(),
                        &mut sid_bytes,
                    )
                } == 0
                {
                    return Err(Error::new(
                        "WORKSPACE_ACL",
                        "well-known SID could not be built",
                    ));
                }
                trusted_sid_storage.push(storage);
            }
            let trusted_sids = std::iter::once(current_sid)
                .chain(
                    trusted_sid_storage
                        .iter_mut()
                        .map(|storage| storage.as_mut_ptr().cast()),
                )
                .collect::<Vec<PSID>>();
            let write_mask = 0x0000_0002u32 // FILE_ADD_FILE / FILE_WRITE_DATA
                | 0x0000_0004u32 // FILE_ADD_SUBDIRECTORY
                | 0x0000_0040u32 // FILE_DELETE_CHILD
                | 0x0001_0000u32 // DELETE
                | 0x0004_0000u32 // WRITE_DAC
                | 0x0008_0000u32 // WRITE_OWNER
                | 0x4000_0000u32 // GENERIC_WRITE
                | 0x1000_0000u32; // GENERIC_ALL
            for index in 0..size_info.AceCount {
                let mut raw_ace = ptr::null_mut();
                // SAFETY: index is within the ACE count reported by GetAclInformation.
                if unsafe { GetAce(acl, index, &mut raw_ace) } == 0 || raw_ace.is_null() {
                    return Err(Error::new(
                        "WORKSPACE_ACL",
                        "workspace DACL ACE is unreadable",
                    ));
                }
                // ACEs 0, 5, 9 and 11 are standard/object/callback allow ACEs.
                let ace_type = unsafe { *(raw_ace.cast::<u8>()) };
                if !matches!(ace_type, 0 | 5 | 9 | 11) {
                    continue;
                }
                let ace_size = unsafe { *((raw_ace.cast::<u8>().add(2)).cast::<u16>()) };
                if usize::from(ace_size) < size_of::<ACCESS_ALLOWED_ACE>() {
                    return Err(Error::new(
                        "WORKSPACE_ACL",
                        "workspace allow ACE is malformed",
                    ));
                }
                // All supported ACCESS_ALLOWED_*_ACE layouts place Mask after
                // ACE_HEADER and SidStart last; Win32 validates the ACE bounds.
                let mask = unsafe { *((raw_ace.cast::<u8>().add(4)).cast::<u32>()) };
                let sid_offset = match ace_type {
                    0 | 9 => 8,
                    // Object ACEs add Flags and up to two GUIDs before SidStart.
                    5 | 11 => {
                        let flags = unsafe { *((raw_ace.cast::<u8>().add(8)).cast::<u32>()) };
                        12 + if flags & 1 != 0 { 16 } else { 0 }
                            + if flags & 2 != 0 { 16 } else { 0 }
                    }
                    _ => unreachable!(),
                };
                if mask & write_mask == 0 {
                    continue;
                }
                if sid_offset + 8 > usize::from(ace_size) {
                    return Err(Error::new(
                        "WORKSPACE_ACL",
                        "workspace allow ACE SID is truncated",
                    ));
                }
                let ace_sid = unsafe { raw_ace.cast::<u8>().add(sid_offset).cast() };
                let trusted = trusted_sids
                    .iter()
                    .any(|trusted| unsafe { EqualSid(ace_sid, *trusted) } != 0);
                if !trusted {
                    return Err(Error::new(
                        "WORKSPACE_ACL",
                        "workspace root grants write access to another local principal",
                    ));
                }
            }
            Ok(())
        })();
        unsafe { CloseHandle(token) };
        token_result
    })();
    // SAFETY: descriptor came from GetNamedSecurityInfoW and is freed once.
    unsafe { LocalFree(descriptor.cast()) };
    result
}

fn validate_project_id(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(workspace_config_error("invalid workspace project ID"));
    }
    Ok(())
}

fn validate_bounded_identifier(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(Error::invalid(format!("invalid {field}")));
    }
    Ok(())
}

fn validate_sha256_claim(value: &str, field: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(Error::invalid(format!(
            "{field} must be sha256:<64 lowercase hex>"
        )));
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid(format!(
            "{field} must be sha256:<64 lowercase hex>"
        )));
    }
    Ok(())
}

fn validate_allowed_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_SCOPE_TEXT_BYTES
        || value.contains(['\\', ':', '\0'])
        || value.chars().any(char::is_control)
        || value.starts_with('/')
        || value.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.eq_ignore_ascii_case(".git")
                || part.ends_with(['.', ' '])
        })
    {
        return Err(Error::invalid(
            "allowed_paths must be literal safe repository-relative paths",
        ));
    }
    Ok(())
}

fn workspace_config_error(message: &str) -> Error {
    Error::new("WORKSPACE_CONFIG", message)
}
