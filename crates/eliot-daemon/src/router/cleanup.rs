//! Bounded artifact cleanup through the authenticated Router and Store writer.
use super::*;
#[cfg(windows)]
use crate::platform::script_file::CleanupTree;
use crate::platform::script_file::directory_identity;
use crate::workspace_git::containment::{
    BranchClassification, CleanupFetches, ContainmentScope, branch_rev_parse, classify_branch,
};
use eliot_core::FailureOrigin;
use eliot_core::api::*;
use std::path::{Component, Path};
use tokio::time::timeout;

mod delete;
mod projects;

const CLEANUP_BACKFILL_BATCH: usize = 8;
const CLEANUP_BACKFILL_LIMIT: usize = 1_024;
const CLEANUP_BACKFILL_BUDGET: Duration = Duration::from_secs(8);
const CLEANUP_PLAN_MAX_PREFLIGHT: Duration = Duration::from_secs(15);

#[derive(Default)]
pub(super) struct AutomaticCleanupState {
    kept_reasons: HashMap<String, String>,
    first_sweep_complete: bool,
    active_sweep: Option<AutomaticCleanupProgress>,
    last_sweep: Option<eliot_core::api::AutomaticCleanupSweep>,
}

#[derive(Default)]
struct AutomaticCleanupProgress {
    summary: eliot_core::api::AutomaticCleanupSweep,
}
const SWARM_PLUGIN_INSTANCE_PURPOSE: &str = "Swarm plugin instance directory";
const SWARM_ARTIFACT_ROOT_PURPOSE: &str = "Swarm artifact root";

fn validate_cleanup_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.components().any(|p| match p {
            Component::ParentDir | Component::CurDir => true,
            Component::Normal(name) => name.to_str().is_none_or(|n| {
                n.contains(':')
                    || n.ends_with(['.', ' '])
                    || crate::platform::script_file::protected_cleanup_name(n)
            }),
            _ => false,
        })
    {
        return Err(Error::new(
            "not_allowed",
            "artifact path is not an absolute safe local directory",
        ));
    }
    Ok(())
}

fn require_cleanup_outside_native_home(canonical: &Path) -> Result<()> {
    for env in [
        "USERPROFILE",
        "HOME",
        "CODEX_HOME",
        "CODEX_SQLITE_HOME",
        "CLAUDE_CONFIG_DIR",
    ] {
        if let Some(home) = std::env::var_os(env).and_then(|p| std::fs::canonicalize(p).ok())
            && (canonical == home
                || home.starts_with(canonical)
                || (matches!(
                    env,
                    "CODEX_HOME" | "CODEX_SQLITE_HOME" | "CLAUDE_CONFIG_DIR"
                ) && canonical.starts_with(home)))
        {
            return Err(Error::new(
                "not_allowed",
                "user or native configuration root is protected",
            ));
        }
    }
    Ok(())
}

fn encoded(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "cleanup response cannot encode"))
}

fn check_worktree_artifact(
    artifact: &ArtifactView,
    workspace: &eliot_core::WorkspacePreparation,
) -> Result<()> {
    let git = artifact
        .git
        .as_ref()
        .ok_or_else(|| Error::new("source_integrity", "worktree Git binding absent"))?;
    if workspace.target_generation != artifact.generation
        || workspace.worktree_path != artifact.path
        || workspace.worktree_path != git.worktree
        || workspace.repo_path != git.repo
        || workspace.repo_common_dir != git.common
        || workspace.requested_branch != git.branch
        || directory_identity(Path::new(&artifact.path))? != artifact.identity
        || directory_identity(Path::new(&git.admin))? != git.admin_identity
    {
        return Err(Error::new("identity_changed", "worktree binding changed"));
    }
    Ok(())
}

pub(super) fn private_chat_root(path: &Path) -> Result<PathBuf> {
    #[cfg(windows)]
    crate::platform::windows::ensure_private_root(path)?;
    #[cfg(unix)]
    crate::platform::unix::ensure_private_root(path)?;
    std::fs::canonicalize(path)
        .map_err(|_| Error::new("unavailable", "private chat root unavailable"))
}

/// A failure of Swarm's own preparation before any native effect (the chat
/// directory, the Agent artifact root): origin `swarm` at its exact `where`,
/// unless the producer already classified it (for example an environment
/// failure of the platform layer).
pub(super) fn swarm_preparation_failure(mut error: Error, where_: &str, evidence: &str) -> Error {
    if error.failure_where.is_none() {
        error = error.with_where(where_);
    }
    if error.origin.is_none() {
        error = error.with_origin(FailureOrigin::Swarm, evidence);
    }
    error
}

impl Router {
    pub(super) fn automatic_cleanup_view(&self) -> eliot_core::api::AutomaticCleanupView {
        let (policy, enabled, after, config_error) =
            match crate::config::load_automation_config(&self.inner.data_root) {
                Ok(config) => {
                    let policy = match config.auto_cleanup {
                        Some(true) => AutomaticCleanupPolicy::Enabled,
                        Some(false) => AutomaticCleanupPolicy::Disabled,
                        None => AutomaticCleanupPolicy::PlanOnly,
                    };
                    (
                        policy,
                        config.auto_cleanup == Some(true),
                        Some(config.auto_cleanup_after),
                        None,
                    )
                }
                Err(error) => (
                    AutomaticCleanupPolicy::Disabled,
                    false,
                    None,
                    Some(error.code),
                ),
            };
        let state = self.inner.automatic_cleanup.lock_recover();
        eliot_core::api::AutomaticCleanupView {
            policy,
            enabled,
            startup_plan_only_pending: !state.first_sweep_complete,
            after,
            config_error,
            last_sweep: state.last_sweep.clone(),
        }
    }

    fn automatic_cleanup_failed(&self, error: &Error) {
        let policy = self.secret_policy_snapshot();
        crate::logging::record(&policy, "daemon.auto_cleanup_failed", &[], Some(error));
        self.inner.automatic_cleanup.lock_recover().last_sweep =
            Some(eliot_core::api::AutomaticCleanupSweep {
                finished_at: now_ms(),
                error_code: Some(error.code.clone()),
                ..Default::default()
            });
    }

    async fn record_automatic_cleanup(
        &self,
        plan: &CleanupPlan,
        applied: Option<&CleanupApplyResponse>,
        enabled: bool,
        plan_only: bool,
    ) -> Result<AutomaticCleanupSweep> {
        let now = now_ms();
        let mut sweep = eliot_core::api::AutomaticCleanupSweep {
            finished_at: now,
            enabled,
            plan_only,
            ..Default::default()
        };
        let policy = self.secret_policy_snapshot();
        let mut changed = Vec::new();
        let mut keep_updates = Vec::new();
        {
            let state = self.inner.automatic_cleanup.lock_recover();
            for item in &plan.items {
                if matches!(
                    item.artifact.kind,
                    ArtifactKind::Branch | ArtifactKind::NativeSession
                ) {
                    continue;
                }
                let settled = applied.and_then(|response| {
                    response
                        .items
                        .iter()
                        .find(|outcome| outcome.artifact_id == item.artifact.id)
                });
                if item.verdict == "safe" {
                    sweep.would_remove += 1;
                }
                let (outcome, reason, bytes) = if item.verdict != "safe" {
                    ("kept", item.reason.as_str(), 0)
                } else if plan_only {
                    ("would_remove", item.reason.as_str(), 0)
                } else if let Some(settled) = settled {
                    match settled.outcome.as_str() {
                        "removed" => ("removed", item.reason.as_str(), settled.bytes_reclaimed),
                        "skipped" => ("kept", settled.reason.as_deref().unwrap_or("skipped"), 0),
                        _ => ("unknown", settled.reason.as_deref().unwrap_or("unknown"), 0),
                    }
                } else {
                    ("unknown", "missing_cleanup_outcome", 0)
                };
                match outcome {
                    "would_remove" => {}
                    "removed" => sweep.removed += 1,
                    "kept" => {
                        sweep.kept += 1;
                        if state
                            .kept_reasons
                            .get(&item.artifact.id)
                            .is_some_and(|prior| prior == reason)
                        {
                            continue;
                        }
                    }
                    _ => sweep.unknown += 1,
                }
                keep_updates.push((
                    item.artifact.id.clone(),
                    (outcome == "kept").then(|| reason.to_owned()),
                ));
                changed.push(json!({
                    "artifact_id": item.artifact.id,
                    "agent": item.artifact.agent,
                    "generation": item.artifact.generation,
                    "kind": item.artifact.kind,
                    "path": policy.redact_bounded(&item.artifact.path, 384),
                    "outcome": outcome,
                    "reason": reason,
                    "bytes_reclaimed": bytes,
                }));
            }
        }
        if !changed.is_empty() {
            let mut store = self.inner.store.lock().await;
            let event = store.record_automatic_cleanup(
                json!({"plan_id": plan.plan_id, "items": changed}),
                now,
            )?;
            self.publish_locked(&[event]);
        }
        let mut state = self.inner.automatic_cleanup.lock_recover();
        for (id, reason) in keep_updates {
            if let Some(reason) = reason {
                // Keep only one reason per retained artifact, with a bound
                // matching the size of the artifact ledger.
                if state.kept_reasons.len() >= 4096 && !state.kept_reasons.contains_key(&id) {
                    state.kept_reasons.clear();
                }
                state.kept_reasons.insert(id, reason);
            } else {
                state.kept_reasons.remove(&id);
            }
        }
        Ok(sweep)
    }

    fn accumulate_automatic_cleanup_page(
        &self,
        page: AutomaticCleanupSweep,
        first_page: bool,
        complete: bool,
    ) -> Option<AutomaticCleanupSweep> {
        let mut state = self.inner.automatic_cleanup.lock_recover();
        if first_page {
            state.active_sweep = Some(AutomaticCleanupProgress {
                summary: AutomaticCleanupSweep {
                    plan_only: true,
                    ..Default::default()
                },
            });
        }
        let progress = state
            .active_sweep
            .get_or_insert_with(|| AutomaticCleanupProgress {
                summary: AutomaticCleanupSweep {
                    plan_only: true,
                    ..Default::default()
                },
            });
        progress.summary.finished_at = page.finished_at;
        progress.summary.enabled = page.enabled;
        progress.summary.plan_only &= page.plan_only;
        progress.summary.would_remove += page.would_remove;
        progress.summary.removed += page.removed;
        progress.summary.kept += page.kept;
        progress.summary.unknown += page.unknown;
        if !complete {
            return None;
        }
        let summary = progress.summary.clone();
        state.active_sweep = None;
        state.last_sweep = Some(summary.clone());
        Some(summary)
    }

    async fn notify_automatic_cleanup_sweep(&self, sweep: &AutomaticCleanupSweep) -> Result<()> {
        let enablement = match (sweep.enabled, sweep.plan_only) {
            (true, true) => "Automatic cleanup is enabled. This first sweep was plan-only.",
            (true, false) => "Automatic cleanup is enabled.",
            (false, _) => "Set automation.auto_cleanup = true to apply.",
        };
        let message = format!(
            "{} worktrees/artifacts would be removed, {} kept. Removed {}, unknown {}. {enablement}",
            sweep.would_remove, sweep.kept, sweep.removed, sweep.unknown
        );
        let id = ulid::Ulid::new().to_string();
        let mut progress = self.progress_receiver();
        let request = json!({
            "id": id,
            "verb": "notify",
            "args": {
                "title": "Automatic cleanup sweep",
                "message": message,
            },
        });
        self.submit(&request, &Principal::Owner, "daemon.archive")
            .await?;
        timeout(Duration::from_secs(10), async {
            loop {
                let state = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_command(&id)?
                    .map(|c| c.state);
                match state {
                    Some(CommandState::Applied) => return Ok(()),
                    Some(state) if state.is_terminal() => {
                        return Err(Error::new(
                            "cleanup_notice_failed",
                            "Owner cleanup notice did not apply",
                        ));
                    }
                    _ => {}
                }
                progress.changed().await.map_err(|_| {
                    Error::new(
                        "unavailable",
                        "Owner cleanup notice settlement is unavailable",
                    )
                })?;
            }
        })
        .await
        .map_err(|_| Error::new("unknown", "Owner cleanup notice settlement timed out"))?
    }

    pub(super) async fn prepare_chat_directory(&self, command: &Command) -> Result<()> {
        self.prepare_chat_directory_once(command)
            .await
            .map_err(|error| {
                swarm_preparation_failure(
                    error,
                    "router.dispatch.prepare_chat",
                    "Swarm's chat directory preparation refused the launch before dispatch",
                )
            })
    }

    async fn prepare_chat_directory_once(&self, command: &Command) -> Result<()> {
        let store = self.inner.store.lock().await;
        let agent = store
            .get_agent(
                command
                    .target
                    .as_deref()
                    .ok_or_else(|| Error::new("invalid", "chat Agent absent"))?,
            )?
            .ok_or_else(|| Error::new("not_found", "chat Agent absent"))?;
        if !agent.chat {
            return Err(Error::new("source_integrity", "chat binding absent"));
        }
        let path = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("source_integrity", "chat workspace absent"))?;
        if let Some(known) = store.known_artifact_path(path)? {
            if known.agent != agent.id
                || known.identity != directory_identity(Path::new(path))?
                || known.purpose != "Swarm chat directory"
            {
                return Err(Error::new(
                    "identity_changed",
                    "chat directory ownership changed",
                ));
            }
            return Ok(());
        }
        drop(store);
        let path = Path::new(path);
        let parent = path
            .parent()
            .ok_or_else(|| Error::new("invalid", "chat parent absent"))?;
        let parent = crate::platform::script_file::Directory::open(parent)?;
        #[cfg(windows)]
        if !crate::platform::windows::create_private_directory(path)? {
            return Err(Error::new("conflict", "chat directory already exists"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(path)
                .map_err(|_| {
                    Error::new(
                        "conflict",
                        "chat directory could not be exclusively created",
                    )
                })?;
        }
        let directory = crate::platform::script_file::Directory::open(&private_chat_root(path)?)?;
        drop(parent);
        self.inner
            .store
            .lock()
            .await
            .record_known_artifact(ArtifactView {
                id: ulid::Ulid::new().to_string(),
                agent: agent.id,
                generation: agent.generation,
                project: String::new(),
                tree: Vec::new(),
                path: directory.path.to_string_lossy().into_owned(),
                kind: ArtifactKind::Other,
                purpose: "Swarm chat directory".to_owned(),
                identity: directory.identity()?,
                created_at: now_ms(),
                state: "registered".to_owned(),
                git: None,
            })
    }

    pub(super) async fn prepare_agent_artifact_root(&self, agent: &Agent) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        if let Some(root) = store.agent_artifact_root(&agent.id, agent.generation)? {
            if directory_identity(Path::new(&root.path))? != root.identity {
                return Err(Error::new(
                    "identity_changed",
                    "Agent artifact root changed",
                ));
            }
            return Ok(());
        }
        let config = crate::config::load_daemon_config(&self.inner.data_root)?;
        let parent = match config.artifact_roots.first() {
            Some(root) => std::fs::canonicalize(root)
                .map_err(|_| Error::new("unavailable", "artifact root unavailable"))?,
            None => {
                let root = self.inner.data_root.join("artifacts");
                std::fs::create_dir_all(&root)
                    .map_err(|_| Error::new("unavailable", "artifact parent unavailable"))?;
                std::fs::canonicalize(root)
                    .map_err(|_| Error::new("unavailable", "artifact parent unavailable"))?
            }
        };
        let parent = crate::platform::script_file::Directory::open(&parent)?;
        let directory = parent.create_directory(&format!("{}-{}", agent.id, agent.generation))?;
        let root = ArtifactView {
            id: ulid::Ulid::new().to_string(),
            agent: agent.id.clone(),
            generation: agent.generation,
            project: agent.project.clone(),
            tree: Vec::new(),
            path: directory.path.to_string_lossy().into_owned(),
            kind: ArtifactKind::Other,
            purpose: "Swarm artifact root".to_owned(),
            identity: directory.identity()?,
            created_at: now_ms(),
            state: "ownership_root".to_owned(),
            git: None,
        };
        store.record_known_artifact(root.clone())?;
        if store
            .agent_artifact_root(&agent.id, agent.generation)?
            .as_ref()
            .map(|row| (&row.path, &row.identity))
            != Some((&root.path, &root.identity))
        {
            return Err(Error::new(
                "conflict",
                "artifact root custody could not be committed",
            ));
        }
        Ok(())
    }

    pub(super) async fn record_agent_artifacts(&self, agent: &Agent) -> Result<()> {
        let workspace = self.inner.store.lock().await.get_workspace(&agent.id)?;
        let mut known = Vec::new();
        if let Some(workspace) = workspace
            && workspace.target_generation == agent.generation
            && (workspace.phase == WorkspacePhase::Ready
                || workspace.phase == WorkspacePhase::Failed
                    && std::fs::symlink_metadata(&workspace.worktree_path).is_ok())
        {
            let probe = crate::git_monitor::GitProbe::new()?;
            let admin = probe
                .workspace_command(
                    Path::new(&workspace.worktree_path),
                    &["rev-parse", "--path-format=absolute", "--git-dir"],
                    Instant::now() + Duration::from_secs(5),
                )
                .await?;
            if admin.code != 0 {
                return Err(Error::new(
                    "unavailable",
                    "worktree administration path unavailable",
                ));
            }
            let admin = std::str::from_utf8(&admin.stdout)
                .map_err(|_| Error::new("invalid", "worktree administration path is not Unicode"))?
                .trim_end_matches(['\r', '\n']);
            let admin = std::fs::canonicalize(admin).map_err(|_| {
                Error::new("unavailable", "worktree administration path unavailable")
            })?;
            let common = std::fs::canonicalize(&workspace.repo_common_dir)
                .map_err(|_| Error::new("unavailable", "worktree common directory unavailable"))?;
            if admin.parent() != Some(common.join("worktrees").as_path()) {
                return Err(Error::new(
                    "not_allowed",
                    "worktree administration escaped common directory",
                ));
            }
            let git = CleanupGitBinding {
                repo: workspace.repo_path,
                common: common.to_string_lossy().into_owned(),
                branch: workspace.requested_branch,
                worktree: workspace.worktree_path.clone(),
                admin: admin.to_string_lossy().into_owned(),
                admin_identity: directory_identity(&admin)?,
                planned_tip: None,
            };
            known.push(ArtifactView {
                id: ulid::Ulid::new().to_string(),
                agent: agent.id.clone(),
                generation: agent.generation,
                project: agent.project.clone(),
                tree: Vec::new(),
                path: workspace.worktree_path.clone(),
                kind: ArtifactKind::Worktree,
                purpose: "Swarm managed worktree".to_owned(),
                identity: directory_identity(Path::new(&workspace.worktree_path))?,
                created_at: agent.created_at,
                state: "registered".to_owned(),
                git: Some(git.clone()),
            });
            let branch_ref = format!("refs/heads/{}", git.branch);
            let oid = probe
                .workspace_command(
                    Path::new(&git.repo),
                    &["rev-parse", "--verify", &branch_ref],
                    Instant::now() + Duration::from_secs(5),
                )
                .await?;
            if oid.code == 0 {
                known.push(ArtifactView {
                    id: ulid::Ulid::new().to_string(),
                    agent: agent.id.clone(),
                    generation: agent.generation,
                    project: agent.project.clone(),
                    tree: Vec::new(),
                    path: format!("{}::{branch_ref}", git.common),
                    kind: ArtifactKind::Branch,
                    purpose: "Swarm managed branch".to_owned(),
                    identity: String::from_utf8_lossy(&oid.stdout).trim().to_owned(),
                    created_at: agent.created_at,
                    state: "registered".to_owned(),
                    git: Some(git),
                });
            }
        } else if let Some(root) = &agent.workspace
            && self
                .inner
                .store
                .lock()
                .await
                .artifact_workspace_exclusive(root)?
        {
            for relative in agent
                .profile_snapshot
                .get("scratch")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                let path = Path::new(root).join(relative);
                if let Ok(identity) = directory_identity(&path)
                    && let Ok(path) = self
                        .checked_artifact_path(agent, &path.to_string_lossy())
                        .await
                {
                    known.push(ArtifactView {
                        id: ulid::Ulid::new().to_string(),
                        agent: agent.id.clone(),
                        generation: agent.generation,
                        project: agent.project.clone(),
                        tree: Vec::new(),
                        path: path.to_string_lossy().into_owned(),
                        kind: ArtifactKind::Build,
                        purpose: "frozen profile scratch".to_owned(),
                        identity,
                        created_at: agent.created_at,
                        state: "registered".to_owned(),
                        git: None,
                    });
                }
            }
        }
        // Exactly the Agent's frozen CARGO_TARGET_DIR, and only when it lies
        // outside the agent workspace: inside, worktree deletion already
        // covers it. Admission still requires a configured artifact root or
        // the agent's own temp root; otherwise the value stays frozen but
        // unrecorded, never force-deleted.
        if let Some(target) = agent
            .profile_snapshot
            .get("target_dir")
            .and_then(Value::as_str)
        {
            let target_path = Path::new(target);
            let outside = match &agent.workspace {
                Some(workspace) => match (
                    std::fs::canonicalize(workspace),
                    std::fs::canonicalize(target_path),
                ) {
                    (Ok(root), Ok(target)) => target != root && !target.starts_with(&root),
                    _ => false,
                },
                None => true,
            };
            if outside
                && let Ok(identity) = directory_identity(target_path)
                && let Ok(path) = self
                    .checked_artifact_path(agent, &target_path.to_string_lossy())
                    .await
            {
                known.push(ArtifactView {
                    id: ulid::Ulid::new().to_string(),
                    agent: agent.id.clone(),
                    generation: agent.generation,
                    project: agent.project.clone(),
                    tree: Vec::new(),
                    path: path.to_string_lossy().into_owned(),
                    kind: ArtifactKind::Build,
                    purpose: "frozen CARGO_TARGET_DIR".to_owned(),
                    identity,
                    created_at: agent.created_at,
                    state: "registered".to_owned(),
                    git: None,
                });
            }
        }
        if let Some(session) = native_session_artifact(agent) {
            known.push(session);
        }
        // Branches outlive their worktree: re-resolve every retained tip
        // through its own binding so the ledger tracks the live branch even
        // after cleanup removed the checkout apply verifies against.
        let retained = self
            .inner
            .store
            .lock()
            .await
            .registered_branches(&agent.id, agent.generation)?;
        for row in retained {
            let Some(git) = row.git.clone() else { continue };
            let probe = crate::git_monitor::GitProbe::new()?;
            let Ok(tip) = probe
                .workspace_command(
                    Path::new(&git.common),
                    &[
                        "rev-parse",
                        "--verify",
                        &format!("refs/heads/{}", git.branch),
                    ],
                    Instant::now() + Duration::from_secs(5),
                )
                .await
            else {
                continue;
            };
            if tip.code != 0 {
                continue;
            }
            let tip = String::from_utf8_lossy(&tip.stdout).trim().to_owned();
            if tip.is_empty() || tip == row.identity {
                continue;
            }
            let mut fresh = row.clone();
            fresh.id = ulid::Ulid::new().to_string();
            fresh.identity = tip;
            known.push(fresh);
        }
        let mut store = self.inner.store.lock().await;
        for artifact in known {
            store.record_known_artifact(artifact)?;
        }
        Ok(())
    }
    async fn checked_artifact_path(&self, agent: &Agent, path: &str) -> Result<PathBuf> {
        let path = Path::new(path);
        validate_cleanup_path(path)?;
        directory_identity(path)?;
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| Error::new("not_allowed", "artifact directory unavailable"))?;
        let daemon_root = std::fs::canonicalize(&self.inner.data_root)
            .map_err(|_| Error::new("unavailable", "daemon root unavailable"))?;
        let owned = self
            .inner
            .store
            .lock()
            .await
            .agent_artifact_root(&agent.id, agent.generation)?
            .ok_or_else(|| Error::new("not_allowed", "Agent has no Swarm-created artifact root"))?;
        let own_temp = std::fs::canonicalize(&owned.path)
            .map_err(|_| Error::new("not_allowed", "Agent artifact root unavailable"))?;
        if directory_identity(&own_temp)? != owned.identity {
            return Err(Error::new(
                "identity_changed",
                "Agent artifact root changed",
            ));
        }
        if (canonical == daemon_root || daemon_root.starts_with(&canonical))
            || (canonical.starts_with(&daemon_root) && !canonical.starts_with(&own_temp))
        {
            return Err(Error::new(
                "not_allowed",
                "daemon data and configuration are protected",
            ));
        }
        require_cleanup_outside_native_home(&canonical)?;
        let chat_directory = if agent.chat {
            let identity = directory_identity(&canonical)?;
            if let Some(workspace) = agent.workspace.as_deref() {
                if directory_identity(Path::new(workspace)).ok().as_ref() == Some(&identity) {
                    self.inner
                        .store
                        .lock()
                        .await
                        .known_artifact_path(workspace)?
                        .is_some_and(|artifact| {
                            artifact.agent == agent.id
                                && artifact.generation == agent.generation
                                && artifact.purpose == "Swarm chat directory"
                                && artifact.identity == identity
                        })
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };
        if !chat_directory && (canonical == own_temp || !canonical.starts_with(&own_temp)) {
            return Err(Error::new(
                "not_allowed",
                "artifact is outside the Agent generation's Swarm-created root",
            ));
        }
        Ok(canonical)
    }

    fn selected_agent_artifact_root(&self, agent: &Agent) -> Result<PathBuf> {
        let config = crate::config::load_daemon_config(&self.inner.data_root)?;
        let parent = config
            .artifact_roots
            .first()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.inner.data_root.join("artifacts"));
        let parent = std::fs::canonicalize(parent)
            .map_err(|_| Error::new("not_allowed", "selected artifact root unavailable"))?;
        std::fs::canonicalize(parent.join(format!("{}-{}", agent.id, agent.generation)))
            .map_err(|_| Error::new("not_allowed", "Agent artifact root unavailable"))
    }

    fn current_managed_worktree_path(&self, path: &str) -> Result<PathBuf> {
        let data_root = std::fs::canonicalize(&self.inner.data_root)
            .map_err(|_| Error::new("unavailable", "daemon root unavailable"))?;
        let managed_root = std::fs::canonicalize(data_root.join("worktrees"))
            .map_err(|_| Error::new("not_allowed", "managed worktree root unavailable"))?;
        let path = std::fs::canonicalize(path)
            .map_err(|_| Error::new("not_allowed", "managed worktree unavailable"))?;
        if managed_root.parent() != Some(data_root.as_path())
            || path == managed_root
            || !path.starts_with(&managed_root)
        {
            return Err(Error::new(
                "not_allowed",
                "worktree is outside this daemon's managed root",
            ));
        }
        Ok(path)
    }

    async fn checked_cleanup_artifact_path(
        &self,
        agent: &Agent,
        artifact: &ArtifactView,
        allow_fenced_root: bool,
    ) -> Result<PathBuf> {
        if artifact.kind == ArtifactKind::Other
            && artifact.state == "ownership_root"
            && artifact.purpose == SWARM_ARTIFACT_ROOT_PURPOSE
        {
            return self
                .checked_owned_artifact_root(agent, artifact, allow_fenced_root)
                .await;
        }
        if artifact.kind == ArtifactKind::Other && artifact.purpose == SWARM_PLUGIN_INSTANCE_PURPOSE
        {
            return self.checked_plugin_instance_path(agent, artifact);
        }
        self.checked_artifact_path(agent, &artifact.path).await
    }

    async fn checked_owned_artifact_root(
        &self,
        agent: &Agent,
        artifact: &ArtifactView,
        allow_fenced_unknown: bool,
    ) -> Result<PathBuf> {
        if artifact.kind != ArtifactKind::Other
            || artifact.state != "ownership_root"
            || artifact.purpose != SWARM_ARTIFACT_ROOT_PURPOSE
            || artifact.agent != agent.id
            || artifact.generation != agent.generation
        {
            return Err(Error::new(
                "stale_generation",
                "owned artifact root binding changed",
            ));
        }
        let store = self.inner.store.lock().await;
        let current = store
            .known_artifact_path(&artifact.path)?
            .ok_or_else(|| Error::new("not_allowed", "Agent artifact root is not retained"))?;
        if current.id != artifact.id
            || current.agent != artifact.agent
            || current.generation != artifact.generation
            || current.path != artifact.path
            || current.identity != artifact.identity
            || current.kind != artifact.kind
            || current.purpose != artifact.purpose
            || !(current.state == "ownership_root"
                || (allow_fenced_unknown && current.state == "unknown"))
        {
            return Err(Error::new(
                "identity_changed",
                "owned artifact root readback changed",
            ));
        }
        if current.state == "ownership_root" {
            let owned = store
                .agent_artifact_root(&agent.id, agent.generation)?
                .ok_or_else(|| Error::new("not_allowed", "Agent artifact root is not owned"))?;
            if owned.id != artifact.id
                || owned.path != artifact.path
                || owned.identity != artifact.identity
            {
                return Err(Error::new(
                    "identity_changed",
                    "owned artifact root binding changed",
                ));
            }
        } else if !allow_fenced_unknown {
            return Err(Error::new(
                "not_allowed",
                "unknown artifact root is not eligible for cleanup",
            ));
        }
        drop(store);
        let source = Path::new(&artifact.path);
        validate_cleanup_path(source)?;
        directory_identity(source)?;
        let path = std::fs::canonicalize(source)
            .map_err(|_| Error::new("not_allowed", "Agent artifact root unavailable"))?;
        if path != self.selected_agent_artifact_root(agent)?
            || crate::platform::script_file::directory_identity(&path)? != artifact.identity
        {
            return Err(Error::new(
                "identity_changed",
                "owned artifact root is outside the selected current root",
            ));
        }
        require_cleanup_outside_native_home(&path)?;
        Ok(path)
    }

    fn checked_plugin_instance_path(
        &self,
        agent: &Agent,
        artifact: &ArtifactView,
    ) -> Result<PathBuf> {
        if artifact.agent != agent.id || artifact.generation != agent.generation {
            return Err(Error::new(
                "stale_generation",
                "plugin artifact generation changed",
            ));
        }
        let data_root = std::fs::canonicalize(&self.inner.data_root)
            .map_err(|_| Error::new("unavailable", "daemon root unavailable"))?;
        let plugin_root = std::fs::canonicalize(data_root.join("plugins"))
            .map_err(|_| Error::new("not_allowed", "plugin instance root unavailable"))?;
        if plugin_root.parent() != Some(data_root.as_path())
            || plugin_root.file_name().and_then(|name| name.to_str()) != Some("plugins")
        {
            return Err(Error::new(
                "not_allowed",
                "plugin instance root is outside this daemon data directory",
            ));
        }
        let path = std::fs::canonicalize(&artifact.path)
            .map_err(|_| Error::new("not_allowed", "plugin instance directory unavailable"))?;
        if path.parent() != Some(plugin_root.as_path()) {
            return Err(Error::new(
                "not_allowed",
                "plugin instance is outside this daemon's plugin root",
            ));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::new("not_allowed", "plugin instance name is invalid"))?;
        let prefix = format!("{}-g{}-", agent.id, agent.generation);
        let suffix = name
            .strip_prefix(&prefix)
            .ok_or_else(|| Error::new("not_allowed", "plugin instance generation is invalid"))?;
        let instance_id = ulid::Ulid::from_string(suffix)
            .map_err(|_| Error::new("not_allowed", "plugin instance identity is invalid"))?;
        if instance_id.to_string() != suffix
            || crate::platform::script_file::directory_identity(&path)? != artifact.identity
        {
            return Err(Error::new(
                "identity_changed",
                "plugin instance directory binding changed",
            ));
        }
        Ok(path)
    }

    async fn automatic_cleanup_path_is_managed(
        &self,
        agent: &Agent,
        artifact: &ArtifactView,
        allow_fenced_root: bool,
    ) -> Result<bool> {
        match artifact.kind {
            ArtifactKind::Worktree => {
                if artifact.agent != agent.id || artifact.generation != agent.generation {
                    return Ok(false);
                }
                self.current_managed_worktree_path(&artifact.path)
                    .map(|_| true)
            }
            ArtifactKind::Build
            | ArtifactKind::DataDir
            | ArtifactKind::Temp
            | ArtifactKind::Other => {
                if artifact.state == "ownership_root"
                    && artifact.purpose == SWARM_ARTIFACT_ROOT_PURPOSE
                {
                    return self
                        .checked_owned_artifact_root(agent, artifact, allow_fenced_root)
                        .await
                        .map(|_| true);
                }
                if artifact.purpose == SWARM_PLUGIN_INSTANCE_PURPOSE {
                    return self
                        .checked_plugin_instance_path(agent, artifact)
                        .map(|_| true);
                }
                if artifact.agent != agent.id || artifact.generation != agent.generation {
                    return Ok(false);
                }
                let selected_root = self.selected_agent_artifact_root(agent)?;
                let owned = self
                    .inner
                    .store
                    .lock()
                    .await
                    .agent_artifact_root(&agent.id, agent.generation)?;
                let Some(owned) = owned else {
                    return Ok(false);
                };
                let owned_path = std::fs::canonicalize(&owned.path)
                    .map_err(|_| Error::new("not_allowed", "Agent artifact root unavailable"))?;
                if owned_path != selected_root
                    || crate::platform::script_file::directory_identity(&owned_path)?
                        != owned.identity
                {
                    return Ok(false);
                }
                let path = self.checked_artifact_path(agent, &artifact.path).await?;
                Ok(path != selected_root && path.starts_with(&selected_root))
            }
            ArtifactKind::Branch
            | ArtifactKind::NativeSession
            | ArtifactKind::ProjectRegistration => Ok(false),
        }
    }

    async fn automatic_cleanup_worktree_keep_reason(
        &self,
        agent: &Agent,
        fetches: Option<&mut CleanupFetches>,
    ) -> Result<Option<String>> {
        let artifact = {
            let store = self.inner.store.lock().await;
            let Some(workspace) = store.get_workspace(&agent.id)? else {
                return Ok(agent
                    .workspace_phase
                    .is_some()
                    .then(|| "worktree_unverifiable".to_owned()));
            };
            if workspace.target_generation != agent.generation {
                return Ok(Some("worktree_generation_changed".to_owned()));
            }
            if workspace.phase == WorkspacePhase::Removed {
                return Ok(None);
            }
            let Some(artifact) = store.known_artifact_path(&workspace.worktree_path)? else {
                return Ok(Some("worktree_unverifiable".to_owned()));
            };
            if artifact.kind != ArtifactKind::Worktree
                || artifact.agent != agent.id
                || artifact.generation != agent.generation
                || artifact.path != workspace.worktree_path
                || artifact.state != "registered"
            {
                return Ok(Some("worktree_unverifiable".to_owned()));
            }
            artifact
        };
        if let Err(error) = self.current_managed_worktree_path(&artifact.path) {
            return Ok(Some(error.code));
        }
        match self.worktree_cleanup_preflight(&artifact, fetches).await {
            Ok(_) => Ok(None),
            Err(error) => Ok(Some(error.code)),
        }
    }

    async fn automatic_cleanup_keep_reason(
        &self,
        artifact: &ArtifactView,
        retention_ms: i64,
        now: i64,
        allow_fenced_root: bool,
        fetches: Option<&mut CleanupFetches>,
    ) -> Result<Option<String>> {
        let (agent, ended_at) = {
            let store = self.inner.store.lock().await;
            let Some(agent) = store.get_agent(&artifact.agent)? else {
                return Ok(Some("agent_missing".to_owned()));
            };
            if agent.generation != artifact.generation {
                return Ok(Some("stale_generation".to_owned()));
            }
            if agent.archived {
                return Ok(Some("archived".to_owned()));
            }
            if !agent.state.is_terminal() {
                return Ok(Some("agent_not_terminal".to_owned()));
            }
            let ended_at = store.terminal_agent_end_time(&agent)?;
            (agent, ended_at)
        };
        let cutoff = now.saturating_sub(retention_ms);
        if artifact.created_at >= cutoff || ended_at >= cutoff {
            return Ok(Some("retention_age".to_owned()));
        }
        if artifact.state == "ownership_root" {
            if artifact.kind != ArtifactKind::Other
                || artifact.purpose != SWARM_ARTIFACT_ROOT_PURPOSE
            {
                return Ok(Some("ownership_root_unverified".to_owned()));
            }
            if self
                .inner
                .store
                .lock()
                .await
                .cleanup_root_has_descendants(artifact)?
            {
                return Ok(Some("artifact_root_has_descendants".to_owned()));
            }
        }
        if matches!(
            artifact.kind,
            ArtifactKind::Branch | ArtifactKind::NativeSession
        ) {
            return Ok(Some("manual_only".to_owned()));
        }
        if artifact.kind != ArtifactKind::Worktree {
            match self
                .automatic_cleanup_worktree_keep_reason(&agent, fetches)
                .await
            {
                Ok(Some(reason)) => return Ok(Some(reason)),
                Err(error) => return Ok(Some(error.code)),
                Ok(None) => {}
            }
        }
        match self
            .automatic_cleanup_path_is_managed(&agent, artifact, allow_fenced_root)
            .await
        {
            Ok(true) => Ok(None),
            Ok(false) => Ok(Some("outside_managed_root".to_owned())),
            Err(error) => Ok(Some(error.code)),
        }
    }

    pub(super) async fn artifact_register(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ArtifactRegisterRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "artifact.register",
            ArtifactRegisterRequest::validate,
        )?;
        let name = principal_name(principal);
        let store = self.inner.store.lock().await;
        // Replay precedes filesystem reads, including after successful cleanup.
        if let Some(value) =
            store.cleanup_replay(&name, &request.id, "artifact.register", params)?
        {
            return Ok(value);
        }
        let Principal::Agent { id, generation } = principal else {
            return Err(Error::new("not_allowed", "only Agents register artifacts"));
        };
        let agent = store
            .get_agent(id)?
            .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
        if agent.generation != *generation {
            return Err(Error::new("stale_generation", "Agent generation changed"));
        }
        drop(store);
        let path = self.checked_artifact_path(&agent, &request.path).await?;
        let identity = directory_identity(&path)?;
        self.cleanup_check_git(&agent, &path).await?;
        let mut store = self.inner.store.lock().await;
        encoded(
            store.register_artifact(
                &name,
                &request,
                path.to_str()
                    .ok_or_else(|| Error::new("invalid", "artifact path is not Unicode"))?,
                &identity,
                now_ms(),
            )?,
        )
    }

    pub(super) async fn cleanup_plan(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        self.cleanup_plan_page(params, principal, None, false).await
    }

    async fn auto_cleanup_plan(
        &self,
        params: &Value,
        retention_ms: i64,
        retain_for_apply: bool,
    ) -> Result<Value> {
        self.cleanup_plan_page(
            params,
            &Principal::Owner,
            Some(retention_ms),
            retain_for_apply,
        )
        .await
    }

    async fn cleanup_plan_page(
        &self,
        params: &Value,
        principal: &Principal,
        auto_cleanup_after_ms: Option<i64>,
        retain_automatic_plan: bool,
    ) -> Result<Value> {
        let auto_cleanup = auto_cleanup_after_ms.is_some();
        let call_deadline = Instant::now() + Duration::from_secs(25);
        let request: CleanupPlanRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "cleanup.plan",
            CleanupPlanRequest::validate,
        )?;
        let name = principal_name(principal);
        let mut page = request
            .continuation
            .as_deref()
            .map(PlanContinuation::decode)
            .transpose()?
            .unwrap_or(PlanContinuation {
                artifact_after: None,
                safe_skipped: 0,
                kept_skipped: 0,
                regs_revision: None,
                snapshot_seq: None,
                safe_ids: None,
                kept_ids: None,
                backfill_after: None,
                inventory_rank: None,
                inventory_seq_after: None,
                request_binding: None,
            });
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: false,
            continuation: None,
        };
        let binding = cleanup_plan_request_binding(&name, &request)?;
        if page
            .request_binding
            .as_deref()
            .is_some_and(|saved| saved != binding.as_str())
        {
            return Err(Error::new("invalid", "cleanup continuation is stale"));
        }
        if !auto_cleanup && page.snapshot_seq.is_none() {
            // Backfill within one slice, then freeze the inventory on the
            // first candidate page. Large or slow fleets resume the backfill.
            page.backfill_after = self
                .backfill_cleanup_inventory(&name, &request, page.backfill_after)
                .await?;
            if page.backfill_after.is_some() {
                page.request_binding = Some(binding);
                plan.truncated = true;
                plan.continuation = Some(page.encode()?);
                return encoded(plan);
            }
        }
        let (safe_projects, kept_projects, regs_revision): (Vec<_>, Vec<_>, Option<String>) =
            if auto_cleanup {
                if page.safe_skipped != 0
                    || page.kept_skipped != 0
                    || page.regs_revision.is_some()
                    || page.safe_ids.is_some()
                    || page.kept_ids.is_some()
                {
                    return Err(Error::new("invalid", "cleanup continuation is stale"));
                }
                (Vec::new(), Vec::new(), None)
            } else if matches!(principal, Principal::Owner) {
                let (registrations, revision) = self.plan_project_registrations(&request).await?;
                let (safe, kept): (Vec<_>, Vec<_>) = registrations
                    .into_iter()
                    .partition(|item| item.verdict == "safe");
                if page.safe_skipped != 0
                    || page.kept_skipped != 0
                    || page.regs_revision.is_some()
                    || page.safe_ids.is_some()
                    || page.kept_ids.is_some()
                {
                    // Offsets index a project set by position: a changed
                    // settings revision restarts the whole listing instead
                    // of skipping into a different set.
                    if page.regs_revision != revision {
                        return Err(Error::new("invalid", "cleanup continuation is stale"));
                    }
                    // Verdicts move without touching the settings revision:
                    // a checkout appearing or disappearing reclassifies its
                    // project between kept and safe. The saved offsets would
                    // then skip or repeat a registration, so the ordered ids
                    // and verdict bands must still match exactly.
                    if !continuation_order_valid(&page, &revision, &safe, &kept) {
                        return Err(Error::new("invalid", "cleanup continuation is stale"));
                    }
                }
                (safe, kept, revision)
            } else {
                // The backfill cursor is not project state: Agents page the
                // fleet scan with it while carrying no registration offsets.
                if page.safe_skipped != 0
                    || page.kept_skipped != 0
                    || page.regs_revision.is_some()
                    || page.safe_ids.is_some()
                    || page.kept_ids.is_some()
                {
                    return Err(Error::new("invalid", "cleanup continuation is stale"));
                }
                (Vec::new(), Vec::new(), None)
            };
        // The token pins the full ordered traversal behind the offsets, so
        // a reclassified project is detected on the next page instead of
        // skipping or repeating a registration. Non-owners never carry
        // project state: any such token in their hands is stale.
        let (safe_ids, kept_ids): (Option<Vec<String>>, Option<Vec<String>>) =
            if matches!(principal, Principal::Owner) && !auto_cleanup {
                (
                    Some(project_ids(&safe_projects)),
                    Some(project_ids(&kept_projects)),
                )
            } else {
                (None, None)
            };
        let safe_total = safe_projects.len();
        let kept_total = kept_projects.len();
        let mut shown_safe = Vec::new();
        for item in safe_projects
            .into_iter()
            .skip(page.safe_skipped)
            .take(MAX_CLEANUP_ITEMS)
        {
            shown_safe.push(item);
        }
        let artifact_limit = MAX_CLEANUP_ITEMS - shown_safe.len();
        let eliot_store::CleanupInventoryPage {
            artifacts,
            truncated: artifacts_truncated,
            snapshot_seq,
            next_rank,
            next_seq,
        } = self.inner.store.lock().await.cleanup_inventory(
            &name,
            &request,
            &page,
            artifact_limit,
            auto_cleanup,
        )?;
        let artifacts_total = artifacts.len();
        let mut middle = Vec::new();
        let mut fetches = CleanupFetches::default();
        for artifact in artifacts {
            let mut candidate = CleanupCandidate {
                artifact,
                bytes: None,
                verdict: "keep".to_owned(),
                reason: "agent_not_quiescent".to_owned(),
            };
            let ownership_root = auto_cleanup
                && candidate.artifact.kind == ArtifactKind::Other
                && candidate.artifact.state == "ownership_root"
                && candidate.artifact.purpose == SWARM_ARTIFACT_ROOT_PURPOSE;
            if candidate.artifact.state != "registered" && !ownership_root {
                candidate.reason = candidate.artifact.state.clone();
                middle.push(candidate);
                continue;
            }
            if auto_cleanup
                && call_deadline.saturating_duration_since(Instant::now())
                    < CLEANUP_PLAN_MAX_PREFLIGHT
            {
                break;
            }
            if let Some(retention_ms) = auto_cleanup_after_ms {
                match self
                    .automatic_cleanup_keep_reason(
                        &candidate.artifact,
                        retention_ms,
                        now_ms(),
                        false,
                        Some(&mut fetches),
                    )
                    .await
                {
                    Ok(Some(reason)) => {
                        candidate.reason = reason;
                        middle.push(candidate);
                        continue;
                    }
                    Err(error) => {
                        candidate.reason = error.code;
                        middle.push(candidate);
                        continue;
                    }
                    Ok(None) => {}
                }
            }
            if auto_cleanup
                && matches!(
                    candidate.artifact.kind,
                    ArtifactKind::Branch | ArtifactKind::NativeSession
                )
            {
                candidate.reason = "manual_only".to_owned();
                middle.push(candidate);
                continue;
            }
            if candidate.artifact.kind == ArtifactKind::Worktree {
                if call_deadline.saturating_duration_since(tokio::time::Instant::now())
                    < CLEANUP_PLAN_MAX_PREFLIGHT
                {
                    break;
                }
                self.classify_worktree(&mut candidate, Some(&mut fetches))
                    .await;
            } else if candidate.artifact.kind == ArtifactKind::Branch {
                let Some(git) = candidate.artifact.git.as_ref() else {
                    candidate.reason = "source_integrity".to_owned();
                    middle.push(candidate);
                    continue;
                };
                if call_deadline.saturating_duration_since(tokio::time::Instant::now())
                    < CLEANUP_PLAN_MAX_PREFLIGHT
                {
                    break;
                }
                if !self
                    .inner
                    .store
                    .lock()
                    .await
                    .cleanup_agent_quiescent(&candidate.artifact)?
                {
                    candidate.reason = "agent_not_quiescent".to_owned();
                    middle.push(candidate);
                    continue;
                }
                match self
                    .cleanup_branch_classification_with_deadline(
                        &candidate.artifact,
                        git,
                        Some(&mut fetches),
                        Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT,
                    )
                    .await
                {
                    Ok(classification) => {
                        candidate.artifact.identity = classification.tip.clone();
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = classification.reason;
                        candidate.bytes = Some(0);
                    }
                    Err(error) => candidate.reason = error.code,
                }
            } else if candidate.artifact.kind == ArtifactKind::NativeSession {
                let swarm_created = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_agent(&candidate.artifact.agent)?
                    .is_some_and(|agent| swarm_created_session_agent(&agent));
                if !swarm_created || !native_archive_supported(&candidate.artifact.purpose) {
                    candidate.reason = "no_native_archive".to_owned();
                } else if !self
                    .inner
                    .store
                    .lock()
                    .await
                    .cleanup_agent_quiescent(&candidate.artifact)?
                {
                    candidate.reason = "agent_not_quiescent".to_owned();
                } else {
                    candidate.verdict = "safe".to_owned();
                    candidate.reason = "native_archive_pending".to_owned();
                    candidate.bytes = None;
                }
            } else if self
                .inner
                .store
                .lock()
                .await
                .cleanup_agent_quiescent(&candidate.artifact)?
            {
                if call_deadline.saturating_duration_since(tokio::time::Instant::now())
                    < CLEANUP_PLAN_MAX_PREFLIGHT
                {
                    break;
                }
                match self.cleanup_preflight(&candidate.artifact).await {
                    Ok(bytes) => {
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = if ownership_root {
                            "terminal_owned_artifact_root"
                        } else {
                            "terminal_owned_artifact"
                        }
                        .to_owned();
                        candidate.bytes = Some(bytes);
                    }
                    Err(error) => candidate.reason = error.code,
                }
            }
            middle.push(candidate);
        }
        let middle_len = middle.len();
        let (items, safe_shown, kept_shown) =
            merge_cleanup_page(shown_safe, middle, kept_projects, page.kept_skipped);
        plan.items = items;
        // Project rows consume page slots: when they displace artifact rows
        // off a full Store page, the listing continues even though the
        // Store reported no truncation of its own.
        plan.truncated = artifacts_truncated
            || middle_len < artifacts_total
            || page.safe_skipped + safe_shown < safe_total
            || page.kept_skipped + kept_shown < kept_total;
        if plan.truncated {
            // The next page resumes after the last shown artifact inside the
            // same ledger snapshot, skipping the project rows already
            // emitted. Registration ids are minted fresh per plan, so the
            // offsets travel in the token instead of as row ids. The ordered
            // project ids pin the traversal: a reclassified project makes
            // the next page stale instead of skipping or repeating a row.
            let last_artifact = plan
                .items
                .iter()
                .rev()
                .find(|item| item.artifact.kind != ArtifactKind::ProjectRegistration)
                .map(|item| item.artifact.id.clone())
                .or(page.artifact_after);
            let keep_scan_cursor = artifacts_total == 0 || middle_len == artifacts_total;
            plan.continuation = Some(
                PlanContinuation {
                    artifact_after: last_artifact,
                    safe_skipped: page.safe_skipped + safe_shown,
                    kept_skipped: page.kept_skipped + kept_shown,
                    regs_revision,
                    snapshot_seq: Some(snapshot_seq),
                    safe_ids,
                    kept_ids,
                    backfill_after: None,
                    inventory_rank: keep_scan_cursor.then_some(next_rank),
                    inventory_seq_after: if keep_scan_cursor { next_seq } else { None },
                    request_binding: Some(binding),
                }
                .encode()?,
            );
        }
        let retain_plan = if auto_cleanup {
            retain_automatic_plan && plan.items.iter().any(|item| item.verdict == "safe")
        } else {
            !plan.items.is_empty() || !plan.truncated
        };
        if retain_plan {
            self.inner
                .store
                .lock()
                .await
                .retain_cleanup_plan(&name, &plan, now_ms())?;
        }
        encoded(plan)
    }

    async fn backfill_cleanup_inventory(
        &self,
        principal: &str,
        request: &CleanupPlanRequest,
        mut after: Option<String>,
    ) -> Result<Option<String>> {
        let deadline = Instant::now() + CLEANUP_BACKFILL_BUDGET;
        let initial_after = after.clone();
        for _ in (0..CLEANUP_BACKFILL_LIMIT).step_by(CLEANUP_BACKFILL_BATCH) {
            let (agents, truncated) = self
                .inner
                .store
                .lock()
                .await
                .list_agents(after.as_deref(), CLEANUP_BACKFILL_BATCH)?;
            let mut eligible = Vec::new();
            for agent in &agents {
                if request
                    .project
                    .as_ref()
                    .is_none_or(|project| project == &agent.project)
                    && self
                        .inner
                        .store
                        .lock()
                        .await
                        .cleanup_visible_agent(principal, agent)?
                {
                    eligible.push(agent);
                }
            }
            let backfill =
                futures_util::future::try_join_all(eligible.into_iter().map(|agent| async move {
                    self.record_agent_artifacts(agent).await.map_err(|error| {
                        let message = format!(
                            "Cleanup inventory could not read {}. {}",
                            agent.id, error.message
                        );
                        error.with_message(message)
                    })
                }));
            match timeout_at(deadline, backfill).await {
                Ok(result) => {
                    result?;
                }
                // A partial batch can leave idempotent ownership records,
                // but only completed batches advance the continuation.
                Err(_) if after != initial_after => break,
                Err(_) => {
                    return Err(Error::new(
                        "unavailable",
                        "Cleanup inventory probe timed out",
                    ));
                }
            }
            if !truncated && agents.len() < CLEANUP_BACKFILL_BATCH {
                return Ok(None);
            }
            after = agents.last().map(|agent| agent.id.clone()).or(after);
            if Instant::now() >= deadline {
                break;
            }
        }
        Ok(after)
    }

    async fn classify_worktree(
        &self,
        candidate: &mut CleanupCandidate,
        fetches: Option<&mut CleanupFetches>,
    ) {
        if candidate.artifact.state != "registered" {
            candidate.reason = candidate.artifact.state.clone();
            return;
        }
        match self
            .worktree_cleanup_preflight(&candidate.artifact, fetches)
            .await
        {
            Ok(classification) => {
                if let Some(git) = candidate.artifact.git.as_mut() {
                    git.planned_tip = Some(classification.tip);
                }
                candidate.verdict = "safe".to_owned();
                candidate.reason = classification.reason;
            }
            Err(error) => candidate.reason = error.code,
        }
    }

    async fn worktree_cleanup_preflight(
        &self,
        artifact: &ArtifactView,
        fetches: Option<&mut CleanupFetches>,
    ) -> Result<BranchClassification> {
        if !cfg!(windows) {
            return Err(Error::new(
                "unsupported",
                "worktree write guard is unavailable",
            ));
        }
        let (preparation, profile) = {
            let store = self.inner.store.lock().await;
            let preparation = store.workspace_removal_eligibility(&artifact.agent, now_ms())?;
            let owner = store
                .get_agent(&artifact.agent)?
                .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
            if !store.cleanup_agent_quiescent(artifact)? {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "workspace owner is not quiescent",
                ));
            }
            (preparation, owner.profile_snapshot)
        };
        check_worktree_artifact(artifact, &preparation)?;
        let git = crate::workspace_git::WorkspaceGit::new()?;
        let preflight = self
            .inner
            .workspaces
            .removal_candidates(
                &git,
                &preparation,
                &profile,
                None,
                fetches,
                Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT,
            )
            .await?;
        Ok(preflight.branch)
    }

    pub(crate) async fn auto_cleanup_worktrees(&self, after: &mut Option<String>) {
        if let Err(error) = self.sweep_automatic_cleanup(after).await {
            if error.code == "invalid" {
                *after = None;
                self.inner.automatic_cleanup.lock_recover().active_sweep = None;
            }
            self.automatic_cleanup_failed(&error);
        }
    }

    async fn sweep_automatic_cleanup(&self, after: &mut Option<String>) -> Result<()> {
        let config = crate::config::load_automation_config(&self.inner.data_root)?;
        if config.auto_cleanup == Some(false) {
            *after = None;
            self.inner.automatic_cleanup.lock_recover().active_sweep = None;
            return Ok(());
        }
        let first_page = after.is_none();
        let plan_only = config.auto_cleanup != Some(true)
            || !self
                .inner
                .automatic_cleanup
                .lock_recover()
                .first_sweep_complete;
        let retention_ms =
            crate::automation::parse_positive_duration_ms(&config.auto_cleanup_after)?;
        let params = json!({"scope": "all", "continuation": after.clone()});
        let plan: CleanupPlan = serde_json::from_value(
            self.auto_cleanup_plan(&params, retention_ms, !plan_only)
                .await?,
        )
        .map_err(|_| Error::new("source_integrity", "automatic cleanup plan readback failed"))?;
        let selected = plan
            .items
            .iter()
            .filter(|item| item.verdict == "safe")
            .map(|item| item.artifact.id.clone())
            .collect::<Vec<_>>();
        let applied: Option<CleanupApplyResponse> = if plan_only || selected.is_empty() {
            None
        } else {
            let request = json!({
                "id": ulid::Ulid::new().to_string(),
                "plan_id": plan.plan_id,
                "artifacts": selected,
            });
            let value = self
                .cleanup_apply_plan(&request, &Principal::Owner, true)
                .await?;
            Some(serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "source_integrity",
                    "automatic cleanup outcome readback failed",
                )
            })?)
        };
        let page = self
            .record_automatic_cleanup(
                &plan,
                applied.as_ref(),
                config.auto_cleanup == Some(true),
                plan_only,
            )
            .await?;
        *after = plan.continuation.clone();
        if let Some(sweep) =
            self.accumulate_automatic_cleanup_page(page, first_page, plan.continuation.is_none())
        {
            self.notify_automatic_cleanup_sweep(&sweep).await?;
            self.inner
                .automatic_cleanup
                .lock_recover()
                .first_sweep_complete = true;
        }
        Ok(())
    }

    async fn automatic_cleanup_effect_gate(&self, artifact: &ArtifactView) -> Result<()> {
        let config = crate::config::load_automation_config(&self.inner.data_root)?;
        if config.auto_cleanup != Some(true) {
            return Err(Error::new(
                "cleanup_disabled",
                "automatic cleanup apply requires explicit enablement",
            ));
        }
        if !self
            .inner
            .automatic_cleanup
            .lock_recover()
            .first_sweep_complete
        {
            return Err(Error::new(
                "cleanup_startup_sweep_pending",
                "the first complete daemon-startup cleanup sweep must be plan-only",
            ));
        }
        let age = crate::automation::parse_positive_duration_ms(&config.auto_cleanup_after)?;
        if let Some(reason) = self
            .automatic_cleanup_keep_reason(artifact, age, now_ms(), true, None)
            .await?
        {
            return Err(Error::new(reason, "automatic cleanup eligibility changed"));
        }
        Ok(())
    }

    async fn cleanup_preflight(&self, artifact: &ArtifactView) -> Result<u64> {
        if artifact.kind == ArtifactKind::NativeSession {
            // The archive itself runs at apply through the harness native
            // API; the plan verdict already gated harness and quiescence.
            return Ok(0);
        }
        if artifact.kind == ArtifactKind::Branch {
            let git = artifact
                .git
                .as_ref()
                .ok_or_else(|| Error::new("invalid", "branch Git binding absent"))?;
            self.cleanup_branch_classification(artifact, git).await?;
            return Ok(0);
        }
        if artifact.kind == ArtifactKind::DataDir {
            Self::cleanup_check_daemon_lock(Path::new(&artifact.path))?;
        }
        let agent = self
            .inner
            .store
            .lock()
            .await
            .get_agent(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
        let path = self
            .checked_cleanup_artifact_path(&agent, artifact, false)
            .await?;
        self.cleanup_check_git(&agent, &path).await?;
        #[cfg(windows)]
        {
            let tree = CleanupTree::open(&path)?;
            if tree.identity != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "artifact directory was substituted",
                ));
            }
            if artifact.state == "ownership_root" {
                if tree.contains_daemon_lock {
                    return Err(Error::new(
                        "nested_daemon_lock",
                        "owned artifact root contains a private daemon.lock marker",
                    ));
                }
                if self
                    .inner
                    .store
                    .lock()
                    .await
                    .cleanup_root_has_descendants(artifact)?
                {
                    return Err(Error::new(
                        "artifact_root_has_descendants",
                        "owned artifact root still has retained descendants",
                    ));
                }
            }
            Ok(tree.bytes)
        }
        #[cfg(not(windows))]
        {
            let _ = path;
            Err(Error::new(
                "unsupported",
                "handle deletion is unavailable on this platform",
            ))
        }
    }

    pub(super) async fn cleanup_apply(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        self.cleanup_apply_plan(params, principal, false).await
    }

    async fn cleanup_apply_plan(
        &self,
        params: &Value,
        principal: &Principal,
        automatic: bool,
    ) -> Result<Value> {
        let request: CleanupApplyRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "cleanup.apply",
            CleanupApplyRequest::validate,
        )?;
        // Remote branch deletion is an explicit Owner-only extra on top of a
        // consumed local apply. It is never automatic and never delegated to
        // managers or Agents.
        if request.delete_remote {
            require_owner(principal)?;
        }
        let name = principal_name(principal);
        let (plan, mut response) = {
            let mut store = self.inner.store.lock().await;
            let (plan, mut response, is_new) =
                store.begin_cleanup_apply(&name, &request, now_ms())?;
            if !is_new {
                return encoded(response);
            }
            self.apply_project_registrations(&mut store, &request, &plan, &mut response)?;
            (plan, response)
        };
        self.apply_cleanup_artifacts(&name, &request, &plan, &mut response, false, automatic)
            .await?;
        encoded(response)
    }

    async fn apply_cleanup_artifacts(
        &self,
        name: &str,
        request: &CleanupApplyRequest,
        plan: &CleanupPlan,
        response: &mut CleanupApplyResponse,
        entire_tree: bool,
        automatic: bool,
    ) -> Result<()> {
        // A native reader may need the managed workspace as its directory.
        let (sessions, other): (Vec<_>, Vec<_>) = plan
            .items
            .iter()
            .enumerate()
            .partition(|(_, item)| item.artifact.kind == ArtifactKind::NativeSession);
        for (index, item) in sessions.into_iter().chain(other) {
            if item.artifact.kind == ArtifactKind::ProjectRegistration
                || item.verdict != "safe"
                || response.items[index].outcome != "unknown"
            {
                continue;
            }
            let blocked_worktree = item.artifact.kind == ArtifactKind::Worktree
                && plan
                    .items
                    .iter()
                    .zip(&response.items)
                    .any(|(session, outcome)| {
                        session.artifact.kind == ArtifactKind::NativeSession
                            && session.verdict == "safe"
                            && request.selects(&session.artifact.id)
                            && outcome.outcome != "removed"
                            && (entire_tree
                                || session.artifact.agent == item.artifact.agent
                                || cleanup_tree_root(&session.artifact).is_some_and(|root| {
                                    cleanup_tree_root(&item.artifact) == Some(root)
                                }))
                    });
            let removal = if blocked_worktree {
                Err(Error::new(
                    "native_session_kept",
                    "The tree's native session cleanup did not complete",
                ))
            } else if automatic {
                match self.automatic_cleanup_effect_gate(&item.artifact).await {
                    Ok(()) => {
                        self.cleanup_remove(name, &item.artifact, request, true)
                            .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                self.cleanup_remove(name, &item.artifact, request, false)
                    .await
            };
            let outcome = match removal {
                Ok(bytes) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    bytes_reclaimed: bytes,
                },
                Err(error) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: if matches!(error.code.as_str(), "unknown" | "remote_delete_failed") {
                        // A failed remote deletion lands here only after the
                        // local branch is already gone: the item is partial,
                        // never a clean skip, and never replayed.
                        "unknown"
                    } else {
                        "skipped"
                    }
                    .to_owned(),
                    reason: Some(error.code),
                    bytes_reclaimed: 0,
                },
            };
            response.bytes_reclaimed += outcome.bytes_reclaimed;
            response.items[index] = outcome.clone();
            self.inner.store.lock().await.settle_cleanup_item(
                name,
                request,
                response,
                &item.artifact,
                &outcome,
            )?;
        }
        Ok(())
    }

    async fn cleanup_remove(
        &self,
        principal: &str,
        artifact: &ArtifactView,
        request: &CleanupApplyRequest,
        automatic: bool,
    ) -> Result<u64> {
        if artifact.kind == ArtifactKind::Branch {
            let git = artifact
                .git
                .as_ref()
                .ok_or_else(|| Error::new("source_integrity", "branch binding absent"))?;
            {
                let store = self.inner.store.lock().await;
                if !store.cleanup_authorized(principal, artifact)?
                    || !store.cleanup_agent_quiescent(artifact)?
                {
                    return Err(Error::new(
                        "agent_not_quiescent",
                        "branch authority changed",
                    ));
                }
            }
            // Re-verify against a fresh fetch at apply time: no plan-time
            // classification is trusted across the plan/apply gap.
            let classification = self.cleanup_branch_classification(artifact, git).await?;
            if classification.tip != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "branch tip moved since the plan was retained",
                ));
            }
            Self::delete_local_branch(git, &artifact.identity).await?;
            if request.delete_remote {
                Self::delete_remote_branch(
                    &self.inner.data_root,
                    git,
                    &classification.remote,
                    &classification.url,
                )
                .await
                .map_err(|error| Error::new("remote_delete_failed", (*error.message).clone()))?;
            }
            return Ok(0);
        }
        if artifact.kind == ArtifactKind::NativeSession {
            // Local native stores are never deleted as files. Codex threads
            // are archived through the harness app-server API and OpenCode
            // sessions are deleted through the harness's documented session
            // delete on the retained per-Agent store; every other harness
            // has no documented archive surface and stays kept. Only a
            // Swarm-created Agent's session is ever archived: attached or
            // observed sessions belong to their own producer.
            let agent = self
                .inner
                .store
                .lock()
                .await
                .get_agent(&artifact.agent)?
                .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
            if !swarm_created_session_agent(&agent) || !native_archive_supported(&artifact.purpose)
            {
                return Err(Error::new(
                    "no_native_archive",
                    "harness has no native archive method",
                ));
            }
            {
                let store = self.inner.store.lock().await;
                if !store.cleanup_authorized(principal, artifact)?
                    || !store.cleanup_agent_quiescent(artifact)?
                {
                    return Err(Error::new(
                        "agent_not_quiescent",
                        "native session authority changed",
                    ));
                }
            }
            let (profile, native_ref) = frozen_archive_profile(&agent, artifact)?;
            return self
                .inner
                .plugin_host
                .archive_native_session(profile, &native_ref, &agent)
                .await
                .map_err(|error| {
                    if matches!(
                        error.code.as_str(),
                        "unavailable"
                            | "unknown"
                            | "cancelled"
                            | "busy"
                            | "native_readback"
                            | "source_integrity"
                            | "native_archive_failed"
                    ) {
                        // Transport, timeout or shutdown: the archive may or
                        // may not have happened. Never claim a skip.
                        Error::new("unknown", (*error.message).clone())
                    } else {
                        error
                    }
                })
                .map(|()| 0);
        }
        if artifact.kind == ArtifactKind::Worktree {
            return self
                .cleanup_remove_worktree(principal, artifact, &request.id, automatic)
                .await;
        }
        let agent = self
            .inner
            .store
            .lock()
            .await
            .get_agent(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
        let path = self
            .checked_cleanup_artifact_path(&agent, artifact, true)
            .await?;
        self.cleanup_check_git(&agent, &path).await?;
        #[cfg(windows)]
        {
            let tree = CleanupTree::open(&path)?;
            if tree.identity != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "artifact directory was substituted",
                ));
            }
            if artifact.kind == ArtifactKind::DataDir {
                // Re-verify at apply time: a live private daemon or a process
                // holding the directory keeps it, even if the lock lapsed at
                // plan time.
                Self::cleanup_check_daemon_lock(Path::new(&artifact.path))?;
            }
            let store = self.inner.store.lock().await;
            if automatic
                && store
                    .get_agent(&artifact.agent)?
                    .is_some_and(|agent| agent.archived)
            {
                return Err(Error::new(
                    "archived",
                    "Archived Agent artifacts are protected",
                ));
            }
            if artifact.state == "ownership_root" && tree.contains_daemon_lock {
                return Err(Error::new(
                    "nested_daemon_lock",
                    "owned artifact root contains a private daemon.lock marker",
                ));
            }
            if artifact.state == "ownership_root" && store.cleanup_root_has_descendants(artifact)? {
                return Err(Error::new(
                    "artifact_root_has_descendants",
                    "owned artifact root still has retained descendants",
                ));
            }
            if !store.cleanup_authorized(principal, artifact)?
                || !store.cleanup_agent_quiescent(artifact)?
            {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "cleanup authority or generation changed",
                ));
            }
            tree.remove()
        }
        #[cfg(not(windows))]
        {
            let _ = path;
            Err(Error::new(
                "unsupported",
                "handle deletion is unavailable on this platform",
            ))
        }
    }

    async fn cleanup_check_git(&self, agent: &Agent, path: &Path) -> Result<()> {
        let canonical = std::fs::canonicalize(path)
            .map_err(|_| Error::new("unavailable", "artifact directory unavailable"))?;
        let path = canonical.as_path();
        let workspace = agent
            .workspace
            .as_deref()
            .ok_or_else(|| Error::new("unavailable", "artifact Agent has no workspace"))?;
        let retained = self.inner.store.lock().await.get_workspace(&agent.id)?;
        let root = if Path::new(workspace).is_dir() {
            Path::new(workspace)
        } else {
            Path::new(
                retained
                    .as_ref()
                    .map(|w| w.repo_path.as_str())
                    .ok_or_else(|| {
                        Error::new("unavailable", "artifact Git workspace unavailable")
                    })?,
            )
        };
        let probe = crate::git_monitor::GitProbe::new()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        // Check both the retained workspace and the candidate's actual containing
        // repository. Another checkout's tracked descendants are protected too.
        for (location, required) in [(root, !agent.chat), (path, false)] {
            let top = probe
                .workspace_command(
                    location,
                    &["rev-parse", "--path-format=absolute", "--show-toplevel"],
                    deadline,
                )
                .await?;
            if top.code != 0 {
                if !required && top.code == 128 {
                    continue;
                }
                return Err(Error::new(
                    "unavailable",
                    "artifact Git identity unavailable",
                ));
            }
            let top = std::str::from_utf8(&top.stdout)
                .map_err(|_| Error::new("unavailable", "Git path unavailable"))?
                .trim_end_matches(['\r', '\n']);
            let top = std::fs::canonicalize(top)
                .map_err(|_| Error::new("unavailable", "Git root unavailable"))?;
            if path == top || top.starts_with(path) {
                return Err(Error::new(
                    "tracked_content",
                    "repository root is protected",
                ));
            }
            if path.starts_with(&top) {
                let argument = crate::git_monitor::git_path_argument(path)?;
                let files = probe
                    .workspace_command(location, &["ls-files", "-z", "--", &argument], deadline)
                    .await?;
                if files.code != 0 {
                    return Err(Error::new(
                        "unavailable",
                        "tracked content check unavailable",
                    ));
                }
                if !files.stdout.is_empty() {
                    return Err(Error::new(
                        "tracked_content",
                        "artifact contains tracked source",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn cleanup_remove_worktree(
        &self,
        principal: &str,
        artifact: &ArtifactView,
        apply_id: &str,
        automatic: bool,
    ) -> Result<u64> {
        let mut progress = self.progress_receiver();
        let admission = {
            let mut store = self.inner.store.lock().await;
            if automatic
                && store
                    .get_agent(&artifact.agent)?
                    .is_some_and(|agent| agent.archived)
            {
                return Err(Error::new(
                    "archived",
                    "Archived Agent worktrees are protected",
                ));
            }
            if !store.cleanup_authorized(principal, artifact)?
                || !store.cleanup_agent_quiescent(artifact)?
            {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "worktree authority changed",
                ));
            }
            let preparation = store.workspace_removal_eligibility(&artifact.agent, now_ms())?;
            check_worktree_artifact(artifact, &preparation)?;
            let id = eliot_store::cleanup_worktree_command_id(apply_id, &artifact.id)?;
            let mut request = SubmitCommand::new(id, "worktree.remove");
            request.target = Some(artifact.agent.clone());
            request.expected_generation = Some(artifact.generation);
            if let Some(tip) = artifact
                .git
                .as_ref()
                .and_then(|git| git.planned_tip.as_ref())
            {
                request.args = json!({"expected_tip": tip});
            }
            let admission = self.admit_prepared_submission(
                &mut store,
                principal,
                PreparedSubmission {
                    request,
                    workspace: None,
                },
                SubmissionContext::default(),
            )?;
            self.publish_locked(&admission.events);
            admission
        };
        self.schedule_admission(&admission);
        self.signal_progress();
        let unknown = || {
            Error::new(
                "unknown",
                format!(
                    "worktree removal {} has no exact settlement",
                    admission.command.id
                ),
            )
        };
        timeout(Duration::from_secs(90), async {
            loop {
                let command = self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_command(&admission.command.id)
                    .map_err(|_| unknown())?
                    .ok_or_else(unknown)?;
                match command.state {
                    CommandState::Applied => return Ok(0),
                    CommandState::Unknown => return Err(unknown()),
                    state if state.is_terminal() => {
                        let code = command
                            .outcome
                            .as_ref()
                            .and_then(|outcome| outcome.get("code"))
                            .and_then(Value::as_str)
                            .unwrap_or("worktree_remove_refused");
                        return Err(Error::new(
                            code,
                            "worktree removal was refused before mutation",
                        ));
                    }
                    _ => {}
                }
                progress.changed().await.map_err(|_| unknown())?;
            }
        })
        .await
        .map_err(|_| unknown())?
    }

    async fn cleanup_branch_classification(
        &self,
        artifact: &ArtifactView,
        git: &CleanupGitBinding,
    ) -> Result<BranchClassification> {
        self.cleanup_branch_classification_with_deadline(
            artifact,
            git,
            None,
            Instant::now() + Duration::from_secs(60),
        )
        .await
    }

    async fn cleanup_branch_classification_with_deadline(
        &self,
        artifact: &ArtifactView,
        git: &CleanupGitBinding,
        fetches: Option<&mut CleanupFetches>,
        deadline: Instant,
    ) -> Result<BranchClassification> {
        let workspace = self
            .inner
            .store
            .lock()
            .await
            .get_workspace(&artifact.agent)?;
        if workspace.is_some_and(|workspace| {
            workspace.phase != WorkspacePhase::Removed
                && workspace.requested_branch == git.branch
                && workspace.repo_common_dir == git.common
        }) {
            return Err(Error::new(
                "worktree_attached",
                "the retained worktree still uses this branch",
            ));
        }
        classify_branch(
            &self.inner.data_root,
            &git.repo,
            Path::new(&git.common),
            &git.branch,
            ContainmentScope::BranchDeletion,
            fetches,
            deadline,
        )
        .await
    }

    /// Only called after fresh current-tip containment. Recheck attachment
    /// and the pinned tip before deleting. Git's ancestry gate does not
    /// recognize squash merges, so use `-D` after these checks.
    async fn delete_local_branch(git: &CleanupGitBinding, expected_tip: &str) -> Result<()> {
        if !crate::git_actions::valid_cleanup_branch(&git.branch) {
            return Err(Error::new(
                "invalid_branch",
                "branch name is not admissible",
            ));
        }
        let repo = Path::new(&git.common);
        let probe = crate::git_monitor::GitProbe::new()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let worktrees = probe
            .workspace_command(repo, &["worktree", "list", "--porcelain"], deadline)
            .await
            .map_err(|_| Error::new("branch_read_failed", "worktree attachments are unreadable"))?;
        if worktrees.code != 0 {
            return Err(Error::new(
                "branch_read_failed",
                "worktree attachments are unreadable",
            ));
        }
        let attached = format!("branch refs/heads/{}", git.branch);
        if String::from_utf8_lossy(&worktrees.stdout)
            .lines()
            .any(|line| line == attached)
        {
            return Err(Error::new(
                "branch_checked_out",
                "branch is checked out in a live worktree",
            ));
        }
        let tip = branch_rev_parse(
            &probe,
            repo,
            &format!("refs/heads/{}", git.branch),
            "branch_absent",
            deadline,
        )
        .await?;
        if tip != expected_tip {
            return Err(Error::new(
                "identity_changed",
                "branch tip moved since the plan was retained",
            ));
        }
        let deleted = match probe
            .workspace_command(repo, &["branch", "-D", "--", &git.branch], deadline)
            .await
        {
            Ok(deleted) => deleted.code,
            Err(_) => -1,
        };
        if deleted == 0 {
            return Ok(());
        }
        let branch_ref = format!("refs/heads/{}", git.branch);
        let again = branch_rev_parse(&probe, repo, &branch_ref, "branch_absent", deadline)
            .await
            .map_err(|_| Error::new("unknown", "branch deletion outcome is uncertain"))?;
        if !again.is_empty() {
            return Err(Error::new(
                "branch_delete_refused",
                "Git refused branch deletion and the branch still exists",
            ));
        }
        Err(Error::new(
            "unknown",
            "branch vanished during deletion; outcome is uncertain",
        ))
    }

    /// Owner-only remote deletion after the local branch is already gone. The
    /// push runs through the isolated transport and the remote branch is
    /// verified absent before and after the push.
    async fn delete_remote_branch(
        data_root: &Path,
        git: &CleanupGitBinding,
        remote: &str,
        url: &str,
    ) -> Result<()> {
        use crate::git_actions::{CleanupTransportOp, run_cleanup_transport};
        if !crate::git_actions::valid_cleanup_branch(&git.branch) {
            return Err(Error::new(
                "invalid_branch",
                "branch name is not admissible",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        if Self::remote_branch_present(data_root, git, remote, url, deadline).await? {
            let push = run_cleanup_transport(
                data_root,
                &git.repo,
                remote,
                url,
                Some(&git.branch),
                CleanupTransportOp::PushDelete,
                deadline,
            )
            .await
            .map_err(|error| {
                Error::new(
                    "remote_delete_failed",
                    format!("remote branch deletion is unavailable: {}", error.message),
                )
            })?;
            if push.interrupted || !push.quiescent || push.exit_code != Some(0) {
                return Err(Error::new(
                    "remote_delete_failed",
                    "remote branch deletion did not complete",
                ));
            }
        }
        if Self::remote_branch_present(data_root, git, remote, url, deadline).await? {
            return Err(Error::new(
                "remote_delete_failed",
                "remote branch is still present after deletion",
            ));
        }
        Ok(())
    }

    async fn remote_branch_present(
        data_root: &Path,
        git: &CleanupGitBinding,
        remote: &str,
        url: &str,
        deadline: Instant,
    ) -> Result<bool> {
        use crate::git_actions::{CleanupTransportOp, run_cleanup_transport};
        let heads = run_cleanup_transport(
            data_root,
            &git.repo,
            remote,
            url,
            Some(&git.branch),
            CleanupTransportOp::ListHeads,
            deadline,
        )
        .await
        .map_err(|_| Error::new("remote_check_failed", "branch remote check is unavailable"))?;
        if heads.interrupted || !heads.quiescent || heads.exit_code != Some(0) {
            return Err(Error::new(
                "remote_check_failed",
                "branch remote check did not complete",
            ));
        }
        Ok(!heads.stdout.is_empty())
    }
}
/// Ordered project ids of one verdict band, in traversal order. The
/// continuation pins these so a reclassified project (same settings
/// revision, different verdict) is detected instead of skipped or repeated.
fn project_ids(candidates: &[CleanupCandidate]) -> Vec<String> {
    candidates
        .iter()
        .map(|item| item.artifact.project.clone())
        .collect()
}

fn cleanup_plan_request_binding(principal: &str, request: &CleanupPlanRequest) -> Result<String> {
    let value = json!({
        "principal": principal,
        "scope": request.scope,
        "project": request.project.as_deref(),
        "older_than": request.older_than,
    });
    let digest = eliot_core::canonical_json_sha256(&value)
        .map_err(|_| Error::new("internal", "cleanup request binding could not encode"))?;
    let mut binding = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        binding.push(HEX[(byte >> 4) as usize] as char);
        binding.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(binding)
}

/// Whether a continued page may reuse its offsets: the settings revision
/// matches and the ordered project ids and verdict bands are unchanged. A
/// checkout appearing or disappearing reclassifies its project without
/// moving the revision; the saved positional offsets would then skip or
/// repeat a registration, so the page is stale instead.
fn continuation_order_valid(
    page: &PlanContinuation,
    revision: &Option<String>,
    safe: &[CleanupCandidate],
    kept: &[CleanupCandidate],
) -> bool {
    page.regs_revision == *revision
        && page.safe_ids.as_ref() == Some(&project_ids(safe))
        && page.kept_ids.as_ref() == Some(&project_ids(kept))
}

/// Splice one listing page in final candidate order: the already-offset
/// safe project rows, the classified artifact page, then the kept rows.
/// Caps the merged page at `MAX_CLEANUP_ITEMS` and reports how many
/// project rows each side contributed, so the continuation skips exactly
/// the rows already emitted (registration ids are minted fresh per plan
/// and can never serve as cursors).
fn merge_cleanup_page(
    safe: Vec<CleanupCandidate>,
    middle: Vec<CleanupCandidate>,
    kept: Vec<CleanupCandidate>,
    kept_skipped: usize,
) -> (Vec<CleanupCandidate>, usize, usize) {
    let safe_shown = safe.len();
    let mut items = safe;
    let room = MAX_CLEANUP_ITEMS - items.len();
    items.extend(middle.into_iter().take(room));
    let mut kept_shown = 0_usize;
    for item in kept
        .into_iter()
        .skip(kept_skipped)
        .take(MAX_CLEANUP_ITEMS - items.len())
    {
        items.push(item);
        kept_shown += 1;
    }
    (items, safe_shown, kept_shown)
}

/// Split the recorded `native:{scope}:{reference}` path into its reference.
/// The scope is cross-checked against the retained native identity so a
/// substituted path can never archive a different thread.
fn native_session_reference(artifact: &ArtifactView) -> Result<String> {
    let path = artifact
        .path
        .strip_prefix("native:")
        .ok_or_else(|| Error::new("source_integrity", "native session path is malformed"))?;
    let scope = artifact.identity.as_str();
    let reference = path
        .strip_prefix(scope)
        .and_then(|rest| rest.strip_prefix(':'))
        .ok_or_else(|| Error::new("source_integrity", "native session path is malformed"))?;
    if reference.is_empty() || reference.len() > 512 || reference.chars().any(char::is_control) {
        return Err(Error::new(
            "source_integrity",
            "native session reference is malformed",
        ));
    }
    Ok(reference.to_owned())
}

fn frozen_archive_profile(agent: &Agent, artifact: &ArtifactView) -> Result<(Value, String)> {
    let reference = native_session_reference(artifact)?;
    let profile = agent.profile_snapshot.clone();
    if !swarm_created_session_agent(agent)
        || agent.id != artifact.agent
        || agent.generation != artifact.generation
        || agent.native_scope.as_deref() != Some(artifact.identity.as_str())
        || agent.native_ref.as_deref() != Some(reference.as_str())
        || !native_archive_supported(&agent.harness)
        || artifact.purpose != agent.harness
        || profile["harness"] != agent.harness.as_str()
    {
        return Err(Error::new(
            "native_scope_unverified",
            "archive identity differs from the frozen native store",
        ));
    }
    if agent.harness == "codex" {
        let binding: eliot_core::NativeStoreBinding = profile
            .get("native_store_binding")
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "native_scope_unverified",
                    "archive requires the frozen native store",
                )
            })
            .and_then(|value| {
                serde_json::from_value(value).map_err(|_| {
                    Error::new(
                        "native_scope_unverified",
                        "frozen archive store is malformed",
                    )
                })
            })?;
        if binding.native_scope != artifact.identity {
            return Err(Error::new(
                "native_scope_unverified",
                "archive identity differs from the frozen native store",
            ));
        }
    }
    Ok((profile, reference))
}

/// Only a Swarm-created Agent's retained session may be archived: a
/// launched Agent, never an attached or observed session (including
/// observed native children) that belongs to its own producer.
fn swarm_created_session_agent(agent: &Agent) -> bool {
    !agent.observe_only && matches!(agent.origin, eliot_core::AgentOrigin::Eliot)
}

fn native_archive_supported(harness: &str) -> bool {
    matches!(harness, "codex" | "opencode")
}

fn cleanup_tree_root(artifact: &ArtifactView) -> Option<&str> {
    let (agent, _) = artifact.tree.last()?.split_once(':')?;
    Some(agent)
}

fn native_session_artifact(agent: &Agent) -> Option<ArtifactView> {
    let scope = agent.native_scope.as_ref()?;
    let reference = agent.native_ref.as_ref()?;
    Some(ArtifactView {
        id: ulid::Ulid::new().to_string(),
        agent: agent.id.clone(),
        generation: agent.generation,
        project: agent.project.clone(),
        tree: Vec::new(),
        path: format!("native:{scope}:{reference}"),
        kind: ArtifactKind::NativeSession,
        purpose: agent.harness.clone(),
        identity: scope.clone(),
        created_at: agent.created_at,
        state: "registered".to_owned(),
        git: None,
    })
}

impl Router {
    /// Keep a registered data directory while a private daemon lock or a
    /// live process holds it. A present `daemon.lock` with a live owner
    /// process (read from the sibling `daemon.json`, never by taking the
    /// lock) keeps the directory; a stale lock with a dead owner does not.
    /// An unreadable lock file also keeps: absence of proof is not proof of
    /// absence. Live processes holding open handles additionally refuse the
    /// tree open at plan and apply time.
    fn cleanup_check_daemon_lock(dir: &Path) -> Result<()> {
        let lock = dir.join("daemon.lock");
        match std::fs::symlink_metadata(&lock) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(Error::new(
                    "daemon_lock_held",
                    "private daemon lock is unreadable; directory is kept",
                ));
            }
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    return Err(Error::new(
                        "identity_changed",
                        "private daemon lock is not a regular file",
                    ));
                }
                if !metadata.is_file() {
                    return Ok(());
                }
            }
        }
        if daemon_lock_owner_alive(dir) {
            return Err(Error::new(
                "daemon_lock_held",
                "a live private daemon holds this data directory",
            ));
        }
        Ok(())
    }

    /// Classify one retirement artifact: the planning gate (quiescence,
    /// then the bound-worktree blocker) runs before any preflight, so a
    /// borrowed or assignment-bound worktree stays `keep` and its row is
    /// never fenced `unknown` across a restart. The apply-time recheck in
    /// `cleanup_remove` stays as the second gate.
    async fn classify_retirement_artifact(&self, artifact: ArtifactView) -> CleanupCandidate {
        let mut candidate = CleanupCandidate {
            artifact,
            bytes: None,
            verdict: "keep".to_owned(),
            reason: "retirement_preflight".to_owned(),
        };
        if candidate.artifact.kind == ArtifactKind::Worktree {
            self.classify_worktree(&mut candidate, None).await;
            return candidate;
        }
        let gate = self
            .inner
            .store
            .lock()
            .await
            .retirement_plan_gate(&candidate.artifact);
        match gate {
            Ok(None) => match self.cleanup_preflight(&candidate.artifact).await {
                Ok(bytes) => {
                    candidate.verdict = "safe".to_owned();
                    candidate.reason = "retirement_owned_artifact".to_owned();
                    candidate.bytes = Some(bytes);
                }
                Err(error) => candidate.reason = error.code,
            },
            Ok(Some(reason)) => candidate.reason = reason.to_owned(),
            Err(error) => candidate.reason = error.code,
        }
        candidate
    }

    /// Retire one assignment Agent's build and temp directories and managed
    /// worktrees after its stop is applied, when the assignment opts in with
    /// `cleanup_on_retire`. This runs inline in the existing stop worker, not
    /// in a second executor: same authority check, same quiescence gate,
    /// same guarded deletion and the same retained receipts as an explicit
    /// `cleanup.plan` + `cleanup.apply` pair. Best effort: a gated-off or
    /// refused item simply stays registered.
    pub(super) async fn retire_assignment_cleanup(&self, agent_id: &str, generation: i64) {
        let key = match self
            .inner
            .store
            .lock()
            .await
            .assignment_key_for_agent(agent_id, generation)
        {
            Ok(key) => key,
            Err(_) => return,
        };
        let Some(key) = key else { return };
        let enabled = crate::config::load_automation_config(&self.inner.data_root)
            .map(|config| {
                config
                    .assignments
                    .get(&key)
                    .is_some_and(|assignment| assignment.cleanup_on_retire)
            })
            .unwrap_or(false);
        if !enabled {
            return;
        }
        let agent = match self.inner.store.lock().await.get_agent(agent_id) {
            Ok(Some(agent)) if agent.generation == generation => agent,
            _ => return,
        };
        if self.record_agent_artifacts(&agent).await.is_err() {
            // Backfill is best effort; registered artifacts still clean up.
        }
        let (inventory, retirement_truncated) = match self
            .inner
            .store
            .lock()
            .await
            .cleanup_retirement_inventory(&agent)
        {
            Ok((artifacts, truncated)) => (artifacts, truncated),
            Err(_) => return,
        };
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: false,
            continuation: None,
        };
        plan.truncated |= retirement_truncated;
        for artifact in inventory {
            if plan.items.len() >= MAX_CLEANUP_ITEMS {
                plan.truncated = true;
                break;
            }
            plan.items
                .push(self.classify_retirement_artifact(artifact).await);
        }
        if !plan.items.iter().any(|item| item.verdict == "safe") {
            return;
        }
        if self
            .inner
            .store
            .lock()
            .await
            .retain_cleanup_plan("owner", &plan, now_ms())
            .is_err()
        {
            return;
        }
        let apply = CleanupApplyRequest {
            id: ulid::Ulid::new().to_string(),
            plan_id: plan.plan_id.clone(),
            delete_remote: false,
            artifacts: None,
        };
        let (_, mut response, is_new) =
            match self
                .inner
                .store
                .lock()
                .await
                .begin_cleanup_apply("owner", &apply, now_ms())
            {
                Ok(applied) => applied,
                Err(_) => return,
            };
        if !is_new {
            return;
        }
        for (index, item) in plan.items.iter().enumerate() {
            if item.verdict != "safe" || response.items[index].outcome != "unknown" {
                continue;
            }
            let outcome = match self
                .cleanup_remove("owner", &item.artifact, &apply, false)
                .await
            {
                Ok(bytes) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    bytes_reclaimed: bytes,
                },
                Err(error) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: if error.code == "unknown" {
                        "unknown"
                    } else {
                        "skipped"
                    }
                    .to_owned(),
                    reason: Some(error.code),
                    bytes_reclaimed: 0,
                },
            };
            response.bytes_reclaimed += outcome.bytes_reclaimed;
            response.items[index] = outcome.clone();
            let _ = self.inner.store.lock().await.settle_cleanup_item(
                "owner",
                &apply,
                &response,
                &item.artifact,
                &outcome,
            );
        }
    }
}

/// Whether the `daemon.json` beside a present `daemon.lock` names a process
/// that is still alive. Unknown or unreadable metadata keeps: only a
/// provably dead owner releases the directory.
fn daemon_lock_owner_alive(dir: &Path) -> bool {
    let info = match std::fs::read(dir.join("daemon.json")) {
        Ok(bytes) => bytes,
        Err(_) => return true,
    };
    let pid: Option<u32> = serde_json::from_slice::<serde_json::Value>(&info)
        .ok()
        .and_then(|value| {
            value
                .get("pid")
                .and_then(serde_json::Value::as_u64)
                .and_then(|pid| u32::try_from(pid).ok())
        });
    match pid {
        Some(pid) if pid != 0 => daemon_pid_alive(pid),
        _ => true,
    }
}

fn daemon_pid_alive(pid: u32) -> bool {
    crate::platform::lock_owner_process_alive(pid)
}
