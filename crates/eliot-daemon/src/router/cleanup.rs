//! Bounded artifact cleanup through the authenticated Router and Store writer.
use super::*;
use crate::git_monitor::GitBinding;
#[cfg(windows)]
use crate::platform::script_file::CleanupTree;
use crate::platform::script_file::directory_identity;
use crate::workspace_git::containment::{BranchClassification, classify_branch};
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

#[expect(
    clippy::disallowed_methods,
    reason = "Recursive directory removal is centralized in daemon cleanup."
)]
pub(crate) fn remove_dir_all(path: &Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(path)
}

#[derive(Default)]
pub(super) struct AutomaticCleanupState {
    kept_reasons: HashMap<String, (ArtifactKind, String, Option<u64>)>,
    owner_decision_worktrees: usize,
    first_sweep_complete: bool,
    active_sweep: Option<AutomaticCleanupProgress>,
    last_sweep: Option<eliot_core::api::AutomaticCleanupSweep>,
}

#[derive(Default)]
struct AutomaticCleanupProgress {
    summary: eliot_core::api::AutomaticCleanupSweep,
    owner_decision_worktrees: usize,
}
const SWARM_PLUGIN_INSTANCE_PURPOSE: &str = "Swarm plugin instance directory";
const SWARM_ARTIFACT_ROOT_PURPOSE: &str = "Swarm artifact root";

fn is_stand_artifact(artifact: &ArtifactView) -> bool {
    artifact.kind == ArtifactKind::DataDir && artifact.purpose == SWARM_STAND_ARTIFACT_PURPOSE
}

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

fn check_worktree_binding<'a>(
    artifact: &'a ArtifactView,
    workspace: &eliot_core::WorkspacePreparation,
) -> Result<&'a CleanupGitBinding> {
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
    {
        return Err(Error::new("identity_changed", "worktree binding changed"));
    }
    Ok(git)
}

fn check_worktree_artifact(
    artifact: &ArtifactView,
    workspace: &eliot_core::WorkspacePreparation,
) -> Result<()> {
    let git = check_worktree_binding(artifact, workspace)?;
    if directory_identity(Path::new(&artifact.path))? != artifact.identity
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
    pub(super) fn automatic_cleanup_view(&self) -> (eliot_core::api::AutomaticCleanupView, usize) {
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
        let view = eliot_core::api::AutomaticCleanupView {
            policy,
            enabled,
            startup_plan_only_pending: !state.first_sweep_complete,
            after,
            config_error,
            last_sweep: state.last_sweep.clone(),
        };
        (view, state.owner_decision_worktrees)
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
                        if state.kept_reasons.get(&item.artifact.id).is_some_and(
                            |(prior_kind, prior_reason, prior_bytes)| {
                                *prior_kind == item.artifact.kind
                                    && prior_reason == reason
                                    && prior_bytes == &item.bytes
                            },
                        ) {
                            continue;
                        }
                    }
                    _ => sweep.unknown += 1,
                }
                keep_updates.push((
                    item.artifact.id.clone(),
                    (outcome == "kept")
                        .then(|| (item.artifact.kind, reason.to_owned(), item.bytes)),
                ));
                changed.push(json!({
                    "artifact_id": item.artifact.id,
                    "agent": item.artifact.agent,
                    "generation": item.artifact.generation,
                    "kind": item.artifact.kind,
                    "path": policy.redact_bounded(&item.artifact.path, 384),
                    "outcome": outcome,
                    "reason": reason,
                    "bytes": item.bytes,
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
        for (id, retained) in keep_updates {
            if let Some(retained) = retained {
                // Keep only one reason per retained artifact, with a bound
                // matching the size of the artifact ledger.
                if state.kept_reasons.len() >= 4096 && !state.kept_reasons.contains_key(&id) {
                    state.kept_reasons.clear();
                }
                state.kept_reasons.insert(id, retained);
            } else {
                state.kept_reasons.remove(&id);
            }
        }
        Ok(sweep)
    }

    fn owner_decision_worktree_page_count(
        plan: &CleanupPlan,
        applied: Option<&CleanupApplyResponse>,
    ) -> usize {
        plan.items
            .iter()
            .filter(|item| {
                if item.artifact.kind != ArtifactKind::Worktree {
                    return false;
                }
                if item.verdict != "safe" {
                    return Self::owner_decision_reason(&item.reason);
                }
                applied
                    .and_then(|response| {
                        response
                            .items
                            .iter()
                            .find(|outcome| outcome.artifact_id == item.artifact.id)
                    })
                    .is_some_and(|outcome| {
                        outcome.outcome == "skipped"
                            && outcome
                                .reason
                                .as_deref()
                                .is_some_and(Self::owner_decision_reason)
                    })
            })
            .count()
    }

    fn owner_decision_reason(reason: &str) -> bool {
        matches!(reason, "dirty" | "unmerged" | "untracked_or_ignored")
    }

    fn accumulate_automatic_cleanup_page(
        &self,
        page: AutomaticCleanupSweep,
        first_page: bool,
        complete: bool,
        owner_decision_worktrees: usize,
    ) -> Option<AutomaticCleanupSweep> {
        let mut state = self.inner.automatic_cleanup.lock_recover();
        if first_page {
            state.active_sweep = Some(AutomaticCleanupProgress {
                summary: AutomaticCleanupSweep {
                    plan_only: true,
                    ..Default::default()
                },
                owner_decision_worktrees: 0,
            });
        }
        let progress = state
            .active_sweep
            .get_or_insert_with(|| AutomaticCleanupProgress {
                summary: AutomaticCleanupSweep {
                    plan_only: true,
                    ..Default::default()
                },
                owner_decision_worktrees: 0,
            });
        progress.summary.finished_at = page.finished_at;
        progress.summary.enabled = page.enabled;
        progress.summary.plan_only &= page.plan_only;
        progress.summary.would_remove += page.would_remove;
        progress.summary.removed += page.removed;
        progress.summary.kept += page.kept;
        progress.summary.unknown += page.unknown;
        progress.owner_decision_worktrees = progress
            .owner_decision_worktrees
            .saturating_add(owner_decision_worktrees);
        if !complete {
            return None;
        }
        let summary = progress.summary.clone();
        state.owner_decision_worktrees = progress.owner_decision_worktrees;
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
                expires_at: None,
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
            expires_at: None,
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
        match self.backfill_agent_artifacts(agent).await? {
            Some((_, error)) if error.code != "already_absent" => Err(error),
            _ => Ok(()),
        }
    }

    async fn backfill_agent_artifacts(&self, agent: &Agent) -> Result<Option<(String, Error)>> {
        let workspace = self.inner.store.lock().await.get_workspace(&agent.id)?;
        let mut known = Vec::new();
        let mut issue = None;
        if let Some(workspace) = workspace
            && workspace.target_generation == agent.generation
            && (workspace.phase == WorkspacePhase::Ready
                || workspace.phase == WorkspacePhase::Failed
                    && std::fs::symlink_metadata(&workspace.worktree_path).is_ok())
        {
            let path = workspace.worktree_path.clone();
            if let Err(error) = self
                .collect_worktree_artifacts(agent, workspace, &mut known)
                .await
            {
                issue = Some((path, error));
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
                        expires_at: None,
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
                    (Err(error), Ok(_)) if error.kind() == std::io::ErrorKind::NotFound => true,
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
                    expires_at: None,
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
        Ok(issue)
    }

    async fn collect_worktree_artifacts(
        &self,
        agent: &Agent,
        workspace: eliot_core::WorkspacePreparation,
        known: &mut Vec<ArtifactView>,
    ) -> Result<()> {
        match std::fs::symlink_metadata(&workspace.worktree_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::new(
                    "already_absent",
                    "managed worktree directory is absent",
                ));
            }
            Err(error) => {
                return Err(Error::new(
                    "platform_io",
                    format!("inspect managed worktree: {error}"),
                ));
            }
            Ok(_) => {}
        }
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
        let admin = std::fs::canonicalize(admin)
            .map_err(|_| Error::new("unavailable", "worktree administration path unavailable"))?;
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
            expires_at: None,
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
                expires_at: None,
                state: "registered".to_owned(),
                git: Some(git),
            });
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

    async fn automatic_cleanup_keep_reason(
        &self,
        artifact: &ArtifactView,
        retention_ms: i64,
        now: i64,
        allow_fenced_root: bool,
    ) -> Result<Option<String>> {
        let (agent, ended_at) = {
            let store = self.inner.store.lock().await;
            let Some(agent) = store.get_agent(&artifact.agent)? else {
                return Ok(Some("agent_missing".to_owned()));
            };
            if agent.generation != artifact.generation {
                return Ok(Some("stale_generation".to_owned()));
            }
            if !agent.state.is_terminal() {
                return Ok(Some("agent_not_terminal".to_owned()));
            }
            let ended_at = store.terminal_agent_end_time(&agent)?;
            (agent, ended_at)
        };
        // An explicit expiry only extends retention; it never shortens it.
        if let Some(expires_at) = artifact.expires_at
            && now < expires_at
        {
            return Ok(Some(
                if is_stand_artifact(artifact) {
                    "stand_not_expired"
                } else {
                    "not_expired"
                }
                .to_owned(),
            ));
        }
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
        let mut request: CleanupPlanRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "cleanup.plan",
            CleanupPlanRequest::validate,
        )?;
        if request.scope == CleanupScope::Default {
            request.scope = if matches!(principal, Principal::Owner) {
                CleanupScope::All
            } else {
                CleanupScope::SelfTree
            };
        }
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
            inventory_issues: Vec::new(),
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
                .backfill_cleanup_inventory(
                    &name,
                    &request,
                    page.backfill_after,
                    &mut plan.inventory_issues,
                )
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
            let mut retention_reason = None;
            if let Some(retention_ms) = auto_cleanup_after_ms {
                match self
                    .automatic_cleanup_keep_reason(
                        &candidate.artifact,
                        retention_ms,
                        now_ms(),
                        false,
                    )
                    .await
                {
                    Ok(Some(reason)) => {
                        if auto_cleanup
                            && candidate.artifact.kind == ArtifactKind::Worktree
                            && reason == "retention_age"
                        {
                            // Classify age-ineligible worktrees too, so dirty
                            // and unmerged work remains visible with its size.
                            retention_reason = Some(reason);
                        } else {
                            candidate.reason = reason;
                            middle.push(candidate);
                            continue;
                        }
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
                self.classify_worktree(
                    &mut candidate,
                    auto_cleanup,
                    !auto_cleanup && matches!(principal, Principal::Owner),
                )
                .await;
                if candidate.verdict == "safe"
                    && let Some(reason) = retention_reason
                {
                    candidate.verdict = "keep".to_owned();
                    candidate.reason = reason;
                    candidate.bytes = self.cleanup_worktree_bytes(&candidate.artifact).await;
                }
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
                    .cleanup_branch_tip(
                        &candidate.artifact,
                        git,
                        Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT,
                    )
                    .await
                {
                    Ok(tip) => {
                        candidate.artifact.identity = tip;
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = "local_branch_retirement".to_owned();
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
                    Ok((bytes, absent)) => {
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = if absent {
                            "already_absent"
                        } else if ownership_root {
                            "terminal_owned_artifact_root"
                        } else if is_stand_artifact(&candidate.artifact) {
                            "expired_stand"
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
        issues: &mut Vec<CleanupInventoryIssue>,
    ) -> Result<Option<String>> {
        let deadline = Instant::now() + CLEANUP_BACKFILL_BUDGET;
        let secrets = self.secret_policy_snapshot();
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
            let results =
                futures_util::future::join_all(eligible.into_iter().map(|agent| async move {
                    (
                        agent,
                        timeout_at(deadline, self.backfill_agent_artifacts(agent)).await,
                    )
                }))
                .await;
            for (agent, result) in results {
                let (path, error) = match result {
                    Ok(Ok(None)) => continue,
                    Ok(Ok(Some((path, error)))) => (Some(path), error),
                    Ok(Err(error)) => return Err(error),
                    Err(_) => (
                        agent.workspace.clone(),
                        Error::new("inventory_timeout", "artifact inventory probe timed out"),
                    ),
                };
                issues.push(CleanupInventoryIssue {
                    agent: agent.id.clone(),
                    generation: agent.generation,
                    path: path.map(|path| secrets.redact_bounded(&path, 4096)),
                    reason: error.code,
                    detail: secrets.redact_bounded(&error.message, 512),
                });
            }
            if !truncated && agents.len() < CLEANUP_BACKFILL_BATCH {
                return Ok(None);
            }
            after = agents.last().map(|agent| agent.id.clone()).or(after);
            if Instant::now() >= deadline
                || issues.len() > MAX_CLEANUP_ITEMS - CLEANUP_BACKFILL_BATCH
            {
                break;
            }
        }
        Ok(after)
    }

    async fn classify_worktree(
        &self,
        candidate: &mut CleanupCandidate,
        require_merged: bool,
        observe_missing: bool,
    ) {
        if candidate.artifact.state != "registered" {
            candidate.reason = candidate.artifact.state.clone();
            return;
        }
        let preflight = self
            .worktree_cleanup_preflight(&candidate.artifact, require_merged, observe_missing)
            .await;
        match preflight {
            Ok(None) => {
                candidate.verdict = "safe".to_owned();
                candidate.reason = "already_absent".to_owned();
                candidate.bytes = Some(0);
            }
            Ok(Some(tip)) => {
                if require_merged
                    && let Err(error) = self
                        .automatic_cleanup_worktree_merged(&candidate.artifact, &tip)
                        .await
                {
                    candidate.reason = error.code;
                    candidate.bytes = self.cleanup_worktree_bytes(&candidate.artifact).await;
                    return;
                }
                if let Some(git) = candidate.artifact.git.as_mut() {
                    git.planned_tip = Some(tip);
                }
                candidate.verdict = "safe".to_owned();
                candidate.reason = "clean_worktree".to_owned();
            }
            Err(error) => {
                candidate.reason = if require_merged
                    && error.code == "not_allowed"
                    && error.message.as_ref()
                        == "untracked or ignored file lies outside allowed scratch"
                {
                    "untracked_or_ignored".to_owned()
                } else {
                    error.code
                };
                candidate.bytes = self.cleanup_worktree_bytes(&candidate.artifact).await;
            }
        }
    }

    async fn cleanup_worktree_bytes(&self, artifact: &ArtifactView) -> Option<u64> {
        let path = self.current_managed_worktree_path(&artifact.path).ok()?;
        let identity = artifact.identity.clone();
        tokio::task::spawn_blocking(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            super::status::disk_directory_bytes(&path, Some(&identity), deadline).ok()
        })
        .await
        .ok()
        .flatten()
    }

    /// The automatic retention worker requires a fresh merged-branch proof.
    /// Owner cleanup plans remain governed by the clean-worktree rule alone.
    async fn automatic_cleanup_worktree_merged(
        &self,
        artifact: &ArtifactView,
        expected_tip: &str,
    ) -> Result<()> {
        let preparation = self
            .inner
            .store
            .lock()
            .await
            .workspace_removal_eligibility(&artifact.agent, now_ms())?;
        check_worktree_artifact(artifact, &preparation)?;
        crate::workspace_git::WorkspaceGit::new()?
            .require_automatic_removal_merge(
                &preparation,
                expected_tip,
                Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT,
            )
            .await
    }

    async fn worktree_cleanup_preflight(
        &self,
        artifact: &ArtifactView,
        automatic: bool,
        observe_missing: bool,
    ) -> Result<Option<String>> {
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
        if observe_missing
            && crate::workspace_git::observe_worktree_absence(Path::new(&artifact.path))?.is_some()
        {
            check_worktree_binding(artifact, &preparation)?;
            return Ok(None);
        }
        check_worktree_artifact(artifact, &preparation)?;
        let git = crate::workspace_git::WorkspaceGit::new()?;
        let removal_profile = if automatic {
            serde_json::json!({"scratch": []})
        } else {
            profile
        };
        let preflight = self
            .inner
            .workspaces
            .removal_candidates(
                &git,
                &preparation,
                &removal_profile,
                None,
                Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT,
            )
            .await?;
        Ok(Some(preflight.tip))
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

    /// Reconcile disappearance independently of the retention cleanup policy.
    /// The ordinary removal command binds generation and rechecks absence.
    pub(crate) async fn reconcile_missing_worktrees(
        &self,
        after: &mut Option<String>,
        in_flight: &mut Option<String>,
    ) -> Result<()> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok(());
        }
        let (workspaces, _, more) = {
            let store = self.inner.store.lock().await;
            if let Some(id) = in_flight.as_deref()
                && store.get_command(id)?.is_some_and(|command| {
                    !command.state.is_terminal() && command.state != CommandState::Unknown
                })
            {
                return Ok(());
            }
            *in_flight = None;
            store.list_managed_workspaces(after.as_deref(), 64)?
        };
        for workspace in workspaces {
            *after = Some(workspace.agent.clone());
            if workspace.phase != WorkspacePhase::Ready
                || !matches!(
                    crate::workspace_git::observe_worktree_absence(Path::new(
                        &workspace.worktree_path
                    )),
                    Ok(Some(_))
                )
            {
                continue;
            }
            let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "worktree.remove");
            request.target = Some(workspace.agent);
            request.expected_generation = Some(workspace.target_generation);
            request.args = json!({"observe_missing": true});
            let admission = {
                let mut store = self.inner.store.lock().await;
                match self.admit_prepared_submission(
                    &mut store,
                    "owner",
                    PreparedSubmission {
                        request,
                        workspace: None,
                    },
                    SubmissionContext::default(),
                ) {
                    Ok(admission) => {
                        self.publish_locked(&admission.events);
                        admission
                    }
                    Err(error)
                        if matches!(
                            error.code.as_str(),
                            "busy" | "bad_state" | "not_found" | "invalid"
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            };
            *in_flight = Some(admission.command.id.clone());
            self.schedule_admission(&admission);
            self.signal_progress();
            return Ok(());
        }
        if !more {
            *after = None;
        }
        Ok(())
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
        let owner_decision_worktrees =
            Self::owner_decision_worktree_page_count(&plan, applied.as_ref());
        *after = plan.continuation.clone();
        if let Some(sweep) = self.accumulate_automatic_cleanup_page(
            page,
            first_page,
            plan.continuation.is_none(),
            owner_decision_worktrees,
        ) {
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
            .automatic_cleanup_keep_reason(artifact, age, now_ms(), true)
            .await?
        {
            return Err(Error::new(reason, "automatic cleanup eligibility changed"));
        }
        if artifact.kind == ArtifactKind::Worktree {
            let expected_tip = artifact
                .git
                .as_ref()
                .and_then(|git| git.planned_tip.as_deref())
                .ok_or_else(|| {
                    Error::new("source_integrity", "automatic worktree plan has no tip")
                })?;
            self.automatic_cleanup_worktree_merged(artifact, expected_tip)
                .await?;
        }
        Ok(())
    }

    fn cleanup_directory_absent(&self, artifact: &ArtifactView) -> Result<bool> {
        if is_stand_artifact(artifact) {
            return Ok(false);
        }
        let path = Path::new(&artifact.path);
        validate_cleanup_path(path)?;
        let mut missing = None;
        for candidate in path.ancestors() {
            match std::fs::symlink_metadata(candidate) {
                Ok(_) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    missing = Some(candidate)
                }
                Err(error) => {
                    return Err(Error::new(
                        "platform_io",
                        format!("inspect cleanup directory: {error}"),
                    ));
                }
            }
        }
        let Some(missing) = missing else {
            return Ok(false);
        };
        let canonical = eliot_core::verbatim_path(path);
        let data_root = self
            .inner
            .data_root
            .canonicalize()
            .map_err(|_| Error::new("unavailable", "daemon root unavailable"))?;
        if data_root.starts_with(&canonical) {
            return Err(Error::new(
                "not_allowed",
                "daemon data and configuration are protected",
            ));
        }
        require_cleanup_outside_native_home(&canonical)?;
        Ok(crate::workspace_git::observe_worktree_absence(missing)?.is_some())
    }

    async fn cleanup_preflight(&self, artifact: &ArtifactView) -> Result<(u64, bool)> {
        if artifact.kind == ArtifactKind::NativeSession {
            // The archive itself runs at apply through the harness native
            // API; the plan verdict already gated harness and quiescence.
            return Ok((0, false));
        }
        if artifact.kind == ArtifactKind::Branch {
            let git = artifact
                .git
                .as_ref()
                .ok_or_else(|| Error::new("invalid", "branch Git binding absent"))?;
            self.cleanup_branch_tip(artifact, git, Instant::now() + Duration::from_secs(30))
                .await?;
            return Ok((0, false));
        }
        if let Some(expires_at) = artifact.expires_at
            && now_ms() < expires_at
        {
            return Err(Error::new(
                if is_stand_artifact(artifact) {
                    "stand_not_expired"
                } else {
                    "not_expired"
                },
                "artifact has not expired",
            ));
        }
        if self.cleanup_directory_absent(artifact)? {
            return Ok((0, true));
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
        if is_stand_artifact(artifact) {
            return crate::stand_cleanup::plan_stand_artifact(
                &self.inner.data_root,
                &path,
                &artifact.identity,
            )
            .await
            .map(|bytes| (bytes, false));
        }
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
            Ok((tree.bytes, false))
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
                && item.reason != "already_absent"
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
                    Ok(()) => self.cleanup_remove(name, item, request, true).await,
                    Err(error) => Err(error),
                }
            } else {
                self.cleanup_remove(name, item, request, false).await
            };
            if automatic && let Err(error) = &removal {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.auto_cleanup_item_failed",
                    &[
                        ("artifact_id", &item.artifact.id),
                        ("agent", &item.artifact.agent),
                    ],
                    Some(error),
                );
            }
            let outcome = match removal {
                Ok(bytes) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: "removed".to_owned(),
                    reason: (item.reason == "already_absent").then(|| item.reason.clone()),
                    detail: None,
                    bytes_reclaimed: bytes,
                },
                Err(error) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    detail: (item.artifact.kind == ArtifactKind::Branch).then(|| {
                        let secrets: Vec<_> =
                            self.inner.secrets.lock_recover().iter().cloned().collect();
                        eliot_core::SecretPolicy::new(secrets).redact_bounded(&error.message, 512)
                    }),
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
        item: &CleanupCandidate,
        request: &CleanupApplyRequest,
        automatic: bool,
    ) -> Result<u64> {
        let artifact = &item.artifact;
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
            // Local retirement pins the current local tip. An explicit remote
            // deletion retains its existing fresh upstream and containment gate.
            let remote_classification = if request.delete_remote {
                Some(self.cleanup_branch_classification(artifact, git).await?)
            } else {
                None
            };
            let tip = if let Some(classification) = &remote_classification {
                classification.tip.as_str().to_owned()
            } else {
                self.cleanup_branch_tip(artifact, git, Instant::now() + Duration::from_secs(30))
                    .await?
            };
            if tip != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "branch tip moved since the plan was retained",
                ));
            }
            Self::delete_local_branch(git, &artifact.identity).await?;
            if let Some(classification) = remote_classification {
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
                .cleanup_remove_worktree(principal, item, &request.id, automatic)
                .await;
        }
        if let Some(expires_at) = artifact.expires_at
            && now_ms() < expires_at
        {
            return Err(Error::new(
                if is_stand_artifact(artifact) {
                    "stand_not_expired"
                } else {
                    "not_expired"
                },
                "artifact has not expired",
            ));
        }
        if self.cleanup_directory_absent(artifact)? {
            let store = self.inner.store.lock().await;
            if !store.cleanup_authorized(principal, artifact)?
                || !store.cleanup_agent_quiescent(artifact)?
            {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "cleanup authority or generation changed",
                ));
            }
            if self.cleanup_directory_absent(artifact)? {
                return Ok(0);
            }
        }
        if item.reason == "already_absent" {
            return Err(Error::new(
                "identity_changed",
                "artifact directory reappeared after planning",
            ));
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
        if is_stand_artifact(artifact) {
            Self::cleanup_check_daemon_lock(Path::new(&artifact.path))?;
            {
                let store = self.inner.store.lock().await;
                if !store.cleanup_authorized(principal, artifact)?
                    || !store.cleanup_agent_quiescent(artifact)?
                {
                    return Err(Error::new(
                        "agent_not_quiescent",
                        "cleanup authority or generation changed",
                    ));
                }
            }
            return crate::stand_cleanup::apply_stand_artifact(
                &self.inner.data_root,
                &path,
                &artifact.identity,
            )
            .await;
        }
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
        item: &CleanupCandidate,
        apply_id: &str,
        automatic: bool,
    ) -> Result<u64> {
        let artifact = &item.artifact;
        let mut progress = self.progress_receiver();
        let admission = {
            let mut store = self.inner.store.lock().await;
            if !store.cleanup_authorized(principal, artifact)?
                || !store.cleanup_agent_quiescent(artifact)?
            {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "worktree authority changed",
                ));
            }
            let preparation = store.workspace_removal_eligibility(&artifact.agent, now_ms())?;
            let id = eliot_store::cleanup_worktree_command_id(apply_id, &artifact.id)?;
            let mut request = SubmitCommand::new(id, "worktree.remove");
            request.target = Some(artifact.agent.clone());
            request.expected_generation = Some(artifact.generation);
            request.args = if item.reason == "already_absent" {
                let git = check_worktree_binding(artifact, &preparation)?;
                match std::fs::symlink_metadata(&git.admin) {
                    Ok(_) if directory_identity(Path::new(&git.admin))? != git.admin_identity => {
                        return Err(Error::new(
                            "identity_changed",
                            "worktree administration identity changed",
                        ));
                    }
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        return Err(Error::new(
                            "platform_io",
                            format!("inspect worktree administration: {error}"),
                        ));
                    }
                    _ => {}
                }
                json!({"observe_missing": true})
            } else {
                check_worktree_artifact(artifact, &preparation)?;
                let tip = artifact
                    .git
                    .as_ref()
                    .and_then(|git| git.planned_tip.as_ref())
                    .ok_or_else(|| {
                        Error::new("source_integrity", "worktree cleanup plan has no tip")
                    })?;
                if automatic {
                    json!({"expected_tip": tip, "cleanup_mode": "automatic"})
                } else {
                    json!({"expected_tip": tip})
                }
            };
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
                        let refusal = command
                            .outcome
                            .as_ref()
                            .map(|outcome| outcome.get("result").unwrap_or(outcome));
                        let code = refusal
                            .and_then(|outcome| outcome.get("code"))
                            .and_then(Value::as_str)
                            .unwrap_or("worktree_remove_refused");
                        let message = refusal
                            .and_then(|outcome| outcome.get("message"))
                            .and_then(Value::as_str)
                            .unwrap_or("worktree removal settled without a recorded reason");
                        let error = Error::new(code, message);
                        return Err(
                            match refusal.and_then(|outcome| outcome["where"].as_str()) {
                                Some(location) => error.with_where(location),
                                None => error,
                            },
                        );
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
            Instant::now() + Duration::from_secs(60),
        )
        .await
    }

    async fn cleanup_branch_tip(
        &self,
        artifact: &ArtifactView,
        git: &CleanupGitBinding,
        deadline: Instant,
    ) -> Result<String> {
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
        Self::pin_local_branch_tip(&git.repo, Path::new(&git.common), &git.branch, deadline).await
    }

    /// Pin a local branch tip for retirement without an upstream containment
    /// scan. The archive ref collision check is local and never contacts a remote.
    pub(crate) async fn pin_local_branch_tip(
        repo: &str,
        common: &Path,
        branch: &str,
        deadline: Instant,
    ) -> Result<String> {
        if !crate::git_actions::valid_cleanup_branch(branch) {
            return Err(Error::new(
                "invalid_branch",
                "branch name is not admissible",
            ));
        }
        let probe = crate::git_monitor::GitProbe::new()?;
        let branch_ref = format!("refs/heads/{branch}");
        let archive_ref = format!("refs/eliot/archive/{branch}");
        let tip = Self::cleanup_ref_oid(&probe, repo, common, &branch_ref, deadline)
            .await?
            .ok_or_else(|| Error::new("branch_absent", "local branch is absent"))?;
        if Self::cleanup_ref_oid(&probe, repo, common, &archive_ref, deadline)
            .await?
            .is_some_and(|archived| archived != tip)
        {
            return Err(Error::new(
                "archive_collision",
                "local branch archive ref already names a different tip",
            ));
        }
        Ok(tip)
    }

    /// Atomically create or verify the local archive ref and CAS-delete the
    /// branch ref. Checkout guards and the plan's pinned local tip are checked
    /// immediately before each transaction.
    pub(crate) async fn delete_local_branch(
        git: &CleanupGitBinding,
        expected_tip: &str,
    ) -> Result<()> {
        if !crate::git_actions::valid_cleanup_branch(&git.branch) {
            return Err(Error::new(
                "invalid_branch",
                "branch name is not admissible",
            ));
        }
        if !eliot_core::api::is_git_oid(expected_tip) {
            return Err(Error::new("invalid_branch", "branch tip is invalid"));
        }
        let repo = git.repo.as_str();
        let common = Path::new(&git.common);
        let probe = crate::git_monitor::GitProbe::new()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let branch_ref = format!("refs/heads/{}", git.branch);
        let archive_ref = format!("refs/eliot/archive/{}", git.branch);
        let branch_tip = Self::cleanup_ref_oid(&probe, repo, common, &branch_ref, deadline).await?;
        let archive_tip =
            Self::cleanup_ref_oid(&probe, repo, common, &archive_ref, deadline).await?;
        if archive_tip
            .as_deref()
            .is_some_and(|archived| archived != expected_tip)
        {
            return Err(Error::new(
                "archive_collision",
                "local branch archive ref already names a different tip",
            ));
        }
        match branch_tip.as_deref() {
            Some(tip) if tip != expected_tip => {
                return Err(Error::new(
                    "identity_changed",
                    "branch tip moved since planning",
                ));
            }
            None if archive_tip.as_deref() == Some(expected_tip) => return Ok(()),
            None => return Err(Error::new("branch_absent", "local branch is absent")),
            Some(_) => {}
        }
        Self::ensure_branch_detached(&probe, repo, common, &git.branch, deadline).await?;
        let archive = match archive_tip {
            Some(_) => format!("verify {archive_ref} {expected_tip}"),
            None => format!("create {archive_ref} {expected_tip}"),
        };
        let transaction = format!(
            "start\noption no-deref\n{archive}\noption no-deref\ndelete {branch_ref} {expected_tip}\nprepare\ncommit\n"
        );
        let refusal =
            Self::run_branch_ref_transaction(&probe, repo, common, &transaction, deadline).await?;
        let unknown = || {
            Error::new(
                "unknown",
                "exact local branch retirement could not be confirmed",
            )
        };
        let branch_after = Self::cleanup_ref_oid(&probe, repo, common, &branch_ref, deadline)
            .await
            .map_err(|_| unknown())?;
        let archive_after = Self::cleanup_ref_oid(&probe, repo, common, &archive_ref, deadline)
            .await
            .map_err(|_| unknown())?;
        if archive_after.as_deref() == Some(expected_tip) && branch_after.is_none() {
            return Ok(());
        }
        if let Some(refusal) = refusal
            && branch_after == branch_tip
            && archive_after == archive_tip
        {
            // Git is run with LC_ALL=C. This D/F conflict includes the
            // blocking ref and the exact archive ref Git tried to create.
            let conflict = format!("' exists; cannot create '{archive_ref}'");
            if let Some((before, _)) = refusal.split_once(&conflict)
                && let Some((_, blocking_ref)) = before.rsplit_once(": '")
            {
                return Err(Error::new(
                    "archive_collision",
                    format!(
                        "Git could not create the archive because {blocking_ref} already exists"
                    ),
                ));
            }
            return Err(Error::new("branch_delete_refused", refusal));
        }
        Err(unknown())
    }

    async fn ensure_branch_detached(
        probe: &crate::git_monitor::GitProbe,
        repo: &str,
        common: &Path,
        branch: &str,
        deadline: Instant,
    ) -> Result<()> {
        let binding = GitBinding {
            git_dir: common,
            common_dir: common,
        };
        let worktrees = probe
            .workspace_command_bound(
                Path::new(repo),
                &["worktree", "list", "--porcelain", "-z"],
                binding,
                deadline,
            )
            .await
            .map_err(|_| Error::new("branch_read_failed", "worktree attachments are unreadable"))?;
        if worktrees.code != 0 {
            return Err(Error::new(
                "branch_read_failed",
                "worktree attachments are unreadable",
            ));
        }
        let attached = format!("branch refs/heads/{branch}");
        if worktrees
            .stdout
            .split(|byte| *byte == 0)
            .any(|field| field == attached.as_bytes())
        {
            return Err(Error::new(
                "branch_checked_out",
                "branch is checked out in a live worktree",
            ));
        }
        Ok(())
    }

    async fn cleanup_ref_oid(
        probe: &crate::git_monitor::GitProbe,
        repo: &str,
        common: &Path,
        reference: &str,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let binding = GitBinding {
            git_dir: common,
            common_dir: common,
        };
        let root = Path::new(repo);
        let output = probe
            .workspace_command_bound(
                root,
                &[
                    "for-each-ref",
                    "--format=%(refname)%00%(objectname)%00%(symref)",
                    reference,
                ],
                binding,
                deadline,
            )
            .await
            .map_err(|_| Error::new("branch_read_failed", "Git ref could not be read"))?;
        if output.code != 0 {
            return Err(Error::new(
                "branch_read_failed",
                "Git ref could not be read",
            ));
        }
        for line in output.stdout.split(|byte| *byte == b'\n') {
            let mut fields = line
                .strip_suffix(b"\r")
                .unwrap_or(line)
                .split(|byte| *byte == 0);
            if fields.next() != Some(reference.as_bytes()) {
                continue;
            }
            let oid = fields
                .next()
                .and_then(|field| std::str::from_utf8(field).ok())
                .filter(|oid| eliot_core::api::is_git_oid(oid))
                .ok_or_else(|| Error::new("branch_read_failed", "Git ref object id is invalid"))?;
            if fields.next() != Some(b"") {
                return Err(Error::new(
                    "branch_read_failed",
                    "branch retirement requires direct Git refs",
                ));
            }
            return Ok(Some(oid.to_owned()));
        }
        Ok(None)
    }

    /// Return a known Git refusal only for a quiescent nonzero exit.
    /// The caller must read the refs to classify the effect.
    async fn run_branch_ref_transaction(
        probe: &crate::git_monitor::GitProbe,
        repo: &str,
        common: &Path,
        transaction: &str,
        deadline: Instant,
    ) -> Result<Option<String>> {
        use crate::scripts::process::{ProcessFailure, ProcessOutcome};
        let binding = GitBinding {
            git_dir: common,
            common_dir: common,
        };
        let command = probe
            .prepare_workspace_command(
                Path::new(repo),
                &["update-ref", "--stdin"],
                binding,
                deadline,
            )
            .await?;
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            return Err(Error::new(
                "unavailable",
                "branch retirement deadline elapsed",
            ));
        }
        let (_keep_open, cancellation) = tokio::sync::watch::channel(false);
        match crate::scripts::process::run_owned_process(
            command,
            transaction.as_bytes().to_vec(),
            timeout,
            std::sync::Arc::new(eliot_core::SecretPolicy::default()),
            cancellation,
        )
        .await
        {
            ProcessOutcome::Completed(capture) if capture.status.success() => Ok(None),
            ProcessOutcome::Completed(capture)
            | ProcessOutcome::Failed {
                reason: ProcessFailure::Exit,
                capture: Some(capture),
            } => {
                let stderr = capture.stderr.text.trim();
                let detail = stderr
                    .lines()
                    .find(|line| line.starts_with("fatal:"))
                    .unwrap_or(stderr);
                Ok(Some(if detail.is_empty() {
                    format!("Git update-ref exited with {}", capture.status)
                } else {
                    eliot_core::SecretPolicy::default().redact_bounded(detail, 1024)
                }))
            }
            ProcessOutcome::Failed {
                reason: ProcessFailure::Exit,
                capture: None,
            } => Ok(Some("Git update-ref exited unsuccessfully".to_owned())),
            ProcessOutcome::NotSpawned(error) | ProcessOutcome::ProgramUnrunnable(error) => {
                Err(error)
            }
            ProcessOutcome::Unknown {
                quiescent: false, ..
            } => Err(Error::new(
                "unknown",
                "local branch retirement process custody is uncertain",
            )),
            _ => Ok(None),
        }
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
    if eliot_core::api::HarnessCapabilities::of(&agent.harness).native_archive
        == Some(eliot_core::api::harness::NativeArchiveBinding::FrozenStore)
    {
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
    eliot_core::api::HarnessCapabilities::of(harness)
        .native_archive
        .is_some()
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
        expires_at: None,
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
            self.classify_worktree(&mut candidate, false, true).await;
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
                Ok((bytes, absent)) => {
                    candidate.verdict = "safe".to_owned();
                    candidate.reason = if absent {
                        "already_absent"
                    } else {
                        "retirement_owned_artifact"
                    }
                    .to_owned();
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
            inventory_issues: Vec::new(),
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
            let outcome = match self.cleanup_remove("owner", item, &apply, false).await {
                Ok(bytes) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    detail: None,
                    bytes_reclaimed: bytes,
                },
                Err(error) => CleanupItemOutcome {
                    artifact_id: item.artifact.id.clone(),
                    detail: None,
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
