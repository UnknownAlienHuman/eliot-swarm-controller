//! Bounded artifact cleanup through the authenticated Router and Store writer.
use super::*;
#[cfg(windows)]
use crate::platform::script_file::CleanupTree;
use crate::platform::script_file::directory_identity;
use eliot_core::FailureOrigin;
use eliot_core::api::*;
use std::path::{Component, Path};
use tokio::time::timeout;

mod delete;
mod projects;

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
        let own_temp = PathBuf::from(&owned.path);
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
        for env in [
            "USERPROFILE",
            "HOME",
            "CODEX_HOME",
            "CODEX_SQLITE_HOME",
            "CLAUDE_CONFIG_DIR",
        ] {
            if let Some(home) = std::env::var_os(env).and_then(|p| std::fs::canonicalize(p).ok())
                && (canonical == home
                    || home.starts_with(&canonical)
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
        let request: CleanupPlanRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "cleanup.plan",
            CleanupPlanRequest::validate,
        )?;
        let name = principal_name(principal);
        let continued = request
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
            });
        // Backfill only facts already retained by Swarm, not arbitrary files.
        // Every page resumes after the previous backfill position instead of
        // restarting from the first row, so a persistently capped fleet
        // still terminates instead of repeating the same empty page.
        let (agents, agents_truncated) = self
            .inner
            .store
            .lock()
            .await
            .list_agents(continued.backfill_after.as_deref(), 1000)?;
        let mut inventory_incomplete = agents_truncated || agents.len() == 1000;
        let mut backfill_after = continued.backfill_after.clone();
        for agent in &agents {
            backfill_after = Some(agent.id.clone());
            if request
                .project
                .as_ref()
                .is_some_and(|p| p != &agent.project)
            {
                continue;
            }
            let allowed = self
                .inner
                .store
                .lock()
                .await
                .cleanup_visible_agent(&name, agent)?;
            if allowed {
                inventory_incomplete |= self.record_agent_artifacts(agent).await.is_err();
            }
        }
        if !inventory_incomplete {
            backfill_after = None;
        }
        // The Store pages the final candidate order once: ordinary rows,
        // then worktrees, then branches and native sessions, each oldest
        // insertion first. The router never re-sorts; it only merges the
        // ephemeral project rows around the pinned artifact page.
        let (artifacts, artifacts_truncated, snapshot_seq) = self
            .inner
            .store
            .lock()
            .await
            .cleanup_inventory(&name, &request)?;
        let page = continued;
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: artifacts_truncated || inventory_incomplete,
            continuation: None,
        };
        let (safe_projects, kept_projects, regs_revision): (Vec<_>, Vec<_>, Option<String>) =
            if matches!(principal, Principal::Owner) {
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
            if matches!(principal, Principal::Owner) {
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
        let artifacts_total = artifacts.len();
        let mut middle = Vec::new();
        for artifact in artifacts
            .into_iter()
            .take(MAX_CLEANUP_ITEMS - shown_safe.len())
        {
            let mut candidate = CleanupCandidate {
                artifact,
                bytes: None,
                verdict: "keep".to_owned(),
                reason: "agent_not_quiescent".to_owned(),
            };
            if candidate.artifact.state != "registered" {
                candidate.reason = candidate.artifact.state.clone();
            } else if candidate.artifact.kind == ArtifactKind::Worktree {
                self.classify_worktree(&mut candidate).await;
            } else if candidate.artifact.kind == ArtifactKind::Branch {
                let Some(git) = candidate.artifact.git.as_ref() else {
                    candidate.reason = "source_integrity".to_owned();
                    middle.push(candidate);
                    continue;
                };
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
                    .cleanup_branch_classification(&candidate.artifact, git)
                    .await
                {
                    Ok(classification) => {
                        // Anchor the freshly classified tip in the snapshot:
                        // the stored row may predate the latest commit, and
                        // the worktree may be gone before apply verifies.
                        candidate.artifact.identity = classification.tip.clone();
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = classification.reason.to_owned();
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
                match self.cleanup_preflight(&candidate.artifact).await {
                    Ok(bytes) => {
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = "terminal_owned_artifact".to_owned();
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
        plan.truncated |= artifact_listing_truncated(
            artifacts_truncated,
            inventory_incomplete,
            artifacts_total,
            middle_len,
            safe_total,
            page.safe_skipped + safe_shown,
            kept_total,
            page.kept_skipped + kept_shown,
        );
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
            plan.continuation = Some(
                PlanContinuation {
                    artifact_after: last_artifact,
                    safe_skipped: page.safe_skipped + safe_shown,
                    kept_skipped: page.kept_skipped + kept_shown,
                    regs_revision,
                    snapshot_seq: Some(snapshot_seq),
                    safe_ids,
                    kept_ids,
                    backfill_after,
                }
                .encode()?,
            );
        }
        self.inner
            .store
            .lock()
            .await
            .retain_cleanup_plan(&name, &plan, now_ms())?;
        encoded(plan)
    }

    async fn classify_worktree(&self, candidate: &mut CleanupCandidate) {
        if candidate.artifact.state != "registered" {
            candidate.reason = candidate.artifact.state.clone();
            return;
        }
        match self.worktree_cleanup_preflight(&candidate.artifact).await {
            Ok(()) => {
                candidate.verdict = "safe".to_owned();
                candidate.reason = "merged_quiet_worktree".to_owned();
            }
            Err(error) => candidate.reason = error.code,
        }
    }

    async fn worktree_cleanup_preflight(&self, artifact: &ArtifactView) -> Result<()> {
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
        self.inner
            .workspaces
            .removal_candidates(&git, &preparation, &profile)
            .await?;
        Ok(())
    }

    pub(crate) async fn auto_cleanup_worktrees(&self, after: &mut Option<String>) {
        let inventory = self
            .inner
            .store
            .lock()
            .await
            .auto_cleanup_worktree_inventory(after.as_deref());
        let Ok((artifacts, more)) = inventory else {
            *after = None;
            return;
        };
        *after = if more {
            artifacts.last().map(|artifact| artifact.id.clone())
        } else {
            None
        };
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: false,
            continuation: None,
        };
        for artifact in artifacts {
            let mut candidate = CleanupCandidate {
                artifact,
                bytes: None,
                verdict: "keep".to_owned(),
                reason: "worktree_preflight".to_owned(),
            };
            self.classify_worktree(&mut candidate).await;
            if candidate.verdict == "safe" {
                plan.items.push(candidate);
            }
        }
        if plan.items.is_empty()
            || !crate::config::load_automation_config(&self.inner.data_root)
                .is_ok_and(|config| config.auto_cleanup)
        {
            return;
        }
        let retained = self
            .inner
            .store
            .lock()
            .await
            .retain_cleanup_plan("owner", &plan, now_ms());
        if retained.is_ok() {
            let request = json!({"id": ulid::Ulid::new().to_string(), "plan_id": plan.plan_id});
            let _ = self.cleanup_apply(&request, &Principal::Owner).await;
        }
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
        let path = self.checked_artifact_path(&agent, &artifact.path).await?;
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
        self.apply_cleanup_artifacts(&name, &request, &plan, &mut response)
            .await?;
        encoded(response)
    }

    async fn apply_cleanup_artifacts(
        &self,
        name: &str,
        request: &CleanupApplyRequest,
        plan: &CleanupPlan,
        response: &mut CleanupApplyResponse,
    ) -> Result<()> {
        for (index, item) in plan.items.iter().enumerate() {
            if item.artifact.kind == ArtifactKind::ProjectRegistration
                || item.verdict != "safe"
                || response.items[index].outcome != "unknown"
            {
                continue;
            }
            let outcome = match self.cleanup_remove(name, &item.artifact, request).await {
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
                .cleanup_remove_worktree(principal, artifact, &request.id)
                .await;
        }
        let agent = self
            .inner
            .store
            .lock()
            .await
            .get_agent(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
        let path = self.checked_artifact_path(&agent, &artifact.path).await?;
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
    ) -> Result<u64> {
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
            check_worktree_artifact(artifact, &preparation)?;
            let id = eliot_store::cleanup_worktree_command_id(apply_id, &artifact.id)?;
            let mut request = SubmitCommand::new(id, "worktree.remove");
            request.target = Some(artifact.agent.clone());
            request.expected_generation = Some(artifact.generation);
            let admission = self.admit_prepared_submission(
                &mut store,
                principal,
                PreparedSubmission {
                    request,
                    workspace: None,
                },
                None,
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
        Self::classify_branch(&self.inner.data_root, git).await
    }

    /// Fetch the branch upstream and classify the local branch. `safe` means
    /// the fresh fetch succeeded, the tip is contained in the upstream
    /// default branch and no commits are unpushed; anything else is a keep
    /// reason or an explicit refusal. The fetch updates remote-tracking
    /// state but never deletes a branch; deletion happens only at apply.
    async fn classify_branch(
        data_root: &Path,
        git: &CleanupGitBinding,
    ) -> Result<BranchClassification> {
        use crate::git_actions::{CleanupTransportOp, run_cleanup_transport};
        if !crate::git_actions::valid_cleanup_branch(&git.branch) {
            return Err(Error::new(
                "invalid_branch",
                "branch name is not admissible",
            ));
        }
        let repo = Path::new(&git.common);
        let probe = crate::git_monitor::GitProbe::new()?;
        let deadline = Instant::now() + Duration::from_secs(60);
        let branch_ref = format!("refs/heads/{}", git.branch);
        let remote = Self::branch_upstream_remote(&probe, repo, &branch_ref, deadline).await?;
        let url = Self::branch_remote_url(&probe, repo, &remote, deadline).await?;
        // A fresh fetch through the same isolated transport as the approved
        // fetch action: private view, isolated HOME and config, no
        // credentials, only file/HTTPS/HTTP/git. It updates remote-tracking
        // refs but never deletes a branch; deletion happens only at apply.
        // No classification is trusted without this fetch completing.
        let fetch = run_cleanup_transport(
            data_root,
            &git.repo,
            &remote,
            &url,
            None,
            CleanupTransportOp::FetchPrune,
            deadline,
        )
        .await
        .map_err(|error| match error.code.as_str() {
            "unsupported" => Error::new(
                "remote_transport_unsupported",
                "upstream transport is not file, HTTPS, HTTP or git",
            ),
            "invalid_remote" | "invalid" => {
                Error::new("invalid_remote", "upstream remote is not admissible")
            }
            _ => Error::new(
                "fetch_failed",
                format!("branch upstream fetch failed: {}", error.message),
            ),
        })?;
        if fetch.interrupted || !fetch.quiescent || fetch.exit_code != Some(0) {
            return Err(Error::new(
                "fetch_failed",
                "branch upstream fetch did not complete",
            ));
        }
        let default_name =
            Self::branch_default_name(data_root, git, &remote, &url, deadline).await?;
        let default_ref = format!("refs/remotes/{remote}/{default_name}");
        let default_tip =
            Self::branch_rev_parse(&probe, repo, &default_ref, "no_default_branch", deadline)
                .await?;
        let tip =
            Self::branch_rev_parse(&probe, repo, &branch_ref, "branch_absent", deadline).await?;
        let ancestor = probe
            .workspace_command(
                repo,
                &["merge-base", "--is-ancestor", &tip, &default_tip],
                deadline,
            )
            .await
            .map_err(|_| Error::new("merge_check_failed", "branch merge check is unavailable"))?;
        let merged = match ancestor.code {
            0 => true,
            1 => false,
            _ => {
                return Err(Error::new(
                    "merge_check_failed",
                    "branch merge check did not complete",
                ));
            }
        };
        let upstream =
            Self::branch_upstream_tip(&probe, repo, &branch_ref, &remote, &git.branch, deadline)
                .await?;
        let unpushed = match upstream.as_deref() {
            Some(upstream) => {
                let count = probe
                    .workspace_command(
                        repo,
                        &["rev-list", "--count", &format!("{upstream}..{tip}")],
                        deadline,
                    )
                    .await
                    .map_err(|_| {
                        Error::new("branch_read_failed", "branch ahead check is unavailable")
                    })?;
                if count.code != 0 {
                    return Err(Error::new(
                        "branch_read_failed",
                        "branch ahead check did not complete",
                    ));
                }
                String::from_utf8_lossy(&count.stdout)
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| {
                        Error::new("branch_read_failed", "branch ahead count is not a number")
                    })?
                    > 0
            }
            None => !merged,
        };
        let remote_heads = run_cleanup_transport(
            data_root,
            &git.repo,
            &remote,
            &url,
            Some(&git.branch),
            CleanupTransportOp::ListHeads,
            deadline,
        )
        .await
        .map_err(|_| Error::new("remote_check_failed", "branch remote check is unavailable"))?;
        if remote_heads.interrupted || !remote_heads.quiescent || remote_heads.exit_code != Some(0)
        {
            return Err(Error::new(
                "remote_check_failed",
                "branch remote check did not complete",
            ));
        }
        let deleted_remotely = remote_heads.stdout.is_empty();
        if merged && !unpushed {
            Ok(BranchClassification {
                reason: if deleted_remotely {
                    "merged_remote_deleted"
                } else {
                    "merged"
                },
                remote,
                url,
                tip,
            })
        } else if !merged {
            Err(Error::new(
                "unmerged",
                "branch tip is not contained in the upstream default branch",
            ))
        } else {
            Err(Error::new(
                "ahead",
                "branch carries commits its upstream tracking ref lacks",
            ))
        }
    }

    async fn branch_upstream_remote(
        probe: &crate::git_monitor::GitProbe,
        repo: &Path,
        branch_ref: &str,
        deadline: Instant,
    ) -> Result<String> {
        let remote = probe
            .workspace_command(
                repo,
                &[
                    "for-each-ref",
                    "--format=%(upstream:remotename)",
                    branch_ref,
                ],
                deadline,
            )
            .await
            .map_err(|_| Error::new("branch_read_failed", "branch upstream is unreadable"))?;
        if remote.code != 0 {
            return Err(Error::new(
                "branch_read_failed",
                "branch upstream is unreadable",
            ));
        }
        let name = String::from_utf8(remote.stdout)
            .map_err(|_| Error::new("branch_read_failed", "branch upstream is not Unicode"))?;
        let name = name.trim();
        if !name.is_empty() {
            return Ok(name.to_owned());
        }
        let origin = probe
            .workspace_command(repo, &["remote", "get-url", "origin"], deadline)
            .await
            .map_err(|_| Error::new("no_upstream_remote", "branch has no upstream remote"))?;
        if origin.code != 0 {
            return Err(Error::new(
                "no_upstream_remote",
                "branch has no upstream remote",
            ));
        }
        Ok("origin".to_owned())
    }

    /// Read the exact remote URL from local repository config. The URL is
    /// validated as an endpoint by the isolated transport before any network
    /// use; only file/HTTPS/HTTP/git transports are admitted.
    async fn branch_remote_url(
        probe: &crate::git_monitor::GitProbe,
        repo: &Path,
        remote: &str,
        deadline: Instant,
    ) -> Result<String> {
        let key = format!("remote.{remote}.url");
        let configured = probe
            .workspace_command(repo, &["config", "--get", &key], deadline)
            .await
            .map_err(|_| Error::new("no_upstream_remote", "branch remote URL is unreadable"))?;
        if configured.code != 0 {
            return Err(Error::new(
                "no_upstream_remote",
                "branch has no configured upstream remote",
            ));
        }
        let url = String::from_utf8(configured.stdout)
            .map_err(|_| Error::new("no_upstream_remote", "branch remote URL is not Unicode"))?;
        let url = url.trim().to_owned();
        if url.is_empty() || url.len() > 4096 || url.chars().any(char::is_control) {
            return Err(Error::new(
                "no_upstream_remote",
                "branch remote URL is not admissible",
            ));
        }
        Ok(url)
    }

    async fn branch_default_name(
        data_root: &Path,
        git: &CleanupGitBinding,
        remote: &str,
        url: &str,
        deadline: Instant,
    ) -> Result<String> {
        use crate::git_actions::{CleanupTransportOp, run_cleanup_transport};
        // The upstream HEAD symref is read fresh from the remote through the
        // isolated transport; a stale local remote-HEAD pointer alone never
        // decides the default branch.
        if let Ok(symref) = run_cleanup_transport(
            data_root,
            &git.repo,
            remote,
            url,
            None,
            CleanupTransportOp::ListSymrefHead,
            deadline,
        )
        .await
            && symref.exit_code == Some(0)
            && symref.quiescent
            && !symref.interrupted
        {
            let output = String::from_utf8_lossy(&symref.stdout);
            for line in output.lines() {
                if let Some(rest) = line.strip_prefix("ref: ")
                    && rest.split_whitespace().nth(1) == Some("HEAD")
                    && let Some(name) = rest
                        .split_whitespace()
                        .next()
                        .and_then(|reference| reference.strip_prefix("refs/heads/"))
                    && crate::git_actions::valid_cleanup_branch(name)
                {
                    return Ok(name.to_owned());
                }
            }
        }
        Err(Error::new(
            "no_default_branch",
            "upstream default branch is unreadable",
        ))
    }

    async fn branch_rev_parse(
        probe: &crate::git_monitor::GitProbe,
        repo: &Path,
        reference: &str,
        missing_code: &'static str,
        deadline: Instant,
    ) -> Result<String> {
        let parsed = probe
            .workspace_command(repo, &["rev-parse", "--verify", reference], deadline)
            .await
            .map_err(|_| Error::new("branch_read_failed", "branch tip is unreadable"))?;
        if parsed.code != 0 {
            return Err(Error::new(missing_code, "branch reference is absent"));
        }
        let oid = String::from_utf8(parsed.stdout)
            .map_err(|_| Error::new("branch_read_failed", "branch tip is not Unicode"))?;
        let oid = oid.trim().to_owned();
        if !eliot_core::api::is_git_oid(&oid) {
            return Err(Error::new(
                "branch_read_failed",
                "branch tip is not an object id",
            ));
        }
        Ok(oid)
    }

    async fn branch_upstream_tip(
        probe: &crate::git_monitor::GitProbe,
        repo: &Path,
        branch_ref: &str,
        remote: &str,
        branch: &str,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let upstream = probe
            .workspace_command(
                repo,
                &["for-each-ref", "--format=%(upstream)", branch_ref],
                deadline,
            )
            .await
            .map_err(|_| Error::new("branch_read_failed", "branch upstream is unreadable"))?;
        if upstream.code != 0 {
            return Err(Error::new(
                "branch_read_failed",
                "branch upstream is unreadable",
            ));
        }
        let name = String::from_utf8_lossy(&upstream.stdout);
        let name = name.trim();
        if !name.is_empty() {
            // A configured upstream can name a tracking ref the fresh prune
            // just removed (remote branch deleted). Only a resolvable
            // tracking ref counts; a stale one is the same as none.
            let parsed = probe
                .workspace_command(repo, &["rev-parse", "--verify", name], deadline)
                .await
                .map_err(|_| Error::new("branch_read_failed", "branch upstream is unreadable"))?;
            if parsed.code == 0 {
                return Ok(Some(name.to_owned()));
            }
            return Ok(None);
        }
        let same_name = format!("refs/remotes/{remote}/{branch}");
        let parsed = probe
            .workspace_command(repo, &["rev-parse", "--verify", &same_name], deadline)
            .await
            .map_err(|_| Error::new("branch_read_failed", "branch tracking ref is unreadable"))?;
        if parsed.code != 0 {
            return Ok(None);
        }
        Ok(Some(same_name))
    }

    /// Re-verify at apply time, then delete the local branch with the safe
    /// `-d` form only. A refused deletion keeps the branch and reports why;
    /// a branch that vanished concurrently is `unknown`, never replayed.
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
        let tip = Self::branch_rev_parse(
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
        // A refused `-d` writes diagnostics, which the process fence turns
        // into an error: either way the branch is re-read before concluding.
        let deleted = match probe
            .workspace_command(repo, &["branch", "-d", &git.branch], deadline)
            .await
        {
            Ok(deleted) => deleted.code,
            Err(_) => -1,
        };
        if deleted == 0 {
            return Ok(());
        }
        let branch_ref = format!("refs/heads/{}", git.branch);
        let again = Self::branch_rev_parse(&probe, repo, &branch_ref, "branch_absent", deadline)
            .await
            .map_err(|_| Error::new("unknown", "branch deletion outcome is uncertain"))?;
        if !again.is_empty() {
            return Err(Error::new(
                "branch_delete_refused",
                "branch is not fully merged and was kept",
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

/// Whether the artifact listing continues: the Store still holds eligible
/// rows, the agent backfill is capped, project rows remain, or project
/// rows displaced artifact rows off this page.
#[allow(clippy::too_many_arguments)]
fn artifact_listing_truncated(
    artifacts_truncated: bool,
    inventory_incomplete: bool,
    artifacts_total: usize,
    middle_len: usize,
    safe_total: usize,
    safe_consumed: usize,
    kept_total: usize,
    kept_consumed: usize,
) -> bool {
    artifacts_truncated
        || inventory_incomplete
        || artifacts_total > middle_len
        || safe_total > safe_consumed
        || kept_total > kept_consumed
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

/// Fresh-fetch branch classification for one plan item.
#[derive(Debug)]
struct BranchClassification {
    reason: &'static str,
    remote: String,
    url: String,
    tip: String,
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
            self.classify_worktree(&mut candidate).await;
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
            let outcome = match self.cleanup_remove("owner", &item.artifact, &apply).await {
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
