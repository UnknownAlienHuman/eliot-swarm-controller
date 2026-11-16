//! Bounded artifact cleanup through the authenticated Router and Store writer.
use super::*;
#[cfg(windows)]
use crate::platform::script_file::CleanupTree;
use crate::platform::script_file::directory_identity;
use eliot_core::api::*;
use std::path::{Component, Path};

fn encoded(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "cleanup response cannot encode"))
}

impl Router {
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
            && workspace.phase == WorkspacePhase::Ready
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
        if let (Some(scope), Some(reference)) = (&agent.native_scope, &agent.native_ref) {
            known.push(ArtifactView {
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
            });
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
        if canonical == own_temp || !canonical.starts_with(&own_temp) {
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
        // Backfill only facts already retained by Swarm, not arbitrary files.
        let agents = self.inner.store.lock().await.list_agents(None, 1000)?;
        let mut inventory_incomplete = agents.len() == 1000;
        for agent in agents {
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
                .cleanup_visible_agent(&name, &agent)?;
            if allowed {
                inventory_incomplete |= self.record_agent_artifacts(&agent).await.is_err();
            }
        }
        let (mut artifacts, truncated) = self
            .inner
            .store
            .lock()
            .await
            .cleanup_inventory(&name, &request)?;
        artifacts.sort_by_key(|a| {
            (
                match a.kind {
                    ArtifactKind::Worktree => 1,
                    ArtifactKind::Branch | ArtifactKind::NativeSession => 2,
                    _ => 0,
                },
                a.id.clone(),
            )
        });
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: truncated || inventory_incomplete,
        };
        for artifact in artifacts {
            let mut candidate = CleanupCandidate {
                artifact,
                bytes: None,
                verdict: "keep".to_owned(),
                reason: "agent_not_quiescent".to_owned(),
            };
            if candidate.artifact.state != "registered" {
                candidate.reason = candidate.artifact.state.clone();
            } else if candidate.artifact.kind == ArtifactKind::Branch {
                let Some(git) = candidate.artifact.git.as_ref() else {
                    candidate.reason = "source_integrity".to_owned();
                    plan.items.push(candidate);
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
                    plan.items.push(candidate);
                    continue;
                }
                match Self::classify_branch(&self.inner.data_root, git).await {
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
                if candidate.artifact.purpose.as_str() != "codex" {
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
            plan.items.push(candidate);
        }
        self.inner
            .store
            .lock()
            .await
            .retain_cleanup_plan(&name, &plan)?;
        encoded(plan)
    }

    async fn cleanup_preflight(&self, artifact: &ArtifactView) -> Result<u64> {
        if artifact.kind == ArtifactKind::Worktree {
            let marker = self.cleanup_worktree_clean(artifact).await?;
            #[cfg(windows)]
            {
                let git = artifact
                    .git
                    .as_ref()
                    .ok_or_else(|| Error::new("invalid", "worktree Git binding absent"))?;
                let tree = CleanupTree::open_worktree(Path::new(&artifact.path), &marker)?;
                let admin = CleanupTree::open(Path::new(&git.admin))?;
                if tree.identity != artifact.identity || admin.identity != git.admin_identity {
                    return Err(Error::new(
                        "identity_changed",
                        "worktree directory substituted",
                    ));
                }
                return Ok(tree.bytes + admin.bytes);
            }
            #[cfg(not(windows))]
            {
                let _ = marker;
                return Err(Error::new(
                    "unsupported",
                    "handle deletion unavailable on this platform",
                ));
            }
        }
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
            Self::classify_branch(&self.inner.data_root, git).await?;
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
        let (plan, mut response, is_new) =
            self.inner
                .store
                .lock()
                .await
                .begin_cleanup_apply(&name, &request, now_ms())?;
        if !is_new {
            return encoded(response);
        }
        for (index, item) in plan.items.iter().enumerate() {
            if item.verdict != "safe" || response.items[index].outcome != "unknown" {
                continue;
            }
            let outcome = match self
                .cleanup_remove(&name, &item.artifact, request.delete_remote)
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
                &name,
                &request,
                &response,
                &item.artifact,
                &outcome,
            )?;
        }
        encoded(response)
    }

    async fn cleanup_remove(
        &self,
        principal: &str,
        artifact: &ArtifactView,
        delete_remote: bool,
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
            let classification = Self::classify_branch(&self.inner.data_root, git).await?;
            if classification.tip != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "branch tip moved since the plan was retained",
                ));
            }
            Self::delete_local_branch(git, &artifact.identity).await?;
            if delete_remote {
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
            // are archived through the harness app-server API; every other
            // harness has no documented archive surface and stays kept.
            if artifact.purpose.as_str() != "codex" {
                return Err(Error::new(
                    "no_native_archive",
                    "harness has no native archive method",
                ));
            }
            let agent = self
                .inner
                .store
                .lock()
                .await
                .get_agent(&artifact.agent)?
                .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
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
                .archive_native_session(profile, &native_ref)
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
            let marker = self.cleanup_worktree_clean(artifact).await?;
            #[cfg(windows)]
            {
                let git = artifact
                    .git
                    .as_ref()
                    .ok_or_else(|| Error::new("source_integrity", "worktree binding absent"))?;
                let root = CleanupTree::open_worktree(Path::new(&artifact.path), &marker)?;
                let admin = CleanupTree::open(Path::new(&git.admin))?;
                if root.identity != artifact.identity || admin.identity != git.admin_identity {
                    return Err(Error::new("identity_changed", "worktree identity changed"));
                }
                let store = self.inner.store.lock().await;
                if !store.cleanup_authorized(principal, artifact)?
                    || !store.cleanup_agent_quiescent(artifact)?
                {
                    return Err(Error::new(
                        "agent_not_quiescent",
                        "worktree authority changed",
                    ));
                }
                let bytes = root.remove()?;
                return admin.remove().map(|extra| bytes + extra).map_err(|_| {
                    Error::new(
                        "unknown",
                        "worktree removed; administration cleanup uncertain",
                    )
                });
            }
            #[cfg(not(windows))]
            {
                let _ = marker;
                return Err(Error::new("unsupported", "handle deletion unavailable"));
            }
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
        for (location, required) in [(root, true), (path, false)] {
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

    async fn cleanup_worktree_clean(&self, artifact: &ArtifactView) -> Result<String> {
        let git = artifact
            .git
            .as_ref()
            .ok_or_else(|| Error::new("source_integrity", "worktree binding absent"))?;
        let retained = self
            .inner
            .store
            .lock()
            .await
            .get_workspace(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "worktree record absent"))?;
        if retained.worktree_path != artifact.path
            || retained.target_generation != artifact.generation
            || retained.requested_branch != git.branch
            || retained.phase != WorkspacePhase::Ready
        {
            return Err(Error::new("identity_changed", "worktree record changed"));
        }
        let probe = crate::git_monitor::GitProbe::new()?;
        let root = Path::new(&git.worktree);
        let directory = crate::platform::script_file::Directory::open(root)?;
        let mut marker = directory.file(".git")?;
        let marker_identity = serde_json::to_string(
            &eliot_plugin::file_identity(&marker)
                .map_err(|_| Error::new("identity_changed", "worktree pointer unavailable"))?,
        )
        .map_err(|_| Error::new("identity_changed", "worktree pointer unavailable"))?;
        let pointer = crate::platform::script_file::read(&mut marker)?;
        let pointer = std::str::from_utf8(&pointer)
            .ok()
            .and_then(|text| text.trim_end_matches(['\r', '\n']).strip_prefix("gitdir: "))
            .ok_or_else(|| Error::new("identity_changed", "worktree pointer malformed"))?;
        if std::fs::canonicalize(pointer).ok().as_deref() != Some(Path::new(&git.admin)) {
            return Err(Error::new("identity_changed", "worktree pointer changed"));
        }
        let admin = crate::platform::script_file::Directory::open(Path::new(&git.admin))?;
        let mut reverse = admin.file("gitdir")?;
        let reverse = crate::platform::script_file::read(&mut reverse)?;
        let reverse = std::str::from_utf8(&reverse)
            .map_err(|_| Error::new("identity_changed", "worktree reverse pointer is invalid"))?;
        let reverse = std::fs::canonicalize(reverse.trim_end_matches(['\r', '\n']))
            .map_err(|_| Error::new("identity_changed", "worktree reverse pointer unavailable"))?;
        if reverse
            != std::fs::canonicalize(root.join(".git"))
                .map_err(|_| Error::new("identity_changed", "worktree pointer unavailable"))?
        {
            return Err(Error::new(
                "identity_changed",
                "worktree registration is not reciprocal",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        for (args, expected) in [
            (
                vec!["rev-parse", "--path-format=absolute", "--git-common-dir"],
                PathBuf::from(&git.common),
            ),
            (
                vec!["rev-parse", "--path-format=absolute", "--git-dir"],
                PathBuf::from(&git.admin),
            ),
        ] {
            let result = probe.workspace_command(root, &args, deadline).await?;
            let actual = std::fs::canonicalize(String::from_utf8_lossy(&result.stdout).trim())
                .map_err(|_| Error::new("identity_changed", "Git directory changed"))?;
            if result.code != 0 || actual != expected {
                return Err(Error::new("identity_changed", "Git directory changed"));
            }
        }
        let head = probe
            .workspace_command(root, &["symbolic-ref", "HEAD"], deadline)
            .await?;
        if head.code != 0
            || String::from_utf8_lossy(&head.stdout).trim() != format!("refs/heads/{}", git.branch)
        {
            return Err(Error::new("identity_changed", "worktree branch changed"));
        }
        let status = probe
            .workspace_command(
                root,
                &[
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--untracked-files=all",
                    "--ignored=matching",
                ],
                deadline,
            )
            .await?;
        if status.code != 0 || !status.stdout.is_empty() {
            return Err(Error::new(
                "dirty_worktree",
                "worktree has tracked, untracked or ignored content",
            ));
        }
        Ok(marker_identity)
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
        if oid.len() != 40 || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
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
    if agent.id != artifact.agent
        || agent.generation != artifact.generation
        || agent.native_scope.as_deref() != Some(artifact.identity.as_str())
        || agent.native_ref.as_deref() != Some(reference.as_str())
        || binding.native_scope != artifact.identity
        || agent.harness != "codex"
        || profile["harness"] != "codex"
    {
        return Err(Error::new(
            "native_scope_unverified",
            "archive identity differs from the frozen native store",
        ));
    }
    Ok((profile, reference))
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
        let inventory = match self.inner.store.lock().await.cleanup_inventory(
            "owner",
            &CleanupPlanRequest {
                scope: CleanupScope::Project,
                project: Some(agent.project.clone()),
                older_than: None,
            },
        ) {
            Ok((artifacts, _)) => artifacts,
            Err(_) => return,
        };
        let mut plan = CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items: Vec::new(),
            truncated: false,
        };
        for artifact in inventory {
            if artifact.agent != agent.id
                || artifact.generation != agent.generation
                || artifact.state != "registered"
                || !matches!(
                    artifact.kind,
                    ArtifactKind::Build | ArtifactKind::Temp | ArtifactKind::Worktree
                )
            {
                continue;
            }
            if plan.items.len() >= MAX_CLEANUP_ITEMS {
                plan.truncated = true;
                break;
            }
            let mut candidate = CleanupCandidate {
                artifact,
                bytes: None,
                verdict: "keep".to_owned(),
                reason: "retirement_preflight".to_owned(),
            };
            let quiescent = self
                .inner
                .store
                .lock()
                .await
                .cleanup_agent_quiescent(&candidate.artifact)
                .unwrap_or(false);
            if quiescent {
                match self.cleanup_preflight(&candidate.artifact).await {
                    Ok(bytes) => {
                        candidate.verdict = "safe".to_owned();
                        candidate.reason = "retirement_owned_artifact".to_owned();
                        candidate.bytes = Some(bytes);
                    }
                    Err(error) => candidate.reason = error.code,
                }
            } else {
                candidate.reason = "agent_not_quiescent".to_owned();
            }
            plan.items.push(candidate);
        }
        if !plan.items.iter().any(|item| item.verdict == "safe") {
            return;
        }
        if self
            .inner
            .store
            .lock()
            .await
            .retain_cleanup_plan("owner", &plan)
            .is_err()
        {
            return;
        }
        let apply = CleanupApplyRequest {
            id: ulid::Ulid::new().to_string(),
            plan_id: plan.plan_id.clone(),
            delete_remote: false,
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
            let outcome = match self.cleanup_remove("owner", &item.artifact, false).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as OsCommand;

    #[tokio::test]
    async fn artifact_root_is_created_exclusively_and_bound_to_generation() {
        let root = std::env::temp_dir().join(format!("eliot-root-{}", ulid::Ulid::new()));
        std::fs::create_dir(&root).unwrap();
        let router = super::super::tests::router_with_data_root(root.clone());
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.expected_generation = Some(0);
        request.args = json!({"project":"p", "workspace":root, "harness":"codex", "profile":"fixture", "profile_snapshot":{}});
        let agent = {
            let mut store = router.inner.store.lock().await;
            let admitted = store.submit("owner", request.clone(), now_ms()).unwrap();
            store.dispatch(&admitted.command.id, now_ms()).unwrap();
            store
                .get_agent(admitted.command.target.as_deref().unwrap())
                .unwrap()
                .unwrap()
        };
        let path = root
            .join("artifacts")
            .join(format!("{}-{}", agent.id, agent.generation));
        std::fs::create_dir_all(&path).unwrap();
        assert!(router.prepare_agent_artifact_root(&agent).await.is_err());
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .agent_artifact_root(&agent.id, agent.generation)
                .unwrap()
                .is_none()
        );
        std::fs::remove_dir(&path).unwrap();
        router.prepare_agent_artifact_root(&agent).await.unwrap();
        let bound = router
            .inner
            .store
            .lock()
            .await
            .agent_artifact_root(&agent.id, agent.generation)
            .unwrap()
            .unwrap();
        assert_eq!(directory_identity(&path).unwrap(), bound.identity);
        router.prepare_agent_artifact_root(&agent).await.unwrap();
        let child = path.join("output");
        std::fs::create_dir(&child).unwrap();
        assert!(
            router
                .checked_artifact_path(&agent, &child.to_string_lossy())
                .await
                .is_ok()
        );
        for reserved in [
            ".git",
            ".codex",
            ".claude",
            ".config",
            ".ssh",
            ".muse",
            ".opencode",
            ".antigravity",
        ] {
            let protected = path.join(reserved).join("descendant");
            std::fs::create_dir_all(&protected).unwrap();
            assert!(
                router
                    .checked_artifact_path(&agent, &protected.to_string_lossy())
                    .await
                    .is_err()
            );
        }
        let sibling = root.join("artifacts").join("a999-1").join("output");
        std::fs::create_dir_all(&sibling).unwrap();
        assert!(
            router
                .checked_artifact_path(&agent, &sibling.to_string_lossy())
                .await
                .is_err()
        );
        let mut later = agent;
        later.generation += 1;
        assert!(
            router
                .checked_artifact_path(&later, &child.to_string_lossy())
                .await
                .is_err()
        );
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn git(dir: &Path, args: &[&str]) -> String {
        let output = OsCommand::new("git")
            .arg("-C")
            .arg(dir)
            .arg("-c")
            .arg("user.name=cleanup-test")
            .arg("-c")
            .arg("user.email=cleanup-test@example.invalid")
            .arg("-c")
            .arg("commit.gpgsign=false")
            .args(args)
            .output()
            .expect("cleanup tests need git on PATH");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("git output is Unicode")
    }

    fn commit_file(work: &Path, name: &str, contents: &str) {
        std::fs::write(work.join(name), contents).expect("write fixture file");
        git(work, &["add", "-A"]);
        git(work, &["commit", "-m", name]);
    }

    /// Bare remote plus a clone checked out at `main`, pushed upstream.
    fn scratch_repo(tag: &str) -> (PathBuf, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("eliot-cleanup-test-{tag}-{}", ulid::Ulid::new()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create fixture root");
        let remote = root.join("remote.git");
        git(&root, &["init", "--bare", "remote.git"]);
        git(&root, &["init", "-b", "main", "seed"]);
        let seed = root.join("seed");
        commit_file(&seed, "seed.txt", "seed\n");
        git(
            &seed,
            &["remote", "add", "origin", &remote.to_string_lossy()],
        );
        git(&seed, &["push", "-u", "origin", "main"]);
        git(&remote, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(&root, &["clone", "remote.git", "work"]);
        let work = root.join("work");
        (root, work)
    }

    fn binding(work: &Path, branch: &str) -> CleanupGitBinding {
        let dir = work.to_string_lossy().into_owned();
        CleanupGitBinding {
            repo: dir.clone(),
            common: dir.clone(),
            branch: branch.to_owned(),
            worktree: dir.clone(),
            admin: dir.clone(),
            admin_identity: "test".to_owned(),
        }
    }

    /// Git marks pack and object files read-only on Windows, which makes a
    /// plain `remove_dir_all` fail; clear the attribute first so fixtures
    /// never leak into the temp directory.
    fn finish(root: &Path) {
        fn writable(path: &Path) {
            let Ok(meta) = std::fs::symlink_metadata(path) else {
                return;
            };
            if meta.is_dir() {
                if let Ok(entries) = std::fs::read_dir(path) {
                    for entry in entries.flatten() {
                        writable(&entry.path());
                    }
                }
            } else {
                let mut permissions = meta.permissions();
                #[allow(clippy::permissions_set_readonly_false)]
                permissions.set_readonly(false);
                let _ = std::fs::set_permissions(path, permissions);
            }
        }
        writable(root);
        std::fs::remove_dir_all(root).expect("remove cleanup fixture");
    }

    #[test]
    fn branch_names_reject_git_metacharacters() {
        for valid in ["main", "feature/k69-cleanup", "a-b_c.d", "x"] {
            assert!(
                crate::git_actions::valid_cleanup_branch(valid),
                "valid rejected: {valid}"
            );
        }
        for invalid in [
            "",
            "-leading-dash",
            "has space",
            "has~tilde",
            "has^caret",
            "has:colon",
            "has?question",
            "has*star",
            "has[bracket",
            "back\\slash",
            "double..dot",
            "trailing/",
            "trailing.lock",
            "nul\0byte",
            "new\nline",
        ] {
            assert!(
                !crate::git_actions::valid_cleanup_branch(invalid),
                "invalid admitted: {invalid:?}"
            );
        }
    }

    #[test]
    fn native_session_reference_splits_scope_and_ref() {
        let artifact = ArtifactView {
            id: "c1".to_owned(),
            agent: "a1".to_owned(),
            generation: 1,
            project: "p".to_owned(),
            tree: Vec::new(),
            path: "native:codex-home-abc:thread-xyz".to_owned(),
            kind: ArtifactKind::NativeSession,
            purpose: "codex".to_owned(),
            identity: "codex-home-abc".to_owned(),
            created_at: 1,
            state: "registered".to_owned(),
            git: None,
        };
        assert_eq!(native_session_reference(&artifact).unwrap(), "thread-xyz");
        for bad in [
            "native:codex-home-abc",
            "file:codex-home-abc:thread-xyz",
            "native:other-scope:thread-xyz",
            "native:codex-home-abc:",
        ] {
            let mut broken = artifact.clone();
            broken.path = bad.to_owned();
            assert!(
                native_session_reference(&broken).is_err(),
                "malformed admitted: {bad}"
            );
        }
    }

    #[test]
    fn archive_uses_frozen_profile_and_rejects_different_scope() {
        let mut agent = Agent::new("a1", "p", "codex", "profile-removed-from-config", 1);
        agent.generation = 1;
        agent.native_scope = Some("frozen-scope".to_owned());
        agent.native_ref = Some("thread".to_owned());
        agent.profile_snapshot = json!({"harness":"codex", "pass_env":[], "pass_env_values":{"CODEX_HOME":null,"CODEX_SQLITE_HOME":null}, "native_store_binding":{"home":"C:/private/home", "root":"C:/private/sqlite", "native_scope":"frozen-scope"}});
        let mut artifact = ArtifactView {
            id: "artifact".to_owned(),
            agent: agent.id.clone(),
            generation: 1,
            project: "p".to_owned(),
            tree: Vec::new(),
            path: "native:frozen-scope:thread".to_owned(),
            kind: ArtifactKind::NativeSession,
            purpose: "codex".to_owned(),
            identity: "frozen-scope".to_owned(),
            created_at: 1,
            state: "registered".to_owned(),
            git: None,
        };
        let (profile, reference) = frozen_archive_profile(&agent, &artifact).unwrap();
        assert_eq!(profile, agent.profile_snapshot);
        assert_eq!(reference, "thread");
        artifact.identity = "other-scope".to_owned();
        artifact.path = "native:other-scope:thread".to_owned();
        assert_eq!(
            frozen_archive_profile(&agent, &artifact).unwrap_err().code,
            "native_scope_unverified"
        );
    }

    #[test]
    fn daemon_lock_absent_is_clean_and_live_owner_keeps() {
        let dir = std::env::temp_dir().join(format!("eliot-lock-test-{}", ulid::Ulid::new()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(Router::cleanup_check_daemon_lock(&dir).is_ok());

        std::fs::write(dir.join("daemon.lock"), []).unwrap();
        std::fs::write(
            dir.join("daemon.json"),
            format!("{{\"pid\":{}}}", std::process::id()),
        )
        .unwrap();
        assert_eq!(
            Router::cleanup_check_daemon_lock(&dir).unwrap_err().code,
            "daemon_lock_held"
        );

        std::fs::write(dir.join("daemon.json"), "{\"pid\":4294967290}").unwrap();
        assert!(Router::cleanup_check_daemon_lock(&dir).is_ok());

        std::fs::remove_file(dir.join("daemon.json")).unwrap();
        assert_eq!(
            Router::cleanup_check_daemon_lock(&dir).unwrap_err().code,
            "daemon_lock_held"
        );
        finish(&dir);
    }

    #[tokio::test]
    async fn another_repositories_tracked_descendants_are_protected() {
        let (root, work) = scratch_repo("primary");
        let other = root.join("other");
        std::fs::create_dir_all(other.join("sources")).unwrap();
        git(&other, &["init", "-b", "main"]);
        std::fs::write(other.join("sources/code.rs"), "fn main() {}\n").unwrap();
        git(&other, &["add", "-A"]);
        git(&other, &["commit", "-m", "tracked"]);
        let router = super::super::tests::router_with_data_root(root.clone());
        let mut agent = Agent::new("a1", "p", "codex", "fixture", 1);
        agent.workspace = Some(work.to_string_lossy().into_owned());
        for candidate in [&other, &other.join("sources")] {
            assert_eq!(
                router
                    .cleanup_check_git(&agent, candidate)
                    .await
                    .unwrap_err()
                    .code,
                "tracked_content"
            );
        }
        drop(router);
        finish(&root);
    }

    #[tokio::test]
    async fn stale_remote_head_cannot_replace_fresh_default_discovery() {
        let (root, work) = scratch_repo("stale-head");
        assert_eq!(
            git(&work, &["symbolic-ref", "refs/remotes/origin/HEAD"]).trim(),
            "refs/remotes/origin/main"
        );
        git(&work, &["branch", "merged"]);
        // Fetch still succeeds, but the remote no longer advertises a default.
        git(
            &root.join("remote.git"),
            &["symbolic-ref", "HEAD", "refs/heads/missing"],
        );
        let result = Router::classify_branch(&root, &binding(&work, "merged")).await;
        assert_eq!(result.err().unwrap().code, "no_default_branch");
        finish(&root);
    }

    #[tokio::test]
    async fn merged_branch_classifies_safe_and_applies() {
        let (root, work) = scratch_repo("merged");
        git(&work, &["checkout", "-b", "feature-a"]);
        commit_file(&work, "a.txt", "a\n");
        git(&work, &["push", "-u", "origin", "feature-a"]);
        git(&work, &["checkout", "main"]);
        git(&work, &["merge", "--ff-only", "feature-a"]);
        git(&work, &["push", "origin", "main"]);

        let git_binding = binding(&work, "feature-a");
        let classification = Router::classify_branch(&root, &git_binding).await.unwrap();
        assert_eq!(classification.reason, "merged");
        assert_eq!(classification.remote, "origin");

        Router::delete_local_branch(&git_binding, &classification.tip)
            .await
            .unwrap();
        let gone = std::process::Command::new("git")
            .arg("-C")
            .arg(&work)
            .args(["rev-parse", "--verify", "refs/heads/feature-a"])
            .output()
            .unwrap();
        assert!(!gone.status.success(), "branch must be gone");
        finish(&root);
    }

    #[tokio::test]
    async fn unmerged_and_ahead_branches_stay_keep() {
        let (root, work) = scratch_repo("keeps");
        git(&work, &["checkout", "-b", "unmerged"]);
        commit_file(&work, "u.txt", "u\n");
        let unmerged = binding(&work, "unmerged");
        assert_eq!(
            Router::classify_branch(&root, &unmerged)
                .await
                .unwrap_err()
                .code,
            "unmerged"
        );

        git(&work, &["checkout", "-b", "ahead", "main"]);
        commit_file(&work, "e1.txt", "e1\n");
        git(&work, &["push", "-u", "origin", "ahead"]);
        git(&work, &["checkout", "-b", "base"]);
        git(&work, &["push", "-u", "origin", "base"]);
        git(&work, &["checkout", "ahead"]);
        commit_file(&work, "e2.txt", "e2\n");
        git(&work, &["push", "origin", "ahead"]);
        git(&work, &["checkout", "main"]);
        git(&work, &["merge", "--ff-only", "ahead"]);
        git(&work, &["push", "origin", "main"]);
        git(&work, &["branch", "--set-upstream-to=origin/base", "ahead"]);
        let ahead_binding = binding(&work, "ahead");
        assert_eq!(
            Router::classify_branch(&root, &ahead_binding)
                .await
                .unwrap_err()
                .code,
            "ahead"
        );

        let refused = Router::delete_local_branch(&unmerged, "bogus-tip").await;
        assert_eq!(refused.unwrap_err().code, "identity_changed");
        let unmerged_tip = git(&work, &["rev-parse", "--verify", "refs/heads/unmerged"]);
        let refused_merge = Router::delete_local_branch(&unmerged, unmerged_tip.trim()).await;
        assert_eq!(refused_merge.unwrap_err().code, "branch_delete_refused");
        finish(&root);
    }

    #[tokio::test]
    async fn remote_deleted_merged_branch_is_safe_and_remote_delete_roundtrips() {
        let (root, work) = scratch_repo("remote");
        git(&work, &["checkout", "-b", "gone"]);
        commit_file(&work, "g.txt", "g\n");
        git(&work, &["push", "-u", "origin", "gone"]);
        git(&work, &["checkout", "main"]);
        git(&work, &["merge", "gone"]);
        git(&work, &["push", "origin", "main"]);
        git(&work, &["push", "origin", "--delete", "gone"]);

        let remote_url = root.join("remote.git").to_string_lossy().into_owned();
        let git_binding = binding(&work, "gone");
        let classification = Router::classify_branch(&root, &git_binding).await.unwrap();
        assert_eq!(classification.reason, "merged_remote_deleted");

        Router::delete_remote_branch(&root, &git_binding, "origin", &remote_url)
            .await
            .unwrap();
        let gone = Router::delete_local_branch(&git_binding, &classification.tip).await;
        assert!(gone.is_ok());
        finish(&root);
    }

    #[tokio::test]
    async fn checked_out_branch_refuses_delete() {
        let (root, work) = scratch_repo("checkedout");
        let current = git(&work, &["rev-parse", "--abbrev-ref", "HEAD"]);
        let tip = git(
            &work,
            &[
                "rev-parse",
                "--verify",
                &format!("refs/heads/{}", current.trim()),
            ],
        );
        let git_binding = binding(&work, current.trim());
        assert_eq!(
            Router::delete_local_branch(&git_binding, tip.trim())
                .await
                .unwrap_err()
                .code,
            "branch_checked_out"
        );
        finish(&root);
    }
}
