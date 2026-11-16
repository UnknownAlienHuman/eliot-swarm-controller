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
                    && let Ok(path) = self.checked_artifact_path(agent, &path.to_string_lossy())
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
        let mut store = self.inner.store.lock().await;
        for artifact in known {
            store.record_known_artifact(artifact)?;
        }
        Ok(())
    }
    fn checked_artifact_path(&self, agent: &Agent, path: &str) -> Result<PathBuf> {
        let path = Path::new(path);
        if !path.is_absolute()
            || path.components().any(|p| match p {
                Component::ParentDir | Component::CurDir => true,
                Component::Normal(name) => name.to_str().is_none_or(|n| {
                    n.contains(':')
                        || n.ends_with(['.', ' '])
                        || [".git", ".codex", ".claude", ".config", ".ssh"]
                            .iter()
                            .any(|x| n.eq_ignore_ascii_case(x))
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
        let own_temp = daemon_root
            .join("artifacts")
            .join(&agent.id)
            .join(agent.generation.to_string());
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
        let config = crate::config::load_daemon_config(&self.inner.data_root)?;
        let mut roots: Vec<PathBuf> = config
            .artifact_roots
            .iter()
            .filter_map(|p| std::fs::canonicalize(p).ok())
            .collect();
        roots.push(own_temp);
        if let Some(workspace) = &agent.workspace
            && let Ok(workspace) = std::fs::canonicalize(workspace)
        {
            roots.push(workspace);
        }
        if !roots
            .iter()
            .any(|root| canonical != *root && canonical.starts_with(root))
        {
            return Err(Error::new(
                "not_allowed",
                "artifact is outside the Agent workspace, configured artifact roots and own temp root",
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
        let path = self.checked_artifact_path(&agent, &request.path)?;
        let identity = directory_identity(&path)?;
        drop(store);
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
                candidate.reason = "fetch_and_merge_verification_required".to_owned();
            } else if candidate.artifact.kind == ArtifactKind::NativeSession {
                candidate.reason =
                    if matches!(candidate.artifact.purpose.as_str(), "codex" | "opencode") {
                        "native_archive_not_implemented"
                    } else {
                        "no_native_archive"
                    }
                    .to_owned();
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
            self.cleanup_worktree_clean(artifact).await?;
            #[cfg(windows)]
            {
                let git = artifact
                    .git
                    .as_ref()
                    .ok_or_else(|| Error::new("invalid", "worktree Git binding absent"))?;
                let tree = CleanupTree::open(Path::new(&artifact.path))?;
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
                return Err(Error::new(
                    "unsupported",
                    "handle deletion unavailable on this platform",
                ));
            }
        }
        if artifact.kind == ArtifactKind::NativeSession {
            return Err(Error::new(
                "no_native_archive",
                "harness archive is not connected",
            ));
        }
        if artifact.kind == ArtifactKind::Branch {
            return Err(Error::new(
                "fetch_required",
                "branch cleanup requires a verified fresh default-branch fetch",
            ));
        }
        let agent = self
            .inner
            .store
            .lock()
            .await
            .get_agent(&artifact.agent)?
            .ok_or_else(|| Error::new("not_found", "Agent absent"))?;
        let path = self.checked_artifact_path(&agent, &artifact.path)?;
        self.cleanup_check_git(&agent, &path).await?;
        #[cfg(windows)]
        {
            let tree = CleanupTree::open(&path).map_err(|_| {
                Error::new(
                    "busy_or_unsafe",
                    "directory is held, linked or exceeds cleanup bounds",
                )
            })?;
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
            let outcome = match self.cleanup_remove(&name, &item.artifact).await {
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

    async fn cleanup_remove(&self, principal: &str, artifact: &ArtifactView) -> Result<u64> {
        if artifact.kind == ArtifactKind::Worktree {
            self.cleanup_worktree_clean(artifact).await?;
            #[cfg(windows)]
            {
                let git = artifact
                    .git
                    .as_ref()
                    .ok_or_else(|| Error::new("source_integrity", "worktree binding absent"))?;
                let root = CleanupTree::open(Path::new(&artifact.path))?;
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
        let path = self.checked_artifact_path(&agent, &artifact.path)?;
        self.cleanup_check_git(&agent, &path).await?;
        #[cfg(windows)]
        {
            let tree = CleanupTree::open(&path).map_err(|_| {
                Error::new(
                    "busy_or_unsafe",
                    "directory is held, linked or exceeds cleanup bounds",
                )
            })?;
            if tree.identity != artifact.identity {
                return Err(Error::new(
                    "identity_changed",
                    "artifact directory was substituted",
                ));
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
        let top = probe
            .workspace_command(
                root,
                &["rev-parse", "--path-format=absolute", "--show-toplevel"],
                deadline,
            )
            .await?;
        if top.code != 0 {
            return Err(Error::new(
                "unavailable",
                "artifact workspace Git identity unavailable",
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
                .workspace_command(root, &["ls-files", "-z", "--", &argument], deadline)
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
        Ok(())
    }

    async fn cleanup_worktree_clean(&self, artifact: &ArtifactView) -> Result<()> {
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
        Ok(())
    }
}
