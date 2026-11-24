use super::*;

impl Router {
    pub(in crate::router) async fn agent_delete(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: AgentDeleteRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;

        let (replay, agents) = self.inner.store.lock().await.inspect_agent_delete(
            &request.id,
            &request.agent,
            now_ms(),
        )?;
        if let Some(replay) = replay {
            return Ok(replay);
        }
        let plan = self.plan_agent_deletion(&agents).await?;
        let mut store = self.inner.store.lock().await;
        store.reserve_agent_delete(&request.id, &request.agent, &agents, now_ms())?;
        let apply = CleanupApplyRequest {
            id: ulid::Ulid::new().to_string(),
            plan_id: plan.plan_id.clone(),
            delete_remote: false,
            artifacts: None,
        };
        let started = if plan.items.is_empty() {
            Ok(None)
        } else {
            store
                .retain_cleanup_plan("owner", &plan, now_ms())
                .and_then(|()| store.begin_cleanup_apply("owner", &apply, now_ms()))
                .map(|(_, response, _)| Some(response))
        };
        drop(store);

        // After reserving the write, settle the operation even if the client
        // disconnects. The durable receipts fence effects across daemon exit.
        let router = self.clone();
        tokio::spawn(async move {
            let result = async {
                if let Some(mut response) = started? {
                    router
                        .apply_cleanup_artifacts("owner", &apply, &plan, &mut response, true, false)
                        .await?;
                    require_agent_delete_cleanup(&plan, &response)?;
                }
                router.inner.store.lock().await.finish_agent_delete(
                    &request.id,
                    &request.agent,
                    &agents,
                    now_ms(),
                )
            }
            .await;
            if let Err(error) = &result
                && router
                    .inner
                    .store
                    .lock()
                    .await
                    .fail_agent_delete(&request.id, &request.agent, error)
                    .is_err()
            {
                return Err(Error::new(
                    "unknown",
                    "Agent deletion settlement could not be retained",
                ));
            }
            result
        })
        .await
        .map_err(|_| Error::new("unknown", "Agent deletion did not report a settlement"))?
    }

    async fn plan_agent_deletion(&self, agents: &[Agent]) -> Result<CleanupPlan> {
        let mut items = Vec::new();
        for agent in agents {
            if let Some(artifact) = self.agent_delete_session(agent).await? {
                items.push(CleanupCandidate {
                    artifact,
                    bytes: None,
                    verdict: "safe".to_owned(),
                    reason: "native_archive_pending".to_owned(),
                });
            }

            if let Some(mut artifact) = self.agent_delete_worktree(agent).await? {
                let tip = self
                    .worktree_cleanup_preflight(&artifact, false, false)
                    .await?;
                if let Some(git) = artifact.git.as_mut() {
                    git.planned_tip = tip;
                }
                items.push(CleanupCandidate {
                    artifact,
                    bytes: None,
                    verdict: "safe".to_owned(),
                    reason: "clean_worktree".to_owned(),
                });
            }
            #[cfg(windows)]
            if let Some(artifact) = self.agent_delete_probe_chat(agent).await? {
                let (bytes, absent) = self.cleanup_preflight(&artifact).await?;
                items.push(CleanupCandidate {
                    artifact,
                    bytes: Some(bytes),
                    verdict: "safe".to_owned(),
                    reason: if absent {
                        "already_absent"
                    } else {
                        "terminal_owned_artifact"
                    }
                    .to_owned(),
                });
            }
            if items.len() > MAX_CLEANUP_ITEMS {
                return Err(Error::new(
                    "unavailable",
                    "Agent cleanup exceeds operation bound",
                ));
            }
        }
        Ok(CleanupPlan {
            plan_id: ulid::Ulid::new().to_string(),
            created_at: now_ms(),
            items,
            inventory_issues: Vec::new(),
            truncated: false,
            continuation: None,
        })
    }

    #[cfg(windows)]
    async fn agent_delete_probe_chat(&self, agent: &Agent) -> Result<Option<ArtifactView>> {
        if !agent.chat
            || agent
                .profile_snapshot
                .get(eliot_core::PROVIDER_PROBE_LAUNCH_ARG)
                != Some(&Value::Bool(true))
        {
            return Ok(None);
        }
        let path = agent.workspace.as_deref().ok_or_else(|| {
            Error::new("source_integrity", "provider probe chat has no workspace")
        })?;
        let path = match std::fs::canonicalize(path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                return Err(Error::new(
                    "unavailable",
                    "provider probe chat is unavailable",
                ));
            }
        };
        let store = self.inner.store.lock().await;
        let artifact = store
            .known_artifact_path(&path.to_string_lossy())?
            .filter(|artifact| {
                artifact.agent == agent.id
                    && artifact.generation == agent.generation
                    && artifact.kind == ArtifactKind::Other
                    && artifact.purpose == "Swarm chat directory"
                    && artifact.state == "registered"
            })
            .ok_or_else(|| Error::new("source_integrity", "provider probe chat custody changed"))?;
        Ok(Some(artifact))
    }

    async fn agent_delete_session(&self, agent: &Agent) -> Result<Option<ArtifactView>> {
        if !swarm_created_session_agent(agent)
            || !native_archive_supported(&agent.harness)
            || agent.native_ref.is_none()
        {
            return Ok(None);
        }
        let expected = native_session_artifact(agent).ok_or_else(|| {
            Error::new(
                "native_scope_unverified",
                "Agent's native session has no retained store binding",
            )
        })?;
        let path = expected.path.clone();
        let mut store = self.inner.store.lock().await;
        store.record_known_artifact(expected)?;
        let artifact = store
            .known_native_session_artifact(&agent.id, &path)?
            .ok_or_else(|| Error::new("source_integrity", "native session custody is absent"))?;
        if artifact.state == "removed" {
            return Ok(None);
        }
        frozen_archive_profile(agent, &artifact)?;
        Ok(Some(artifact))
    }

    async fn agent_delete_worktree(&self, agent: &Agent) -> Result<Option<ArtifactView>> {
        let store = self.inner.store.lock().await;
        let Some(workspace) = store.get_workspace(&agent.id)? else {
            return Ok(None);
        };
        if workspace.phase == WorkspacePhase::Removed {
            return Ok(None);
        }
        if workspace.phase == WorkspacePhase::Failed
            && let Err(error) = std::fs::symlink_metadata(&workspace.worktree_path)
            && error.kind() == std::io::ErrorKind::NotFound
        {
            return Ok(None);
        }
        let current = store.known_artifact_path(&workspace.worktree_path)?;
        drop(store);
        let artifact = if let Some(artifact) = current {
            artifact
        } else {
            self.record_agent_artifacts(agent).await?;
            self.inner
                .store
                .lock()
                .await
                .known_artifact_path(&workspace.worktree_path)?
                .ok_or_else(|| {
                    Error::new("source_integrity", "managed worktree custody is absent")
                })?
        };
        if artifact.kind != ArtifactKind::Worktree
            || artifact.agent != agent.id
            || artifact.generation != agent.generation
            || artifact.state != "registered"
        {
            return Err(Error::new("busy", "managed worktree custody changed"));
        }
        Ok(Some(artifact))
    }
}

fn require_agent_delete_cleanup(plan: &CleanupPlan, response: &CleanupApplyResponse) -> Result<()> {
    if let Some((item, _)) = plan
        .items
        .iter()
        .zip(&response.items)
        .find(|(_, outcome)| outcome.outcome == "unknown")
    {
        return Err(Error::new(
            "unknown",
            format!(
                "{} cleanup is uncertain. Swarm kept the task.",
                item.artifact.agent
            ),
        ));
    }
    if let Some((item, outcome)) = plan
        .items
        .iter()
        .zip(&response.items)
        .filter(|(_, outcome)| outcome.outcome != "removed")
        .min_by_key(|(item, _)| item.artifact.kind != ArtifactKind::NativeSession)
    {
        let reason = outcome.reason.as_deref().unwrap_or("cleanup_refused");
        let object = match item.artifact.kind {
            ArtifactKind::NativeSession => "native chat",
            ArtifactKind::Other => "provider probe directory",
            _ => "managed worktree",
        };
        return Err(Error::new(
            reason,
            format!(
                "{} {object} cleanup was refused. Swarm kept the task.",
                item.artifact.agent
            ),
        ));
    }
    Ok(())
}
