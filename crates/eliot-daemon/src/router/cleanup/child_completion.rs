//! Direct-child completion handling in the existing K69 sweep.

use super::*;
use eliot_store::{ChildCleanupJob, ChildCleanupOutcome, ChildCleanupState};

const CHILD_CLEANUP_PAGE: usize = 4;
const CHILD_CLEANUP_RECHECK_MS: i64 = 60_000;

impl Router {
    pub(in crate::router) async fn record_child_cleanup_result_read(
        &self,
        principal: &Principal,
        child: &str,
        generation: i64,
        turn: &str,
    ) {
        let Principal::Agent {
            id: parent,
            generation: parent_generation,
        } = principal
        else {
            return;
        };
        if let Err(error) = self.inner.store.lock().await.record_child_cleanup_result(
            parent,
            *parent_generation,
            child,
            generation,
            turn,
            now_ms(),
        ) && !matches!(error.code.as_str(), "bad_state" | "not_found")
        {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.child_cleanup_receipt_failed",
                &[("agent", child)],
                Some(&error),
            );
        }
    }

    pub(super) async fn sweep_child_worktree_cleanup(&self) -> Result<()> {
        self.check_child_cleanup_enabled()?;
        let jobs = self
            .inner
            .store
            .lock()
            .await
            .child_cleanup_due(now_ms(), CHILD_CLEANUP_PAGE)?;
        for job in jobs {
            self.check_child_cleanup_enabled()?;
            let mut job = self
                .inner
                .store
                .lock()
                .await
                .reserve_child_cleanup_ids(&job.child, job.child_generation)?;
            if job.state == ChildCleanupState::Pending {
                match self.clean_completed_child_worktree(&job).await {
                    Ok(true) => {
                        self.inner.store.lock().await.finish_child_cleanup(
                            &job.child,
                            job.child_generation,
                            ChildCleanupOutcome::Removed,
                            "removed",
                            None,
                        )?;
                        continue;
                    }
                    Ok(false) => {
                        self.defer_child_cleanup(&job, "waiting_for_stop").await?;
                        continue;
                    }
                    Err(error) if error.code == "interrupted" => return Err(error),
                    Err(error)
                        if error.code == "busy"
                            && error.message.as_ref()
                                == "worktree has not been quiet for the required removal period" =>
                    {
                        self.defer_child_cleanup(&job, &error.message).await?;
                        continue;
                    }
                    Err(error) => {
                        job.reason = error.code;
                        job.detail = Some(
                            self.secret_policy_snapshot()
                                .redact_bounded(&error.message, 512),
                        );
                        self.inner.store.lock().await.finish_child_cleanup(
                            &job.child,
                            job.child_generation,
                            ChildCleanupOutcome::Kept,
                            &job.reason,
                            job.detail.as_deref(),
                        )?;
                    }
                }
            }
            if let Err(error) = self.send_child_cleanup_notice(&job).await {
                self.defer_child_cleanup(&job, &error.code).await?;
            }
        }
        Ok(())
    }

    fn check_child_cleanup_enabled(&self) -> Result<()> {
        self.check_cleanup_worker_shutdown()?;
        if crate::config::load_automation_config(&self.inner.data_root)?.auto_cleanup == Some(false)
        {
            return Err(Error::new("interrupted", "Owner paused automatic cleanup"));
        }
        Ok(())
    }

    async fn defer_child_cleanup(&self, job: &ChildCleanupJob, reason: &str) -> Result<()> {
        self.inner.store.lock().await.retry_child_cleanup_after(
            &job.child,
            job.child_generation,
            now_ms().saturating_add(CHILD_CLEANUP_RECHECK_MS),
            reason,
        )
    }

    async fn clean_completed_child_worktree(&self, job: &ChildCleanupJob) -> Result<bool> {
        let (child, workspace) = {
            let store = self.inner.store.lock().await;
            let child = store
                .get_agent(&job.child)?
                .ok_or_else(|| Error::new("not_found", "child missing"))?;
            let parent = store
                .get_agent(&job.parent)?
                .ok_or_else(|| Error::new("not_found", "manager missing"))?;
            if child.generation != job.child_generation
                || child.parent.as_deref() != Some(&job.parent)
                || parent.generation != job.parent_generation
            {
                return Err(Error::new(
                    "stale_generation",
                    "child or manager binding changed",
                ));
            }
            if child.archived {
                return Err(Error::new("owner_hold", "the child is archived"));
            }
            let workspace = store
                .get_workspace(&job.child)?
                .ok_or_else(|| Error::new("not_found", "managed worktree missing"))?;
            (child, workspace)
        };
        if workspace.target_generation != job.child_generation {
            return Err(Error::new(
                "stale_generation",
                "worktree generation changed",
            ));
        }
        if workspace.phase == eliot_core::WorkspacePhase::Removed {
            return Ok(true);
        }
        if !matches!(child.state, AgentState::Idle | AgentState::Stopped) {
            return Err(Error::new(
                "child_not_quiescent",
                "child no longer has an idle or stopped generation",
            ));
        }
        let mut artifact = self
            .inner
            .store
            .lock()
            .await
            .known_artifact_path(&workspace.worktree_path)?;
        if artifact.is_none() {
            self.record_agent_artifacts(&child).await?;
            artifact = self
                .inner
                .store
                .lock()
                .await
                .known_artifact_path(&workspace.worktree_path)?;
        }
        let artifact = artifact.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "worktree artifact ownership is unavailable",
            )
        })?;
        if artifact.kind != ArtifactKind::Worktree
            || artifact.generation != job.child_generation
            || artifact.agent != job.child
        {
            return Err(Error::new(
                "identity_changed",
                "worktree artifact owner changed",
            ));
        }
        if artifact.state != "registered" {
            return Err(Error::new(
                artifact.state,
                "worktree artifact is not registered",
            ));
        }
        if artifact
            .expires_at
            .is_some_and(|expires| expires > now_ms())
        {
            return Err(Error::new(
                "not_expired",
                "worktree is held until its recorded expiry",
            ));
        }
        if let Some(reason) = self
            .inner
            .store
            .lock()
            .await
            .cleanup_worktree_blocker(&artifact)?
        {
            return Err(Error::new(
                reason,
                "worktree has a retained assignment or borrower",
            ));
        }
        if child.state == AgentState::Idle {
            self.stop_idle_child_for_cleanup(job, &workspace, &artifact)
                .await?;
            return Ok(false);
        }
        self.apply_completed_child_plan(job, artifact).await
    }

    async fn stop_idle_child_for_cleanup(
        &self,
        job: &ChildCleanupJob,
        workspace: &eliot_core::WorkspacePreparation,
        artifact: &ArtifactView,
    ) -> Result<()> {
        let stop_id = child_cleanup_id(&job.stop_id)?;
        let retained = {
            let store = self.inner.store.lock().await;
            let retained = store.get_command(stop_id)?;
            if retained.is_none() {
                store.child_cleanup_stop_guard(&job.child, job.child_generation)?;
            }
            retained
        };
        if let Some(retained) = retained {
            if matches!(
                retained.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
            ) {
                return Ok(());
            }
            let reason = retained
                .outcome
                .as_ref()
                .and_then(|outcome| {
                    outcome["code"]
                        .as_str()
                        .or_else(|| outcome["reason"].as_str())
                })
                .unwrap_or("stop_unconfirmed");
            return Err(Error::new(
                reason,
                "the reserved cleanup stop did not prove exit",
            ));
        }
        check_worktree_artifact(artifact, workspace)?;
        let git = crate::workspace_git::WorkspaceGit::new()?;
        let deadline = Instant::now() + CLEANUP_PLAN_MAX_PREFLIGHT;
        let preflight = self
            .inner
            .workspaces
            .removal_candidates(&git, workspace, &json!({"scratch": []}), None, deadline)
            .await?;
        git.require_automatic_removal_merge(workspace, &preflight.tip, deadline)
            .await?;
        self.check_child_cleanup_enabled()?;
        let admission = {
            let mut store = self.inner.store.lock().await;
            store.child_cleanup_stop_guard(&job.child, job.child_generation)?;
            let mut request = SubmitCommand::new(stop_id, "stop");
            request.target = Some(job.child.clone());
            request.expected_generation = Some(job.child_generation);
            request.args = json!({"force": false});
            let admission = self.admit_prepared_submission(
                &mut store,
                "owner",
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
        Ok(())
    }

    async fn apply_completed_child_plan(
        &self,
        job: &ChildCleanupJob,
        mut artifact: ArtifactView,
    ) -> Result<bool> {
        let plan_id = child_cleanup_id(&job.plan_id)?;
        let apply = CleanupApplyRequest {
            id: child_cleanup_id(&job.apply_id)?.to_owned(),
            plan_id: plan_id.to_owned(),
            artifacts: None,
            delete_remote: false,
        };
        let existing = self.inner.store.lock().await.cleanup_plan("owner", plan_id);
        match existing {
            Ok(_) => {}
            Err(error) if error.code == "not_found" => {
                let tip = self
                    .worktree_cleanup_preflight(&artifact, true, false)
                    .await?
                    .ok_or_else(|| {
                        Error::new("merge_proof_unavailable", "no current worktree tip")
                    })?;
                self.automatic_cleanup_worktree_merged(&artifact, &tip)
                    .await?;
                if let Some(git) = artifact.git.as_mut() {
                    git.planned_tip = Some(tip);
                }
                let plan = CleanupPlan {
                    plan_id: plan_id.to_owned(),
                    created_at: now_ms(),
                    items: vec![CleanupCandidate {
                        artifact,
                        bytes: None,
                        verdict: "safe".to_owned(),
                        reason: "clean_worktree".to_owned(),
                    }],
                    inventory_issues: Vec::new(),
                    truncated: false,
                    continuation: None,
                };
                self.inner
                    .store
                    .lock()
                    .await
                    .retain_cleanup_plan("owner", &plan, now_ms())?;
            }
            Err(error) => return Err(error),
        }
        let (plan, mut response, is_new) =
            self.inner
                .store
                .lock()
                .await
                .begin_cleanup_apply("owner", &apply, now_ms(), false)?;
        if is_new {
            self.apply_cleanup_artifacts(
                "owner",
                &apply,
                &plan,
                &mut response,
                CleanupApplyMode::ChildCompletion(job),
            )
            .await?;
        }
        let outcome = response.items.first().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "cleanup apply returned no worktree result",
            )
        })?;
        if outcome.outcome == "removed" {
            return Ok(true);
        }
        let code = outcome
            .reason
            .as_deref()
            .unwrap_or(outcome.outcome.as_str());
        Err(Error::new(
            code,
            outcome
                .detail
                .as_deref()
                .unwrap_or("guarded cleanup kept the worktree"),
        ))
    }

    pub(super) async fn child_completion_effect_gate(
        &self,
        job: &ChildCleanupJob,
        artifact: &ArtifactView,
    ) -> Result<()> {
        self.check_child_cleanup_enabled()?;
        if artifact.kind != ArtifactKind::Worktree
            || artifact.agent != job.child
            || artifact.generation != job.child_generation
        {
            return Err(Error::new(
                "source_integrity",
                "cleanup plan does not match the completed child",
            ));
        }
        {
            let store = self.inner.store.lock().await;
            let child = store
                .get_agent(&job.child)?
                .ok_or_else(|| Error::new("not_found", "child missing"))?;
            let parent = store.get_agent(&job.parent)?;
            if child.generation != job.child_generation
                || child.parent.as_deref() != Some(&job.parent)
                || parent.is_none_or(|parent| parent.generation != job.parent_generation)
                || child.state != AgentState::Stopped
            {
                return Err(Error::new(
                    "stale_generation",
                    "child cleanup binding changed",
                ));
            }
            if child.archived
                || artifact
                    .expires_at
                    .is_some_and(|expires| expires > now_ms())
            {
                return Err(Error::new("owner_hold", "worktree is held"));
            }
        }
        let tip = artifact
            .git
            .as_ref()
            .and_then(|git| git.planned_tip.as_deref())
            .ok_or_else(|| {
                Error::new("merge_proof_unavailable", "planned worktree tip is missing")
            })?;
        self.automatic_cleanup_worktree_merged(artifact, tip).await
    }

    async fn send_child_cleanup_notice(&self, job: &ChildCleanupJob) -> Result<()> {
        let request = MailSendRequest {
            id: child_cleanup_id(&job.notice_id)?.to_owned(),
            to: vec![job.parent.clone()],
            text: format!(
                "Swarm kept the worktree for child {} generation {} ({}). {}",
                job.child,
                job.child_generation,
                job.reason,
                job.detail.as_deref().unwrap_or("")
            ),
            mode: MailMode::Queue,
            wake: true,
            thread: None,
            reply_to_delivery: None,
        };
        let change = {
            let mut store = self.inner.store.lock().await;
            let parent = store.get_agent(&job.parent)?;
            let child = store.get_agent(&job.child)?;
            let in_scope = parent.is_some_and(|agent| agent.generation == job.parent_generation)
                && child.is_some_and(|agent| {
                    agent.parent.as_deref() == Some(&job.parent)
                        && agent.generation == job.child_generation
                });
            in_scope
                .then(|| self.mail_send_locked(&mut store, SWARM_MAIL_SENDER, &request, false))
                .transpose()?
        };
        if let Some(change) = change {
            self.mail_send_finish(&request, change).await?;
        }
        self.inner
            .store
            .lock()
            .await
            .mark_child_cleanup_notice_delivered(&job.child, job.child_generation)
    }
}

fn child_cleanup_id(id: &Option<String>) -> Result<&str> {
    id.as_deref()
        .ok_or_else(|| Error::new("source_integrity", "child cleanup action ID missing"))
}
