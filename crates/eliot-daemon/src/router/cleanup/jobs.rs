//! Explicit cleanup jobs outlive their admission RPC, while the Store owns
//! the original binding, every attempt fence and the visible progress.
use super::*;

const APPLY: &str = "cleanup.apply";
const APPLY_ALL: &str = "cleanup.apply_all";
const OWNER: &str = "owner";

struct CleanupWorker(Router);

impl Drop for CleanupWorker {
    fn drop(&mut self) {
        *self.0.inner.cleanup_worker.lock_recover() = None;
        self.0.signal_progress();
    }
}

impl Router {
    pub(in crate::router) async fn cleanup_apply(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CleanupApplyRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            APPLY,
            CleanupApplyRequest::validate,
        )?;
        if request.delete_remote {
            require_owner(principal)?;
        }
        let name = principal_name(principal);
        let payload = encoded(&request)?;
        let mut store = self.inner.store.lock().await;
        if store
            .cleanup_replay(&name, &request.id, APPLY, &payload)?
            .is_some()
        {
            let active = self.cleanup_worker_is_live(&request.id);
            if !active {
                // The exact retry can also reconcile a previously dispatched
                // worktree command whose durable applied result arrived late.
                store.begin_cleanup_apply(&name, &request, now_ms(), true)?;
            }
            let (_, retained) = store.cleanup_job_receipt(&name, &request.id)?;
            return encoded(visible_apply(decode_receipt(retained)?, active));
        }
        self.check_cleanup_admission()?;
        let (plan, mut response, _) = store.begin_cleanup_apply(&name, &request, now_ms(), true)?;
        let accepted = encoded(visible_apply(response.clone(), true))?;
        let worker = self.claim_cleanup_worker(&request.id);
        tokio::spawn(async move {
            let router = &worker.0;
            let result = router
                .run_cleanup_page(
                    &name,
                    &request,
                    &plan,
                    &mut response,
                    CleanupApplyMode::Manual,
                )
                .await;
            response.state = Some(settlement_state(&result));
            let settle = router.inner.store.lock().await.update_cleanup_job(
                &name,
                &request.id,
                APPLY,
                &response,
                None,
            );
            router.log_cleanup_error(&request.id, result.and(settle));
        });
        Ok(accepted)
    }

    pub(in crate::router) async fn cleanup_apply_all(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: CleanupApplyAllRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            APPLY_ALL,
            CleanupApplyAllRequest::validate,
        )?;
        let payload = encoded(&request)?;
        let mut store = self.inner.store.lock().await;
        if store
            .cleanup_replay(OWNER, &request.id, APPLY_ALL, &payload)?
            .is_some()
        {
            return encoded(self.read_cleanup_status(&store, OWNER, &request.id)?);
        }
        self.check_cleanup_admission()?;
        let mut status = CleanupJobStatus {
            id: request.id.clone(),
            state: CleanupJobState::Running,
            tally: CleanupTally::default(),
            apply: None,
            current_apply_id: None,
            reason: None,
        };
        let accepted = encoded(&status)?;
        store.record_write(&request.id, OWNER, APPLY_ALL, &payload, &accepted, now_ms())?;
        let worker = self.claim_cleanup_worker(&request.id);
        tokio::spawn(async move {
            let router = &worker.0;
            let result = router
                .run_bulk_cleanup(request.plan_request(), &mut status)
                .await;
            status.state = settlement_state(&result);
            status.reason = result.as_ref().err().map(|error| error.code.clone());
            let settle = router.persist_bulk_status(&status).await;
            router.log_cleanup_error(&status.id, result.and(settle));
        });
        Ok(accepted)
    }

    pub(in crate::router) async fn cleanup_status(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CleanupStatusRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "cleanup.status",
            CleanupStatusRequest::validate,
        )?;
        let store = self.inner.store.lock().await;
        encoded(self.read_cleanup_status(&store, &principal_name(principal), &request.id)?)
    }

    fn check_cleanup_admission(&self) -> Result<()> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new("bad_state", "daemon admission is closed"));
        }
        if self.inner.cleanup_worker.lock_recover().is_some() {
            return Err(Error::new("busy", "a cleanup apply is already running"));
        }
        Ok(())
    }

    // Admission and worker registration both run behind the Store lock.
    fn claim_cleanup_worker(&self, id: &str) -> CleanupWorker {
        *self.inner.cleanup_worker.lock_recover() = Some(id.to_owned());
        CleanupWorker(self.clone())
    }

    fn cleanup_worker_is_live(&self, id: &str) -> bool {
        self.inner.cleanup_worker.lock_recover().as_deref() == Some(id)
    }

    fn read_cleanup_status(
        &self,
        store: &eliot_store::Store,
        principal: &str,
        id: &str,
    ) -> Result<CleanupJobStatus> {
        let (method, value) = store.cleanup_job_receipt(principal, id)?;
        let live = self.cleanup_worker_is_live(id);
        if method == APPLY {
            return page_status(id, value, live);
        }
        let mut status: CleanupJobStatus = decode_receipt(value)?;
        if status.state == CleanupJobState::Running && !live {
            status.state = CleanupJobState::Interrupted;
            status.reason = Some("cleanup_worker_lost".to_owned());
        }
        if let Some(page) = status.current_apply_id.take() {
            match store.cleanup_job_receipt(principal, &page) {
                Ok((_, value)) => status.tally.count(
                    &decode_receipt(value)?,
                    status.state == CleanupJobState::Running,
                ),
                Err(error) if error.code == "not_found" => {}
                Err(error) => return Err(error),
            }
        }
        Ok(status)
    }

    async fn run_cleanup_page(
        &self,
        name: &str,
        request: &CleanupApplyRequest,
        plan: &CleanupPlan,
        response: &mut CleanupApplyResponse,
        mode: CleanupApplyMode<'_>,
    ) -> Result<()> {
        {
            let mut store = self.inner.store.lock().await;
            self.check_cleanup_worker_shutdown()?;
            self.apply_project_registrations(&mut store, request, plan, response)?;
        }
        self.apply_cleanup_artifacts(name, request, plan, response, mode)
            .await
    }

    async fn run_bulk_cleanup(
        &self,
        mut request: CleanupPlanRequest,
        status: &mut CleanupJobStatus,
    ) -> Result<()> {
        let mut native_kept = HashSet::new();
        loop {
            self.check_cleanup_worker_shutdown()?;
            let params = encoded(&request)?;
            let mut page = self
                .cleanup_plan_page(&params, &Principal::Owner, None, false, true)
                .await?;
            status.tally.inventory_issues += page.inventory_issues.len() as u64;
            let after = page.continuation.take();
            self.apply_bulk_page(page, status, &mut native_kept).await?;
            let Some(after) = after else { break };
            if request.continuation.as_ref() == Some(&after) {
                return Err(Error::new("internal", "cleanup inventory did not advance"));
            }
            request.continuation = Some(after);
        }

        // Project removals change the settings revision used by preview
        // continuations. Snapshot project identities once, then recheck each
        // bounded batch against its current settings revision.
        let (projects, _) = self.plan_project_registrations(&request).await?;
        let mut initial = projects;
        for batch in initial.chunks_mut(MAX_CLEANUP_ITEMS) {
            self.check_cleanup_worker_shutdown()?;
            let (current, _) = self.plan_project_registrations(&request).await?;
            let by_id: HashMap<_, _> = current
                .into_iter()
                .map(|item| (item.artifact.project.clone(), item))
                .collect();
            for item in batch.iter_mut().filter(|item| item.verdict == "safe") {
                if let Some(current) = by_id.get(&item.artifact.project)
                    && current.artifact.path == item.artifact.path
                {
                    *item = current.clone();
                } else {
                    item.verdict = "keep".to_owned();
                    item.reason = "identity_changed".to_owned();
                }
            }
            let now = now_ms();
            let plan = CleanupPlan {
                plan_id: ulid::Ulid::new().to_string(),
                created_at: now,
                items: batch.to_vec(),
                inventory_issues: Vec::new(),
                truncated: false,
                continuation: None,
            };
            if plan.items.iter().any(|item| item.verdict == "safe") {
                self.inner
                    .store
                    .lock()
                    .await
                    .retain_cleanup_plan(OWNER, &plan, now)?;
            }
            self.apply_bulk_page(plan, status, &mut native_kept).await?;
        }
        Ok(())
    }

    async fn apply_bulk_page(
        &self,
        plan: CleanupPlan,
        status: &mut CleanupJobStatus,
        native_kept: &mut HashSet<String>,
    ) -> Result<()> {
        if plan.items.iter().any(|item| item.verdict == "safe") {
            let request = CleanupApplyRequest {
                id: ulid::Ulid::new().to_string(),
                plan_id: plan.plan_id.clone(),
                delete_remote: false,
                artifacts: None,
            };
            status.current_apply_id = Some(request.id.clone());
            self.persist_bulk_status(status).await?;
            let (_, mut response, _) = self.inner.store.lock().await.begin_cleanup_apply(
                OWNER,
                &request,
                now_ms(),
                false,
            )?;
            self.run_cleanup_page(
                OWNER,
                &request,
                &plan,
                &mut response,
                CleanupApplyMode::Bulk(native_kept),
            )
            .await?;
            for (item, outcome) in plan.items.iter().zip(&response.items) {
                if item.artifact.kind == ArtifactKind::NativeSession
                    && item.verdict == "safe"
                    && outcome.outcome != "removed"
                {
                    native_kept.insert(
                        cleanup_tree_root(&item.artifact)
                            .unwrap_or(&item.artifact.agent)
                            .to_owned(),
                    );
                }
            }
            status.tally.count(&response, false);
            status.current_apply_id = None;
        } else {
            for item in plan.items {
                *status.tally.skipped.entry(item.reason).or_default() += 1;
            }
        }
        status.tally.pages += 1;
        self.persist_bulk_status(status).await
    }

    async fn persist_bulk_status(&self, status: &CleanupJobStatus) -> Result<()> {
        self.inner.store.lock().await.update_cleanup_job(
            OWNER,
            &status.id,
            APPLY_ALL,
            status,
            (status.state != CleanupJobState::Running).then(now_ms),
        )
    }

    pub(super) fn check_cleanup_worker_shutdown(&self) -> Result<()> {
        if self.is_force_requested() {
            return Err(Error::new(
                "interrupted",
                "forced shutdown interrupted cleanup",
            ));
        }
        Ok(())
    }

    fn log_cleanup_error(&self, id: &str, result: Result<()>) {
        if let Err(error) = result {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.cleanup_worker_failed",
                &[("cleanup_id", id)],
                Some(&error),
            );
        }
    }
}

fn settlement_state(result: &Result<()>) -> CleanupJobState {
    match result {
        Ok(()) => CleanupJobState::Completed,
        Err(error) if error.code == "interrupted" => CleanupJobState::Interrupted,
        Err(_) => CleanupJobState::Failed,
    }
}

fn decode_receipt<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|_| Error::new("source_integrity", "invalid cleanup receipt"))
}

fn visible_apply(mut apply: CleanupApplyResponse, live: bool) -> CleanupApplyResponse {
    if apply.state == Some(CleanupJobState::Running) {
        if live {
            for item in &mut apply.items {
                if item.outcome == "unknown" && item.reason.as_deref() == Some("attempt_reserved") {
                    item.outcome = "pending".to_owned();
                }
            }
        } else {
            apply.state = Some(CleanupJobState::Interrupted);
        }
    }
    apply
}

fn page_status(id: &str, value: Value, live: bool) -> Result<CleanupJobStatus> {
    let apply: CleanupApplyResponse = decode_receipt(value)?;
    let mut tally = CleanupTally {
        pages: 1,
        ..CleanupTally::default()
    };
    tally.count(&apply, live);
    let apply = visible_apply(apply, live);
    let state = apply.state.unwrap_or(CleanupJobState::Completed);
    Ok(CleanupJobStatus {
        id: id.to_owned(),
        state,
        tally,
        apply: Some(apply),
        current_apply_id: None,
        reason: match state {
            CleanupJobState::Failed => Some("cleanup_worker_failed".to_owned()),
            CleanupJobState::Interrupted => Some("cleanup_worker_interrupted".to_owned()),
            _ => None,
        },
    })
}
