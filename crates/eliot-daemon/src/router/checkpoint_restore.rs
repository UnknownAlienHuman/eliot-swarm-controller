use super::*;
use eliot_core::api::{CheckpointRestoreRequest, CheckpointRestoreResponse};
use eliot_store::{CheckpointRestoreAdmission, CheckpointRestoreIntent};

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct RestoreDecision {
    id: String,
    restore_id: String,
    confirm: Option<bool>,
    reason: Option<String>,
}

impl Router {
    pub(super) async fn checkpoint_restore(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let mut request: CheckpointRestoreRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let retained = self
            .inner
            .store
            .lock()
            .await
            .retained_checkpoint_restore(&request)?;
        if let Some(outcome) = retained {
            self.release_settled_restore(&request.id).await?;
            return encode_outcome(outcome?);
        }
        let (workspace, common_dir) = crate::workspace_git::WorkspaceGit::new()?
            .repository_identity(&request.workspace)
            .await?;
        let _guard = self.workspace_check_lock(&workspace).lock_owned().await;
        let admission = self.inner.store.lock().await.reserve_checkpoint_restore(
            &request,
            &workspace,
            &common_dir,
            now_ms(),
        )?;
        let oid = match admission {
            CheckpointRestoreAdmission::Replay(outcome) => return encode_outcome(outcome?),
            CheckpointRestoreAdmission::Start { oid } => oid,
        };
        request.workspace = workspace;
        let limits = crate::config::load_daemon_config(&self.inner.data_root)
            .unwrap_or_default()
            .checkpoint_limits();
        let policy = self.secret_policy_snapshot();
        let writer = crate::work_truth::CheckpointRestoreWriter {
            store: &self.inner.store,
            id: &request.id,
        };
        let outcome = crate::work_truth::restore_checkpoint(
            &self.inner.data_root,
            &request,
            &oid,
            &common_dir,
            &policy,
            limits,
            &writer,
        )
        .await
        .map(|restored| CheckpointRestoreResponse {
            head: restored.head,
            paths: restored.paths,
        })
        .map_err(|mut error| {
            error.message = Box::new(policy.redact_text(&error.message));
            error
        });
        let outcome = self
            .inner
            .store
            .lock()
            .await
            .settle_checkpoint_restore(&request.id, outcome)?;
        self.signal_progress();
        encode_outcome(outcome?)
    }

    pub(crate) async fn recover_checkpoint_restores(&self) -> Result<()> {
        let intents = self
            .inner
            .store
            .lock()
            .await
            .checkpoint_restore_intents(None)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        for intent in intents {
            let probe = Self::await_restore_writer_exit(&intent, deadline).await;
            self.reconcile_restore_writer(&intent.id, probe).await?;
        }
        Ok(())
    }

    async fn release_settled_restore(&self, id: &str) -> Result<()> {
        let intents = self
            .inner
            .store
            .lock()
            .await
            .checkpoint_restore_intents(Some(id))?;
        if let Some(intent) = intents.first()
            && intent.writer_boot.is_some()
        {
            self.reconcile_restore_writer(id, crate::work_truth::restore_writer_closed(intent))
                .await?;
        }
        Ok(())
    }

    pub(super) async fn recheck_checkpoint_restore_writers(&self, after: &str) -> Result<String> {
        let intents = self
            .inner
            .store
            .lock()
            .await
            .pending_checkpoint_restore_writers(after)?;
        let mut cursor = String::new();
        for intent in intents {
            let probe = crate::work_truth::restore_writer_closed(&intent);
            self.reconcile_restore_writer(&intent.id, probe).await?;
            cursor = intent.id;
        }
        Ok(cursor)
    }

    async fn reconcile_restore_writer(&self, id: &str, probe: Result<bool>) -> Result<()> {
        let (closed, diagnostic) = match probe {
            Ok(closed) => (closed, None),
            Err(error) => {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "checkpoint.restore.custody_check_failed",
                    &[("id", id)],
                    Some(&error),
                );
                (false, Some(error.code))
            }
        };
        self.inner.store.lock().await.recover_checkpoint_restore(
            id,
            closed,
            diagnostic.as_deref(),
        )?;
        if closed {
            self.signal_progress();
        }
        Ok(())
    }

    pub(super) async fn release_checkpoint_restore(
        &self,
        params: &Value,
        principal: &Principal,
        abandon: bool,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: RestoreDecision = eliot_core::api::discovery::decode_params(params.clone())?;
        let reason = if abandon {
            request
                .reason
                .as_deref()
                .filter(|_| request.confirm == Some(true))
                .ok_or_else(|| {
                    Error::new(
                        "invalid",
                        "abandoning restore requires explicit confirmation and a reason",
                    )
                })
                .map(Some)?
        } else {
            None
        };
        let mut store = self.inner.store.lock().await;
        super::owner_inbox::check_binding(&store)?;
        let writer_closed = if abandon {
            false
        } else {
            match store
                .checkpoint_restore_intents(Some(&request.restore_id))?
                .first()
            {
                Some(intent) => crate::work_truth::restore_writer_closed(intent)?,
                None => false,
            }
        };
        let result = store.release_checkpoint_restore(
            &request.id,
            &request.restore_id,
            writer_closed,
            reason,
            now_ms(),
        )?;
        drop(store);
        self.signal_progress();
        Ok(result)
    }

    pub(super) async fn stop_checkpoint_restore(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: RestoreDecision = eliot_core::api::discovery::decode_params(params.clone())?;
        let intent = {
            let mut store = self.inner.store.lock().await;
            super::owner_inbox::check_binding(&store)?;
            let admission = store.record_write(
                &request.id,
                "owner",
                "checkpoint.restore.stop",
                &json!({"restore_id": request.restore_id}),
                &Value::Null,
                now_ms(),
            )?;
            if !admission.is_new {
                if admission.result.is_null() {
                    return Err(Error::new(
                        "unknown",
                        "restore writer stop outcome is unrecorded",
                    ));
                }
                return serde_json::from_value(admission.result).map_err(|_| {
                    Error::new("source_integrity", "restore writer stop receipt is invalid")
                })?;
            }
            store
                .checkpoint_restore_intents(Some(&request.restore_id))?
                .pop()
        };
        let outcome = if let Some(intent) = intent {
            tokio::task::spawn_blocking(move || crate::work_truth::stop_restore_writer(&intent))
                .await
                .unwrap_or_else(|_| {
                    Err(Error::new(
                        "unknown",
                        "restore writer stop task did not settle",
                    ))
                })
        } else {
            Err(Error::new(
                "stale_item",
                "checkpoint restore no longer needs a writer stop",
            ))
        };
        self.reconcile_restore_writer(&request.restore_id, outcome.clone().map(|()| true))
            .await?;
        let response =
            outcome.map(|()| json!({"restore_id": request.restore_id, "writer_closed": true}));
        self.inner
            .store
            .lock()
            .await
            .settle_checkpoint_restore_stop(&request.id, &response)?;
        response
    }

    async fn await_restore_writer_exit(
        intent: &CheckpointRestoreIntent,
        deadline: tokio::time::Instant,
    ) -> Result<bool> {
        loop {
            match crate::work_truth::restore_writer_closed(intent) {
                Ok(false) if tokio::time::Instant::now() < deadline => {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "Restart briefly waits for the exact interrupted Git writer to exit."
                    )]
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                result => return result,
            }
        }
    }
}

fn encode_outcome(outcome: CheckpointRestoreResponse) -> Result<Value> {
    serde_json::to_value(outcome)
        .map_err(|_| Error::new("internal", "checkpoint restore result cannot be encoded"))
}
