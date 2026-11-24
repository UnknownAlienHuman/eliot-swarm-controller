//! Owner transfer admission; delivery stays on ordinary Router command workers.
use super::*;
use eliot_core::api::{CommandCancelRequest, HandoffRequest};

impl Router {
    pub(super) async fn swarm_handoff(
        &self,
        params: &Value,
        principal: &Principal,
        owner_process: Option<&OwnerProcess>,
    ) -> Result<Value> {
        // Authority lives in the Store: Owner anywhere with claims/Goal/children,
        // or a manager for two workers (never itself) in its own controlled scope
        // for caller-issued task inputs only, never moving children.
        let request: HandoffRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        {
            let store = self.inner.store.lock().await;
            if let Some(value) = store.replay_handoff(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode handoff replay"));
            }
            store.check_handoff_caller(&principal_name(principal), &request)?;
        }
        let source = self
            .inner
            .store
            .lock()
            .await
            .get_agent(&request.from)?
            .filter(|source| source.generation == request.from_generation)
            .ok_or_else(|| Error::new("stale_generation", "handoff source generation moved"))?;
        let task = self
            .prepare_handoff_task(&source, Some(&request.task_command))
            .await?;
        let git = self.finish_handoff_facts(&source, task).await?;
        let change = {
            let mut store = self.inner.store.lock().await;
            if let Some(value) = store.replay_handoff(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode handoff replay"));
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new("unavailable", "drain prevents new handoffs"));
            }
            if policy.contains_value(params) {
                return Err(Error::new(
                    "secret_in_payload",
                    "handoff request contains a known credential",
                ));
            }
            let change = store.handoff(
                &principal_name(principal),
                &request,
                &policy,
                owner_process,
                &git,
                now_ms(),
            )?;
            self.publish_locked(&change.events);
            // No await between commit and scheduling: cancellation cannot strand inputs.
            if change.is_new {
                self.queue_handoff_task_card(
                    &mut store,
                    &change.value,
                    &change.events,
                    git.applied_task_text(),
                );
                for child in &change.value.children {
                    self.schedule_command(child.clone(), None);
                }
            }
            change
        };
        self.signal_progress();
        if change.is_new {
            self.notify_claim_conflicts(
                &change.value.to,
                &change.value.claims,
                &change.value.claim_conflicts,
                None,
            )
            .await;
        }
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode handoff"))
    }

    pub(super) async fn command_cancel(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        // Authority lives in the Store: Owner withdraws handoff inputs, or a
        // manager cancels a send it issued itself in its controlled scope
        // (handoff child, ordinary SEND or own selector SEND child).
        let request: CommandCancelRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let change = {
            let mut store = self.inner.store.lock().await;
            super::owner_inbox::check_binding(&store)?;
            let change =
                store.cancel_handoff_input(&principal_name(principal), &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode cancellation"))
    }
}
