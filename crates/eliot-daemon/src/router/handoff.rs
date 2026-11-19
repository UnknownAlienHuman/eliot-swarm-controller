//! Owner transfer admission; delivery stays on ordinary Router command workers.
use super::*;
use eliot_core::api::{CommandCancelRequest, HandoffRequest};

impl Router {
    pub(super) async fn swarm_handoff(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        // Authority lives in the Store: Owner anywhere with claims/Goal/children,
        // or a manager for two workers (never itself) in its own controlled scope
        // for caller-issued task inputs only, never moving children. Reparent and
        // switch stay Owner-only.
        let request: HandoffRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
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
            let change = store.handoff(&principal_name(principal), &request, &policy, now_ms())?;
            self.publish_locked(&change.events);
            // No await between commit and scheduling: cancellation cannot strand inputs.
            if change.is_new {
                for child in &change.value.children {
                    self.schedule_command(child.clone(), None);
                }
            }
            change
        };
        self.signal_progress();
        if change.is_new {
            self.notify_claim_conflicts(&change.value.claim_conflicts)
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
