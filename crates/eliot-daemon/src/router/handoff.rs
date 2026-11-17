//! Owner transfer admission; delivery stays on ordinary Router command workers.
use super::*;
use eliot_core::api::{CommandCancelRequest, HandoffRequest};

impl Router {
    pub(super) async fn swarm_handoff(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
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
        require_owner(principal)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn derived_launch_input_is_readable_but_cancel_refuses_its_scope() {
        let router = super::super::tests::router();
        let root = ulid::Ulid::new().to_string();
        let derived = format!("{root}/send");
        let before = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(root.clone(), "launch");
            launch.expected_generation = Some(0);
            launch.args = json!({"project":"k120", "workspace":"k120-unit", "harness":"claude", "profile":"chk-claude", "profile_snapshot":{"control":"self"}, "text":"read README.md"});
            let admission = store.submit("owner", launch, now_ms()).unwrap();
            let input = admission.linked_command.unwrap();
            assert_eq!(input.id, derived);
            assert!(matches!(
                input.state,
                CommandState::Accepted | CommandState::Queued
            ));
            input
        };
        let owner = |payload| json!({"_auth":{"kind":"owner","credential":"owner-credential"},"_payload":payload});
        let read = router
            .handle("command.get", owner(json!({"id":derived})))
            .await
            .unwrap();
        assert_eq!(read["id"], derived);
        let children = router
            .handle("command.children", owner(json!({"id":root})))
            .await
            .unwrap();
        assert_eq!(children["children"][0]["id"], derived);
        let error = router
            .handle(
                "command.cancel",
                owner(json!({"id":ulid::Ulid::new().to_string(),"command_id":derived})),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        assert!(error.message.contains("launch/resume inputs"));
        let error = router.handle("command.resolve", owner(json!({"id":ulid::Ulid::new().to_string(),"command_id":derived,"as":"failed","reason":"owner assertion"}))).await.unwrap_err();
        assert_eq!(error.code, "bad_state");
        let store = router.inner.store.lock().await;
        let after = store.get_command(&derived).unwrap().unwrap();
        assert_eq!(before, after);
        assert_eq!(after.turn_id, None);
    }
}
