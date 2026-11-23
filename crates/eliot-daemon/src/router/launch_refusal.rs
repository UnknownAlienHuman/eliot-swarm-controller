use super::*;

impl Router {
    pub(super) async fn reconcile_launch_refusal(&self, event: &Event) -> Result<()> {
        let initial_refusal = event.kind == "turn.completed"
            && event.source == "stream"
            && event.data["model_unavailable"] == true;
        let stopped = event.kind == "command.applied"
            && event.source == "daemon"
            && event.data["outcome"]["process_custody_exit_proven"] == true
            && event.data["outcome"]["native_source_drain_proven"] == true;
        let failed_start = event.kind == "command.failed"
            && event.source == "daemon"
            && (event.data["outcome"]["native_start_refused"] == true
                || event.data["outcome"]["unspawned_start"] == true);
        if !(initial_refusal || stopped || failed_start) {
            return Ok(());
        }
        let (Some(id), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(());
        };
        let admission = {
            let mut store = self.inner.store.lock().await;
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(());
            }
            let Some(agent) = store
                .get_agent(id)?
                .filter(|agent| agent.generation == generation)
            else {
                return Ok(());
            };
            let mut request = if initial_refusal {
                let Some(turn) = event.data["turn_id"].as_str() else {
                    return Ok(());
                };
                if store
                    .refused_initial_model_workspace(&agent, turn, None)?
                    .is_none()
                {
                    return Ok(());
                }
                let reason = event.data["native_error"]["message"]
                    .as_str()
                    .unwrap_or("OpenCode refused the requested model before inference");
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.launch_refused",
                    &[("agent", id), ("turn_id", turn)],
                    Some(&Error::new("model_unavailable", reason)),
                );
                let mut stop = SubmitCommand::new(ulid::Ulid::new().to_string(), "stop");
                stop.args = json!({"refused_launch_turn": turn});
                stop
            } else {
                let Some(command_id) = event.data["command_id"].as_str() else {
                    return Ok(());
                };
                if failed_start {
                    let outcome = &event.data["outcome"];
                    let reason = outcome["message"]
                        .as_str()
                        .or_else(|| outcome["reason"].as_str())
                        .unwrap_or("Native start refused before work");
                    let code = outcome["code"].as_str().unwrap_or("start_refused");
                    let mut error = Error::new(code, reason);
                    if let Some(location) = outcome["where"].as_str() {
                        error = error.with_where(location);
                    }
                    if let Some(evidence) = outcome["origin_evidence"].as_str() {
                        let origin = serde_json::from_value(outcome["origin"].clone())
                            .unwrap_or(eliot_core::FailureOrigin::Unknown);
                        error = error.with_origin(origin, evidence);
                    }
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "daemon.launch_refused",
                        &[("agent", id), ("command_id", command_id)],
                        Some(&error),
                    );
                }
                let Some(workspace) = store.refused_launch_cleanup_workspace(&agent, command_id)?
                else {
                    return Ok(());
                };
                let mut remove =
                    SubmitCommand::new(ulid::Ulid::new().to_string(), "worktree.remove");
                remove.args = json!({
                    "expected_tip": workspace.base_oid,
                    "refused_launch_command": command_id,
                });
                remove
            };
            request.target = Some(agent.id);
            request.expected_generation = Some(generation);
            let admission = store.submit("owner", request, now_ms())?;
            self.publish_locked(&admission.events);
            admission
        };
        self.schedule_command(admission.command.id, None);
        self.signal_progress();
        Ok(())
    }
}
