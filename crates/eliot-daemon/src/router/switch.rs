//! One durable switch worker composes normal launch and H2 transfer.
use super::*;
use eliot_core::api::SwitchRequest;

impl Router {
    pub(super) async fn swarm_switch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: SwitchRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let change = {
            let mut store = self.inner.store.lock().await;
            if let Some(value) = store.replay_switch(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode switch replay"));
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new("unavailable", "drain prevents new switches"));
            }
            let change =
                store.reserve_switch(&principal_name(principal), &request, &policy, now_ms())?;
            self.publish_locked(&change.events);
            // A disconnected caller cannot cancel the independently scheduled operation.
            if !change.events.is_empty() {
                self.schedule_command(request.id.clone(), None);
            }
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode switch"))
    }

    pub(super) async fn process_switch(&self, id: &str) {
        let work = {
            let mut store = self.inner.store.lock().await;
            match store.start_switch(id, now_ms()) {
                Ok(change) => {
                    self.publish_locked(&change.events);
                    change.value
                }
                Err(_) => None,
            }
        };
        let Some((request, plan)) = work else {
            return;
        };
        let prepared = async {
            let source = {
                let store = self.inner.store.lock().await;
                store
                    .get_agent(&request.from)?
                    .ok_or_else(|| Error::new("not_found", "source Agent missing"))?
            };
            let mut launch = SubmitCommand::new(&plan.launch_command, "launch");
            let workspace = request
                .launch
                .workspace
                .as_deref()
                .or(source.workspace.as_deref())
                .ok_or_else(|| {
                    Error::new(
                        "invalid",
                        "switch requires a workspace when A has no retained workspace",
                    )
                })?;
            launch.args = request.launch.args(workspace)?;
            let scope = source.project_scope();
            let mut prepared = self
                .prepare_submission_after_authorization(launch, &Principal::Owner, false, false)
                .await?;
            // The replacement retains the source's authorization namespace for handoff.
            prepared.request.args["project"] = json!(scope);
            if let Some(workspace) = &mut prepared.workspace {
                workspace.project = scope.to_owned();
                prepared.request.args["workspace_preparation"] = serde_json::to_value(workspace)
                    .map_err(|_| Error::new("internal", "cannot encode switch workspace"))?;
            }
            let policy = self.secret_policy_snapshot();
            if policy.contains_value(&prepared.request.args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "switch launch contains a known secret",
                ));
            }
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission =
                store.admit_switch_launch(id, prepared.request, prepared.workspace, now_ms())?;
            self.publish_locked(&admission.events);
            Ok::<_, Error>(admission)
        }
        .await;
        let admission = match prepared {
            Ok(admission) => admission,
            Err(error) => {
                let mut store = self.inner.store.lock().await;
                if let Ok(change) = store.fail_switch_preparation(id, &error.code, now_ms()) {
                    self.publish_locked(&change.events);
                }
                self.signal_progress();
                return;
            }
        };
        // The same ordinary worker owns native dispatch and settlement. No linked launch
        // prompt is admitted: only H2 can create B's first SEND.
        let mut progress = self.inner.progress.subscribe();
        self.clone()
            .process_command(admission.command.id, None)
            .await;
        // Native ready batches can race the start reply. Wait on committed progress,
        // never infer readiness from Applied alone or from an Owner assertion.
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                {
                    let store = self.inner.store.lock().await;
                    let value = store.switch_result(id)?;
                    if value.launch_state != Some(CommandState::Applied)
                        || store.switch_launch_ready(id)?
                    {
                        return Ok::<_, Error>(());
                    }
                }
                if progress.changed().await.is_err() {
                    return Ok(());
                }
            }
        })
        .await;
        let policy = self.secret_policy_snapshot();
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = store.finish_switch(id, &policy, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
                if let Some(handoff) = &change.value.handoff {
                    for child in &handoff.children {
                        self.schedule_command(child.clone(), None);
                    }
                }
            }
            result
        };
        self.signal_progress();
        if let Ok(change) = result
            && let Some(handoff) = change.value.handoff
        {
            self.notify_claim_conflicts(&handoff.claim_conflicts).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn switch_preparation_failure_and_drain_retry_are_retained_without_launch() {
        let router = super::super::tests::router();
        assert_eq!(
            router
                .swarm_switch(
                    &json!({}),
                    &Principal::Agent {
                        id: "a1".into(),
                        generation: 1
                    }
                )
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let source = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({"project":"missing-h3-workspace","workspace":"missing-h3-workspace","harness":"codex","profile":"worker","profile_snapshot":{"control":"tree"}});
            let admission = store.submit("owner", launch, now_ms()).unwrap();
            let agent = admission.command.target.unwrap();
            store.dispatch(&admission.command.id, now_ms()).unwrap();
            crate::test_support::install_fixture_sources(
                &mut store,
                &agent,
                &admission.command.id,
                1,
                now_ms(),
            );
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".into(),
                        cursor: None,
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".into(),
                            native_id: "h3-ready".into(),
                            data: json!({"command_id":admission.command.id}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&admission.command.id, CommandState::Applied, None, now_ms())
                .unwrap();
            agent
        };
        let params = json!({"id":ulid::Ulid::new().to_string(),"from":source,"from_generation":1,"task_command":"retained-task","commands":[],"launch":{"profile":"missing-h3-profile"}});
        let mut progress = router.inner.progress.subscribe();
        let first = router
            .swarm_switch(&params, &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["state"], "accepted");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if router
                    .inner
                    .store
                    .lock()
                    .await
                    .switch_result(params["id"].as_str().unwrap())
                    .unwrap()
                    .state
                    == CommandState::Rejected
                {
                    break;
                }
                progress.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        router.inner.mode.store(MODE_DRAIN, Ordering::SeqCst);
        let retry = router
            .swarm_switch(&params, &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(retry["state"], "rejected");
        assert_eq!(retry["launch_command"], first["launch_command"]);
        assert_eq!(retry["handoff_command"], first["handoff_command"]);
        assert!(retry["created"].is_null());
        assert_eq!(router.inner.store.lock().await.agent_count().unwrap(), 1);
        let mut changed = params;
        changed["launch"]["profile"] = json!("different");
        assert_eq!(
            router
                .swarm_switch(&changed, &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );
    }
}
