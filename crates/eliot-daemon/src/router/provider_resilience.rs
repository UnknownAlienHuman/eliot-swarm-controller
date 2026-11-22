//! Bounded provider-hold maintenance through the ordinary launch and SEND
//! admission/dispatch path.

use super::*;
use eliot_store::{Admission, ProviderProbeCandidate};
use tokio::sync::watch;

const PROVIDER_PROBE_TIMEOUT: Duration = Duration::from_secs(90);
const PROBE_CLEANUP_WAIT: Duration = Duration::from_secs(5);

pub(crate) struct ProviderProbeWork {
    send_command_id: Option<String>,
    agent_id: String,
}

impl Router {
    pub(crate) async fn notify_provider_attention(&self) -> Result<()> {
        let notices = {
            let mut store = self.inner.store.lock().await;
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(());
            }
            let notices =
                store.admit_provider_attention_notices(now_ms(), &self.secret_policy_snapshot())?;
            for notice in &notices {
                self.publish_locked(&notice.events);
            }
            notices
        };
        if !notices.is_empty() {
            for notice in &notices {
                self.schedule_admission(notice);
            }
            self.signal_progress();
        }
        Ok(())
    }

    /// Lift due known resets and admit at most `available` unknown-reset
    /// probes. Store admission claims the hold before launch dispatch can
    /// spawn a native process.
    pub(crate) async fn maintain_provider_holds(
        &self,
        available: usize,
    ) -> Result<Vec<ProviderProbeWork>> {
        let now = now_ms();
        let candidates = {
            let mut store = self.inner.store.lock().await;
            let events = store.release_expired_provider_holds(now, 64)?;
            self.publish_locked(&events);
            if !events.is_empty() {
                self.signal_progress();
                self.signal_launch_arbitration();
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(Vec::new());
            }
            store
                .due_provider_probes(now, available.min(eliot_store::PROVIDER_PROBE_BATCH_LIMIT))?
        };

        let mut started = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            match self.start_provider_probe(&candidate).await {
                Ok(work) => started.push(work),
                Err(error) => {
                    let reason = match error.code.as_str() {
                        "not_found" | "unavailable" => "profile_unavailable",
                        "not_allowed" | "secret_in_payload" | "invalid" | "unsupported" => {
                            "unsafe_profile"
                        }
                        _ => "probe_admission_failed",
                    };
                    {
                        let mut store = self.inner.store.lock().await;
                        let deferred =
                            store.defer_provider_probe(&candidate.scope, now_ms(), reason)?;
                        self.publish_locked(&deferred);
                    }
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "provider.hold_probe_deferred",
                        &[("scope", candidate.scope.as_str())],
                        Some(&error),
                    );
                }
            }
        }
        Ok(started)
    }

    async fn start_provider_probe(
        &self,
        candidate: &ProviderProbeCandidate,
    ) -> Result<ProviderProbeWork> {
        let profile = self.resolve_profile_cached(&candidate.profile)?;
        let harness = profile
            .get("harness")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "provider probe profile has no harness"))?;
        if harness != "codex" {
            return Err(Error::new(
                "unsupported",
                "automatic minimal provider probes currently require a Codex profile",
            ));
        }
        // Codex otherwise makes a separate catalogue read to fill an
        // omitted model during launch preparation. A probe is one minimal
        // inference request, so defer profiles without an already frozen
        // model instead of doing extra provider work.
        if profile.get("route_binding").is_none_or(Value::is_null)
            && profile
                .get("model")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            && profile
                .get("daemon_model")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(Error::new(
                "unavailable",
                "provider probe profile needs an explicit model",
            ));
        }

        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({
            "profile": candidate.profile,
            "chat": true,
            "text": eliot_core::PROVIDER_PROBE_PROMPT,
        });
        // This is a daemon-owned root chat with no project, registered
        // workspace, sources, or user task. The model can only affect its
        // private scratch context.
        let mut prepared = self.prepare_submission(request, &Principal::Owner).await?;
        let snapshot = prepared
            .request
            .args
            .get_mut("profile_snapshot")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "Codex provider probe lost its frozen profile snapshot",
                )
            })?;
        if snapshot.get("harness").and_then(Value::as_str) != Some("codex") {
            return Err(Error::new(
                "unsupported",
                "provider probe harness changed during preparation",
            ));
        }
        snapshot.insert(
            eliot_core::PROVIDER_PROBE_LAUNCH_ARG.to_owned(),
            Value::Bool(true),
        );
        snapshot.remove("collaboration_mode");
        snapshot.insert(
            "permissions".to_owned(),
            Value::String("read-only".to_owned()),
        );
        snapshot.insert(
            "approval_policy".to_owned(),
            Value::String("never".to_owned()),
        );
        snapshot.insert(
            "native_subagents".to_owned(),
            Value::String("deny".to_owned()),
        );
        snapshot.insert("mcp".to_owned(), Value::String("minimal".to_owned()));
        snapshot.insert("mcp_allow".to_owned(), json!([]));
        prepared.request.args[eliot_core::PROVIDER_PROBE_LAUNCH_ARG] =
            Value::String(candidate.scope.clone());
        let admission = self.admit_provider_command(prepared).await?;
        let send = admission.linked_command.as_ref().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "provider probe launch did not admit its linked SEND",
            )
        })?;
        let agent_id = send.target.clone().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "provider probe SEND has no Agent target",
            )
        })?;
        Ok(ProviderProbeWork {
            send_command_id: Some(send.id.clone()),
            agent_id,
        })
    }

    async fn admit_provider_command(&self, prepared: PreparedSubmission) -> Result<Admission> {
        let (admission, needs_worker) = {
            let mut store = self.inner.store.lock().await;
            let admission = self.admit_prepared_submission(
                &mut store,
                "owner",
                prepared,
                SubmissionContext::default(),
            )?;
            self.publish_locked(&admission.events);
            let needs_worker = admission.is_new && command_needs_worker(&store, &admission.command);
            (admission, needs_worker)
        };
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        if needs_worker {
            self.schedule_admission(&admission);
        }
        self.signal_progress();
        Ok(admission)
    }

    pub(crate) async fn recover_ended_provider_probes(
        &self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<()> {
        let mut after = 0;
        while !*shutdown.borrow() {
            let page = self.inner.store.lock().await.ended_provider_probes(after)?;
            if page.is_empty() {
                break;
            }
            for (id, agent_id) in page {
                if *shutdown.borrow() {
                    return Ok(());
                }
                if let Err(error) = self
                    .finish_provider_probe(
                        ProviderProbeWork {
                            send_command_id: None,
                            agent_id,
                        },
                        shutdown.clone(),
                    )
                    .await
                {
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "daemon.provider_probe_recovery_failed",
                        &[],
                        Some(&error),
                    );
                }
                after = id;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// Always attempt cleanup after settling or stopping an owned probe.
    /// Startup uses this same path for an Agent with already-ended custody.
    pub(crate) async fn finish_provider_probe(
        &self,
        work: ProviderProbeWork,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let stopped = self.stop_provider_probe(&work, &mut shutdown).await;
        let cleaned = self.cleanup_ended_provider_probe(&work.agent_id).await;
        stopped.and(cleaned)
    }

    async fn stop_provider_probe(
        &self,
        work: &ProviderProbeWork,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let Some(command_id) = work.send_command_id.as_deref() else {
            return Ok(());
        };
        let waited = tokio::time::timeout(
            PROVIDER_PROBE_TIMEOUT,
            self.wait_for_provider_command(command_id, shutdown),
        )
        .await;
        let send = match waited {
            Ok(Ok(Some(send))) => Some(send),
            _ => self.inner.store.lock().await.get_command(command_id)?,
        };
        let Some(send) = send else {
            return Ok(());
        };
        let Some(send_generation) = send.generation else {
            return Err(Error::new(
                "source_integrity",
                "provider probe SEND lost its generation binding",
            ));
        };
        if !send.probe || send.target.as_deref() != Some(work.agent_id.as_str()) {
            return Err(Error::new(
                "source_integrity",
                "provider probe SEND changed its Agent binding",
            ));
        }

        let current = {
            let store = self.inner.store.lock().await;
            store.get_agent(&work.agent_id)?
        };
        let Some(agent) = current else {
            return Ok(());
        };
        if !agent.is_automatic_provider_probe()
            || (agent.generation != send_generation && agent.generation != 0)
        {
            return Err(Error::new(
                "stale_generation",
                "provider probe Agent generation changed before cleanup",
            ));
        }
        if !agent.state.is_terminal() {
            let mut stop = SubmitCommand::new(ulid::Ulid::new().to_string(), "stop");
            stop.target = Some(agent.id.clone());
            stop.expected_generation = Some(agent.generation);
            let prepared = self.prepare_submission(stop, &Principal::Owner).await?;
            let admission = self.admit_provider_command(prepared).await?;
            let stopped = tokio::time::timeout(
                PROBE_CLEANUP_WAIT,
                self.wait_for_provider_command(&admission.command.id, shutdown),
            )
            .await;
            let Ok(stopped) = stopped else {
                return Ok(());
            };
            let Some(stopped) = stopped? else {
                return Ok(());
            };
            if stopped.state == CommandState::Unknown {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn cleanup_ended_provider_probe(&self, agent_id: &str) -> Result<()> {
        let current = {
            let store = self.inner.store.lock().await;
            store.get_agent(agent_id)?
        };
        if !current.is_some_and(|agent| {
            agent.is_automatic_provider_probe()
                && matches!(
                    agent.state,
                    AgentState::Stopped | AgentState::Failed | AgentState::Lost
                )
        }) {
            return Ok(());
        }
        let request = eliot_core::api::AgentDeleteRequest {
            id: ulid::Ulid::new().to_string(),
            agent: agent_id.to_owned(),
        };
        let params = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "provider probe cleanup request is invalid"))?;
        let id = required_text(&params, "id")?;
        let agent = required_text(&params, "agent")?;
        let deadline = tokio::time::Instant::now() + PROBE_CLEANUP_WAIT;
        loop {
            // Inspection commits no effects. Wait for source/process custody
            // to drain before reserving the one real deletion.
            let inspected = {
                let mut store = self.inner.store.lock().await;
                store
                    .reconcile_ended_provider_probe(agent, now_ms())
                    .and_then(|events| {
                        self.publish_locked(&events);
                        // A restarted lost Agent can keep its resumable Git observer
                        // open after the native Job has been fenced. A maintenance
                        // probe has no resumable task or Git work to carry forward.
                        if let Some(current) = store.get_agent(agent)?
                            && store
                                .list_source_states(agent, current.generation)?
                                .iter()
                                .any(|source| {
                                    source["source"] == "git" && source["state"] == "open"
                                })
                        {
                            let closed = store.set_git_source_status(
                                agent,
                                current.generation,
                                "stale",
                                true,
                                now_ms(),
                            )?;
                            self.publish_locked(&closed);
                        }
                        store.inspect_agent_delete(id, agent, now_ms())
                    })
            };
            match inspected {
                Ok(_) => break,
                Err(error) if error.code == "busy" && tokio::time::Instant::now() < deadline => {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "This bounded wait is owned by the surrounding operation."
                    )]
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.agent_delete(&params, &Principal::Owner).await?;
        Ok(())
    }

    async fn wait_for_provider_command(
        &self,
        command_id: &str,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<Command>> {
        let mut progress = self.inner.progress.subscribe();
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            let _ = *progress.borrow_and_update();
            let marks = self.wait_unknown_marks().await;
            let readback = {
                let store = self.inner.store.lock().await;
                let Some(command) = store.get_command(command_id)? else {
                    return Ok(None);
                };
                if command.state == CommandState::Unknown || marks.causes.contains_key(&command.id)
                {
                    return Ok(None);
                }
                let turn_status = if let (Some(agent), Some(generation), Some(turn_id)) = (
                    command.target.as_deref(),
                    command.generation,
                    command.turn_id.as_deref(),
                ) {
                    let Some(turn) = store.get_turn(agent, generation, turn_id)? else {
                        return Ok(None);
                    };
                    let turn_ids = [turn_id.to_owned()];
                    if store
                        .turns_with_unknown_commands(agent, generation, &turn_ids, &marks.memory)?
                        .contains(turn_id)
                    {
                        return Ok(None);
                    }
                    Some(Self::turn_entry_status(&turn))
                } else {
                    None
                };
                (command, turn_status)
            };
            let (command, turn_status) = readback;
            if let Some(status) = turn_status {
                match status {
                    "still_running" => {}
                    "succeeded" | "failed" | "interrupted" => return Ok(Some(command)),
                    // A turn with an unknown source/outcome is not cleanup
                    // authority, even if the command itself is Applied.
                    "unknown" => return Ok(None),
                    _ => return Ok(None),
                }
            } else if command.state.is_terminal() {
                // Applied SENDs must carry the exact turn association. A
                // missing turn cannot prove that the native effect ended.
                if command.verb == "send" && command.state == CommandState::Applied {
                    return Ok(None);
                }
                return Ok(Some(command));
            }
            tokio::select! {
                changed = progress.changed() => {
                    if changed.is_err() {
                        return Err(Error::new("unavailable", "provider probe progress closed"));
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(None);
                    }
                }
            }
        }
    }
}
