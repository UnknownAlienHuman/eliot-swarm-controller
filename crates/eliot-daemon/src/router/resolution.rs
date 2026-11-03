//! Owner resolution of unknown commands and turns.
//!
//! Both RPCs validate once through a shared request gate, then resolve
//! through the one Store writer. A replayed turn-resolution write id returns
//! its stored response before any plugin readback. Otherwise turn resolution
//! on a live generation first reads the exact native turn back through the
//! owned plugin; only a readback proving the turn inactive becomes a
//! resolution proof. Native I/O runs outside the Store lock; the resolving
//! transaction re-checks the stored terminality and the proof binding.
use super::{Principal, Router, contains_known_secret_including_keys, require_owner};
use crate::{common::now_ms, plugin_host::SecretSet};
use eliot_core::{
    Agent, Error, Result,
    api::{CommandResolveRequest, TurnResolveRequest},
};
use eliot_store::TurnInactiveProof;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Authorize, secret-scan, parse, and validate one resolution request: the
/// single validation gate for both resolution RPCs.
fn decode_resolve_request<T>(
    params: &Value,
    principal: &Principal,
    secrets: &SecretSet,
    method: &str,
    noun: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned,
{
    require_owner(principal)?;
    if contains_known_secret_including_keys(params, secrets) {
        return Err(Error::new(
            "secret_in_payload",
            format!("{noun} contains a known credential"),
        ));
    }
    let request: T = serde_json::from_value(params.clone())
        .map_err(|_| Error::new("invalid", format!("{method} params are invalid")))?;
    validate(&request)?;
    Ok(request)
}

/// Interpret one `agent.snapshot` response as proof that the exact requested
/// turn is inactive: the snapshot must bind this Agent generation, name the
/// turn as its last turn with `terminal: true` and a terminal status, and not
/// report it as still active. Anything else — including a contradictory
/// readback, a failed readback, or an unknown snapshot — proves nothing.
fn turn_inactive_proof(
    snapshot: &Value,
    agent_id: &str,
    generation: i64,
    turn_id: &str,
) -> Option<TurnInactiveProof> {
    if snapshot.get("agent_id").and_then(Value::as_str) != Some(agent_id) {
        return None;
    }
    if snapshot.get("generation").and_then(Value::as_i64) != Some(generation) {
        return None;
    }
    let last_turn = snapshot.get("last_turn")?;
    if last_turn.get("turn_id").and_then(Value::as_str) != Some(turn_id) {
        return None;
    }
    // Explicit terminality only: the flag and the status must agree that
    // the turn is terminal. A contradictory readback — terminal:false with a
    // terminal status, or terminal:true with a running status — proves
    // nothing, and neither does a terminal status without the explicit flag.
    let status_terminal = matches!(
        last_turn.get("status").and_then(Value::as_str),
        Some("completed" | "failed" | "interrupted")
    );
    let flag_terminal = last_turn.get("terminal").and_then(Value::as_bool) == Some(true);
    if !(status_terminal && flag_terminal) {
        return None;
    }
    if snapshot.get("active_turn").and_then(Value::as_str) == Some(turn_id) {
        return None;
    }
    Some(TurnInactiveProof {
        turn_id: turn_id.to_owned(),
    })
}

impl Router {
    pub(super) async fn command_resolve(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CommandResolveRequest = decode_resolve_request(
            params,
            principal,
            &self.inner.secrets,
            "command.resolve",
            "command resolution",
            CommandResolveRequest::validate,
        )?;
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.resolve_command("owner", &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "command resolution could not be encoded"))
    }

    pub(super) async fn turn_resolve(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: TurnResolveRequest = decode_resolve_request(
            params,
            principal,
            &self.inner.secrets,
            "turn.resolve",
            "turn resolution",
            TurnResolveRequest::validate,
        )?;
        // Replay first: a write id already bound to this exact assertion
        // returns its stored response without any plugin readback. A
        // conflicting binding fails here exactly as the resolving
        // transaction would report it.
        let replay = {
            let store = self.inner.store.lock().await;
            store.stored_turn_resolution("owner", &request)?
        };
        if let Some(value) = replay {
            self.signal_progress();
            return serde_json::to_value(value)
                .map_err(|_| Error::new("internal", "turn resolution could not be encoded"));
        }
        // Read the stored generation and the readback binding first; the
        // resolving transaction below re-checks terminality and the proof.
        let (agent, readback_command) = {
            let store = self.inner.store.lock().await;
            let agent = store.get_agent(&request.agent)?;
            let readback_command = match &agent {
                Some(agent)
                    if !agent.state.is_terminal() && agent.generation == request.generation =>
                {
                    store.turn_readback_command(
                        &request.agent,
                        request.generation,
                        &request.turn_id,
                    )?
                }
                _ => None,
            };
            (agent, readback_command)
        };
        let proof = match (agent, readback_command) {
            (Some(agent), Some(command_id)) => {
                self.snapshot_turn_proof(&agent, &command_id, &request.turn_id)
                    .await
            }
            _ => None,
        };
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.resolve_turn("owner", &request, proof.as_ref(), now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "turn resolution could not be encoded"))
    }

    /// Prove one turn inactive through the owned plugin. Any readback
    /// failure — no runtime, timeout, unknown snapshot — yields no proof,
    /// and the Store then refuses the live generation with `bad_state`.
    async fn snapshot_turn_proof(
        &self,
        agent: &Agent,
        command_id: &str,
        turn_id: &str,
    ) -> Option<TurnInactiveProof> {
        let snapshot = self
            .inner
            .plugin_host
            .snapshot_turn(agent, command_id, turn_id)
            .await
            .ok()?;
        turn_inactive_proof(&snapshot, &agent.id, agent.generation, turn_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use eliot_core::{
        CommandState, NativeEvent, SourceBatch, SourceHandoff, SourceHandoffGap, SubmitCommand,
        api::TurnResolveStatus, parse_agent_id,
    };
    use eliot_plugin::sync::LockExt;
    use serde_json::json;
    use std::{path::PathBuf, sync::Arc};
    use tokio::sync::Mutex as AsyncMutex;

    fn router() -> Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    #[tokio::test]
    async fn resolution_rpcs_are_owner_only_registered_and_secret_scanned() {
        let router = router();
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .command_resolve(&Value::Null, &agent)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            router
                .turn_resolve(&Value::Null, &agent)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        router
            .inner
            .secrets
            .lock_recover()
            .insert("router-test-known-secret".to_owned());
        assert_eq!(
            router
                .command_resolve(
                    &json!({"id":"resolve-id", "command_id":"command-id", "as":"failed",
                        "reason":"leaks router-test-known-secret"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "secret_in_payload"
        );
        assert_eq!(
            router
                .turn_resolve(
                    &json!({"id":"resolve-id", "agent":"a1", "generation":1,
                        "turn_id":"turn-1", "status":"failed",
                        "reason":"leaks router-test-known-secret"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err()
                .code,
            "secret_in_payload"
        );
        // Registered methods reject malformed params as invalid, not as unimplemented.
        for (method, payload) in [
            ("command.resolve", json!({})),
            ("turn.resolve", json!({})),
            ("command.children", json!({})),
        ] {
            assert_eq!(
                router
                    .handle(
                        method,
                        json!({
                            "_auth":{"kind":"owner", "credential":"owner-credential"},
                            "_payload":payload
                        })
                    )
                    .await
                    .unwrap_err()
                    .code,
                "invalid"
            );
        }
    }

    #[tokio::test]
    async fn command_resolve_handler_records_the_owner_assertion() {
        let router = router();
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
        launch.args = json!({"project":"resolve-rpc", "workspace":"synthetic-workspace"});
        launch.expected_generation = Some(0);
        let agent = {
            let mut store = router.inner.store.lock().await;
            let agent = store
                .submit("owner", launch, now)
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch_id, now).unwrap();
            store
                .settle(&launch_id, CommandState::Unknown, None, now)
                .unwrap();
            agent
        };
        let resolve_id = ulid::Ulid::new().to_string();
        let response = router
            .command_resolve(
                &json!({"id":resolve_id, "command_id":launch_id, "as":"failed",
                    "reason":"owner verified the uncertain launch"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(response["id"], resolve_id);
        assert_eq!(response["command_id"], launch_id);
        assert_eq!(response["state"], "failed");
        let read = router
            .command_get(&json!({"id":launch_id}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(read["state"], "failed");
        assert_eq!(read["target"], agent);
        assert_eq!(read["outcome"]["resolution_source"], "owner");
    }

    #[tokio::test]
    async fn queued_send_wakes_on_command_resolved_and_dispatches() {
        let router = router();
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
        launch.args = json!({"project":"resolve-wake", "workspace":"synthetic-workspace"});
        launch.expected_generation = Some(0);
        let (first_id, second_id) = {
            let mut store = router.inner.store.lock().await;
            let agent = store
                .submit("owner", launch, now)
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch_id, now).unwrap();
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id":launch_id}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
            store
                .settle(&launch_id, CommandState::Applied, None, now)
                .unwrap();
            let mut first = SubmitCommand::new(ulid::Ulid::new().to_string(), "send");
            first.target = Some(agent.clone());
            first.expected_generation = Some(1);
            first.args = json!({"text":"uncertain input"});
            let first_id = first.id.clone();
            store.submit("owner", first, now).unwrap();
            store.dispatch(&first_id, now).unwrap();
            store
                .settle(&first_id, CommandState::Unknown, None, now)
                .unwrap();
            let mut second = SubmitCommand::new(ulid::Ulid::new().to_string(), "send");
            second.target = Some(agent.clone());
            second.expected_generation = Some(1);
            second.args = json!({"text":"gated input"});
            let second_id = second.id.clone();
            let admitted = store.submit("owner", second, now).unwrap();
            assert_eq!(admitted.command.state, CommandState::Queued);
            (first_id, second_id)
        };

        let mut events = router.inner.bus.subscribe();
        router.schedule_command(second_id.clone(), None);
        // Grace for the spawned dispatch task to reach its event waiter. No
        // bus traffic exists yet, so the later wake can only come from the
        // owner resolution published below.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        {
            let mut store = router.inner.store.lock().await;
            let request = CommandResolveRequest {
                id: ulid::Ulid::new().to_string(),
                command_id: first_id.clone(),
                resolution: eliot_core::api::CommandResolution::Failed,
                reason: "owner verified the uncertain effect".to_owned(),
            };
            let change = store.resolve_command("owner", &request, now_ms()).unwrap();
            assert_eq!(change.events.len(), 1);
            assert_eq!(change.events[0].kind, "command.resolved");
            router.publish_locked(&change.events);
        }

        let dispatched = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let event = events.recv().await.expect("Router event bus remains open");
                if event.kind == "command.dispatched"
                    && event.data.get("command_id").and_then(Value::as_str)
                        == Some(second_id.as_str())
                {
                    break event;
                }
            }
        })
        .await
        .expect("command.resolved wakes the queued send to dispatch");
        assert_eq!(dispatched.data["command_id"], second_id);
    }

    async fn attach_observer(router: &Router, now: i64) -> (String, String) {
        let mut store = router.inner.store.lock().await;
        let admitted = store
            .submit(
                "owner",
                SubmitCommand {
                    id: ulid::Ulid::new().to_string(),
                    target: None,
                    verb: "attach".to_owned(),
                    args: json!({"mode":"observe","profile":"fake","harness":"fake",
                        "native_scope":"fixture:scope","native_ref":"fixture-native",
                        "profile_snapshot":{"harness":"fake"}}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap();
        let agent = admitted.command.target.unwrap();
        let command_id = admitted.command.id.clone();
        store.dispatch(&command_id, now).unwrap();
        (agent, command_id)
    }

    #[tokio::test]
    async fn turn_resolve_refuses_observe_only_agents_after_restart() {
        let router = router();
        let now = now_ms();
        let (agent, attach_id) = attach_observer(&router, now).await;
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle_observe_attach(
                    &attach_id,
                    json!({"agent_id":agent,"generation":1,"command_id":attach_id,
                        "native_scope":"fixture:scope","native_ref":"fixture-native",
                        "state":"attached","observation_state":"unknown",
                        "metadata":{},"coverage":{}}),
                    now + 1,
                )
                .unwrap();
            store
                .record_observed_source_handoff(
                    SourceHandoff {
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "observe".to_owned(),
                        families: vec!["turn".to_owned(), "message".to_owned()],
                        successor: "transcript".to_owned(),
                        mechanism: "rollout_tail".to_owned(),
                        cursor: None,
                        gap: Some(SourceHandoffGap {
                            gap_id: "edge-gap".to_owned(),
                            reason: "live_edge".to_owned(),
                            from: None,
                            to: Some("edge:0".to_owned()),
                        }),
                    },
                    ("fixture:scope", "fixture-native"),
                    now + 2,
                )
                .unwrap();
            let batch_id = ulid::Ulid::new().to_string();
            store
                .put_observed_batch(
                    SourceBatch {
                        batch_id: batch_id.clone(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "transcript".to_owned(),
                        events: vec![NativeEvent {
                            ts: Some(now + 3),
                            kind: "message.completed".to_owned(),
                            native_id: batch_id.clone(),
                            data: json!({"turn_id":"observed-turn","text":"observed answer",
                                "phase":"final_answer"}),
                        }],
                        cursor: Some(format!("edge:{batch_id}")),
                    },
                    ("fixture:scope", "fixture-native"),
                    now + 3,
                )
                .unwrap();
            store.recover_on_restart(now + 4).unwrap();
            let agent_row = store.get_agent(&agent).unwrap().unwrap();
            assert!(agent_row.observe_only);
            assert!(agent_row.active_turn.is_none());
        }
        let error = router
            .turn_resolve(
                &json!({"id":ulid::Ulid::new().to_string(), "agent":agent, "generation":1,
                    "turn_id":"observed-turn", "status":"failed",
                    "reason":"must be refused for an observer"}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }

    #[tokio::test]
    async fn command_resolve_refuses_observe_only_agent_commands() {
        let router = router();
        let now = now_ms();
        let (_, attach_id) = attach_observer(&router, now).await;
        {
            let mut store = router.inner.store.lock().await;
            store.recover_on_restart(now + 1).unwrap();
            assert_eq!(
                store.get_command(&attach_id).unwrap().unwrap().state,
                CommandState::Unknown
            );
        }
        for resolution in ["failed", "applied"] {
            let error = router
                .command_resolve(
                    &json!({"id":ulid::Ulid::new().to_string(), "command_id":attach_id,
                        "as":resolution, "reason":"must be refused for an observer"}),
                    &Principal::Owner,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "not_allowed");
        }
    }

    async fn ready_agent(router: &Router, project: &str, now: i64) -> String {
        let launch_id = ulid::Ulid::new().to_string();
        let mut store = router.inner.store.lock().await;
        let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
        launch.args = json!({"project":project, "workspace":"synthetic-workspace"});
        launch.expected_generation = Some(0);
        let agent = store
            .submit("owner", launch, now)
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&launch_id, now).unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .unwrap();
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .unwrap();
        agent
    }

    #[tokio::test]
    async fn turn_resolve_on_a_live_generation_without_a_plugin_is_bad_state() {
        let router = router();
        let now = now_ms();
        let agent = ready_agent(&router, "resolve-live-rpc", now).await;
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut send = SubmitCommand::new(send_id.clone(), "send");
            send.target = Some(agent.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text":"held input"});
            store.submit("owner", send, now).unwrap();
            store.dispatch(&send_id, now).unwrap();
            store
                .put_batch(
                    SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id":send_id, "turn_id":"turn-1"}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
        }
        // No plugin runtime exists in this fixture, so the readback yields
        // no proof and the live generation stays fenced.
        assert_eq!(router.inner.plugin_host.snapshot_turn_calls(), 0);
        let error = router
            .turn_resolve(
                &json!({"id":ulid::Ulid::new().to_string(), "agent":agent, "generation":1,
                    "turn_id":"turn-1", "status":"failed",
                    "reason":"must be refused on a live generation"}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        // The fixture observes readback attempts: a fresh live resolve
        // performs exactly one.
        assert_eq!(router.inner.plugin_host.snapshot_turn_calls(), 1);
    }

    #[tokio::test]
    async fn turn_resolve_on_a_terminal_generation_needs_no_plugin() {
        let router = router();
        let now = now_ms();
        let agent = ready_agent(&router, "resolve-terminal-rpc", now).await;
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut send = SubmitCommand::new(send_id.clone(), "send");
            send.target = Some(agent.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text":"held input"});
            store.submit("owner", send, now).unwrap();
            store.dispatch(&send_id, now).unwrap();
            store
                .put_batch(
                    SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id":send_id, "turn_id":"turn-1"}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
            let mut stop = SubmitCommand::new(ulid::Ulid::new().to_string(), "stop");
            stop.target = Some(agent.clone());
            stop.expected_generation = Some(1);
            let stop_id = stop.id.clone();
            store.submit("owner", stop, now).unwrap();
            store.dispatch(&stop_id, now).unwrap();
            store
                .settle(&stop_id, CommandState::Applied, None, now)
                .unwrap();
        }
        let resolve_id = ulid::Ulid::new().to_string();
        let response = router
            .turn_resolve(
                &json!({"id":resolve_id, "agent":agent, "generation":1,
                    "turn_id":"turn-1", "status":"failed",
                    "reason":"owner verified the held turn never completed"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(response["id"], resolve_id);
        assert_eq!(response["agent"], agent);
        assert_eq!(response["generation"], 1);
        assert_eq!(response["turn_id"], "turn-1");
        assert_eq!(response["status"], "failed");
    }

    #[tokio::test]
    async fn turn_resolve_replay_returns_the_stored_response_without_a_readback() {
        // File-backed so a second connection can pin the turn gate: no
        // current Store flow marks a controlled turn unknown while its
        // generation stays live (stop and restart always terminalize it).
        let db_dir =
            std::env::temp_dir().join(format!("eliot-resolve-replay-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&db_dir).unwrap();
        let store = eliot_store::Store::open(&db_dir.join("resolve-replay.sqlite")).unwrap();
        let router = Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        );
        let now = now_ms();
        let agent = ready_agent(&router, "resolve-replay-rpc", now).await;
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut send = SubmitCommand::new(send_id.clone(), "send");
            send.target = Some(agent.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text":"held input"});
            store.submit("owner", send, now).unwrap();
            store.dispatch(&send_id, now).unwrap();
            store
                .put_batch(
                    SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id":send_id, "turn_id":"turn-1"}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
            // The generation stays live: no stop is issued.
            assert!(
                !store
                    .get_agent(&agent)
                    .unwrap()
                    .unwrap()
                    .state
                    .is_terminal()
            );
        }
        {
            let pin = rusqlite::Connection::open(db_dir.join("resolve-replay.sqlite")).unwrap();
            let agent_num = parse_agent_id(&agent).unwrap();
            pin.execute(
                "UPDATE turns SET state = 'unknown'
                 WHERE agent = ?1 AND generation = 1 AND turn_id = 'turn-1'",
                [agent_num],
            )
            .unwrap();
        }
        // A fresh resolution on this live generation attempts the native
        // readback (and fails without a plugin runtime): the replay below
        // would read back too if it did not hit the stored response.
        let fresh_error = router
            .turn_resolve(
                &json!({"id":ulid::Ulid::new().to_string(), "agent":agent, "generation":1,
                    "turn_id":"turn-1", "status":"failed",
                    "reason":"proves this live generation reads back"}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(fresh_error.code, "bad_state");
        assert_eq!(router.inner.plugin_host.snapshot_turn_calls(), 1);
        // Store the resolution with a native-readback proof, as a prior
        // proof-backed resolution would have stored it. The generation is
        // still live and the readback binding is still present.
        let resolve_id = ulid::Ulid::new().to_string();
        let request = TurnResolveRequest {
            id: resolve_id.clone(),
            agent: agent.clone(),
            generation: 1,
            turn_id: "turn-1".to_owned(),
            status: TurnResolveStatus::Failed,
            reason: "owner verified the held turn never completed".to_owned(),
        };
        let first = {
            let mut store = router.inner.store.lock().await;
            let proof = TurnInactiveProof {
                turn_id: "turn-1".to_owned(),
            };
            let change = store
                .resolve_turn("owner", &request, Some(&proof), now_ms())
                .unwrap();
            assert_eq!(change.events.len(), 1);
            let agent_row = store.get_agent(&agent).unwrap().unwrap();
            assert!(!agent_row.state.is_terminal());
            assert_eq!(agent_row.generation, 1);
            assert_eq!(
                store
                    .turn_readback_command(&agent, 1, "turn-1")
                    .unwrap()
                    .as_deref(),
                Some(send_id.as_str())
            );
            serde_json::to_value(&change.value).unwrap()
        };
        assert_eq!(first["turn_id"], "turn-1");
        assert_eq!(first["status"], "failed");
        let mut events = router.inner.bus.subscribe();
        let payload = json!({"id":resolve_id, "agent":agent,
            "generation":1, "turn_id":"turn-1", "status":"failed",
            "reason":"owner verified the held turn never completed"});
        let replay = router
            .turn_resolve(&payload, &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(replay, first);
        // The replay resolves nothing new and performs no native I/O,
        // against a generation that is still live.
        assert!(events.try_recv().is_err());
        assert_eq!(router.inner.plugin_host.snapshot_turn_calls(), 1);
        drop(router);
        let _ = std::fs::remove_dir_all(&db_dir);
    }

    #[tokio::test]
    async fn command_resolve_allows_an_unknown_launch_only_as_failed() {
        let router = router();
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
            launch.args = json!({"project":"resolve-start-rpc", "workspace":"synthetic-workspace"});
            launch.expected_generation = Some(0);
            store.submit("owner", launch, now).unwrap();
            store.dispatch(&launch_id, now).unwrap();
            store
                .settle(&launch_id, CommandState::Unknown, None, now)
                .unwrap();
        }
        let error = router
            .command_resolve(
                &json!({"id":ulid::Ulid::new().to_string(), "command_id":launch_id,
                    "as":"applied", "reason":"must be refused without an Agent transition"}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        let resolve_id = ulid::Ulid::new().to_string();
        let response = router
            .command_resolve(
                &json!({"id":resolve_id, "command_id":launch_id,
                    "as":"failed", "reason":"owner verified the uncertain launch"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(response["id"], resolve_id);
        assert_eq!(response["command_id"], launch_id);
        assert_eq!(response["state"], "failed");
    }

    #[test]
    fn turn_inactive_proof_accepts_only_exact_inactive_snapshots() {
        // Recorded `agent.snapshot` shapes: Codex reports an explicit
        // terminal flag per turn, the fake reports active/last turns only.
        let codex_terminal = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "idle", "native_scope": "scope", "native_ref": "thread-1",
            "active_turn": null,
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "completed", "final_text": "done", "terminal": true},
        });
        assert_eq!(
            turn_inactive_proof(&codex_terminal, "a1", 1, "turn-1").unwrap(),
            TurnInactiveProof {
                turn_id: "turn-1".to_owned(),
            }
        );
        let codex_running = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "busy", "native_scope": "scope", "native_ref": "thread-1",
            "active_turn": "turn-1",
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "inProgress", "final_text": null, "terminal": false},
        });
        assert_eq!(turn_inactive_proof(&codex_running, "a1", 1, "turn-1"), None);
        let codex_unknown = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "unknown", "active_turn": null, "last_turn": null,
            "source_unknown": true, "result_known": false,
        });
        assert_eq!(turn_inactive_proof(&codex_unknown, "a1", 1, "turn-1"), None);
        // A last turn that is merely not active is not proof: it must be
        // explicitly terminal. Neither an in-progress status, an unknown
        // status, nor a missing terminal marker proves inactivity.
        for last_turn in [
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "inProgress", "final_text": null}),
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "running", "final_text": null}),
            json!({"command_id": "send-1", "turn_id": "turn-1"}),
        ] {
            let snapshot = json!({
                "agent_id": "a1", "generation": 1, "command_id": "send-1",
                "state": "busy", "active_turn": null, "last_turn": last_turn,
            });
            assert_eq!(
                turn_inactive_proof(&snapshot, "a1", 1, "turn-1"),
                None,
                "accepted non-terminal last turn {last_turn}"
            );
        }
        // A contradictory flag is not proof either: terminal:false with a
        // terminal status, or terminal:true with a running status.
        for last_turn in [
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "completed", "final_text": "done", "terminal": false}),
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "failed", "final_text": null, "terminal": false}),
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "interrupted", "final_text": null, "terminal": false}),
            json!({"command_id": "send-1", "turn_id": "turn-1",
                "status": "inProgress", "final_text": null, "terminal": true}),
        ] {
            let snapshot = json!({
                "agent_id": "a1", "generation": 1, "command_id": "send-1",
                "state": "busy", "active_turn": null, "last_turn": last_turn,
            });
            assert_eq!(
                turn_inactive_proof(&snapshot, "a1", 1, "turn-1"),
                None,
                "accepted contradictory last turn {last_turn}"
            );
        }

        // The fake reports a terminal status without the explicit terminal
        // flag: not proof.
        let fake_completed = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "idle", "active_turn": null,
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "completed", "final_text": "done"},
        });
        assert_eq!(
            turn_inactive_proof(&fake_completed, "a1", 1, "turn-1"),
            None
        );
        let fake_failed = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "idle", "active_turn": null,
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "failed", "final_text": null},
        });
        assert_eq!(turn_inactive_proof(&fake_failed, "a1", 1, "turn-1"), None);
        let interrupted = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "idle", "active_turn": null,
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "interrupted", "final_text": null},
        });
        assert_eq!(turn_inactive_proof(&interrupted, "a1", 1, "turn-1"), None);
        let fake_active = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "busy", "active_turn": "turn-1", "last_turn": null,
        });
        assert_eq!(turn_inactive_proof(&fake_active, "a1", 1, "turn-1"), None);
        // An uncertain fake turn keeps its active marker even though a last
        // turn is also recorded: still no proof.
        let fake_uncertain = json!({
            "agent_id": "a1", "generation": 1, "command_id": "send-1",
            "state": "unknown", "active_turn": "turn-1",
            "last_turn": {"command_id": "send-1", "turn_id": "turn-1",
                "status": "completed", "final_text": "done"},
        });
        assert_eq!(
            turn_inactive_proof(&fake_uncertain, "a1", 1, "turn-1"),
            None
        );

        // Binding mismatches and malformed shapes prove nothing.
        for (snapshot, agent, generation, turn) in [
            (codex_terminal.clone(), "a2", 1, "turn-1"),
            (codex_terminal.clone(), "a1", 2, "turn-1"),
            (codex_terminal.clone(), "a1", 1, "turn-2"),
            (fake_completed.clone(), "a1", 1, "turn-2"),
            (json!({}), "a1", 1, "turn-1"),
            (
                json!({"agent_id":"a1","generation":1,"last_turn":{}}),
                "a1",
                1,
                "turn-1",
            ),
        ] {
            assert_eq!(
                turn_inactive_proof(&snapshot, agent, generation, turn),
                None
            );
        }
    }
}
