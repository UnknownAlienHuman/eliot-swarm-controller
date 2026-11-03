use super::{Store, TurnInactiveProof};
use eliot_core::{
    AgentState, CommandState, NativeEvent, SourceBatch, SourceHandoff, SourceHandoffGap,
    SubmitCommand, TurnState,
    api::{CommandResolution, CommandResolveRequest, TurnResolveRequest, TurnResolveStatus},
    parse_agent_id,
};
use rusqlite::OptionalExtension;
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use ulid::Ulid;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready"),
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit session ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle launch");
    agent
}

fn send_request(agent: &str, text: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "send");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
    request.args = json!({"text": text});
    request
}

fn native_batch(agent: &str, generation: i64, kind: &str, data: Value) -> SourceBatch {
    SourceBatch {
        batch_id: Ulid::new().to_string(),
        agent_id: agent.to_owned(),
        generation,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: kind.to_owned(),
            native_id: Ulid::new().to_string(),
            data,
        }],
        cursor: None,
    }
}

fn resolve_command_request(
    command_id: &str,
    resolution: CommandResolution,
) -> CommandResolveRequest {
    CommandResolveRequest {
        id: Ulid::new().to_string(),
        command_id: command_id.to_owned(),
        resolution,
        reason: "owner verified the uncertain effect".to_owned(),
    }
}

fn resolve_turn_request(
    agent: &str,
    generation: i64,
    turn_id: &str,
    status: TurnResolveStatus,
) -> TurnResolveRequest {
    TurnResolveRequest {
        id: Ulid::new().to_string(),
        agent: agent.to_owned(),
        generation,
        turn_id: turn_id.to_owned(),
        status,
        reason: "owner verified the uncertain turn".to_owned(),
    }
}

/// Resolution source recorded for a command: the outcome of a resolved
/// command carries `resolution_source=owner`; anything else has none.
fn command_resolution_source(store: &Store, id: &str) -> Option<String> {
    store
        .get_command(id)
        .unwrap()
        .and_then(|command| command.outcome)
        .and_then(|outcome| {
            outcome
                .get("resolution_source")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

/// Resolution source recorded for a turn: the latest `turn.resolved` event
/// for the exact turn carries `resolution_source=owner` in its data; a turn
/// with no such event was never owner-resolved.
fn turn_resolution_source(
    store: &Store,
    agent: &str,
    generation: i64,
    turn_id: &str,
) -> Option<String> {
    let agent_num = parse_agent_id(agent).unwrap();
    let data: Option<String> = store
        .connection
        .query_row(
            "SELECT data FROM events
             WHERE kind = 'turn.resolved' AND agent = ?1 AND generation = ?2
               AND json_extract(data, '$.turn_id') = ?3
             ORDER BY seq DESC LIMIT 1",
            rusqlite::params![agent_num, generation, turn_id],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    data.and_then(|json| {
        serde_json::from_str::<Value>(&json).ok().and_then(|value| {
            value
                .get("resolution_source")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
    })
}

fn write_row(store: &Store, id: &str) -> (String, String, Value) {
    store
        .connection
        .query_row(
            "SELECT principal, method, result FROM writes WHERE id = ?1",
            [id],
            |row| {
                let result: String = row.get(2)?;
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    serde_json::from_str(&result).unwrap(),
                ))
            },
        )
        .unwrap()
}

fn capacity_generation(store: &Store, agent: &str) -> Option<i64> {
    let agent_num = parse_agent_id(agent).unwrap();
    store
        .connection
        .query_row(
            "SELECT capacity_generation FROM agents WHERE id = ?1",
            [agent_num],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn resolve_unknown_send_to_failed_releases_only_its_input_gate() {
    let mut store = store();
    let now = now_ms();
    let first_agent = ready_agent(&mut store, "resolve-first", now);
    let second_agent = ready_agent(&mut store, "resolve-second", now);

    let first_send = send_request(&first_agent, "uncertain first input");
    let first_id = first_send.id.clone();
    store
        .submit("owner", first_send, now)
        .expect("admit first send");
    store.dispatch(&first_id, now).expect("dispatch first send");
    store
        .settle(&first_id, CommandState::Unknown, None, now)
        .expect("retain unknown first send");

    let second_send = send_request(&second_agent, "uncertain second input");
    let second_id = second_send.id.clone();
    store
        .submit("owner", second_send, now)
        .expect("admit second send");
    store
        .dispatch(&second_id, now)
        .expect("dispatch second send");
    store
        .settle(&second_id, CommandState::Unknown, None, now)
        .expect("retain unknown second send");

    // A later send queues behind each unknown gate.
    let first_next = send_request(&first_agent, "later first input");
    let first_next_id = first_next.id.clone();
    let admitted = store
        .submit("owner", first_next, now)
        .expect("admit later send");
    assert_eq!(admitted.command.state, CommandState::Queued);
    let second_next = send_request(&second_agent, "later second input");
    let second_next_id = second_next.id.clone();
    store
        .submit("owner", second_next, now)
        .expect("admit later send");

    let high_water = store.high_water().unwrap();
    let request = resolve_command_request(&first_id, CommandResolution::Failed);
    let resolve_id = request.id.clone();
    let change = store
        .resolve_command("owner", &request, now)
        .expect("resolve unknown send");
    assert_eq!(change.value.id, resolve_id);
    assert_eq!(change.value.command_id, first_id);
    assert_eq!(change.value.state, CommandState::Failed);
    assert_eq!(change.events.len(), 1);
    let event = &change.events[0];
    assert_eq!(event.kind, "command.resolved");
    assert_eq!(event.agent.as_deref(), Some(first_agent.as_str()));
    assert_eq!(event.generation, Some(1));
    assert_eq!(event.source, "daemon");
    assert_eq!(event.data["command_id"], first_id);
    assert_eq!(event.data["state"], "failed");
    assert_eq!(event.data["write_id"], resolve_id);
    assert_eq!(event.data["resolution_source"], "owner");
    assert_eq!(event.data["reason"], request.reason);

    let resolved = store.get_command(&first_id).unwrap().unwrap();
    assert_eq!(resolved.state, CommandState::Failed);
    assert_eq!(
        resolved.outcome,
        Some(json!({
            "resolution_source": "owner",
            "reason": request.reason,
        }))
    );
    assert!(resolved.turn_id.is_none());
    assert_eq!(
        command_resolution_source(&store, &first_id),
        Some("owner".to_owned())
    );
    let (principal, method, result) = write_row(&store, &resolve_id);
    assert_eq!(principal, "owner");
    assert_eq!(method, "command.resolve");
    assert_eq!(result["command_id"], first_id);
    assert_eq!(result["state"], "failed");

    // No native receipt was emitted for the assertion.
    let events = store.read_events(high_water, 100, None).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "command.resolved");

    // Only the resolved gate released: the sibling unknown still holds its Agent.
    let second = store.get_command(&second_id).unwrap().unwrap();
    assert_eq!(second.state, CommandState::Unknown);
    assert_eq!(command_resolution_source(&store, &second_id), None);
    let blocked = store.dispatch(&second_next_id, now).unwrap().value;
    assert_eq!(blocked.state, CommandState::Queued);
    let released = store.dispatch(&first_next_id, now).unwrap().value;
    assert_eq!(released.state, CommandState::Dispatched);
}

#[test]
fn resolve_unknown_interrupt_to_applied_keeps_turn_binding_and_turn_gate() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-applied", now);

    let send = send_request(&agent, "associated input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");

    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(agent.clone());
    interrupt.args = json!({});
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some("turn-1".to_owned());
    let interrupt_id = interrupt.id.clone();
    let admitted = store
        .submit("owner", interrupt, now)
        .expect("admit interrupt");
    assert_eq!(admitted.command.turn_id.as_deref(), Some("turn-1"));
    store
        .dispatch(&interrupt_id, now)
        .expect("dispatch interrupt");
    store
        .settle(&interrupt_id, CommandState::Unknown, None, now)
        .expect("retain unknown interrupt");

    let request = resolve_command_request(&interrupt_id, CommandResolution::Applied);
    let change = store
        .resolve_command("owner", &request, now)
        .expect("resolve unknown interrupt");
    assert_eq!(change.value.state, CommandState::Applied);
    assert_eq!(change.events.len(), 1);
    assert_eq!(change.events[0].kind, "command.resolved");
    assert_eq!(change.events[0].data["state"], "applied");

    let resolved = store.get_command(&interrupt_id).unwrap().unwrap();
    assert_eq!(resolved.state, CommandState::Applied);
    assert_eq!(resolved.turn_id.as_deref(), Some("turn-1"));
    assert_eq!(
        command_resolution_source(&store, &interrupt_id),
        Some("owner".to_owned())
    );

    // The live turn and its gate are untouched by the command assertion.
    let turn = store.get_turn(&agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(turn.state, TurnState::Running);
    assert_eq!(turn_resolution_source(&store, &agent, 1, "turn-1"), None);
    let agent_row = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(agent_row.active_turn.as_deref(), Some("turn-1"));
    let next = send_request(&agent, "later input");
    let next_id = next.id.clone();
    let admitted = store.submit("owner", next, now).expect("admit later send");
    assert_eq!(admitted.command.state, CommandState::Queued);

    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": send_id, "turn_id": "turn-1", "status": "completed"}),
            ),
            now,
        )
        .expect("commit native turn completion");
    let released = store.dispatch(&next_id, now).unwrap().value;
    assert_eq!(released.state, CommandState::Dispatched);
}

#[test]
fn resolve_send_with_lost_turn_association_invents_nothing() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-lost", now);

    let send = send_request(&agent, "input whose turn never committed");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .settle(&send_id, CommandState::Unknown, None, now)
        .expect("retain unknown send");
    assert!(
        store
            .get_command(&send_id)
            .unwrap()
            .unwrap()
            .turn_id
            .is_none()
    );

    let request = resolve_command_request(&send_id, CommandResolution::Failed);
    store
        .resolve_command("owner", &request, now)
        .expect("resolve send without a turn");

    let resolved = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(resolved.state, CommandState::Failed);
    assert!(resolved.turn_id.is_none());
    let turn_rows: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM turns", [], |row| row.get(0))
        .unwrap();
    assert_eq!(turn_rows, 0);
    let turn_events = store
        .read_events(0, 1000, None)
        .unwrap()
        .into_iter()
        .filter(|event| event.kind.starts_with("turn."))
        .count();
    assert_eq!(turn_events, 0);
}

#[test]
fn resolve_unknown_launch_leaves_agent_and_capacity_untouched() {
    let mut store = store();
    let now = now_ms();
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":"resolve-launch", "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    store
        .settle(&launch_id, CommandState::Unknown, None, now)
        .expect("retain unknown launch");

    let high_water = store.high_water().unwrap();
    let request = resolve_command_request(&launch_id, CommandResolution::Failed);
    let change = store
        .resolve_command("owner", &request, now)
        .expect("resolve unknown launch");
    assert_eq!(change.value.state, CommandState::Failed);
    assert_eq!(
        store.get_command(&launch_id).unwrap().unwrap().state,
        CommandState::Failed
    );
    assert_eq!(
        command_resolution_source(&store, &launch_id),
        Some("owner".to_owned())
    );

    // Unlike native launch failure, the owner assertion neither fails the
    // Agent nor releases its capacity reservation.
    let agent_row = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(agent_row.state, AgentState::Starting);
    assert_eq!(agent_row.generation, 1);
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let events = store.read_events(high_water, 100, None).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "command.resolved");
}

#[test]
fn resolve_command_refusals_keep_unknown_work_fenced() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-refusals", now);

    let applied_send = send_request(&agent, "applied input");
    let applied_id = applied_send.id.clone();
    store
        .submit("owner", applied_send, now)
        .expect("admit send");
    store.dispatch(&applied_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": applied_id, "turn_id": "turn-applied"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": applied_id, "turn_id": "turn-applied", "status": "completed"}),
            ),
            now,
        )
        .expect("commit native turn completion");
    store
        .settle(
            &applied_id,
            CommandState::Applied,
            Some(json!({"turn_id": "turn-applied"})),
            now,
        )
        .expect("settle native applied send");

    let dispatched_send = send_request(&agent, "dispatched input");
    let dispatched_id = dispatched_send.id.clone();
    store
        .submit("owner", dispatched_send, now)
        .expect("admit send");
    store.dispatch(&dispatched_id, now).expect("dispatch send");

    let queued_send = send_request(&agent, "queued input");
    let queued_id = queued_send.id.clone();
    let queued = store.submit("owner", queued_send, now).expect("admit send");
    assert_eq!(queued.command.state, CommandState::Queued);

    for ((command_id, expected), resolution) in [
        (("missing-command", "not_found"), CommandResolution::Failed),
        (
            (applied_id.as_str(), "bad_state"),
            CommandResolution::Failed,
        ),
        (
            (dispatched_id.as_str(), "bad_state"),
            CommandResolution::Failed,
        ),
        (
            (dispatched_id.as_str(), "bad_state"),
            CommandResolution::Applied,
        ),
        ((queued_id.as_str(), "bad_state"), CommandResolution::Failed),
    ] {
        let error = store
            .resolve_command(
                "owner",
                &resolve_command_request(command_id, resolution),
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, expected, "command {command_id}");
    }

    // A live dispatched command stays fenced: no assertion was recorded.
    assert_eq!(
        store.get_command(&dispatched_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    assert_eq!(command_resolution_source(&store, &dispatched_id), None);

    let error = store
        .resolve_command(
            "agent:a1:1",
            &resolve_command_request(&dispatched_id, CommandResolution::Failed),
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "not_allowed");

    let mut empty_command = resolve_command_request(&dispatched_id, CommandResolution::Failed);
    empty_command.command_id = " ".to_owned();
    assert_eq!(
        store
            .resolve_command("owner", &empty_command, now)
            .unwrap_err()
            .code,
        "invalid"
    );
    let mut empty_reason = resolve_command_request(&dispatched_id, CommandResolution::Failed);
    empty_reason.reason = String::new();
    assert_eq!(
        store
            .resolve_command("owner", &empty_reason, now)
            .unwrap_err()
            .code,
        "invalid"
    );
}

#[test]
fn resolve_command_write_ids_replay_or_conflict() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-replay", now);

    let send = send_request(&agent, "uncertain input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .settle(&send_id, CommandState::Unknown, None, now)
        .expect("retain unknown send");

    let request = resolve_command_request(&send_id, CommandResolution::Failed);
    let resolve_id = request.id.clone();
    let first = store
        .resolve_command("owner", &request, now)
        .expect("resolve unknown send");
    assert_eq!(first.events.len(), 1);
    let high_water = store.high_water().unwrap();

    let replay = store
        .resolve_command("owner", &request, now)
        .expect("replay retained resolution");
    assert_eq!(replay.value, first.value);
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), high_water);

    let mut conflict = request.clone();
    conflict.reason = "a different assertion".to_owned();
    assert_eq!(
        store
            .resolve_command("owner", &conflict, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let mut command_collision = resolve_command_request(&send_id, CommandResolution::Failed);
    command_collision.id = send_id.clone();
    assert_eq!(
        store
            .resolve_command("owner", &command_collision, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let turn_collision = TurnResolveRequest {
        id: resolve_id.clone(),
        agent: agent.clone(),
        generation: 1,
        turn_id: "turn-1".to_owned(),
        status: TurnResolveStatus::Failed,
        reason: "owner verified the uncertain turn".to_owned(),
    };
    assert_eq!(
        store
            .resolve_turn("owner", &turn_collision, None, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let expired_ms = now - (25 * 60 * 60 * 1000);
    let expired = CommandResolveRequest {
        id: Ulid::from_parts(expired_ms as u64, 0).to_string(),
        command_id: send_id.clone(),
        resolution: CommandResolution::Failed,
        reason: "stale assertion".to_owned(),
    };
    assert_eq!(
        store
            .resolve_command("owner", &expired, now)
            .unwrap_err()
            .code,
        "id_expired"
    );
}

#[test]
fn owner_command_assertion_is_distinguishable_from_native_receipt() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-provenance", now);

    let native_send = send_request(&agent, "native input");
    let native_id = native_send.id.clone();
    store.submit("owner", native_send, now).expect("admit send");
    store.dispatch(&native_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": native_id, "turn_id": "turn-native"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": native_id, "turn_id": "turn-native", "status": "completed"}),
            ),
            now,
        )
        .expect("commit native turn completion");
    let native_outcome = json!({"turn_id": "turn-native"});
    store
        .settle(
            &native_id,
            CommandState::Applied,
            Some(native_outcome.clone()),
            now,
        )
        .expect("settle native applied send");

    let uncertain_send = send_request(&agent, "uncertain input");
    let uncertain_id = uncertain_send.id.clone();
    store
        .submit("owner", uncertain_send, now)
        .expect("admit send");
    store.dispatch(&uncertain_id, now).expect("dispatch send");
    store
        .settle(&uncertain_id, CommandState::Unknown, None, now)
        .expect("retain unknown send");
    let request = resolve_command_request(&uncertain_id, CommandResolution::Applied);
    store
        .resolve_command("owner", &request, now)
        .expect("resolve unknown send");

    let native = store.get_command(&native_id).unwrap().unwrap();
    assert_eq!(native.outcome, Some(native_outcome));
    assert_eq!(command_resolution_source(&store, &native_id), None);
    let asserted = store.get_command(&uncertain_id).unwrap().unwrap();
    assert_eq!(
        asserted.outcome,
        Some(json!({"resolution_source": "owner", "reason": request.reason}))
    );
    assert_eq!(
        command_resolution_source(&store, &uncertain_id),
        Some("owner".to_owned())
    );

    let kinds: Vec<(String, String)> = store
        .read_events(0, 1000, None)
        .unwrap()
        .into_iter()
        .filter_map(|event| {
            event.data.get("command_id").and_then(|id| {
                (id == &native_id || id == &uncertain_id)
                    .then(|| (id.as_str().unwrap().to_owned(), event.kind.clone()))
            })
        })
        .collect();
    assert!(kinds.contains(&(native_id.clone(), "command.applied".to_owned())));
    assert!(!kinds.contains(&(native_id, "command.resolved".to_owned())));
    assert!(kinds.contains(&(uncertain_id.clone(), "command.resolved".to_owned())));
    assert!(!kinds.contains(&(uncertain_id, "command.applied".to_owned())));
}

#[test]
fn resolve_unknown_turn_after_stop_completes_with_asserted_status() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-turn", now);

    let send = send_request(&agent, "held input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");

    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now).expect("admit stop");
    store.dispatch(&stop_id, now).expect("dispatch stop");
    store
        .settle(&stop_id, CommandState::Applied, None, now)
        .expect("settle applied stop");

    let agent_row = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(agent_row.state, AgentState::Stopped);
    assert!(agent_row.active_turn.is_none());
    let turn = store.get_turn(&agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(turn.state, TurnState::Unknown);
    // The applied send keeps its exact turn association across the stop.
    let send_row = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(send_row.state, CommandState::Applied);
    assert_eq!(send_row.turn_id.as_deref(), Some("turn-1"));

    let high_water = store.high_water().unwrap();
    let request = resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Failed);
    let resolve_id = request.id.clone();
    let change = store
        .resolve_turn("owner", &request, None, now)
        .expect("resolve unknown turn");
    assert_eq!(change.value.id, resolve_id);
    assert_eq!(change.value.agent, agent);
    assert_eq!(change.value.generation, 1);
    assert_eq!(change.value.turn_id, "turn-1");
    assert_eq!(change.value.status, TurnResolveStatus::Failed);
    assert_eq!(change.events.len(), 1);
    let event = &change.events[0];
    assert_eq!(event.kind, "turn.resolved");
    assert_eq!(event.agent.as_deref(), Some(agent.as_str()));
    assert_eq!(event.generation, Some(1));
    assert_eq!(event.source, "daemon");
    assert_eq!(event.data["turn_id"], "turn-1");
    assert_eq!(event.data["status"], "failed");
    assert_eq!(event.data["write_id"], resolve_id);
    assert_eq!(event.data["resolution_source"], "owner");
    assert_eq!(event.data["reason"], request.reason);

    let resolved = store.get_turn(&agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(resolved.state, TurnState::Completed);
    assert_eq!(resolved.status.as_deref(), Some("failed"));
    assert_eq!(resolved.completed_at, Some(now));
    assert!(resolved.final_text.is_none());
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "turn-1"),
        Some("owner".to_owned())
    );
    let (principal, method, result) = write_row(&store, &resolve_id);
    assert_eq!(principal, "owner");
    assert_eq!(method, "turn.resolve");
    assert_eq!(result["turn_id"], "turn-1");
    assert_eq!(result["status"], "failed");

    // Only the turn gate released: the associated send and Agent are untouched.
    let send_row = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(send_row.state, CommandState::Applied);
    assert_eq!(send_row.turn_id.as_deref(), Some("turn-1"));
    assert_eq!(command_resolution_source(&store, &send_id), None);
    let agent_row = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(agent_row.state, AgentState::Stopped);
    let events = store.read_events(high_water, 100, None).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "turn.resolved");
}

#[test]
fn resolve_turn_refusals_keep_active_and_terminal_turns_fenced() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-turn-refusals", now);

    let send = send_request(&agent, "held input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-active"}),
            ),
            now,
        )
        .expect("commit exact turn association");

    // A still-active turn in a live generation cannot be owner-resolved.
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed),
            None,
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    assert_eq!(
        store
            .get_turn(&agent, 1, "turn-active")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Running
    );
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "turn-active"),
        None
    );

    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": send_id, "turn_id": "turn-active", "status": "completed"}),
            ),
            now,
        )
        .expect("commit native turn completion");
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed),
            None,
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");

    for (agent_id, generation, turn_id, expected) in [
        ("a999", 1, "turn-active", "not_found"),
        (agent.as_str(), 1, "turn-missing", "not_found"),
        (agent.as_str(), 2, "turn-active", "stale_generation"),
    ] {
        let error = store
            .resolve_turn(
                "owner",
                &resolve_turn_request(agent_id, generation, turn_id, TurnResolveStatus::Failed),
                None,
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, expected, "turn {turn_id}");
    }

    let error = store
        .resolve_turn(
            "agent:a1:1",
            &resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed),
            None,
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "not_allowed");

    let mut empty_agent = resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed);
    empty_agent.agent = " ".to_owned();
    assert_eq!(
        store
            .resolve_turn("owner", &empty_agent, None, now)
            .unwrap_err()
            .code,
        "invalid"
    );
    let mut negative = resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed);
    negative.generation = -1;
    assert_eq!(
        store
            .resolve_turn("owner", &negative, None, now)
            .unwrap_err()
            .code,
        "invalid"
    );
    let mut empty_turn = resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed);
    empty_turn.turn_id = String::new();
    assert_eq!(
        store
            .resolve_turn("owner", &empty_turn, None, now)
            .unwrap_err()
            .code,
        "invalid"
    );
    let mut empty_reason =
        resolve_turn_request(&agent, 1, "turn-active", TurnResolveStatus::Failed);
    empty_reason.reason = "  ".to_owned();
    assert_eq!(
        store
            .resolve_turn("owner", &empty_reason, None, now)
            .unwrap_err()
            .code,
        "invalid"
    );
}

#[test]
fn resolve_turn_write_ids_replay_or_conflict() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-turn-replay", now);

    let send = send_request(&agent, "held input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now).expect("admit stop");
    store.dispatch(&stop_id, now).expect("dispatch stop");
    store
        .settle(&stop_id, CommandState::Applied, None, now)
        .expect("settle applied stop");

    let request = resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Interrupted);
    let resolve_id = request.id.clone();
    let first = store
        .resolve_turn("owner", &request, None, now)
        .expect("resolve unknown turn");
    assert_eq!(first.events.len(), 1);
    let high_water = store.high_water().unwrap();

    let replay = store
        .resolve_turn("owner", &request, None, now)
        .expect("replay retained resolution");
    assert_eq!(replay.value, first.value);
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), high_water);

    let mut conflict = request.clone();
    conflict.status = TurnResolveStatus::Completed;
    assert_eq!(
        store
            .resolve_turn("owner", &conflict, None, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let mut command_collision =
        resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Interrupted);
    command_collision.id = send_id.clone();
    assert_eq!(
        store
            .resolve_turn("owner", &command_collision, None, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let mut cross_collision = resolve_command_request(&send_id, CommandResolution::Failed);
    cross_collision.id = resolve_id;
    assert_eq!(
        store
            .resolve_command("owner", &cross_collision, now)
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let expired_ms = now - (25 * 60 * 60 * 1000);
    let expired = TurnResolveRequest {
        id: Ulid::from_parts(expired_ms as u64, 0).to_string(),
        agent: agent.clone(),
        generation: 1,
        turn_id: "turn-1".to_owned(),
        status: TurnResolveStatus::Failed,
        reason: "stale assertion".to_owned(),
    };
    assert_eq!(
        store
            .resolve_turn("owner", &expired, None, now)
            .unwrap_err()
            .code,
        "id_expired"
    );
}

#[test]
fn resolve_turn_leaves_sibling_unknown_turn_untouched() {
    let mut store = store();
    let now = now_ms();
    let first_agent = ready_agent(&mut store, "resolve-turn-first", now);
    let second_agent = ready_agent(&mut store, "resolve-turn-second", now);

    for (agent, project) in [(&first_agent, "first"), (&second_agent, "second")] {
        let send = send_request(agent, &format!("{project} held input"));
        let send_id = send.id.clone();
        store.submit("owner", send, now).expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        store
            .put_batch(
                native_batch(
                    agent,
                    1,
                    "turn.started",
                    json!({"command_id": send_id, "turn_id": "turn-1"}),
                ),
                now,
            )
            .expect("commit exact turn association");
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.target = Some(agent.clone());
        stop.expected_generation = Some(1);
        let stop_id = stop.id.clone();
        store.submit("owner", stop, now).expect("admit stop");
        store.dispatch(&stop_id, now).expect("dispatch stop");
        store
            .settle(&stop_id, CommandState::Applied, None, now)
            .expect("settle applied stop");
    }

    store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&first_agent, 1, "turn-1", TurnResolveStatus::Failed),
            None,
            now,
        )
        .expect("resolve first unknown turn");

    let resolved = store.get_turn(&first_agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(resolved.state, TurnState::Completed);
    let sibling = store.get_turn(&second_agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(sibling.state, TurnState::Unknown);
    assert_eq!(
        turn_resolution_source(&store, &second_agent, 1, "turn-1"),
        None
    );
}

#[test]
fn owner_turn_assertion_is_distinguishable_from_native_completion() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-turn-provenance", now);

    let native_send = send_request(&agent, "native input");
    let native_id = native_send.id.clone();
    store.submit("owner", native_send, now).expect("admit send");
    store.dispatch(&native_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": native_id, "turn_id": "turn-native"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": native_id, "turn_id": "turn-native", "status": "completed"}),
            ),
            now,
        )
        .expect("commit native turn completion");

    let uncertain_send = send_request(&agent, "uncertain input");
    let uncertain_id = uncertain_send.id.clone();
    store
        .submit("owner", uncertain_send, now)
        .expect("admit send");
    store.dispatch(&uncertain_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": uncertain_id, "turn_id": "turn-uncertain"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now).expect("admit stop");
    store.dispatch(&stop_id, now).expect("dispatch stop");
    store
        .settle(&stop_id, CommandState::Applied, None, now)
        .expect("settle applied stop");
    store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-uncertain", TurnResolveStatus::Failed),
            None,
            now,
        )
        .expect("resolve unknown turn");

    let native = store.get_turn(&agent, 1, "turn-native").unwrap().unwrap();
    assert_eq!(native.state, TurnState::Completed);
    assert_eq!(native.status.as_deref(), Some("completed"));
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "turn-native"),
        None
    );
    let asserted = store
        .get_turn(&agent, 1, "turn-uncertain")
        .unwrap()
        .unwrap();
    assert_eq!(asserted.state, TurnState::Completed);
    assert_eq!(asserted.status.as_deref(), Some("failed"));
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "turn-uncertain"),
        Some("owner".to_owned())
    );

    let kinds: Vec<(String, String)> = store
        .read_events(0, 1000, None)
        .unwrap()
        .into_iter()
        .filter_map(|event| {
            event.data.get("turn_id").and_then(|id| {
                (id == "turn-native" || id == "turn-uncertain")
                    .then(|| (id.as_str().unwrap().to_owned(), event.kind.clone()))
            })
        })
        .collect();
    assert!(kinds.contains(&("turn-native".to_owned(), "turn.completed".to_owned())));
    assert!(!kinds.contains(&("turn-native".to_owned(), "turn.resolved".to_owned())));
    assert!(kinds.contains(&("turn-uncertain".to_owned(), "turn.resolved".to_owned())));
    assert!(!kinds.contains(&("turn-uncertain".to_owned(), "turn.completed".to_owned())));
}

fn attach_request() -> SubmitCommand {
    SubmitCommand {
        id: Ulid::new().to_string(),
        target: None,
        verb: "attach".to_owned(),
        args: json!({"mode":"observe","profile":"fake","harness":"fake",
            "native_scope":"fixture:scope","native_ref":"fixture-native",
            "profile_snapshot":{"harness":"fake"}}),
        expected_generation: None,
        expected_turn_id: None,
        request_id: None,
        probe: false,
    }
}

fn attach_observer(store: &mut Store, now: i64) -> (String, String) {
    let request = attach_request();
    let command_id = request.id.clone();
    let admitted = store.submit("owner", request, now).expect("admit attach");
    let agent = admitted.command.target.expect("attach target");
    store.dispatch(&command_id, now).expect("dispatch attach");
    (agent, command_id)
}

fn settle_attach(store: &mut Store, agent: &str, command_id: &str, now: i64) {
    store
        .settle_observe_attach(
            command_id,
            json!({"agent_id":agent,"generation":1,"command_id":command_id,
                "native_scope":"fixture:scope","native_ref":"fixture-native",
                "state":"attached","observation_state":"unknown",
                "metadata":{},"coverage":{}}),
            now,
        )
        .expect("settle observer attach");
}

fn observe_transcript(store: &mut Store, agent: &str, now: i64) {
    store
        .record_observed_source_handoff(
            SourceHandoff {
                agent_id: agent.to_owned(),
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
            now,
        )
        .expect("hand observed turns to the transcript reader");
}

fn observed_message(store: &mut Store, agent: &str, turn_id: &str, now: i64) {
    let batch_id = Ulid::new().to_string();
    store
        .put_observed_batch(
            SourceBatch {
                batch_id: batch_id.clone(),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "transcript".to_owned(),
                events: vec![NativeEvent {
                    ts: Some(now),
                    kind: "message.completed".to_owned(),
                    native_id: batch_id.clone(),
                    data: json!({"turn_id":turn_id,"text":"observed answer","phase":"final_answer"}),
                }],
                cursor: Some(format!("edge:{batch_id}")),
            },
            ("fixture:scope", "fixture-native"),
            now,
        )
        .expect("commit observed message");
}

#[test]
fn resolve_turn_refuses_observe_only_agents_before_and_after_restart() {
    let mut store = store();
    let now = now_ms();
    let (agent, attach_id) = attach_observer(&mut store, now);
    settle_attach(&mut store, &agent, &attach_id, now + 1);
    observe_transcript(&mut store, &agent, now + 2);
    observed_message(&mut store, &agent, "observed-turn", now + 3);
    let turn = store.get_turn(&agent, 1, "observed-turn").unwrap().unwrap();
    assert_eq!(turn.state, TurnState::Unknown);

    // An attached observer owns nothing: refusal does not wait for a restart.
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "observed-turn", TurnResolveStatus::Failed),
            None,
            now + 4,
        )
        .unwrap_err();
    assert_eq!(error.code, "not_allowed");

    // Restart fences the observer as unknown with no active turn; the stored
    // unknown turn must still not pass.
    store.recover_on_restart(now + 5).expect("recover");
    let agent_row = store.get_agent(&agent).unwrap().unwrap();
    assert!(agent_row.observe_only);
    assert_eq!(agent_row.state, AgentState::Unknown);
    assert!(agent_row.active_turn.is_none());
    assert_eq!(
        store
            .get_turn(&agent, 1, "observed-turn")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Unknown
    );

    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "observed-turn", TurnResolveStatus::Failed),
            None,
            now + 6,
        )
        .unwrap_err();
    assert_eq!(error.code, "not_allowed");
    assert_eq!(
        store
            .get_turn(&agent, 1, "observed-turn")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Unknown
    );
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "observed-turn"),
        None
    );
}

#[test]
fn resolve_command_refuses_observe_only_agent_commands() {
    let mut store = store();
    let now = now_ms();
    let (agent, attach_id) = attach_observer(&mut store, now);
    store.recover_on_restart(now + 1).expect("recover");
    assert_eq!(
        store.get_command(&attach_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert!(store.get_agent(&agent).unwrap().unwrap().observe_only);

    for resolution in [CommandResolution::Failed, CommandResolution::Applied] {
        let error = store
            .resolve_command(
                "owner",
                &resolve_command_request(&attach_id, resolution),
                now + 2,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }
    assert_eq!(
        store.get_command(&attach_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(command_resolution_source(&store, &attach_id), None);
}

#[test]
fn resolve_turn_needs_a_terminal_generation_or_a_native_proof() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-live-proof", now);

    let send = send_request(&agent, "held input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    assert!(
        !store
            .get_agent(&agent)
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
    );

    // No current Store flow marks a controlled turn unknown while its
    // generation stays live (stop and restart always terminalize it), so pin
    // the gate directly: the stored active turn alone is never proof.
    let agent_num = parse_agent_id(&agent).unwrap();
    store
        .connection
        .execute(
            "UPDATE turns SET state = 'unknown' WHERE agent = ?1 AND generation = 1 AND turn_id = 'turn-1'",
            [agent_num],
        )
        .expect("retain unknown turn on the live generation");
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Failed),
            None,
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    store
        .connection
        .execute(
            "UPDATE agents SET active_turn = NULL WHERE id = ?1",
            [agent_num],
        )
        .expect("clear the stored active turn");
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Failed),
            None,
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");

    // A proof for another turn proves nothing about this one.
    let other = TurnInactiveProof {
        turn_id: "turn-other".to_owned(),
    };
    let error = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Failed),
            Some(&other),
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");

    // A native readback proving this exact turn inactive releases it, while
    // the live Agent and its send stay exactly as they were.
    let proof = TurnInactiveProof {
        turn_id: "turn-1".to_owned(),
    };
    let change = store
        .resolve_turn(
            "owner",
            &resolve_turn_request(&agent, 1, "turn-1", TurnResolveStatus::Failed),
            Some(&proof),
            now,
        )
        .expect("resolve proven-inactive turn on a live generation");
    assert_eq!(change.events.len(), 1);
    assert_eq!(change.events[0].kind, "turn.resolved");
    let resolved = store.get_turn(&agent, 1, "turn-1").unwrap().unwrap();
    assert_eq!(resolved.state, TurnState::Completed);
    assert_eq!(resolved.status.as_deref(), Some("failed"));
    assert_eq!(
        turn_resolution_source(&store, &agent, 1, "turn-1"),
        Some("owner".to_owned())
    );
    assert!(
        !store
            .get_agent(&agent)
            .unwrap()
            .unwrap()
            .state
            .is_terminal()
    );
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Applied
    );
}

#[test]
fn resolve_command_allows_launch_and_resume_only_as_failed() {
    let mut store = store();
    let now = now_ms();

    let launch_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":"resolve-start-rule", "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit launch");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    store
        .settle(&launch_id, CommandState::Unknown, None, now)
        .expect("retain unknown launch");
    let error = store
        .resolve_command(
            "owner",
            &resolve_command_request(&launch_id, CommandResolution::Applied),
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    assert_eq!(
        store.get_command(&launch_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(command_resolution_source(&store, &launch_id), None);

    let agent = ready_agent(&mut store, "resolve-resume-rule", now);
    store
        .put_batch(native_batch(&agent, 1, "session.ended", json!({})), now)
        .expect("commit terminal session fact");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now).expect("admit stop");
    store.dispatch(&stop_id, now).expect("dispatch stop");
    store
        .settle(&stop_id, CommandState::Applied, None, now)
        .expect("settle applied stop");
    // The synthetic launch retains no native subject; record one so the
    // resume exercises admission instead of the native-subject fence.
    store
        .connection
        .execute(
            "UPDATE agents SET native_scope = 'fixture:scope', native_ref = 'fixture-native'
             WHERE id = ?1",
            [parse_agent_id(&agent).unwrap()],
        )
        .expect("retain a native subject for the stopped Agent");
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store.submit("owner", resume, now).expect("admit resume");
    store.dispatch(&resume_id, now).expect("dispatch resume");
    store
        .settle(&resume_id, CommandState::Unknown, None, now)
        .expect("retain unknown resume");
    let error = store
        .resolve_command(
            "owner",
            &resolve_command_request(&resume_id, CommandResolution::Applied),
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    store
        .resolve_command(
            "owner",
            &resolve_command_request(&resume_id, CommandResolution::Failed),
            now,
        )
        .expect("resolve unknown resume as failed");
    let resolved = store.get_command(&resume_id).unwrap().unwrap();
    assert_eq!(resolved.state, CommandState::Failed);
    assert_eq!(
        command_resolution_source(&store, &resume_id),
        Some("owner".to_owned())
    );
}

#[test]
fn turn_readback_command_finds_the_latest_bound_send() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "resolve-readback", now);

    let send = send_request(&agent, "held input");
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    assert_eq!(
        store.turn_readback_command(&agent, 1, "turn-1").unwrap(),
        Some(send_id.clone())
    );
    assert_eq!(
        store
            .turn_readback_command(&agent, 1, "turn-missing")
            .unwrap(),
        None
    );

    // A turn control bound to the same turn is not a readback binding.
    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(agent.clone());
    interrupt.args = json!({});
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some("turn-1".to_owned());
    store
        .submit("owner", interrupt, now)
        .expect("admit interrupt");
    assert_eq!(
        store.turn_readback_command(&agent, 1, "turn-1").unwrap(),
        Some(send_id)
    );

    let other = ready_agent(&mut store, "resolve-readback-other", now);
    assert_eq!(
        store.turn_readback_command(&other, 1, "turn-1").unwrap(),
        None
    );
}
