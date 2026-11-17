use super::*;

#[test]
fn attention_commit_binds_agent_generation_and_subject_atomically() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-bind", now);
    let generation = store.get_agent(&agent).unwrap().unwrap().generation;
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"probe": true}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit attention emit");
    store
        .dispatch(&id, now + 1)
        .expect("dispatch attention emit");
    let report = json!({
        "schema": 1,
        "incident_id": "attention-test",
        "phase": "incident",
        "reason": "source_unhealthy",
        "agent_id": agent,
        "generation": generation,
    });
    let change = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": report}),
            now + 2,
        )
        .expect("atomic attention commit");
    assert_eq!(change.value.state, CommandState::Applied);
    assert_eq!(change.events.len(), 2);
    let event = &change.events[0];
    assert_eq!(event.kind, "x.user.agent.attention");
    assert_eq!(event.agent.as_deref(), Some(agent.as_str()));
    assert_eq!(event.generation, Some(generation));
    // The wire shape stays compatible: the report rides at data.data.
    assert_eq!(
        event
            .data
            .get("data")
            .and_then(|data| data.get("agent_id"))
            .and_then(|id| id.as_str()),
        Some(agent.as_str())
    );
    let expected_state = store.get_agent(&agent).unwrap().unwrap().state;
    assert_eq!(
        event
            .subject
            .as_ref()
            .expect("attention carries a subject")
            .state,
        expected_state
    );
    assert_eq!(change.events[1].kind, "command.applied");
    // An exact retry returns the recorded result without a new event.
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events after atomic commit");
    let retry = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": report}),
            now + 3,
        )
        .expect("idempotent attention retry");
    assert_eq!(retry.value.state, CommandState::Applied);
    assert!(retry.events.is_empty());
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after retry"),
        event_count
    );
}

#[test]
fn attention_commit_rejects_stale_evidence_without_a_binding() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-stale", now);
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"probe": true}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit attention emit");
    store
        .dispatch(&id, now + 1)
        .expect("dispatch attention emit");
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events before stale commit");
    let error = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": {"agent_id": agent, "generation": 999}}),
            now + 2,
        )
        .expect_err("stale generation is rejected");
    assert_eq!(error.code, "stale_generation");
    assert_eq!(
        store.get_command(&id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after stale commit"),
        event_count
    );
}

#[test]
fn public_emit_cannot_use_the_reserved_attention_topic() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-reserved", now);
    // A public `emit` naming the reserved topic is rejected at admission, so
    // it can never acquire an authoritative subject binding.
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: Ulid::new().to_string(),
                target: None,
                selector: None,
                verb: "emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"agent_id": agent, "generation": 1}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect_err("reserved topic is rejected");
    assert_eq!(error.code, "invalid");
    // The internal verb admits only the reserved topic.
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: Ulid::new().to_string(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.other", "data": {}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect_err("internal verb carries only the reserved topic");
    assert_eq!(error.code, "invalid");
    // Even a directly admitted public emit can never reach the bound commit:
    // the bound commit requires the internal verb.
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "emit".to_owned(),
                args: json!({"topic": "agent.probe", "data": {}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit ordinary emit");
    store.dispatch(&id, now + 1).expect("dispatch emit");
    let error = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": {"agent_id": agent, "generation": 1}}),
            now + 2,
        )
        .expect_err("public emit cannot use the bound commit");
    assert_eq!(error.code, "invalid");
}
