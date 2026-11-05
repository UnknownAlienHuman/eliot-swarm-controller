use super::*;
use eliot_core::api::HierarchyLimits;

const NOW: i64 = 1_800_000_000_000;
const SCOPE: &str = "codex:test-native-store";

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn producer(store: &mut Store, native_ref: &str) -> String {
    let command_id = Ulid::from_parts(NOW as u64, (count(store, "agents") + 1) as u128);
    let mut launch = SubmitCommand::new(command_id.to_string(), "launch");
    launch.expected_generation = Some(0);
    launch.args = json!({"harness":"codex", "profile":"codex-test",
        "profile_snapshot":{}, "project":"native-child-test", "workspace":"synthetic-workspace"});
    let command = launch.id.clone();
    let id = store
        .submit("owner", launch, NOW)
        .unwrap()
        .command
        .target
        .unwrap();
    store.dispatch(&command, NOW).unwrap();
    store.put_batch(SourceBatch {
        batch_id: format!("{native_ref}:ready"), agent_id: id.clone(), generation: 1,
        source: INITIAL_SOURCE.to_owned(), cursor: None,
        events: vec![NativeEvent { kind: "session.ready".to_owned(),
            native_id: format!("{native_ref}:ready"), ts: None,
            data: json!({"command_id":command,"native_scope":SCOPE,"native_ref":native_ref}) }],
    }, NOW).unwrap();
    store
        .settle(&command, CommandState::Applied, None, NOW)
        .unwrap();
    id
}

fn request(parent: &str, parent_ref: &str, child_ref: &str) -> ChildRegistration {
    ChildRegistration {
        registration_id: format!("spawn:{child_ref}"),
        parent_id: parent.to_owned(),
        parent_generation: 1,
        parent_native_ref: parent_ref.to_owned(),
        native_scope: SCOPE.to_owned(),
        native_ref: child_ref.to_owned(),
    }
}

fn register(store: &mut Store, producer: &str, request: ChildRegistration) -> ChildRegistrationAck {
    store
        .register_native_child(producer, 1, request, NOW + 1)
        .unwrap()
        .value
}

fn event(kind: &str, id: &str, data: Value) -> NativeEvent {
    NativeEvent {
        kind: kind.to_owned(),
        native_id: id.to_owned(),
        ts: Some(NOW + 2),
        data,
    }
}

fn batch(child: &str, id: &str, events: Vec<NativeEvent>) -> SourceBatch {
    SourceBatch {
        batch_id: id.to_owned(),
        agent_id: child.to_owned(),
        generation: 1,
        source: INITIAL_SOURCE.to_owned(),
        events,
        cursor: Some(id.to_owned()),
    }
}

fn gap(child: &str) -> SourceGap {
    SourceGap {
        gap_id: "child-buffer-overflow".to_owned(),
        agent_id: child.to_owned(),
        generation: 1,
        source: INITIAL_SOURCE.to_owned(),
        families: vec!["turn".to_owned(), "message".to_owned()],
        reason: "native child buffer overflowed".to_owned(),
        from: None,
        to: None,
    }
}

fn count(store: &Store, table: &str) -> i64 {
    // Test-owned, constant table names only.
    store
        .connection
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[test]
fn registration_commits_tokenless_child_and_only_its_observation_source() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let request = request(&parent, "parent", "child");
    let change = store
        .register_native_child(&parent, 1, request.clone(), NOW + 1)
        .unwrap();
    request.validate_ack(&change.value).unwrap();
    assert_eq!(change.events.len(), 1);
    assert_eq!(change.events[0].kind, "agent.created");
    let child = &change.value.agent;
    assert_eq!(store.get_agent(&child.id).unwrap().as_ref(), Some(child));
    assert_eq!(child.parent.as_deref(), Some(parent.as_str()));
    assert_eq!(child.root, parent);
    assert_eq!(child.depth, 1);
    assert_eq!(child.origin, core::AgentOrigin::Native);
    assert!(child.observe_only);
    assert!(!child.own_identity);
    assert_eq!(child.state, AgentState::Unknown);
    assert_eq!(child.workspace, None);
    assert_eq!(child.effective, None);
    assert_eq!(store.get_agent_token(&child.id, 1).unwrap(), None);
    let sources = store.list_source_states(&child.id, 1).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(
        sources[0]["families"],
        json!(["message", "session", "turn"])
    );
    assert_eq!(sources[0]["source"], "stream");
    assert_eq!(sources[0]["state"], "open");
    assert_eq!(count(&store, "processes"), 0);
    assert_eq!(count(&store, "native_child_registrations"), 1);
    assert_eq!(
        store
            .ensure_process_source(&child.id, 1, NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn registration_retry_after_gap_and_reopen_returns_the_original_ack_without_events() {
    let path = std::env::temp_dir().join(format!("eliot-native-child-{}", Ulid::new()));
    let (parent, request, original) = {
        let mut store = Store::open(&path).unwrap();
        let parent = producer(&mut store, "parent");
        let request = request(&parent, "parent", "child");
        let ack = register(&mut store, &parent, request.clone());
        store
            .record_native_child_source_gap(&parent, 1, gap(&ack.agent.id), NOW + 2)
            .unwrap();
        (parent, request, ack)
    };
    let mut reopened = Store::open(&path).unwrap();
    let high_water = reopened.high_water().unwrap();
    let replay = reopened
        .register_native_child(&parent, 1, request, NOW + 50)
        .unwrap();
    assert_eq!(replay.value, original);
    assert!(replay.events.is_empty());
    assert_eq!(reopened.high_water().unwrap(), high_water);
    assert_eq!(count(&reopened, "agents"), 2);
    assert_eq!(count(&reopened, "native_child_registrations"), 1);
    assert_eq!(
        reopened
            .read_native_child_source_state(&parent, 1, &original.agent.id, 1, "stream")
            .unwrap()["gap_id"],
        "child-buffer-overflow"
    );
    drop(reopened);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn native_children_keep_immediate_parent_and_visibility_above_every_launch_limit() {
    let mut store = store();
    let root = producer(&mut store, "root");
    store
        .set_hierarchy_limits(HierarchyLimits {
            max_depth: 1,
            max_children_per_agent: 1,
            max_tree_agents: 1,
            ..HierarchyLimits::default()
        })
        .unwrap();
    let first = register(&mut store, &root, request(&root, "root", "first"));
    let second = register(&mut store, &root, request(&root, "root", "second"));
    let nested = register(
        &mut store,
        &root,
        request(&first.agent.id, "first", "nested"),
    );
    let mut other_nested = request(&second.agent.id, "second", "other-nested");
    // Registration ids are scoped to the immediate parent generation, even
    // when both parents use the same authenticated producer connection.
    other_nested.registration_id = "spawn:nested".to_owned();
    register(&mut store, &root, other_nested);
    assert_eq!(second.agent.parent.as_deref(), Some(root.as_str()));
    assert_eq!(
        nested.agent.parent.as_deref(),
        Some(first.agent.id.as_str())
    );
    assert_eq!(nested.agent.root, root);
    assert_eq!(nested.agent.depth, 2);
    assert_eq!(store.list_agents(None, 100).unwrap().len(), 5);
    let mut moved = request(&second.agent.id, "second", "nested");
    moved.registration_id = "attempted-reparent".to_owned();
    assert_eq!(
        store
            .register_native_child(&root, 1, moved, NOW + 3)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert_eq!(
        store.get_agent(&nested.agent.id).unwrap().unwrap().parent,
        nested.agent.parent
    );
}

#[test]
fn registration_rejects_changed_binding_foreign_parent_and_stale_generation() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let other = producer(&mut store, "other");
    let request = request(&parent, "parent", "child");
    let child = register(&mut store, &parent, request.clone());
    let high_water = store.high_water().unwrap();
    let mut changed = request.clone();
    changed.native_ref = "another-child".to_owned();
    assert_eq!(
        store
            .register_native_child(&parent, 1, changed, NOW + 2)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    let mut wrong_parent = request.clone();
    wrong_parent.parent_native_ref = "unrelated-parent".to_owned();
    assert_eq!(
        store
            .register_native_child(&parent, 1, wrong_parent, NOW + 2)
            .unwrap_err()
            .code,
        "source_integrity"
    );
    assert_eq!(
        store
            .register_native_child(&other, 1, request.clone(), NOW + 2)
            .unwrap_err()
            .code,
        "source_integrity"
    );
    let mut stale = request.clone();
    stale.parent_generation = 2;
    assert_eq!(
        store
            .register_native_child(&parent, 1, stale, NOW + 2)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(
        store
            .register_native_child(&parent, 2, request, NOW + 2)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(store.high_water().unwrap(), high_water);
    assert_eq!(count(&store, "native_child_registrations"), 1);
    assert_eq!(
        store.get_agent(&child.agent.id).unwrap().unwrap(),
        child.agent
    );
}

#[test]
fn native_child_batch_uses_its_grant_without_widening_control_or_external_attach() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let other = producer(&mut store, "other");
    let child = register(&mut store, &parent, request(&parent, "parent", "child"));
    let native_batch = batch(
        &child.agent.id,
        "child-turn",
        vec![
            event(
                "session.ready",
                "child-ready",
                json!({"native_scope":SCOPE,"native_ref":"child"}),
            ),
            event("turn.started", "start", json!({"turn_id":"child-turn"})),
            event(
                "message.completed",
                "message",
                json!({"turn_id":"child-turn", "phase":"final_answer", "text":"child result"}),
            ),
            event(
                "turn.completed",
                "complete",
                json!({"turn_id":"child-turn", "status":"completed"}),
            ),
        ],
    );
    assert_eq!(
        store
            .put_batch(native_batch.clone(), NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        store
            .put_observed_batch(native_batch.clone(), (SCOPE, "child"), NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        store
            .put_native_child_batch(&other, 1, native_batch.clone(), NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let committed = store
        .put_native_child_batch(&parent, 1, native_batch.clone(), NOW + 2)
        .unwrap();
    assert_eq!(committed.events.len(), 4);
    assert!(
        committed
            .events
            .iter()
            .all(|event| event.agent.as_deref() == Some(child.agent.id.as_str()))
    );
    assert!(committed.events.iter().all(|event| event.cause.is_none()));
    let replay = store
        .put_native_child_batch(&parent, 1, native_batch, NOW + 3)
        .unwrap();
    assert_eq!(committed.ack, replay.ack);
    assert!(replay.events.is_empty());
    let turns = store.list_turns(&child.agent.id, 1, 100).unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].state, TurnState::Completed);
    assert_eq!(turns[0].final_text.as_deref(), Some("child result"));
    assert!(store.list_turns(&parent, 1, 100).unwrap().is_empty());
    assert_eq!(
        store.get_agent(&child.agent.id).unwrap().unwrap(),
        child.agent
    );
    assert_eq!(
        store.get_agent(&parent).unwrap().unwrap().state,
        AgentState::Idle
    );
}

#[test]
fn child_grant_rejects_control_claims_other_families_and_wrong_subjects() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let child = register(&mut store, &parent, request(&parent, "parent", "child"));
    let before = store.high_water().unwrap();
    for native in [
        event(
            "turn.started",
            "forged-command",
            json!({"turn_id":"t", "command_id":"forged"}),
        ),
        event(
            "request.opened",
            "request",
            json!({"request_id":"r", "turn_id":"t"}),
        ),
        event(
            "tool.started",
            "tool",
            json!({"turn_id":"t", "tool":"shell"}),
        ),
        event(
            "session.ready",
            "foreign-ready",
            json!({"native_scope":SCOPE,"native_ref":"parent"}),
        ),
    ] {
        assert!(
            store
                .put_native_child_batch(
                    &parent,
                    1,
                    batch(&child.agent.id, "refused", vec![native]),
                    NOW + 2
                )
                .is_err()
        );
    }
    let mut wrong_source = batch(
        &child.agent.id,
        "refused",
        vec![event("turn.started", "s", json!({"turn_id":"t"}))],
    );
    wrong_source.source = "transcript".to_owned();
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, wrong_source, NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let mut process_gap = gap(&child.agent.id);
    process_gap.families = vec!["process".to_owned()];
    assert_eq!(
        store
            .record_native_child_source_gap(&parent, 1, process_gap, NOW + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(store.high_water().unwrap(), before);
    assert!(
        store
            .list_turns(&child.agent.id, 1, 100)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn publication_grant_rechecks_the_immediate_parent_generation() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let parent = register(&mut store, &root, request(&root, "root", "parent"));
    let registration = request(&parent.agent.id, "parent", "nested");
    let child = register(&mut store, &root, registration.clone());
    store
        .connection
        .execute(
            "UPDATE agents SET generation = 2 WHERE id = ?1",
            [parse_agent_id(&parent.agent.id).unwrap()],
        )
        .unwrap();
    let high_water = store.high_water().unwrap();
    assert_eq!(
        store
            .register_native_child(&root, 1, registration, NOW + 3)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(
        store
            .read_native_child_source_state(&root, 1, &child.agent.id, 1, "stream")
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(
        store
            .record_native_child_source_gap(&root, 1, gap(&child.agent.id), NOW + 3)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(
        store
            .put_native_child_batch(
                &root,
                1,
                batch(
                    &child.agent.id,
                    "late",
                    vec![event("turn.started", "late", json!({"turn_id":"late"})),]
                ),
                NOW + 3
            )
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn parent_terminal_never_completes_child_and_retired_producer_cannot_publish() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let registration = request(&parent, "parent", "child");
    let child = register(&mut store, &parent, registration.clone());
    store
        .put_native_child_batch(
            &parent,
            1,
            batch(
                &child.agent.id,
                "running",
                vec![event(
                    "turn.started",
                    "child-start",
                    json!({"turn_id":"child-turn"}),
                )],
            ),
            NOW + 2,
        )
        .unwrap();
    store
        .put_batch(
            batch(
                &parent,
                "parent-ended",
                vec![event("session.ended", "parent-ended", json!({}))],
            ),
            NOW + 3,
        )
        .unwrap();
    assert_eq!(
        store.list_turns(&child.agent.id, 1, 100).unwrap()[0].state,
        TurnState::Running
    );
    assert_eq!(
        store.get_agent(&child.agent.id).unwrap().unwrap().state,
        AgentState::Unknown
    );
    assert!(
        store
            .register_native_child(&parent, 1, registration, NOW + 4)
            .is_err()
    );
    assert!(
        store
            .read_native_child_source_state(&parent, 1, &child.agent.id, 1, "stream")
            .is_err()
    );
    assert!(
        store
            .record_native_child_source_gap(&parent, 1, gap(&child.agent.id), NOW + 4)
            .is_err()
    );
    assert!(
        store
            .put_native_child_batch(
                &parent,
                1,
                batch(
                    &child.agent.id,
                    "late",
                    vec![event(
                        "turn.completed",
                        "unproven",
                        json!({"turn_id":"child-turn","status":"completed"})
                    ),]
                ),
                NOW + 4
            )
            .is_err()
    );
}

#[test]
fn registration_and_batch_failures_roll_back_their_whole_transaction() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let high_water = store.high_water().unwrap();
    let sources = count(&store, "source_state");
    let owners = count(&store, "source_owners");
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER reject_child BEFORE INSERT ON native_child_registrations
         BEGIN SELECT RAISE(ABORT, 'synthetic child persistence failure'); END;",
        )
        .unwrap();
    assert_eq!(
        store
            .register_native_child(&parent, 1, request(&parent, "parent", "child"), NOW + 1)
            .unwrap_err()
            .code,
        "internal"
    );
    assert!(store.is_write_faulted());
    assert_eq!(count(&store, "agents"), 1);
    assert_eq!(count(&store, "native_child_registrations"), 0);
    assert_eq!(count(&store, "source_state"), sources);
    assert_eq!(count(&store, "source_owners"), owners);
    assert_eq!(store.high_water().unwrap(), high_water);

    let mut store = self::store();
    let parent = producer(&mut store, "parent");
    let child = register(&mut store, &parent, request(&parent, "parent", "child"));
    let source = store
        .read_source_state(&child.agent.id, 1, "stream")
        .unwrap();
    let high_water = store.high_water().unwrap();
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER reject_child_ack BEFORE UPDATE ON source_state
         BEGIN SELECT RAISE(ABORT, 'synthetic child batch failure'); END;",
        )
        .unwrap();
    assert_eq!(
        store
            .put_native_child_batch(
                &parent,
                1,
                batch(
                    &child.agent.id,
                    "rejected",
                    vec![event("turn.started", "rolled-back", json!({"turn_id":"t"})),]
                ),
                NOW + 2
            )
            .unwrap_err()
            .code,
        "internal"
    );
    assert!(store.is_write_faulted());
    assert!(
        store
            .list_turns(&child.agent.id, 1, 100)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .read_source_state(&child.agent.id, 1, "stream")
            .unwrap(),
        source
    );
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn child_wire_rejects_undeclared_authority_and_ack_identity_changes() {
    let mut store = store();
    let parent = producer(&mut store, "parent");
    let registration = request(&parent, "parent", "child");
    let mut wire = serde_json::to_value(&registration).unwrap();
    wire["own_identity"] = json!(true);
    assert!(serde_json::from_value::<ChildRegistration>(wire).is_err());
    assert!(
        serde_json::from_value::<ChildRegistration>(json!([
            "registration",
            parent,
            1,
            "parent",
            SCOPE,
            "child"
        ]))
        .is_err()
    );
    let ack = register(&mut store, &parent, registration.clone());
    let mut forged = ack.clone();
    forged.agent.own_identity = true;
    assert!(registration.validate_ack(&forged).is_err());
    forged = ack.clone();
    forged
        .source_plan
        .insert("process".to_owned(), "os".to_owned());
    assert!(registration.validate_ack(&forged).is_err());
    let token = store.get_agent_token(&parent, 1).unwrap().unwrap();
    let mut secret = request(&parent, "parent", "secret-child");
    secret.registration_id = token;
    assert_eq!(
        store
            .register_native_child(&parent, 1, secret, NOW + 2)
            .unwrap_err()
            .code,
        "secret_in_payload"
    );
}

fn start_child_turn(store: &mut Store, producer: &str, child: &str, turn: &str) {
    store
        .put_native_child_batch(
            producer,
            1,
            batch(
                child,
                turn,
                vec![event(
                    "turn.started",
                    &format!("{turn}:start"),
                    json!({"turn_id":turn}),
                )],
            ),
            NOW + 2,
        )
        .unwrap();
}

#[test]
fn producer_loss_marks_nested_children_unknown_then_closes_without_another_gap() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let foreign = producer(&mut store, "foreign");
    let first = register(&mut store, &root, request(&root, "root", "first"));
    let nested = register(
        &mut store,
        &root,
        request(&first.agent.id, "first", "nested"),
    );
    let unrelated = register(
        &mut store,
        &foreign,
        request(&foreign, "foreign", "unrelated"),
    );
    store.put_native_child_batch(&root, 1, batch(&first.agent.id, "known-completion", vec![
        event("turn.started", "done-start", json!({"turn_id":"done"})),
        event("turn.completed", "done-end", json!({"turn_id":"done", "status":"completed", "final_text":"retained result"})),
    ]), NOW + 2).unwrap();
    let completed = store.get_turn(&first.agent.id, 1, "done").unwrap().unwrap();
    start_child_turn(&mut store, &root, &first.agent.id, "first-active");
    start_child_turn(&mut store, &root, &nested.agent.id, "nested-active");
    start_child_turn(&mut store, &foreign, &unrelated.agent.id, "foreign-active");
    // Loss must preserve the exact active binding when metadata has one.
    store
        .connection
        .execute(
            "UPDATE agents SET active_turn = 'first-active' WHERE id = ?1",
            [parse_agent_id(&first.agent.id).unwrap()],
        )
        .unwrap();
    let first_metadata = store.get_agent(&first.agent.id).unwrap().unwrap();
    store
        .put_batch(
            batch(
                &root,
                "root-ended",
                vec![event("session.ended", "root-ended", json!({}))],
            ),
            NOW + 3,
        )
        .unwrap();

    let lost = store
        .mark_native_children_unavailable(&root, 1, "reader_lost", false, NOW + 4)
        .unwrap();
    assert_eq!(
        lost.iter()
            .filter(|event| event.kind == "source.gap")
            .count(),
        2
    );
    assert_eq!(
        lost.iter()
            .filter(|event| event.kind == "turn.unknown")
            .count(),
        2
    );
    assert!(
        !lost
            .iter()
            .any(|event| event.kind == "turn.completed" || event.kind == "source.closed")
    );
    let first_source = store
        .read_source_state(&first.agent.id, 1, "stream")
        .unwrap();
    for (child, turn) in [
        (&first.agent.id, "first-active"),
        (&nested.agent.id, "nested-active"),
    ] {
        let source = store.read_source_state(child, 1, "stream").unwrap();
        assert_eq!(source["health"], "stale");
        assert_eq!(source["state"], "open");
        let turn = store.get_turn(child, 1, turn).unwrap().unwrap();
        assert_eq!(turn.state, TurnState::Unknown);
        assert_eq!(turn.completed_at, None);
        assert_eq!(turn.started_at, None);
    }
    assert_eq!(
        store.get_turn(&first.agent.id, 1, "done").unwrap().unwrap(),
        completed
    );
    assert_eq!(
        store.get_agent(&first.agent.id).unwrap().unwrap(),
        first_metadata
    );
    assert_eq!(
        store
            .read_source_state(&unrelated.agent.id, 1, "stream")
            .unwrap()["health"],
        "ok"
    );
    assert_eq!(
        store
            .get_turn(&unrelated.agent.id, 1, "foreign-active")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Running
    );

    let closed = store
        .mark_native_children_unavailable(&root, 1, "reader_drained", true, NOW + 5)
        .unwrap();
    assert_eq!(closed.len(), 2);
    assert!(closed.iter().all(|event| event.kind == "source.closed"));
    let after = store
        .read_source_state(&first.agent.id, 1, "stream")
        .unwrap();
    assert_eq!(after["state"], "closed");
    assert_eq!(after["health"], "stale");
    for field in ["gap_id", "gap_digest", "gap_seq", "gap_from", "gap_to"] {
        assert_eq!(after[field], first_source[field], "retained {field}");
    }
    let high_water = store.high_water().unwrap();
    assert!(
        store
            .mark_native_children_unavailable(&root, 1, "reader_drained", true, NOW + 6)
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn old_producer_cleanup_does_not_touch_new_generation_children_or_active_binding() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let old = register(&mut store, &root, request(&root, "root", "old"));
    start_child_turn(&mut store, &root, &old.agent.id, "old-turn");
    // Model a replacement already installed before its retiring actor reports
    // closure. Only the old producer tuple may be affected by that callback.
    let root_num = parse_agent_id(&root).unwrap();
    let tx = store.connection.transaction().unwrap();
    tx.execute(
        "UPDATE agents SET generation = 2, native_ref = 'root-resumed' WHERE id = ?1",
        [root_num],
    )
    .unwrap();
    install_initial_source_plan(&tx, root_num, 2, "codex", NOW + 3).unwrap();
    tx.commit().unwrap();
    let mut registration = request(&root, "root-resumed", "new");
    registration.parent_generation = 2;
    let new = store
        .register_native_child(&root, 2, registration, NOW + 3)
        .unwrap()
        .value;
    let current = store.get_agent(&root).unwrap().unwrap();
    let new_source = store.read_source_state(&new.agent.id, 1, "stream").unwrap();
    let events = store
        .mark_native_children_unavailable(&root, 1, "retired_reader_closed", true, NOW + 4)
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.agent.as_deref() == Some(old.agent.id.as_str()))
    );
    assert_eq!(
        store.read_source_state(&old.agent.id, 1, "stream").unwrap()["state"],
        "closed"
    );
    assert_eq!(
        store
            .get_turn(&old.agent.id, 1, "old-turn")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Unknown
    );
    assert_eq!(
        store.read_source_state(&new.agent.id, 1, "stream").unwrap(),
        new_source
    );
    assert_eq!(store.get_agent(&root).unwrap().unwrap(), current);
    assert_eq!(
        store
            .mark_native_children_unavailable(&root, 3, "unbound_future_reader", true, NOW + 5)
            .unwrap_err()
            .code,
        "stale_generation"
    );
}

#[test]
fn unavailable_children_roll_back_gap_turns_and_health_together() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let first = register(&mut store, &root, request(&root, "root", "first"));
    let second = register(&mut store, &root, request(&root, "root", "second"));
    start_child_turn(&mut store, &root, &first.agent.id, "first-active");
    start_child_turn(&mut store, &root, &second.agent.id, "second-active");
    let before_first = store
        .read_source_state(&first.agent.id, 1, "stream")
        .unwrap();
    let before_second = store
        .read_source_state(&second.agent.id, 1, "stream")
        .unwrap();
    let high_water = store.high_water().unwrap();
    store
        .connection
        .execute_batch(&format!(
            "CREATE TRIGGER reject_child_unavailable BEFORE INSERT ON events
         WHEN NEW.kind = 'source.health' AND NEW.agent = {}
         BEGIN SELECT RAISE(ABORT, 'synthetic child unavailable failure'); END;",
            parse_agent_id(&second.agent.id).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        store
            .mark_native_children_unavailable(&root, 1, "reader_lost", true, NOW + 4)
            .unwrap_err()
            .code,
        "internal"
    );
    assert!(store.is_write_faulted());
    assert_eq!(
        store
            .read_source_state(&first.agent.id, 1, "stream")
            .unwrap(),
        before_first
    );
    assert_eq!(
        store
            .read_source_state(&second.agent.id, 1, "stream")
            .unwrap(),
        before_second
    );
    for (child, turn) in [
        (&first.agent.id, "first-active"),
        (&second.agent.id, "second-active"),
    ] {
        assert_eq!(
            store.get_turn(child, 1, turn).unwrap().unwrap().state,
            TurnState::Running
        );
    }
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn parent_turn_completion_does_not_retire_child_observation() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let child = register(&mut store, &root, request(&root, "root", "child"));
    start_child_turn(&mut store, &root, &child.agent.id, "child-active");
    let mut send = SubmitCommand::new(Ulid::from_parts(NOW as u64, 100).to_string(), "send");
    send.target = Some(root.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text":"parent task"});
    let command_id = send.id.clone();
    store.submit("owner", send, NOW + 3).unwrap();
    store.dispatch(&command_id, NOW + 3).unwrap();
    store
        .put_batch(
            batch(
                &root,
                "parent-turn-start",
                vec![event(
                    "turn.started",
                    "parent-turn-start",
                    json!({"turn_id":"parent-turn", "command_id": &command_id}),
                )],
            ),
            NOW + 3,
        )
        .unwrap();
    store
        .put_batch(
            batch(
                &root,
                "parent-turn-complete",
                vec![event(
                    "turn.completed",
                    "parent-turn-complete",
                    json!({"turn_id":"parent-turn", "command_id": command_id, "status":"completed"}),
                )],
            ),
            NOW + 4,
        )
        .unwrap();
    assert_eq!(
        store
            .get_turn(&root, 1, "parent-turn")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Completed
    );
    assert_eq!(
        store
            .get_turn(&child.agent.id, 1, "child-active")
            .unwrap()
            .unwrap()
            .state,
        TurnState::Running
    );
    let source = store
        .read_source_state(&child.agent.id, 1, "stream")
        .unwrap();
    assert_eq!(source["health"], "ok");
    assert_eq!(source["state"], "open");
    assert_eq!(source["gap_id"], Value::Null);
}
