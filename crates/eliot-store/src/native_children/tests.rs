use super::*;
use eliot_core::api::HierarchyLimits;

const NOW: i64 = 1_800_000_000_000;
const SCOPE: &str = "codex:test-native-store";

#[test]
fn activity_projection_preserves_control_state_and_prefers_open_turns() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let child = register(&mut store, &root, request(&root, "root", "child"));
    assert_eq!(
        child.agent.activity.as_ref().unwrap().state,
        NativeChildActivityState::Unknown
    );
    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &child.agent.id,
                "activity-start",
                vec![event(
                    "turn.started",
                    "activity-start",
                    json!({"turn_id":"one"}),
                )],
            ),
            NOW + 2,
        )
        .unwrap();
    let running = store.get_agent(&child.agent.id).unwrap().unwrap();
    assert_eq!(running.state, AgentState::Unknown);
    assert!(!running.own_identity);
    assert_eq!(
        running.activity.as_ref().unwrap().state,
        NativeChildActivityState::Running
    );
    assert_eq!(
        running.activity.as_ref().unwrap().last_progress_at,
        Some(NOW + 2)
    );
    let summary = attention_summary(&store, &root, 1, NOW + 1002, 1000);
    assert_eq!(
        (summary.unfinished, summary.stalled, summary.lost),
        (1, 1, 0)
    );
    assert!(
        store
            .list_attention_agents(None, 100)
            .unwrap()
            .iter()
            .any(|agent| agent.id == root)
    );
    assert_eq!(
        attention_summary(&store, &root, 2, NOW + 1002, 1000).unfinished,
        0
    );

    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &child.agent.id,
                "activity-terminal",
                vec![event(
                    "turn.completed",
                    "activity-interrupted",
                    json!({"turn_id":"one", "status":"interrupted"}),
                )],
            ),
            NOW + 3,
        )
        .unwrap();
    assert_eq!(
        store
            .get_agent(&child.agent.id)
            .unwrap()
            .unwrap()
            .activity
            .unwrap()
            .state,
        NativeChildActivityState::Interrupted
    );
    assert_eq!(
        attention_summary(&store, &root, 1, NOW + 1002, 1000).unfinished,
        0
    );
    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &child.agent.id,
                "activity-second",
                vec![event(
                    "turn.started",
                    "activity-second",
                    json!({"turn_id":"two"}),
                )],
            ),
            NOW + 4,
        )
        .unwrap();
    assert_eq!(
        store
            .get_agent(&child.agent.id)
            .unwrap()
            .unwrap()
            .activity
            .unwrap()
            .state,
        NativeChildActivityState::Running
    );
    store
        .mark_native_children_unavailable(&root, 1, "reader_lost", true, NOW + 5)
        .unwrap();
    let lost = store.get_agent(&child.agent.id).unwrap().unwrap();
    assert_eq!(lost.state, AgentState::Unknown);
    assert!(lost.activity.as_ref().unwrap().has_open_turn);
    assert_eq!(lost.activity.unwrap().state, NativeChildActivityState::Lost);
    assert_eq!(
        attention_summary(&store, &root, 1, NOW + 1002, 1000).lost,
        1
    );
}

#[test]
fn activity_uses_exact_parent_sources_and_aggregates_nested_children() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let child = register(&mut store, &root, request(&root, "root", "child"));
    let nested = register(
        &mut store,
        &root,
        request(&child.agent.id, "child", "nested"),
    );
    for (agent, turn) in [
        (&child.agent.id, "child-turn"),
        (&nested.agent.id, "nested-turn"),
    ] {
        start_child_turn(&mut store, &root, agent, turn);
    }
    // A split message owner can lose coverage independently of the turn owner.
    store
        .connection
        .execute(
            "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
         VALUES (?1, 1, 'message-tail', 'closed', 'stale', ?2)",
            params![parse_agent_id(&root).unwrap(), NOW + 3],
        )
        .unwrap();
    store.connection.execute(
        "UPDATE source_owners SET source = 'message-tail' WHERE agent = ?1 AND generation = 1 AND family = 'message'",
        [parse_agent_id(&root).unwrap()],
    ).unwrap();
    assert_eq!(
        store
            .get_agent(&child.agent.id)
            .unwrap()
            .unwrap()
            .activity
            .unwrap()
            .state,
        NativeChildActivityState::Lost
    );
    store.connection.execute(
        "UPDATE source_owners SET source = 'stream' WHERE agent = ?1 AND generation = 1 AND family = 'message'",
        [parse_agent_id(&root).unwrap()],
    ).unwrap();
    store
        .record_native_child_source_gap(&root, 1, gap(&child.agent.id), NOW + 3)
        .unwrap();
    let activity = store
        .get_agent(&nested.agent.id)
        .unwrap()
        .unwrap()
        .activity
        .unwrap();
    assert_eq!(activity.state, NativeChildActivityState::Lost);
    let summary = attention_summary(&store, &root, 1, NOW + 4, 1000);
    assert_eq!((summary.unfinished, summary.lost), (2, 2));
    assert_eq!(
        attention_summary(&store, &child.agent.id, 1, NOW + 4, 1000).unfinished,
        0
    );
}

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn attention_summary(
    store: &Store,
    manager: &str,
    generation: i64,
    now: i64,
    suspicion_ms: i64,
) -> NativeChildrenAttention {
    let mut summary = NativeChildrenAttention::default();
    let Some(through) = store
        .native_children_attention_end(manager, generation)
        .unwrap()
    else {
        return summary;
    };
    let mut after = None;
    loop {
        let page = store
            .native_children_attention_page(
                manager,
                generation,
                after.as_deref(),
                &through,
                now,
                suspicion_ms,
            )
            .unwrap();
        summary.append(page.summary);
        match page.next {
            Some(next) => after = Some(next),
            None => return summary,
        }
    }
}

#[test]
fn activity_pages_advance_over_finished_rows_and_freeze_the_registration_range() {
    let mut store = store();
    let root = producer(&mut store, "root");
    for index in 0..NATIVE_CHILD_ATTENTION_PAGE_SIZE {
        let child = register(
            &mut store,
            &root,
            request(&root, "root", &format!("done-{index}")),
        );
        store
            .put_native_child_batch(
                &root,
                1,
                batch(
                    &child.agent.id,
                    &format!("done-{index}"),
                    vec![event(
                        "turn.completed",
                        &format!("done-{index}"),
                        json!({"turn_id":"done", "status":"completed"}),
                    )],
                ),
                NOW + 2,
            )
            .unwrap();
    }
    let active = register(&mut store, &root, request(&root, "root", "active"));
    start_child_turn(&mut store, &root, &active.agent.id, "active");
    let through = store
        .native_children_attention_end(&root, 1)
        .unwrap()
        .unwrap();
    let first = store
        .native_children_attention_page(&root, 1, None, &through, NOW + 1002, 1000)
        .unwrap();
    assert_eq!(first.summary.unfinished, 0);
    assert!(first.next.is_some());
    let late = register(&mut store, &root, request(&root, "root", "late"));
    start_child_turn(&mut store, &root, &late.agent.id, "late");
    let last = store
        .native_children_attention_page(&root, 1, first.next.as_deref(), &through, NOW + 1002, 1000)
        .unwrap();
    assert!(last.next.is_none());
    assert_eq!(last.summary.children[0].0, active.agent.id);
    assert_eq!(last.summary.unfinished, 1);
    assert_eq!(
        attention_summary(&store, &root, 1, NOW + 1002, 1000).unfinished,
        2
    );
}

#[test]
fn activity_query_plans_seek_indexes_instead_of_retained_history() {
    let store = store();
    // These are the production query strings, planned by bundled rusqlite.
    for (sql, args, expected) in [
        (OPEN_TURN_SQL, vec![1i64, 1], "turns_native_activity"),
        (
            NATIVE_PROGRESS_SQL,
            vec![1, 1],
            "events_native_progress_time",
        ),
        (TERMINAL_ACTIVITY_SQL, vec![1, 1], "turns_native_activity"),
        (GIT_PROGRESS_SQL, vec![1, 1], "events_git_progress_time"),
        (
            CHILDREN_ATTENTION_PAGE_SQL,
            vec![1, 1, 0, 1000, NATIVE_CHILD_ATTENTION_PAGE_SIZE as i64],
            "native_children_producer",
        ),
    ] {
        let mut statement = store
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        let details = statement
            .query_map(rusqlite::params_from_iter(args), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(details.contains(expected), "{details}");
        assert!(
            !details.contains("SCAN ") && !details.contains("USE TEMP B-TREE"),
            "{details}"
        );
        println!("{expected}: {details}");
    }
}

#[test]
fn activity_open_turn_lookup_stops_with_many_unresolved_turns() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let child = register(&mut store, &root, request(&root, "root", "many-turns"));
    let child_num = parse_agent_id(&child.agent.id).unwrap();
    store.connection.execute(
        "WITH RECURSIVE n(value) AS (VALUES(1) UNION ALL SELECT value + 1 FROM n WHERE value < 10000)
         INSERT INTO turns(agent, generation, turn_id, state)
         SELECT ?1, 1, printf('turn-%d', value), 'unknown' FROM n", [child_num],
    ).unwrap();
    let mut statement = store.connection.prepare(OPEN_TURN_SQL).unwrap();
    assert!(
        statement
            .query_row(params![child_num, 1], |_| Ok(()))
            .optional()
            .unwrap()
            .is_some()
    );
    let steps = statement.get_status(rusqlite::StatementStatus::VmStep);
    assert!(
        steps < 100,
        "open-turn existence traversed unresolved rows: {steps} VM steps"
    );
    println!("open-turn presence with 10000 unresolved turns: {steps} VM steps");
    assert!(
        store
            .get_agent(&child.agent.id)
            .unwrap()
            .unwrap()
            .activity
            .unwrap()
            .has_open_turn
    );
}

#[test]
fn activity_revision_is_transactional_scoped_and_survives_progress_pruning() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let foreign = producer(&mut store, "foreign");
    let before = store.native_children_revision(&root, 1).unwrap().unwrap();
    let binding = request(&root, "root", "child");
    let child = register(&mut store, &root, binding.clone());
    let registered = store.native_children_revision(&root, 1).unwrap().unwrap();
    assert!(registered > before);
    register(&mut store, &root, binding);
    assert_eq!(
        store.native_children_revision(&root, 1).unwrap(),
        Some(registered)
    );
    start_child_turn(&mut store, &root, &child.agent.id, "open");
    let progressed = store.native_children_revision(&root, 1).unwrap().unwrap();
    assert!(progressed > registered);
    assert_eq!(
        store.native_children_revision(&foreign, 1).unwrap(),
        Some(0)
    );
    let tx = store.connection.transaction().unwrap();
    insert_daemon_event(
        &tx,
        NOW + 3,
        "turn.completed",
        "turn",
        Some(parse_agent_id(&child.agent.id).unwrap()),
        Some(1),
        json!({"turn_id":"open"}),
    )
    .unwrap();
    tx.rollback().unwrap();
    assert_eq!(
        store.native_children_revision(&root, 1).unwrap(),
        Some(progressed)
    );
    let tx = store.connection.transaction().unwrap();
    for (kind, data) in [
        ("x.user.noise", json!({})),
        ("source.health", json!({"source":"os"})),
        ("source.health", json!({"source":"git"})),
    ] {
        insert_daemon_event(
            &tx,
            NOW + 3,
            kind,
            "source",
            Some(parse_agent_id(&root).unwrap()),
            Some(1),
            data,
        )
        .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        store.native_children_revision(&root, 1).unwrap(),
        Some(progressed)
    );
    let tx = store.connection.transaction().unwrap();
    insert_daemon_event(
        &tx,
        NOW + 4,
        "source.health",
        "source",
        Some(parse_agent_id(&root).unwrap()),
        Some(1),
        json!({"source":"stream"}),
    )
    .unwrap();
    tx.commit().unwrap();
    let source_changed = store.native_children_revision(&root, 1).unwrap().unwrap();
    assert!(source_changed > progressed);
    store
        .put_batch(
            batch(
                &root,
                "parent-ended",
                vec![event("session.ended", "parent-ended", json!({}))],
            ),
            NOW + 5,
        )
        .unwrap();
    let parent_ended = store.native_children_revision(&root, 1).unwrap().unwrap();
    assert!(parent_ended > source_changed);
    store
        .connection
        .execute(
            "DELETE FROM events WHERE agent = ?1 AND kind = 'turn.started'",
            [parse_agent_id(&child.agent.id).unwrap()],
        )
        .unwrap();
    assert!(store.native_children_revision(&root, 1).unwrap().unwrap() > parent_ended);
    assert!(
        store
            .get_agent(&child.agent.id)
            .unwrap()
            .unwrap()
            .activity
            .unwrap()
            .has_open_turn
    );
}

#[test]
fn activity_progress_keeps_timestamp_order_and_native_git_boundaries() {
    let mut store = store();
    let root = producer(&mut store, "root");
    let agent = parse_agent_id(&root).unwrap();
    for (offset, generation, source, kind, native_id) in [
        (3, 1, "stream", "message.completed", Some("newer-time")),
        (2, 1, "stream", "turn.started", Some("later-sequence")),
        (4, 1, "git", "file.changed", None),
        (10, 1, "os", "tool.completed", Some("cpu-noise")),
        (11, 1, "stream", "message.completed", None),
        (
            12,
            2,
            "stream",
            "message.completed",
            Some("other-generation"),
        ),
    ] {
        store
            .connection
            .execute(
                "INSERT INTO events(ts, kind, agent, generation, source, native_id, data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, '{}')",
                params![NOW + offset, kind, agent, generation, source, native_id],
            )
            .unwrap();
    }
    assert_eq!(
        store.native_model_progress(&root, 1).unwrap(),
        Some(NOW + 3)
    );
    assert_eq!(store.last_model_progress(&root, 1).unwrap(), Some(NOW + 4));
    assert_eq!(
        store.native_model_progress(&root, 2).unwrap(),
        Some(NOW + 12)
    );
    assert_eq!(store.last_model_progress("a999", 1).unwrap(), None);
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
    // Match the current host handshake before publishing native facts.
    let plan = ["session", "turn", "message", "tool", "request"]
        .into_iter()
        .map(|family| (family.to_owned(), INITIAL_SOURCE.to_owned()))
        .collect();
    store
        .install_plugin_sources(&id, 1, &command, &plan, NOW)
        .unwrap();
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
    request
        .validate_ack(&change.value, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
        .unwrap();
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
    let current = store.get_agent(&child.agent.id).unwrap().unwrap();
    assert_eq!(
        current.activity.as_ref().unwrap().state,
        NativeChildActivityState::Idle
    );
    let mut expected = child.agent;
    expected.activity = current.activity.clone();
    assert_eq!(current, expected);
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
    assert!(
        registration
            .validate_ack(&forged, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
            .is_err()
    );
    forged = ack.clone();
    forged
        .source_plan
        .insert("process".to_owned(), "os".to_owned());
    assert!(
        registration
            .validate_ack(&forged, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
            .is_err()
    );
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
    let current = store.get_agent(&first.agent.id).unwrap().unwrap();
    assert_eq!(
        current.activity.as_ref().unwrap().state,
        NativeChildActivityState::Lost
    );
    let mut expected = first_metadata;
    expected.activity = current.activity.clone();
    assert_eq!(current, expected);
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
    install_initial_source_plan(&tx, root_num, 2, NOW + 3).unwrap();
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

fn seed_restart_recovery_children(path: &Path) -> (String, String, String, String) {
    let mut store = Store::open(path).unwrap();
    let root = producer(&mut store, "root");
    let running = register(&mut store, &root, request(&root, "root", "running"));
    let partial = register(&mut store, &root, request(&root, "root", "partial"));
    let completed = register(&mut store, &root, request(&root, "root", "completed"));

    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &running.agent.id,
                "running-turn",
                vec![event(
                    "turn.started",
                    "running-start",
                    json!({"turn_id":"running-turn", "started_at":NOW + 10}),
                )],
            ),
            NOW + 10,
        )
        .unwrap();
    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &partial.agent.id,
                "partial-turn",
                vec![
                    event(
                        "turn.started",
                        "partial-start",
                        json!({"turn_id":"partial-turn", "started_at":NOW + 11}),
                    ),
                    event(
                        "message.completed",
                        "partial-message",
                        json!({"turn_id":"partial-turn", "phase":"final_answer", "text":"partial answer"}),
                    ),
                ],
            ),
            NOW + 11,
        )
        .unwrap();
    store
        .put_native_child_batch(
            &root,
            1,
            batch(
                &completed.agent.id,
                "completed-turn",
                vec![
                    event(
                        "turn.started",
                        "completed-start",
                        json!({"turn_id":"completed-turn", "started_at":NOW + 12}),
                    ),
                    event(
                        "turn.completed",
                        "completed-end",
                        json!({"turn_id":"completed-turn", "status":"completed", "started_at":NOW + 12, "completed_at":NOW + 13, "final_text":"finished answer"}),
                    ),
                ],
            ),
            NOW + 13,
        )
        .unwrap();

    (root, running.agent.id, partial.agent.id, completed.agent.id)
}

fn recover_and_assert_child_turns(
    store: &mut Store,
    running_id: &str,
    partial_id: &str,
    completed_id: &str,
) -> Turn {
    let running_before = store
        .get_turn(running_id, 1, "running-turn")
        .unwrap()
        .unwrap();
    let partial_before = store
        .get_turn(partial_id, 1, "partial-turn")
        .unwrap()
        .unwrap();
    let completed_before = store
        .get_turn(completed_id, 1, "completed-turn")
        .unwrap()
        .unwrap();
    assert_eq!(running_before.state, TurnState::Running);
    assert_eq!(running_before.started_at, Some(NOW + 10));
    assert_eq!(partial_before.state, TurnState::Running);
    assert_eq!(partial_before.started_at, Some(NOW + 11));
    assert_eq!(partial_before.final_text.as_deref(), Some("partial answer"));
    assert_eq!(completed_before.state, TurnState::Completed);
    assert_eq!(completed_before.started_at, Some(NOW + 12));
    assert_eq!(completed_before.completed_at, Some(NOW + 13));

    let recovered = store.recover_on_restart(NOW + 50).unwrap();
    assert_eq!(
        recovered
            .iter()
            .filter(|event| event.kind == "turn.unknown")
            .count(),
        2
    );
    assert!(recovered.iter().all(|event| event.kind != "turn.completed"));
    let running_after = store
        .get_turn(running_id, 1, "running-turn")
        .unwrap()
        .unwrap();
    let partial_after = store
        .get_turn(partial_id, 1, "partial-turn")
        .unwrap()
        .unwrap();
    assert_eq!(running_after.state, TurnState::Unknown);
    assert_eq!(running_after.started_at, running_before.started_at);
    assert_eq!(running_after.completed_at, running_before.completed_at);
    assert_eq!(partial_after.state, TurnState::Unknown);
    assert_eq!(partial_after.started_at, partial_before.started_at);
    assert_eq!(partial_after.completed_at, partial_before.completed_at);
    assert_eq!(partial_after.final_text, partial_before.final_text);
    assert_eq!(
        store
            .get_turn(completed_id, 1, "completed-turn")
            .unwrap()
            .unwrap(),
        completed_before
    );
    completed_before
}

fn assert_restart_retained_generation(store: &Store, root: &str) {
    let retained = store.restart_retained_generations(None).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        (retained[0].agent.as_str(), retained[0].generation),
        (root, 1)
    );
    assert!(retained[0].named_custody);
    assert!(retained[0].processes.is_empty());
}

fn release_restart_custody(store: &mut Store, root: &str) {
    // This process-free SQLite fixture simulates the caller's explicit closure
    // proof; it does not qualify real OS custody.
    let released = store
        .release_restart_closed_generation(root, 1, "windows_job_closed", false, NOW + 51)
        .unwrap();
    assert!(
        released
            .iter()
            .any(|event| event.kind == "capacity.released")
    );
    assert!(store.restart_retained_generations(None).unwrap().is_empty());
}

fn assert_recovery_is_idempotent(store: &mut Store) {
    let high_water = store.high_water().unwrap();
    assert!(store.recover_on_restart(NOW + 51).unwrap().is_empty());
    assert_eq!(store.high_water().unwrap(), high_water);
}

fn resume_recovered_producer(store: &mut Store, root: &str) {
    let mut resume = SubmitCommand::new(
        Ulid::from_parts((NOW + 52) as u64, 900).to_string(),
        "resume",
    );
    resume.target = Some(root.to_owned());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store.submit("owner", resume, NOW + 52).unwrap();
    store.dispatch(&resume_id, NOW + 53).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: "root-resume-ready".to_owned(),
                agent_id: root.to_owned(),
                generation: 2,
                source: INITIAL_SOURCE.to_owned(),
                cursor: Some("root-resume-ready".to_owned()),
                events: vec![event(
                    "session.ready",
                    "root-resume-ready",
                    json!({"command_id":resume_id, "native_scope":SCOPE, "native_ref":"root"}),
                )],
            },
            NOW + 54,
        )
        .unwrap();
    store
        .settle(&resume_id, CommandState::Applied, None, NOW + 54)
        .unwrap();

    let resumed = store.get_agent(root).unwrap().unwrap();
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.state, AgentState::Idle);
    assert_eq!(resumed.native_scope.as_deref(), Some(SCOPE));
    assert_eq!(resumed.native_ref.as_deref(), Some("root"));
    let source = store.read_source_state(root, 2, INITIAL_SOURCE).unwrap();
    assert_eq!(source["state"], "open");
    assert_eq!(source["health"], "ok");
}

fn assert_stale_cleanup_preserves_fresh_child(
    store: &mut Store,
    root: &str,
    old_children: [&str; 3],
    completed_before: &Turn,
) {
    let mut fresh_request = request(root, "root", "fresh");
    fresh_request.parent_generation = 2;
    let fresh = store
        .register_native_child(root, 2, fresh_request, NOW + 56)
        .unwrap()
        .value;
    store
        .put_native_child_batch(
            root,
            2,
            batch(
                &fresh.agent.id,
                "fresh-active",
                vec![event(
                    "turn.started",
                    "fresh-active-start",
                    json!({"turn_id":"fresh-active", "started_at":NOW + 54}),
                )],
            ),
            NOW + 57,
        )
        .unwrap();
    let fresh_source_before = store
        .read_source_state(&fresh.agent.id, 1, "stream")
        .unwrap();
    let fresh_turn_before = store
        .get_turn(&fresh.agent.id, 1, "fresh-active")
        .unwrap()
        .unwrap();

    let stale_cleanup = store
        .mark_native_children_unavailable(root, 1, "retired_reader_closed", true, NOW + 58)
        .unwrap();
    assert!(!stale_cleanup.is_empty());
    assert!(stale_cleanup.iter().all(|event| {
        event
            .agent
            .as_deref()
            .is_some_and(|agent| old_children.contains(&agent))
    }));
    assert_eq!(
        store
            .read_source_state(&fresh.agent.id, 1, "stream")
            .unwrap(),
        fresh_source_before
    );
    assert_eq!(
        store
            .get_turn(&fresh.agent.id, 1, "fresh-active")
            .unwrap()
            .unwrap(),
        fresh_turn_before
    );
    assert_eq!(
        store
            .get_turn(old_children[2], 1, "completed-turn")
            .unwrap()
            .unwrap(),
        *completed_before
    );
}

#[test]
fn restart_recovery_preserves_child_turns_after_disk_reopen_and_scopes_stale_cleanup() {
    let path = std::env::temp_dir().join(format!("eliot-native-child-recovery-{}", Ulid::new()));
    let (root, running_id, partial_id, completed_id) = seed_restart_recovery_children(&path);
    let mut store = Store::open(&path).unwrap();
    let completed_before =
        recover_and_assert_child_turns(&mut store, &running_id, &partial_id, &completed_id);
    assert_restart_retained_generation(&store, &root);
    release_restart_custody(&mut store, &root);
    assert_recovery_is_idempotent(&mut store);
    resume_recovered_producer(&mut store, &root);
    assert_stale_cleanup_preserves_fresh_child(
        &mut store,
        &root,
        [&running_id, &partial_id, &completed_id],
        &completed_before,
    );
    drop(store);
    std::fs::remove_file(path).unwrap();
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

const MUSE_SCOPE: &str = "muse:test-native-store";

fn muse_producer(store: &mut Store, native_ref: &str) -> String {
    let command_id = Ulid::from_parts(NOW as u64, (count(store, "agents") + 1) as u128);
    let mut launch = SubmitCommand::new(command_id.to_string(), "launch");
    launch.expected_generation = Some(0);
    launch.args = json!({
        "harness": "muse",
        "profile": "muse-test",
        "profile_snapshot": {},
        "project": "native-child-muse-test",
        "workspace": "synthetic-workspace"
    });
    let command = launch.id.clone();
    let id = store
        .submit("owner", launch, NOW)
        .unwrap()
        .command
        .target
        .unwrap();
    store.dispatch(&command, NOW).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{native_ref}:ready"),
                agent_id: id.clone(),
                generation: 1,
                source: INITIAL_SOURCE.to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    kind: "session.ready".to_owned(),
                    native_id: format!("{native_ref}:ready"),
                    ts: None,
                    data: json!({"command_id": command, "native_scope": MUSE_SCOPE, "native_ref": native_ref}),
                }],
            },
            NOW,
        )
        .unwrap();
    store
        .settle(&command, CommandState::Applied, None, NOW)
        .unwrap();
    id
}

fn muse_child_native_ref(session: &str, run_id: &str, child_id: &str, attempt: u32) -> String {
    format!("muse:child:{}", json!([session, run_id, child_id, attempt]))
}

fn muse_child_registration_id(session: &str, run_id: &str, child_id: &str, attempt: u32) -> String {
    format!("muse:spawn:{}", json!([session, run_id, child_id, attempt]))
}

fn muse_child_turn_id(session: &str, run_id: &str, child_id: &str, attempt: u32) -> String {
    format!("muse:turn:{}", json!([session, run_id, child_id, attempt]))
}

fn muse_child_request(
    parent: &str,
    parent_ref: &str,
    run_id: &str,
    child_id: &str,
    attempt: u32,
) -> ChildRegistration {
    ChildRegistration {
        registration_id: muse_child_registration_id(parent_ref, run_id, child_id, attempt),
        parent_id: parent.to_owned(),
        parent_generation: 1,
        parent_native_ref: parent_ref.to_owned(),
        native_scope: MUSE_SCOPE.to_owned(),
        native_ref: muse_child_native_ref(parent_ref, run_id, child_id, attempt),
    }
}

#[test]
fn muse_child_registration_derives_muse_harness_and_turn_only_coverage() {
    let mut store = store();
    let parent = muse_producer(&mut store, "muse-session-1");
    let request = muse_child_request(&parent, "muse-session-1", "wf-run-1", "child-step-1", 1);
    let expected_native_ref =
        muse_child_native_ref("muse-session-1", "wf-run-1", "child-step-1", 1);
    let change = store
        .register_native_child(&parent, 1, request.clone(), NOW + 1)
        .unwrap();
    request
        .validate_ack(&change.value, "muse", &MUSE_NATIVE_CHILD_FAMILIES)
        .unwrap();

    let child = &change.value.agent;
    assert_eq!(child.harness, "muse");
    assert_eq!(child.generation, 1);
    assert_eq!(child.depth, 1);
    assert_eq!(child.origin, core::AgentOrigin::Native);
    assert!(child.observe_only);
    assert!(!child.own_identity);
    assert_eq!(child.state, AgentState::Unknown);
    assert_eq!(child.parent.as_deref(), Some(parent.as_str()));
    assert_eq!(child.root, parent);
    assert_eq!(child.native_scope.as_deref(), Some(MUSE_SCOPE));
    assert_eq!(
        child.native_ref.as_deref(),
        Some(expected_native_ref.as_str())
    );
    assert_eq!(store.get_agent_token(&child.id, 1).unwrap(), None);

    let sources = store.list_source_states(&child.id, 1).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["families"], json!(["turn"]));
    assert_eq!(sources[0]["source"], "stream");
    assert_eq!(sources[0]["state"], "open");

    assert_eq!(
        change.value.source_plan,
        BTreeMap::from([("turn".to_owned(), "stream".to_owned())])
    );

    assert_eq!(
        request
            .validate_ack(&change.value, "codex", &CODEX_NATIVE_CHILD_FAMILIES)
            .unwrap_err()
            .code,
        "source_integrity"
    );

    assert_eq!(
        request
            .validate_ack(&change.value, "muse", &["turn", "session"])
            .unwrap_err()
            .code,
        "source_integrity"
    );
    assert_eq!(
        request
            .validate_ack(&change.value, "muse", &["turn", "message"])
            .unwrap_err()
            .code,
        "source_integrity"
    );
}

#[test]
fn muse_child_grant_accepts_turn_and_denies_session_or_message() {
    let mut store = store();
    let parent = muse_producer(&mut store, "muse-session-1");
    let request = muse_child_request(&parent, "muse-session-1", "wf-run-1", "child-step-1", 1);
    let child_ack = store
        .register_native_child(&parent, 1, request.clone(), NOW + 1)
        .unwrap()
        .value;
    let child_id = child_ack.agent.id;

    let turn_id = muse_child_turn_id("muse-session-1", "wf-run-1", "child-step-1", 1);
    let turn_batch = batch(
        &child_id,
        "muse-turn-1",
        vec![
            event("turn.started", "turn-start", json!({"turn_id": &turn_id})),
            event(
                "turn.completed",
                "turn-end",
                json!({"turn_id": &turn_id, "status": "completed"}),
            ),
        ],
    );
    let commit = store
        .put_native_child_batch(&parent, 1, turn_batch, NOW + 2)
        .unwrap();
    assert_eq!(commit.events.len(), 2);
    let turns = store.list_turns(&child_id, 1, 100).unwrap();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].turn_id, turn_id);
    assert_eq!(turns[0].state, TurnState::Completed);

    // DENY session.ready (no session.ready/ended for MUSE attempts)
    let session_ready_batch = batch(
        &child_id,
        "session-ready-attempt",
        vec![event(
            "session.ready",
            "child-ready",
            json!({"native_scope": MUSE_SCOPE, "native_ref": request.native_ref}),
        )],
    );
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, session_ready_batch, NOW + 3)
            .unwrap_err()
            .code,
        "source_integrity"
    );

    // DENY session.ended
    let session_ended_batch = batch(
        &child_id,
        "session-ended-attempt",
        vec![event(
            "session.ended",
            "child-ended",
            json!({"reason": "completed"}),
        )],
    );
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, session_ended_batch, NOW + 3)
            .unwrap_err()
            .code,
        "source_integrity"
    );

    // DENY message.completed (turn-only grant)
    let message_batch = batch(
        &child_id,
        "message-attempt",
        vec![event(
            "message.completed",
            "msg-1",
            json!({"turn_id": &turn_id, "text": "hello"}),
        )],
    );
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, message_batch, NOW + 3)
            .unwrap_err()
            .code,
        "source_integrity"
    );

    // DENY family outside any native child (not_allowed)
    let tool_batch = batch(
        &child_id,
        "tool-attempt",
        vec![event(
            "tool.started",
            "tool-1",
            json!({"turn_id": &turn_id, "tool": "exec"}),
        )],
    );
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, tool_batch, NOW + 3)
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn muse_child_source_gap_accepts_turn_and_denies_wrong_families() {
    let mut store = store();
    let parent = muse_producer(&mut store, "muse-session-1");
    let request = muse_child_request(&parent, "muse-session-1", "wf-run-1", "child-step-1", 1);
    let child_ack = store
        .register_native_child(&parent, 1, request, NOW + 1)
        .unwrap()
        .value;
    let child_id = child_ack.agent.id;

    // Gap with turn family succeeds:
    let turn_gap = SourceGap {
        gap_id: "muse-turn-gap".to_owned(),
        agent_id: child_id.clone(),
        generation: 1,
        source: INITIAL_SOURCE.to_owned(),
        families: vec!["turn".to_owned()],
        reason: "stream drop".to_owned(),
        from: None,
        to: None,
    };
    store
        .record_native_child_source_gap(&parent, 1, turn_gap, NOW + 2)
        .unwrap();

    // Gap claiming session family denied:
    let session_gap = SourceGap {
        gap_id: "muse-session-gap".to_owned(),
        agent_id: child_id.clone(),
        generation: 1,
        source: INITIAL_SOURCE.to_owned(),
        families: vec!["session".to_owned()],
        reason: "wrong family".to_owned(),
        from: None,
        to: None,
    };
    assert_eq!(
        store
            .record_native_child_source_gap(&parent, 1, session_gap, NOW + 3)
            .unwrap_err()
            .code,
        "source_integrity"
    );

    // Gap claiming process family denied by require_families:
    let process_gap = SourceGap {
        gap_id: "muse-process-gap".to_owned(),
        agent_id: child_id,
        generation: 1,
        source: INITIAL_SOURCE.to_owned(),
        families: vec!["process".to_owned()],
        reason: "wrong family".to_owned(),
        from: None,
        to: None,
    };
    assert_eq!(
        store
            .record_native_child_source_gap(&parent, 1, process_gap, NOW + 3)
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn muse_child_preserves_generation_fencing() {
    let mut store = store();
    let parent = muse_producer(&mut store, "muse-session-1");
    let request = muse_child_request(&parent, "muse-session-1", "wf-run-1", "child-step-1", 1);

    // Stale producer generation rejected:
    assert_eq!(
        store
            .register_native_child(&parent, 2, request.clone(), NOW + 1)
            .unwrap_err()
            .code,
        "stale_generation"
    );

    // Stale parent generation rejected:
    let mut stale_parent = request.clone();
    stale_parent.parent_generation = 2;
    assert_eq!(
        store
            .register_native_child(&parent, 1, stale_parent, NOW + 1)
            .unwrap_err()
            .code,
        "stale_generation"
    );

    let child_ack = store
        .register_native_child(&parent, 1, request, NOW + 1)
        .unwrap()
        .value;
    let child_id = child_ack.agent.id;

    // Advance producer generation:
    store
        .connection
        .execute(
            "UPDATE agents SET generation = 2 WHERE id = ?1",
            [parse_agent_id(&parent).unwrap()],
        )
        .unwrap();

    // Batch with old producer generation 1 is fenced out:
    let turn_id = muse_child_turn_id("muse-session-1", "wf-run-1", "child-step-1", 1);
    let turn_batch = batch(
        &child_id,
        "muse-turn-fenced",
        vec![event(
            "turn.started",
            "turn-start",
            json!({"turn_id": &turn_id}),
        )],
    );
    assert_eq!(
        store
            .put_native_child_batch(&parent, 1, turn_batch, NOW + 2)
            .unwrap_err()
            .code,
        "stale_generation"
    );
}

#[test]
fn muse_child_unavailable_cleanup_gaps_only_turn_family() {
    let mut store = store();
    let parent = muse_producer(&mut store, "muse-session-1");
    let request = muse_child_request(&parent, "muse-session-1", "wf-run-1", "child-step-1", 1);
    let child_ack = store
        .register_native_child(&parent, 1, request, NOW + 1)
        .unwrap()
        .value;
    let child_id = child_ack.agent.id;

    let turn_id = muse_child_turn_id("muse-session-1", "wf-run-1", "child-step-1", 1);
    store
        .put_native_child_batch(
            &parent,
            1,
            batch(
                &child_id,
                "muse-turn-live",
                vec![event(
                    "turn.started",
                    "turn-start",
                    json!({"turn_id": &turn_id}),
                )],
            ),
            NOW + 2,
        )
        .unwrap();

    let events = store
        .mark_native_children_unavailable(&parent, 1, "producer disconnected", true, NOW + 3)
        .unwrap();

    assert_eq!(
        store
            .get_turn(&child_id, 1, &turn_id)
            .unwrap()
            .unwrap()
            .state,
        TurnState::Unknown
    );

    let gap_event = events
        .iter()
        .find(|e| e.kind == "source.gap" && e.agent.as_deref() == Some(child_id.as_str()))
        .expect("child gap event");
    assert_eq!(gap_event.data["families"], json!(["turn"]));

    let source = store.read_source_state(&child_id, 1, "stream").unwrap();
    assert_eq!(source["state"], "closed");
}
