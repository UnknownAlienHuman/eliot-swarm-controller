use super::*;
use std::path::Path;

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn definition(id: &str, enabled: bool) -> RuleDefinitionInput {
    let value = json!({"id": id, "enabled": enabled});
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: core::canonical_json_sha256(&value).unwrap(),
        enabled,
    }
}

fn insert_test_event(store: &mut Store, kind: &str, now: i64) -> i64 {
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let event = insert_daemon_event(&tx, now, kind, "rule_test", None, None, json!({})).unwrap();
    tx.commit().unwrap();
    event.seq
}

fn insert_test_agent(store: &mut Store) -> (i64, String) {
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    tx.execute(
        "INSERT INTO agents(generation, project, harness, profile, state, created_at, updated_at)
             VALUES (1, 'project-test', 'fake', 'fake', 'idle', 1, 1)",
        [],
    )
    .unwrap();
    let agent = tx.last_insert_rowid();
    tx.commit().unwrap();
    (agent, format_agent_id(agent))
}

fn ready_terminal_fake_agent(
    store: &mut Store,
    project: &str,
    now: i64,
) -> (String, String, String) {
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": project,
        "harness": "codex",
        "profile": "fake",
        "profile_snapshot": {},
        "workspace": format!("synthetic-{project}"),
    });
    launch.expected_generation = Some(0);
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .unwrap()
        .command
        .target
        .expect("fixture launch Agent");
    let native_scope = format!("{project}:{agent}:scope");
    let native_ref = format!("{project}:{agent}:thread");
    store.dispatch(&launch_id, now + 1).unwrap();
    crate::tests::install_fixture_sources(store, &agent, &launch_id, 1, now + 1);
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
                    data: json!({
                        "command_id": launch_id.as_str(),
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 2,
        )
        .unwrap();
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .unwrap();
    crate::tests::install_live_custody_observations(store, &agent, 1, now + 3);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ended"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: format!("{project}:ended"),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 4,
        )
        .unwrap();
    crate::tests::simulate_normal_stop_proof(store, &agent, 1, now + 4);
    (agent, native_scope, native_ref)
}

fn insert_test_rule_action(
    store: &mut Store,
    principal: &str,
    rule: &str,
    target: &str,
    generation: i64,
    now: i64,
) {
    let parent_id = Ulid::new().to_string();
    let child_id = Ulid::new().to_string();
    let target_agent = parse_agent_id(target).unwrap();
    ensure_test_project_action_scope(store, principal, target_agent, generation, now);
    let payload_digest = [0_u8; 32];
    let args = json!({"rule": rule});
    let outcome = json!({"children": [child_id]});
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let parent = CommandInsert {
        id: &parent_id,
        principal,
        method: COMMAND_METHOD,
        payload_digest: &payload_digest,
        target: None,
        selector: None,
        expected_generation: None,
        generation: None,
        expected_turn_id: None,
        parent_command: None,
        link_kind: None,
        ordinal: None,
        turn_id: None,
        request_id: None,
        probe: false,
        verb: "rule",
        args: &args,
        state: CommandState::Applied,
        outcome: Some(&outcome),
        created_at: now,
        updated_at: now,
    };
    admit_command(
        &tx,
        &parent,
        json!({"command_id": parent_id, "children": [child_id]}),
    )
    .unwrap();

    let child_args = json!({});
    let child = CommandInsert {
        id: &child_id,
        principal,
        method: COMMAND_METHOD,
        payload_digest: &payload_digest,
        target: Some(target_agent),
        selector: None,
        expected_generation: Some(generation),
        generation: Some(generation),
        expected_turn_id: None,
        parent_command: Some(&parent_id),
        link_kind: Some("rule_action"),
        ordinal: Some(0),
        turn_id: None,
        request_id: None,
        probe: false,
        verb: "stop",
        args: &child_args,
        state: CommandState::Accepted,
        outcome: None,
        created_at: now,
        updated_at: now,
    };
    insert_command(&tx, &child, &ordinary_command_cause(&child_id)).unwrap();
    tx.commit().unwrap();
}

fn ensure_test_project_action_scope(
    store: &mut Store,
    principal: &str,
    agent: i64,
    generation: i64,
    now: i64,
) {
    let Some(project) = principal.strip_prefix("project:") else {
        return;
    };
    let checkout = format!("synthetic-checkout-{project}");
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.clone(),
                repo_common_dir: format!("synthetic-common-{project}"),
                git: None,
                profiles: vec!["fake".to_owned()],
                actions: vec!["stop".to_owned()],
                interpreters: std::collections::BTreeMap::new(),
                run_env: Vec::new(),
            },
            now,
        )
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE agents SET project = ?1, profile = 'fake', workspace = ?2,
                     generation = ?3 WHERE id = ?4",
            params![project, checkout, generation, agent],
        )
        .unwrap();
}

fn insert_test_turn_completed(store: &mut Store, agent: i64, now: i64) -> i64 {
    insert_test_turn_completed_with_cause(store, agent, now, None)
}

fn insert_test_turn_completed_with_cause(
    store: &mut Store,
    agent: i64,
    now: i64,
    cause: Option<core::EventCause>,
) -> i64 {
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let event = insert_event(
        &tx,
        EventInsert {
            ts: now,
            kind: "turn.completed",
            family: Some("turn"),
            agent: Some(agent),
            generation: Some(1),
            source: "stream",
            native_id: None,
            cursor: None,
            payload_digest: None,
            subject: None,
            cause,
            data: json!({
                "status": "completed",
                "cause": {"command_id": "forged", "depth": 5}
            }),
        },
    )
    .unwrap();
    tx.commit().unwrap();
    event.seq
}

fn stop_request(agent: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
    request
}

fn interrupt_request(agent: &str, generation: i64, turn_id: Option<&str>) -> SubmitCommand {
    let mut request = SubmitCommand::new("", "interrupt");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(generation);
    request.expected_turn_id = turn_id.map(str::to_owned);
    request
}

fn steer_request(agent: &str, generation: i64, turn_id: &str, text: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new("", "steer");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(generation);
    request.expected_turn_id = Some(turn_id.to_owned());
    request.args = json!({"text": text});
    request
}

fn set_interrupt_test_turn(store: &mut Store, agent: i64, turn_id: &str) {
    store
        .connection
        .execute(
            "UPDATE agents SET harness = 'codex', state = 'busy', active_turn = ?1
                 WHERE id = ?2",
            params![turn_id, agent],
        )
        .unwrap();
}

fn insert_test_interrupt_event(
    store: &mut Store,
    agent: i64,
    generation: i64,
    turn_id: Option<&str>,
    now: i64,
) -> i64 {
    let data = match turn_id {
        Some(turn_id) => json!({"turn_id": turn_id}),
        None => json!({}),
    };
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let event = insert_event(
        &tx,
        EventInsert {
            ts: now,
            kind: "turn.started",
            family: Some("turn"),
            agent: Some(agent),
            generation: Some(generation),
            source: "stream",
            native_id: None,
            cursor: None,
            payload_digest: None,
            subject: None,
            cause: None,
            data,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    event.seq
}

#[test]
fn event_interrupt_binds_exact_turn_and_replays_the_same_child() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    set_interrupt_test_turn(&mut store, agent, "turn-captured");
    let rule = definition("interrupt-rule", true);
    let state = store.ensure_rule_state("owner", &rule, 10).unwrap();
    let event_seq = insert_test_interrupt_event(&mut store, agent, 1, Some("turn-captured"), 11);
    let request = interrupt_request(&agent_id, 1, Some("turn-captured"));
    assert!(valid_rule_action_request(&request, true));
    assert!(!valid_rule_action_request(&request, false));

    let first = store
        .admit_rule_action("owner", &state, event_seq, request.clone(), 12)
        .unwrap();
    assert!(first.is_new);
    let parent_id = first.parent.as_ref().expect("admitted parent").id.clone();
    let child = &first
        .child
        .as_ref()
        .expect("admitted interrupt child")
        .command;
    assert_eq!(child.verb, "interrupt");
    assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(child.expected_generation, Some(1));
    assert_eq!(child.turn_id.as_deref(), Some("turn-captured"));
    assert_eq!(child.state, CommandState::Accepted);
    let child_id = child.id.clone();
    let retained_expected_turn: Option<String> = store
        .connection
        .query_row(
            "SELECT expected_turn_id FROM commands WHERE id = ?1",
            [&child_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_expected_turn.as_deref(), Some("turn-captured"));

    let replay = store
        .admit_rule_action("owner", &state, event_seq, request, 13)
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, parent_id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, child_id);
    assert!(replay.events.is_empty());
    assert!(store.get_command(&child_id).unwrap().is_some());
}

#[test]
fn event_interrupt_rejects_wrong_agent_generation_or_missing_captured_turn() {
    for mismatch in [
        "agent",
        "generation",
        "missing_event_turn",
        "missing_request_turn",
    ] {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        set_interrupt_test_turn(&mut store, agent, "turn-captured");
        if mismatch == "generation" {
            store
                .connection
                .execute("UPDATE agents SET generation = 2 WHERE id = ?1", [agent])
                .unwrap();
        }
        let other = if mismatch == "agent" {
            let (other, other_id) = insert_test_agent(&mut store);
            set_interrupt_test_turn(&mut store, other, "turn-captured");
            Some(other_id)
        } else {
            None
        };
        let rule = definition("interrupt-negative", true);
        let state = store.ensure_rule_state("owner", &rule, 20).unwrap();
        let event_turn = (mismatch != "missing_event_turn").then_some("turn-captured");
        let event_seq = insert_test_interrupt_event(&mut store, agent, 1, event_turn, 21);
        let (target, generation) = match mismatch {
            "agent" => (other.as_deref().unwrap(), 1),
            "generation" => (agent_id.as_str(), 2),
            "missing_event_turn" | "missing_request_turn" => (agent_id.as_str(), 1),
            _ => unreachable!(),
        };
        let request_turn = (mismatch != "missing_request_turn").then_some("turn-captured");
        let request = interrupt_request(target, generation, request_turn);

        let admission = store
            .admit_rule_action("owner", &state, event_seq, request, 22)
            .unwrap();
        assert!(admission.child.is_none(), "{mismatch} created a child");
        assert_eq!(admission.state.current_cursor, event_seq);
        let parent = admission.parent.expect("committed rejection parent");
        assert_eq!(parent.state, CommandState::Rejected);
        assert_eq!(parent.outcome.as_ref().unwrap()["children"], json!([]));
        assert_eq!(
            parent.outcome.as_ref().unwrap()["code"],
            "rule_action_binding_invalid"
        );
        let child_id =
            derived_rule_id("action-0", "owner", &rule.id, state.revision, event_seq).unwrap();
        assert!(store.get_command(&child_id).unwrap().is_none());
    }
}

#[test]
fn event_steer_binds_exact_committed_turn_replays_and_never_retargets() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    set_interrupt_test_turn(&mut store, agent, "turn-captured");
    let rule = definition("steer-rule", true);
    let state = store.ensure_rule_state("owner", &rule, 30).unwrap();
    let event_seq = insert_test_interrupt_event(&mut store, agent, 1, Some("turn-captured"), 31);
    let request = steer_request(&agent_id, 1, "turn-captured", "continue this turn");
    assert!(valid_event_rule_action_request(&request));
    assert!(!valid_rule_action_request(&request, true));

    let first = store
        .admit_rule_action("owner", &state, event_seq, request.clone(), 32)
        .unwrap();
    assert!(first.is_new);
    let parent_id = first.parent.as_ref().unwrap().id.clone();
    let child = &first.child.as_ref().expect("admitted STEER child").command;
    assert_eq!(child.verb, "steer");
    assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(child.expected_generation, Some(1));
    assert_eq!(child.turn_id.as_deref(), Some("turn-captured"));
    assert_eq!(child.args, json!({"text":"continue this turn"}));
    let child_id = child.id.clone();
    let retained_expected_turn: Option<String> = store
        .connection
        .query_row(
            "SELECT expected_turn_id FROM commands WHERE id = ?1",
            [&child_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_expected_turn.as_deref(), Some("turn-captured"));

    store
        .connection
        .execute(
            "UPDATE agents SET active_turn = 'turn-new' WHERE id = ?1",
            [agent],
        )
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action("owner", &state, event_seq, request, 33)
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, parent_id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, child_id);
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);

    assert_eq!(
        store.dispatch(&child_id, 34).unwrap_err().code,
        "stale_generation"
    );
    assert_eq!(
        store.get_command(&child_id).unwrap().unwrap().state,
        CommandState::Accepted
    );
}

#[test]
fn event_steer_rejects_forged_event_turn_and_stale_current_turn() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    set_interrupt_test_turn(&mut store, agent, "turn-current");
    let rule = definition("steer-binding-rule", true);
    let state = store.ensure_rule_state("owner", &rule, 40).unwrap();

    let forged_event = insert_test_interrupt_event(&mut store, agent, 1, Some("turn-event"), 41);
    let forged = store
        .admit_rule_action(
            "owner",
            &state,
            forged_event,
            steer_request(&agent_id, 1, "turn-forged", "must not retarget"),
            42,
        )
        .unwrap();
    assert!(forged.child.is_none());
    assert_eq!(
        forged.parent.unwrap().outcome.unwrap()["code"],
        "rule_action_binding_invalid"
    );

    let stale_event =
        insert_test_interrupt_event(&mut store, agent, 1, Some("turn-historical"), 43);
    let current = store.get_rule_state("owner", &rule.id).unwrap().unwrap();
    let stale = store
        .admit_rule_action(
            "owner",
            &current,
            stale_event,
            steer_request(&agent_id, 1, "turn-historical", "must remain stale"),
            44,
        )
        .unwrap();
    assert!(stale.child.is_none());
    assert_eq!(
        stale.parent.unwrap().outcome.unwrap()["code"],
        "stale_generation"
    );
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands WHERE verb = 'steer'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn event_inject_binds_exact_agent_generation_and_turn() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    store
            .connection
            .execute(
                "UPDATE agents SET harness = 'codex', state = 'busy', active_turn = 'turn-captured',
                 effective = '{\"context_points\":[\"turn_safe_point\",\"next_input\"]}' WHERE id = ?1",
                [agent],
            )
            .unwrap();
    let rule = definition("inject-rule", true);
    let state = store.ensure_rule_state("owner", &rule, 60).unwrap();
    let event_seq = insert_test_interrupt_event(&mut store, agent, 1, Some("turn-captured"), 61);
    let mut request = SubmitCommand::new("", "inject");
    request.target = Some(agent_id.clone());
    request.expected_generation = Some(1);
    request.expected_turn_id = Some("turn-captured".to_owned());
    request.args = json!({"text": "ctx", "point": "turn_safe_point"});
    assert!(valid_event_rule_action_request(&request));
    let admitted = store
        .admit_rule_action("owner", &state, event_seq, request, 62)
        .unwrap();
    assert!(admitted.is_new);
    let child = &admitted
        .child
        .as_ref()
        .expect("admitted inject child")
        .command;
    assert_eq!(child.verb, "inject");
    assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(child.expected_generation, Some(1));
    assert_eq!(child.turn_id.as_deref(), Some("turn-captured"));
    let child_id = child.id.clone();
    let retained_expected_turn: Option<String> = store
        .connection
        .query_row(
            "SELECT expected_turn_id FROM commands WHERE id = ?1",
            [&child_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_expected_turn.as_deref(), Some("turn-captured"));

    // A forged turn does not bind the captured event.
    let current = store.get_rule_state("owner", &rule.id).unwrap().unwrap();
    let forged_event = insert_test_interrupt_event(&mut store, agent, 1, Some("turn-event"), 63);
    let mut forged = SubmitCommand::new("", "inject");
    forged.target = Some(agent_id.clone());
    forged.expected_generation = Some(1);
    forged.expected_turn_id = Some("turn-forged".to_owned());
    forged.args = json!({"text": "ctx", "point": "turn_safe_point"});
    let rejected = store
        .admit_rule_action("owner", &current, forged_event, forged, 64)
        .unwrap();
    assert!(rejected.child.is_none());
    assert_eq!(
        rejected.parent.unwrap().outcome.unwrap()["code"],
        "rule_action_binding_invalid"
    );
}

#[test]
fn event_inject_cannot_rebind_an_old_event_to_a_resumed_generation() {
    for point in ["next_input", "turn_safe_point"] {
        for generation in [None, Some(2), Some(1)] {
            let mut store = store();
            let (agent, agent_id) = insert_test_agent(&mut store);
            let rule = definition("old-inject", true);
            let state = store.ensure_rule_state("owner", &rule, 60).unwrap();
            let event = insert_test_interrupt_event(&mut store, agent, 1, Some("reused-turn"), 61);
            store.connection.execute(
                    "UPDATE agents SET generation = 2, harness = 'codex', state = 'busy',
                     active_turn = 'reused-turn',
                     effective = '{\"context_points\":[\"turn_safe_point\",\"next_input\"]}' WHERE id = ?1",
                    [agent],
                ).unwrap();
            let mut request = SubmitCommand::new("", "inject");
            request.target = Some(agent_id);
            request.expected_generation = generation;
            request.expected_turn_id =
                (point == "turn_safe_point").then(|| "reused-turn".to_owned());
            request.args = json!({"text": "old context", "point": point});
            let admission = store
                .admit_rule_action("owner", &state, event, request, 62)
                .unwrap();
            assert!(
                admission.child.is_none(),
                "{point} {generation:?} crossed generations"
            );
            assert_eq!(
                admission.parent.unwrap().outcome.unwrap()["code"],
                if generation == Some(1) {
                    "stale_generation"
                } else {
                    "rule_action_binding_invalid"
                }
            );
            assert_eq!(
                store
                    .connection
                    .query_row(
                        "SELECT COUNT(*) FROM commands WHERE verb = 'inject'",
                        [],
                        |row| row.get::<_, i64>(0)
                    )
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn project_rule_inject_admits_only_its_live_project_agent() {
    for in_project in [true, false] {
        let mut store = store();
        let (agent, agent_id) = insert_test_agent(&mut store);
        store
            .upsert_project_registration(
                "owner",
                &ProjectRegistrationInput {
                    id: "alpha".to_owned(),
                    checkout: "alpha-checkout".to_owned(),
                    repo_common_dir: "alpha-common".to_owned(),
                    git: None,
                    profiles: vec!["fake".to_owned()],
                    actions: vec!["inject".to_owned()],
                    interpreters: std::collections::BTreeMap::new(),
                    run_env: Vec::new(),
                },
                59,
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET workspace = ?1, harness = 'codex',
                 effective = '{\"context_points\":[\"next_input\"]}' WHERE id = ?2",
                params![
                    if in_project {
                        "alpha-checkout"
                    } else {
                        "beta-checkout"
                    },
                    agent
                ],
            )
            .unwrap();
        let value = json!({"id": "project-inject", "enabled": true, "do": [{"action": "inject"}]});
        let rule = RuleDefinitionInput {
            id: "project-inject".to_owned(),
            canonical_definition: serde_json::to_string(&value).unwrap(),
            definition_digest: core::canonical_json_sha256(&value).unwrap(),
            enabled: true,
        };
        let state = store.ensure_rule_state("project:alpha", &rule, 60).unwrap();
        let event = insert_test_interrupt_event(&mut store, agent, 1, Some("turn"), 61);
        let mut request = SubmitCommand::new("", "inject");
        request.target = Some(agent_id);
        request.expected_generation = Some(1);
        request.args = json!({"text": "project context", "point": "next_input"});
        let admission = store
            .admit_rule_action("project:alpha", &state, event, request, 62)
            .unwrap();
        if in_project {
            let child = admission.child.expect("live project inject").command;
            assert_eq!(child.expected_generation, Some(1));
            assert_eq!(child.principal, "project:alpha");
        } else {
            assert!(admission.child.is_none());
            assert_eq!(
                admission.parent.unwrap().outcome.unwrap()["code"],
                "not_allowed"
            );
        }
    }
}

#[test]
fn historical_interrupt_does_not_retarget_a_new_active_turn() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
    let (agent, agent_id) = insert_test_agent(&mut store);
    set_interrupt_test_turn(&mut store, agent, "turn-current");
    let rule = definition("history-interrupt", true);
    let state = store.ensure_rule_state("owner", &rule, now - 2).unwrap();
    let event_seq =
        insert_test_interrupt_event(&mut store, agent, 1, Some("turn-historical"), now - 1);
    let advanced = store
        .advance_rule_cursor("owner", &state, event_seq, RuleCursorOutcome::NoMatch, now)
        .unwrap();
    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        rule.id.as_str(),
        state.current_cursor,
        Some(event_seq),
    );
    let actions = [RuleCatchUpAction::command(
        event_seq,
        interrupt_request(&agent_id, 1, Some("turn-historical")),
    )];
    let high_water = store.high_water().unwrap();

    let first = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &advanced.value,
            event_seq,
            &actions,
            now + 1,
        )
        .unwrap();
    assert!(first.is_new);
    assert_eq!(first.parent.state, CommandState::Applied);
    assert!(first.children.is_empty());
    let outcome = first.parent.outcome.as_ref().expect("catch-up outcome");
    assert_eq!(outcome["children"], json!([]));
    assert!(outcome.get("code").is_none());
    let rejected = first
        .events
        .iter()
        .find(|event| {
            event.kind == "rule.rejected"
                && event.data["parent_command"] == request.id
                && event.data["event_seq"] == event_seq
        })
        .expect("stale historical interrupt rejection marker");
    assert_eq!(rejected.data["code"], "stale_generation");
    assert_eq!(
        first
            .events
            .iter()
            .filter(|event| {
                event.kind == "rule.rejected"
                    && event.data["parent_command"] == request.id
                    && event.data["event_seq"] == event_seq
            })
            .count(),
        1
    );
    let child_id =
        derived_rule_id("action-0", "owner", &rule.id, state.revision, event_seq).unwrap();
    assert!(store.get_command(&child_id).unwrap().is_none());
    let committed_high_water = store.high_water().unwrap();
    assert!(committed_high_water > high_water);
    assert_eq!(
        store
            .get_rule_state("owner", &rule.id)
            .unwrap()
            .unwrap()
            .current_cursor,
        event_seq
    );

    let replay = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &advanced.value,
            event_seq,
            &actions,
            now + 2,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.id, first.parent.id);
    assert_eq!(replay.parent.state, CommandState::Applied);
    assert!(replay.children.is_empty());
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), committed_high_water);
}

#[test]
fn catch_up_rejections_do_not_consume_rate_before_a_valid_child() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
    let (agent, agent_id) = insert_test_agent(&mut store);
    set_interrupt_test_turn(&mut store, agent, "turn-current");

    let mut rule = definition("history-interrupt-limit", true);
    let mut canonical: Value = serde_json::from_str(&rule.canonical_definition).unwrap();
    canonical["limit"] = json!({
        "max": 1,
        "per": "rule",
        "window_ms": RULE_ACTION_WINDOW_MS,
    });
    rule.canonical_definition = serde_json::to_string(&canonical).unwrap();
    rule.definition_digest = core::canonical_json_sha256(&canonical).unwrap();

    let state = store.ensure_rule_state("owner", &rule, now - 3).unwrap();
    let stale_event =
        insert_test_interrupt_event(&mut store, agent, 1, Some("turn-historical"), now - 3);
    let current_event =
        insert_test_interrupt_event(&mut store, agent, 1, Some("turn-current"), now - 2);
    let prepared_rejection_event =
        insert_test_interrupt_event(&mut store, agent, 1, Some("turn-current"), now - 1);
    assert!(stale_event < current_event && current_event < prepared_rejection_event);
    let advanced = store
        .advance_rule_cursor(
            "owner",
            &state,
            prepared_rejection_event,
            RuleCursorOutcome::NoMatch,
            now + 1,
        )
        .unwrap();
    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        rule.id.as_str(),
        state.current_cursor,
        Some(prepared_rejection_event),
    );
    let actions = [
        RuleCatchUpAction::command(
            stale_event,
            interrupt_request(&agent_id, 1, Some("turn-historical")),
        ),
        RuleCatchUpAction::command(
            current_event,
            interrupt_request(&agent_id, 1, Some("turn-current")),
        ),
        RuleCatchUpAction::rejected(prepared_rejection_event, "invalid"),
    ];

    let admission = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &advanced.value,
            prepared_rejection_event,
            &actions,
            now + 2,
        )
        .unwrap();
    assert!(admission.is_new);
    assert_eq!(admission.parent.state, CommandState::Applied);
    assert_eq!(admission.children.len(), 1);
    let admitted_child = &admission.children[0].command;
    assert_eq!(admitted_child.verb, "interrupt");
    assert_eq!(admitted_child.turn_id.as_deref(), Some("turn-current"));
    assert_eq!(
        admission.parent.outcome.as_ref().unwrap()["children"],
        json!([admitted_child.id.clone()])
    );
    let rejected_child_id = derived_rule_id(
        "action-0",
        "owner",
        &rule.id,
        state.revision,
        prepared_rejection_event,
    )
    .unwrap();
    assert!(store.get_command(&rejected_child_id).unwrap().is_none());
    let failed_submit_child_id =
        derived_rule_id("action-0", "owner", &rule.id, state.revision, stale_event).unwrap();
    assert!(
        store
            .get_command(&failed_submit_child_id)
            .unwrap()
            .is_none()
    );
    let failed_submit_rejected = admission
        .events
        .iter()
        .find(|event| {
            event.kind == "rule.rejected"
                && event.data["event_seq"] == stale_event
                && event.data["parent_command"] == request.id
        })
        .expect("failed ordinary submit has a rejection marker");
    assert_eq!(failed_submit_rejected.data["code"], "stale_generation");
    let rejected = admission
        .events
        .iter()
        .find(|event| {
            event.kind == "rule.rejected"
                && event.data["event_seq"] == prepared_rejection_event
                && event.data["parent_command"] == request.id
        })
        .expect("prepared rejection has a marker");
    assert_eq!(rejected.data["code"], "invalid");
    let fired = admission
        .events
        .iter()
        .find(|event| {
            event.kind == "rule.fired"
                && event.data["event_seq"] == current_event
                && event.data["child_command"] == admitted_child.id
        })
        .expect("valid second action is admitted");
    assert_eq!(fired.data["parent_command"], request.id);

    let linked_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
                 WHERE parent_command = ?1 AND link_kind = 'rule_action'",
            [&admission.parent.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(linked_children, 1);

    let high_water = store.high_water().unwrap();
    let replay = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &advanced.value,
            prepared_rejection_event,
            &actions,
            now + 3,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.id, admission.parent.id);
    assert_eq!(replay.children[0].command.id, admitted_child.id);
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn new_rule_seeds_once_and_changed_definition_retires_old_revision() {
    let mut store = store();
    let first_event = insert_test_event(&mut store, "fixture", 1);
    let first = definition("r1", true);
    let seeded = store
        .reconcile_rule_definitions("owner", std::slice::from_ref(&first), &[], 2, &[])
        .unwrap();
    let view = seeded.value[0].view.clone();
    assert_eq!(view.current_cursor, first_event);

    let later_event = insert_test_event(&mut store, "fixture", 3);
    let unchanged = store
        .reconcile_rule_definitions("owner", &[first], &[], 4, &[])
        .unwrap();
    assert_eq!(unchanged.value[0].view, view);

    let changed = definition("r1", false);
    let reconciled = store
        .reconcile_rule_definitions("owner", std::slice::from_ref(&changed), &[], 5, &[])
        .unwrap();
    let updated = reconciled.value[0].view.clone();
    assert_eq!(updated.revision, view.revision + 1);
    assert_eq!(updated.current_cursor, later_event);
    assert!(!updated.enabled);
    assert!(updated.retiring_revision.is_none());
    assert_eq!(reconciled.events.len(), 1);
    assert_eq!(reconciled.events[0].kind, "rule.skipped");
    assert_eq!(
        store
            .reconcile_rule_definitions("owner", &[changed], &[], 6, &[])
            .unwrap()
            .value[0]
            .view,
        updated
    );
    assert!(later_event > view.current_cursor);
}

#[test]
fn enabled_definition_edit_retires_through_captured_high_water() {
    let mut store = store();
    let first_event = insert_test_event(&mut store, "fixture", 1);
    let old = definition("r1", true);
    let old_view = store.ensure_rule_state("owner", &old, 2).unwrap();
    let boundary = insert_test_event(&mut store, "fixture", 3);
    let changed = RuleDefinitionInput {
        canonical_definition: serde_json::to_string(&json!({"id":"r1","enabled":true,"new":true}))
            .unwrap(),
        definition_digest: core::canonical_json_sha256(
            &json!({"id":"r1","enabled":true,"new":true}),
        )
        .unwrap(),
        ..old
    };

    let view = store.ensure_rule_state("owner", &changed, 4).unwrap();
    assert_eq!(view.revision, old_view.revision + 1);
    assert_eq!(view.current_cursor, boundary);
    assert_eq!(view.retiring_revision, Some(old_view.revision));
    assert_eq!(view.retiring_cursor, Some(first_event));
    assert_eq!(view.retire_through, Some(boundary));

    let advanced = store
        .advance_rule_cursor("owner", &view, boundary, RuleCursorOutcome::NoMatch, 5)
        .unwrap();
    assert!(advanced.value.retiring_revision.is_none());
    assert!(advanced.value.retiring_cursor.is_none());
    assert!(advanced.value.retire_through.is_none());
}

#[test]
fn retiring_revision_admits_with_its_own_deterministic_key() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    let old = definition("stop-rule", true);
    let old_view = store.ensure_rule_state("owner", &old, 1).unwrap();
    let event_seq = insert_test_turn_completed(&mut store, agent, 2);
    let changed_value = json!({"id":"stop-rule","enabled":true,"matcher":"updated"});
    let changed = RuleDefinitionInput {
        canonical_definition: serde_json::to_string(&changed_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&changed_value).unwrap(),
        ..old
    };
    let current = store.ensure_rule_state("owner", &changed, 3).unwrap();
    assert_eq!(current.retiring_revision, Some(old_view.revision));

    let admission = store
        .admit_rule_action("owner", &current, event_seq, stop_request(&agent_id), 4)
        .unwrap();
    let parent = admission.parent.expect("retiring rule parent");
    assert_eq!(parent.args["revision"], old_view.revision);
    assert_eq!(
        parent.id,
        derived_rule_id("parent", "owner", "stop-rule", old_view.revision, event_seq).unwrap()
    );
    assert!(admission.child.is_some());
    assert!(admission.state.retiring_revision.is_none());
}

#[test]
fn no_match_and_disabled_range_bookkeeping_reaches_quiescence() {
    let mut store = store();
    let enabled = store
        .ensure_rule_state("owner", &definition("enabled", true), 1)
        .unwrap();
    let no_match_seq = insert_test_event(&mut store, "rule.skipped", 2);
    let no_match = store
        .advance_rule_cursor(
            "owner",
            &enabled,
            no_match_seq,
            RuleCursorOutcome::NoMatch,
            3,
        )
        .unwrap();
    assert!(no_match.events.is_empty());
    let high_water_after_no_match = store.high_water().unwrap();

    let disabled = store
        .ensure_rule_state("owner", &definition("disabled", false), 4)
        .unwrap();
    let trigger_seq = insert_test_event(&mut store, "turn.completed", 5);
    let skipped = store
        .advance_rule_cursor(
            "owner",
            &disabled,
            trigger_seq,
            RuleCursorOutcome::Disabled,
            6,
        )
        .unwrap();
    assert_eq!(skipped.events.len(), 1);
    assert_eq!(skipped.events[0].kind, "rule.skipped");

    // A disabled consumer of the marker must not create another marker.
    let marker_seq = skipped.events[0].seq;
    let marker = store
        .advance_rule_cursor(
            "owner",
            &skipped.value,
            marker_seq,
            RuleCursorOutcome::Disabled,
            7,
        )
        .unwrap();
    assert!(marker.events.is_empty());
    assert_eq!(store.high_water().unwrap(), marker_seq);
    assert_eq!(high_water_after_no_match, no_match_seq);

    let disabled_again = store
        .advance_rule_cursor(
            "owner",
            &marker.value,
            marker_seq,
            RuleCursorOutcome::Disabled,
            8,
        )
        .unwrap();
    assert!(disabled_again.events.is_empty());
    assert_eq!(store.high_water().unwrap(), marker_seq);
}

#[test]
fn cursor_persistence_failure_rolls_back_and_faults_writer() {
    let mut store = store();
    let view = store
        .ensure_rule_state("owner", &definition("r1", true), 1)
        .unwrap();
    let event_seq = insert_test_event(&mut store, "fixture", 2);
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER fail_rule_cursor BEFORE UPDATE OF current_cursor ON rule_state
                 BEGIN SELECT RAISE(ABORT, 'injected rule cursor failure'); END;",
        )
        .unwrap();

    assert_eq!(
        store
            .advance_rule_cursor("owner", &view, event_seq, RuleCursorOutcome::NoMatch, 3,)
            .unwrap_err()
            .code,
        "internal"
    );
    assert!(store.is_write_faulted());
    assert_eq!(store.get_rule_state("owner", "r1").unwrap().unwrap(), view);
    assert_eq!(store.high_water().unwrap(), event_seq);
}

#[test]
fn backlog_guard_counts_committed_rows_then_pauses_and_clears_retirement() {
    let mut store = store();
    let mut state = store
        .ensure_rule_state("owner", &definition("r1", true), 1)
        .unwrap();
    let starting_cursor = state.current_cursor;
    let first = insert_test_event(&mut store, "fixture", 2);
    assert_eq!(first, starting_cursor + 1);
    let deleted = insert_test_event(&mut store, "fixture", 3);
    let third = insert_test_event(&mut store, "fixture", 4);
    store
        .connection
        .execute("DELETE FROM events WHERE seq = ?1", [deleted])
        .unwrap();

    let below_limit = store
        .settle_rule_backlog("owner", &state, third, 2, 5)
        .unwrap();
    assert_eq!(below_limit.value.view.current_cursor, state.current_cursor);
    assert!(below_limit.value.view.paused_reason.is_none());

    let fourth = insert_test_event(&mut store, "fixture", 6);
    let over_limit = store
        .settle_rule_backlog("owner", &state, fourth, 2, 7)
        .unwrap();
    state = over_limit.value.view.clone();
    assert_eq!(state.current_cursor, fourth);
    assert_eq!(state.paused_reason.as_deref(), Some("backlog_exceeded"));
    assert!(state.retiring_revision.is_none());
    assert_eq!(over_limit.events.len(), 1);
    assert_eq!(over_limit.events[0].data["from_seq"], starting_cursor + 1);
    assert_eq!(over_limit.events[0].data["through_seq"], fourth);
}

#[test]
fn verified_depth_suppresses_without_parent_child_or_event() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    let state = store
        .ensure_rule_state("owner", &definition("stop-rule", true), 1)
        .unwrap();
    let trigger_cause = core::EventCause {
        command_id: Ulid::new().to_string(),
        rule: Some("upstream".to_owned()),
        remote_call: None,
        depth: 5,
    };
    let event_seq =
        insert_test_turn_completed_with_cause(&mut store, agent, 2, Some(trigger_cause));

    let suppressed = store
        .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), 3)
        .unwrap();
    assert!(suppressed.is_new);
    assert_eq!(suppressed.state.current_cursor, event_seq);
    assert!(suppressed.parent.is_none());
    assert!(suppressed.child.is_none());
    assert!(suppressed.events.is_empty());
    assert_eq!(store.high_water().unwrap(), event_seq);
}

#[test]
fn rejected_cursor_suppresses_sixth_depth_and_rule_marker_quiesces() {
    let mut store = store();
    let (agent, _) = insert_test_agent(&mut store);
    let state_five = store
        .ensure_rule_state("owner", &definition("r5", true), 1)
        .unwrap();
    let cause_five = core::EventCause {
        command_id: Ulid::new().to_string(),
        rule: Some("upstream".to_owned()),
        remote_call: None,
        depth: 5,
    };
    let event_five = insert_test_turn_completed_with_cause(&mut store, agent, 2, Some(cause_five));
    let events_before = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let commands_before = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();

    let suppressed = store
        .advance_rule_cursor(
            "owner",
            &state_five,
            event_five,
            RuleCursorOutcome::Rejected {
                code: "render_error".to_owned(),
            },
            3,
        )
        .unwrap();
    assert_eq!(suppressed.value.current_cursor, event_five);
    assert!(suppressed.events.is_empty());
    assert_eq!(store.high_water().unwrap(), event_five);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        events_before
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before
    );

    let state_four = store
        .ensure_rule_state("owner", &definition("r4", true), 4)
        .unwrap();
    let cause_four = core::EventCause {
        command_id: Ulid::new().to_string(),
        rule: Some("upstream".to_owned()),
        remote_call: None,
        depth: 4,
    };
    let event_four =
        insert_test_turn_completed_with_cause(&mut store, agent, 5, Some(cause_four.clone()));
    let rejected = store
        .advance_rule_cursor(
            "owner",
            &state_four,
            event_four,
            RuleCursorOutcome::Rejected {
                code: "render_error".to_owned(),
            },
            6,
        )
        .unwrap();
    assert_eq!(rejected.value.current_cursor, event_four);
    assert_eq!(rejected.events.len(), 1);
    assert_eq!(rejected.events[0].kind, "rule.rejected");
    assert_eq!(rejected.events[0].cause, Some(cause_four));

    let marker_seq = rejected.events[0].seq;
    let marker = store
        .advance_rule_cursor(
            "owner",
            &rejected.value,
            marker_seq,
            RuleCursorOutcome::Rejected {
                code: "render_error".to_owned(),
            },
            7,
        )
        .unwrap();
    assert_eq!(marker.value.current_cursor, marker_seq);
    assert!(marker.events.is_empty());
    assert_eq!(store.high_water().unwrap(), marker_seq);
}

#[test]
fn global_rule_guard_counts_all_principals_with_custom_windows() {
    assert_eq!(GLOBAL_RULE_ACTION_LIMIT, 120);
    assert_eq!(RULE_ACTION_WINDOW_MS, 60_000);

    let mut store = store();
    let now = 100_000;
    let action_at = now - RULE_ACTION_WINDOW_MS / 2;
    let custom_window_ms = RULE_ACTION_WINDOW_MS / 4;
    let (agent, agent_id) = insert_test_agent(&mut store);
    assert!(action_at >= now - RULE_ACTION_WINDOW_MS);
    assert!(action_at < now - custom_window_ms);

    for index in 0..GLOBAL_RULE_ACTION_LIMIT {
        let (principal, rule) = match index % 4 {
            0 => ("owner", "stop-rule"),
            1 => ("project:alpha", "alpha-rule"),
            2 => ("owner", "other-rule"),
            _ => ("project:beta", "beta-rule"),
        };
        insert_test_rule_action(&mut store, principal, rule, &agent_id, 1, action_at);
    }

    let action_ids_before = {
        let mut statement = store
            .connection
            .prepare(
                "SELECT id FROM commands
                     WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                     ORDER BY id",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(action_ids_before.len(), GLOBAL_RULE_ACTION_LIMIT as usize);

    let mut custom_definition = definition("stop-rule", true);
    let mut canonical_definition: Value =
        serde_json::from_str(&custom_definition.canonical_definition).unwrap();
    canonical_definition["limit"] = json!({
        "max": 121,
        "per": "rule",
        "window_ms": custom_window_ms,
    });
    custom_definition.canonical_definition = serde_json::to_string(&canonical_definition).unwrap();
    custom_definition.definition_digest =
        core::canonical_json_sha256(&canonical_definition).unwrap();

    let state = store
        .ensure_rule_state("owner", &custom_definition, now - 1)
        .unwrap();
    let event_seq = insert_test_turn_completed(&mut store, agent, now);
    let admission = store
        .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
        .unwrap();

    assert!(admission.is_new);
    assert!(admission.child.is_none());
    let rejected_parent = admission.parent.as_ref().expect("rejected rule parent");
    assert_eq!(rejected_parent.state, CommandState::Rejected);
    assert_eq!(
        rejected_parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert_eq!(
        rejected_parent.outcome.as_ref().unwrap()["code"],
        "rate_limited"
    );
    let rejected_parent_id = rejected_parent.id.clone();

    let replay = store
        .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.child.is_none());
    assert_eq!(
        replay.parent.as_ref().expect("retained rule parent").id,
        rejected_parent_id
    );

    let action_ids_after = {
        let mut statement = store
            .connection
            .prepare(
                "SELECT id FROM commands
                     WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL
                     ORDER BY id",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(action_ids_after, action_ids_before);
}

#[test]
fn finite_history_catch_up_replays_before_current_gates() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (agent, agent_id) = insert_test_agent(&mut store);
    let event_seq = insert_test_turn_completed(&mut store, agent, now - 1);
    let state = store
        .ensure_rule_state("owner", &definition("history-rule", true), now)
        .unwrap();
    let captured_through = state.current_cursor;
    assert!(state.retiring_revision.is_none());
    assert!(event_seq <= captured_through);

    let after_seq = event_seq - 1;
    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "history-rule",
        after_seq,
        Some(captured_through),
    );
    let actions = [RuleCatchUpAction::command(
        event_seq,
        stop_request(&agent_id),
    )];
    let first = store
        .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
        .unwrap();

    assert!(first.is_new);
    assert_eq!(first.parent.verb, "rule.catch_up");
    assert_eq!(first.parent.args["revision"].as_i64(), Some(state.revision));
    assert_eq!(first.children.len(), 1);
    let child = &first.children[0].command;
    assert_eq!(child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(child.expected_generation, Some(1));
    assert_eq!(child.generation, Some(1));
    let child_id = child.id.clone();
    let parent_id = first.parent.id.clone();

    let disabled_state = store
        .ensure_rule_state("owner", &definition("history-rule", false), now + 1)
        .unwrap();
    assert!(!disabled_state.enabled);
    assert!(disabled_state.revision > state.revision);
    let high_water_after_edit = store.high_water().unwrap();

    // Reuse the old view and exact request after the live definition changed.
    let replay = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &actions,
            now + 2,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.id, parent_id);
    assert_eq!(
        replay.parent.args["revision"].as_i64(),
        Some(state.revision)
    );
    assert_eq!(replay.children.len(), 1);
    assert_eq!(replay.children[0].command.id, child_id);
    assert!(replay.events.is_empty());
    assert_eq!(store.high_water().unwrap(), high_water_after_edit);

    let changed_request = core::api::RuleCatchUpRequest::new(
        request.id.clone(),
        "history-rule",
        after_seq,
        Some(captured_through + 1),
    );
    let conflict = store
        .admit_rule_catch_up(
            "owner",
            &changed_request,
            &disabled_state,
            captured_through + 1,
            &[],
            now + 3,
        )
        .unwrap_err();
    assert_eq!(conflict.code, "id_conflict");
    assert_eq!(store.high_water().unwrap(), high_water_after_edit);
}

#[test]
fn finite_history_catch_up_unknown_child_blocks_new_work() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (agent, agent_id) = insert_test_agent(&mut store);
    let event_seq = insert_test_turn_completed(&mut store, agent, now - 1);
    let state = store
        .ensure_rule_state("owner", &definition("history-rule", true), now)
        .unwrap();
    let captured_through = state.current_cursor;
    assert!(event_seq <= captured_through);

    insert_test_rule_action(&mut store, "owner", "history-rule", &agent_id, 1, now);
    let unknown_child_id: String = store
        .connection
        .query_row(
            "SELECT id FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        store
            .connection
            .execute(
                "UPDATE commands SET state = 'unknown' WHERE id = ?1",
                [&unknown_child_id],
            )
            .unwrap(),
        1
    );

    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "history-rule",
        event_seq - 1,
        Some(captured_through),
    );
    let actions = [RuleCatchUpAction::command(
        event_seq,
        stop_request(&agent_id),
    )];
    let commands_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();

    let error = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &actions,
            now + 1,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    let commands_after: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_after: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let unknown_state: String = store
        .connection
        .query_row(
            "SELECT state FROM commands WHERE id = ?1",
            [&unknown_child_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(commands_after, commands_before);
    assert_eq!(events_after, events_before);
    assert_eq!(store.high_water().unwrap(), high_water_before);
    assert_eq!(unknown_state, "unknown");
}

#[test]
fn finite_history_catch_up_uses_authoritative_coverage_floor() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (agent, _) = insert_test_agent(&mut store);
    let first_seq = insert_test_turn_completed(&mut store, agent, now - 1);
    let missing_seq = insert_test_turn_completed(&mut store, agent, now - 1);
    let last_seq = insert_test_turn_completed(&mut store, agent, now - 1);
    let state = store
        .ensure_rule_state("owner", &definition("history-rule", true), now)
        .unwrap();
    let captured_through = state.current_cursor;
    assert!(last_seq <= captured_through);

    assert_eq!(
        store
            .connection
            .execute(
                "DELETE FROM events WHERE seq IN (?1, ?2)",
                params![first_seq, missing_seq],
            )
            .unwrap(),
        2
    );
    let coverage_floor: i64 = store
        .connection
        .query_row(
            "SELECT pruned_through FROM rule_history_coverage WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(coverage_floor, 0);

    let sparse_request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "history-rule",
        0,
        Some(captured_through),
    );
    let sparse = store
        .admit_rule_catch_up("owner", &sparse_request, &state, captured_through, &[], now)
        .unwrap();
    assert!(sparse.is_new);
    assert!(sparse.children.is_empty());

    assert_eq!(
        store
            .connection
            .execute(
                "UPDATE rule_history_coverage SET pruned_through = ?1 WHERE singleton = 1",
                [missing_seq],
            )
            .unwrap(),
        1
    );
    let gap_id = Ulid::new();
    let gap_now = i64::try_from(gap_id.timestamp_ms()).unwrap() + 1;
    let gap_request = core::api::RuleCatchUpRequest::new(
        gap_id.to_string(),
        "history-rule",
        0,
        Some(captured_through),
    );
    let commands_before_gap: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_gap: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let high_water_before_gap = store.high_water().unwrap();

    let error = store
        .admit_rule_catch_up(
            "owner",
            &gap_request,
            &state,
            captured_through,
            &[],
            gap_now,
        )
        .unwrap_err();
    assert_eq!(error.code, "history_gap");
    let commands_after_gap: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_after_gap: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(commands_after_gap, commands_before_gap);
    assert_eq!(events_after_gap, events_before_gap);
    assert_eq!(store.high_water().unwrap(), high_water_before_gap);
}

#[test]
fn finite_history_catch_up_oversized_result_rolls_back() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (agent, agent_id) = insert_test_agent(&mut store);
    let action_count = RULE_ACTION_LIMIT as usize + 1;
    let mut event_seqs = Vec::with_capacity(action_count);
    for _ in 0..action_count {
        event_seqs.push(insert_test_turn_completed(&mut store, agent, now - 1));
    }

    let rule_id = "r".repeat(48 * 1024);
    let rule_definition = definition(&rule_id, true);
    let state = store
        .ensure_rule_state("owner", &rule_definition, now)
        .unwrap();
    let captured_through = state.current_cursor;
    assert!(
        event_seqs
            .last()
            .is_some_and(|seq| *seq <= captured_through)
    );
    assert!(rule_id.len() + 1024 < MAX_EVENT_BYTES);

    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        rule_id.clone(),
        0,
        Some(captured_through),
    );
    let action_request = stop_request(&agent_id);
    let actions = event_seqs
        .iter()
        .map(|event_seq| RuleCatchUpAction::command(*event_seq, action_request.clone()))
        .collect::<Vec<_>>();
    let encoded_actions = serde_json::to_vec(&actions).unwrap();
    assert!(encoded_actions.len() <= MAX_PAGE_BYTES);
    // Each history marker repeats the rule in both data and cause.
    assert!(actions.len() * 2 * rule_id.len() > MAX_PAGE_BYTES);

    let commands_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();

    let first_error =
        match store.admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
        {
            Err(error) => error,
            Ok(_) => panic!("oversized catch-up result was admitted"),
        };
    assert_eq!(first_error.code, "frame_too_large");
    assert!(first_error.message.contains("bounded response size"));
    let commands_after_first: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_after_first: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(commands_after_first, commands_before);
    assert_eq!(events_after_first, events_before);
    assert_eq!(store.high_water().unwrap(), high_water_before);

    let retry_error = match store.admit_rule_catch_up(
        "owner",
        &request,
        &state,
        captured_through,
        &actions,
        now + 1,
    ) {
        Err(error) => error,
        Ok(_) => panic!("oversized catch-up retry was admitted"),
    };
    assert_eq!(retry_error.code, "frame_too_large");
    assert!(retry_error.message.contains("bounded response size"));
    let commands_after_retry: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_after_retry: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(commands_after_retry, commands_before);
    assert_eq!(events_after_retry, events_before);
    assert_eq!(store.high_water().unwrap(), high_water_before);
}

#[test]
fn finite_history_catch_up_launch_page_overflow_rolls_back() {
    const ACTION_COUNT: usize = 8;

    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
    let workspace = std::env::temp_dir()
        .join("eliot-owner-catch-up-launch")
        .display()
        .to_string();
    let rule_id = "owner-catch-up-launch-page";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "command.accepted"},
        "do": [{"action": "launch", "profile": "fake", "workspace": workspace}],
        "limit": {
            "max": RULE_ACTION_LIMIT as u64,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        },
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let launch_text = "x".repeat(MAX_EVENT_BYTES - 4 * 1024);
    assert_eq!(launch_text.len(), 60 * 1024);
    assert!(launch_text.len() < MAX_TEXT_BYTES);
    assert!(ACTION_COUNT < RULE_ACTION_LIMIT as usize);
    assert!(ACTION_COUNT < GLOBAL_RULE_ACTION_LIMIT as usize);
    let pair_text_bytes = launch_text.len() * 2;
    assert!(
        pair_text_bytes + 16 * 1024 < MAX_PAGE_BYTES,
        "one Launch plus its linked SEND remains below the page bound"
    );
    assert!(
        pair_text_bytes * ACTION_COUNT > MAX_PAGE_BYTES,
        "all eight Launch+SEND pairs exceed the full admission-page bound"
    );

    let mut event_seqs = Vec::with_capacity(ACTION_COUNT);
    for _ in 0..ACTION_COUNT {
        event_seqs.push(insert_test_event(&mut store, "command.accepted", now - 1));
    }
    let state = store.ensure_rule_state("owner", &rule, now).unwrap();
    let captured_through = state.current_cursor;
    assert!(
        event_seqs
            .last()
            .is_some_and(|seq| *seq <= captured_through)
    );

    let actions = event_seqs
        .iter()
        .map(|event_seq| {
            let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
            launch.args = json!({
                "project": workspace,
                "harness": "codex",
                "profile": "fake",
                "profile_snapshot": {},
                "workspace": workspace,
                "text": launch_text,
            });
            launch.expected_generation = Some(0);
            assert!(valid_targetless_rule_launch_request(&launch));
            assert!(
                serde_json::to_vec(&launch).unwrap().len() <= MAX_FRAME_BYTES,
                "each targetless launch request remains inside its frame bound"
            );
            RuleCatchUpAction::command(*event_seq, launch)
        })
        .collect::<Vec<_>>();
    let encoded_actions = serde_json::to_vec(&actions).unwrap();
    assert!(
        encoded_actions.len() <= MAX_PAGE_BYTES,
        "the complete catch-up request remains inside its 900 KiB bound"
    );

    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        rule_id,
        event_seqs[0] - 1,
        Some(captured_through),
    );
    let commands_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let agents_before = store.agent_count().unwrap();
    let events_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();
    let cursor_before = state.current_cursor;

    let error = store
        .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
        .expect_err("oversized whole launch admission set must roll back");
    assert_eq!(error.code, "frame_too_large");
    assert!(
        error
            .message
            .contains("recovered rule admissions exceed the page byte bound"),
        "failure must come from the full catch-up admission-page check: {}",
        error.message
    );

    assert!(store.get_command(&request.id).unwrap().is_none());
    for event_seq in &event_seqs {
        let launch_id =
            derived_rule_id("action-0", "owner", rule_id, state.revision, *event_seq).unwrap();
        assert!(store.get_command(&launch_id).unwrap().is_none());
        assert!(
            store
                .get_command(&format!("{launch_id}/send"))
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before
    );
    assert_eq!(store.agent_count().unwrap(), agents_before);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        events_before
    );
    assert_eq!(store.high_water().unwrap(), high_water_before);
    assert_eq!(
        store
            .get_rule_state("owner", rule_id)
            .unwrap()
            .unwrap()
            .current_cursor,
        cursor_before
    );
    assert!(!store.is_write_faulted());
}

#[test]
fn finite_history_catch_up_allows_cursor_only_progress() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (agent, agent_id) = insert_test_agent(&mut store);
    let historical_event = insert_test_turn_completed(&mut store, agent, now - 1);
    let seeded_state = store
        .ensure_rule_state("owner", &definition("history-rule", true), now)
        .unwrap();
    let captured_through = seeded_state.current_cursor;
    assert!(seeded_state.retiring_revision.is_none());
    assert!(historical_event <= captured_through);

    let later_unmatched_event = insert_test_event(&mut store, "fixture.unmatched", now + 1);
    let advanced = store
        .advance_rule_cursor(
            "owner",
            &seeded_state,
            later_unmatched_event,
            RuleCursorOutcome::NoMatch,
            now + 2,
        )
        .unwrap();
    assert!(advanced.events.is_empty());
    assert_eq!(advanced.value.current_cursor, later_unmatched_event);
    assert!(advanced.value.current_cursor > seeded_state.current_cursor);
    assert_eq!(advanced.value.revision, seeded_state.revision);

    let after_seq = historical_event - 1;
    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "history-rule",
        after_seq,
        Some(captured_through),
    );
    let actions = [RuleCatchUpAction::command(
        historical_event,
        stop_request(&agent_id),
    )];
    let admission = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &seeded_state,
            captured_through,
            &actions,
            now + 3,
        )
        .unwrap();
    assert!(admission.is_new);
    assert_eq!(admission.parent.id, request.id);
    assert_eq!(admission.children.len(), 1);
    assert_eq!(
        admission.children[0].command.target.as_deref(),
        Some(agent_id.as_str())
    );
    assert_eq!(admission.children[0].command.expected_generation, Some(1));

    let current_state = store
        .get_rule_state("owner", "history-rule")
        .unwrap()
        .unwrap();
    assert_eq!(current_state.current_cursor, advanced.value.current_cursor);
    assert_eq!(current_state.revision, seeded_state.revision);

    let wrong_id = Ulid::new();
    let wrong_now = i64::try_from(wrong_id.timestamp_ms()).unwrap() + 1;
    let wrong_request = core::api::RuleCatchUpRequest::new(
        wrong_id.to_string(),
        "history-rule",
        after_seq,
        Some(captured_through),
    );
    let mut wrong_revision_state = current_state.clone();
    wrong_revision_state.revision += 1;
    let commands_before_wrong_revision: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_wrong_revision: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let high_water_before_wrong_revision = store.high_water().unwrap();

    let error = store
        .admit_rule_catch_up(
            "owner",
            &wrong_request,
            &wrong_revision_state,
            captured_through,
            &[],
            wrong_now,
        )
        .unwrap_err();
    assert_eq!(error.code, "bad_state");
    let commands_after_wrong_revision: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_after_wrong_revision: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        commands_after_wrong_revision,
        commands_before_wrong_revision
    );
    assert_eq!(events_after_wrong_revision, events_before_wrong_revision);
    assert_eq!(
        store.high_water().unwrap(),
        high_water_before_wrong_revision
    );
    assert_eq!(
        store
            .get_rule_state("owner", "history-rule")
            .unwrap()
            .unwrap(),
        current_state
    );
}

#[test]
fn catch_up_children_wait_for_predecessor_before_recovery_or_dispatch() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
    let (first_agent, first_target) = insert_test_agent(&mut store);
    let (second_agent, second_target) = insert_test_agent(&mut store);
    let first_event = insert_test_turn_completed(&mut store, first_agent, now - 1);
    let second_event = insert_test_turn_completed(&mut store, second_agent, now - 1);

    let mut rule_definition = definition("ordered-rule", true);
    let mut canonical_definition: Value =
        serde_json::from_str(&rule_definition.canonical_definition).unwrap();
    canonical_definition["limit"] = json!({
        "max": 2,
        "per": "rule",
        "window_ms": RULE_ACTION_WINDOW_MS,
    });
    rule_definition.canonical_definition = serde_json::to_string(&canonical_definition).unwrap();
    rule_definition.definition_digest = core::canonical_json_sha256(&canonical_definition).unwrap();
    let state = store
        .ensure_rule_state("owner", &rule_definition, now)
        .unwrap();
    let captured_through = state.current_cursor;
    assert!(second_event <= captured_through);

    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "ordered-rule",
        first_event - 1,
        Some(captured_through),
    );
    let actions = [
        RuleCatchUpAction::command(first_event, stop_request(&first_target)),
        RuleCatchUpAction::command(second_event, stop_request(&second_target)),
    ];
    let admission = store
        .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
        .unwrap();
    assert!(admission.is_new);
    assert_eq!(admission.children.len(), 2);
    assert_ne!(
        admission.children[0].command.target,
        admission.children[1].command.target
    );

    let first_child_id = admission.children[0].command.id.clone();
    let second_child_id = admission.children[1].command.id.clone();
    let first_link: (String, i64) = store
        .connection
        .query_row(
            "SELECT parent_command, ordinal FROM commands WHERE id = ?1",
            [&first_child_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let second_link: (String, i64) = store
        .connection
        .query_row(
            "SELECT parent_command, ordinal FROM commands WHERE id = ?1",
            [&second_child_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_link, (admission.parent.id.clone(), 0));
    assert_eq!(second_link, (admission.parent.id.clone(), 1));

    for (step, predecessor_state) in ["accepted", "queued", "dispatched", "unknown"]
        .into_iter()
        .enumerate()
    {
        match predecessor_state {
            "accepted" => {}
            "queued" => {
                assert_eq!(
                    store
                        .connection
                        .execute(
                            "UPDATE commands SET state = 'queued' WHERE id = ?1",
                            [&first_child_id],
                        )
                        .unwrap(),
                    1
                );
            }
            "dispatched" => {
                assert_eq!(
                    store
                        .dispatch(&first_child_id, now + 10 + step as i64)
                        .unwrap()
                        .value
                        .state,
                    CommandState::Dispatched
                );
            }
            "unknown" => {
                assert_eq!(
                    store
                        .settle(
                            &first_child_id,
                            CommandState::Unknown,
                            Some(json!({"reason": "fixture uncertainty"})),
                            now + 10 + step as i64,
                        )
                        .unwrap()
                        .value
                        .state,
                    CommandState::Unknown
                );
            }
            _ => unreachable!(),
        }

        let expected_state = match predecessor_state {
            "accepted" => CommandState::Accepted,
            "queued" => CommandState::Queued,
            "dispatched" => CommandState::Dispatched,
            "unknown" => CommandState::Unknown,
            _ => unreachable!(),
        };
        assert_eq!(
            store.get_command(&first_child_id).unwrap().unwrap().state,
            expected_state
        );

        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            !recoverable
                .iter()
                .any(|command| command.command.id == second_child_id),
            "child 2 became recoverable while child 1 was {predecessor_state}"
        );
        let held = store
            .dispatch(&second_child_id, now + 20 + step as i64)
            .unwrap();
        assert_eq!(
            held.value.state,
            CommandState::Queued,
            "child 2 was not held while child 1 was {predecessor_state}"
        );
        assert!(
            !held
                .events
                .iter()
                .any(|event| event.kind == "command.dispatched")
        );
        if predecessor_state == "accepted" {
            let queued = held
                .events
                .iter()
                .find(|event| event.kind == "command.queued")
                .expect("held child emits its queued transition");
            assert_eq!(queued.data["reason"], "rule_action_predecessor_pending");
        } else {
            assert!(held.events.is_empty());
        }
        assert_eq!(
            store.get_command(&second_child_id).unwrap().unwrap().state,
            CommandState::Queued
        );
        let child_two_dispatches: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events
                     WHERE kind = 'command.dispatched'
                       AND json_extract(data, '$.command_id') = ?1",
                [&second_child_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_two_dispatches, 0);
    }

    assert_eq!(
        store
            .settle(
                &first_child_id,
                CommandState::Applied,
                Some(json!({"result": "stopped"})),
                now + 30,
            )
            .unwrap()
            .value
            .state,
        CommandState::Applied
    );
    let recoverable = store.recover_rule_children(None, 100).unwrap();
    assert!(
        recoverable
            .iter()
            .any(|command| command.command.id == second_child_id)
    );
    assert_eq!(
        store
            .dispatch(&second_child_id, now + 31)
            .unwrap()
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn catch_up_terminal_predecessor_cancels_unstarted_sibling() {
    for terminal_predecessor in ["failed", "rejected", "cancelled"] {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 1;
        let (first_agent, first_target) = insert_test_agent(&mut store);
        let (second_agent, second_target) = insert_test_agent(&mut store);
        let first_event = insert_test_turn_completed(&mut store, first_agent, now - 1);
        let second_event = insert_test_turn_completed(&mut store, second_agent, now - 1);

        let mut rule_definition = definition("ordered-rule", true);
        let mut canonical_definition: Value =
            serde_json::from_str(&rule_definition.canonical_definition).unwrap();
        canonical_definition["limit"] = json!({
            "max": 2,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        });
        rule_definition.canonical_definition =
            serde_json::to_string(&canonical_definition).unwrap();
        rule_definition.definition_digest =
            core::canonical_json_sha256(&canonical_definition).unwrap();
        let state = store
            .ensure_rule_state("owner", &rule_definition, now)
            .unwrap();
        let captured_through = state.current_cursor;
        assert!(second_event <= captured_through);

        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "ordered-rule",
            first_event - 1,
            Some(captured_through),
        );
        let actions = [
            RuleCatchUpAction::command(first_event, stop_request(&first_target)),
            RuleCatchUpAction::command(second_event, stop_request(&second_target)),
        ];
        let admission = store
            .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
            .unwrap();
        assert_eq!(admission.children.len(), 2);
        let first_child_id = admission.children[0].command.id.clone();
        let second_child_id = admission.children[1].command.id.clone();

        let expected_predecessor_state = match terminal_predecessor {
            "failed" => {
                store.dispatch(&first_child_id, now + 1).unwrap();
                store
                    .settle(
                        &first_child_id,
                        CommandState::Failed,
                        Some(json!({"reason": "fixture failure"})),
                        now + 2,
                    )
                    .unwrap();
                CommandState::Failed
            }
            "rejected" => {
                store
                    .settle(
                        &first_child_id,
                        CommandState::Rejected,
                        Some(json!({"reason": "fixture rejection"})),
                        now + 1,
                    )
                    .unwrap();
                CommandState::Rejected
            }
            "cancelled" => {
                store.mark_lost(&first_target, 1, now + 1).unwrap();
                CommandState::Cancelled
            }
            _ => unreachable!(),
        };
        assert_eq!(
            store.get_command(&first_child_id).unwrap().unwrap().state,
            expected_predecessor_state
        );

        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .any(|command| command.command.id == second_child_id),
            "Store recovery must surface the terminal follower for cancellation"
        );
        let cancelled = store.dispatch(&second_child_id, now + 3).unwrap();
        assert_eq!(cancelled.value.state, CommandState::Cancelled);
        let cancelled_event = cancelled
            .events
            .iter()
            .find(|event| event.kind == "command.cancelled")
            .expect("Store gate cancels the unstarted follower");
        assert_eq!(
            cancelled_event.data["reason"],
            "rule_action_predecessor_terminal"
        );
        let recoverable_after_cancel = store.recover_rule_children(None, 100).unwrap();
        assert!(
            !recoverable_after_cancel
                .iter()
                .any(|command| command.command.id == second_child_id)
        );
        assert_eq!(
            store.get_command(&second_child_id).unwrap().unwrap().state,
            CommandState::Cancelled,
            "child 2 was not cancelled after child 1 became {terminal_predecessor}"
        );
        let child_two_dispatches: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events
                     WHERE kind = 'command.dispatched'
                       AND json_extract(data, '$.command_id') = ?1",
                [&second_child_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_two_dispatches, 0);
    }
}

#[test]
fn catch_up_targetless_predecessor_rejection_cancels_follower() {
    let mut store = store();
    let request_id = Ulid::new();
    let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
    let first_event = insert_test_event(&mut store, "var.changed", now - 2);
    let second_event = insert_test_event(&mut store, "var.changed", now - 1);
    let rule = definition("targetless-sibling-cancel", true);
    let state = store.ensure_rule_state("owner", &rule, now).unwrap();
    let captured_through = state.current_cursor;
    assert!(second_event <= captured_through);

    let mut first_request = SubmitCommand::new("", "var.set");
    first_request.args = json!({"scope": "global", "key": "sibling-key", "value": 1});
    let mut second_request = SubmitCommand::new("", "var.set");
    second_request.args = json!({"scope": "global", "key": "sibling-key", "value": 2});
    let request = core::api::RuleCatchUpRequest::new(
        request_id.to_string(),
        "targetless-sibling-cancel",
        first_event - 1,
        Some(captured_through),
    );
    let actions = [
        RuleCatchUpAction::command(first_event, first_request),
        RuleCatchUpAction::command(second_event, second_request),
    ];
    let admission = store
        .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
        .unwrap();
    assert_eq!(admission.children.len(), 2);
    let first_child_id = admission.children[0].command.id.clone();
    let second_child_id = admission.children[1].command.id.clone();
    assert_eq!(admission.children[0].command.verb, "var.set");
    assert_eq!(admission.children[1].command.verb, "var.set");
    assert!(admission.children[1].command.target.is_none());

    let held = store.dispatch(&second_child_id, now + 1).unwrap();
    assert_eq!(held.value.state, CommandState::Queued);
    let queued_event = held
        .events
        .iter()
        .find(|event| event.kind == "command.queued")
        .expect("pending predecessor queues the targetless follower");
    assert_eq!(
        queued_event.data["reason"],
        "rule_action_predecessor_pending"
    );

    store
        .settle(
            &first_child_id,
            CommandState::Rejected,
            Some(json!({"reason": "version_conflict"})),
            now + 2,
        )
        .unwrap();

    let recoverable = store.recover_rule_children(None, 100).unwrap();
    assert!(
        recoverable
            .iter()
            .any(|command| command.command.id == second_child_id),
        "Store recovery must surface the targetless follower for cancellation"
    );
    let cancelled = store.dispatch(&second_child_id, now + 3).unwrap();
    assert_eq!(cancelled.value.state, CommandState::Cancelled);
    let cancelled_event = cancelled
        .events
        .iter()
        .find(|event| event.kind == "command.cancelled")
        .expect("Store gate cancels the targetless follower");
    assert_eq!(
        cancelled_event.data["reason"],
        "rule_action_predecessor_terminal"
    );
    assert_eq!(
        store.get_command(&second_child_id).unwrap().unwrap().state,
        CommandState::Cancelled,
    );
    let recoverable_after_cancel = store.recover_rule_children(None, 100).unwrap();
    assert!(
        !recoverable_after_cancel
            .iter()
            .any(|command| command.command.id == second_child_id)
    );
    let follower_dispatches: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events
                 WHERE kind = 'command.dispatched'
                   AND json_extract(data, '$.command_id') = ?1",
            [&second_child_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(follower_dispatches, 0);
}

#[test]
fn scheduled_and_manual_timer_store_paths_reject_resume_without_old_generation() {
    let mut store = store();
    let seed = Ulid::new();
    let now = i64::try_from(seed.timestamp_ms()).unwrap() + 10;
    let rule_id = "timer-resume-missing-generation";
    let rule = definition(rule_id, true);
    let rule_state = store.ensure_rule_state("owner", &rule, now).unwrap();
    let scheduled_at_ms = now + 1;
    let timer = store
        .ensure_timer_state(
            "owner",
            &TimerDefinitionInput {
                id: rule_id.to_owned(),
                schedule_revision: 1,
                schedule: core::api::TimerSchedule::Once {
                    at: scheduled_at_ms,
                },
                enabled: true,
                initial_next_due_at: Some(scheduled_at_ms),
            },
            now,
        )
        .unwrap();
    let mut scheduled_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    scheduled_resume.target = Some("a1".to_owned());
    let scheduled = store
        .commit_timer_slot(
            "owner",
            &timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: None,
                action: Some(scheduled_resume),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();
    let scheduled_parent = scheduled.parent.unwrap();
    assert_eq!(scheduled_parent.state, CommandState::Rejected);
    assert_eq!(
        scheduled_parent.outcome.as_ref().unwrap()["code"],
        "rule_action_binding_invalid"
    );
    assert!(scheduled.child.is_none());

    let manual = core::api::TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let mut manual_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    manual_resume.target = Some("a1".to_owned());
    let command_count_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();
    let error = store
        .admit_manual_timer_run(
            "owner",
            &manual,
            &scheduled.state,
            rule_state.revision,
            manual_resume,
            scheduled_at_ms + 1,
        )
        .unwrap_err();
    assert_eq!(error.code, "invalid");
    let command_count_after: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    assert_eq!(command_count_after, command_count_before);
    assert_eq!(store.high_water().unwrap(), high_water_before);
}

#[test]
fn ordinary_fake_resume_preserves_optional_expected_generation() {
    let mut store = store();
    let seed = Ulid::new();
    let now = i64::try_from(seed.timestamp_ms()).unwrap() + 20;
    let (agent_id, native_scope, native_ref) =
        ready_terminal_fake_agent(&mut store, "ordinary-fake-resume", now);
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    request.target = Some(agent_id.clone());
    let resume_id = request.id.clone();
    let admission = store.submit("owner", request, now + 5).unwrap();
    assert_eq!(admission.command.expected_generation, None);
    assert_eq!(admission.command.generation, Some(1));
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(
        store
            .dispatch(&resume_id, now + 6)
            .unwrap()
            .value
            .generation,
        Some(2)
    );
    crate::tests::install_fixture_sources(&mut store, &agent_id, &resume_id, 2, now + 6);
    store
        .put_batch(
            SourceBatch {
                batch_id: "ordinary-fake-resume:ready".to_owned(),
                agent_id: agent_id.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ordinary-fake-resume:ready".to_owned(),
                    data: json!({
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 7,
        )
        .unwrap();
    store
        .settle(&resume_id, CommandState::Applied, None, now + 8)
        .unwrap();
    let resumed = store.get_agent(&agent_id).unwrap().unwrap();
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.native_scope.as_deref(), Some(native_scope.as_str()));
    assert_eq!(resumed.native_ref.as_deref(), Some(native_ref.as_str()));
}

#[test]
fn live_fake_resume_reuses_subject_and_recovers_only_its_exact_linked_input() {
    let mut store = store();
    let seed = Ulid::new();
    let now = i64::try_from(seed.timestamp_ms()).unwrap() + 20;
    let (agent_id, native_scope, native_ref) =
        ready_terminal_fake_agent(&mut store, "live-fake-resume", now);
    let agent_num = parse_agent_id(&agent_id).unwrap();
    let rule_id = "live-fake-resume";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "turn.completed"},
        "do": [{
            "action": "resume",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}",
            "text": "new resume input",
        }],
        "limit": {
            "max": 1,
            "per": "agent",
            "window_ms": RULE_ACTION_WINDOW_MS,
        },
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let state = store.ensure_rule_state("owner", &rule, now + 5).unwrap();
    let event_seq = insert_test_turn_completed(&mut store, agent_num, now + 6);

    let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    request.target = Some(agent_id.clone());
    request.expected_generation = Some(1);
    request.args = json!({"text": "new resume input"});
    let admission = store
        .admit_rule_action("owner", &state, event_seq, request, now + 8)
        .unwrap();
    let resume = admission.child.as_ref().expect("resume rule child");
    let resume_id = resume.command.id.clone();
    assert_eq!(resume.command.verb, "resume");
    assert_eq!(resume.command.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(resume.command.expected_generation, Some(1));
    assert_eq!(resume.command.generation, Some(1));
    assert_eq!(resume.command.state, CommandState::Accepted);
    let linked = resume
        .linked_command
        .as_ref()
        .expect("resume text is stored as its linked SEND");
    let send_id = linked.id.clone();
    assert_eq!(send_id, format!("{resume_id}/send"));
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], "new resume input");
    let parent_args: String = store
        .connection
        .query_row(
            "SELECT args FROM commands WHERE id = ?1",
            [&resume_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&parent_args).unwrap(),
        json!({})
    );
    let link: (String, String, i64) = store
        .connection
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [&send_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(link, (resume_id.clone(), "send".to_owned(), 0));

    let limit = core::api::RuleLimit {
        max: 1,
        per: core::api::RuleLimitScope::Agent,
        window_ms: RULE_ACTION_WINDOW_MS,
    };
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (scoped_count, global_count) =
        rule_action_counts(&tx, "owner", rule_id, Some(&agent_id), &limit, now + 8).unwrap();
    tx.commit().unwrap();
    assert_eq!((scoped_count, global_count), (1, 1));

    let mut ordinary_future_send = SubmitCommand::new(Ulid::new().to_string(), "send");
    ordinary_future_send.target = Some(agent_id.clone());
    ordinary_future_send.expected_generation = Some(2);
    ordinary_future_send.args = json!({"text": "not a resume child"});
    assert_eq!(
        store
            .submit("owner", ordinary_future_send, now + 9)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    let mut competing_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    competing_resume.target = Some(agent_id.clone());
    competing_resume.expected_generation = Some(1);
    assert_eq!(
        store
            .submit("owner", competing_resume, now + 10)
            .unwrap_err()
            .code,
        "bad_state",
        "an unresolved resume holds this old generation",
    );

    assert_eq!(
        store.dispatch(&resume_id, now + 11).unwrap().value.state,
        CommandState::Dispatched
    );
    crate::tests::install_fixture_sources(&mut store, &agent_id, &resume_id, 2, now + 11);
    let starting = store.get_agent(&agent_id).unwrap().unwrap();
    assert_eq!(starting.generation, 2);
    assert_eq!(starting.state, AgentState::Starting);
    assert_eq!(
        starting.native_scope.as_deref(),
        Some(native_scope.as_str())
    );
    assert_eq!(starting.native_ref.as_deref(), Some(native_ref.as_str()));

    let high_water_before_mismatch = store.high_water().unwrap();
    let wrong_command = store
        .put_batch(
            SourceBatch {
                batch_id: "live-fake-resume:wrong-command".to_owned(),
                agent_id: agent_id.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "live-fake-resume:wrong-command".to_owned(),
                    data: json!({
                        "command_id": "not-the-resume-command",
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 12,
        )
        .unwrap_err();
    assert_eq!(wrong_command.code, "source_integrity");
    assert_eq!(store.high_water().unwrap(), high_water_before_mismatch);

    let mismatched = store
        .put_batch(
            SourceBatch {
                batch_id: "live-fake-resume:mismatched-ready".to_owned(),
                agent_id: agent_id.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "live-fake-resume:mismatched-ready".to_owned(),
                    data: json!({
                        "native_scope": "forged-scope",
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 12,
        )
        .unwrap_err();
    assert_eq!(mismatched.code, "source_integrity");
    assert_eq!(store.high_water().unwrap(), high_water_before_mismatch);
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Queued,
        "mismatched readiness keeps the future input held",
    );

    store
        .put_batch(
            SourceBatch {
                batch_id: "live-fake-resume:ready".to_owned(),
                agent_id: agent_id.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "live-fake-resume:ready".to_owned(),
                    data: json!({
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 13,
        )
        .unwrap();
    store
        .settle(&resume_id, CommandState::Applied, None, now + 14)
        .unwrap();

    let recovered = store.recover_rule_children(None, 100).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].command.id, resume_id);
    assert_eq!(recovered[0].command.state, CommandState::Applied);
    let recovered_input = recovered[0]
        .linked_command
        .as_ref()
        .expect("recovery exposes the still-queued input");
    assert_eq!(recovered_input.id, send_id);
    assert_eq!(recovered_input.state, CommandState::Queued);
    assert_eq!(
        store
            .recover_rule_children(Some(&resume_id), 100)
            .unwrap()
            .len(),
        0,
        "the resume ULID is the recovery page cursor",
    );

    let replay = store
        .replay_rule_event_action("owner", &admission.state, state.revision, event_seq)
        .unwrap()
        .expect("retained live resume action");
    let replayed_resume = replay.child.unwrap();
    assert_eq!(replayed_resume.command.id, resume_id);
    assert_eq!(replayed_resume.command.expected_generation, Some(1));
    assert_eq!(replayed_resume.command.generation, Some(2));
    assert_eq!(
        replayed_resume.linked_command.unwrap().id,
        send_id,
        "replay remains bound after the Agent advances to N+1",
    );

    assert_eq!(
        store.dispatch(&send_id, now + 15).unwrap().value.state,
        CommandState::Dispatched
    );
    let turn_id = format!("resume-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: "live-fake-resume:turn".to_owned(),
                agent_id: agent_id.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "live-fake-resume:turn-started".to_owned(),
                    data: json!({
                        "command_id": send_id.as_str(),
                        "turn_id": turn_id.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 16,
        )
        .unwrap();
    let applied_input = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(applied_input.state, CommandState::Applied);
    assert_eq!(applied_input.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(
        store
            .get_turn(&agent_id, 2, &turn_id)
            .unwrap()
            .unwrap()
            .state,
        TurnState::Running
    );

    let (second_agent, _, _) =
        ready_terminal_fake_agent(&mut store, "live-fake-resume-unknown", now + 30);
    let second_event =
        insert_test_turn_completed(&mut store, parse_agent_id(&second_agent).unwrap(), now + 35);
    let mut unknown_request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    unknown_request.target = Some(second_agent.clone());
    unknown_request.expected_generation = Some(1);
    unknown_request.args = json!({"text": "uncertain resume input"});
    let unknown_admission = store
        .admit_rule_action(
            "owner",
            &admission.state,
            second_event,
            unknown_request,
            now + 36,
        )
        .unwrap();
    let unknown = unknown_admission.child.unwrap();
    let unknown_id = unknown.command.id;
    let unknown_send_id = unknown.linked_command.unwrap().id;
    store.dispatch(&unknown_id, now + 37).unwrap();
    store
        .settle(
            &unknown_id,
            CommandState::Unknown,
            Some(json!({"reason": "resume outcome is uncertain"})),
            now + 38,
        )
        .unwrap();
    let recoverable = store.recover_rule_children(None, 100).unwrap();
    assert!(
        recoverable
            .iter()
            .all(|action| action.command.id != unknown_id),
        "an unknown resume is never replayed"
    );
    assert_eq!(
        store.get_command(&unknown_send_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        store.dispatch(&unknown_id, now + 39).unwrap_err().code,
        "bad_state"
    );
}

#[test]
fn resume_linked_send_cause_suppresses_the_same_rule_without_trusting_cause_label() {
    let mut store = store();
    let seed = Ulid::new();
    let now = i64::try_from(seed.timestamp_ms()).unwrap() + 20;
    let (agent_id, _, _) = ready_terminal_fake_agent(&mut store, "resume-self-cause", now);
    let agent_num = parse_agent_id(&agent_id).unwrap();
    let rule_id = "resume-self-cause";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "turn.completed"},
        "do": [{
            "action": "resume",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}",
            "text": "same-rule resume input",
        }],
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let state = store.ensure_rule_state("owner", &rule, now + 5).unwrap();
    let first_event = insert_test_turn_completed(&mut store, agent_num, now + 6);
    let mut first_request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    first_request.target = Some(agent_id.clone());
    first_request.expected_generation = Some(1);
    first_request.args = json!({"text": "same-rule resume input"});
    let first = store
        .admit_rule_action("owner", &state, first_event, first_request, now + 7)
        .unwrap();
    let first_child = first.child.as_ref().expect("admitted resume action");
    let send_id = first_child.linked_command.as_ref().unwrap().id.clone();
    let mut send_cause = command_cause_tx(&store.connection, &send_id)
        .unwrap()
        .expect("retained linked SEND cause");
    assert_eq!(send_cause.command_id, send_id);
    assert_eq!(send_cause.rule.as_deref(), Some(rule_id));
    assert_eq!(send_cause.depth, 1);
    send_cause.rule = Some("misleading-rule-label".to_owned());

    let second_event =
        insert_test_turn_completed_with_cause(&mut store, agent_num, now + 8, Some(send_cause));
    let mut second_request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    second_request.target = Some(agent_id);
    second_request.expected_generation = Some(1);
    second_request.args = json!({"text": "same-rule resume input"});
    let commands_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let suppressed = store
        .admit_rule_action("owner", &first.state, second_event, second_request, now + 9)
        .unwrap();
    assert!(suppressed.parent.is_none());
    assert!(suppressed.child.is_none());
    assert_eq!(suppressed.state.current_cursor, second_event);
    let commands_after: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    assert_eq!(commands_after, commands_before);
}

#[test]
fn catch_up_resume_siblings_wait_for_exact_linked_send_and_preserve_old_agent_on_cancel() {
    let rule_id = "catch-up-resume-siblings";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "turn.completed"},
        "do": [{
            "action": "resume",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}",
            "text": "resume from captured result",
        }],
        "limit": {
            "max": 2,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        },
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let resume_request = |agent: &str, text: &str| {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(1);
        request.args = json!({"text": text});
        request
    };
    let prepare_pair = || {
        let mut store = store();
        let seed = Ulid::new();
        let now = i64::try_from(seed.timestamp_ms()).unwrap() + 25;
        let (first_agent, first_scope, first_ref) =
            ready_terminal_fake_agent(&mut store, "catch-up-resume-first", now);
        let (second_agent, second_scope, second_ref) =
            ready_terminal_fake_agent(&mut store, "catch-up-resume-second", now + 10);
        let first_event =
            insert_test_turn_completed(&mut store, parse_agent_id(&first_agent).unwrap(), now + 21);
        let second_event = insert_test_turn_completed(
            &mut store,
            parse_agent_id(&second_agent).unwrap(),
            now + 22,
        );
        let state = store.ensure_rule_state("owner", &rule, now + 23).unwrap();
        let captured_through = state.current_cursor;
        assert!(second_event <= captured_through);
        let request = core::api::RuleCatchUpRequest::new(
            Ulid::new().to_string(),
            rule_id,
            first_event - 1,
            Some(captured_through),
        );
        let actions = [
            RuleCatchUpAction::command(first_event, resume_request(&first_agent, "first input")),
            RuleCatchUpAction::command(second_event, resume_request(&second_agent, "second input")),
        ];
        let admission = store
            .admit_rule_catch_up(
                "owner",
                &request,
                &state,
                captured_through,
                &actions,
                now + 23,
            )
            .unwrap();
        assert_eq!(admission.children.len(), 2);
        let first = &admission.children[0];
        let second = &admission.children[1];
        assert_eq!(first.command.verb, "resume");
        assert_eq!(second.command.verb, "resume");
        let first_resume_id = first.command.id.clone();
        let first_send_id = first.linked_command.as_ref().unwrap().id.clone();
        let second_resume_id = second.command.id.clone();
        let second_send_id = second.linked_command.as_ref().unwrap().id.clone();
        assert_eq!(first_send_id, format!("{first_resume_id}/send"));
        assert_eq!(second_send_id, format!("{second_resume_id}/send"));
        assert_eq!(first.command.expected_generation, Some(1));
        assert_eq!(first.command.generation, Some(1));
        assert_eq!(first.linked_command.as_ref().unwrap().generation, Some(2));
        assert_eq!(second.linked_command.as_ref().unwrap().generation, Some(2));
        (
            store,
            now + 24,
            first_resume_id,
            first_send_id,
            second_resume_id,
            second_send_id,
            first_agent,
            second_agent,
            first_scope,
            first_ref,
            second_scope,
            second_ref,
        )
    };
    let apply_first_resume = |store: &mut Store,
                              resume_id: &str,
                              agent: &str,
                              native_scope: &str,
                              native_ref: &str,
                              now: i64| {
        store.dispatch(resume_id, now).unwrap();
        crate::tests::install_fixture_sources(store, agent, resume_id, 2, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("resume-ready-{resume_id}"),
                    agent_id: agent.to_owned(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("resume-ready-{resume_id}"),
                        data: json!({
                            "native_scope": native_scope,
                            "native_ref": native_ref,
                        }),
                    }],
                    cursor: None,
                },
                now + 1,
            )
            .unwrap();
        store
            .settle(resume_id, CommandState::Applied, None, now + 2)
            .unwrap();
    };

    for input_state in ["accepted", "queued", "dispatched", "unknown"] {
        let (
            mut store,
            now,
            first_resume_id,
            first_send_id,
            second_resume_id,
            second_send_id,
            first_agent,
            second_agent,
            first_scope,
            first_ref,
            _second_scope,
            _second_ref,
        ) = prepare_pair();
        apply_first_resume(
            &mut store,
            &first_resume_id,
            &first_agent,
            &first_scope,
            &first_ref,
            now,
        );
        match input_state {
            "accepted" | "queued" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = ?1 WHERE id = ?2",
                        rusqlite::params![input_state, first_send_id],
                    )
                    .unwrap();
            }
            "dispatched" => {
                assert_eq!(
                    store.dispatch(&first_send_id, now + 3).unwrap().value.state,
                    CommandState::Dispatched
                );
            }
            "unknown" => {
                store.dispatch(&first_send_id, now + 3).unwrap();
                store
                    .settle(
                        &first_send_id,
                        CommandState::Unknown,
                        Some(json!({"reason": "input association is uncertain"})),
                        now + 4,
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let expected = match input_state {
            "accepted" => CommandState::Accepted,
            "queued" => CommandState::Queued,
            "dispatched" => CommandState::Dispatched,
            "unknown" => CommandState::Unknown,
            _ => unreachable!(),
        };
        assert_eq!(
            store.get_command(&first_send_id).unwrap().unwrap().state,
            expected
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .all(|action| action.command.id != second_resume_id),
            "successor resume recovered while predecessor input was {input_state}"
        );
        let held = store.dispatch(&second_resume_id, now + 10).unwrap();
        assert_eq!(held.value.state, CommandState::Queued);
        assert!(
            held.events
                .iter()
                .all(|event| event.kind != "command.dispatched")
        );
        let old_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(old_agent.generation, 1);
        assert_eq!(old_agent.state, AgentState::Stopped);
        let reservation: Option<i64> = store
            .connection
            .query_row(
                "SELECT capacity_generation FROM agents WHERE id = ?1",
                [parse_agent_id(&second_agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reservation, Some(2));
        assert_eq!(
            store.get_command(&second_send_id).unwrap().unwrap().state,
            CommandState::Queued
        );
    }

    {
        let (
            mut store,
            now,
            first_resume_id,
            first_send_id,
            second_resume_id,
            _second_send_id,
            first_agent,
            second_agent,
            first_scope,
            first_ref,
            _second_scope,
            _second_ref,
        ) = prepare_pair();
        apply_first_resume(
            &mut store,
            &first_resume_id,
            &first_agent,
            &first_scope,
            &first_ref,
            now,
        );
        store.dispatch(&first_send_id, now + 3).unwrap();
        let turn_id = Ulid::new().to_string();
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("resume-turn-{first_send_id}"),
                    agent_id: first_agent.clone(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("resume-turn-started-{first_send_id}"),
                        data: json!({
                            "command_id": first_send_id.as_str(),
                            "turn_id": turn_id.as_str(),
                        }),
                    }],
                    cursor: None,
                },
                now + 4,
            )
            .unwrap();
        let first_send = store.get_command(&first_send_id).unwrap().unwrap();
        assert_eq!(first_send.state, CommandState::Applied);
        assert_eq!(first_send.turn_id.as_deref(), Some(turn_id.as_str()));
        assert_eq!(
            store
                .get_turn(&first_agent, 2, &turn_id)
                .unwrap()
                .unwrap()
                .state,
            TurnState::Running
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .any(|action| action.command.id == second_resume_id)
        );
        assert_eq!(
            store
                .dispatch(&second_resume_id, now + 5)
                .unwrap()
                .value
                .state,
            CommandState::Dispatched,
            "exact input association releases the sibling before turn completion",
        );
        let next_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(next_agent.generation, 2);
        assert_eq!(next_agent.state, AgentState::Starting);
        assert_eq!(
            store
                .get_turn(&first_agent, 2, &turn_id)
                .unwrap()
                .unwrap()
                .state,
            TurnState::Running
        );
    }

    for terminal_state in ["rejected", "failed", "cancelled"] {
        let (
            mut store,
            now,
            first_resume_id,
            first_send_id,
            second_resume_id,
            second_send_id,
            first_agent,
            second_agent,
            first_scope,
            first_ref,
            second_scope,
            second_ref,
        ) = prepare_pair();
        apply_first_resume(
            &mut store,
            &first_resume_id,
            &first_agent,
            &first_scope,
            &first_ref,
            now,
        );
        match terminal_state {
            "rejected" => {
                store
                    .settle(
                        &first_send_id,
                        CommandState::Rejected,
                        Some(json!({"reason": "resume input rejected"})),
                        now + 3,
                    )
                    .unwrap();
            }
            "failed" => {
                store.dispatch(&first_send_id, now + 3).unwrap();
                store
                    .settle(
                        &first_send_id,
                        CommandState::Failed,
                        Some(json!({"reason": "resume input failed"})),
                        now + 4,
                    )
                    .unwrap();
            }
            "cancelled" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = 'cancelled' WHERE id = ?1",
                        [&first_send_id],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let cancelled = store.dispatch(&second_resume_id, now + 10).unwrap();
        assert_eq!(cancelled.value.state, CommandState::Cancelled);
        assert!(cancelled.events.iter().any(|event| {
            event.kind == "command.cancelled" && event.data["command_id"] == second_resume_id
        }));
        assert!(cancelled.events.iter().any(|event| {
            event.kind == "command.cancelled" && event.data["command_id"] == second_send_id
        }));
        assert!(
            !cancelled
                .events
                .iter()
                .any(|event| event.kind == "agent.failed")
        );
        assert_eq!(
            store.get_command(&second_send_id).unwrap().unwrap().state,
            CommandState::Cancelled
        );
        let old_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(old_agent.generation, 1);
        assert_eq!(old_agent.state, AgentState::Stopped);
        assert_eq!(
            old_agent.native_scope.as_deref(),
            Some(second_scope.as_str())
        );
        assert_eq!(
            old_agent.native_ref.as_deref(),
            Some(second_ref.as_str()),
            "cancelling an undispatched resume preserves the terminal native subject",
        );
        let reservation: Option<i64> = store
            .connection
            .query_row(
                "SELECT capacity_generation FROM agents WHERE id = ?1",
                [parse_agent_id(&second_agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            reservation, None,
            "resume cancellation releases N+1 capacity"
        );
    }
}

#[test]
fn catch_up_launch_sibling_gate_tracks_linked_send_and_cleans_terminal_successors() {
    let rule_id = "catch-up-launch-sibling";
    let workspace = "synthetic-catch-up-launch-sibling";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "command.accepted"},
        "do": [{"action": "launch", "profile": "fake", "workspace": workspace}],
        "limit": {
            "max": 2,
            "per": "rule",
            "window_ms": RULE_ACTION_WINDOW_MS,
        },
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let launch_request = |text: &str| {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
        request.args = json!({
            "project": workspace,
            "harness": "codex",
            "profile": "fake",
            "profile_snapshot": {},
            "workspace": workspace,
            "text": text,
        });
        request.expected_generation = Some(0);
        request
    };
    let prepare_pair = || {
        let mut store = store();
        let request_id = Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).unwrap() + 10;
        let first_event = insert_test_event(&mut store, "command.accepted", now - 1);
        let second_event = insert_test_event(&mut store, "command.accepted", now - 1);
        let state = store.ensure_rule_state("owner", &rule, now).unwrap();
        let captured_through = state.current_cursor;
        assert!(second_event <= captured_through);
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            rule_id,
            first_event - 1,
            Some(captured_through),
        );
        let actions = [
            RuleCatchUpAction::command(first_event, launch_request("first linked launch input")),
            RuleCatchUpAction::command(
                second_event,
                launch_request("successor linked launch input"),
            ),
        ];
        let admitted = store
            .admit_rule_catch_up("owner", &request, &state, captured_through, &actions, now)
            .unwrap();
        assert_eq!(admitted.children.len(), 2);
        let first = &admitted.children[0];
        let second = &admitted.children[1];
        assert_eq!(first.command.verb, "launch");
        assert_eq!(second.command.verb, "launch");
        let first_launch_id = first.command.id.clone();
        let first_send_id = first
            .linked_command
            .as_ref()
            .expect("first launch has its linked SEND")
            .id
            .clone();
        let second_launch_id = second.command.id.clone();
        let second_send_id = second
            .linked_command
            .as_ref()
            .expect("successor launch has its linked SEND")
            .id
            .clone();
        assert_eq!(first_send_id, format!("{first_launch_id}/send"));
        assert_eq!(second_send_id, format!("{second_launch_id}/send"));
        let first_agent = first.command.target.clone().expect("first launch Agent");
        let second_agent = second
            .command
            .target
            .clone()
            .expect("successor launch Agent");

        store.dispatch(&first_launch_id, now + 1).unwrap();
        crate::tests::install_fixture_sources(
            &mut store,
            &first_agent,
            &first_launch_id,
            1,
            now + 1,
        );
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("catch-up-first-launch-ready-{request_id}"),
                    agent_id: first_agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("catch-up-first-launch-ready-{request_id}"),
                        data: json!({"command_id": first_launch_id}),
                    }],
                    cursor: None,
                },
                now + 2,
            )
            .unwrap();
        store
            .settle(&first_launch_id, CommandState::Applied, None, now + 3)
            .unwrap();
        assert_eq!(
            store.get_command(&first_launch_id).unwrap().unwrap().state,
            CommandState::Applied
        );
        (
            store,
            now + 4,
            first_launch_id,
            first_send_id,
            second_launch_id,
            second_send_id,
            first_agent,
            second_agent,
        )
    };

    for input_state in ["accepted", "queued", "dispatched", "unknown"] {
        let (
            mut store,
            now,
            _first_launch_id,
            first_send_id,
            second_launch_id,
            second_send_id,
            _first_agent,
            second_agent,
        ) = prepare_pair();
        match input_state {
            "accepted" | "queued" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = ?1 WHERE id = ?2",
                        rusqlite::params![input_state, first_send_id],
                    )
                    .unwrap();
            }
            "dispatched" => {
                assert_eq!(
                    store.dispatch(&first_send_id, now).unwrap().value.state,
                    CommandState::Dispatched
                );
            }
            "unknown" => {
                store.dispatch(&first_send_id, now).unwrap();
                store
                    .settle(
                        &first_send_id,
                        CommandState::Unknown,
                        Some(json!({"reason": "fixture uncertainty"})),
                        now + 1,
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let expected_input_state = match input_state {
            "accepted" => CommandState::Accepted,
            "queued" => CommandState::Queued,
            "dispatched" => CommandState::Dispatched,
            "unknown" => CommandState::Unknown,
            _ => unreachable!(),
        };
        assert_eq!(
            store.get_command(&first_send_id).unwrap().unwrap().state,
            expected_input_state
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .all(|action| action.command.id != second_launch_id),
            "successor recovered while first Launch input was {input_state}"
        );
        let held = store.dispatch(&second_launch_id, now + 10).unwrap();
        assert_eq!(held.value.state, CommandState::Queued);
        assert!(
            !held
                .events
                .iter()
                .any(|event| event.kind == "command.dispatched")
        );
        assert_eq!(held.value.generation, Some(0));
        let second_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(second_agent.generation, 0);
        assert_eq!(second_agent.state, AgentState::Created);
        let reservation: Option<i64> = store
            .connection
            .query_row(
                "SELECT capacity_generation FROM agents WHERE id = ?1",
                [eliot_core::parse_agent_id(&second_agent.id).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reservation, Some(1));
        assert_eq!(
            store.get_command(&second_send_id).unwrap().unwrap().state,
            CommandState::Queued
        );
        let recoverable_after_hold = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable_after_hold
                .iter()
                .all(|action| action.command.id != second_launch_id)
        );
    }

    {
        let (
            mut store,
            now,
            _first_launch_id,
            first_send_id,
            second_launch_id,
            _second_send_id,
            first_agent,
            second_agent,
        ) = prepare_pair();
        let turn_id = Ulid::new().to_string();
        store.dispatch(&first_send_id, now).unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: first_agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({
                            "command_id": first_send_id,
                            "turn_id": turn_id,
                        }),
                    }],
                    cursor: None,
                },
                now + 1,
            )
            .unwrap();
        let first_send = store.get_command(&first_send_id).unwrap().unwrap();
        assert_eq!(first_send.state, CommandState::Applied);
        assert_eq!(first_send.turn_id.as_deref(), Some(turn_id.as_str()));
        assert_eq!(
            store
                .get_turn(&first_agent, 1, &turn_id)
                .unwrap()
                .unwrap()
                .state,
            TurnState::Running
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .any(|action| action.command.id == second_launch_id)
        );
        assert_eq!(
            store
                .dispatch(&second_launch_id, now + 2)
                .unwrap()
                .value
                .state,
            CommandState::Dispatched
        );
        let second_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(second_agent.generation, 1);
        assert_eq!(second_agent.state, AgentState::Starting);
        assert_eq!(
            store
                .get_turn(&first_agent, 1, &turn_id)
                .unwrap()
                .unwrap()
                .state,
            TurnState::Running
        );
    }

    for terminal_input_state in ["rejected", "failed", "cancelled"] {
        let (
            mut store,
            now,
            _first_launch_id,
            first_send_id,
            second_launch_id,
            second_send_id,
            _first_agent,
            second_agent,
        ) = prepare_pair();
        match terminal_input_state {
            "rejected" => {
                store
                    .settle(
                        &first_send_id,
                        CommandState::Rejected,
                        Some(json!({"reason": "fixture rejection"})),
                        now,
                    )
                    .unwrap();
            }
            "failed" => {
                store.dispatch(&first_send_id, now).unwrap();
                store
                    .settle(
                        &first_send_id,
                        CommandState::Failed,
                        Some(json!({"reason": "fixture failure"})),
                        now + 1,
                    )
                    .unwrap();
            }
            "cancelled" => {
                assert_eq!(
                    store
                        .connection
                        .execute(
                            "UPDATE commands SET state = 'cancelled' WHERE id = ?1",
                            [&first_send_id],
                        )
                        .unwrap(),
                    1
                );
            }
            _ => unreachable!(),
        }
        assert_eq!(
            store.get_command(&first_send_id).unwrap().unwrap().state,
            match terminal_input_state {
                "rejected" => CommandState::Rejected,
                "failed" => CommandState::Failed,
                "cancelled" => CommandState::Cancelled,
                _ => unreachable!(),
            }
        );
        let recoverable = store.recover_rule_children(None, 100).unwrap();
        assert!(
            recoverable
                .iter()
                .any(|action| action.command.id == second_launch_id),
            "terminal first input makes the undispatched successor recoverable"
        );
        let cancelled = store.dispatch(&second_launch_id, now + 10).unwrap();
        assert_eq!(cancelled.value.state, CommandState::Cancelled);
        assert!(cancelled.events.iter().any(|event| {
            event.kind == "command.cancelled" && event.data["command_id"] == second_launch_id
        }));
        assert!(cancelled.events.iter().any(|event| {
            event.kind == "command.cancelled" && event.data["command_id"] == second_send_id
        }));
        assert!(cancelled.events.iter().any(|event| {
            event.kind == "agent.failed"
                && event.data["command_id"] == second_launch_id
                && event.data["reason"] == "launch_not_dispatched"
        }));
        assert_eq!(
            store.get_command(&second_send_id).unwrap().unwrap().state,
            CommandState::Cancelled
        );
        let second_agent = store.get_agent(&second_agent).unwrap().unwrap();
        assert_eq!(second_agent.generation, 0);
        assert_eq!(second_agent.state, AgentState::Failed);
        assert!(second_agent.is_terminal());
        let reservation: Option<i64> = store
            .connection
            .query_row(
                "SELECT capacity_generation FROM agents WHERE id = ?1",
                [eliot_core::parse_agent_id(&second_agent.id).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reservation, None);
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM events
                         WHERE kind = 'command.dispatched'
                           AND json_extract(data, '$.command_id') = ?1",
                    [&second_launch_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
    }
}

#[test]
fn send_self_suppression_uses_verified_principal_and_rule_identity() {
    let mut store = store();
    let now = 20_000;
    let (agent, agent_id) = insert_test_agent(&mut store);
    ensure_test_project_action_scope(&mut store, "project:alpha", agent, 1, now);

    let owner_value = json!({"id": "same-rule", "enabled": true, "allow_self": false});
    let owner_definition = RuleDefinitionInput {
        id: "same-rule".to_owned(),
        canonical_definition: serde_json::to_string(&owner_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&owner_value).unwrap(),
        enabled: true,
    };
    let project_value = json!({
        "id": "same-rule",
        "enabled": true,
        "allow_self": false,
        "do": [{"action": "stop"}],
    });
    let project_definition = RuleDefinitionInput {
        id: "same-rule".to_owned(),
        canonical_definition: serde_json::to_string(&project_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&project_value).unwrap(),
        enabled: true,
    };
    let owner_state = store
        .ensure_rule_state("owner", &owner_definition, now)
        .unwrap();
    let project_state = store
        .ensure_rule_state("project:alpha", &project_definition, now)
        .unwrap();

    let project_event = insert_test_turn_completed(&mut store, agent, now + 1);
    let project_source = store
        .admit_rule_action(
            "project:alpha",
            &project_state,
            project_event,
            stop_request(&agent_id),
            now + 2,
        )
        .unwrap();
    let project_child = &project_source
        .child
        .as_ref()
        .expect("project rule child")
        .command;
    let project_cause = command_cause_tx(&store.connection, &project_child.id)
        .unwrap()
        .expect("Store-verified project rule cause");
    assert_eq!(project_cause.rule.as_deref(), Some("same-rule"));
    assert_eq!(project_cause.depth, 1);
    let project_origin: (String, String) = store
        .connection
        .query_row(
            "SELECT parent.principal, json_extract(parent.args, '$.rule')
                 FROM commands child JOIN commands parent ON parent.id = child.parent_command
                 WHERE child.id = ?1",
            [&project_child.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        project_origin,
        ("project:alpha".to_owned(), "same-rule".to_owned())
    );
    store
        .settle(
            &project_child.id,
            CommandState::Rejected,
            Some(json!({"reason": "source fixture complete"})),
            now + 3,
        )
        .unwrap();

    let cross_namespace_event =
        insert_test_turn_completed_with_cause(&mut store, agent, now + 4, Some(project_cause));
    let mut cross_namespace_send = SubmitCommand::new(Ulid::new().to_string(), "send");
    cross_namespace_send.target = Some(agent_id.clone());
    cross_namespace_send.expected_generation = Some(1);
    cross_namespace_send.args = json!({"text": "same rule ID from another namespace"});
    let cross_namespace = store
        .admit_rule_action(
            "owner",
            &owner_state,
            cross_namespace_event,
            cross_namespace_send,
            now + 5,
        )
        .unwrap();
    let foreign_send = &cross_namespace
        .child
        .as_ref()
        .expect("different principal is not a self send")
        .command;
    assert_eq!(foreign_send.verb, "send");
    assert_eq!(
        foreign_send.args["text"],
        "same rule ID from another namespace"
    );
    store
        .settle(
            &foreign_send.id,
            CommandState::Rejected,
            Some(json!({"reason": "close namespace fixture"})),
            now + 6,
        )
        .unwrap();

    let owner_source_event = insert_test_turn_completed(&mut store, agent, now + 7);
    let owner_source = store
        .admit_rule_action(
            "owner",
            &cross_namespace.state,
            owner_source_event,
            stop_request(&agent_id),
            now + 8,
        )
        .unwrap();
    let owner_child = &owner_source
        .child
        .as_ref()
        .expect("owner rule child")
        .command;
    let owner_cause = command_cause_tx(&store.connection, &owner_child.id)
        .unwrap()
        .expect("Store-verified owner rule cause");
    let owner_origin: (String, String) = store
        .connection
        .query_row(
            "SELECT parent.principal, json_extract(parent.args, '$.rule')
                 FROM commands child JOIN commands parent ON parent.id = child.parent_command
                 WHERE child.id = ?1",
            [&owner_child.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(owner_origin, ("owner".to_owned(), "same-rule".to_owned()));
    store
        .settle(
            &owner_child.id,
            CommandState::Rejected,
            Some(json!({"reason": "verified origin fixture"})),
            now + 9,
        )
        .unwrap();

    let own_event =
        insert_test_turn_completed_with_cause(&mut store, agent, now + 10, Some(owner_cause));
    let mut own_send = SubmitCommand::new(Ulid::new().to_string(), "send");
    own_send.target = Some(agent_id);
    own_send.expected_generation = Some(1);
    own_send.args = json!({"text": "verified self send"});
    let commands_before_suppression: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let high_water_before_suppression = store.high_water().unwrap();
    let suppressed = store
        .admit_rule_action("owner", &owner_source.state, own_event, own_send, now + 11)
        .unwrap();
    assert!(suppressed.is_new);
    assert!(suppressed.parent.is_none());
    assert!(suppressed.child.is_none());
    assert!(suppressed.events.is_empty());
    assert_eq!(suppressed.state.current_cursor, own_event);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_suppression
    );
    assert_eq!(store.high_water().unwrap(), high_water_before_suppression);
}

#[test]
fn allow_self_send_permits_cause_depth_through_five_only() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    let definition_value = json!({
        "id": "bounded-send",
        "enabled": true,
        "allow_self": true,
    });
    let rule_definition = RuleDefinitionInput {
        id: "bounded-send".to_owned(),
        canonical_definition: serde_json::to_string(&definition_value).unwrap(),
        definition_digest: core::canonical_json_sha256(&definition_value).unwrap(),
        enabled: true,
    };
    let mut state = store
        .ensure_rule_state("owner", &rule_definition, 10_000)
        .unwrap();
    let mut cause = None;
    let mut now = 10_001;

    for expected_depth in 1_u8..=4 {
        let event_seq =
            insert_test_turn_completed_with_cause(&mut store, agent, now, cause.clone());
        let admission = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now + 1)
            .unwrap();
        let child = admission.child.expect("caused stop child").command;
        let child_cause = command_cause_tx(&store.connection, &child.id)
            .unwrap()
            .expect("verified Store cause");
        assert_eq!(child_cause.rule.as_deref(), Some("bounded-send"));
        assert_eq!(child_cause.depth, expected_depth);
        store
            .settle(
                &child.id,
                CommandState::Rejected,
                Some(json!({"reason": "close bounded cause fixture"})),
                now + 2,
            )
            .unwrap();
        state = admission.state;
        cause = Some(child_cause);
        now += 3;
    }

    let send_event = insert_test_turn_completed_with_cause(&mut store, agent, now, cause);
    let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
    send.target = Some(agent_id.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text": "bounded self send"});
    let admission = store
        .admit_rule_action("owner", &state, send_event, send, now + 1)
        .unwrap();
    let send_child = &admission
        .child
        .as_ref()
        .expect("allow_self sends at depth five")
        .command;
    assert_eq!(send_child.verb, "send");
    assert_eq!(send_child.args["text"], "bounded self send");
    let send_cause = command_cause_tx(&store.connection, &send_child.id)
        .unwrap()
        .expect("verified depth-five send cause");
    assert_eq!(send_cause.rule.as_deref(), Some("bounded-send"));
    assert_eq!(send_cause.depth, 5);
    store
        .settle(
            &send_child.id,
            CommandState::Rejected,
            Some(json!({"reason": "close bounded send fixture"})),
            now + 2,
        )
        .unwrap();

    let sixth_event =
        insert_test_turn_completed_with_cause(&mut store, agent, now + 3, Some(send_cause));
    let commands_before_suppression: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let bounded = store
        .admit_rule_action(
            "owner",
            &admission.state,
            sixth_event,
            SubmitCommand {
                id: Ulid::new().to_string(),
                target: Some(agent_id),
                selector: None,
                verb: "send".to_owned(),
                args: json!({"text": "over depth bound"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 4,
        )
        .unwrap();
    assert!(bounded.is_new);
    assert!(bounded.parent.is_none());
    assert!(bounded.child.is_none());
    assert!(bounded.events.is_empty());
    assert_eq!(bounded.state.current_cursor, sixth_event);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_suppression
    );
}

#[test]
fn unknown_send_is_not_replayed_by_live_history_or_recovery() {
    let mut store = store();
    let now = 30_000;
    let (live_agent, live_agent_id) = insert_test_agent(&mut store);
    let (history_agent, history_agent_id) = insert_test_agent(&mut store);
    let live_definition = definition("live-unknown-send", true);
    let live_state = store
        .ensure_rule_state("owner", &live_definition, now)
        .unwrap();
    let live_event = insert_test_turn_completed(&mut store, live_agent, now + 1);
    let mut live_send = SubmitCommand::new(Ulid::new().to_string(), "send");
    live_send.target = Some(live_agent_id.clone());
    live_send.expected_generation = Some(1);
    live_send.args = json!({"text": "live uncertain send"});
    let live_admission = store
        .admit_rule_action("owner", &live_state, live_event, live_send.clone(), now + 2)
        .unwrap();
    let live_child = &live_admission
        .child
        .as_ref()
        .expect("live send child")
        .command;
    let live_child_id = live_child.id.clone();
    assert_eq!(live_child.verb, "send");
    store.dispatch(&live_child_id, now + 3).unwrap();
    store
        .settle(
            &live_child_id,
            CommandState::Unknown,
            Some(json!({"reason": "fixture uncertain send"})),
            now + 4,
        )
        .unwrap();
    assert_eq!(
        store.get_command(&live_child_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    let live_recovery = store.recover_rule_children(None, 100).unwrap();
    assert!(
        !live_recovery
            .iter()
            .any(|command| command.command.id == live_child_id)
    );

    let commands_before_live_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_live_replay = store.high_water().unwrap();
    let live_replay = store
        .admit_rule_action("owner", &live_state, live_event, live_send, now + 5)
        .unwrap();
    assert!(!live_replay.is_new);
    assert_eq!(
        live_replay.parent.unwrap().id,
        live_admission.parent.unwrap().id
    );
    assert_eq!(live_replay.child.unwrap().command.id, live_child_id);
    assert!(live_replay.events.is_empty());
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_live_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_live_replay);

    let history_event = insert_test_turn_completed(&mut store, history_agent, now + 6);
    let history_state = store
        .ensure_rule_state("owner", &definition("history-unknown-send", true), now + 7)
        .unwrap();
    let history_id = Ulid::new();
    let history_now = i64::try_from(history_id.timestamp_ms()).unwrap() + 1;
    let history_request = core::api::RuleCatchUpRequest::new(
        history_id.to_string(),
        "history-unknown-send",
        history_event - 1,
        Some(history_state.current_cursor),
    );
    let mut history_send = SubmitCommand::new(Ulid::new().to_string(), "send");
    history_send.target = Some(history_agent_id);
    history_send.expected_generation = Some(1);
    history_send.args = json!({"text": "history uncertain send"});
    let history_actions = [RuleCatchUpAction::command(history_event, history_send)];
    let history_admission = store
        .admit_rule_catch_up(
            "owner",
            &history_request,
            &history_state,
            history_state.current_cursor,
            &history_actions,
            history_now,
        )
        .unwrap();
    assert_eq!(history_admission.children.len(), 1);
    let history_child_id = history_admission.children[0].command.id.clone();
    store.dispatch(&history_child_id, history_now + 1).unwrap();
    store
        .settle(
            &history_child_id,
            CommandState::Unknown,
            Some(json!({"reason": "fixture uncertain history send"})),
            history_now + 2,
        )
        .unwrap();
    let commands_before_history_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_history_replay = store.high_water().unwrap();
    let history_replay = store
        .replay_rule_catch_up("owner", &history_request)
        .unwrap()
        .expect("retained catch-up parent");
    assert!(!history_replay.is_new);
    assert_eq!(history_replay.parent.id, history_request.id);
    assert_eq!(history_replay.children.len(), 1);
    assert_eq!(history_replay.children[0].command.id, history_child_id);
    assert_eq!(
        history_replay.children[0].command.state,
        CommandState::Unknown
    );
    assert!(history_replay.events.is_empty());

    let recovered = store.recover_rule_children(None, 100).unwrap();
    assert!(!recovered.iter().any(|command| {
        command.command.id == live_child_id || command.command.id == history_child_id
    }));
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_history_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_history_replay);
}

#[test]
fn per_agent_limit_is_scoped_by_principal_and_rule_across_generations() {
    let mut store = store();
    let now = 10_000;
    let (alpha_agent_num, alpha_agent) = insert_test_agent(&mut store);
    let (_, beta_agent) = insert_test_agent(&mut store);
    let owner_rule = store
        .ensure_rule_state("owner", &definition("same-rule", true), now - 3)
        .unwrap();
    let owner_event = insert_test_turn_completed(&mut store, alpha_agent_num, now - 2);
    store
        .admit_rule_action(
            "owner",
            &owner_rule,
            owner_event,
            stop_request(&alpha_agent),
            now - 1,
        )
        .unwrap();
    insert_test_rule_action(
        &mut store,
        "project:alpha",
        "same-rule",
        &alpha_agent,
        1,
        now - 3,
    );
    store
        .connection
        .execute(
            "UPDATE agents SET generation = 2 WHERE id = ?1",
            [alpha_agent_num],
        )
        .unwrap();
    insert_test_rule_action(
        &mut store,
        "project:alpha",
        "same-rule",
        &alpha_agent,
        2,
        now - 2,
    );
    insert_test_rule_action(
        &mut store,
        "project:alpha",
        "other-rule",
        &alpha_agent,
        2,
        now - 1,
    );
    insert_test_rule_action(&mut store, "project:beta", "same-rule", &beta_agent, 1, now);

    let limit = core::api::RuleLimit {
        max: 2,
        per: core::api::RuleLimitScope::Agent,
        window_ms: RULE_ACTION_WINDOW_MS,
    };
    let tx = store
        .connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let (same_rule, global) = rule_action_counts(
        &tx,
        "project:alpha",
        "same-rule",
        Some(&alpha_agent),
        &limit,
        now,
    )
    .unwrap();
    let (other_rule, _) = rule_action_counts(
        &tx,
        "project:alpha",
        "other-rule",
        Some(&alpha_agent),
        &limit,
        now,
    )
    .unwrap();
    let (other_project, _) = rule_action_counts(
        &tx,
        "project:beta",
        "same-rule",
        Some(&beta_agent),
        &limit,
        now,
    )
    .unwrap();
    tx.rollback().unwrap();

    assert_eq!(same_rule, 2);
    assert!(rule_limit_reached(same_rule, limit.max));
    assert_eq!(other_rule, 1);
    assert!(!rule_limit_reached(other_rule, limit.max));
    assert_eq!(other_project, 1);
    assert!(!rule_limit_reached(other_project, limit.max));
    assert_eq!(global, 5);
}

#[test]
fn malformed_legacy_command_cause_is_unbound_not_a_read_failure() {
    let mut store = store();
    let (agent, agent_id) = insert_test_agent(&mut store);
    let state = store
        .ensure_rule_state("owner", &definition("stop-rule", true), 1)
        .unwrap();
    let event_seq = insert_test_turn_completed(&mut store, agent, 2);
    let admission = store
        .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), 3)
        .unwrap();
    let child = admission.child.unwrap().command;
    store
        .connection
        .execute(
            "UPDATE commands SET cause = 'legacy-untyped-value' WHERE id = ?1",
            [&child.id],
        )
        .unwrap();

    assert_eq!(
        command_cause_tx(&store.connection, &child.id).unwrap(),
        None
    );
    assert!(
        store
            .get_rule_state("owner", "stop-rule")
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store
            .list_rule_processing_states("owner", None, 10)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn coordination_owner_and_project_children_dispatch_apply_and_replay_once() {
    for principal in ["owner", "project:project-test"] {
        for verb in ["mail.send", "var.set"] {
            let mut store = store();
            let (agent_num, agent_id) = insert_test_agent(&mut store);
            let now = 1_800_000_000_000;
            if principal != "owner" {
                store
                    .upsert_project_registration(
                        "owner",
                        &ProjectRegistrationInput {
                            id: "project-test".to_owned(),
                            checkout: "registered-checkout".to_owned(),
                            repo_common_dir: "registered-common".to_owned(),
                            git: None,
                            profiles: Vec::new(),
                            actions: vec![verb.to_owned()],
                            interpreters: std::collections::BTreeMap::new(),
                            run_env: Vec::new(),
                        },
                        now,
                    )
                    .unwrap();
            }
            let args = if verb == "mail.send" {
                json!({"to": [agent_id], "text": "completed"})
            } else {
                json!({"scope": "project:project-test", "key": "result", "value": "done", "expect_version": 0})
            };
            let mut action = args.clone();
            action["action"] = json!(verb);
            let value = json!({"id": "coordination", "enabled": true, "do": [action]});
            let input = RuleDefinitionInput {
                id: "coordination".to_owned(),
                canonical_definition: serde_json::to_string(&value).unwrap(),
                definition_digest: core::canonical_json_sha256(&value).unwrap(),
                enabled: true,
            };
            let state = store.ensure_rule_state(principal, &input, now).unwrap();
            let seq = insert_test_turn_completed(&mut store, agent_num, now);
            let mut request = SubmitCommand::new("", verb);
            request.args = args;
            let admission = store
                .admit_rule_action(principal, &state, seq, request.clone(), now)
                .unwrap();
            let child = admission
                .child
                .as_ref()
                .expect("coordination child")
                .command
                .clone();
            let dispatched = store.dispatch(&child.id, now + 1).unwrap();
            assert_eq!(dispatched.value.state, CommandState::Dispatched);
            assert_eq!(dispatched.events.last().unwrap().kind, "command.dispatched");
            let apply = |store: &mut Store| {
                if verb == "mail.send" {
                    store.commit_mail_send_applied(
                        &child.id,
                        core::api::MailLimits::default(),
                        now + 2,
                    )
                } else {
                    store.commit_var_set_applied(&child.id, now + 2)
                }
                .unwrap()
            };
            let applied = apply(&mut store);
            assert_eq!(applied.value.state, CommandState::Applied);
            assert!(applied.events.iter().all(|event| {
                event
                    .cause
                    .as_ref()
                    .is_some_and(|cause| cause.command_id == child.id && cause.depth == 1)
            }));
            if verb == "mail.send" {
                let delivery =
                    applied.value.outcome.as_ref().unwrap()["deliveries"][0]["delivery_id"]
                        .as_str()
                        .unwrap();
                let head = delivery
                    .split_once('/')
                    .unwrap()
                    .0
                    .strip_prefix("rule:")
                    .unwrap();
                assert_eq!(ulid_timestamp_ms(head), Some(now));
                assert_eq!(
                    store
                        .connection
                        .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                            .get::<_, i64>(0))
                        .unwrap(),
                    1
                );
            } else {
                assert_eq!(applied.events[0].kind, "var.changed");
                assert_eq!(applied.events[1].kind, "command.applied");
                assert_eq!(applied.value.outcome.as_ref().unwrap()["version"], 1);
            }
            let replay = store
                .admit_rule_action(principal, &state, seq, request, now + 3)
                .unwrap();
            assert!(!replay.is_new);
            assert_eq!(replay.child.unwrap().command.id, child.id);
            assert!(replay.events.is_empty());
            let repeated = apply(&mut store);
            assert_eq!(repeated.value, applied.value);
            assert!(repeated.events.is_empty());
            assert_eq!(
                store
                    .connection
                    .query_row("SELECT COUNT(*) FROM writes", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    }
}

#[test]
fn coordination_var_changed_preserves_cause_through_depth_five() {
    let mut store = store();
    let mut state = store
        .ensure_rule_state("owner", &definition("var-loop", true), 10)
        .unwrap();
    let mut seq = insert_test_event(&mut store, "var.changed", 11);
    for depth in 1..=6 {
        let mut request = SubmitCommand::new("", "var.set");
        request.args = json!({"scope": "global", "key": "loop", "value": depth});
        let admission = store
            .admit_rule_action("owner", &state, seq, request, 12 + i64::from(depth))
            .unwrap();
        state = admission.state;
        if depth == 6 {
            assert!(admission.parent.is_none());
            assert!(admission.child.is_none());
            assert!(admission.events.is_empty());
            break;
        }
        let child = admission.child.unwrap().command;
        store.dispatch(&child.id, 20).unwrap();
        let applied = store.commit_var_set_applied(&child.id, 21).unwrap();
        let event = &applied.events[0];
        assert_eq!(event.kind, "var.changed");
        let cause = event.cause.as_ref().expect("verified variable producer");
        assert_eq!(cause.command_id, child.id);
        assert_eq!(cause.depth, depth);
        seq = event.seq;
    }
    assert_eq!(
        store
            .connection
            .query_row("SELECT version FROM vars WHERE key = 'loop'", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        5
    );
}

#[test]
fn coordination_rule_mail_ack_survives_pruned_delivery() {
    let mut store = store();
    let (agent_num, agent_id) = insert_test_agent(&mut store);
    let now = 1_800_000_000_000;
    let state = store
        .ensure_rule_state("owner", &definition("mail-result", true), now)
        .unwrap();
    let seq = insert_test_turn_completed(&mut store, agent_num, now);
    let mut request = SubmitCommand::new("", "mail.send");
    request.args = json!({"to": [agent_id], "text": "completed"});
    let child = store
        .admit_rule_action("owner", &state, seq, request, now)
        .unwrap()
        .child
        .unwrap()
        .command;
    store.dispatch(&child.id, now).unwrap();
    let applied = store
        .commit_mail_send_applied(&child.id, core::api::MailLimits::default(), now)
        .unwrap();
    let delivery = applied.value.outcome.unwrap()["deliveries"][0]["delivery_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let ack = core::api::MailAckRequest {
        id: Ulid::from_parts(now as u64, 1).to_string(),
        agent: Some(agent_id),
        deliveries: vec![delivery],
    };
    store.mail_ack("owner", &ack, now).unwrap();
    let later = now + 31 * 24 * 60 * 60 * 1_000;
    assert_eq!(
        store
            .prune_retention(None, None, Some(30), later)
            .unwrap()
            .mail_deleted,
        1
    );
    let fresh_ack = core::api::MailAckRequest {
        id: Ulid::from_parts(later as u64, 2).to_string(),
        ..ack
    };
    assert_eq!(
        store
            .mail_ack("owner", &fresh_ack, later)
            .unwrap()
            .value
            .acked,
        fresh_ack.deliveries
    );
}

#[test]
fn stop_admission_counts_only_children_and_recovers_undispatched_children() {
    let mut store = store();
    let (agent_num, agent_id) = insert_test_agent(&mut store);
    let mut state = store
        .ensure_rule_state("owner", &definition("stop-rule", true), 10_000)
        .unwrap();
    let now = 10_000;

    for _ in 0..RULE_ACTION_LIMIT {
        let event_seq = insert_test_turn_completed(&mut store, agent_num, now);
        let admission = store
            .admit_rule_action("owner", &state, event_seq, stop_request(&agent_id), now)
            .unwrap();
        assert!(admission.is_new);
        let child = &admission
            .child
            .as_ref()
            .expect("admitted stop child")
            .command;
        assert_eq!(child.verb, "stop");
        let parent = admission.parent.as_ref().expect("admitted rule parent");
        assert_eq!(parent.state, CommandState::Applied);
        assert_eq!(parent.outcome.as_ref().unwrap()["children"][0], child.id);
        let child_event = admission
            .events
            .iter()
            .find(|event| {
                event.kind == "command.accepted"
                    && event.data["command_id"].as_str() == Some(child.id.as_str())
            })
            .expect("child accepted event");
        assert_eq!(
            child_event.cause,
            Some(core::EventCause {
                command_id: child.id.clone(),
                rule: Some("stop-rule".to_owned()),
                remote_call: None,
                depth: 1,
            })
        );
        assert_eq!(
            command_cause_tx(&store.connection, &child.id)
                .unwrap()
                .as_ref(),
            child_event.cause.as_ref()
        );
        let child_event_cause: Option<String> = store
            .connection
            .query_row(
                "SELECT cause FROM events WHERE seq = ?1",
                [child_event.seq],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            decode_event_cause(child_event_cause.as_deref(), Some(&child.id)).as_ref(),
            child_event.cause.as_ref()
        );
        let parent_event = admission
            .events
            .iter()
            .find(|event| event.data["command_id"].as_str() == Some(parent.id.as_str()))
            .expect("parent command event");
        assert_eq!(
            parent_event.cause,
            Some(core::EventCause {
                command_id: parent.id.clone(),
                rule: Some("stop-rule".to_owned()),
                remote_call: None,
                depth: 1,
            })
        );
        assert_eq!(
            command_cause_tx(&store.connection, &parent.id)
                .unwrap()
                .as_ref(),
            parent_event.cause.as_ref()
        );
        let fired = admission
            .events
            .iter()
            .find(|event| event.kind == "rule.fired")
            .expect("committed rule firing event");
        assert_eq!(fired.data["parent_command"], parent.id);
        assert_eq!(fired.data["child_command"], child.id);
        assert_eq!(fired.cause.as_ref(), parent_event.cause.as_ref());
        state = admission.state;
    }

    let rate_event_seq = insert_test_turn_completed(&mut store, agent_num, now);
    let limited = store
        .admit_rule_action(
            "owner",
            &state,
            rate_event_seq,
            stop_request(&agent_id),
            now,
        )
        .unwrap();
    assert!(limited.is_new);
    assert!(limited.child.is_none());
    let rejected_parent = limited.parent.as_ref().expect("rejected rule parent");
    assert_eq!(rejected_parent.state, CommandState::Rejected);
    assert_eq!(
        rejected_parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert_eq!(
        rejected_parent.outcome.as_ref().unwrap()["code"],
        "rate_limited"
    );
    let rejected = limited
        .events
        .iter()
        .find(|event| event.kind == "rule.rejected")
        .expect("committed rejection event");
    assert_eq!(
        rejected
            .cause
            .as_ref()
            .map(|cause| cause.command_id.as_str()),
        Some(rejected_parent.id.as_str())
    );
    assert_eq!(limited.state.current_cursor, rate_event_seq);

    let first_page = store.recover_rule_children(None, 4).unwrap();
    assert_eq!(first_page.len(), 4);
    let after = first_page.last().unwrap().command.id.clone();
    let second_page = store.recover_rule_children(Some(&after), 100).unwrap();
    assert_eq!(second_page.len(), RULE_ACTION_LIMIT as usize - 4);
    assert!(second_page.first().unwrap().command.id > after);
    let pending_after_pages = first_page
        .into_iter()
        .chain(second_page)
        .collect::<Vec<_>>();
    assert_eq!(pending_after_pages.len(), RULE_ACTION_LIMIT as usize);
    assert!(
        pending_after_pages
            .iter()
            .all(|command| command.command.state == CommandState::Accepted)
    );

    store
        .connection
        .execute(
            "UPDATE commands SET state = 'dispatched' WHERE id = ?1",
            [&pending_after_pages[0].command.id],
        )
        .unwrap();
    let pending_after_dispatch = store.recover_rule_children(None, 100).unwrap();
    assert_eq!(pending_after_dispatch.len(), RULE_ACTION_LIMIT as usize - 1);
    assert!(pending_after_dispatch.iter().all(|command| {
        matches!(
            command.command.state,
            CommandState::Accepted | CommandState::Queued
        )
    }));
}

#[test]
fn faulted_writer_holds_rule_child_recovery_without_mutating_commands() {
    let mut store = store();
    let (_, agent_id) = insert_test_agent(&mut store);
    insert_test_rule_action(&mut store, "owner", "faulted-recovery", &agent_id, 1, 100);
    insert_test_rule_action(&mut store, "owner", "faulted-recovery", &agent_id, 1, 101);

    let seeded = store.recover_rule_children(None, 10).unwrap();
    assert_eq!(seeded.len(), 2);
    assert!(
        seeded
            .iter()
            .all(|command| command.command.state == CommandState::Accepted)
    );
    store
        .connection
        .execute(
            // Seed both states accepted by recovery without building a predecessor chain.
            "UPDATE commands SET state = 'queued' WHERE id = ?1",
            [&seeded[0].command.id],
        )
        .unwrap();

    let healthy = store.recover_rule_children(None, 10).unwrap();
    assert_eq!(healthy.len(), 2);
    assert!(
        healthy
            .iter()
            .any(|command| command.command.state == CommandState::Accepted)
    );
    assert!(
        healthy
            .iter()
            .any(|command| command.command.state == CommandState::Queued)
    );

    store.write_faulted.set(true);
    assert!(store.recover_rule_children(None, 10).unwrap().is_empty());
    assert!(store.is_write_faulted());

    let accepted = healthy
        .iter()
        .find(|command| command.command.state == CommandState::Accepted)
        .unwrap();
    let error = store.dispatch(&accepted.command.id, 102).unwrap_err();
    assert_eq!(error.code, "unavailable");
    for command in &healthy {
        let retained = store.get_command(&command.command.id).unwrap().unwrap();
        assert_eq!(retained.state, command.command.state);
    }
    let dispatched: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dispatched, 0);
}
