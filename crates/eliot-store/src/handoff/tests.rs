use super::*;

fn now() -> i64 {
    Ulid::new().timestamp_ms() as i64
}
fn agent(store: &mut Store) -> String {
    let mut r = SubmitCommand::new(Ulid::new().to_string(), "launch");
    r.expected_generation = Some(0);
    r.args = json!({"project":"handoff","workspace":"handoff-unit","harness":"codex","profile":"worker","profile_snapshot":{"control":"tree"}});
    let admitted = store.submit("owner", r, now()).unwrap();
    let id = admitted.command.target.unwrap();
    // Exercise Store admission/ready facts; no vendor process is started by unit tests.
    store.dispatch(&admitted.command.id, now()).unwrap();
    crate::tests::install_fixture_sources(store, &id, &admitted.command.id, 1, now());
    store
        .put_batch(
            core::SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: id.clone(),
                generation: 1,
                source: "stream".into(),
                cursor: None,
                events: vec![core::NativeEvent {
                    ts: None,
                    kind: "session.ready".into(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":admitted.command.id}),
                }],
            },
            now(),
        )
        .unwrap();
    store
        .settle(&admitted.command.id, CommandState::Applied, None, now())
        .unwrap();
    id
}
fn send(store: &mut Store, agent: &str, text: &str) -> String {
    let mut r = SubmitCommand::new(Ulid::new().to_string(), "send");
    r.target = Some(agent.into());
    r.expected_generation = Some(1);
    r.args = json!({"text":text});
    store.submit("owner", r, now()).unwrap().command.id
}
fn setup() -> (Store, HandoffRequest) {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut store);
    let b = agent(&mut store);
    let task = send(&mut store, &a, "original task");
    let queued = send(&mut store, &a, "selected queued work");
    let r = HandoffRequest {
        id: Ulid::new().to_string(),
        from: a,
        from_generation: 1,
        to: b,
        to_generation: 1,
        task_command: task,
        commands: vec![queued],
    };
    (store, r)
}

#[test]
fn handoff_cancels_without_effect_links_and_orders_successors() {
    let (mut s, r) = setup();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert!(result.is_new);
    assert_eq!(result.value.children.len(), 2);
    let old = s.get_command(&r.commands[0]).unwrap().unwrap();
    assert_eq!(old.state, CommandState::Cancelled);
    assert_eq!(old.outcome.unwrap()["no_effect"], true);
    let successor = &result.value.transfers[0].successor;
    assert_eq!(
        s.get_command(successor).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        s.dispatch(successor, now()).unwrap().value.state,
        CommandState::Queued
    );
    s.dispatch(&result.value.brief_command, now()).unwrap();
    s.put_batch(core::SourceBatch {batch_id:Ulid::new().to_string(),agent_id:r.to.clone(),generation:1,source:"stream".into(),cursor:None,events:vec![
        core::NativeEvent {ts:None,kind:"turn.started".into(),native_id:"brief-started".into(),data:json!({"command_id":result.value.brief_command,"turn_id":"brief-turn"})},
        core::NativeEvent {ts:None,kind:"turn.completed".into(),native_id:"brief-completed".into(),data:json!({"command_id":result.value.brief_command,"turn_id":"brief-turn","status":"completed"})},
    ]},now()).unwrap();
    let readback = s.replay_handoff("owner", &r).unwrap().unwrap();
    assert_eq!(readback.brief_state, CommandState::Applied);
    assert_eq!(readback.successor_gate, "released");
    assert_eq!(
        s.dispatch(successor, now()).unwrap().value.state,
        CommandState::Dispatched
    );
    assert_eq!(
        s.get_command(&r.task_command).unwrap().unwrap().state,
        CommandState::Accepted
    );
}

#[test]
fn handoff_replay_does_not_repeat_transfer_and_conflicts_fail() {
    let (mut s, r) = setup();
    let first = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    s.connection
        .execute(
            "UPDATE agents SET generation=2,state='stopped' WHERE id=?1",
            [parse_agent_id(&r.to).unwrap()],
        )
        .unwrap();
    let replay = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.value.children, first.value.children);
    let mut conflict = r.clone();
    conflict.commands.clear();
    assert_eq!(
        s.handoff("owner", &conflict, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "id_conflict"
    );
}

#[test]
fn handoff_active_unknown_and_generation_refusals_are_atomic() {
    for case in [
        "active",
        "unknown",
        "generation",
        "destination",
        "principal",
    ] {
        let (mut s, mut r) = setup();
        match case {
            "active" => {
                s.connection
                    .execute(
                        "UPDATE agents SET state='busy',active_turn='running' WHERE id=?1",
                        [parse_agent_id(&r.from).unwrap()],
                    )
                    .unwrap();
            }
            "unknown" => {
                s.connection
                    .execute(
                        "UPDATE commands SET state='unknown' WHERE id=?1",
                        [&r.task_command],
                    )
                    .unwrap();
            }
            "generation" => r.from_generation = 2,
            "destination" => {
                let dest = r.to.clone();
                send(&mut s, &dest, "existing input");
            }
            _ => {}
        }
        let high = s.high_water().unwrap();
        let principal = if case == "principal" {
            "agent:a1:1"
        } else {
            "owner"
        };
        assert!(
            s.handoff(principal, &r, &core::SecretPolicy::default(), now())
                .is_err(),
            "{case}"
        );
        assert_eq!(s.high_water().unwrap(), high);
        assert!(matches!(
            s.get_command(&r.commands[0]).unwrap().unwrap().state,
            CommandState::Accepted | CommandState::Queued
        ));
        assert!(s.get_command(&r.id).unwrap().is_none());
    }
}

fn claim(s: &mut Store, agent: &str, glob: &str) -> core::api::ClaimSetResponse {
    let r=serde_json::from_value(json!({"id":Ulid::new().to_string(),"agent":agent,"glob":glob,"note":"task area","ttl_secs":900})).unwrap();
    s.claim_set("owner", &r, now()).unwrap().value
}

fn goal(s: &mut Store, agent: &str) {
    let definition=serde_json::from_value(json!({"text":"finish transferred task","check":["native-check"],"max_turns":20,"max_rounds":10,"cadence_ms":60000})).unwrap();
    s.goal_set(
        agent,
        "eliot",
        &Ulid::new().to_string(),
        "owner",
        &definition,
        now(),
    )
    .unwrap();
}

#[test]
fn handoff_preserves_claim_expiry_and_reports_advisory_conflicts() {
    let (mut s, r) = setup();
    let original = claim(&mut s, &r.from, "src/**");
    let other = agent(&mut s);
    claim(&mut s, &other, "src/**");
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(result.value.claims, vec!["src/**"]);
    assert_eq!(result.value.claim_conflicts.len(), 1);
    let rows: Vec<(i64, i64, String, i64)> = s
        .connection
        .prepare("SELECT agent,generation,glob,expires_at FROM claims WHERE agent IN (?1,?2)")
        .unwrap()
        .query_map(
            params![
                parse_agent_id(&r.from).unwrap(),
                parse_agent_id(&r.to).unwrap()
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            parse_agent_id(&r.to).unwrap(),
            1,
            "src/**".into(),
            original.expires_at
        )]
    );
}

#[test]
fn handoff_exact_claim_collision_and_capacity_refuse_without_overwrite() {
    for collision in [true, false] {
        let (mut s, r) = setup();
        claim(&mut s, &r.from, "area/**");
        if collision {
            claim(&mut s, &r.to, "area/**");
        } else {
            for i in 0..core::api::MAX_CLAIMS_PER_AGENT {
                claim(&mut s, &r.to, &format!("other/{i}"));
            }
        }
        let before = s.high_water().unwrap();
        assert!(
            s.handoff("owner", &r, &core::SecretPolicy::default(), now())
                .is_err()
        );
        assert_eq!(s.high_water().unwrap(), before);
        let count: i64 = s
            .connection
            .query_row(
                "SELECT COUNT(*) FROM claims WHERE agent=?1",
                [parse_agent_id(&r.from).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_ne!(
            s.get_command(&r.commands[0]).unwrap().unwrap().state,
            CommandState::Cancelled
        );
    }
}

#[test]
fn handoff_goal_is_paused_clears_source_and_charges_next_destination_turn() {
    let (mut s, r) = setup();
    goal(&mut s, &r.from);
    s.connection.execute("UPDATE goals SET state=json_set(state,'$.counters.turns_used',5,'$.counters.rounds',2,'$.last_continuation_command_id',NULL),last_round_key='idle:100' WHERE agent=?1",[parse_agent_id(&r.from).unwrap()]).unwrap();
    let before = s.goal_get(&r.from, "eliot").unwrap().unwrap();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    let after = s.goal_get(&r.to, "eliot").unwrap().unwrap();
    assert_eq!(after.status, core::api::GoalStatus::Paused);
    assert_eq!(after.definition, before.definition);
    assert_eq!(after.counters.turns_used, 5);
    assert_eq!(after.counters.rounds, 2);
    assert_eq!(after.last_round_key, None);
    assert_eq!(result.value.goal_revision, Some(after.revision));
    assert_eq!(
        s.goal_get(&r.from, "eliot").unwrap().unwrap().status,
        core::api::GoalStatus::Cleared
    );
    s.connection.execute("INSERT INTO turns(agent,generation,turn_id,state) VALUES (?1,1,'next-b-turn','completed')",[parse_agent_id(&r.to).unwrap()]).unwrap();
    assert_eq!(
        s.goal_get(&r.to, "eliot")
            .unwrap()
            .unwrap()
            .counters
            .turns_used,
        6
    );
}

#[test]
fn handoff_refuses_destination_goal_unresolved_rounds_and_continuation() {
    for case in [
        "destination",
        "running",
        "unknown",
        "continuation",
        "stale_goal",
    ] {
        let (mut s, r) = setup();
        goal(&mut s, &r.from);
        match case {
            "destination" => goal(&mut s, &r.to),
            "running" | "unknown" => {
                s.connection.execute("INSERT INTO goal_markers(agent,mode,generation,revision,round_key,holder,state,updated_at) VALUES (?1,'eliot',99,1,1,'old-holder',?2,0)",params![parse_agent_id(&r.from).unwrap(),case]).unwrap();
            }
            "continuation" => {
                s.connection.execute("UPDATE goals SET state=json_set(state,'$.last_continuation_command_id',?1) WHERE agent=?2",params![r.commands[0],parse_agent_id(&r.from).unwrap()]).unwrap();
            }
            _ => {
                s.connection
                    .execute(
                        "UPDATE goals SET generation=99 WHERE agent=?1",
                        [parse_agent_id(&r.from).unwrap()],
                    )
                    .unwrap();
            }
        }
        let high = s.high_water().unwrap();
        assert!(
            s.handoff("owner", &r, &core::SecretPolicy::default(), now())
                .is_err(),
            "{case}"
        );
        assert_eq!(s.high_water().unwrap(), high);
    }
}

#[test]
fn handoff_does_not_overwrite_an_unrelated_goal_or_bypass_a_source_block() {
    let (mut s, r) = setup();
    goal(&mut s, &r.to);
    let before = s.goal_get(&r.to, "eliot").unwrap().unwrap();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(result.value.goal_revision, None);
    assert_eq!(s.goal_get(&r.to, "eliot").unwrap().unwrap(), before);
    let (mut s, r) = setup();
    goal(&mut s, &r.from);
    s.connection.execute("UPDATE goals SET state=json_set(state,'$.status','blocked','$.blocked_reason','turn_limit') WHERE agent=?1",[parse_agent_id(&r.from).unwrap()]).unwrap();
    assert_eq!(
        s.handoff("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(
        s.goal_get(&r.from, "eliot").unwrap().unwrap().status,
        core::api::GoalStatus::Blocked
    );
}

#[test]
fn handoff_can_transfer_the_undispatched_original_task_once() {
    let (mut s, mut r) = setup();
    r.task_command = r.commands[0].clone();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(result.value.transfers.len(), 1);
    assert_eq!(result.value.children.len(), 2);
    assert_eq!(
        s.get_command(&r.task_command).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert!(
        s.get_command(&result.value.brief_command)
            .unwrap()
            .unwrap()
            .args["text"]
            .as_str()
            .unwrap()
            .contains("context only")
    );
}

#[test]
fn handoff_source_and_destination_state_authority_and_original_task_refusals() {
    for case in [
        "source_terminal",
        "destination_terminal",
        "source_observer",
        "destination_observer",
        "project",
        "missing_task",
        "wrong_task_generation",
        "wrong_task_target",
        "destination_generation",
    ] {
        let (mut s, mut r) = setup();
        match case {
            "source_terminal" | "destination_terminal" => {
                let id = if case == "source_terminal" {
                    &r.from
                } else {
                    &r.to
                };
                s.connection
                    .execute(
                        "UPDATE agents SET state='stopped' WHERE id=?1",
                        [parse_agent_id(id).unwrap()],
                    )
                    .unwrap();
            }
            "source_observer" | "destination_observer" => {
                let id = if case == "source_observer" {
                    &r.from
                } else {
                    &r.to
                };
                s.connection
                    .execute(
                        "UPDATE agents SET observe_only=1,token=NULL WHERE id=?1",
                        [parse_agent_id(id).unwrap()],
                    )
                    .unwrap();
            }
            "project" => {
                s.connection
                    .execute(
                        "UPDATE agents SET project='other' WHERE id=?1",
                        [parse_agent_id(&r.to).unwrap()],
                    )
                    .unwrap();
            }
            "missing_task" => r.task_command = "pruned-or-absent".into(),
            "wrong_task_generation" => {
                s.connection
                    .execute(
                        "UPDATE commands SET generation=99 WHERE id=?1",
                        [&r.task_command],
                    )
                    .unwrap();
            }
            "wrong_task_target" => {
                let c = agent(&mut s);
                r.task_command = send(&mut s, &c, "foreign task");
            }
            _ => r.to_generation = 99,
        }
        let high = s.high_water().unwrap();
        assert!(
            s.handoff("owner", &r, &core::SecretPolicy::default(), now())
                .is_err(),
            "{case}"
        );
        assert_eq!(s.high_water().unwrap(), high);
        assert_ne!(
            s.get_command(&r.commands[0]).unwrap().unwrap().state,
            CommandState::Cancelled
        );
    }
}

#[test]
fn handoff_children_use_reparent_and_failure_rolls_back_claims_and_goal() {
    for observe in [false, true] {
        let (mut s, r) = setup();
        claim(&mut s, &r.from, "child-area");
        goal(&mut s, &r.from);
        let child = agent(&mut s);
        s.connection
            .execute(
                "UPDATE agents SET parent=?1,root=?1,depth=1,observe_only=?2,token=CASE WHEN ?2 THEN NULL ELSE token END WHERE id=?3",
                params![
                    parse_agent_id(&r.from).unwrap(),
                    observe,
                    parse_agent_id(&child).unwrap()
                ],
            )
            .unwrap();
        let result = s.handoff("owner", &r, &core::SecretPolicy::default(), now());
        if observe {
            assert!(result.is_err());
            assert_eq!(
                s.goal_get(&r.from, "eliot").unwrap().unwrap().status,
                core::api::GoalStatus::Active
            );
            assert_ne!(
                s.get_command(&r.commands[0]).unwrap().unwrap().state,
                CommandState::Cancelled
            );
            assert!(s.get_command(&r.id).unwrap().is_none());
            let holder: i64 = s
                .connection
                .query_row(
                    "SELECT agent FROM claims WHERE glob='child-area'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(holder, parse_agent_id(&r.from).unwrap());
        } else {
            assert_eq!(result.unwrap().value.reparented.len(), 1);
            let child = s.get_agent(&child).unwrap().unwrap();
            assert_eq!(child.parent, Some(r.to.clone()));
            assert_eq!(child.root, r.to);
            assert_eq!(child.depth, 1);
        }
    }
}

#[test]
fn handoff_child_cycle_refuses_atomically() {
    let (mut s, r) = setup();
    s.connection
        .execute(
            "UPDATE agents SET parent=?1,root=?1,depth=1 WHERE id=?2",
            params![
                parse_agent_id(&r.from).unwrap(),
                parse_agent_id(&r.to).unwrap()
            ],
        )
        .unwrap();
    assert_eq!(
        s.handoff("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_ne!(
        s.get_command(&r.commands[0]).unwrap().unwrap().state,
        CommandState::Cancelled
    );
}

#[test]
fn handoff_brief_redacts_labels_truncation_and_retains_history_pointers() {
    let (mut s, r) = setup();
    let secret = "private-credential-value";
    let text = format!("{secret} {}", "🙂".repeat(120_000));
    s.connection
        .execute(
            "UPDATE commands SET args=?1 WHERE id=?2",
            params![encode_json(&json!({"text":text})).unwrap(), r.task_command],
        )
        .unwrap();
    s.connection.execute("INSERT INTO turns(agent,generation,turn_id,state,final_text,completed_at) VALUES (?1,1,'source-final','completed',?2,1)",params![parse_agent_id(&r.from).unwrap(),format!("result {secret}")]).unwrap();
    let policy = core::SecretPolicy::new([secret]);
    let result = s.handoff("owner", &r, &policy, now()).unwrap();
    assert!(result.value.brief_truncated);
    let brief = s.get_command(&result.value.brief_command).unwrap().unwrap();
    let text = brief.args["text"].as_str().unwrap();
    assert!(!text.contains(secret));
    assert!(text.contains("[REDACTED]"));
    assert!(text.contains("TRUNCATED"));
    assert!(text.contains(&r.task_command));
    assert!(text.contains("source-final"));
    assert!(text.len() <= MAX_INPUT_BYTES);
}

#[test]
fn handoff_newly_known_secret_in_successor_refuses() {
    let (mut s, r) = setup();
    let policy = core::SecretPolicy::new(["selected queued work"]);
    assert_eq!(
        s.handoff("owner", &r, &policy, now()).unwrap_err().code,
        "secret_in_payload"
    );
    assert!(s.get_command(&r.id).unwrap().is_none());
}

#[test]
fn handoff_failed_and_unknown_brief_hold_until_owner_resolution() {
    for state in [CommandState::Failed, CommandState::Unknown] {
        let (mut s, r) = setup();
        let result = s
            .handoff("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap();
        s.dispatch(&result.value.brief_command, now()).unwrap();
        s.settle(&result.value.brief_command, state, None, now())
            .unwrap();
        let successor = &result.value.transfers[0].successor;
        assert_eq!(
            s.dispatch(successor, now()).unwrap().value.state,
            CommandState::Queued
        );
        let parent = s.get_command(&r.id).unwrap().unwrap();
        assert_eq!(
            parent.outcome.unwrap()["successor_gate"],
            if state == CommandState::Unknown {
                "queued_brief_unknown"
            } else {
                "queued_brief_failed"
            }
        );
        let resolve=serde_json::from_value(json!({"id":Ulid::new().to_string(),"command_id":result.value.brief_command,"as":"applied","reason":"Owner verified brief delivery"})).unwrap();
        assert_eq!(
            s.resolve_command("agent:a1:1", &resolve, now())
                .unwrap_err()
                .code,
            "not_allowed"
        );
        s.resolve_command("owner", &resolve, now()).unwrap();
        assert_eq!(
            s.dispatch(successor, now()).unwrap().value.state,
            CommandState::Dispatched
        );
    }
}

#[test]
fn handoff_owner_can_cancel_held_successors_and_retry_without_native_effect() {
    let (mut s, r) = setup();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    let cancel = CommandCancelRequest {
        id: Ulid::new().to_string(),
        command_id: result.value.transfers[0].successor.clone(),
    };
    assert_eq!(
        s.cancel_handoff_input("agent:a1:1", &cancel, now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        s.cancel_handoff_input("owner", &cancel, now())
            .unwrap()
            .value
            .state,
        CommandState::Cancelled
    );
    assert!(
        s.cancel_handoff_input("owner", &cancel, now())
            .unwrap()
            .events
            .is_empty()
    );
    assert_eq!(
        s.get_command(&cancel.command_id).unwrap().unwrap().turn_id,
        None
    );
    let ordinary = CommandCancelRequest {
        id: Ulid::new().to_string(),
        command_id: r.task_command,
    };
    assert_eq!(
        s.cancel_handoff_input("owner", &ordinary, now())
            .unwrap_err()
            .code,
        "bad_state"
    );
    s.dispatch(&result.value.brief_command, now()).unwrap();
    let dispatched = CommandCancelRequest {
        id: Ulid::new().to_string(),
        command_id: result.value.brief_command,
    };
    assert_eq!(
        s.cancel_handoff_input("owner", &dispatched, now())
            .unwrap_err()
            .code,
        "bad_state"
    );
}

#[test]
fn handoff_restart_preserves_unknown_brief_and_withheld_successors() {
    let (mut s, r) = setup();
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    s.dispatch(&result.value.brief_command, now()).unwrap();
    s.recover_on_restart(now()).unwrap();
    assert_eq!(
        s.get_command(&result.value.brief_command)
            .unwrap()
            .unwrap()
            .state,
        CommandState::Unknown
    );
    let successor = &result.value.transfers[0].successor;
    assert_eq!(
        s.get_command(successor).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        s.dispatch(successor, now()).unwrap().value.state,
        CommandState::Queued
    );
    let keys = s.recover_accepted_command_keys(None, 100).unwrap();
    assert!(keys.iter().any(|(id, _, _)| id == successor));
    assert!(
        !keys
            .iter()
            .any(|(id, _, _)| id == &result.value.brief_command)
    );
    assert!(s.recover_accepted_admission("absent-id").unwrap().is_none());
    assert!(
        !s.handoff("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap()
            .is_new
    );
}

#[test]
fn handoff_selection_refuses_bound_effects_and_controls_without_changes() {
    for case in [
        "dispatched",
        "unknown",
        "applied",
        "cancelled",
        "turn",
        "request",
        "expected_turn",
        "context",
        "probe",
        "generation",
        "missing",
    ] {
        let (mut s, mut r) = setup();
        let id = r.commands[0].clone();
        match case {
            "dispatched" | "unknown" | "applied" | "cancelled" => {
                s.connection
                    .execute(
                        "UPDATE commands SET state=?1 WHERE id=?2",
                        params![case, id],
                    )
                    .unwrap();
            }
            "turn" => {
                s.connection
                    .execute(
                        "UPDATE commands SET turn_id='already-bound' WHERE id=?1",
                        [&id],
                    )
                    .unwrap();
            }
            "request" => {
                s.connection
                    .execute(
                        "UPDATE commands SET request_id='request' WHERE id=?1",
                        [&id],
                    )
                    .unwrap();
            }
            "expected_turn" => {
                s.connection
                    .execute(
                        "UPDATE commands SET expected_turn_id='turn' WHERE id=?1",
                        [&id],
                    )
                    .unwrap();
            }
            "context" => {
                s.connection.execute("UPDATE commands SET args=json_set(args,'$.context_items',json('[]')) WHERE id=?1",[&id]).unwrap();
            }
            "probe" => {
                s.connection
                    .execute("UPDATE commands SET probe=1 WHERE id=?1", [&id])
                    .unwrap();
            }
            "generation" => {
                s.connection
                    .execute("UPDATE commands SET generation=2 WHERE id=?1", [&id])
                    .unwrap();
            }
            _ => r.commands[0] = "not-retained".into(),
        }
        let high = s.high_water().unwrap();
        assert!(
            s.handoff("owner", &r, &core::SecretPolicy::default(), now())
                .is_err(),
            "{case}"
        );
        assert_eq!(s.high_water().unwrap(), high);
        assert!(s.get_command(&r.id).unwrap().is_none());
    }
}

#[test]
fn handoff_selection_limit_duplicate_and_source_forms() {
    let (mut s, mut r) = setup();
    r.commands = vec!["same".into(); 2];
    assert_eq!(r.validate().unwrap_err().code, "invalid");
    r.commands = (0..65).map(|i| format!("id-{i}")).collect();
    assert_eq!(r.validate().unwrap_err().code, "invalid");
    r.commands.clear();
    r.from = r.to.clone();
    assert_eq!(r.validate().unwrap_err().code, "invalid");
    let a = agent(&mut s);
    r.from = a;
    r.commands = vec!["one".into(), "two".into()];
    s.set_hierarchy_limits(core::api::HierarchyLimits {
        max_selector_targets: 1,
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        s.handoff("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "fanout_limit"
    );
}

#[test]
fn handoff_accepts_64_successors_in_the_requested_order() {
    let (mut s, mut r) = setup();
    r.commands.clear();
    for i in 0..64 {
        r.commands.push(send(&mut s, &r.from, &format!("task {i}")));
    }
    let result = s
        .handoff("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(result.value.children.len(), 65);
    for (i, t) in result.value.transfers.iter().enumerate() {
        assert_eq!(t.original, r.commands[i]);
        assert_eq!(
            s.get_command(&t.successor).unwrap().unwrap().args["text"],
            format!("task {i}")
        );
    }
}
