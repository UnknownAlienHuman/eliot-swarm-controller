use super::*;
use eliot_core::api::SwitchLaunch;

fn now() -> i64 {
    Ulid::new().timestamp_ms() as i64
}
fn setup() -> (Store, SwitchRequest) {
    setup_at(Path::new(":memory:"))
}
fn setup_at(path: &Path) -> (Store, SwitchRequest) {
    let mut s = Store::open(path).unwrap();
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({"project":"switch","workspace":"switch-unit","harness":"codex","profile":"worker","profile_snapshot":{"control":"tree"}});
    let admission = s.submit("owner", launch, now()).unwrap();
    let a = admission.command.target.unwrap();
    ready(&mut s, &admission.command.id, &a);
    let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
    send.target = Some(a.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text":"selected task"});
    let task = s.submit("owner", send, now()).unwrap().command.id;
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a,
        from_generation: 1,
        task_command: task.clone(),
        commands: vec![task],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low"}),
        )
        .unwrap(),
    };
    (s, request)
}
fn ready(s: &mut Store, id: &str, agent: &str) {
    s.dispatch(id, now()).unwrap();
    crate::tests::install_fixture_sources(s, agent, id, 1, now());
    s.put_batch(
        core::SourceBatch {
            batch_id: Ulid::new().to_string(),
            agent_id: agent.into(),
            generation: 1,
            source: "stream".into(),
            cursor: None,
            events: vec![core::NativeEvent {
                ts: None,
                kind: "session.ready".into(),
                native_id: Ulid::new().to_string(),
                data: json!({"command_id":id}),
            }],
        },
        now(),
    )
    .unwrap();
    s.settle(
        id,
        CommandState::Applied,
        Some(json!({"state":"ready"})),
        now(),
    )
    .unwrap();
}
fn begin(s: &mut Store, r: &SwitchRequest) -> SwitchResponse {
    let p = s
        .reserve_switch("owner", r, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert!(p.created.is_none());
    assert!(s.get_command(&p.launch_command).unwrap().is_none());
    assert!(s.start_switch(&r.id, now()).unwrap().value.is_some());
    assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
    p
}
fn launch(s: &mut Store, r: &SwitchRequest, p: &SwitchResponse) -> String {
    let mut c = SubmitCommand::new(&p.launch_command, "launch");
    c.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree","model":"gemini-4-hydrogen-low"}});
    s.admit_switch_launch(&r.id, c, None, now())
        .unwrap()
        .command
        .target
        .unwrap()
}

#[test]
fn switch_reserves_before_launch_and_replay_never_creates_another_agent() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    assert_eq!(s.agent_count().unwrap(), 1);
    let b = launch(&mut s, &r, &p);
    let replay = s.replay_switch("owner", &r).unwrap().unwrap();
    assert_eq!(replay.launch_command, p.launch_command);
    assert_eq!(replay.created.unwrap().agent, b);
    assert_eq!(s.agent_count().unwrap(), 2);
    let mut changed = r.clone();
    changed.launch.model = Some("another".into());
    assert_eq!(
        s.replay_switch("owner", &changed).unwrap_err().code,
        "id_conflict"
    );
    assert_eq!(s.agent_count().unwrap(), 2);
    let parent = s.get_command(&r.id).unwrap().unwrap();
    let children = s.list_command_children(&parent, None, 10).unwrap().children;
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, p.launch_command);
    assert!(
        s.get_command(&format!("{}/send", p.launch_command))
            .unwrap()
            .is_none()
    );
}

#[test]
fn switch_refuses_busy_source_before_reserving_or_launching() {
    let (mut s, r) = setup();
    s.connection
        .execute(
            "UPDATE agents SET state='busy', active_turn='native-turn' WHERE id=?1",
            [parse_agent_id(&r.from).unwrap()],
        )
        .unwrap();
    assert_eq!(
        s.reserve_switch("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "busy"
    );
    assert_eq!(s.agent_count().unwrap(), 1);
    assert!(s.get_command(&r.id).unwrap().is_none());
}

#[test]
fn switch_uses_handoff_transaction_only_after_exact_ready() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    assert!(!s.switch_launch_ready(&r.id).unwrap());
    ready(&mut s, &p.launch_command, &b);
    assert!(s.switch_launch_ready(&r.id).unwrap());
    let result = s
        .finish_switch(&r.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Applied);
    let h = result.handoff.unwrap();
    assert_eq!(h.to, b);
    assert_eq!(h.id, p.handoff_command);
    assert_eq!(
        s.get_command(&r.commands[0]).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    let children = s
        .list_command_children(&s.get_command(&r.id).unwrap().unwrap(), None, 10)
        .unwrap()
        .children;
    assert_eq!(children.len(), 2);
    assert_eq!(children[1].id, p.handoff_command);
    let again = s
        .finish_switch(&r.id, &core::SecretPolicy::default(), now())
        .unwrap();
    assert!(again.events.is_empty());
    assert_eq!(again.value.handoff.unwrap().children, h.children);
    assert_eq!(s.agent_count().unwrap(), 2);
}

#[test]
fn switch_boundary_failure_keeps_created_b_and_source_intact_for_ordinary_handoff() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    ready(&mut s, &p.launch_command, &b);
    s.connection
        .execute(
            "UPDATE agents SET state='busy', active_turn='new-turn' WHERE id=?1",
            [parse_agent_id(&r.from).unwrap()],
        )
        .unwrap();
    let result = s
        .finish_switch(&r.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Failed);
    assert_eq!(result.error.as_deref(), Some("busy"));
    assert_eq!(result.created.unwrap().agent, b);
    assert!(result.handoff.is_none());
    assert_eq!(result.next_action, "finish_with_ordinary_swarm.handoff");
    assert_eq!(
        s.get_command(&r.commands[0]).unwrap().unwrap().state,
        CommandState::Accepted
    );
    s.connection
        .execute(
            "UPDATE agents SET state='idle', active_turn=NULL WHERE id=?1",
            [parse_agent_id(&r.from).unwrap()],
        )
        .unwrap();
    let handoff = r.handoff(&Ulid::new().to_string(), &b, 1);
    s.handoff("owner", &handoff, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(
        s.replay_switch("owner", &r).unwrap().unwrap().state,
        CommandState::Failed
    );
    assert_eq!(s.agent_count().unwrap(), 2);
}

#[test]
fn switch_failed_and_unknown_launches_never_transfer_or_retry() {
    for state in [CommandState::Failed, CommandState::Unknown] {
        let (mut s, r) = setup();
        let p = begin(&mut s, &r);
        let b = launch(&mut s, &r, &p);
        s.dispatch(&p.launch_command, now()).unwrap();
        s.settle(&p.launch_command, state, None, now()).unwrap();
        let result = s
            .finish_switch(&r.id, &core::SecretPolicy::default(), now())
            .unwrap()
            .value;
        assert_eq!(
            result.state,
            if state == CommandState::Unknown {
                CommandState::Unknown
            } else {
                CommandState::Failed
            }
        );
        assert_eq!(result.created.unwrap().agent, b);
        assert!(result.handoff.is_none());
        assert_eq!(
            s.get_command(&r.commands[0]).unwrap().unwrap().state,
            CommandState::Accepted
        );
        assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
        assert_eq!(
            s.replay_switch("owner", &r)
                .unwrap()
                .unwrap()
                .launch_command,
            p.launch_command
        );
    }
}

#[test]
fn switch_missing_or_wrong_ready_and_prior_input_refuse() {
    for case in ["missing", "wrong_command", "prior_input"] {
        let (mut s, r) = setup();
        let p = begin(&mut s, &r);
        let b = launch(&mut s, &r, &p);
        ready(&mut s, &p.launch_command, &b);
        if case == "missing" {
            s.connection
                .execute(
                    "DELETE FROM events WHERE agent=?1 AND kind='session.ready'",
                    [parse_agent_id(&b).unwrap()],
                )
                .unwrap();
        }
        if case == "wrong_command" {
            s.connection.execute("UPDATE events SET data=json_set(data,'$.command_id','unrelated') WHERE agent=?1 AND kind='session.ready'",[parse_agent_id(&b).unwrap()]).unwrap();
        }
        if case == "prior_input" {
            let mut c = SubmitCommand::new(Ulid::new().to_string(), "send");
            c.target = Some(b);
            c.args = json!({"text":"other input"});
            s.submit("owner", c, now()).unwrap();
        }
        let result = s
            .finish_switch(&r.id, &core::SecretPolicy::default(), now())
            .unwrap()
            .value;
        assert_eq!(result.state, CommandState::Failed, "{case}");
        assert!(result.handoff.is_none());
        assert_eq!(
            s.get_command(&r.commands[0]).unwrap().unwrap().state,
            CommandState::Accepted
        );
    }
}

#[test]
fn switch_native_ready_without_command_id_binds_the_unique_new_generation_start() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    ready(&mut s, &p.launch_command, &b);
    s.connection.execute("UPDATE events SET data=json_remove(data,'$.command_id'),cause=NULL WHERE agent=?1 AND kind='session.ready'",[parse_agent_id(&b).unwrap()]).unwrap();
    assert!(s.switch_launch_ready(&r.id).unwrap());
    let result = s
        .finish_switch(&r.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Applied);
    assert_eq!(result.handoff.unwrap().to, b);
}

#[test]
fn switch_restart_preserves_plan_and_unknown_launch_without_replay() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    s.dispatch(&p.launch_command, now()).unwrap();
    s.recover_on_restart(now()).unwrap();
    let result = s.replay_switch("owner", &r).unwrap().unwrap();
    assert_eq!(result.state, CommandState::Unknown);
    assert_eq!(result.launch_state, Some(CommandState::Unknown));
    assert_eq!(result.launch_command, p.launch_command);
    assert_eq!(result.handoff_command, p.handoff_command);
    assert_eq!(result.created.unwrap().agent, b);
    assert!(result.handoff.is_none());
    assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
    assert_eq!(s.agent_count().unwrap(), 2);
}

#[test]
fn switch_restart_recovers_only_the_exact_unattempted_launch_child() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    s.recover_on_restart(now()).unwrap();
    let keys = s.recover_accepted_command_keys(None, 100).unwrap();
    assert!(keys.iter().any(|key| key.0 == p.launch_command));
    assert!(!keys.iter().any(|key| key.0 == r.id));
    let admission = s
        .recover_accepted_admission(&p.launch_command)
        .unwrap()
        .unwrap();
    assert_eq!(admission.command.target.as_deref(), Some(b.as_str()));
    assert!(!admission.is_new);
    ready(&mut s, &p.launch_command, &b);
    let result = s.replay_switch("owner", &r).unwrap().unwrap();
    assert_eq!(result.state, CommandState::Unknown);
    assert_eq!(result.launch_state, Some(CommandState::Applied));
    assert_eq!(result.next_action, "finish_with_ordinary_swarm.handoff");
    assert!(result.handoff.is_none());
    assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
    assert_eq!(s.agent_count().unwrap(), 2);
}

#[test]
fn switch_prelaunch_interruption_and_preparation_refusal_keep_the_reserved_ids() {
    for interrupted in [true, false] {
        let (mut s, r) = setup();
        let p = begin(&mut s, &r);
        if interrupted {
            s.recover_on_restart(now()).unwrap();
        } else {
            s.fail_switch_preparation(&r.id, "profile_error", now())
                .unwrap();
        }
        let replay = s.replay_switch("owner", &r).unwrap().unwrap();
        assert_eq!(
            replay.state,
            if interrupted {
                CommandState::Unknown
            } else {
                CommandState::Rejected
            }
        );
        assert_eq!(replay.launch_command, p.launch_command);
        assert_eq!(replay.handoff_command, p.handoff_command);
        assert!(replay.created.is_none());
        assert!(replay.handoff.is_none());
        assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
        assert_eq!(s.agent_count().unwrap(), 1);
    }
}

#[test]
fn switch_concurrent_reservation_replays_the_same_ids_without_faulting_either_writer() {
    let path = std::env::temp_dir().join(format!("eliot-h3-{}.db", Ulid::new()));
    let (s, r) = setup_at(&path);
    drop(s);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let results = std::thread::scope(|scope| {
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let path = path.clone();
            let request = r.clone();
            let barrier = barrier.clone();
            jobs.push(scope.spawn(move || {
                let mut s = Store::open(&path).unwrap();
                barrier.wait();
                let value = s
                    .reserve_switch("owner", &request, &core::SecretPolicy::default(), now())
                    .unwrap()
                    .value;
                assert!(!s.is_write_faulted());
                assert_eq!(s.agent_count().unwrap(), 1);
                (value.launch_command, value.handoff_command)
            }));
        }
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results[0], results[1]);
    for name in [
        path.clone(),
        path.with_extension("db-wal"),
        path.with_extension("db-shm"),
    ] {
        let _ = std::fs::remove_file(name);
    }
}

#[test]
fn switch_applied_launch_after_restart_requires_readiness_inspection_not_handoff_now() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    ready(&mut s, &p.launch_command, &b);
    s.recover_on_restart(now()).unwrap();
    let value = s.replay_switch("owner", &r).unwrap().unwrap();
    assert_eq!(value.state, CommandState::Unknown);
    assert_eq!(value.launch_state, Some(CommandState::Applied));
    assert_eq!(
        value.next_action,
        "inspect_readiness_then_ordinary_swarm.handoff"
    );
    assert_eq!(s.get_agent(&b).unwrap().unwrap().state, AgentState::Lost);
    assert!(value.handoff.is_none());
    assert!(s.start_switch(&r.id, now()).unwrap().value.is_none());
}

#[test]
fn switch_prepared_selector_change_refuses_before_allocating_b() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let mut c = SubmitCommand::new(&p.launch_command, "launch");
    c.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"wrong-model","profile":"new","profile_snapshot":{"control":"tree"}});
    assert_eq!(
        s.admit_switch_launch(&r.id, c, None, now())
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert_eq!(s.agent_count().unwrap(), 1);
    assert!(s.get_command(&p.launch_command).unwrap().is_none());
}

#[test]
fn switch_never_adopts_an_unrelated_command_using_a_reserved_id() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let mut foreign = SubmitCommand::new(&p.launch_command, "launch");
    foreign.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree","model":"gemini-4-hydrogen-low"}});
    s.submit("owner", foreign.clone(), now()).unwrap();
    assert_eq!(
        s.admit_switch_launch(&r.id, foreign, None, now())
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert!(s.switch_result(&r.id).unwrap().created.is_none());
    let parent = s.get_command(&r.id).unwrap().unwrap();
    assert!(
        s.list_command_children(&parent, None, 10)
            .unwrap()
            .children
            .is_empty()
    );
}

#[test]
fn switch_launch_refuses_input_parent_project_and_stale_source_before_allocation() {
    for case in ["text", "parent", "project", "stale"] {
        let (mut s, r) = setup();
        let p = begin(&mut s, &r);
        let mut c = SubmitCommand::new(&p.launch_command, "launch");
        c.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree"}});
        match case {
            "text" => c.args["text"] = json!("must not run"),
            "parent" => c.args["parent"] = json!(r.from),
            "project" => c.args["project"] = json!("another-project"),
            _ => {
                s.connection
                    .execute(
                        "UPDATE agents SET generation=2 WHERE id=?1",
                        [parse_agent_id(&r.from).unwrap()],
                    )
                    .unwrap();
            }
        }
        assert!(
            s.admit_switch_launch(&r.id, c, None, now()).is_err(),
            "{case}"
        );
        assert_eq!(s.agent_count().unwrap(), 1, "{case}");
        assert!(s.get_command(&p.launch_command).unwrap().is_none());
    }
}

#[test]
fn switch_authority_generation_secret_and_selection_refusals_are_atomic() {
    let (mut s, r) = setup();
    assert_eq!(
        s.reserve_switch("agent:a1:1", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let mut stale = r.clone();
    stale.from_generation = 2;
    assert_eq!(
        s.reserve_switch("owner", &stale, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "stale_generation"
    );
    let policy = core::SecretPolicy::new(["gemini-4-hydrogen-low"]);
    assert_eq!(
        s.reserve_switch("owner", &r, &policy, now())
            .unwrap_err()
            .code,
        "secret_in_payload"
    );
    let mut duplicate = r.clone();
    duplicate.commands.push(duplicate.commands[0].clone());
    assert_eq!(
        s.reserve_switch("owner", &duplicate, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_eq!(s.agent_count().unwrap(), 1);
    assert!(s.get_command(&r.id).unwrap().is_none());
}

#[test]
fn switch_savepoint_rolls_back_partial_claim_transfer_on_hierarchy_failure() {
    let (mut s, r) = setup();
    let p = begin(&mut s, &r);
    let b = launch(&mut s, &r, &p);
    ready(&mut s, &p.launch_command, &b);
    s.claim_set(
        "owner",
        &core::api::ClaimSetRequest {
            id: Ulid::new().to_string(),
            agent: Some(r.from.clone()),
            glob: "source/**".into(),
            note: None,
            ttl_secs: Some(300),
        },
        now(),
    )
    .unwrap();
    // A child which cannot be moved makes re-parent fail after the claim transfer attempt.
    s.connection
        .execute(
            "UPDATE agents SET parent=?1,root=?1,depth=1 WHERE id=?2",
            params![
                parse_agent_id(&r.from).unwrap(),
                parse_agent_id(&b).unwrap()
            ],
        )
        .unwrap();
    let result = s
        .finish_switch(&r.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Failed);
    assert!(result.handoff.is_none());
    let source: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE agent=?1",
            [parse_agent_id(&r.from).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    let destination: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM claims WHERE agent=?1",
            [parse_agent_id(&b).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(source, 1);
    assert_eq!(destination, 0);
    assert_eq!(
        s.get_command(&r.commands[0]).unwrap().unwrap().state,
        CommandState::Accepted
    );
}
