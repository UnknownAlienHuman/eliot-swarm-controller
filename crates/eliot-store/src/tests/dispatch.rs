use super::*;

#[test]
fn fake_interrupt_admits_and_dispatches_only_for_its_exact_active_turn() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "fake-interrupt-current-turn", now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);
    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(agent);
    interrupt.args = json!({});
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some(turn_id.clone());
    let interrupt_id = interrupt.id.clone();
    let admitted = store
        .submit("owner", interrupt, now + 4)
        .expect("admit exact Fake interrupt");
    assert_eq!(admitted.command.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(
        store.dispatch(&interrupt_id, now + 5).unwrap().value.state,
        CommandState::Dispatched
    );
}

#[test]
fn fake_steer_admits_and_dispatches_exact_turn_text_without_retargeting() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "fake-steer-current-turn", now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);

    let mut steer = SubmitCommand::new(Ulid::new().to_string(), "steer");
    steer.target = Some(agent.clone());
    steer.args = json!({"text":"continue this turn"});
    steer.expected_generation = Some(1);
    steer.expected_turn_id = Some(turn_id.clone());
    let steer_id = steer.id.clone();
    let admitted = store
        .submit("owner", steer, now + 4)
        .expect("Fake supports exact-turn STEER");
    assert_eq!(admitted.command.verb, "steer");
    assert_eq!(admitted.command.target.as_deref(), Some(agent.as_str()));
    assert_eq!(admitted.command.generation, Some(1));
    assert_eq!(admitted.command.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(admitted.command.args, json!({"text":"continue this turn"}));
    assert_eq!(
        store.dispatch(&steer_id, now + 5).unwrap().value.state,
        CommandState::Dispatched
    );

    let mut stale_turn = SubmitCommand::new(Ulid::new().to_string(), "steer");
    stale_turn.target = Some(agent.clone());
    stale_turn.args = json!({"text":"must not retarget"});
    stale_turn.expected_generation = Some(1);
    stale_turn.expected_turn_id = Some("fake-turn-old".to_owned());
    let stale_turn_id = stale_turn.id.clone();
    let high_water = store.high_water().unwrap();
    assert_eq!(
        store.submit("owner", stale_turn, now + 6).unwrap_err().code,
        "stale_generation"
    );
    assert!(store.get_command(&stale_turn_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), high_water);

    let mut stale_generation = SubmitCommand::new(Ulid::new().to_string(), "steer");
    stale_generation.target = Some(agent.clone());
    stale_generation.args = json!({"text":"must not retarget"});
    stale_generation.expected_generation = Some(2);
    stale_generation.expected_turn_id = Some(turn_id.clone());
    let stale_generation_id = stale_generation.id.clone();
    assert_eq!(
        store
            .submit("owner", stale_generation, now + 7)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert!(store.get_command(&stale_generation_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), high_water);
}

#[test]
fn fake_interrupt_rejects_stale_bindings_and_dispatches_exact_request_replies() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "fake-interrupt-stale-turn", now);
    let sources = store.list_source_states(&agent, 1).unwrap();
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0]["source"], "stream");
    assert_eq!(
        sources[0]["families"],
        json!(["message", "request", "session", "tool", "turn"])
    );
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);

    let mut stale_turn = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    stale_turn.target = Some(agent.clone());
    stale_turn.args = json!({});
    stale_turn.expected_generation = Some(1);
    stale_turn.expected_turn_id = Some("fake-turn-old".to_owned());
    let stale_turn_id = stale_turn.id.clone();
    let before_stale_turn = store.high_water().unwrap();
    assert_eq!(
        store.submit("owner", stale_turn, now + 4).unwrap_err().code,
        "stale_generation"
    );
    assert!(store.get_command(&stale_turn_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), before_stale_turn);

    let mut stale_generation = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    stale_generation.target = Some(agent.clone());
    stale_generation.args = json!({});
    stale_generation.expected_generation = Some(2);
    stale_generation.expected_turn_id = Some(turn_id.clone());
    let stale_generation_id = stale_generation.id.clone();
    let before_stale_generation = store.high_water().unwrap();
    assert_eq!(
        store
            .submit("owner", stale_generation, now + 5)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert!(store.get_command(&stale_generation_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), before_stale_generation);

    let send_id = turn_id
        .strip_prefix("fake-turn-")
        .expect("Fake turn id retains its source send id");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("fake-replies-{send_id}"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    NativeEvent {
                        ts: None,
                        kind: "request.opened".to_owned(),
                        native_id: "fake-reply-approve".to_owned(),
                        data: json!({
                            "command_id": send_id,
                            "request_id": "fake-approve-request",
                            "turn_id": turn_id,
                            "kind": "approval",
                            "details": {}
                        }),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "request.opened".to_owned(),
                        native_id: "fake-reply-deny".to_owned(),
                        data: json!({
                            "command_id": send_id,
                            "request_id": "fake-deny-request",
                            "turn_id": turn_id,
                            "kind": "approval",
                            "details": {}
                        }),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "request.opened".to_owned(),
                        native_id: "fake-reply-answer".to_owned(),
                        data: json!({
                            "command_id": send_id,
                            "request_id": "fake-answer-request",
                            "turn_id": turn_id,
                            "kind": "question",
                            "details": {
                                "questions": [{"id":"q1", "is_secret":false}]
                            }
                        }),
                    },
                ],
                cursor: None,
            },
            now + 6,
        )
        .expect("commit exact Fake reply requests");

    for (verb, request_id, args) in [
        ("approve", "fake-approve-request", json!({})),
        ("deny", "fake-deny-request", json!({})),
        (
            "answer",
            "fake-answer-request",
            json!({"answers":{"q1":{"answers":["Continue"]}}}),
        ),
    ] {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), verb);
        request.target = Some(agent.clone());
        request.args = args;
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        request.request_id = Some(request_id.to_owned());
        let command_id = request.id.clone();
        let admitted = store
            .submit("owner", request, now + 7)
            .unwrap_or_else(|error| panic!("Fake {verb} should admit its exact request: {error}"));
        assert_eq!(admitted.command.request_id.as_deref(), Some(request_id));
        assert_eq!(admitted.command.turn_id.as_deref(), Some(turn_id.as_str()));
        assert_eq!(
            store.dispatch(&command_id, now + 8).unwrap().value.state,
            CommandState::Dispatched,
            "Fake {verb} should pass the dispatch fence"
        );
    }
}

#[test]
fn session_end_keeps_dispatched_stop_pending_until_exact_reply() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "stop-session-ended", now);
    let agent_num = eliot_core::parse_agent_id(&agent).expect("Agent id");
    store
        .connection
        .execute(
            "UPDATE agents SET native_scope = ?1, native_ref = ?2 WHERE id = ?3",
            rusqlite::params![
                "stop-session-ended:scope",
                "stop-session-ended:thread",
                agent_num
            ],
        )
        .expect("retain a native subject for resume");
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 1,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 1).expect("dispatch stop");
    install_live_custody_observations(&mut store, &agent, 1, now + 1);

    store
        .put_batch(
            SourceBatch {
                batch_id: "stop-session-ended:session-ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "stop-session-ended:session-ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 2,
        )
        .expect("commit authoritative session end");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 2);

    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Stopped
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Dispatched,
        "session end alone does not acknowledge the stop RPC"
    );

    let mut blocked_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    blocked_resume.target = Some(agent.clone());
    blocked_resume.expected_generation = Some(1);
    let blocked_resume_id = blocked_resume.id.clone();
    assert_eq!(
        store
            .submit("owner", blocked_resume, now + 3)
            .unwrap_err()
            .code,
        "bad_state",
        "a dispatched old-generation Stop still owns the host lifecycle",
    );
    assert!(store.get_command(&blocked_resume_id).unwrap().is_none());

    store
        .settle(
            &stop_id,
            CommandState::Unknown,
            Some(json!({"reason":"stop reply was lost"})),
            now + 4,
        )
        .expect("retain uncertain old-generation stop");
    let mut unknown_blocked_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    unknown_blocked_resume.target = Some(agent.clone());
    unknown_blocked_resume.expected_generation = Some(1);
    let unknown_blocked_id = unknown_blocked_resume.id.clone();
    assert_eq!(
        store
            .submit("owner", unknown_blocked_resume, now + 5)
            .unwrap_err()
            .code,
        "bad_state",
        "an unknown Stop remains a generation-transition fence",
    );
    assert!(store.get_command(&unknown_blocked_id).unwrap().is_none());

    let settled = store
        .settle(
            &stop_id,
            CommandState::Applied,
            Some(json!({"state":"applied"})),
            now + 6,
        )
        .expect("the exact stop RPC reply settles its command");
    assert_eq!(settled.value.state, CommandState::Applied);
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Stopped
    );

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store
        .submit("owner", resume, now + 7)
        .expect("exact Stop reply reopens the retained Agent for resume");
    assert_eq!(
        store
            .dispatch(&resume_id, now + 8)
            .unwrap()
            .value
            .generation,
        Some(2)
    );
}

#[test]
fn resume_dispatch_rechecks_a_stop_that_becomes_dispatched_after_admission() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "resume-stop-dispatch-race", now);
    let agent_num = eliot_core::parse_agent_id(&agent).expect("Agent id");
    store
        .connection
        .execute(
            "UPDATE agents SET native_scope = ?1, native_ref = ?2 WHERE id = ?3",
            rusqlite::params![
                "resume-stop-dispatch-race:scope",
                "resume-stop-dispatch-race:thread",
                agent_num
            ],
        )
        .expect("retain a native subject for resume");
    install_live_custody_observations(&mut store, &agent, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-stop-dispatch-race:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-stop-dispatch-race:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 1,
        )
        .expect("commit terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 1);

    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store
        .submit("owner", stop, now + 2)
        .expect("admit old-generation Stop");
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store
        .submit("owner", resume, now + 3)
        .expect("admit resume while the old Stop remains undispatched");
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    assert_eq!(
        store.dispatch(&stop_id, now + 4).unwrap().value.state,
        CommandState::Dispatched
    );
    assert_eq!(
        store.dispatch(&resume_id, now + 5).unwrap_err().code,
        "bad_state",
        "a Stop dispatched after resume admission still holds the generation fence",
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 1);
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Accepted
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    store
        .settle(
            &stop_id,
            CommandState::Applied,
            Some(json!({"state":"applied"})),
            now + 6,
        )
        .expect("exact Stop reply clears the old-generation gate");
    assert_eq!(
        store
            .dispatch(&resume_id, now + 7)
            .unwrap()
            .value
            .generation,
        Some(2)
    );
}

#[test]
fn generic_generation_loss_keeps_dispatched_stop_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "stop-generation-lost", now);
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 1,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 1).expect("dispatch stop");

    store
        .mark_lost(&agent, 1, now + 2)
        .expect("generation loss remains uncertain");

    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Unknown
    );
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Lost
    );
    assert!(
        store.dispatch(&stop_id, now + 3).is_err(),
        "unknown stop cannot be replayed"
    );
}

#[test]
fn workspace_exceptions_stay_marker_bound_across_unknown_readback() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let start_id = Ulid::new().to_string();
    let input = WorkspacePreparationInput {
        project: "/synthetic-owner".to_owned(),
        repo_path: "/synthetic-owner".to_owned(),
        repo_common_dir: "/synthetic-owner/.git".to_owned(),
        requested_branch: "codex/audit".to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: "a".repeat(40),
        worktree_path: "/synthetic-worktree".to_owned(),
        frozen_setup: Vec::new(),
    };
    let start = store.submit_with_workspace("owner", SubmitCommand {
        id: start_id.clone(), target: None, selector: None, verb: "launch".to_owned(),
        args: json!({"project":input.project,"workspace":input.repo_path,"worktree":input.requested_branch,
            "harness":"codex","profile":"fake",
            "profile_snapshot":{"capacity_scopes":[{"scope":"profile:fake","max_live":1}]}}),
        expected_generation: Some(0), expected_turn_id: None, request_id: None, probe: false,
    }, input.clone(), now).expect("admit prepared launch");
    let agent = start.command.target.expect("workspace owner");
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    for state in [CommandState::Failed, CommandState::Unknown] {
        assert_eq!(
            store
                .settle(&start_id, state, None, now)
                .expect_err("generic settlement cannot claim preparation exception")
                .code,
            "bad_state"
        );
    }
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Accepted
    );
    store
        .settle_workspace_creation(
            &start_id,
            1,
            WorkspaceCreationOutcome::Unknown {
                code: "creation_readback_unknown".to_owned(),

                message: "synthetic uncertainty".to_owned(),

                failure_where: None,

                origin: None,

                origin_evidence: None,
            },
            now,
        )
        .expect("typed marker permits unknown before native dispatch");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Unknown
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .generation,
        0
    );
    assert_eq!(store.get_agent_token(&agent, 0).expect("no token"), None);
    store
        .dispatch(&start_id, now)
        .expect_err("unknown native effect cannot be replayed");
    let ready = store
        .settle_workspace_creation(
            &start_id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path,
                repo_common_dir: input.repo_common_dir,
                worktree_path: input.worktree_path,
                branch: input.requested_branch,
                head: input.base_oid,
            },
            now,
        )
        .expect("exact creation readback restores only remaining work");
    assert_eq!(ready.value.phase, WorkspacePhase::Ready);
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .generation,
        0
    );

    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit exact pre-native stop");
    store
        .dispatch(&stop_id, now)
        .expect("dispatch cancellation marker");
    store
        .recover_on_restart(now)
        .expect("recover pending cleanup without native dispatch");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Unknown
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Unknown
    );
    store
        .settle_workspace_cancel(&stop_id, &start_id, 1, WorkspaceCancelOutcome::Stopped, now)
        .expect("exact cleanup settles Unknown start cancellation and stop independently");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Cancelled
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Applied
    );
    let owner = store.get_agent(&agent).expect("Agent").expect("owner");
    assert_eq!(owner.generation, 0);
    assert_eq!(owner.state, AgentState::Stopped);
    assert_eq!(capacity_generation(&store, &agent), None);
    assert_eq!(store.get_agent_token(&agent, 0).expect("no token"), None);
    assert!(
        store
            .list_source_states(&agent, 0)
            .expect("no native source installed")
            .is_empty()
    );
    store
        .submit(
            "owner",
            capacity_launch_request(
                "workspace-capacity-reuse",
                "fake",
                json!([{"scope":"profile:fake","max_live":1}]),
            ),
            now + 1,
        )
        .expect("proven pre-native cancellation releases the slot");
}

#[test]
fn failed_dispatched_start_keeps_reservation_until_owned_exit_then_releases() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let scopes = json!([{"scope":"profile:failed-start","max_live":1}]);
    // First launch on the profile: dispatch it and install the fixture
    // native plan plus the live owned-plugin observation, mirroring the
    // daemon's manifest check and spawn before `agent.start`.
    let launch = capacity_launch_request("failed-start-first", "failed-start", scopes.clone());
    let launch_id = launch.id.clone();
    {
        let admission = store
            .submit("owner", launch, now)
            .expect("admit first failed-start launch");
        let agent = admission.command.target.expect("launch target");
        assert_eq!(capacity_generation(&store, &agent), Some(1));
        store
            .dispatch(&launch_id, now)
            .expect("dispatch first launch");
        install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
        install_live_custody_observations(&mut store, &agent, 1, now);
        // A dispatched Failed start settles Failed with the plugin's bound
        // failed result, but without any exit proof the reservation is kept:
        // releasing on a guess could over-admit a still-live plugin.
        let failed_outcome = json!({
            "state": "failed",
            "agent_id": agent,
            "generation": 1,
            "command_id": launch_id,
            "reason": "synthetic catalogue miss",
        });
        let settled = store
            .settle(
                &launch_id,
                CommandState::Failed,
                Some(failed_outcome.clone()),
                now,
            )
            .expect("settle failed start without exit proof");
        assert_eq!(settled.value.state, CommandState::Failed);
        assert_eq!(settled.value.outcome.as_ref(), Some(&failed_outcome));
        assert_eq!(
            store
                .get_agent(&agent)
                .expect("Agent")
                .expect("owner")
                .state,
            AgentState::Failed
        );
        assert_eq!(
            capacity_generation(&store, &agent),
            Some(1),
            "failed start without proven exit keeps its reservation"
        );
        assert_eq!(
            store
                .submit(
                    "owner",
                    capacity_launch_request("failed-start-blocked", "failed-start", scopes.clone()),
                    now + 1,
                )
                .unwrap_err()
                .code,
            "capacity_unavailable"
        );
        // Replay of the failed command returns the identical Failed result
        // with no new events and no native I/O.
        let high_water = store.high_water().unwrap();
        let replay = store
            .settle(
                &launch_id,
                CommandState::Failed,
                Some(failed_outcome.clone()),
                now + 2,
            )
            .expect("replay failed start");
        assert!(replay.events.is_empty());
        assert_eq!(replay.value.state, CommandState::Failed);
        assert_eq!(replay.value.outcome.as_ref(), Some(&failed_outcome));
        assert_eq!(store.high_water().unwrap(), high_water);
        assert_eq!(capacity_generation(&store, &agent), Some(1));
        // Complete the owned-exit proof in host order after the Failed
        // settlement: exit observation, native drain, OS-source closure, then
        // the owned-control-exit record. The OS-source closure releases the
        // reservation through the existing proof path.
        store
            .observe_processes(
                &agent,
                1,
                &[ProcessRecord {
                    identity: simulated_plugin_identity(&agent, 1),
                    owner: format!("agent:{agent}#1"),
                    role: "plugin".to_owned(),
                    state: ProcessState::Exited,
                    exit_code: Some(0),
                    updated_at: now + 3,
                }],
                now + 3,
            )
            .expect("observe owned plugin exit");
        store
            .close_native_sources(&agent, 1, now + 3)
            .expect("close drained native sources");
        store
            .set_process_source_status(&agent, 1, "ok", true, now + 3)
            .expect("close OS process source after exit");
        let released = store
            .record_owned_control_exit(&agent, 1, now + 3)
            .expect("record owned control exit proof");
        assert!(
            released
                .iter()
                .any(|event| event.kind == "capacity.released"),
            "owned exit proof releases the failed generation"
        );
        assert_eq!(capacity_generation(&store, &agent), None);
    }
    // The freed profile slot admits a second launch immediately.
    let second_agent = store
        .submit(
            "owner",
            capacity_launch_request("failed-start-second", "failed-start", scopes.clone()),
            now + 4,
        )
        .expect("released slot admits a second launch")
        .command
        .target
        .expect("second launch target");
    assert_eq!(capacity_generation(&store, &second_agent), Some(1));

    // A dispatched Failed start whose exit is already proven releases
    // through settlement on the same terms as Rejected.
    let proven_scopes = json!([{"scope":"profile:failed-start-proven","max_live":1}]);
    let proven_launch =
        capacity_launch_request("failed-start-proven", "failed-start-proven", proven_scopes);
    let proven_id = proven_launch.id.clone();
    let proven_agent = store
        .submit("owner", proven_launch, now + 5)
        .expect("admit proven-exit launch")
        .command
        .target
        .expect("proven launch target");
    store
        .dispatch(&proven_id, now + 5)
        .expect("dispatch proven-exit launch");
    install_fixture_sources(&mut store, &proven_agent, &proven_id, 1, now + 5);
    install_live_custody_observations(&mut store, &proven_agent, 1, now + 5);
    simulate_normal_stop_proof(&mut store, &proven_agent, 1, now + 6);
    assert_eq!(capacity_generation(&store, &proven_agent), None);
    let proven_outcome = json!({
        "state": "failed",
        "agent_id": proven_agent,
        "generation": 1,
        "command_id": proven_id,
        "reason": "synthetic catalogue miss",
    });
    let proven_settled = store
        .settle(
            &proven_id,
            CommandState::Failed,
            Some(proven_outcome.clone()),
            now + 7,
        )
        .expect("settle failed start with proven exit");
    assert_eq!(proven_settled.value.state, CommandState::Failed);
    assert_eq!(proven_settled.value.outcome.as_ref(), Some(&proven_outcome));
    assert_eq!(capacity_generation(&store, &proven_agent), None);

    // The existing Rejected release behavior is unchanged: a dispatched
    // start that settles Rejected releases its slot immediately.
    let rejected_scopes = json!([{"scope":"profile:failed-start-rejected","max_live":1}]);
    let rejected_launch = capacity_launch_request(
        "failed-start-rejected",
        "failed-start-rejected",
        rejected_scopes,
    );
    let rejected_id = rejected_launch.id.clone();
    let rejected_agent = store
        .submit("owner", rejected_launch, now + 8)
        .expect("admit rejected launch")
        .command
        .target
        .expect("rejected launch target");
    store
        .dispatch(&rejected_id, now + 8)
        .expect("dispatch rejected launch");
    store
        .settle(
            &rejected_id,
            CommandState::Rejected,
            Some(json!({"code": "native_not_dispatched"})),
            now + 8,
        )
        .expect("settle rejected start");
    assert_eq!(capacity_generation(&store, &rejected_agent), None);
    store
        .submit(
            "owner",
            capacity_launch_request(
                "failed-start-rejected-second",
                "failed-start-rejected",
                json!([{"scope":"profile:failed-start-rejected","max_live":1}]),
            ),
            now + 9,
        )
        .expect("rejected release still admits a second launch");
}

#[test]
fn send_association_emits_applied_once_with_native_batch_ack() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":"send-event", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":{}}),
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
    install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: "ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready".to_owned(),
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle start");
    let send_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "send".to_owned(),
                args: json!({"text":"hello"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    let before = store.high_water().expect("event boundary");
    let batch = SourceBatch {
        batch_id: "send".to_owned(),
        agent_id: agent.clone(),
        generation: 1,
        source: "stream".to_owned(),
        events: vec![
            NativeEvent {
                ts: None,
                kind: "turn.started".to_owned(),
                native_id: "turn-start".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1"}),
            },
            NativeEvent {
                ts: None,
                kind: "message.completed".to_owned(),
                native_id: "message".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1", "text":"hello"}),
            },
            NativeEvent {
                ts: None,
                kind: "turn.completed".to_owned(),
                native_id: "turn-end".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1", "status":"completed"}),
            },
        ],
        cursor: Some("send:1".to_owned()),
    };
    let mut invalid = batch.clone();
    invalid.events[2].data["turn_id"] = json!("wrong-turn");
    store
        .put_batch(invalid, now)
        .expect_err("invalid terminal rolls back association and event");
    assert_eq!(
        store
            .get_command(&send_id)
            .expect("command")
            .expect("send")
            .state,
        CommandState::Dispatched
    );
    assert!(
        store
            .read_events(before, 100, None)
            .expect("rolled back events")
            .is_empty()
    );

    let committed = store
        .put_batch(batch.clone(), now)
        .expect("commit complete native batch");
    assert_eq!(committed.ack.last_seq - committed.ack.first_seq + 1, 3);
    let applied: Vec<_> = committed
        .events
        .iter()
        .filter(|event| event.kind == "command.applied" && event.data["command_id"] == send_id)
        .collect();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].source, "daemon");
    assert_eq!(applied[0].agent.as_deref(), Some(agent.as_str()));
    assert_eq!(applied[0].generation, Some(1));
    assert_eq!(applied[0].data["outcome"], json!({"turn_id":"turn-1"}));
    assert!(applied[0].seq > committed.ack.last_seq);
    let duplicate = store
        .put_batch(batch, now)
        .expect("duplicate batch readback");
    assert_eq!(duplicate.ack, committed.ack);
    assert!(duplicate.events.is_empty());
    let readback = store
        .settle(
            &send_id,
            CommandState::Applied,
            Some(json!({"turn_id":"turn-1"})),
            now,
        )
        .expect("same send settlement readback");
    assert!(readback.events.is_empty());
    assert_eq!(
        store
            .read_events(before, 100, None)
            .expect("persisted events")
            .iter()
            .filter(|event| event.kind == "command.applied" && event.data["command_id"] == send_id)
            .count(),
        1
    );
}

#[test]
fn terminal_turn_persists_canonical_outcome_and_legacy_status_classifies() {
    // Canonical outcomes decided at the plugin boundary persist next to the
    // raw native reason; rows written without one classify explicitly from
    // the legacy status and never as success.
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":"outcome-turn", "workspace":"synthetic-workspace", "harness":"codex", "profile":"codex-fixture", "profile_snapshot":{}}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit codex launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    let plan = ["session", "turn", "message", "tool", "request"]
        .into_iter()
        .map(|family| (family.to_owned(), "stream".to_owned()))
        .collect();
    store
        .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
        .expect("install stream source owners");
    store
        .put_batch(
            SourceBatch {
                batch_id: "ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready".to_owned(),
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle start");

    let complete_turn = |store: &mut Store,
                         send_id: &str,
                         turn_id: &str,
                         status: &str,
                         outcome: Option<&str>,
                         native_status: Option<&str>,
                         now: i64| {
        let mut data = json!({"command_id":send_id, "turn_id":turn_id, "status":status});
        if let Some(outcome) = outcome {
            data["outcome"] = json!(outcome);
        }
        if let Some(native_status) = native_status {
            data["native_status"] = json!(native_status);
        }
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{turn_id}:terminal"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: format!("{turn_id}:terminal"),
                        data,
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit terminal turn");
    };
    let start_turn = |store: &mut Store, send_id: &str, turn_id: &str, now: i64| {
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{turn_id}:started"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("{turn_id}:started"),
                        data: json!({"command_id":send_id, "turn_id":turn_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit turn start");
    };
    let send_and_start = |store: &mut Store, text: &str, now: i64| -> (String, String) {
        let send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text":text}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        let turn_id = format!("turn-{send_id}");
        start_turn(store, &send_id, &turn_id, now + 1);
        (send_id, turn_id)
    };

    // Interrupted turn with the adapter's canonical outcome and raw reason.
    let (send_id, turn_id) = send_and_start(&mut store, "interrupt me", now + 1);
    complete_turn(
        &mut store,
        &send_id,
        &turn_id,
        "interrupted",
        Some("interrupted"),
        Some("cancelled"),
        now + 2,
    );
    let turn = store
        .get_turn(&agent, 1, &turn_id)
        .expect("read turn")
        .expect("turn exists");
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.outcome, Some(TurnOutcome::Interrupted));
    assert_eq!(turn.native_status.as_deref(), Some("cancelled"));

    // Legacy row without a stored outcome classifies from its status.
    let (legacy_send, legacy_turn) = send_and_start(&mut store, "legacy", now + 3);
    complete_turn(
        &mut store,
        &legacy_send,
        &legacy_turn,
        "completed",
        None,
        None,
        now + 4,
    );
    let legacy = store
        .get_turn(&agent, 1, &legacy_turn)
        .expect("read legacy turn")
        .expect("legacy turn exists");
    assert_eq!(legacy.outcome, Some(TurnOutcome::Success));
    assert_eq!(legacy.native_status, None);

    // An unrecognized legacy status is unknown, never success.
    let (unknown_send, unknown_turn) = send_and_start(&mut store, "unknown", now + 5);
    complete_turn(
        &mut store,
        &unknown_send,
        &unknown_turn,
        "bogus",
        None,
        None,
        now + 6,
    );
    let unknown = store
        .get_turn(&agent, 1, &unknown_turn)
        .expect("read unknown turn")
        .expect("unknown turn exists");
    assert_eq!(unknown.outcome, Some(TurnOutcome::Unknown));

    // Owner resolution of a lost turn records its asserted outcome.
    let (lost_send, lost_turn) = send_and_start(&mut store, "lost", now + 7);
    let _ = lost_send;
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{lost_turn}:ended"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: format!("{lost_turn}:ended"),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 8,
        )
        .expect("commit session end");
    let lost = store
        .get_turn(&agent, 1, &lost_turn)
        .expect("read lost turn")
        .expect("lost turn exists");
    assert_eq!(lost.state, TurnState::Unknown);
    assert_eq!(lost.outcome, None);
    store
        .resolve_turn(
            "owner",
            &TurnResolveRequest {
                id: Ulid::new().to_string(),
                agent: agent.clone(),
                generation: 1,
                turn_id: lost_turn.clone(),
                status: TurnResolveStatus::Interrupted,
                reason: "owner asserts the lost turn was interrupted".to_owned(),
            },
            None,
            now + 9,
        )
        .expect("resolve lost turn");
    let resolved = store
        .get_turn(&agent, 1, &lost_turn)
        .expect("read resolved turn")
        .expect("resolved turn exists");
    assert_eq!(resolved.state, TurnState::Completed);
    assert_eq!(resolved.status.as_deref(), Some("interrupted"));
    assert_eq!(resolved.outcome, Some(TurnOutcome::Interrupted));
}

#[test]
fn claude_legacy_rows_classify_from_the_raw_terminal_reason() {
    // Pre-outcome Claude rows carry the raw terminal reason in `status`
    // (`success`, `api_error*`), not the normalized lifecycle status, so a
    // harness-blind read reports them as unknown. Retrying the identical
    // retained command with `--wait` must exit 0/1, not 4.
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":"claude-legacy", "workspace":"synthetic-workspace", "harness":"claude", "profile":"claude-fixture", "profile_snapshot":{}}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit claude launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    let plan = ["session", "turn", "message", "tool", "request"]
        .into_iter()
        .map(|family| (family.to_owned(), "stream".to_owned()))
        .collect();
    store
        .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
        .expect("install stream source owners");
    store
        .put_batch(
            SourceBatch {
                batch_id: "ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready".to_owned(),
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle start");

    let legacy_turn = |store: &mut Store, status: &str, now: i64| -> String {
        let send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text":"legacy"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        let turn_id = format!("turn-{send_id}");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{turn_id}:started"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("{turn_id}:started"),
                        data: json!({"command_id":send_id, "turn_id":turn_id}),
                    }],
                    cursor: None,
                },
                now + 1,
            )
            .expect("commit turn start");
        // No `outcome`: a row written before canonical outcomes existed.
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{turn_id}:terminal"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: format!("{turn_id}:terminal"),
                        data: json!({"command_id":send_id, "turn_id":turn_id, "status":status}),
                    }],
                    cursor: None,
                },
                now + 2,
            )
            .expect("commit legacy terminal turn");
        turn_id
    };

    let mut tick = 0;
    for (status, expected) in [
        ("success", TurnOutcome::Success),
        ("api_error", TurnOutcome::Failure),
        ("api_error_rate_limited", TurnOutcome::Failure),
        ("error", TurnOutcome::Failure),
        ("cancelled", TurnOutcome::Interrupted),
        ("completed", TurnOutcome::Success),
        ("bogus", TurnOutcome::Unknown),
    ] {
        tick += 10;
        let turn_id = legacy_turn(&mut store, status, now + tick);
        let turn = store
            .get_turn(&agent, 1, &turn_id)
            .expect("read legacy turn")
            .expect("legacy turn exists");
        assert_eq!(turn.state, TurnState::Completed, "status={status}");
        assert_eq!(turn.outcome, Some(expected), "status={status}");
        assert_eq!(turn.status.as_deref(), Some(status), "status={status}");
    }

    // The harness-aware read also feeds the list path, not just `get_turn`,
    // so a retained command retried with `--wait` exits 0/1, not 4.
    let turns = store.list_turns(&agent, 1, 50).expect("list legacy turns");
    assert!(
        turns
            .iter()
            .any(|turn| turn.status.as_deref() == Some("success")
                && turn.outcome == Some(TurnOutcome::Success)),
        "list_turns must classify the claude success row"
    );
    assert!(
        turns
            .iter()
            .any(|turn| turn.status.as_deref() == Some("api_error")
                && turn.outcome == Some(TurnOutcome::Failure)),
        "list_turns must classify the claude api_error row"
    );
    // The list path agrees with the `get_turn` classification.
    for (status, expected) in [
        ("cancelled", TurnOutcome::Interrupted),
        ("completed", TurnOutcome::Success),
        ("bogus", TurnOutcome::Unknown),
    ] {
        assert!(
            turns.iter().any(
                |turn| turn.status.as_deref() == Some(status) && turn.outcome == Some(expected)
            ),
            "list_turns must classify status={status} as {expected:?}"
        );
    }
}

#[test]
fn secret_question_answer_is_rejected_before_command_persistence() {
    assert!(contains_secret_marker(
        &json!({"questions": [{"is_secret": true}]})
    ));
    assert!(contains_secret_marker(
        &json!({"native": {"question": {"isSecret": true}}})
    ));
    assert!(!contains_secret_marker(
        &json!({"questions": [{"is_secret": false}]})
    ));

    let launch_id = Ulid::new().to_string();
    let send_id = Ulid::new().to_string();
    let answer_id = Ulid::new().to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system time in milliseconds fits in i64");
    let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({
                    "project": "secret-answer-regression",
                    "workspace": "synthetic-workspace",
                    "harness": "codex",
                    "profile": "synthetic-codex",
                    "profile_snapshot": {}
                }),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit Codex launch");
    let started = store
        .dispatch(&launch_id, now)
        .expect("dispatch Codex launch");
    let agent_id = admission
        .command
        .target
        .expect("launch admission returns its Agent");
    install_fixture_sources(&mut store, &agent_id, &launch_id, 1, now);
    assert_eq!(started.value.generation, Some(1));
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready-1".to_owned(),
                    data: json!({
                        "native_scope": "synthetic-codex-home",
                        "native_ref": "synthetic-thread",
                        "effective": {}
                    }),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit synthetic session readiness");
    store
        .settle(
            &launch_id,
            CommandState::Applied,
            Some(json!({"state": "ready"})),
            now,
        )
        .expect("settle launch");
    store
        .submit(
            "owner",
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent_id.clone()),
                selector: None,
                verb: "send".to_owned(),
                args: json!({"text": "trigger synthetic request"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit synthetic input");
    store
        .dispatch(&send_id, now)
        .expect("dispatch synthetic input");
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "turn-started-1".to_owned(),
                    data: json!({"command_id": send_id, "turn_id": "turn-1"}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit synthetic turn start");
    let rule_id = "secret-answer-event-rule";
    let rule_value = json!({"id":rule_id,"enabled":true});
    let rule_definition = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let rule_state = store
        .ensure_rule_state("owner", &rule_definition, now + 1)
        .expect("install event rule before request event");
    let request_batch = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.opened".to_owned(),
                    native_id: "request-opened-1".to_owned(),
                    data: json!({
                        "command_id": send_id,
                        "request_id": "public-request-1",
                        "turn_id": "turn-1",
                        "kind": "question",
                        "details": {
                            "questions": [{"id": "q1", "is_secret": true}]
                        }
                    }),
                }],
                cursor: None,
            },
            now + 2,
        )
        .expect("commit synthetic secret question");
    let request_event_seq = request_batch
        .events
        .iter()
        .find(|event| {
            event.kind == "request.opened" && event.data["request_id"] == "public-request-1"
        })
        .expect("retained exact request.opened event")
        .seq;

    let marker = "synthetic-answer-secret-marker";
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: answer_id.clone(),
                target: Some(agent_id.clone()),
                selector: None,
                verb: "answer".to_owned(),
                args: json!({"answers":{"q1":{"answers":[marker]}}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect_err("secret question answers must be rejected");
    assert_eq!(error.code, "secret_in_payload");
    assert!(
        store
            .get_command(&answer_id)
            .expect("read rejected answer command")
            .is_none(),
        "rejected answer args must not be persisted"
    );

    let mut rule_answer = SubmitCommand::new(String::new(), "answer");
    rule_answer.target = Some(agent_id.clone());
    rule_answer.args = json!({"answers":{"q1":{"answers":[marker]}}});
    rule_answer.expected_generation = Some(1);
    rule_answer.expected_turn_id = Some("turn-1".to_owned());
    rule_answer.request_id = Some("public-request-1".to_owned());
    let rejected = store
        .admit_rule_action(
            "owner",
            &rule_state,
            request_event_seq,
            rule_answer,
            now + 3,
        )
        .expect("secret rule answer is a durable deterministic rejection");
    assert!(rejected.child.is_none());
    let parent = rejected
        .parent
        .expect("retained secret answer refusal parent");
    assert_eq!(parent.state, CommandState::Rejected);
    assert_eq!(parent.outcome.as_ref().unwrap()["children"], json!([]));
    assert_eq!(
        parent.outcome.as_ref().unwrap()["code"],
        "secret_in_payload"
    );
    assert_eq!(parent.args["rule"], json!(rule_id));
    assert_eq!(parent.args["event_seq"], json!(request_event_seq));
    assert!(parent.args.get("answers").is_none());
    assert!(!serde_json::to_string(&parent).unwrap().contains(marker));
    assert!(
        !serde_json::to_string(&rejected.events)
            .unwrap()
            .contains(marker)
    );
    assert_eq!(rejected.state.current_cursor, request_event_seq);
    let parent_id = parent.id.clone();
    let persisted_answers: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE verb = 'answer'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted_answers, 0);

    let closed = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.closed".to_owned(),
                    native_id: "request-closed-after-secret-refusal".to_owned(),
                    data: json!({"request_id":"public-request-1", "turn_id":"turn-1"}),
                }],
                cursor: None,
            },
            now + 4,
        )
        .expect("close the request after its refusal was committed");
    let closed_seq = closed.events[0].seq;
    let advanced = store
        .advance_rule_cursor(
            "owner",
            &rejected.state,
            closed_seq,
            RuleCursorOutcome::NoMatch,
            now + 5,
        )
        .expect("scan the request closure without firing the rule");
    assert_eq!(advanced.value.current_cursor, closed_seq);

    let mut replay_answer = SubmitCommand::new(String::new(), "answer");
    replay_answer.target = Some(agent_id.clone());
    replay_answer.args = json!({"answers":{"q1":{"answers":[marker]}}});
    replay_answer.expected_generation = Some(1);
    replay_answer.expected_turn_id = Some("turn-1".to_owned());
    replay_answer.request_id = Some("public-request-1".to_owned());
    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action(
            "owner",
            &advanced.value,
            request_event_seq,
            replay_answer,
            now + 6,
        )
        .expect("replay the retained refusal after pending state changed");
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, parent_id);
    assert_eq!(
        replay.parent.as_ref().unwrap().outcome.as_ref().unwrap()["code"],
        "secret_in_payload"
    );
    assert!(replay.child.is_none());
    assert!(replay.events.is_empty());
    assert_eq!(replay.state.current_cursor, closed_seq);
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);

    let next_request = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.opened".to_owned(),
                    native_id: "request-opened-read-failure".to_owned(),
                    data: json!({
                        "command_id": send_id,
                        "request_id": "public-request-read-failure",
                        "turn_id": "turn-1",
                        "kind": "question",
                        "details": {"questions":[{"id":"q1", "is_secret":false}]}
                    }),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit the next public request");
    let next_event_seq = next_request.events[0].seq;
    let high_water_before_read_failure = store.high_water().unwrap();
    store
        .connection
        .execute("DROP TABLE pending_requests", [])
        .expect("inject an unreadable pending-request table");
    let mut unreadable_answer = SubmitCommand::new(String::new(), "answer");
    unreadable_answer.target = Some(agent_id);
    unreadable_answer.args = json!({"answers":{"q1":{"answers":["Continue"]}}});
    unreadable_answer.expected_generation = Some(1);
    unreadable_answer.expected_turn_id = Some("turn-1".to_owned());
    unreadable_answer.request_id = Some("public-request-read-failure".to_owned());
    let read_failure = store
        .admit_rule_action(
            "owner",
            &replay.state,
            next_event_seq,
            unreadable_answer,
            now + 8,
        )
        .expect_err("pending-request read failure must propagate");
    assert_eq!(read_failure.code, "internal");
    assert_eq!(
        store
            .get_rule_state("owner", rule_id)
            .unwrap()
            .unwrap()
            .current_cursor,
        closed_seq
    );
    assert_eq!(store.high_water().unwrap(), high_water_before_read_failure);
    let persisted_answers: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE verb = 'answer'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted_answers, 0);
}
