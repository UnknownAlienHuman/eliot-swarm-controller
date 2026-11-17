use super::*;

#[test]
fn event_subject_is_persisted_at_write_time_without_changing_native_data_digest() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let project = "event-subject-snapshot";
    let launch_id = Ulid::new().to_string();
    let mut request = SubmitCommand::new(launch_id.clone(), "launch");
    request.args = json!({"project":project, "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":{}});
    request.expected_generation = Some(0);
    let admission = store.submit("owner", request, now).unwrap();
    let agent = admission.command.target.clone().unwrap();
    let created = admission
        .events
        .iter()
        .find(|event| event.kind == "agent.created")
        .unwrap();
    let created_subject = created.subject.as_ref().unwrap();
    assert_eq!(created_subject.project, project);
    assert_eq!(created_subject.harness, "codex");
    assert_eq!(created_subject.state, AgentState::Created);

    let dispatched = store.dispatch(&launch_id, now + 1).unwrap();
    install_fixture_sources(&mut store, &agent, &launch_id, 1, now + 1);
    let starting = dispatched
        .events
        .iter()
        .find(|event| event.kind == "agent.starting")
        .unwrap();
    assert_eq!(starting.generation, Some(1));
    assert_eq!(
        starting.subject.as_ref().unwrap().state,
        AgentState::Starting
    );

    let native = NativeEvent {
        kind: "session.ready".to_owned(),
        native_id: format!("{project}:ready"),
        data: json!({"command_id":launch_id}),
        ts: None,
    };
    let batch = store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent,
                generation: 1,
                source: "stream".to_owned(),
                events: vec![native.clone()],
                cursor: None,
            },
            now + 2,
        )
        .unwrap();
    let ready = batch
        .events
        .iter()
        .find(|event| event.kind == "session.ready")
        .unwrap();
    assert_eq!(ready.subject.as_ref().unwrap().state, AgentState::Idle);
    assert_eq!(ready.data, native.data);
    let ready_agent = ready.agent.as_deref().unwrap();
    let ready_agent_num = eliot_core::parse_agent_id(ready_agent).unwrap();
    let ready_generation = ready.generation.unwrap();
    let ready_native_id = ready.native_id.as_deref().unwrap();
    let stored_digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM events WHERE seq = ?1 AND agent = ?2
             AND generation = ?3 AND source = 'stream' AND native_id = ?4",
            rusqlite::params![
                ready.seq,
                ready_agent_num,
                ready_generation,
                ready_native_id
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stored_digest,
        crate::canonical::digest(&native.semantic_value()).unwrap()
    );
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .unwrap();

    let reread = store
        .read_events(ready.seq - 1, 1, Some(ready.seq))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reread.subject, ready.subject);
}

#[test]
fn typed_command_cause_updates_persisted_and_returned_event_without_legacy_backfill() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "cause-annotation", now);
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
    request.target = Some(agent);
    request.expected_generation = Some(1);
    let tx = store
        .connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let mut admission = crate::admission::submit_tx(
        &tx,
        "owner",
        request.clone(),
        None,
        now + 1,
        false,
        eliot_core::api::HierarchyLimits::default(),
    )
    .unwrap();
    let event_index = admission
        .events
        .iter()
        .position(|event| {
            event.kind == "command.accepted"
                && event
                    .data
                    .get("command_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(request.id.as_str())
        })
        .unwrap();
    let event_seq = admission.events[event_index].seq;
    assert_eq!(
        admission.events[event_index].cause,
        Some(EventCause {
            command_id: request.id.clone(),
            rule: None,
            remote_call: None,
            depth: 0,
        })
    );

    let cause = EventCause {
        command_id: request.id.clone(),
        rule: Some("stop-on-result".to_owned()),
        remote_call: None,
        depth: 1,
    };
    crate::annotate_new_command_cause_tx(
        &tx,
        &request.id,
        &mut admission.events[event_index],
        cause.clone(),
    )
    .unwrap();
    crate::commit_transaction(tx, &store.write_faulted).unwrap();

    assert_eq!(admission.events[event_index].cause, Some(cause.clone()));
    assert_eq!(
        crate::command_cause_tx(&store.connection, &request.id).unwrap(),
        Some(cause.clone())
    );
    let persisted = store
        .read_events(event_seq - 1, 1, Some(event_seq))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(persisted.cause, Some(cause.clone()));

    store
        .connection
        .execute(
            "UPDATE commands SET cause = ?1 WHERE id = ?2",
            rusqlite::params!["legacy-untyped-command-cause", request.id.as_str()],
        )
        .unwrap();
    assert_eq!(
        crate::command_cause_tx(&store.connection, &request.id).unwrap(),
        None,
        "unrecognized legacy command causes are unbound rather than fatal",
    );
    assert_eq!(
        store
            .read_events(event_seq - 1, 1, Some(event_seq))
            .unwrap()
            .pop()
            .unwrap()
            .cause,
        Some(cause),
        "event cause is not reconstructed from later command state",
    );
}

#[test]
fn native_turn_cause_uses_exact_verified_command_and_ignores_payload_cause() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "cause-native-turn", now);
    let send_id = Ulid::new().to_string();
    let mut send = SubmitCommand::new(send_id.clone(), "send");
    send.target = Some(agent.clone());
    send.args = json!({"text":"hello"});
    send.expected_generation = Some(1);
    let admission = store.submit("owner", send, now + 1).unwrap();
    let accepted = admission
        .events
        .iter()
        .find(|event| event.kind == "command.accepted")
        .unwrap();
    assert_eq!(
        accepted.cause,
        Some(EventCause {
            command_id: send_id.clone(),
            rule: None,
            remote_call: None,
            depth: 0,
        })
    );
    store.dispatch(&send_id, now + 2).unwrap();

    let native = NativeEvent {
        kind: "turn.started".to_owned(),
        native_id: "cause-native-turn:started".to_owned(),
        data: json!({
            "command_id": send_id,
            "turn_id": "cause-turn-1",
            "cause": {"command_id":"forged","rule":"forged","depth":5},
        }),
        ts: None,
    };
    let batch = store
        .put_batch(
            SourceBatch {
                batch_id: "cause-native-turn:batch".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![native.clone()],
                cursor: None,
            },
            now + 3,
        )
        .unwrap();
    let started = batch
        .events
        .iter()
        .find(|event| event.kind == "turn.started")
        .unwrap();
    assert_eq!(
        started.cause,
        Some(EventCause {
            command_id: send_id,
            rule: None,
            remote_call: None,
            depth: 0,
        })
    );
    assert_eq!(started.data, native.data);

    let agent_num = eliot_core::parse_agent_id(&agent).unwrap();
    let stored_digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM events WHERE seq = ?1 AND agent = ?2
             AND generation = 1 AND source = 'stream' AND native_id = ?3",
            rusqlite::params![started.seq, agent_num, native.native_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stored_digest,
        crate::canonical::digest(&native.semantic_value()).unwrap()
    );
    let reread = store
        .read_events(started.seq - 1, 1, Some(started.seq))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reread.cause, started.cause);
}

#[test]
fn local_notify_commits_user_event_and_applied_atomically() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "notify".to_owned(),
                args: json!({"message": "hello"}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit notify");
    store.dispatch(&id, now + 1).expect("dispatch notify");
    let change = store
        .commit_user_event_applied(&id, "x.user.notify", json!({"message": "hello"}), now + 2)
        .expect("atomic notify commit");
    assert_eq!(change.value.state, CommandState::Applied);
    assert_eq!(change.events.len(), 2);
    assert_eq!(change.events[0].kind, "x.user.notify");
    assert_eq!(change.events[1].kind, "command.applied");
    assert_eq!(
        change.value.outcome,
        Some(json!({"kind": "x.user.notify", "seq": change.events[0].seq}))
    );
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events after atomic commit");
    // An exact retry returns the recorded result without a new event.
    let retry = store
        .commit_user_event_applied(&id, "x.user.notify", json!({"message": "hello"}), now + 3)
        .expect("idempotent notify retry");
    assert_eq!(retry.value.state, CommandState::Applied);
    assert_eq!(retry.value.outcome, change.value.outcome);
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
fn local_user_event_atomic_commit_rejects_mismatch_and_leaves_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "emit".to_owned(),
                args: json!({"topic": "alerts", "data": {"n": 1}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit emit");
    store.dispatch(&id, now + 1).expect("dispatch emit");
    let error = store
        .commit_user_event_applied(&id, "x.user.other", json!({"data": {"n": 1}}), now + 2)
        .expect_err("kind mismatch is rejected");
    assert_eq!(error.code, "invalid");
    assert_eq!(
        store.get_command(&id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    let change = store
        .commit_user_event_applied(&id, "x.user.alerts", json!({"data": {"n": 1}}), now + 3)
        .expect("atomic emit commit");
    assert_eq!(change.value.state, CommandState::Applied);
    assert_eq!(change.events.len(), 2);
    assert_eq!(change.events[0].kind, "x.user.alerts");
    assert_eq!(change.events[1].kind, "command.applied");
    // Legacy crash rows stay Unknown: no reconciliation without retained evidence.
    let legacy = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: legacy.clone(),
                target: None,
                selector: None,
                verb: "notify".to_owned(),
                args: json!({"message": "legacy"}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 4,
        )
        .expect("admit legacy notify");
    store
        .dispatch(&legacy, now + 5)
        .expect("dispatch legacy notify");
    store
        .settle(
            &legacy,
            CommandState::Unknown,
            Some(json!({"reason": "daemon_restart"})),
            now + 6,
        )
        .expect("legacy crash row stays unknown");
    let error = store
        .commit_user_event_applied(
            &legacy,
            "x.user.notify",
            json!({"message": "legacy"}),
            now + 7,
        )
        .expect_err("legacy unknown is left alone");
    assert_eq!(error.code, "bad_state");
    let command = store.get_command(&legacy).unwrap().unwrap();
    assert_eq!(command.state, CommandState::Unknown);
    assert_eq!(command.outcome, Some(json!({"reason": "daemon_restart"})));
}

#[test]
fn projection_gap_turn_unknown_settles_once_without_resurrection() {
    let mut store = Store::open(Path::new(":memory:")).expect("open store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _, _, _) = ready_muse_agent(
        &mut store,
        "projection-gap",
        "muse-test",
        "synthetic-projection-gap",
        now,
    );
    let send = send_request(&agent, 1, "projection gap turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 4).expect("admit send");
    store.dispatch(&send_id, now + 5).expect("dispatch send");
    let turn_id = format!("fake-turn-{send_id}");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("projection-gap-{send_id}-started"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("projection-gap-{send_id}:started"),
                    data: json!({"command_id": send_id, "turn_id": turn_id.clone()}),
                }],
                cursor: None,
            },
            now + 6,
        )
        .expect("commit turn started");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("projection-gap-{send_id}-unknown"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.unknown".to_owned(),
                    native_id: format!("projection-gap-{send_id}:turn.unknown"),
                    data: json!({
                        "command_id": send_id,
                        "turn_id": turn_id.clone(),
                        "reason": "Muse native view recovery did not prove live delivery after the recorded gap (projectionUnavailable)",
                    }),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit turn unknown");
    let agent_row = store
        .get_agent(&agent)
        .expect("read agent")
        .expect("agent row");
    assert_eq!(agent_row.state, AgentState::Idle);
    assert_eq!(agent_row.active_turn, None);
    assert!(!agent_row.is_terminal());
    let turn = store
        .get_turn(&agent, 1, &turn_id)
        .expect("read turn")
        .expect("turn row");
    assert_eq!(turn.state, TurnState::Unknown);
    assert_eq!(turn.outcome, Some(TurnOutcome::Unknown));
    let late = store.put_batch(
        SourceBatch {
            batch_id: format!("projection-gap-{send_id}-late"),
            agent_id: agent.clone(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                ts: None,
                kind: "turn.completed".to_owned(),
                native_id: format!("projection-gap-{send_id}:late-completed"),
                data: json!({"command_id": send_id, "turn_id": turn_id.clone(), "status": "completed"}),
            }],
            cursor: None,
        },
        now + 8,
    );
    assert!(
        late.is_err(),
        "late terminal must not resurrect the unknown turn"
    );
    let duplicate = store.put_batch(
        SourceBatch {
            batch_id: format!("projection-gap-{send_id}-unknown-again"),
            agent_id: agent.clone(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                ts: None,
                kind: "turn.unknown".to_owned(),
                native_id: format!("projection-gap-{send_id}:turn.unknown-again"),
                data: json!({"command_id": send_id, "turn_id": turn_id.clone(), "reason": "retry"}),
            }],
            cursor: None,
        },
        now + 9,
    );
    assert!(duplicate.is_err(), "turn unknown settles exactly once");
    let turn = store
        .get_turn(&agent, 1, &turn_id)
        .expect("read turn")
        .expect("turn row");
    assert_eq!(turn.state, TurnState::Unknown);
}

#[test]
fn observe_events_commit_with_closed_kinds_and_roll_up() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "observe-probe", now);

    // Unknown kinds never commit.
    let rejected =
        store.commit_observe_event(Some(&agent), Some(1), "agent.attention", json!({}), now);
    assert_eq!(rejected.expect_err("unknown kind rejects").code, "invalid");

    // Targetless drift commits without an Agent binding.
    let drifted = eliot_core::check_harness_drift("muse", Some("1.4.20"), None)
        .expect("drift fixture")
        .payload();
    let event = store
        .commit_observe_event(None, None, eliot_core::HARNESS_DRIFT_KIND, drifted, now)
        .expect("commit drift");
    assert_eq!(event.kind, "harness.drift");
    assert_eq!(event.agent, None);

    // A drifted generation never commits under a dead binding.
    let stale = store.commit_observe_event(
        Some(&agent),
        Some(9),
        eliot_core::ACTIVITY_HEAVY_KIND,
        json!({}),
        now,
    );
    assert_eq!(
        stale.expect_err("drifted generation rejects").code,
        "stale_generation"
    );

    let episode = HeavyEpisode {
        category: "cargo build".to_owned(),
        category_source: eliot_core::CATEGORY_SOURCE_CMDLINE.to_owned(),
        started_at: now,
        ended_at: now + 45_000,
        end_source: "observed_exit".to_owned(),
        duration_ms: 45_000,
        cpu_ms: 65_000,
        peak_rss_bytes: 10,
        exit_code: Some(0),
    };
    assert!(episode.crosses(&eliot_core::HeavyThresholds::default()));
    let committed = store
        .commit_observe_event(
            Some(&agent),
            Some(1),
            eliot_core::ACTIVITY_HEAVY_KIND,
            episode.payload(),
            now + 1,
        )
        .expect("commit episode");
    assert_eq!(committed.agent.as_deref(), Some(agent.as_str()));

    let rollup = store
        .heavy_rollup(&agent, 1)
        .expect("roll up")
        .expect("one episode");
    assert_eq!(rollup.episodes, 1);
    assert_eq!(rollup.latest_category.as_deref(), Some("cargo build"));
    assert_eq!(rollup.total_cpu_ms, 65_000);
    assert!(
        store
            .heavy_rollup(&agent, 2)
            .expect("empty generation")
            .is_none()
    );

    let growth = store
        .session_growth_view("codex", &agent, 1, &SessionGrowthThresholds::default())
        .expect("growth view");
    assert!(growth.is_none());
}
