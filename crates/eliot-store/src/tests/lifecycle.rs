use super::*;

#[test]
fn archived_agent_activity_reappears_in_default_list_atomically() {
    for verb in ["send", "inject"] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = Ulid::new().timestamp_ms() as i64;
        let agent = ready_steer_agent(&mut store, verb, now);
        if verb == "inject" {
            store
                .connection
                .execute(
                    "UPDATE agents SET effective=?1 WHERE id=?2",
                    rusqlite::params![
                        r#"{"context_points":["next_input"]}"#,
                        eliot_core::parse_agent_id(&agent).unwrap()
                    ],
                )
                .unwrap();
        }
        store
            .change_agent_tree(&Ulid::new().to_string(), &agent, Some(true), now)
            .unwrap();
        let list = |store: &Store| {
            store
                .list_agent_summaries(
                    &crate::AgentListFilter::default(),
                    None,
                    20,
                    now,
                    &SessionGrowthThresholds::default(),
                )
                .unwrap()
        };
        assert_eq!(list(&store).total, 0);
        let mut request = send_request(&agent, 1, "read the roadmap");
        request.verb = verb.to_owned();
        if verb == "inject" {
            request.args = json!({"point":"next_input","text":"read the roadmap"});
        }
        let mut invalid = request.clone();
        invalid.id = Ulid::new().to_string();
        invalid.expected_generation = Some(99);
        assert!(store.submit("owner", invalid, now).is_err());
        assert!(store.get_agent(&agent).unwrap().unwrap().archived);
        store.submit("owner", request.clone(), now).unwrap();
        assert!(!store.get_agent(&agent).unwrap().unwrap().archived);
        assert_eq!(list(&store).total, 1);
        assert_eq!(list(&store).summaries[0].id, agent);
        assert!(!store.submit("owner", request, now).unwrap().is_new);
    }
}

#[test]
fn stop_resume_stop_delete_removes_processes_of_every_generation() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let agent = ready_steer_agent(&mut store, "delete-generations", now);
    let initial = store.get_agent(&agent).unwrap().unwrap();
    for generation in [1, 2] {
        install_live_custody_observations(&mut store, &agent, generation, now);
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.target = Some(agent.clone());
        stop.expected_generation = Some(generation);
        let stop_id = stop.id.clone();
        store.submit("owner", stop, now).unwrap();
        store.dispatch(&stop_id, now).unwrap();
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("delete:{generation}:ended"),
                    agent_id: agent.clone(),
                    generation,
                    source: "stream".to_owned(),
                    cursor: None,
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: format!("delete:{generation}:ended"),
                        data: json!({"command_id":stop_id}),
                    }],
                },
                now,
            )
            .unwrap();
        store
            .settle(&stop_id, CommandState::Applied, None, now)
            .unwrap();
        simulate_normal_stop_proof(&mut store, &agent, generation, now);
        store.connection.execute("INSERT INTO processes(host_boot_id,pid,start_time,owner,role,state,exit_code,updated_at) VALUES ('workspace',?1,0,?2,'setup','exited',0,?3)", rusqlite::params![generation,format!("workspace:{agent}#{generation}"),now]).unwrap();
        if generation == 1 {
            store
                .change_agent_tree(&Ulid::new().to_string(), &agent, Some(true), now)
                .unwrap();
            let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
            resume.target = Some(agent.clone());
            resume.expected_generation = Some(1);
            let resume_id = resume.id.clone();
            store.submit("owner", resume, now).unwrap();
            assert!(!store.get_agent(&agent).unwrap().unwrap().archived);
            store.dispatch(&resume_id, now).unwrap();
            install_fixture_sources(&mut store, &agent, &resume_id, 2, now);
            store.put_batch(SourceBatch {
                batch_id: "delete:resumed".to_owned(), agent_id: agent.clone(), generation: 2,
                source: "stream".to_owned(), cursor: None,
                events: vec![NativeEvent { ts: None, kind: "session.ready".to_owned(),
                    native_id: "delete:resumed".to_owned(), data: json!({"command_id":resume_id,"native_scope":initial.native_scope,"native_ref":initial.native_ref}) }],
            }, now).unwrap();
            store
                .settle(&resume_id, CommandState::Applied, None, now)
                .unwrap();
        }
    }
    store.connection.execute("INSERT INTO processes(host_boot_id,pid,start_time,owner,role,state,exit_code,updated_at) VALUES ('other',10,0,'agent:a10#1','plugin','exited',0,?1)", [now]).unwrap();
    let delete_id = Ulid::new().to_string();
    let result = store
        .change_agent_tree(&delete_id, &agent, None, now)
        .unwrap();
    assert!(store.get_agent(&agent).unwrap().is_none());
    let owners: Vec<String> = store
        .connection
        .prepare("SELECT owner FROM processes")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(owners, vec!["agent:a10#1"]);
    assert_eq!(
        store
            .change_agent_tree(&delete_id, &agent, None, now)
            .unwrap(),
        result
    );
}

#[test]
fn delete_ignores_other_agents_sources_with_or_without_cursors() {
    for source in ["git", "stream"] {
        for cursor in [None, Some("0"), Some("1")] {
            let mut store = Store::open(Path::new(":memory:")).unwrap();
            let now = Ulid::new().timestamp_ms() as i64;
            store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('','codex','p','stopped',1,?1,?1),('','codex','p','idle',1,?1,?1)",[now]).unwrap();
            store.connection.execute("INSERT INTO source_state(agent,generation,source,cursor,state,health,updated_at) VALUES (2,1,?1,?2,'open','error',?3)",rusqlite::params![source,cursor,now]).unwrap();
            store.connection.execute("INSERT INTO events(ts,kind,agent,generation,source,data) VALUES (?1,'session.ended',1,1,'stream','{}'),(?1,'session.ready',2,1,'stream','{}'),(?1,'agent.stopped',1,1,'daemon','{}')",[now]).unwrap();
            let delete_id = Ulid::new().to_string();
            let reply = store
                .change_agent_tree(&delete_id, "a1", None, now)
                .unwrap();
            assert_eq!(reply["deleted"], true);
            assert!(store.get_agent("a1").unwrap().is_none());
            assert_eq!(
                store.get_agent("a2").unwrap().unwrap().state,
                AgentState::Idle
            );
            assert_eq!(
                store
                    .connection
                    .query_row("SELECT COUNT(*) FROM events WHERE agent=2", [], |r| r
                        .get::<_, i64>(0))
                    .unwrap(),
                1
            );
            assert_eq!(
                store
                    .change_agent_tree(&delete_id, "a1", None, now)
                    .unwrap(),
                reply
            );
        }
    }
}

#[test]
fn delete_retains_enabled_current_and_retiring_rule_history_pins() {
    for retiring in [false, true] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = Ulid::new().timestamp_ms() as i64;
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('','codex','p','stopped',1,?1,?1)",[now]).unwrap();
        let definition = json!({"id":"delete-pin","enabled":true});
        store
            .ensure_rule_state(
                "owner",
                &RuleDefinitionInput {
                    id: "delete-pin".to_owned(),
                    canonical_definition: definition.to_string(),
                    definition_digest: eliot_core::canonical_json_sha256(&definition).unwrap(),
                    enabled: true,
                },
                now,
            )
            .unwrap();
        store.connection.execute("INSERT INTO events(ts,kind,agent,generation,source,data) VALUES (?1,'agent.stopped',1,1,'daemon','{}')",[now]).unwrap();
        if retiring {
            store.connection.execute("UPDATE rule_state SET current_cursor=1,retiring_revision=1,retiring_definition='{}',retiring_cursor=0,retire_through=1",[]).unwrap();
        }
        let delete_id = Ulid::new().to_string();
        let error = store
            .change_agent_tree(&delete_id, "a1", None, now)
            .unwrap_err();
        assert_eq!(error.code, "busy");
        assert_eq!(
            error.message.as_str(),
            "retained history is pinned by a live rule cursor"
        );
        assert!(store.get_agent("a1").unwrap().is_some());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT pruned_through FROM rule_history_coverage",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        // Both ordinary advancement and disabling release the protection.
        if retiring {
            store
                .connection
                .execute("UPDATE rule_state SET retiring_cursor=1", [])
                .unwrap();
        } else {
            store
                .connection
                .execute("UPDATE rule_state SET enabled=0", [])
                .unwrap();
        }
        store
            .change_agent_tree(&delete_id, "a1", None, now)
            .unwrap();
    }
}

#[test]
fn delete_refuses_its_own_live_git_source_even_without_a_cursor() {
    for state in ["idle", "stopped"] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = Ulid::new().timestamp_ms() as i64;
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('','codex','p',?1,1,?2,?2)",rusqlite::params![state,now]).unwrap();
        store.connection.execute("INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (1,1,'git','open','error',?1)",[now]).unwrap();
        assert_eq!(
            store
                .change_agent_tree(&Ulid::new().to_string(), "a1", None, now)
                .unwrap_err()
                .code,
            "busy"
        );
        assert!(store.get_agent("a1").unwrap().is_some());
    }
}

#[test]
fn delete_refuses_unconsumed_cleanup_and_preserves_consumed_replay() {
    use eliot_core::api::{
        ArtifactKind, ArtifactView, CleanupApplyRequest, CleanupCandidate, CleanupPlan,
    };
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    for _ in 0..2 {
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('p','codex','p','stopped',2,?1,?1)",[now]).unwrap();
    }
    let artifact = ArtifactView {
        id: Ulid::new().to_string(),
        agent: "a1".to_owned(),
        generation: 1,
        project: "p".to_owned(),
        tree: vec!["a1:1".to_owned()],
        path: "C:/scratch/old".to_owned(),
        kind: ArtifactKind::Temp,
        purpose: "old output".to_owned(),
        identity: "old".to_owned(),
        created_at: now,
        state: "registered".to_owned(),
        git: None,
    };
    let plan = CleanupPlan {
        plan_id: Ulid::new().to_string(),
        created_at: now,
        items: vec![CleanupCandidate {
            artifact,
            bytes: None,
            verdict: "keep".to_owned(),
            reason: "retained".to_owned(),
        }],
        truncated: false,
    };
    store.retain_cleanup_plan("owner", &plan).unwrap();
    let mut unrelated = plan.clone();
    unrelated.plan_id = Ulid::new().to_string();
    unrelated.items[0].artifact.agent = "a2".to_owned();
    store.retain_cleanup_plan("owner", &unrelated).unwrap();
    let delete_id = Ulid::new().to_string();
    assert_eq!(
        store
            .change_agent_tree(&delete_id, "a1", None, now)
            .unwrap_err()
            .code,
        "busy"
    );
    assert!(store.get_agent("a1").unwrap().is_some());
    let request = CleanupApplyRequest {
        id: Ulid::new().to_string(),
        plan_id: plan.plan_id.clone(),
        delete_remote: false,
        artifacts: None,
    };
    let (_, first, is_new) = store.begin_cleanup_apply("owner", &request, now).unwrap();
    assert!(is_new);
    store
        .change_agent_tree(&delete_id, "a1", None, now)
        .unwrap();
    assert!(store.cleanup_plan("owner", &unrelated.plan_id).is_ok());
    let (_, replay, is_new) = store.begin_cleanup_apply("owner", &request, now).unwrap();
    assert!(!is_new);
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(replay).unwrap()
    );
}

#[test]
fn failed_turn_records_its_command_as_preceded_by() {
    // A non-success terminal turn links the command whose terminal event
    // settled it; success links nothing. Legacy rows read back as None.
    let mut store = Store::open(Path::new(":memory:")).expect("open store");
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
                args: json!({"project":"preceded-turn", "workspace":"synthetic-workspace", "harness":"codex", "profile":"codex-fixture", "profile_snapshot":{}}),
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
    let run_turn = |store: &mut Store, status: &str, outcome: Option<&str>| -> (String, String) {
        let send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text":"hi"}),
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
        for (kind, suffix, with_outcome) in [
            ("turn.started", "started", false),
            ("turn.completed", "terminal", true),
        ] {
            let mut data = json!({"command_id":send_id, "turn_id":turn_id, "status":status});
            if with_outcome && let Some(outcome) = outcome {
                data["outcome"] = json!(outcome);
            }
            store
                .put_batch(
                    SourceBatch {
                        batch_id: format!("{turn_id}:{suffix}"),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: kind.to_owned(),
                            native_id: format!("{turn_id}:{suffix}"),
                            data,
                        }],
                        cursor: None,
                    },
                    now,
                )
                .expect("commit turn event");
        }
        (send_id, turn_id)
    };
    let (failed_send, failed_turn) = run_turn(&mut store, "error", Some("failure"));
    let turn = store
        .get_turn(&agent, 1, &failed_turn)
        .expect("read failed turn")
        .expect("failed turn present");
    assert_eq!(turn.outcome, Some(TurnOutcome::Failure));
    assert_eq!(turn.preceded_by.as_deref(), Some(failed_send.as_str()));
    let (ok_send, ok_turn) = run_turn(&mut store, "completed", Some("success"));
    let _ = ok_send;
    let turn = store
        .get_turn(&agent, 1, &ok_turn)
        .expect("read success turn")
        .expect("success turn present");
    assert_eq!(turn.outcome, Some(TurnOutcome::Success));
    assert_eq!(turn.preceded_by, None);
}

#[test]
fn startup_lost_agents_returns_bounded_retained_losses() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let lost = ready_attention_agent(&mut store, "attention-startup-lost", now);
    store.mark_lost(&lost, 1, now + 1).expect("mark lost");
    let healthy = ready_attention_agent(&mut store, "attention-startup-live", now + 2);
    let losses = store
        .startup_lost_agents(8, None)
        .expect("read startup losses");
    assert!(
        losses.iter().any(|loss| {
            loss.agent == lost && loss.generation == 1 && loss.event_seq.is_some_and(|seq| seq > 0)
        }),
        "retained loss carries its latest agent.lost sequence"
    );
    assert!(
        losses.iter().all(|loss| loss.agent != healthy),
        "live agents are not loss evidence"
    );
    let bounded = store
        .startup_lost_agents(1, None)
        .expect("bounded startup losses");
    assert_eq!(bounded.len(), 1);
}

#[test]
fn startup_lost_agents_pages_beyond_the_first_bound() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut lost_agents = Vec::new();
    for index in 0..5 {
        let agent =
            ready_attention_agent(&mut store, &format!("attention-startup-page-{index}"), now);
        store
            .mark_lost(&agent, 1, now + 1 + index)
            .expect("mark lost");
        lost_agents.push(agent);
    }
    let mut drained = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .startup_lost_agents(2, after)
            .expect("read startup loss page");
        if page.is_empty() {
            break;
        }
        after = page.last().map(|loss| (loss.updated_at, loss.agent_num));
        drained.extend(page.into_iter().map(|loss| loss.agent));
        assert!(drained.len() <= lost_agents.len() + 2, "pages stay bounded");
    }
    for agent in &lost_agents {
        assert!(
            drained.iter().any(|drained| drained == agent),
            "every eligible loss drains through the cursor"
        );
    }
    // Rows without retained evidence are skipped by the caller, never
    // synthesized: the query surfaces them with no sequence.
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-startup-noseq", now);
    store.mark_lost(&agent, 1, now + 1).expect("mark lost");
    let agent_num = eliot_core::parse_agent_id(&agent).expect("valid Agent id");
    let pruned: i64 = store
        .connection
        .query_row(
            "SELECT seq FROM events WHERE kind = 'agent.lost' AND agent = ?1 AND generation = 1",
            [agent_num],
            |row| row.get(0),
        )
        .expect("loss event exists");
    store
        .connection
        .execute("DELETE FROM events WHERE seq = ?1", [pruned])
        .expect("prune loss evidence");
    let losses = store
        .startup_lost_agents(8, None)
        .expect("read pruned losses");
    assert_eq!(losses.len(), 1);
    assert_eq!(
        losses[0].event_seq, None,
        "missing evidence surfaces as no sequence, never seq 0"
    );
}
