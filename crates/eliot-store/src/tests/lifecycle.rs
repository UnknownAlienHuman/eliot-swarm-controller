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
fn delete_supersedes_only_the_tree_items_in_an_unapplied_plan() {
    use eliot_core::api::{
        ArtifactKind, ArtifactView, CleanupApplyRequest, CleanupCandidate, CleanupPlan,
    };
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    for parent in [None, Some(1), None] {
        store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at,parent) VALUES ('p','codex','p','stopped',1,?1,?1,?2)",rusqlite::params![now,parent]).unwrap();
    }
    let mut candidate = |agent: &str, verdict: &str| {
        let artifact = ArtifactView {
            id: Ulid::new().to_string(),
            agent: agent.to_owned(),
            generation: 1,
            project: "p".to_owned(),
            tree: Vec::new(),
            path: format!("C:/scratch/{agent}"),
            kind: ArtifactKind::Temp,
            purpose: "old output".to_owned(),
            identity: agent.to_owned(),
            created_at: now,
            state: "registered".to_owned(),
            git: None,
        };
        store.record_known_artifact(artifact.clone()).unwrap();
        CleanupCandidate {
            artifact,
            bytes: None,
            verdict: verdict.to_owned(),
            reason: "reviewed".to_owned(),
        }
    };
    let plan = CleanupPlan {
        plan_id: Ulid::new().to_string(),
        created_at: now,
        items: vec![
            candidate("a1", "safe"),
            candidate("a2", "keep"),
            candidate("a3", "safe"),
        ],
        truncated: false,
    };
    store.retain_cleanup_plan("owner", &plan).unwrap();
    let delete_id = Ulid::new().to_string();
    let deleted = store
        .change_agent_tree(&delete_id, "a1", None, now)
        .unwrap();
    assert_eq!(deleted["affected"], 2);
    assert!(store.get_agent("a1").unwrap().is_none());
    assert!(store.get_agent("a2").unwrap().is_none());
    assert!(store.get_agent("a3").unwrap().is_some());
    let request = CleanupApplyRequest {
        id: Ulid::new().to_string(),
        plan_id: plan.plan_id.clone(),
        delete_remote: false,
        artifacts: Some(vec![plan.items[0].artifact.id.clone()]),
    };
    let (_, first, is_new) = store.begin_cleanup_apply("owner", &request, now).unwrap();
    assert!(is_new);
    assert_eq!(first.bytes_reclaimed, 0);
    for (outcome, input) in first.items[..2].iter().zip(&plan.items) {
        assert_eq!(outcome.artifact_id, input.artifact.id);
        assert_eq!(outcome.outcome, "skipped");
        assert_eq!(outcome.reason.as_deref(), Some("superseded"));
    }
    assert_eq!(first.items[2].outcome, "skipped");
    assert_eq!(first.items[2].reason.as_deref(), Some("not_selected"));
    assert_eq!(
        store
            .known_artifact_path("C:/scratch/a3")
            .unwrap()
            .unwrap()
            .state,
        "registered"
    );
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

#[test]
fn failed_agent_deletion_retains_custody_and_unknown_turn_guards() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('','antigravity','p','failed',1,?1,?1)",[now]).unwrap();
    store.connection.execute("INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (1,1,'stream','open','error',?1)",[now]).unwrap();
    let assert_busy = |store: &mut Store| {
        assert_eq!(
            store
                .change_agent_tree(&Ulid::new().to_string(), "a1", None, now)
                .unwrap_err()
                .code,
            "busy"
        );
        assert!(store.get_agent("a1").unwrap().is_some());
    };
    assert_busy(&mut store);
    store
        .connection
        .execute("UPDATE source_state SET state='closed' WHERE agent=1", [])
        .unwrap();
    insert_process(&store, "failed-start-boot", 10, 1, "agent:a1#1", "running");
    assert_busy(&mut store);
    store
        .connection
        .execute("UPDATE processes SET state='exited',exit_code=1", [])
        .unwrap();
    store.connection.execute("INSERT INTO turns(agent,generation,turn_id,state,started_at) VALUES (1,1,'failed-start-turn','unknown',?1)",[now]).unwrap();
    assert_busy(&mut store);
    store
        .connection
        .execute("DELETE FROM turns WHERE agent=1", [])
        .unwrap();
    store
        .connection
        .execute("UPDATE agents SET state='lost' WHERE id=1", [])
        .unwrap();
    assert_busy(&mut store);
    store
        .connection
        .execute("UPDATE agents SET state='failed' WHERE id=1", [])
        .unwrap();
    let id = Ulid::new().to_string();
    let reply = store.change_agent_tree(&id, "a1", None, now).unwrap();
    assert_eq!(reply["deleted"], true);
    assert!(store.get_agent("a1").unwrap().is_none());
    assert_eq!(
        store.change_agent_tree(&id, "a1", None, now).unwrap(),
        reply
    );
}

#[test]
fn a_lost_tree_archives_and_unarchives_but_does_not_delete() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let agent = ready_steer_agent(&mut store, "archive-lost", now);
    store.mark_lost(&agent, 1, now + 1).unwrap();
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().state,
        AgentState::Lost
    );
    assert_eq!(
        store
            .change_agent_tree(&Ulid::new().to_string(), &agent, None, now + 2)
            .unwrap_err()
            .code,
        "busy"
    );
    let reply = store
        .change_agent_tree(&Ulid::new().to_string(), &agent, Some(true), now + 2)
        .unwrap();
    assert_eq!(reply["archived"], true);
    assert!(store.get_agent(&agent).unwrap().unwrap().archived);
    store
        .change_agent_tree(&Ulid::new().to_string(), &agent, Some(false), now + 3)
        .unwrap();
    assert!(!store.get_agent(&agent).unwrap().unwrap().archived);
}

#[test]
fn archiving_an_ended_agent_requires_drained_custody_and_names_the_blocker() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    store.connection.execute("INSERT INTO agents(project,harness,profile,state,generation,created_at,updated_at) VALUES ('','antigravity','p','failed',1,?1,?1)",[now]).unwrap();
    insert_process(&store, "archive-boot", 4242, 1, "agent:a1#1", "running");
    let archive = |store: &mut Store, archived: bool| {
        store.change_agent_tree(&Ulid::new().to_string(), "a1", Some(archived), now)
    };
    let refused = |store: &mut Store, blocker: &str| {
        let error = archive(store, true).unwrap_err();
        assert_eq!(error.code, "busy");
        assert!(
            error.message.contains(&format!("a1 still has {blocker}")),
            "{}",
            error.message
        );
        assert!(!store.get_agent("a1").unwrap().unwrap().archived);
    };
    refused(&mut store, "running child process 4242 (agent:a1#1)");
    store
        .connection
        .execute("UPDATE agents SET state='lost' WHERE id=1", [])
        .unwrap();
    refused(&mut store, "running child process 4242 (agent:a1#1)");
    store
        .connection
        .execute("UPDATE processes SET state='exited',exit_code=1", [])
        .unwrap();
    store.connection.execute("INSERT INTO artifacts(id,agent,generation,path,body) VALUES ('art-archive',1,1,'C:/scratch/archive','{\"state\":\"unknown\"}')",[]).unwrap();
    refused(&mut store, "unknown artifact art-archive");
    store
        .connection
        .execute("DELETE FROM artifacts", [])
        .unwrap();
    store.connection.execute("INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (1,1,'stream','open','error',?1)",[now]).unwrap();
    refused(&mut store, "open stream source (generation 1)");
    store
        .connection
        .execute("UPDATE source_state SET state='closed' WHERE agent=1", [])
        .unwrap();
    // Eliot's git observer of a resumable lost Agent stays open: archive
    // does not wait for it, delete's drain would.
    store.connection.execute("INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (1,1,'git','open','ok',?1)",[now]).unwrap();
    assert_eq!(archive(&mut store, true).unwrap()["archived"], true);
    store
        .connection
        .execute("UPDATE agents SET state='failed' WHERE id=1", [])
        .unwrap();
    let error = store
        .change_agent_tree(&Ulid::new().to_string(), "a1", None, now)
        .unwrap_err();
    assert!(
        error.message.contains(
            "a1 still has open git source (generation 1); agent.delete requires drained custody"
        ),
        "{}",
        error.message
    );
    // Unarchive only reveals, so it needs no drain.
    insert_process(&store, "archive-boot", 4243, 1, "agent:a1#1", "unknown");
    assert_eq!(archive(&mut store, false).unwrap()["archived"], false);
    // An idle Agent keeps its live custody: archive never stops a generation.
    let idle = ready_steer_agent(&mut store, "archive-idle", now);
    insert_process(
        &store,
        "archive-boot",
        4244,
        1,
        &format!("agent:{idle}#1"),
        "running",
    );
    assert_eq!(
        store
            .change_agent_tree(&Ulid::new().to_string(), &idle, Some(true), now)
            .unwrap()["archived"],
        true
    );
}

#[test]
fn restart_stops_an_idle_agent_after_owned_exit_and_keeps_its_session() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let agent = ready_steer_agent(&mut store, "restart-exited-idle", now);
    install_live_custody_observations(&mut store, &agent, 1, now + 1);
    let send = send_request(&agent, 1, "complete before restart");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 2).unwrap();
    store.dispatch(&send_id, now + 3).unwrap();
    let turn_id = "completed-before-restart";
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": turn_id}),
            ),
            now + 4,
        )
        .unwrap();
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": send_id, "turn_id": turn_id, "status": "completed"}),
            ),
            now + 5,
        )
        .unwrap();
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 6);
    let before = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(before.state, AgentState::Idle);

    let events = store.recover_on_restart(now + 7).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, "agent.stopped");
    assert_eq!(events[0].data["reason"], "owned_exit_before_restart");
    let after = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(after.state, AgentState::Stopped);
    assert_eq!(after.generation, 1);
    assert_eq!(after.native_scope, before.native_scope);
    assert_eq!(after.native_ref, before.native_ref);
    let turn = store.get_turn(&agent, 1, turn_id).unwrap().unwrap();
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.outcome, Some(TurnOutcome::Success));
    assert!(store.recover_on_restart(now + 8).unwrap().is_empty());

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store.submit("owner", resume, now + 9).unwrap();
    store.dispatch(&resume_id, now + 10).unwrap();
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 2);
}

#[test]
fn restart_loses_an_idle_agent_with_live_custody_or_an_uncertain_send() {
    for exited in [false, true] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = Ulid::new().timestamp_ms() as i64;
        let agent = ready_steer_agent(&mut store, "restart-unproven-idle", now);
        install_live_custody_observations(&mut store, &agent, 1, now + 1);
        let send_id = exited.then(|| {
            let send = send_request(&agent, 1, "unobserved send");
            let id = send.id.clone();
            store.submit("owner", send, now + 2).unwrap();
            store.dispatch(&id, now + 3).unwrap();
            simulate_normal_stop_proof(&mut store, &agent, 1, now + 4);
            id
        });
        assert_eq!(
            store.get_agent(&agent).unwrap().unwrap().state,
            AgentState::Idle
        );

        let events = store.recover_on_restart(now + 5).unwrap();
        assert_eq!(
            store.get_agent(&agent).unwrap().unwrap().state,
            AgentState::Lost
        );
        assert!(events.iter().any(|event| {
            event.kind == "agent.lost" && event.data["reason"] == eliot_core::DAEMON_RESTART_REASON
        }));
        assert!(events.iter().all(|event| event.kind != "agent.stopped"));
        if let Some(id) = send_id {
            assert_eq!(
                store.get_command(&id).unwrap().unwrap().state,
                CommandState::Unknown
            );
        } else {
            let state: String = store
                .connection
                .query_row("SELECT state FROM processes", [], |row| row.get(0))
                .unwrap();
            assert_eq!(state, "unknown");
        }
    }
}

#[test]
fn an_archived_agent_lost_with_unresolved_custody_stays_in_the_default_list() {
    use crate::AgentListFilter;
    use eliot_core::api::ArchivedFilter;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let list = |store: &Store, archived: ArchivedFilter| {
        let page = store
            .list_agent_summaries(
                &AgentListFilter {
                    archived,
                    ..Default::default()
                },
                None,
                20,
                now,
                &SessionGrowthThresholds::default(),
            )
            .unwrap();
        let ids: Vec<String> = page.summaries.into_iter().map(|row| row.id).collect();
        assert_eq!(page.total, ids.len());
        ids
    };
    // An idle Agent keeps its live process when archived and leaves the
    // default list.
    let agent = ready_steer_agent(&mut store, "archive-loss", now);
    insert_process(
        &store,
        "archive-boot",
        4245,
        1,
        &format!("agent:{agent}#1"),
        "running",
    );
    store
        .change_agent_tree(&Ulid::new().to_string(), &agent, Some(true), now)
        .unwrap();
    assert!(list(&store, ArchivedFilter::Exclude).is_empty());
    // A daemon restart loses the generation and leaves the process
    // unresolved: the lost Agent shows in the default list, still archived.
    store.recover_on_restart(now + 1).unwrap();
    let lost = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(lost.state, AgentState::Lost);
    assert!(lost.archived);
    assert_eq!(
        list(&store, ArchivedFilter::Exclude),
        std::slice::from_ref(&agent)
    );
    assert_eq!(
        list(&store, ArchivedFilter::Only),
        std::slice::from_ref(&agent)
    );
    // Once the process is proven gone the archive hides it again.
    store
        .connection
        .execute("UPDATE processes SET state='exited',exit_code=0", [])
        .unwrap();
    assert!(list(&store, ArchivedFilter::Exclude).is_empty());
    assert_eq!(list(&store, ArchivedFilter::Only), [agent]);
}

#[test]
fn historical_unknown_turn_resolves_after_resume_and_unblocks_delete() {
    // #610: resuming with a retained unknown old turn used to make that turn
    // unresolvable (exact-generation gate) while archive/delete kept
    // rejecting it (whole-history scan). A superseded generation provably
    // ended when resume committed, so its unknown turns stay
    // owner-resolvable without touching the current generation.
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent_with_native_identity(
        &mut store,
        "resolve-historical",
        now,
        Some(("scope-610", "ref-610")),
    );
    let send = send_request(&agent, 1, "historical work");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 1).unwrap();
    store.dispatch(&send_id, now + 1).unwrap();
    install_live_custody_observations(&mut store, &agent, 1, now + 1);
    let turn_id = format!("turn-{send_id}");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": turn_id}),
            ),
            now + 2,
        )
        .unwrap();
    // Native loss without a terminal: the turn stays unknown and generation
    // 1 ends terminal with no active turn.
    store
        .put_batch(
            SourceBatch {
                batch_id: "historical:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "historical:ended".to_owned(),
                    data: json!({}),
                }],
            },
            now + 3,
        )
        .unwrap();
    // Custody closes without resolving the turn, releasing the generation
    // for a later resume while the unknown turn is retained.
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 3);
    let stuck = store.get_turn(&agent, 1, &turn_id).unwrap().unwrap();
    assert_eq!(stuck.state, TurnState::Unknown);
    let before = store.get_agent(&agent).unwrap().unwrap();
    assert!(before.state.is_terminal());
    assert!(before.active_turn.is_none());

    // Resume to generation 2.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store.submit("owner", resume, now + 4).unwrap();
    store.dispatch(&resume_id, now + 4).unwrap();
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 4);
    store
        .put_batch(
            SourceBatch {
                batch_id: "historical:resumed".to_owned(),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "historical:resumed".to_owned(),
                    data: json!({"command_id": resume_id, "native_scope": "scope-610", "native_ref": "ref-610"}),
                }],
            },
            now + 4,
        )
        .unwrap();
    store
        .settle(&resume_id, CommandState::Applied, None, now + 4)
        .unwrap();
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 2);

    // The old unknown turn resolves without a readback: generation 1
    // provably ended when resume committed.
    store
        .resolve_turn(
            "owner",
            &TurnResolveRequest {
                id: Ulid::new().to_string(),
                agent: agent.clone(),
                generation: 1,
                turn_id: turn_id.clone(),
                status: TurnResolveStatus::Failed,
                reason: "gen1 ended without a terminal; owner retires the assertion".to_owned(),
            },
            None,
            now + 5,
        )
        .expect("superseded unknown turn resolves");
    let resolved = store.get_turn(&agent, 1, &turn_id).unwrap().unwrap();
    assert_eq!(resolved.state, TurnState::Completed);
    assert_eq!(resolved.outcome, Some(TurnOutcome::Failure));

    // Future generations stay stale.
    let future = store
        .resolve_turn(
            "owner",
            &TurnResolveRequest {
                id: Ulid::new().to_string(),
                agent: agent.clone(),
                generation: 3,
                turn_id: turn_id.clone(),
                status: TurnResolveStatus::Failed,
                reason: "future generation is never resolvable".to_owned(),
            },
            None,
            now + 5,
        )
        .unwrap_err();
    assert_eq!(future.code, "stale_generation");
}

#[test]
fn detached_external_observer_retires_and_attached_observer_can_only_unarchive() {
    // #651: a successfully detached external observer has unknown state by
    // contract, no turns, no live commands and closed custody: it must
    // retire through archive/delete without pretending native stopped.
    // A still-attached observer keeps its open source and stays blocked.
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let attach = |store: &mut Store, scope: &str, reference: &str, now: i64| -> (String, String) {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "attach");
        request.args = json!({
            "profile": "reader", "harness": "codex",
            "native_scope": scope, "native_ref": reference,
            "mode": "observe", "profile_snapshot": {},
            "project": "observed",
        });
        let admitted = store.submit("owner", request, now).unwrap();
        let agent = admitted
            .command
            .target
            .expect("attach creates the observer");
        let attach_id = admitted.command.id.clone();
        store.dispatch(&attach_id, now).unwrap();
        let agent_row = store.get_agent(&agent).unwrap().unwrap();
        store
            .settle_observe_attach(
                &attach_id,
                json!({
                    "agent_id": agent, "generation": agent_row.generation,
                    "command_id": attach_id,
                    "native_scope": scope, "native_ref": reference,
                    "state": "attached", "observation_state": "unknown",
                    "metadata": {}, "coverage": {},
                }),
                now,
            )
            .unwrap();
        (agent, attach_id)
    };
    let detach = |store: &mut Store, agent: &str, scope: &str, reference: &str, now: i64| {
        let generation = store.get_agent(agent).unwrap().unwrap().generation;
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "detach");
        request.target = Some(agent.to_owned());
        request.args = json!({"mode": "observe"});
        request.expected_generation = Some(generation);
        let admitted = store.submit("owner", request, now).unwrap();
        let detach_id = admitted.command.id.clone();
        store.dispatch(&detach_id, now).unwrap();
        let agent_row = store.get_agent(agent).unwrap().unwrap();
        store
            .settle_observe_detach(
                &detach_id,
                json!({
                    "agent_id": agent, "generation": agent_row.generation,
                    "command_id": detach_id,
                    "native_scope": scope, "native_ref": reference,
                    "state": "detached", "observation_state": "unknown",
                }),
                now,
            )
            .unwrap();
    };

    // Attach then detach: no turns, no live commands, closed custody.
    let (observer, _) = attach(&mut store, "scope-651", "ref-651", now);
    detach(&mut store, &observer, "scope-651", "ref-651", now);
    let settled = store.get_agent(&observer).unwrap().unwrap();
    assert_eq!(settled.state, AgentState::Unknown);
    assert!(settled.observe_only);
    assert!(settled.active_turn.is_none());
    let archived = store
        .change_agent_tree(&Ulid::new().to_string(), &observer, Some(true), now)
        .expect("detached observer archives");
    assert_eq!(archived["archived"], true);
    assert!(store.get_agent(&observer).unwrap().unwrap().archived);
    let deleted = store
        .change_agent_tree(&Ulid::new().to_string(), &observer, None, now)
        .expect("detached observer deletes");
    assert_eq!(deleted["deleted"], true);
    assert!(store.get_agent(&observer).unwrap().is_none());
    assert!(!store.is_write_faulted());

    // A still-attached observer keeps its open source and stays blocked even
    // though Unknown observe-only rows now retire: custody still proves the
    // live reader.
    let (live, _) = attach(&mut store, "scope-651-live", "ref-651-live", now);
    for archived in [None, Some(true)] {
        let blocked = store
            .change_agent_tree(&Ulid::new().to_string(), &live, archived, now)
            .unwrap_err();
        assert_eq!(blocked.code, "busy");
        assert!(
            blocked.message.contains("open observe source"),
            "{}",
            blocked.message
        );
    }
    let unarchived = store
        .change_agent_tree(&Ulid::new().to_string(), &live, Some(false), now)
        .expect("unarchive skips custody for an external observer");
    assert_eq!(unarchived["archived"], false);
}

#[test]
fn reattached_archived_observer_clears_archived_and_lists_its_live_reader() {
    // #651: reattaching an archived observer opens a new `observe` source,
    // which clears the archive flag so the live reader is never hidden from
    // the default fleet filter.
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let attach = |store: &mut Store, now: i64| -> (String, String) {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "attach");
        request.args = json!({
            "profile": "reader", "harness": "codex",
            "native_scope": "scope-651-reattach", "native_ref": "ref-651-reattach",
            "mode": "observe", "profile_snapshot": {},
            "project": "observed",
        });
        let admitted = store.submit("owner", request, now).unwrap();
        let agent = admitted
            .command
            .target
            .expect("attach creates the observer");
        let attach_id = admitted.command.id.clone();
        store.dispatch(&attach_id, now).unwrap();
        let agent_row = store.get_agent(&agent).unwrap().unwrap();
        store
            .settle_observe_attach(
                &attach_id,
                json!({
                    "agent_id": agent, "generation": agent_row.generation,
                    "command_id": attach_id,
                    "native_scope": "scope-651-reattach", "native_ref": "ref-651-reattach",
                    "state": "attached", "observation_state": "unknown",
                    "metadata": {}, "coverage": {},
                }),
                now,
            )
            .unwrap();
        (agent, attach_id)
    };
    let list_default = |store: &Store| {
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

    let (observer, _) = attach(&mut store, now);
    let generation = store.get_agent(&observer).unwrap().unwrap().generation;
    let mut detach = SubmitCommand::new(Ulid::new().to_string(), "detach");
    detach.target = Some(observer.clone());
    detach.args = json!({"mode": "observe"});
    detach.expected_generation = Some(generation);
    let admitted = store.submit("owner", detach, now).unwrap();
    let detach_id = admitted.command.id.clone();
    store.dispatch(&detach_id, now).unwrap();
    let detached_row = store.get_agent(&observer).unwrap().unwrap();
    store
        .settle_observe_detach(
            &detach_id,
            json!({
                "agent_id": observer, "generation": detached_row.generation,
                "command_id": detach_id,
                "native_scope": "scope-651-reattach", "native_ref": "ref-651-reattach",
                "state": "detached", "observation_state": "unknown",
            }),
            now,
        )
        .unwrap();
    store
        .change_agent_tree(&Ulid::new().to_string(), &observer, Some(true), now)
        .expect("detached observer archives");
    assert!(store.get_agent(&observer).unwrap().unwrap().archived);
    assert!(
        list_default(&store)
            .summaries
            .iter()
            .all(|row| row.id != observer)
    );

    // Reattaching the same native subject reuses the archived row and opens a
    // new `observe` source, which clears the flag before the reader settles.
    let (same, _) = attach(&mut store, now + 1);
    assert_eq!(same, observer);
    let reattached = store.get_agent(&observer).unwrap().unwrap();
    assert!(!reattached.archived);
    assert!(
        store
            .observer_active(&observer, reattached.generation)
            .unwrap()
    );
    assert!(
        list_default(&store)
            .summaries
            .iter()
            .any(|row| row.id == observer)
    );
    assert!(!store.is_write_faulted());
}
