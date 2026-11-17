use super::*;

#[test]
fn opencode_launch_passes_admission_and_dispatch_gates() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": "opencode-launch-gates",
        "harness": "opencode",
        "profile": "opencode-test",
        "profile_snapshot": {"harness": "opencode", "model": "test-provider/test-model"},
        "workspace": "synthetic-opencode-launch",
    });
    launch.expected_generation = Some(0);
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit opencode launch")
        .command
        .target
        .expect("opencode launch Agent");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch opencode launch");
    install_fixture_sources(&mut store, &agent, &launch_id, 1, now + 1);
    store
        .put_batch(
            SourceBatch {
                batch_id: "opencode-launch-gates:ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "opencode-launch-gates:ready".to_owned(),
                    data: json!({
                        "command_id": launch_id.as_str(),
                        "native_scope": "opencode-store:test",
                        "native_ref": "ses_test00000000000000000001",
                    }),
                }],
                cursor: None,
            },
            now + 2,
        )
        .expect("commit opencode session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .expect("settle opencode launch");
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().harness,
        "opencode"
    );
}

fn end_muse_session(store: &mut Store, agent: &str, project: &str, now: i64) {
    install_live_custody_observations(store, agent, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ended"),
                agent_id: agent.to_owned(),
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
            now,
        )
        .expect("commit muse terminal session fact");
    simulate_normal_stop_proof(store, agent, 1, now);
}

fn gen1_event_rows(store: &Store, agent: &str) -> i64 {
    store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE agent = ?1 AND generation = 1",
            [eliot_core::parse_agent_id(agent).expect("valid Agent id")],
            |row| row.get(0),
        )
        .expect("count generation-1 event rows")
}

#[test]
fn muse_resume_passes_admission_and_dispatch_gates_with_exact_linked_input() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "muse-resume-gates";
    let (agent, _, native_scope, native_ref) = ready_muse_agent(
        &mut store,
        project,
        "muse-test",
        "synthetic-muse-resume",
        now,
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().harness, "muse");

    // Note 34c: Eliot's own terminal gate for the old generation. A resume
    // while generation 1 is still running is refused before any native I/O;
    // the plugin's `notLoaded` preflight never substitutes for this gate.
    let mut early = SubmitCommand::new(Ulid::new().to_string(), "resume");
    early.target = Some(agent.clone());
    early.expected_generation = Some(1);
    early.args = json!({"text": "too early"});
    assert_eq!(
        store
            .submit("owner", early, now + 5)
            .expect_err("live generation refuses resume")
            .code,
        "bad_state"
    );
    end_muse_session(&mut store, &agent, project, now + 6);

    // Admission mints the linked SEND id before any write: the exact
    // `{resume}/send` handle the new generation will dispatch, carrying the
    // exact linked text while the parent keeps empty args.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "continue in the new generation"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit("owner", resume, now + 7)
        .expect("admit muse resume with linked input");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(admission.command.generation, Some(1));
    assert_eq!(admission.command.args, json!({}));
    let linked = admission
        .linked_command
        .as_ref()
        .expect("resume text is stored as its linked SEND");
    let send_id = linked.id.clone();
    assert_eq!(send_id, format!("{resume_id}/send"));
    assert_eq!(linked.verb, "send");
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], "continue in the new generation");

    assert_eq!(
        store
            .dispatch(&resume_id, now + 8)
            .expect("dispatch muse resume")
            .value
            .state,
        CommandState::Dispatched
    );
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 8);
    let starting = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(starting.generation, 2);
    assert_eq!(starting.state, AgentState::Starting);
    assert_eq!(
        starting.native_scope.as_deref(),
        Some(native_scope.as_str())
    );
    assert_eq!(starting.native_ref.as_deref(), Some(native_ref.as_str()));

    // Note 34b (Store half): generation-1 output after the resume is
    // rejected, never applied to generation 2. The old host's batch cannot
    // land new rows once the Agent has advanced.
    let gen1_rows = gen1_event_rows(&store, &agent);
    assert_eq!(
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:stale-turn"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("{project}:stale-turn"),
                        data: json!({"command_id": "gen1-command", "turn_id": "gen1-turn"}),
                    }],
                    cursor: None,
                },
                now + 9,
            )
            .expect_err("stale generation-1 batch is rejected")
            .code,
        "stale_generation"
    );
    assert_eq!(gen1_event_rows(&store, &agent), gen1_rows);

    // Generation-2 readiness binds the exact resume command id minted before
    // the write; a forged command id proves nothing.
    assert_eq!(
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:wrong-command"),
                    agent_id: agent.clone(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:wrong-command"),
                        data: json!({
                            "command_id": "not-the-resume-command",
                            "native_scope": native_scope.as_str(),
                            "native_ref": native_ref.as_str(),
                        }),
                    }],
                    cursor: None,
                },
                now + 10,
            )
            .expect_err("forged readiness command is rejected")
            .code,
        "source_integrity"
    );
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready-2"),
                    data: json!({
                        "command_id": resume_id.as_str(),
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 11,
        )
        .expect("commit generation-2 readiness");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 12)
        .expect("settle muse resume");
    assert_eq!(
        store
            .dispatch(&send_id, now + 13)
            .expect("dispatch linked muse input")
            .value
            .state,
        CommandState::Dispatched
    );
    let turn_id = format!("muse-resume-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:turn-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("{project}:turn-2-started"),
                    data: json!({
                        "command_id": send_id.as_str(),
                        "turn_id": turn_id.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 14,
        )
        .expect("associate the exact linked muse input");
    let applied = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(applied.state, CommandState::Applied);
    assert_eq!(applied.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(
        store.get_turn(&agent, 2, &turn_id).unwrap().unwrap().state,
        TurnState::Running
    );
}

#[test]
fn muse_project_resume_passes_the_projects_gate_with_linked_input() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "muse-project-resume";
    let checkout = "/synthetic-muse-project-resume";
    let principal = format!("project:{project}");
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: format!("{checkout}/.git"),
                git: None,
                profiles: vec!["muse-profile".to_owned()],
                actions: vec!["resume".to_owned(), "send".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now,
        )
        .expect("register project resume and linked send");
    let (agent, _, native_scope, native_ref) =
        ready_muse_agent(&mut store, project, "muse-profile", checkout, now + 1);
    end_muse_session(&mut store, &agent, project, now + 6);

    // The project principal's linked SEND passes the projects resume binding
    // gate (`projects.rs`), which admits the `muse` harness like the other real harnesses.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "project resume input"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit(&principal, resume, now + 7)
        .expect("admit project muse resume with linked input");
    let send_id = admission
        .linked_command
        .as_ref()
        .expect("project resume text is stored as its linked SEND")
        .id
        .clone();
    assert_eq!(send_id, format!("{resume_id}/send"));
    store
        .dispatch(&resume_id, now + 8)
        .expect("dispatch project muse resume");
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 8);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready-2"),
                    data: json!({
                        "command_id": resume_id.as_str(),
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 9,
        )
        .expect("commit project generation-2 readiness");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 10)
        .expect("settle project muse resume");
    store
        .dispatch(&send_id, now + 11)
        .expect("dispatch linked project input against its next-generation binding");
    let turn_id = format!("muse-project-resume-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:turn-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("{project}:turn-2-started"),
                    data: json!({
                        "command_id": send_id.as_str(),
                        "turn_id": turn_id.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 12,
        )
        .expect("associate the exact linked project input");
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Applied
    );
}

#[test]
fn muse_resume_gates_refuse_an_unlisted_harness() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");

    // Admission refuses resume for a harness outside codex/muse/claude/opencode/antigravity
    // even when the old generation is terminal with a retained native subject.
    let (agent, _, _, _) = ready_muse_agent(
        &mut store,
        "muse-harness-refusal",
        "muse-test",
        "synthetic-muse-harness-refusal",
        now,
    );
    end_muse_session(&mut store, &agent, "muse-harness-refusal", now + 5);
    store
        .connection
        .execute(
            "UPDATE agents SET harness = 'other' WHERE id = ?1",
            [eliot_core::parse_agent_id(&agent).expect("valid Agent id")],
        )
        .expect("retarget the Agent at an unlisted harness");
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    assert_eq!(
        store
            .submit("owner", resume, now + 6)
            .expect_err("unlisted harness refuses resume at admission")
            .code,
        "unsupported"
    );

    // Dispatch deliberately rechecks only generation, state, and pending
    // work: harness support was proven at admission and the harness is
    // frozen, so a resume admitted for `muse` dispatches on that proof
    // (#309 removed the redundant dispatch-time harness recheck).
    let (second, _, _, _) = ready_muse_agent(
        &mut store,
        "muse-harness-refusal-dispatch",
        "muse-test",
        "synthetic-muse-harness-refusal-dispatch",
        now + 10,
    );
    end_muse_session(
        &mut store,
        &second,
        "muse-harness-refusal-dispatch",
        now + 15,
    );
    let mut second_resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    second_resume.target = Some(second.clone());
    second_resume.expected_generation = Some(1);
    let second_id = second_resume.id.clone();
    store
        .submit("owner", second_resume, now + 16)
        .expect("admit the muse resume before the harness proof");
    assert_eq!(
        store
            .dispatch(&second_id, now + 17)
            .expect("admitted muse resume dispatches on its admission proof")
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn muse_unknown_resume_is_never_redriven() {
    use eliot_core::api::{CommandResolution, CommandResolveRequest};

    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "muse-unknown-resume";
    let (agent, _, _, _) = ready_muse_agent(
        &mut store,
        project,
        "muse-test",
        "synthetic-muse-unknown-resume",
        now,
    );
    end_muse_session(&mut store, &agent, project, now + 5);

    // Note 34d: the lost resume reply settles the command Unknown. After a
    // plugin restart Eliot must never drive it as a second `session/resume`
    // write: only a Store-confirmed dispatch reaches a plugin, and dispatch
    // requires an Accepted/Queued command.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "uncertain resume input"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit("owner", resume.clone(), now + 6)
        .expect("admit muse resume");
    let send_id = admission
        .linked_command
        .as_ref()
        .expect("unknown resume still banks its linked input")
        .id
        .clone();
    store
        .dispatch(&resume_id, now + 7)
        .expect("dispatch muse resume");
    store
        .settle(
            &resume_id,
            CommandState::Unknown,
            Some(json!({"reason": "resume reply was lost"})),
            now + 8,
        )
        .expect("settle the lost resume reply as unknown");

    // A replay of the same command id is idempotent: not new, no events, no
    // second dispatch — the router only schedules new admissions.
    let replay = store
        .submit("owner", resume, now + 9)
        .expect("replay the unknown resume id");
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.command.state, CommandState::Unknown);
    assert_eq!(
        replay
            .linked_command
            .as_ref()
            .expect("replay keeps the linked input")
            .id,
        send_id
    );

    // The Unknown command itself cannot dispatch again, and the Agent has
    // advanced past the old generation, so no fresh resume can target it
    // either. The linked input stays queued, never dispatched.
    assert_eq!(
        store
            .dispatch(&resume_id, now + 10)
            .expect_err("unknown resume cannot dispatch")
            .code,
        "bad_state"
    );
    let mut fresh = SubmitCommand::new(Ulid::new().to_string(), "resume");
    fresh.target = Some(agent.clone());
    fresh.expected_generation = Some(1);
    assert_eq!(
        store
            .submit("owner", fresh, now + 11)
            .expect_err("old generation no longer resumes")
            .code,
        "stale_generation"
    );
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Queued
    );

    // Owner resolution of the uncertain resume is failed-only: it records an
    // outcome without ever re-driving the native write.
    let refused = store
        .resolve_command(
            "owner",
            &CommandResolveRequest {
                id: Ulid::new().to_string(),
                command_id: resume_id.clone(),
                resolution: CommandResolution::Applied,
                reason: "must not claim an unproven resume".to_owned(),
            },
            now + 12,
        )
        .expect_err("unknown resume cannot resolve as applied");
    assert_eq!(refused.code, "bad_state");
    store
        .resolve_command(
            "owner",
            &CommandResolveRequest {
                id: Ulid::new().to_string(),
                command_id: resume_id.clone(),
                resolution: CommandResolution::Failed,
                reason: "owner confirmed the resume never took effect".to_owned(),
            },
            now + 13,
        )
        .expect("unknown resume resolves as failed");
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Failed
    );
}

#[test]
fn muse_replies_and_turn_controls_pass_admission_and_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "muse-request-replies";
    let (agent, _, _, _) = ready_muse_agent(
        &mut store,
        project,
        "muse-test",
        "synthetic-muse-request-replies",
        now,
    );
    let send = send_request(&agent, 1, "open a request turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 5).unwrap();
    store.dispatch(&send_id, now + 6).unwrap();
    let turn_id = format!("muse-request-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":send_id, "turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit the muse request turn");
    let open_request = |native_id: &str, request_id: &str, kind: &str| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id":send_id,
            "request_id":request_id,
            "turn_id":turn_id,
            "kind":kind,
            "details":{"session_id":"native-session","turn_id":turn_id},
        }),
    };
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    open_request("muse-reply-approve", "muse-request-approve", "approval"),
                    open_request("muse-reply-deny", "muse-request-deny", "approval"),
                    open_request("muse-reply-answer", "muse-request-answer", "user_input"),
                ],
                cursor: None,
            },
            now + 8,
        )
        .expect("commit muse pending requests");

    // A1 connects approve/deny/answer for `muse`: each admits against its
    // exact pending request and dispatches to the plugin's agent.command.
    for (verb, request_id) in [
        ("approve", "muse-request-approve"),
        ("deny", "muse-request-deny"),
        ("answer", "muse-request-answer"),
    ] {
        let mut reply = SubmitCommand::new(Ulid::new().to_string(), verb);
        reply.target = Some(agent.clone());
        reply.expected_generation = Some(1);
        reply.expected_turn_id = Some(turn_id.clone());
        reply.request_id = Some(request_id.to_owned());
        if verb == "answer" {
            reply.args = json!({"answers": {"q1": {"answers": ["proceed"]}}});
        }
        let reply_id = reply.id.clone();
        let admission = store
            .submit("owner", reply, now + 9)
            .unwrap_or_else(|error| panic!("admit muse {verb}: {error:?}"));
        assert_eq!(admission.command.state, CommandState::Accepted);
        assert_eq!(
            store
                .dispatch(&reply_id, now + 10)
                .unwrap_or_else(|error| panic!("dispatch muse {verb}: {error:?}"))
                .value
                .state,
            CommandState::Dispatched
        );
    }

    // A2 connects steer/interrupt for `muse`: each admits against the exact
    // active turn and dispatches to the plugin's agent.command.
    for verb in ["steer", "interrupt"] {
        let mut control = SubmitCommand::new(Ulid::new().to_string(), verb);
        control.target = Some(agent.clone());
        control.expected_generation = Some(1);
        control.expected_turn_id = Some(turn_id.clone());
        if verb == "steer" {
            control.args = json!({"text": "follow-up"});
        }
        let control_id = control.id.clone();
        let admission = store
            .submit("owner", control, now + 11)
            .unwrap_or_else(|error| panic!("admit muse {verb}: {error:?}"));
        assert_eq!(admission.command.state, CommandState::Accepted);
        assert_eq!(
            store
                .dispatch(&control_id, now + 12)
                .unwrap_or_else(|error| panic!("dispatch muse {verb}: {error:?}"))
                .value
                .state,
            CommandState::Dispatched
        );
    }
}

/// Launch an `opencode`-harness Agent through generation-1 readiness.
fn ready_opencode_agent(
    store: &mut Store,
    project: &str,
    profile: &str,
    workspace: &str,
    now: i64,
) -> (String, String, String, String) {
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": project,
        "harness": "opencode",
        "profile": profile,
        "profile_snapshot": {"harness": "opencode", "model": "test-provider/test-model"},
        "workspace": workspace,
    });
    launch.expected_generation = Some(0);
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit opencode launch")
        .command
        .target
        .expect("opencode launch Agent");
    let native_scope = format!("{project}:{agent}:scope");
    let native_ref = format!("{project}:{agent}:session");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch opencode launch");
    install_fixture_sources(store, &agent, &launch_id, 1, now + 1);
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
        .expect("commit opencode session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .expect("settle opencode launch");
    (agent, launch_id, native_scope, native_ref)
}

fn end_opencode_session(store: &mut Store, agent: &str, project: &str, now: i64) {
    install_live_custody_observations(store, agent, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ended"),
                agent_id: agent.to_owned(),
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
            now,
        )
        .expect("commit opencode terminal session fact");
    simulate_normal_stop_proof(store, agent, 1, now);
}

#[test]
fn opencode_resume_passes_admission_and_dispatch_gates() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "opencode-resume-gates";
    let (agent, _, native_scope, native_ref) = ready_opencode_agent(
        &mut store,
        project,
        "opencode-test",
        "synthetic-opencode-resume",
        now,
    );
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().harness,
        "opencode"
    );

    let mut early = SubmitCommand::new(Ulid::new().to_string(), "resume");
    early.target = Some(agent.clone());
    early.expected_generation = Some(1);
    assert_eq!(
        store
            .submit("owner", early, now + 5)
            .expect_err("live generation refuses resume")
            .code,
        "bad_state"
    );
    end_opencode_session(&mut store, &agent, project, now + 6);

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    let admission = store
        .submit("owner", resume, now + 7)
        .expect("admit opencode resume");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(admission.command.generation, Some(1));
    assert_eq!(
        store
            .dispatch(&resume_id, now + 8)
            .expect("dispatch opencode resume")
            .value
            .state,
        CommandState::Dispatched
    );
    let starting = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(starting.generation, 2);
    assert_eq!(starting.state, AgentState::Starting);
    assert_eq!(
        starting.native_scope.as_deref(),
        Some(native_scope.as_str())
    );
    assert_eq!(starting.native_ref.as_deref(), Some(native_ref.as_str()));
}

#[test]
fn opencode_replies_and_interrupt_pass_admission_and_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "opencode-request-replies";
    let (agent, _, _, _) = ready_opencode_agent(
        &mut store,
        project,
        "opencode-test",
        "synthetic-opencode-request-replies",
        now,
    );
    let send = send_request(&agent, 1, "open a request turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 5).unwrap();
    store.dispatch(&send_id, now + 6).unwrap();
    let turn_id = format!("opencode-request-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":send_id, "turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit the opencode request turn");
    let open_request = |native_id: &str, request_id: &str, kind: &str| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id":send_id,
            "request_id":request_id,
            "turn_id":turn_id,
            "kind":kind,
            "details":{"permission_id":request_id},
        }),
    };
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    open_request("opencode-reply-approve", "per_approve", "approval"),
                    open_request("opencode-reply-deny", "per_deny", "approval"),
                    open_request("opencode-reply-answer", "frm_answer", "question"),
                ],
                cursor: None,
            },
            now + 8,
        )
        .expect("commit opencode pending requests");

    for (verb, request_id) in [
        ("approve", "per_approve"),
        ("deny", "per_deny"),
        ("answer", "frm_answer"),
    ] {
        let mut reply = SubmitCommand::new(Ulid::new().to_string(), verb);
        reply.target = Some(agent.clone());
        reply.expected_generation = Some(1);
        reply.expected_turn_id = Some(turn_id.clone());
        reply.request_id = Some(request_id.to_owned());
        if verb == "answer" {
            reply.args = json!({"answers": {"q1": {"answers": ["proceed"]}}});
        }
        let reply_id = reply.id.clone();
        let admission = store
            .submit("owner", reply, now + 9)
            .unwrap_or_else(|error| panic!("admit opencode {verb}: {error:?}"));
        assert_eq!(admission.command.state, CommandState::Accepted);
        assert_eq!(
            store
                .dispatch(&reply_id, now + 10)
                .unwrap_or_else(|error| panic!("dispatch opencode {verb}: {error:?}"))
                .value
                .state,
            CommandState::Dispatched
        );
    }

    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(agent.clone());
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some(turn_id.clone());
    let interrupt_id = interrupt.id.clone();
    assert_eq!(
        store
            .submit("owner", interrupt, now + 11)
            .expect("admit opencode interrupt")
            .command
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .dispatch(&interrupt_id, now + 12)
            .expect("dispatch opencode interrupt")
            .value
            .state,
        CommandState::Dispatched
    );

    // Steer admits and dispatches at the Store level like the other
    // harnesses; control-verb capability is enforced by the PluginHost
    // manifest, not the Store (#332).
    let mut steer = SubmitCommand::new(Ulid::new().to_string(), "steer");
    steer.target = Some(agent.clone());
    steer.expected_generation = Some(1);
    steer.expected_turn_id = Some(turn_id.clone());
    steer.args = json!({"text": "follow-up"});
    let steer_id = steer.id.clone();
    assert_eq!(
        store
            .submit("owner", steer, now + 13)
            .expect("admit opencode steer")
            .command
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .dispatch(&steer_id, now + 14)
            .expect("dispatch opencode steer")
            .value
            .state,
        CommandState::Dispatched
    );
}

/// Launch a `claude`-harness Agent through generation-1 readiness,
/// returning the Agent, its launch command, and its retained native
/// subject. The session stays live; tests end it with
/// `end_claude_session` when they need a terminal old generation.
fn ready_claude_agent(
    store: &mut Store,
    project: &str,
    profile: &str,
    workspace: &str,
    now: i64,
) -> (String, String, String, String) {
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": project,
        "harness": "claude",
        "profile": profile,
        "profile_snapshot": {"harness": "claude", "model": "claude-test-model"},
        "workspace": workspace,
    });
    launch.expected_generation = Some(0);
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit claude launch")
        .command
        .target
        .expect("claude launch Agent");
    let native_scope = format!("{project}:{agent}:scope");
    let native_ref = format!("{project}:{agent}:session");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch claude launch");
    install_fixture_sources(store, &agent, &launch_id, 1, now + 1);
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
        .expect("commit claude session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .expect("settle claude launch");
    (agent, launch_id, native_scope, native_ref)
}

fn end_claude_session(store: &mut Store, agent: &str, project: &str, now: i64) {
    install_live_custody_observations(store, agent, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ended"),
                agent_id: agent.to_owned(),
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
            now,
        )
        .expect("commit claude terminal session fact");
    simulate_normal_stop_proof(store, agent, 1, now);
}

#[test]
fn claude_replies_and_interrupt_pass_admission_and_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "claude-interrupt-gates";
    let (agent, _, _, _) = ready_claude_agent(
        &mut store,
        project,
        "claude-test",
        "synthetic-claude-interrupt",
        now,
    );
    let send = send_request(&agent, 1, "run a turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 5).unwrap();
    store.dispatch(&send_id, now + 6).unwrap();
    let turn_id = format!("claude-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":send_id, "turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit the claude turn");

    // PR1 connects interrupt for `claude`: it admits against the exact
    // active turn and dispatches to the plugin's agent.command.
    let mut control = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    control.target = Some(agent.clone());
    control.expected_generation = Some(1);
    control.expected_turn_id = Some(turn_id.clone());
    let control_id = control.id.clone();
    let admission = store
        .submit("owner", control, now + 8)
        .expect("admit claude interrupt");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(
        store
            .dispatch(&control_id, now + 9)
            .expect("dispatch claude interrupt")
            .value
            .state,
        CommandState::Dispatched
    );

    let open_request = |native_id: &str, request_id: &str| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id": send_id,
            "request_id": request_id,
            "turn_id": turn_id,
            "kind": "permission",
            "details": {"tool_name": "Bash", "input": {"command": "ls"}},
        }),
    };
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    open_request("claude-reply-approve", "claude-request-approve"),
                    open_request("claude-reply-deny", "claude-request-deny"),
                    open_request("claude-reply-answer", "claude-request-answer"),
                ],
                cursor: None,
            },
            now + 8,
        )
        .expect("commit claude pending permission requests");

    // PR2 connects approve/deny/answer for `claude`: each admits against its
    // exact pending request and dispatches to the plugin's agent.command.
    // Control-verb capability moved to the PluginHost manifest (#332), so the
    // Store admits every known control verb, including steer.
    for (verb, request_id) in [
        ("approve", "claude-request-approve"),
        ("deny", "claude-request-deny"),
        ("answer", "claude-request-answer"),
    ] {
        let mut reply = SubmitCommand::new(Ulid::new().to_string(), verb);
        reply.target = Some(agent.clone());
        reply.expected_generation = Some(1);
        reply.expected_turn_id = Some(turn_id.clone());
        reply.request_id = Some(request_id.to_owned());
        if verb == "answer" {
            reply.args = json!({"answers": {"q1": {"answers": ["proceed"]}}});
        }
        let reply_id = reply.id.clone();
        let admission = store
            .submit("owner", reply, now + 9)
            .unwrap_or_else(|error| panic!("admit claude {verb}: {error:?}"));
        assert_eq!(admission.command.state, CommandState::Accepted);
        assert_eq!(
            store
                .dispatch(&reply_id, now + 10)
                .unwrap_or_else(|error| panic!("dispatch claude {verb}: {error:?}"))
                .value
                .state,
            CommandState::Dispatched
        );
    }

    // Steer admits and dispatches at the Store level like the other
    // harnesses; the native hook delivery is a daemon concern (#342).
    let mut steer = SubmitCommand::new(Ulid::new().to_string(), "steer");
    steer.target = Some(agent.clone());
    steer.expected_generation = Some(1);
    steer.expected_turn_id = Some(turn_id.clone());
    steer.args = json!({"text": "follow-up"});
    let steer_id = steer.id.clone();
    assert_eq!(
        store
            .submit("owner", steer, now + 11)
            .expect("admit claude steer")
            .command
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .dispatch(&steer_id, now + 12)
            .expect("dispatch claude steer")
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn claude_resume_passes_admission_and_dispatch_gates_with_exact_linked_input() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "claude-resume-gates";
    let (agent, _, native_scope, native_ref) = ready_claude_agent(
        &mut store,
        project,
        "claude-test",
        "synthetic-claude-resume",
        now,
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().harness, "claude");

    // Eliot's own terminal gate for the old generation. A resume while
    // generation 1 is still running is refused before any native I/O.
    let mut early = SubmitCommand::new(Ulid::new().to_string(), "resume");
    early.target = Some(agent.clone());
    early.expected_generation = Some(1);
    early.args = json!({"text": "too early"});
    assert_eq!(
        store
            .submit("owner", early, now + 5)
            .expect_err("live generation refuses resume")
            .code,
        "bad_state"
    );
    end_claude_session(&mut store, &agent, project, now + 6);

    // Admission mints the linked SEND id before any write: the exact
    // `{resume}/send` handle the new generation will dispatch, carrying the
    // exact linked text while the parent keeps empty args.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "continue in the new generation"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit("owner", resume, now + 7)
        .expect("admit claude resume with linked input");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(admission.command.generation, Some(1));
    assert_eq!(admission.command.args, json!({}));
    let linked = admission
        .linked_command
        .as_ref()
        .expect("resume text is stored as its linked SEND");
    let send_id = linked.id.clone();
    assert_eq!(send_id, format!("{resume_id}/send"));
    assert_eq!(linked.verb, "send");
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], "continue in the new generation");

    assert_eq!(
        store
            .dispatch(&resume_id, now + 8)
            .expect("dispatch claude resume")
            .value
            .state,
        CommandState::Dispatched
    );
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 8);
    let starting = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(starting.generation, 2);
    assert_eq!(starting.state, AgentState::Starting);
    assert_eq!(
        starting.native_scope.as_deref(),
        Some(native_scope.as_str())
    );
    assert_eq!(starting.native_ref.as_deref(), Some(native_ref.as_str()));

    // Generation-1 output after the resume is rejected, never applied to
    // generation 2. The old host's batch cannot land new rows once the
    // Agent has advanced.
    let gen1_rows = gen1_event_rows(&store, &agent);
    assert_eq!(
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:stale-turn"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: format!("{project}:stale-turn"),
                        data: json!({"command_id": "gen1-command", "turn_id": "gen1-turn"}),
                    }],
                    cursor: None,
                },
                now + 9,
            )
            .expect_err("stale generation-1 batch is rejected")
            .code,
        "stale_generation"
    );
    assert_eq!(gen1_event_rows(&store, &agent), gen1_rows);

    // Generation-2 readiness binds the exact resume command id minted before
    // the write; a forged command id proves nothing.
    assert_eq!(
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:wrong-command"),
                    agent_id: agent.clone(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:wrong-command"),
                        data: json!({
                            "command_id": "not-the-resume-command",
                            "native_scope": native_scope.as_str(),
                            "native_ref": native_ref.as_str(),
                        }),
                    }],
                    cursor: None,
                },
                now + 10,
            )
            .expect_err("forged readiness command is rejected")
            .code,
        "source_integrity"
    );
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready-2"),
                    data: json!({
                        "command_id": resume_id.as_str(),
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 11,
        )
        .expect("commit generation-2 readiness");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 12)
        .expect("settle claude resume");
    assert_eq!(
        store
            .dispatch(&send_id, now + 13)
            .expect("dispatch linked claude input")
            .value
            .state,
        CommandState::Dispatched
    );
    let turn_id = format!("claude-resume-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:turn-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("{project}:turn-2-started"),
                    data: json!({
                        "command_id": send_id.as_str(),
                        "turn_id": turn_id.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 14,
        )
        .expect("associate the exact linked claude input");
    let applied = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(applied.state, CommandState::Applied);
    assert_eq!(applied.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(
        store.get_turn(&agent, 2, &turn_id).unwrap().unwrap().state,
        TurnState::Running
    );
}

#[test]
fn claude_project_resume_passes_the_projects_gate_with_linked_input() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "claude-project-resume";
    let checkout = "/synthetic-claude-project-resume";
    let principal = format!("project:{project}");
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: format!("{checkout}/.git"),
                git: None,
                profiles: vec!["claude-profile".to_owned()],
                actions: vec!["resume".to_owned(), "send".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now,
        )
        .expect("register project resume and linked send");
    let (agent, _, native_scope, native_ref) =
        ready_claude_agent(&mut store, project, "claude-profile", checkout, now + 1);
    end_claude_session(&mut store, &agent, project, now + 6);

    // The project principal's linked SEND passes the projects resume binding
    // gate (`projects.rs`), which admits the `claude` harness like
    // Codex/Fake/Muse.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "project resume input"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit(&principal, resume, now + 7)
        .expect("admit project claude resume with linked input");
    let send_id = admission
        .linked_command
        .as_ref()
        .expect("project resume text is stored as its linked SEND")
        .id
        .clone();
    assert_eq!(send_id, format!("{resume_id}/send"));
    store
        .dispatch(&resume_id, now + 8)
        .expect("dispatch project claude resume");
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 8);
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready-2"),
                    data: json!({
                        "command_id": resume_id.as_str(),
                        "native_scope": native_scope.as_str(),
                        "native_ref": native_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 9,
        )
        .expect("commit project generation-2 readiness");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 10)
        .expect("settle project claude resume");
    store
        .dispatch(&send_id, now + 11)
        .expect("dispatch linked project input against its next-generation binding");
    let turn_id = format!("claude-project-resume-turn-{}", Ulid::new());
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:turn-2"),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("{project}:turn-2-started"),
                    data: json!({
                        "command_id": send_id.as_str(),
                        "turn_id": turn_id.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 12,
        )
        .expect("associate the exact linked project input");
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Applied
    );
}
