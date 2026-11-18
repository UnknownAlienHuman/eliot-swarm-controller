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

fn register_project(store: &mut Store, id: &str) {
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: id.into(),
                checkout: format!("/managed/{id}"),
                repo_common_dir: format!("/managed/{id}/.git"),
                git: None,
                profiles: vec!["approved".into()],
                actions: vec!["launch".into()],
                interpreters: std::collections::BTreeMap::new(),
                run_env: Vec::new(),
            },
            now(),
        )
        .unwrap();
}

fn with_target(r: &SwitchRequest, project: &str) -> SwitchRequest {
    let mut changed = r.clone();
    changed.id = Ulid::new().to_string();
    changed.launch.project = Some(project.to_owned());
    changed
}

#[allow(clippy::too_many_arguments)]
fn admit_target(
    s: &mut Store,
    id: &str,
    plan: &SwitchResponse,
    scope: &str,
    registered_project: &str,
    revision: Option<i64>,
    blank_scope: bool,
    preparation: Option<core::WorkspacePreparationInput>,
) -> String {
    let workspace = s
        .get_project_registration("owner", registered_project)
        .ok()
        .flatten()
        .map(|registration| registration.checkout)
        .unwrap_or_else(|| format!("/managed/{registered_project}"));
    let mut c = SubmitCommand::new(plan.launch_command.clone(), "launch");
    c.args = json!({"project":scope,"workspace":workspace,"harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree","model":"gemini-4-hydrogen-low"},"registered_project":registered_project});
    if let Some(revision) = revision {
        c.args[core::api::SWITCH_PROJECT_REVISION_ARG] = json!(revision);
    }
    if blank_scope {
        c.args[core::api::SWITCH_BLANK_SCOPE_ARG] = json!(true);
    }
    s.admit_switch_launch(id, c, preparation, now())
        .unwrap()
        .command
        .target
        .unwrap()
}

fn chat_source(s: &mut Store) -> (String, String) {
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({"chat": true, "workspace": "chat-unit", "harness": "codex", "profile": "worker", "profile_snapshot": {"control": "self"}});
    let admitted = s.submit("owner", launch, now()).unwrap().command;
    let a = admitted.target.unwrap();
    ready(s, &admitted.id, &a);
    let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
    send.target = Some(a.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text":"continued task"});
    let task = s.submit("owner", send, now()).unwrap().command.id;
    (a, task)
}

#[test]
fn switch_target_project_must_be_registered_at_reserve() {
    let (mut s, r) = setup();
    let target = with_target(&r, "ghost");
    assert_eq!(
        s.reserve_switch("owner", &target, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "not_found"
    );
    assert_eq!(s.agent_count().unwrap(), 1);
}

#[test]
fn switch_projectless_source_needs_a_target_project() {
    let (mut s, r) = setup();
    s.connection
        .execute(
            "UPDATE agents SET project='', project_scope=NULL WHERE id=?1",
            [parse_agent_id(&r.from).unwrap()],
        )
        .unwrap();
    assert_eq!(
        s.reserve_switch("owner", &r, &core::SecretPolicy::default(), now())
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_eq!(s.agent_count().unwrap(), 1);
}

#[test]
fn switch_continue_binds_target_registration_and_records_continuation() {
    let (mut s, r) = setup();
    register_project(&mut s, "app");
    let revision = s
        .get_project_registration("owner", "app")
        .unwrap()
        .unwrap()
        .revision;
    let t = with_target(&r, "app");
    let p = begin(&mut s, &t);
    // A prepared launch outside the target repository is refused.
    let mut foreign = SubmitCommand::new(p.launch_command.clone(), "launch");
    foreign.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree","model":"gemini-4-hydrogen-low"},"registered_project":"other"});
    foreign.args[core::api::SWITCH_PROJECT_REVISION_ARG] = json!(revision);
    assert_eq!(
        s.admit_switch_launch(&t.id, foreign, None, now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    // A revoked registration between preparation and admission is refused.
    let mut stale = SubmitCommand::new(p.launch_command.clone(), "launch");
    stale.args = json!({"project":"switch","workspace":"switch-unit","harness":"antigravity","model":"gemini-4-hydrogen-low","profile":"new","profile_snapshot":{"control":"tree","model":"gemini-4-hydrogen-low"},"registered_project":"app"});
    stale.args[core::api::SWITCH_PROJECT_REVISION_ARG] = json!(revision + 1);
    assert_eq!(
        s.admit_switch_launch(&t.id, stale, None, now())
            .unwrap_err()
            .code,
        "stale_assignment"
    );
    let b = admit_target(
        &mut s,
        &t.id,
        &p,
        "switch",
        "app",
        Some(revision),
        false,
        None,
    );
    let agent = s.get_agent(&b).unwrap().unwrap();
    assert_eq!(agent.project, "app");
    assert_eq!(agent.project_scope(), "switch");
    ready(&mut s, &p.launch_command, &b);
    let change = s
        .finish_switch(&t.id, &core::SecretPolicy::default(), now())
        .unwrap();
    assert_eq!(change.value.state, CommandState::Applied);
    // The completion's overlay event is the typed one every menu write
    // emits: B's row at its revision, so a loaded sidebar refreshes B.
    let event = change
        .events
        .iter()
        .find(|event| event.kind == core::api::ORGANIZATION_CHANGED_KIND)
        .expect("switch organization event");
    let event: core::api::OrganizationChangedEvent =
        serde_json::from_value(event.data.clone()).unwrap();
    event.validate().unwrap();
    assert_eq!(event.method, "swarm.switch");
    assert_eq!(event.rows.len(), 1);
    assert_eq!(event.rows[0].agent, b);
    assert_eq!(event.rows[0].revision, 1);
    let continued: Option<String> = s
        .connection
        .query_row(
            "SELECT continued_from FROM chat_organization WHERE agent=?1",
            [parse_agent_id(&b).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(continued.as_deref(), Some(t.from.as_str()));
}

#[test]
fn switch_chat_source_continues_into_a_project_with_blank_scope() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    register_project(&mut s, "app");
    let revision = s
        .get_project_registration("owner", "app")
        .unwrap()
        .unwrap()
        .revision;
    let (a, task) = chat_source(&mut s);
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a.clone(),
        from_generation: 1,
        task_command: task.clone(),
        commands: vec![task],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","project":"app"}),
        )
        .unwrap(),
    };
    let plan = begin(&mut s, &request);
    let b = admit_target(
        &mut s,
        &request.id,
        &plan,
        "",
        "app",
        Some(revision),
        true,
        None,
    );
    let agent = s.get_agent(&b).unwrap().unwrap();
    assert_eq!(agent.project, "app");
    assert_eq!(agent.project_scope(), "");
    ready(&mut s, &plan.launch_command, &b);
    let result = s
        .finish_switch(&request.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Applied);
    assert_eq!(result.handoff.unwrap().to, b);
    let projection = s
        .chat_organization(
            "owner",
            &core::api::ChatOrganizationRequest {
                project: Some("app".to_owned()),
                after: None,
                archived: core::api::ArchivedFilter::Exclude,
            },
            &core::api::ScopeLimits::default(),
            now(),
        )
        .unwrap();
    let member = projection
        .groups
        .iter()
        .flat_map(|group| &group.members)
        .find(|member| member.agent == b)
        .unwrap();
    assert_eq!(member.continued_from.as_deref(), Some(a.as_str()));
}

#[test]
fn switch_continue_accepts_preparation_bound_to_the_target_project() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let root = std::env::temp_dir().join(format!("eliot-k185-{}", Ulid::new()));
    let repo = root.join("repo");
    let checkout = repo.to_string_lossy().into_owned();
    let common_dir = repo.join(".git").to_string_lossy().into_owned();
    let revision = s
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: "app".into(),
                checkout: checkout.clone(),
                repo_common_dir: common_dir.clone(),
                git: None,
                profiles: vec!["approved".into()],
                actions: vec!["launch".into()],
                interpreters: std::collections::BTreeMap::new(),
                run_env: Vec::new(),
            },
            now(),
        )
        .unwrap()
        .value
        .revision;
    let (a, task) = chat_source(&mut s);
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a.clone(),
        from_generation: 1,
        task_command: task.clone(),
        commands: vec![task],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","project":"app"}),
        )
        .unwrap(),
    };
    let plan = begin(&mut s, &request);
    // The workspace record keeps the canonical checkout, and P's ID travels
    // separately in `registered_project`, so project-scoped automation accepts
    // B's ready worktree. A preparation whose project, checkout, or common dir
    // differs from the selected registration is refused before admission.
    let preparation = core::WorkspacePreparationInput {
        project: checkout.clone(),
        repo_path: checkout.clone(),
        repo_common_dir: common_dir.clone(),
        requested_branch: "swarm/k185-continue".to_owned(),
        base_ref: None,
        base_oid: "0".repeat(40),
        worktree_path: root.join("wt").to_string_lossy().into_owned(),
        frozen_setup: Vec::new(),
        generated: None,
    };
    for bad in [
        core::WorkspacePreparationInput {
            project: "app".to_owned(),
            ..preparation.clone()
        },
        core::WorkspacePreparationInput {
            repo_path: root.join("other").to_string_lossy().into_owned(),
            ..preparation.clone()
        },
        core::WorkspacePreparationInput {
            repo_common_dir: root.join("other/.git").to_string_lossy().into_owned(),
            ..preparation.clone()
        },
    ] {
        let mut c = SubmitCommand::new(plan.launch_command.clone(), "launch");
        c.args = json!({
            "project": "",
            "workspace": checkout,
            "harness": "antigravity",
            "model": "gemini-4-hydrogen-low",
            "profile": "new",
            "profile_snapshot": {"control": "tree", "model": "gemini-4-hydrogen-low"},
            "registered_project": "app",
            core::api::SWITCH_PROJECT_REVISION_ARG: revision,
            core::api::SWITCH_BLANK_SCOPE_ARG: true,
        });
        assert_eq!(
            s.admit_switch_launch(&request.id, c, Some(bad), now())
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }
    let b = admit_target(
        &mut s,
        &request.id,
        &plan,
        "",
        "app",
        Some(revision),
        true,
        Some(preparation.clone()),
    );
    let agent = s.get_agent(&b).unwrap().unwrap();
    assert_eq!(agent.project, "app");
    assert_eq!(agent.project_scope(), "");
    // The frozen worktree still settles through the ordinary workspace
    // lifecycle before native dispatch.
    s.settle_workspace_creation(
        &plan.launch_command,
        1,
        core::WorkspaceCreationOutcome::Verified {
            repo_path: preparation.repo_path.clone(),
            repo_common_dir: preparation.repo_common_dir.clone(),
            worktree_path: preparation.worktree_path.clone(),
            branch: preparation.requested_branch.clone(),
            head: preparation.base_oid.clone(),
        },
        now(),
    )
    .unwrap();
    ready(&mut s, &plan.launch_command, &b);
    let result = s
        .finish_switch(&request.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(result.state, CommandState::Applied);
    let stored: String = s
        .connection
        .query_row(
            "SELECT project FROM workspaces WHERE agent=?1",
            [parse_agent_id(&b).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored, checkout);
    let continued: Option<String> = s
        .connection
        .query_row(
            "SELECT continued_from FROM chat_organization WHERE agent=?1",
            [parse_agent_id(&b).unwrap()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(continued.as_deref(), Some(a.as_str()));
}

#[test]
fn ordinary_worktree_switch_after_chat_continue_admits_against_registered_checkout() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let root = std::env::temp_dir().join(format!("eliot-k185c-store-{}", Ulid::new()));
    let repo = root.join("repo");
    let checkout = repo.to_string_lossy().into_owned();
    let common_dir = repo.join(".git").to_string_lossy().into_owned();
    let revision = s
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: "app".into(),
                checkout: checkout.clone(),
                repo_common_dir: common_dir.clone(),
                git: None,
                profiles: vec!["approved".into()],
                actions: vec!["launch".into()],
                interpreters: std::collections::BTreeMap::new(),
                run_env: Vec::new(),
            },
            now(),
        )
        .unwrap()
        .value
        .revision;
    let (a, task_a) = chat_source(&mut s);
    let cont_req = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a,
        from_generation: 1,
        task_command: task_a.clone(),
        commands: vec![task_a],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","project":"app"}),
        )
        .unwrap(),
    };
    let plan1 = begin(&mut s, &cont_req);
    let prep1 = core::WorkspacePreparationInput {
        project: checkout.clone(),
        repo_path: checkout.clone(),
        repo_common_dir: common_dir.clone(),
        requested_branch: "app/a2-retained".to_owned(),
        base_ref: None,
        base_oid: "0".repeat(40),
        worktree_path: root.join("wt-b").to_string_lossy().into_owned(),
        frozen_setup: Vec::new(),
        generated: None,
    };
    let b = admit_target(
        &mut s,
        &cont_req.id,
        &plan1,
        "",
        "app",
        Some(revision),
        true,
        Some(prep1.clone()),
    );
    s.settle_workspace_creation(
        &plan1.launch_command,
        1,
        core::WorkspaceCreationOutcome::Verified {
            repo_path: prep1.repo_path.clone(),
            repo_common_dir: prep1.repo_common_dir.clone(),
            worktree_path: prep1.worktree_path.clone(),
            branch: prep1.requested_branch.clone(),
            head: prep1.base_oid.clone(),
        },
        now(),
    )
    .unwrap();
    ready(&mut s, &plan1.launch_command, &b);
    s.finish_switch(&cont_req.id, &core::SecretPolicy::default(), now())
        .unwrap();

    let mut send_b = SubmitCommand::new(Ulid::new().to_string(), "send");
    send_b.target = Some(b.clone());
    send_b.expected_generation = Some(1);
    send_b.args = json!({"text": "next branch"});
    let task_b = s.submit("owner", send_b, now()).unwrap().command.id;

    let switch_req = SwitchRequest {
        id: Ulid::new().to_string(),
        from: b,
        from_generation: 1,
        task_command: task_b.clone(),
        commands: vec![task_b],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","worktree":"feature/next"}),
        )
        .unwrap(),
    };
    let plan2 = begin(&mut s, &switch_req);
    let mut launch_c = SubmitCommand::new(plan2.launch_command.clone(), "launch");
    launch_c.args = json!({
        "project": "",
        "workspace": prep1.worktree_path,
        "worktree": "feature/next",
        "harness": "antigravity",
        "model": "gemini-4-hydrogen-low",
        "profile": "new",
        "profile_snapshot": {"control": "tree", "model": "gemini-4-hydrogen-low"},
        "registered_project": "app",
        core::api::SWITCH_BLANK_SCOPE_ARG: true,
    });
    // Freezing the linked worktree path itself still fails admission; freezing
    // against P's registered checkout with the retained base commit succeeds.
    let wrong_prep = core::WorkspacePreparationInput {
        project: prep1.worktree_path.clone(),
        repo_path: prep1.worktree_path.clone(),
        repo_common_dir: common_dir.clone(),
        requested_branch: "feature/next".to_owned(),
        base_ref: None,
        base_oid: "1".repeat(40),
        worktree_path: root.join("wt-c").to_string_lossy().into_owned(),
        frozen_setup: Vec::new(),
        generated: None,
    };
    assert_eq!(
        s.admit_switch_launch(&switch_req.id, launch_c.clone(), Some(wrong_prep), now())
            .unwrap_err()
            .code,
        "invalid"
    );
    let fixed_prep = core::WorkspacePreparationInput {
        project: checkout.clone(),
        repo_path: checkout.clone(),
        repo_common_dir: common_dir,
        requested_branch: "feature/next".to_owned(),
        base_ref: None,
        base_oid: "1".repeat(40),
        worktree_path: root.join("wt-c").to_string_lossy().into_owned(),
        frozen_setup: Vec::new(),
        generated: None,
    };
    let c = s
        .admit_switch_launch(&switch_req.id, launch_c, Some(fixed_prep), now())
        .unwrap()
        .command
        .target
        .unwrap();
    let agent_c = s.get_agent(&c).unwrap().unwrap();
    assert_eq!(agent_c.project, "app");
    assert_eq!(agent_c.project_scope(), "");
    let ws_c = s.get_workspace(&c).unwrap().unwrap();
    assert_eq!(ws_c.project, checkout);
    assert_eq!(ws_c.repo_path, checkout);
    assert_eq!(ws_c.base_oid, "1".repeat(40));
    assert_eq!(ws_c.requested_branch, "feature/next");
}

#[test]
fn switch_marker_rechecks_registration_revision_and_identity_before_git_setup_and_dispatch() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let root = std::env::temp_dir().join(format!("eliot-k185b-gate-{}", Ulid::new()));
    let repo = root.join("repo");
    let checkout = repo.to_string_lossy().into_owned();
    let common_dir = repo.join(".git").to_string_lossy().into_owned();
    let input = ProjectRegistrationInput {
        id: "app".into(),
        checkout: checkout.clone(),
        repo_common_dir: common_dir.clone(),
        git: None,
        profiles: vec!["approved".into()],
        actions: vec!["launch".into()],
        interpreters: std::collections::BTreeMap::new(),
        run_env: Vec::new(),
    };
    let revision = s
        .upsert_project_registration("owner", &input, now())
        .unwrap()
        .value
        .revision;
    let (a, task) = chat_source(&mut s);
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a,
        from_generation: 1,
        task_command: task.clone(),
        commands: vec![task],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","project":"app"}),
        )
        .unwrap(),
    };
    let plan = begin(&mut s, &request);
    let setup_step = core::WorkspaceSetup {
        program: core::FrozenSetupFile {
            canonical_path: repo
                .join("scripts/setup.cmd")
                .to_string_lossy()
                .into_owned(),
            size: 12,
            sha256: "a".repeat(64),
        },
        args: Vec::new(),
        timeout_ms: 5_000,
    };
    let preparation = core::WorkspacePreparationInput {
        project: checkout.clone(),
        repo_path: checkout.clone(),
        repo_common_dir: common_dir.clone(),
        requested_branch: "swarm/k185b-gate".to_owned(),
        base_ref: None,
        base_oid: "0".repeat(40),
        worktree_path: root.join("wt").to_string_lossy().into_owned(),
        frozen_setup: vec![setup_step],
        generated: None,
    };
    let b = admit_target(
        &mut s,
        &request.id,
        &plan,
        "",
        "app",
        Some(revision),
        true,
        Some(preparation.clone()),
    );
    let current = s.get_workspace(&b).unwrap().unwrap();

    // 1. A registration revision change before Git refuses `begin_workspace_git_effect`.
    let mut bumped = input.clone();
    bumped.actions.push("send".into());
    s.upsert_project_registration("owner", &bumped, now())
        .unwrap();
    assert_eq!(
        s.begin_workspace_git_effect(&plan.launch_command, &current, now())
            .unwrap_err()
            .code,
        "stale_assignment"
    );
    // Restore the expected revision and reserve Git, then settle creation to SetupReady.
    s.connection
        .execute(
            "UPDATE project_registrations SET revision=?1 WHERE id='app'",
            [revision],
        )
        .unwrap();
    assert_eq!(
        s.begin_workspace_git_effect(&plan.launch_command, &current, now())
            .unwrap(),
        crate::WorkspaceGitReservation::Reserved
    );
    let setup_ready = s
        .settle_workspace_creation(
            &plan.launch_command,
            1,
            core::WorkspaceCreationOutcome::Verified {
                repo_path: preparation.repo_path.clone(),
                repo_common_dir: preparation.repo_common_dir.clone(),
                worktree_path: preparation.worktree_path.clone(),
                branch: preparation.requested_branch.clone(),
                head: preparation.base_oid.clone(),
            },
            now(),
        )
        .unwrap()
        .value;
    assert_eq!(setup_ready.phase, core::WorkspacePhase::SetupReady);

    // 2. A registration change before setup refuses `begin_workspace_setup`.
    s.upsert_project_registration("owner", &input, now())
        .unwrap();
    assert_eq!(
        s.begin_workspace_setup(&plan.launch_command, 1, 0, now())
            .unwrap_err()
            .code,
        "stale_assignment"
    );
    s.connection
        .execute(
            "UPDATE project_registrations SET revision=?1 WHERE id='app'",
            [revision],
        )
        .unwrap();
    s.begin_workspace_setup(&plan.launch_command, 1, 0, now())
        .unwrap();

    // 3. A registration change while setup is running stops native dispatch,
    // and pre-dispatch rejection settles the launch and its workspace cleanly.
    s.upsert_project_registration("owner", &bumped, now())
        .unwrap();
    let ready_ws = s
        .settle_workspace_setup(
            &plan.launch_command,
            1,
            0,
            core::WorkspaceSetupOutcome::Succeeded,
            now(),
        )
        .unwrap()
        .value;
    assert_eq!(ready_ws.phase, core::WorkspacePhase::Ready);
    assert_eq!(
        s.dispatch(&plan.launch_command, now()).unwrap_err().code,
        "stale_assignment"
    );
    // Identity drift at the same revision also refuses before dispatch.
    s.connection
        .execute(
            "UPDATE project_registrations SET revision=?1, repo_common_dir=?2 WHERE id='app'",
            params![
                revision,
                root.join(" swapped/.git").to_string_lossy().into_owned()
            ],
        )
        .unwrap();
    assert_eq!(
        s.dispatch(&plan.launch_command, now()).unwrap_err().code,
        "not_allowed"
    );
    let settled = s
        .settle(
            &plan.launch_command,
            CommandState::Rejected,
            Some(json!({"reason": "stale_assignment"})),
            now(),
        )
        .unwrap()
        .value;
    assert_eq!(settled.state, CommandState::Rejected);
    assert_eq!(
        s.get_workspace(&b).unwrap().unwrap().phase,
        core::WorkspacePhase::Failed
    );
    assert_eq!(s.get_agent(&b).unwrap().unwrap().state, AgentState::Failed);
}

#[test]
fn pre_v65_switch_without_project_field_replays_with_identical_digest() {
    let (mut s, mut r) = setup();
    r.launch.project = None;
    let encoded = serde_json::to_value(&r).unwrap();
    assert!(encoded["launch"].get("project").is_none());
    let first = s
        .reserve_switch("owner", &r, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    let replayed = s.replay_switch("owner", &r).unwrap().unwrap();
    assert_eq!(replayed.launch_command, first.launch_command);
    assert_eq!(replayed.handoff_command, first.handoff_command);
}

#[test]
fn ordinary_switch_after_continuation_uses_execution_project_and_preserves_both_fields() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    register_project(&mut s, "app");
    let revision = s
        .get_project_registration("owner", "app")
        .unwrap()
        .unwrap()
        .revision;
    let (a, task) = chat_source(&mut s);
    let first = SwitchRequest {
        id: Ulid::new().to_string(),
        from: a,
        from_generation: 1,
        task_command: task.clone(),
        commands: vec![task],
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low","project":"app"}),
        )
        .unwrap(),
    };
    let plan1 = begin(&mut s, &first);
    let b = admit_target(
        &mut s,
        &first.id,
        &plan1,
        "",
        "app",
        Some(revision),
        true,
        None,
    );
    ready(&mut s, &plan1.launch_command, &b);
    let finished1 = s
        .finish_switch(&first.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(finished1.state, CommandState::Applied);
    let b_task = finished1.handoff.unwrap().children[0].clone();
    let b_agent = s.get_agent(&b).unwrap().unwrap();
    assert_eq!(b_agent.project, "app");
    assert_eq!(b_agent.project_scope(), "");

    // Settle B's transferred brief so B is quiescent for the next ordinary switch.
    s.dispatch(&b_task, now()).unwrap();
    s.put_batch(
        SourceBatch {
            batch_id: Ulid::new().to_string(),
            agent_id: b.clone(),
            generation: 1,
            source: "stream".into(),
            cursor: None,
            events: vec![
                NativeEvent {
                    ts: None,
                    kind: "turn.started".into(),
                    native_id: "b-turn-start".into(),
                    data: json!({"turn_id": "t1", "command_id": b_task}),
                },
                NativeEvent {
                    ts: None,
                    kind: "turn.completed".into(),
                    native_id: "b-turn-done".into(),
                    data: json!({"turn_id": "t1", "command_id": b_task, "status": "ok", "final_text": "ready"}),
                },
            ],
        },
        now(),
    )
    .unwrap();
    s.settle(
        &b_task,
        CommandState::Applied,
        Some(json!({"turn_id": "t1"})),
        now(),
    )
    .unwrap();

    // Ordinary switch of B (launch.project is None): B's execution project is
    // "app" and its authorization namespace is "", so no target is required
    // and C preserves both fields.
    let second = SwitchRequest {
        id: Ulid::new().to_string(),
        from: b.clone(),
        from_generation: 1,
        task_command: b_task,
        commands: Vec::new(),
        launch: serde_json::from_value::<SwitchLaunch>(
            json!({"harness":"antigravity","model":"gemini-4-hydrogen-low"}),
        )
        .unwrap(),
    };
    let plan2 = begin(&mut s, &second);
    let mut wrong_project = SubmitCommand::new(plan2.launch_command.clone(), "launch");
    wrong_project.args = json!({
        "project": "",
        "workspace": "/managed/app",
        "harness": "antigravity",
        "model": "gemini-4-hydrogen-low",
        "profile": "new",
        "profile_snapshot": {"control": "tree", "model": "gemini-4-hydrogen-low"},
        core::api::SWITCH_BLANK_SCOPE_ARG: true,
    });
    assert_eq!(
        s.admit_switch_launch(&second.id, wrong_project, None, now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let c = admit_target(&mut s, &second.id, &plan2, "", "app", None, true, None);
    let c_agent = s.get_agent(&c).unwrap().unwrap();
    assert_eq!(c_agent.project, "app");
    assert_eq!(c_agent.project_scope(), "");
    ready(&mut s, &plan2.launch_command, &c);
    let finished2 = s
        .finish_switch(&second.id, &core::SecretPolicy::default(), now())
        .unwrap()
        .value;
    assert_eq!(finished2.state, CommandState::Applied);
    assert_eq!(finished2.handoff.unwrap().to, c);
}
