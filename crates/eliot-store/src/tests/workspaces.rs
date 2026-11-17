use super::*;

#[test]
fn pending_plain_workspace_resume_reserves_next_generation_and_stale_exit_cannot_release_it() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "resume-capacity", now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install persisted process source");
    let numeric_agent = eliot_core::parse_agent_id(&agent).unwrap();
    let process_owner = format!("agent:{agent}#1");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-boot', 81001, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![process_owner, now + 2],
        )
        .expect("persist old generation process exit");
    store
        .set_process_source_status(&agent, 1, "ok", true, now + 2)
        .expect("commit old generation process source closure");
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-capacity:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-capacity:ended".to_owned(),
                    data: json!({}),
                }],
            },
            now + 3,
        )
        .expect("commit terminal session fact");
    store
        .record_owned_control_exit(&agent, 1, now + 3)
        .expect("record owned control exit proof");
    assert_eq!(capacity_generation(&store, &agent), None);

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store
        .submit("owner", resume.clone(), now + 4)
        .expect("admit resume into the existing non-managed workspace");
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    let mut duplicate = resume;
    duplicate.id = Ulid::new().to_string();
    let high_water = store.high_water().unwrap();
    assert_eq!(
        store
            .submit("owner", duplicate.clone(), now + 5)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert!(store.get_command(&duplicate.id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), high_water);

    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 6)
            .expect("late generation-1 exit is harmless")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    store
        .dispatch(&resume_id, now + 7)
        .expect("dispatch exact reservation");
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().generation,
        2,
        "old generation cleanup cannot change the resumed generation"
    );
    assert!(numeric_agent > 0);
}

#[test]
fn managed_workspace_resume_rejection_restores_ready_after_project_revocation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "resume-withdrawal-project";
    let checkout = "/synthetic-resume-withdrawal";
    let profile = "resume-withdrawal-profile";
    let (agent, workspace_input) = ready_managed_codex_agent(&mut store, checkout, profile, now);
    let registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: format!("{checkout}/.git"),
        git: None,
        profiles: vec![profile.to_owned()],
        actions: vec!["resume".to_owned(), "send".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 5)
        .expect("grant Project resume and linked input");
    install_live_custody_observations(&mut store, &agent, 1, now + 5);
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-withdrawal-project:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-withdrawal-project:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 6,
        )
        .expect("commit retained terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 6);
    let old_agent = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(old_agent.generation, 1);
    assert_eq!(old_agent.state, AgentState::Stopped);
    let ready_workspace = store.get_workspace(&agent).unwrap().unwrap();

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "continue after revoked Project grant"});
    let resume_id = resume.id.clone();
    let principal = format!("project:{project}");
    let admission = store
        .submit_with_workspace(&principal, resume.clone(), workspace_input.clone(), now + 7)
        .expect("admit Project resume with linked input");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(admission.command.args, json!({}));
    let linked = admission
        .linked_command
        .expect("resume text is its linked SEND");
    let linked_id = linked.id.clone();
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.args["text"], "continue after revoked Project grant");
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    let pending_workspace = store.get_workspace(&agent).unwrap().unwrap();
    assert_eq!(pending_workspace.phase, WorkspacePhase::Creating);
    assert_eq!(pending_workspace.target_generation, 2);
    assert_eq!(pending_workspace.start_command_id, resume_id);

    store
        .reconcile_project_registrations("owner", &[], now + 8)
        .expect("revoke current Project registration");
    assert!(
        store
            .get_project_registration("owner", project)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .settle(
                &resume_id,
                CommandState::Rejected,
                Some(json!({"reason": "project_registration_revoked"})),
                now + 9,
            )
            .unwrap_err()
            .code,
        "bad_state",
        "generic settlement remains closed for a managed workspace resume"
    );
    let rejected = store
        .reject_workspace_resume(
            &resume.id,
            &json!({"reason":"project_registration_revoked"}),
            now + 10,
        )
        .expect("withdraw admitted resume without renewed Project authority");
    assert_eq!(rejected.value.state, CommandState::Rejected);
    assert_eq!(
        rejected.value.outcome.as_ref().unwrap()["native_dispatched"],
        false
    );
    assert!(rejected.events.iter().any(|event| {
        event.kind == "command.cancelled" && event.data["command_id"] == linked_id
    }));
    assert_eq!(
        store.get_command(&linked_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap(), old_agent);
    assert_eq!(capacity_generation(&store, &agent), None);
    let restored = store.get_workspace(&agent).unwrap().unwrap();
    assert_eq!(restored.phase, WorkspacePhase::Ready);
    assert_eq!(restored.target_generation, 1);
    assert_eq!(restored.start_command_id, ready_workspace.start_command_id);
    assert_eq!(restored.outcome, ready_workspace.outcome);
    assert_eq!(restored.setup_index, ready_workspace.setup_index);
    assert_eq!(restored.project, ready_workspace.project);
    assert_eq!(restored.repo_path, ready_workspace.repo_path);
    assert_eq!(restored.repo_common_dir, ready_workspace.repo_common_dir);
    assert_eq!(restored.requested_branch, ready_workspace.requested_branch);
    assert_eq!(restored.base_ref, ready_workspace.base_ref);
    assert_eq!(restored.base_oid, ready_workspace.base_oid);
    assert_eq!(restored.worktree_path, ready_workspace.worktree_path);
    assert_eq!(restored.frozen_setup, ready_workspace.frozen_setup);

    store
        .upsert_project_registration("owner", &registration, now + 11)
        .expect("restore the Project grant for a new explicit resume");
    resume.id = Ulid::new().to_string();
    let retry_id = resume.id.clone();
    let retry = store
        .submit_with_workspace(&principal, resume, workspace_input, now + 12)
        .expect("restored Ready generation admits a fresh resume");
    assert!(retry.is_new);
    assert_eq!(retry.command.state, CommandState::Accepted);
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    let retry_workspace = store.get_workspace(&agent).unwrap().unwrap();
    assert_eq!(retry_workspace.phase, WorkspacePhase::Creating);
    assert_eq!(retry_workspace.target_generation, 2);
    assert_eq!(retry_workspace.start_command_id, retry_id);
}

#[test]
fn managed_catch_up_resume_sibling_cancellation_restores_for_retry() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let rule_id = "managed-resume-sibling-cancel";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "session.ended"},
        "do": [{
            "action": "resume",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}",
            "text": "resume from captured terminal event",
        }],
        "limit": {"max": 2, "per": "rule", "window_ms": 60_000},
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let checkout = "/synthetic-resume-sibling";
    let (first_agent, _) =
        ready_managed_codex_agent(&mut store, checkout, "resume-sibling-first", now);
    let (second_agent, second_workspace) =
        ready_managed_codex_agent(&mut store, checkout, "resume-sibling-second", now + 10);
    let first_prior_workspace = store.get_workspace(&first_agent).unwrap().unwrap();
    let second_prior_workspace = store.get_workspace(&second_agent).unwrap().unwrap();
    install_live_custody_observations(&mut store, &first_agent, 1, now + 10);
    let first_end = store
        .put_batch(
            SourceBatch {
                batch_id: "managed-resume-sibling:first:ended".to_owned(),
                agent_id: first_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "managed-resume-sibling:first:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 20,
        )
        .expect("commit first captured terminal event");
    let first_event = first_end
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .expect("first terminal event")
        .seq;
    simulate_normal_stop_proof(&mut store, &first_agent, 1, now + 20);
    install_live_custody_observations(&mut store, &second_agent, 1, now + 20);
    let second_end = store
        .put_batch(
            SourceBatch {
                batch_id: "managed-resume-sibling:second:ended".to_owned(),
                agent_id: second_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "managed-resume-sibling:second:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 20,
        )
        .expect("commit second captured terminal event");
    let second_event = second_end
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .expect("second terminal event")
        .seq;
    simulate_normal_stop_proof(&mut store, &second_agent, 1, now + 20);
    let state = store
        .ensure_rule_state("owner", &rule, now + 21)
        .expect("capture both history events");
    let captured_through = state.current_cursor;
    let request = RuleCatchUpRequest::new(
        Ulid::new().to_string(),
        rule_id,
        first_event - 1,
        Some(captured_through),
    );
    let first_old_agent = store.get_agent(&first_agent).unwrap().unwrap();
    let second_old_agent = store.get_agent(&second_agent).unwrap().unwrap();
    let resume_request = |agent: &str| {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(1);
        request.args = json!({"text": "resume from captured terminal event"});
        request
    };
    let admission = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &[
                RuleCatchUpAction::command(first_event, resume_request(&first_agent)),
                RuleCatchUpAction::command(second_event, resume_request(&second_agent)),
            ],
            now + 22,
        )
        .expect("admit ordered managed resume siblings");
    assert_eq!(admission.children.len(), 2);
    let first_resume_id = admission.children[0].command.id.clone();
    let first_send_id = admission.children[0]
        .linked_command
        .as_ref()
        .unwrap()
        .id
        .clone();
    let second_resume_id = admission.children[1].command.id.clone();
    let second_send_id = admission.children[1]
        .linked_command
        .as_ref()
        .unwrap()
        .id
        .clone();
    assert_eq!(
        admission.children[1]
            .linked_command
            .as_ref()
            .unwrap()
            .generation,
        Some(2)
    );
    store
        .reject_workspace_resume(
            &first_resume_id,
            &json!({"reason":"fixture_pre_dispatch_rejection"}),
            now + 23,
        )
        .expect("withdraw the known no-effect predecessor");
    assert_eq!(
        store.get_command(&first_send_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    let cancelled = store
        .dispatch(&second_resume_id, now + 24)
        .expect("terminal predecessor cancels its undispatched sibling");
    assert_eq!(cancelled.value.state, CommandState::Cancelled);
    assert!(cancelled.events.iter().any(|event| {
        event.kind == "command.cancelled" && event.data["command_id"] == second_send_id
    }));
    assert_eq!(
        store.get_command(&second_send_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(
        store.get_agent(&first_agent).unwrap().unwrap(),
        first_old_agent
    );
    assert_eq!(
        store.get_agent(&second_agent).unwrap().unwrap(),
        second_old_agent
    );
    assert_eq!(capacity_generation(&store, &first_agent), None);
    assert_eq!(capacity_generation(&store, &second_agent), None);
    for (agent, original) in [
        (&first_agent, &first_prior_workspace),
        (&second_agent, &second_prior_workspace),
    ] {
        let restored = store.get_workspace(agent).unwrap().unwrap();
        assert_eq!(restored.phase, WorkspacePhase::Ready);
        assert_eq!(restored.target_generation, 1);
        assert_eq!(restored.start_command_id, original.start_command_id);
        assert_eq!(restored.outcome, original.outcome);
        assert_eq!(restored.project, original.project);
        assert_eq!(restored.repo_path, original.repo_path);
        assert_eq!(restored.repo_common_dir, original.repo_common_dir);
        assert_eq!(restored.requested_branch, original.requested_branch);
        assert_eq!(restored.base_ref, original.base_ref);
        assert_eq!(restored.base_oid, original.base_oid);
        assert_eq!(restored.worktree_path, original.worktree_path);
        assert_eq!(restored.frozen_setup, original.frozen_setup);
    }
    assert!(
        store
            .recover_rule_children(None, 100)
            .unwrap()
            .iter()
            .all(|child| child.command.id != second_resume_id)
    );

    let mut retry_request = SubmitCommand::new(Ulid::new().to_string(), "resume");
    retry_request.target = Some(second_agent.clone());
    retry_request.expected_generation = Some(1);
    retry_request.args = json!({"text": "resume from captured terminal event"});
    let retry_id = retry_request.id.clone();
    let retry = store
        .submit_with_workspace("owner", retry_request, second_workspace, now + 25)
        .expect("restored Ready workspace admits a fresh resume");
    assert!(retry.is_new);
    assert_eq!(retry.command.state, CommandState::Accepted);
    assert_eq!(capacity_generation(&store, &second_agent), Some(2));
    let retry_workspace = store.get_workspace(&second_agent).unwrap().unwrap();
    assert_eq!(retry_workspace.phase, WorkspacePhase::Creating);
    assert_eq!(retry_workspace.target_generation, 2);
    assert_eq!(retry_workspace.start_command_id, retry_id);
}

#[test]
fn ready_retained_resume_unknown_is_typed_and_dispatched_winner_is_refused() {
    {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let (agent, workspace_input) = ready_managed_codex_agent(
            &mut store,
            "/synthetic-ready-resume-unknown",
            "ready-resume-unknown-profile",
            now,
        );
        install_live_custody_observations(&mut store, &agent, 1, now + 4);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "ready-resume-unknown:ended".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: "ready-resume-unknown:ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now + 5,
            )
            .expect("commit retained terminal fact");
        simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);
        let old_agent = store.get_agent(&agent).unwrap().unwrap();
        let prior_workspace = store.get_workspace(&agent).unwrap().unwrap();
        let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
        resume.target = Some(agent.clone());
        resume.expected_generation = Some(1);
        resume.args = json!({"text": "keep pending input during readback"});
        let resume_id = resume.id.clone();
        let admission = store
            .submit_with_workspace("owner", resume, workspace_input.clone(), now + 6)
            .expect("admit retained managed Resume");
        let send_id = admission.linked_command.unwrap().id;
        assert_eq!(capacity_generation(&store, &agent), Some(2));

        store
            .settle_workspace_creation(
                &resume_id,
                2,
                WorkspaceCreationOutcome::Verified {
                    repo_path: workspace_input.repo_path.clone(),
                    repo_common_dir: workspace_input.repo_common_dir.clone(),
                    worktree_path: workspace_input.worktree_path.clone(),
                    branch: workspace_input.requested_branch.clone(),
                    head: workspace_input.base_oid.clone(),
                },
                now + 7,
            )
            .expect("complete retained identity inspection");
        let ready = store.get_workspace(&agent).unwrap().unwrap();
        assert_eq!(ready.phase, WorkspacePhase::Ready);
        assert_eq!(ready.target_generation, 2);
        assert_eq!(ready.setup_index, ready.frozen_setup.len());
        assert_eq!(ready.start_command_id, resume_id);

        let unknown = store
            .settle_workspace_creation(
                &resume_id,
                2,
                WorkspaceCreationOutcome::Unknown {
                    code: "retained_inspection_unknown".to_owned(),

                    message: "retained checkout inspection did not complete".to_owned(),

                    failure_where: None,

                    origin: None,

                    origin_evidence: None,
                },
                now + 8,
            )
            .expect("typed Unknown records the lost retained-workspace inspection");
        assert_eq!(unknown.value.phase, WorkspacePhase::Unknown);
        assert_eq!(unknown.value.target_generation, 2);
        assert_eq!(unknown.value.start_command_id, resume_id);
        assert_eq!(unknown.value.setup_index, prior_workspace.setup_index);
        assert_eq!(unknown.value.frozen_setup, prior_workspace.frozen_setup);
        assert_eq!(unknown.value.project, prior_workspace.project);
        assert_eq!(unknown.value.repo_path, prior_workspace.repo_path);
        assert_eq!(
            unknown.value.repo_common_dir,
            prior_workspace.repo_common_dir
        );
        assert_eq!(
            unknown.value.requested_branch,
            prior_workspace.requested_branch
        );
        assert_eq!(unknown.value.base_ref, prior_workspace.base_ref);
        assert_eq!(unknown.value.base_oid, prior_workspace.base_oid);
        assert_eq!(unknown.value.worktree_path, prior_workspace.worktree_path);
        assert!(
            unknown
                .value
                .outcome
                .as_ref()
                .unwrap()
                .get("resume_previous")
                .is_some()
        );
        assert_eq!(
            store.get_command(&resume_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        assert_eq!(
            store.get_command(&send_id).unwrap().unwrap().state,
            CommandState::Queued
        );
        assert_eq!(capacity_generation(&store, &agent), Some(2));
        let mut expected_agent = old_agent;
        expected_agent.workspace_phase = Some(WorkspacePhase::Unknown);
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), expected_agent);
        assert_eq!(store.get_agent_token(&agent, 2).unwrap(), None);
    }

    {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let (agent, workspace_input) = ready_managed_codex_agent(
            &mut store,
            "/synthetic-resume-winner-dispatched",
            "resume-winner-dispatched-profile",
            now,
        );
        install_live_custody_observations(&mut store, &agent, 1, now + 4);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "resume-winner-dispatched:ended".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: "resume-winner-dispatched:ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now + 5,
            )
            .expect("commit retained terminal fact");
        simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);
        let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
        resume.target = Some(agent.clone());
        resume.expected_generation = Some(1);
        resume.args = json!({"text": "input remains owned by dispatched Resume"});
        let resume_id = resume.id.clone();
        let admission = store
            .submit_with_workspace("owner", resume, workspace_input.clone(), now + 6)
            .expect("admit retained managed Resume");
        let send_id = admission.linked_command.unwrap().id;
        store
            .settle_workspace_creation(
                &resume_id,
                2,
                WorkspaceCreationOutcome::Verified {
                    repo_path: workspace_input.repo_path.clone(),
                    repo_common_dir: workspace_input.repo_common_dir.clone(),
                    worktree_path: workspace_input.worktree_path.clone(),
                    branch: workspace_input.requested_branch.clone(),
                    head: workspace_input.base_oid.clone(),
                },
                now + 7,
            )
            .expect("complete retained identity inspection");
        store
            .dispatch(&resume_id, now + 8)
            .expect("winner commits the N+1 generation before the losing readback");

        let dispatched_resume = store.get_command(&resume_id).unwrap().unwrap();
        let dispatched_agent = store.get_agent(&agent).unwrap().unwrap();
        let dispatched_workspace = store.get_workspace(&agent).unwrap().unwrap();
        let dispatched_token = store.get_agent_token(&agent, 2).unwrap();
        let high_water = store.high_water().unwrap();
        let event_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let error = store
            .settle_workspace_creation(
                &resume_id,
                2,
                WorkspaceCreationOutcome::Unknown {
                    code: "late_retained_inspection_error".to_owned(),

                    message: "losing inspection completed after native dispatch".to_owned(),

                    failure_where: None,

                    origin: None,

                    origin_evidence: None,
                },
                now + 9,
            )
            .expect_err("post-dispatch stale inspection cannot settle the winner");
        assert_eq!(error.code, "stale_generation");
        assert_eq!(
            store.get_command(&resume_id).unwrap().unwrap(),
            dispatched_resume
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), dispatched_agent);
        assert_eq!(
            store.get_workspace(&agent).unwrap().unwrap(),
            dispatched_workspace
        );
        assert_eq!(store.get_agent_token(&agent, 2).unwrap(), dispatched_token);
        assert_eq!(
            store.get_command(&send_id).unwrap().unwrap().state,
            CommandState::Queued
        );
        assert_eq!(capacity_generation(&store, &agent), Some(2));
        assert_eq!(store.high_water().unwrap(), high_water);
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            event_count
        );
    }
}

#[test]
fn confirmed_managed_resume_stop_restores_ready_and_allows_retry() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, workspace_input) = ready_managed_codex_agent(
        &mut store,
        "/synthetic-resume-stop-confirmed",
        "resume-stop-confirmed-profile",
        now,
    );
    install_live_custody_observations(&mut store, &agent, 1, now + 4);
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-stop-confirmed:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-stop-confirmed:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 5,
        )
        .expect("commit retained terminal fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);
    let old_agent = store.get_agent(&agent).unwrap().unwrap();
    let prior_workspace = store.get_workspace(&agent).unwrap().unwrap();

    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store
        .submit("owner", stop, now + 6)
        .expect("admit old-generation Stop before Resume");
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "retry after confirmed Stop"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit_with_workspace("owner", resume.clone(), workspace_input.clone(), now + 7)
        .expect("admit managed Resume with linked SEND");
    let send_id = admission.linked_command.unwrap().id;
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    store
        .request_workspace_cancel(&stop_id, &resume_id, 2, now + 8)
        .expect("latch the exact old-generation Stop against the Resume marker");
    let pending_workspace = store.get_workspace(&agent).unwrap().unwrap();
    assert_eq!(
        pending_workspace.outcome.as_ref().unwrap()["cancel_requested"],
        true
    );
    assert_eq!(
        pending_workspace.outcome.as_ref().unwrap()["stop_command_id"],
        stop_id
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );

    let settled = store
        .settle_workspace_cancel(
            &stop_id,
            &resume_id,
            2,
            WorkspaceCancelOutcome::Stopped,
            now + 9,
        )
        .expect("confirmed Stop settles and restores the retained Ready marker");
    assert_eq!(settled.value.phase, WorkspacePhase::Ready);
    assert_eq!(settled.value.target_generation, 1);
    assert_eq!(
        settled.value.start_command_id,
        prior_workspace.start_command_id
    );
    assert_eq!(settled.value.outcome, prior_workspace.outcome);
    assert_eq!(settled.value.project, prior_workspace.project);
    assert_eq!(settled.value.repo_path, prior_workspace.repo_path);
    assert_eq!(
        settled.value.repo_common_dir,
        prior_workspace.repo_common_dir
    );
    assert_eq!(
        settled.value.requested_branch,
        prior_workspace.requested_branch
    );
    assert_eq!(settled.value.base_ref, prior_workspace.base_ref);
    assert_eq!(settled.value.base_oid, prior_workspace.base_oid);
    assert_eq!(settled.value.worktree_path, prior_workspace.worktree_path);
    assert_eq!(settled.value.setup_index, prior_workspace.setup_index);
    assert_eq!(settled.value.frozen_setup, prior_workspace.frozen_setup);
    assert_eq!(store.get_agent(&agent).unwrap().unwrap(), old_agent);
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Applied
    );
    assert_eq!(capacity_generation(&store, &agent), None);

    resume.id = Ulid::new().to_string();
    let retry_id = resume.id.clone();
    let retry = store
        .submit_with_workspace("owner", resume, workspace_input, now + 10)
        .expect("restored Ready generation admits a fresh Resume");
    assert!(retry.is_new);
    assert_eq!(retry.command.state, CommandState::Accepted);
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    let retry_workspace = store.get_workspace(&agent).unwrap().unwrap();
    assert_eq!(retry_workspace.phase, WorkspacePhase::Creating);
    assert_eq!(retry_workspace.target_generation, 2);
    assert_eq!(retry_workspace.start_command_id, retry_id);
}

#[test]
fn managed_resume_sibling_waits_for_exact_workspace_stop_settlement() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let rule_id = "managed-resume-stop-sibling";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "session.ended"},
        "do": [{
            "action": "resume",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}",
            "text": "resume from captured terminal event",
        }],
        "limit": {"max": 2, "per": "rule", "window_ms": 60_000},
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let checkout = "/synthetic-resume-stop-sibling";
    let (first_agent, _) =
        ready_managed_codex_agent(&mut store, checkout, "resume-stop-first", now);
    let (second_agent, second_workspace_input) =
        ready_managed_codex_agent(&mut store, checkout, "resume-stop-second", now + 10);
    install_live_custody_observations(&mut store, &first_agent, 1, now + 10);
    let first_end = store
        .put_batch(
            SourceBatch {
                batch_id: "resume-stop-sibling:first:ended".to_owned(),
                agent_id: first_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-stop-sibling:first:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 20,
        )
        .expect("commit first terminal event");
    let first_event = first_end
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .unwrap()
        .seq;
    simulate_normal_stop_proof(&mut store, &first_agent, 1, now + 20);
    install_live_custody_observations(&mut store, &second_agent, 1, now + 20);
    let second_end = store
        .put_batch(
            SourceBatch {
                batch_id: "resume-stop-sibling:second:ended".to_owned(),
                agent_id: second_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-stop-sibling:second:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 20,
        )
        .expect("commit second terminal event");
    let second_event = second_end
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .unwrap()
        .seq;
    simulate_normal_stop_proof(&mut store, &second_agent, 1, now + 20);
    let second_old_agent = store.get_agent(&second_agent).unwrap().unwrap();
    let second_prior_workspace = store.get_workspace(&second_agent).unwrap().unwrap();
    let state = store
        .ensure_rule_state("owner", &rule, now + 21)
        .expect("capture sibling event history");
    let captured_through = state.current_cursor;
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(second_agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store
        .submit("owner", stop, now + 22)
        .expect("admit the exact old-generation Stop");
    let request = RuleCatchUpRequest::new(
        Ulid::new().to_string(),
        rule_id,
        first_event - 1,
        Some(captured_through),
    );
    let resume_request = |agent: &str| {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(1);
        request.args = json!({"text": "resume from captured terminal event"});
        request
    };
    let admission = store
        .admit_rule_catch_up(
            "owner",
            &request,
            &state,
            captured_through,
            &[
                RuleCatchUpAction::command(first_event, resume_request(&first_agent)),
                RuleCatchUpAction::command(second_event, resume_request(&second_agent)),
            ],
            now + 23,
        )
        .expect("admit ordered Resume siblings");
    let first_resume_id = admission.children[0].command.id.clone();
    let second_resume_id = admission.children[1].command.id.clone();
    let second_send_id = admission.children[1]
        .linked_command
        .as_ref()
        .unwrap()
        .id
        .clone();
    store
        .request_workspace_cancel(&stop_id, &second_resume_id, 2, now + 24)
        .expect("latch exact Stop ownership on the second Resume");
    store
        .reject_workspace_resume(
            &first_resume_id,
            &json!({"reason":"fixture_pre_dispatch_rejection"}),
            now + 25,
        )
        .expect("make the first Resume sibling terminal");

    let held = store
        .dispatch(&second_resume_id, now + 26)
        .expect("pending exact Stop holds sibling withdrawal");
    assert_eq!(held.value.state, CommandState::Queued);
    assert!(held.events.iter().any(|event| {
        event.kind == "command.queued" && event.data["reason"] == "workspace_stop_pending"
    }));
    assert_eq!(
        store.get_command(&second_send_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    assert_eq!(capacity_generation(&store, &second_agent), Some(2));
    let pending_workspace = store.get_workspace(&second_agent).unwrap().unwrap();
    assert_eq!(pending_workspace.start_command_id, second_resume_id);
    assert_eq!(
        pending_workspace.outcome.as_ref().unwrap()["cancel_requested"],
        true
    );
    assert_eq!(
        pending_workspace.outcome.as_ref().unwrap()["stop_command_id"],
        stop_id
    );

    let settled = store
        .settle_workspace_cancel(
            &stop_id,
            &second_resume_id,
            2,
            WorkspaceCancelOutcome::Stopped,
            now + 27,
        )
        .expect("exact Stop settlement owns restoration and Resume withdrawal");
    assert_eq!(settled.value.phase, WorkspacePhase::Ready);
    assert_eq!(settled.value.target_generation, 1);
    assert_eq!(
        settled.value.start_command_id,
        second_prior_workspace.start_command_id
    );
    assert_eq!(settled.value.outcome, second_prior_workspace.outcome);
    assert_eq!(
        store.get_agent(&second_agent).unwrap().unwrap(),
        second_old_agent
    );
    assert_eq!(
        store.get_command(&second_resume_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(
        store.get_command(&second_send_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Applied
    );
    assert_eq!(capacity_generation(&store, &second_agent), None);
    let retry = store
        .submit_with_workspace(
            "owner",
            resume_request(&second_agent),
            second_workspace_input,
            now + 28,
        )
        .expect("confirmed Stop restoration permits a fresh Resume");
    assert_eq!(retry.command.state, CommandState::Accepted);
    assert_eq!(capacity_generation(&store, &second_agent), Some(2));
}

#[test]
fn unknown_managed_resume_stop_preserves_marker_input_and_reservation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, workspace_input) = ready_managed_codex_agent(
        &mut store,
        "/synthetic-resume-stop-unknown",
        "resume-stop-unknown-profile",
        now,
    );
    install_live_custody_observations(&mut store, &agent, 1, now + 4);
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-stop-unknown:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-stop-unknown:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 5,
        )
        .expect("commit retained terminal fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);
    let old_agent = store.get_agent(&agent).unwrap().unwrap();
    let prior_workspace = store.get_workspace(&agent).unwrap().unwrap();
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now + 6).expect("admit Stop");
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "keep uncertain input"});
    let resume_id = resume.id.clone();
    let admission = store
        .submit_with_workspace("owner", resume, workspace_input, now + 7)
        .expect("admit managed Resume");
    let send_id = admission.linked_command.unwrap().id;
    store
        .request_workspace_cancel(&stop_id, &resume_id, 2, now + 8)
        .expect("latch exact Stop ownership");
    let unknown = store
        .settle_workspace_cancel(
            &stop_id,
            &resume_id,
            2,
            WorkspaceCancelOutcome::Unknown {
                code: "cleanup_unconfirmed".to_owned(),
                message: "Stop custody is unresolved".to_owned(),
            },
            now + 9,
        )
        .expect("retain unconfirmed Stop custody");
    assert_eq!(unknown.value.phase, WorkspacePhase::Unknown);
    assert_eq!(unknown.value.start_command_id, resume_id);
    assert_eq!(unknown.value.target_generation, 2);
    assert_eq!(
        unknown.value.outcome.as_ref().unwrap()["cancel_requested"],
        true
    );
    assert_eq!(
        unknown.value.outcome.as_ref().unwrap()["stop_command_id"],
        stop_id
    );
    let unknown_agent = store.get_agent(&agent).unwrap().unwrap();
    let mut expected_unknown_agent = old_agent.clone();
    expected_unknown_agent.workspace_phase = Some(WorkspacePhase::Unknown);
    assert_eq!(unknown_agent, expected_unknown_agent);
    assert_eq!(unknown.value.project, prior_workspace.project);
    assert_eq!(unknown.value.repo_path, prior_workspace.repo_path);
    assert_eq!(
        unknown.value.repo_common_dir,
        prior_workspace.repo_common_dir
    );
    assert_eq!(
        unknown.value.requested_branch,
        prior_workspace.requested_branch
    );
    assert_eq!(unknown.value.base_ref, prior_workspace.base_ref);
    assert_eq!(unknown.value.base_oid, prior_workspace.base_oid);
    assert_eq!(unknown.value.worktree_path, prior_workspace.worktree_path);
    assert_eq!(unknown.value.setup_index, prior_workspace.setup_index);
    assert_eq!(unknown.value.frozen_setup, prior_workspace.frozen_setup);
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    let high_water = store.high_water().unwrap();
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        store
            .reject_workspace_resume(
                &resume_id,
                &json!({"reason":"must_not_override_unknown_stop"}),
                now + 10
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.get_workspace(&agent).unwrap().unwrap(), unknown.value);
    assert_eq!(store.get_agent(&agent).unwrap().unwrap(), unknown_agent);
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        store.get_command(&send_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        store.get_command(&stop_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    assert_eq!(store.high_water().unwrap(), high_water);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        event_count
    );
    assert_eq!(prior_workspace.phase, WorkspacePhase::Ready);
}

#[test]
fn managed_resume_withdrawal_refuses_uncertain_inputs_and_foreign_markers() {
    for fence in [
        "dispatched_input",
        "unknown_input",
        "dispatched_resume",
        "unknown_resume",
        "unknown_stop",
        "foreign_marker",
    ] {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let (agent, workspace_input) = ready_managed_codex_agent(
            &mut store,
            "/synthetic-resume-withdrawal-fence",
            "resume-withdrawal-fence-profile",
            now,
        );
        let foreign_start_id = store
            .get_workspace(&agent)
            .unwrap()
            .unwrap()
            .start_command_id;
        let stop_id = if fence == "unknown_stop" {
            let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
            stop.target = Some(agent.clone());
            stop.expected_generation = Some(1);
            let id = stop.id.clone();
            store
                .submit("owner", stop, now + 5)
                .expect("admit old-generation stop before resume");
            Some(id)
        } else {
            None
        };
        install_live_custody_observations(&mut store, &agent, 1, now + 5);
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("resume-withdrawal-fence:{fence}:ended"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: format!("resume-withdrawal-fence:{fence}:ended"),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now + 6,
            )
            .expect("commit terminal fact");
        simulate_normal_stop_proof(&mut store, &agent, 1, now + 6);
        let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
        resume.target = Some(agent.clone());
        resume.expected_generation = Some(1);
        resume.args = json!({"text": "fenced retained input"});
        let resume_id = resume.id.clone();
        let admission = store
            .submit_with_workspace("owner", resume, workspace_input.clone(), now + 7)
            .expect("admit exact managed resume");
        let linked_id = admission.linked_command.unwrap().id;
        let mut reject_now = now + 8;

        match fence {
            "dispatched_input" | "unknown_input" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = ?1 WHERE id = ?2",
                        rusqlite::params![fence.strip_suffix("_input").unwrap(), linked_id],
                    )
                    .unwrap();
            }
            "dispatched_resume" | "unknown_resume" => {
                store
                    .settle_workspace_creation(
                        &resume_id,
                        2,
                        WorkspaceCreationOutcome::Verified {
                            repo_path: workspace_input.repo_path.clone(),
                            repo_common_dir: workspace_input.repo_common_dir.clone(),
                            worktree_path: workspace_input.worktree_path.clone(),
                            branch: workspace_input.requested_branch.clone(),
                            head: workspace_input.base_oid.clone(),
                        },
                        now + 8,
                    )
                    .expect("verify the retained worktree before native dispatch");
                store
                    .dispatch(&resume_id, now + 9)
                    .expect("dispatch the native resume");
                if fence == "unknown_resume" {
                    store
                        .settle(
                            &resume_id,
                            CommandState::Unknown,
                            Some(json!({"reason": "fixture_unknown_resume"})),
                            now + 10,
                        )
                        .expect("retain the unknown native resume result");
                    reject_now = now + 11;
                } else {
                    reject_now = now + 10;
                }
            }
            "unknown_stop" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = 'unknown' WHERE id = ?1",
                        [stop_id.as_deref().unwrap()],
                    )
                    .unwrap();
            }
            "foreign_marker" => {
                store
                    .connection
                    .execute(
                        "UPDATE workspaces SET start_command_id = ?1 WHERE agent = ?2",
                        rusqlite::params![
                            foreign_start_id,
                            eliot_core::parse_agent_id(&agent).unwrap()
                        ],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let command_before = store.get_command(&resume_id).unwrap().unwrap();
        let linked_before = store.get_command(&linked_id).unwrap().unwrap();
        let agent_before = store.get_agent(&agent).unwrap().unwrap();
        let workspace_before = store.get_workspace(&agent).unwrap().unwrap();
        let capacity_before = capacity_generation(&store, &agent);
        let high_water_before = store.high_water().unwrap();
        let event_count_before: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();

        let error = store
            .reject_workspace_resume(
                &resume_id,
                &json!({"reason":"fixture_rejection"}),
                reject_now,
            )
            .expect_err("uncertain or foreign custody must remain fenced");
        assert_eq!(
            error.code,
            if fence == "foreign_marker" {
                "stale_generation"
            } else {
                "bad_state"
            },
            "{fence}"
        );
        assert_eq!(
            store.get_command(&resume_id).unwrap().unwrap(),
            command_before
        );
        assert_eq!(
            store.get_command(&linked_id).unwrap().unwrap(),
            linked_before
        );
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), agent_before);
        assert_eq!(
            store.get_workspace(&agent).unwrap().unwrap(),
            workspace_before
        );
        assert_eq!(capacity_generation(&store, &agent), capacity_before);
        assert_eq!(store.high_water().unwrap(), high_water_before);
        let event_count_after: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count_after, event_count_before);
        assert!(!store.is_write_faulted());
    }
}

#[test]
fn exact_undispatched_resume_rejection_releases_only_its_next_generation_slot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "resume-withdrawal", now);
    install_live_custody_observations(&mut store, &agent, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: "resume-withdrawal:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "resume-withdrawal:ended".to_owned(),
                    data: json!({}),
                }],
            },
            now + 1,
        )
        .expect("commit terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 1);
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "newly admitted resume input"});
    let resume_id = resume.id.clone();
    let retained_request = resume.clone();
    let admission = store.submit("owner", resume, now + 2).unwrap();
    assert_eq!(admission.command.args, json!({}));
    let linked = admission
        .linked_command
        .expect("resume text is a linked SEND");
    let linked_id = linked.id.clone();
    assert_eq!(linked_id, format!("{resume_id}/send"));
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], "newly admitted resume input");
    let linked_cause = crate::command_cause_tx(&store.connection, &linked_id)
        .unwrap()
        .expect("ordinary linked SEND cause");
    assert_eq!(linked_cause.command_id, linked_id);
    assert!(linked_cause.rule.is_none());
    assert_eq!(linked_cause.depth, 0);
    let old_agent = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    let settled = store
        .settle(
            &resume_id,
            CommandState::Rejected,
            Some(json!({"reason":"native_not_dispatched"})),
            now + 3,
        )
        .expect("exact no-session rejection withdraws this resume");
    assert!(settled.events.iter().any(|event| {
        event.kind == "command.cancelled" && event.data["command_id"] == linked_id
    }));
    assert_eq!(
        store.get_command(&linked_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap(), old_agent);
    let pending_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE parent_command = ?1 AND link_kind = 'send' AND ordinal = 0
               AND state IN ('accepted','queued','dispatched','unknown')",
            [&resume_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        pending_children, 0,
        "pre-dispatch failure leaves no open input"
    );
    assert_eq!(capacity_generation(&store, &agent), None);
    let replay = store
        .submit("owner", retained_request, now + 4)
        .expect("the original rejected resume retains its cancellation");
    assert!(!replay.is_new);
    assert_eq!(replay.command.state, CommandState::Rejected);
    assert_eq!(
        replay.linked_command.unwrap().state,
        CommandState::Cancelled
    );

    let mut next = SubmitCommand::new(Ulid::new().to_string(), "resume");
    next.target = Some(agent.clone());
    next.expected_generation = Some(1);
    store
        .submit("owner", next, now + 5)
        .expect("a withdrawn resume no longer blocks a new exact resume");
    assert_eq!(capacity_generation(&store, &agent), Some(2));
}

#[test]
fn managed_workspace_pages_list_every_retained_worktree_with_total_and_cursor() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (first, first_input) =
        ready_managed_codex_agent(&mut store, "/synthetic-list", "list-a", now);
    let (second, second_input) =
        ready_managed_codex_agent(&mut store, "/synthetic-list", "list-b", now + 1);
    let (page, total, more) = store.list_managed_workspaces(None, 1).unwrap();
    assert_eq!((total, more), (2, true));
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].agent, first);
    assert_eq!(page[0].worktree_path, first_input.worktree_path);
    let (rest, total, more) = store
        .list_managed_workspaces(Some(&page[0].agent), 1)
        .unwrap();
    assert_eq!((total, more), (2, false));
    assert_eq!(rest[0].agent, second);
    assert_eq!(rest[0].worktree_path, second_input.worktree_path);
    let (after_last, total, more) = store.list_managed_workspaces(Some(&second), 10).unwrap();
    assert!(after_last.is_empty());
    assert_eq!((total, more), (2, false));
}
