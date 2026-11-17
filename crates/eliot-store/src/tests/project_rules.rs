use super::*;

fn project_send_rule_input(id: &str) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "on": {"event": "command.accepted"},
        "do": [{
            "action": "send",
            "target": "${event.agent}",
            "expected_generation": "${event.generation}"
        }]
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).expect("encode send rule"),
        definition_digest: eliot_core::canonical_json_sha256(&value).expect("digest send rule"),
        enabled: true,
    }
}

fn assert_no_rule_action_children(store: &Store, parent_id: &str) {
    let children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [parent_id],
            |row| row.get(0),
        )
        .expect("count rule-action children");
    assert_eq!(children, 0);
}

fn assert_live_send_binding_rejection(
    store: &Store,
    admission: RuleEventAdmission,
    event_seq: i64,
) {
    let parent = admission.parent.expect("rejected live-rule parent");
    assert_eq!(parent.state, CommandState::Rejected);
    assert_eq!(
        parent.outcome.as_ref().unwrap()["code"],
        "rule_action_binding_invalid"
    );
    assert_eq!(parent.outcome.as_ref().unwrap()["children"], json!([]));
    assert!(admission.child.is_none());
    assert_eq!(admission.state.current_cursor, event_seq);
    assert!(admission.events.iter().any(|event| {
        event.kind == "rule.rejected"
            && event.data["event_seq"] == event_seq
            && event.data["code"] == "rule_action_binding_invalid"
    }));
    assert_no_rule_action_children(store, &parent.id);
}

fn assert_history_send_binding_rejection(
    store: &Store,
    admission: RuleCatchUpAdmission,
    event_seq: i64,
) {
    assert_eq!(admission.parent.state, CommandState::Applied);
    assert_eq!(
        admission.parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert!(admission.children.is_empty());
    assert!(admission.events.iter().any(|event| {
        event.kind == "rule.rejected"
            && event.data["event_seq"] == event_seq
            && event.data["code"] == "rule_action_binding_invalid"
    }));
    assert_no_rule_action_children(store, &admission.parent.id);
}

fn launch_event_seq_for_agent(store: &Store, agent: &str) -> i64 {
    let agent_num = eliot_core::parse_agent_id(agent).expect("valid trigger Agent ID");
    store
        .connection
        .query_row(
            "SELECT seq FROM events
             WHERE agent = ?1 AND kind = 'command.accepted'
               AND json_extract(data, '$.verb') = 'launch'
             ORDER BY seq DESC LIMIT 1",
            [agent_num],
            |row| row.get(0),
        )
        .expect("read trigger launch event sequence")
}

fn recovered_admission_page_bytes(page: &[crate::Admission]) -> usize {
    let commands = page
        .iter()
        .map(|admission| &admission.command)
        .collect::<Vec<_>>();
    let linked = page
        .iter()
        .filter_map(|admission| admission.linked_command.as_ref())
        .collect::<Vec<_>>();
    serde_json::to_vec(&commands)
        .expect("encode recovered launch commands")
        .len()
        .saturating_add(
            serde_json::to_vec(&linked)
                .expect("encode recovered linked sends")
                .len(),
        )
}

fn record_recovered_launch_page(
    page: &[crate::Admission],
    expected_launch_ids: &[String],
    expected_send_ids: &[String],
    seen_launch_ids: &mut Vec<String>,
    seen_send_ids: &mut Vec<String>,
    launch_text: &str,
) {
    assert!(
        !page.is_empty(),
        "recovery must advance with a nonempty prefix"
    );
    assert!(recovered_admission_page_bytes(page) <= crate::MAX_PAGE_BYTES);
    for admission in page {
        let launch_id = admission.command.id.clone();
        assert!(expected_launch_ids.contains(&launch_id));
        assert!(
            !seen_launch_ids.contains(&launch_id),
            "launch recovered twice"
        );
        assert_eq!(admission.command.verb, "launch");
        assert_eq!(admission.command.expected_generation, Some(0));
        let linked = admission
            .linked_command
            .as_ref()
            .expect("recovery preserves the launch linked SEND");
        let send_id = linked.id.clone();
        assert!(expected_send_ids.contains(&send_id));
        assert!(
            !seen_send_ids.contains(&send_id),
            "linked SEND recovered twice"
        );
        assert_eq!(send_id, format!("{launch_id}/send"));
        assert_eq!(linked.verb, "send");
        assert_eq!(linked.expected_generation, Some(1));
        assert_eq!(linked.generation, Some(1));
        assert_eq!(linked.args["text"].as_str(), Some(launch_text));
        assert!(admission.events.is_empty());
        seen_launch_ids.push(launch_id);
        seen_send_ids.push(send_id);
    }
}

#[test]
fn project_command_binding_is_captured_and_revalidated_before_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-binding";
    let agent = ready_agent(&mut store, project, now);
    let registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: "synthetic-workspace".to_owned(),
        repo_common_dir: "synthetic-repository".to_owned(),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["stop".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    let registered = store
        .upsert_project_registration("owner", &registration, now + 1)
        .expect("register project")
        .value;
    assert_eq!(registered.revision, 1);
    assert_eq!(
        store
            .get_project_registration("owner", project)
            .expect("read project registration"),
        Some(registered.clone())
    );
    assert_eq!(
        store
            .list_project_registrations("owner", None, 10)
            .expect("list project registrations"),
        vec![registered.clone()]
    );

    let stop_id = Ulid::new().to_string();
    store
        .submit(
            &format!("project:{project}"),
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
            now + 2,
        )
        .expect("admit project stop under the registered profile/action");
    let binding: String = store
        .connection
        .query_row(
            "SELECT project_binding FROM commands WHERE id = ?1",
            [&stop_id],
            |row| row.get(0),
        )
        .expect("persist frozen project binding");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&binding).expect("decode project binding")["registration_revision"],
        json!(registered.revision)
    );

    assert!(
        store
            .reconcile_project_registrations("owner", &[], now + 3)
            .expect("remove project registration")
            .value
            .is_empty()
    );
    let next = store
        .upsert_project_registration("owner", &registration, now + 4)
        .expect("re-register project")
        .value;
    assert!(next.revision > registered.revision);

    let error = store
        .dispatch(&stop_id, now + 5)
        .expect_err("revoked project command must fail its dispatch fence");
    assert_eq!(error.code, "not_allowed");
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("read admitted stop")
            .expect("stop remains retained")
            .state,
        CommandState::Accepted,
        "failed dispatch validation must not dispatch or consume the command"
    );
}

#[test]
fn project_stop_rejects_setup_ready_next_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-workspace-binding";
    let start_id = Ulid::new().to_string();
    let checkout = "/synthetic-owner";
    let repo_common_dir = "/synthetic-owner/.git";
    let worktree_path = "/synthetic-worktree";
    let branch = "codex/project-binding";
    let base_oid = "b".repeat(40);
    let input = WorkspacePreparationInput {
        project: checkout.to_owned(),
        repo_path: checkout.to_owned(),
        repo_common_dir: repo_common_dir.to_owned(),
        requested_branch: branch.to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: base_oid.clone(),
        worktree_path: worktree_path.to_owned(),
        frozen_setup: vec![eliot_core::WorkspaceSetup {
            program: eliot_core::FrozenSetupFile {
                canonical_path: std::env::temp_dir()
                    .join("eliot-project-setup")
                    .to_string_lossy()
                    .into_owned(),
                size: 1,
                sha256: "a".repeat(64),
            },
            args: Vec::new(),
            timeout_ms: 1_000,
        }],
    };
    let launch = store
        .submit_with_workspace(
            "owner",
            SubmitCommand {
                id: start_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({
                    "project": checkout,
                    "workspace": checkout,
                    "worktree": branch,
                    "harness": "codex",
                    "profile": "fake",
                    "profile_snapshot": {}
                }),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            input.clone(),
            now,
        )
        .expect("admit managed project launch");
    let agent = launch.command.target.expect("workspace Agent");
    store
        .settle_workspace_creation(
            &start_id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path.clone(),
                repo_common_dir: input.repo_common_dir.clone(),
                worktree_path: input.worktree_path.clone(),
                branch: input.requested_branch.clone(),
                head: input.base_oid.clone(),
            },
            now + 1,
        )
        .expect("verify L3 workspace with canonical checkout project");
    let (workspace_phase, target_generation): (String, i64) = store
        .connection
        .query_row(
            "SELECT phase, target_generation FROM workspaces WHERE agent = ?1",
            [eliot_core::parse_agent_id(&agent).expect("valid Agent id")],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read L3 workspace phase");
    assert_eq!(workspace_phase, "setup_ready");
    assert_eq!(target_generation, 1);
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 0);
    let registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: repo_common_dir.to_owned(),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["stop".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 2)
        .expect("register project");
    let stop_id = Ulid::new().to_string();
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events before denied stop");
    let error = store
        .submit(
            &format!("project:{project}"),
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 3,
        )
        .expect_err("project stop cannot target setup in the next generation");
    assert_eq!(error.code, "not_allowed");
    assert!(
        store
            .get_command(&stop_id)
            .expect("read denied stop")
            .is_none()
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after denied stop"),
        event_count
    );
}

#[test]
fn project_stop_cannot_cancel_a_new_workspace_generation_after_dispatch_admission() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-workspace-generation";
    let checkout = "/synthetic-owner";
    let repo_common_dir = "/synthetic-owner/.git";
    let worktree_path = "/synthetic-worktree";
    let branch = "codex/project-generation";
    let base_oid = "c".repeat(40);
    let input = WorkspacePreparationInput {
        project: checkout.to_owned(),
        repo_path: checkout.to_owned(),
        repo_common_dir: repo_common_dir.to_owned(),
        requested_branch: branch.to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: base_oid.clone(),
        worktree_path: worktree_path.to_owned(),
        frozen_setup: Vec::new(),
    };
    let launch_id = Ulid::new().to_string();
    let launch = store
        .submit_with_workspace(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({
                    "project": checkout,
                    "workspace": checkout,
                    "worktree": branch,
                    "harness": "codex",
                    "profile": "project-profile",
                    "profile_snapshot": {}
                }),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            input.clone(),
            now,
        )
        .expect("admit managed Codex project launch");
    let agent = launch.command.target.expect("workspace Agent");
    store
        .settle_workspace_creation(
            &launch_id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path.clone(),
                repo_common_dir: input.repo_common_dir.clone(),
                worktree_path: input.worktree_path.clone(),
                branch: input.requested_branch.clone(),
                head: input.base_oid.clone(),
            },
            now + 1,
        )
        .expect("verify managed project workspace");
    store
        .dispatch(&launch_id, now + 2)
        .expect("dispatch launch");
    install_fixture_sources(&mut store, &agent, &launch_id, 1, now + 2);
    store
        .put_batch(
            SourceBatch {
                batch_id: "project-workspace-generation:ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "project-workspace-generation:ready".to_owned(),
                    data: json!({
                        "command_id": launch_id,
                        "native_scope": "project-workspace-generation:scope",
                        "native_ref": "project-workspace-generation:thread"
                    }),
                }],
                cursor: None,
            },
            now + 3,
        )
        .expect("commit Codex readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 4)
        .expect("settle launch");
    install_live_custody_observations(&mut store, &agent, 1, now + 4);
    store
        .put_batch(
            SourceBatch {
                batch_id: "project-workspace-generation:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "project-workspace-generation:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 5,
        )
        .expect("commit terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("read Agent")
            .expect("Agent exists")
            .state,
        AgentState::Stopped
    );

    let registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: repo_common_dir.to_owned(),
        git: None,
        profiles: vec!["project-profile".to_owned()],
        actions: vec!["stop".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 6)
        .expect("register project");
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            &format!("project:{project}"),
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
            now + 6,
        )
        .expect("admit project stop against the ready current generation");
    let binding: String = store
        .connection
        .query_row(
            "SELECT project_binding FROM commands WHERE id = ?1",
            [&stop_id],
            |row| row.get(0),
        )
        .expect("persist frozen managed workspace binding");
    let binding: serde_json::Value =
        serde_json::from_str(&binding).expect("decode managed workspace binding");
    assert_eq!(binding["workspace_phase"], "ready");
    assert_eq!(binding["workspace_target_generation"], 1);

    let resume_id = Ulid::new().to_string();
    store
        .submit_with_workspace(
            "owner",
            SubmitCommand {
                id: resume_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "resume".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            input,
            now + 7,
        )
        .expect("admit next-generation workspace resume");
    let workspace_before: (String, i64, String) = store
        .connection
        .query_row(
            "SELECT phase, target_generation, outcome FROM workspaces WHERE agent = ?1",
            [eliot_core::parse_agent_id(&agent).expect("valid Agent id")],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read next-generation workspace before stale stop dispatch");
    assert_eq!(workspace_before.0, "creating");
    assert_eq!(workspace_before.1, 2);
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events before stale stop dispatch");

    let error = store
        .dispatch(&stop_id, now + 8)
        .expect_err("old project stop must not cancel the new workspace preparation");
    assert_eq!(error.code, "not_allowed");
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("read project stop")
            .expect("stop retained")
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .get_command(&resume_id)
            .expect("read resume")
            .expect("resume retained")
            .state,
        CommandState::Accepted
    );
    let workspace_after: (String, i64, String) = store
        .connection
        .query_row(
            "SELECT phase, target_generation, outcome FROM workspaces WHERE agent = ?1",
            [eliot_core::parse_agent_id(&agent).expect("valid Agent id")],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read resumed workspace after stale stop dispatch");
    assert_eq!(workspace_after, workspace_before);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after denied dispatch"),
        event_count
    );
}

#[test]
fn project_approved_send_uses_existing_store_admission_and_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "approved-send";
    let profile = "project-send-profile";
    let checkout = "/synthetic-approved-send";
    let (agent, _) = ready_managed_codex_agent(&mut store, checkout, profile, now);
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: format!("{checkout}/.git"),
                git: None,
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now + 5,
        )
        .expect("register approved send action");

    let send_id = Ulid::new().to_string();
    let admission = store
        .submit(
            &format!("project:{project}"),
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent),
                selector: None,
                verb: "send".to_owned(),
                args: json!({"text":"project-scoped-send"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 6,
        )
        .expect("admit through ordinary Store send path");
    assert_eq!(admission.command.state, CommandState::Accepted);
    assert_eq!(
        store
            .dispatch(&send_id, now + 7)
            .expect("dispatch admitted send")
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn live_project_rule_send_rejects_other_valid_agent_at_same_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "live-event-binding";
    let principal = format!("project:{project}");
    let checkout = "/synthetic-live-event-binding";
    let profile = "live-event-binding-profile";
    let (event_agent, _) = ready_managed_codex_agent(&mut store, checkout, profile, now);
    let (other_agent, _) = ready_managed_codex_agent(&mut store, checkout, profile, now + 10);
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: format!("{checkout}/.git"),
                git: None,
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now + 20,
        )
        .expect("register project SEND action");
    let rule_id = "live-agent-binding";
    let state = store
        .ensure_rule_state(&principal, &project_send_rule_input(rule_id), now + 21)
        .expect("seed project rule before trigger event");

    let trigger = store
        .submit(
            "owner",
            send_request(&event_agent, 1, "trigger-live-rule"),
            now + 22,
        )
        .expect("admit trigger SEND on source Agent");
    let event_seq = trigger
        .events
        .iter()
        .find(|event| {
            event.kind == "command.accepted" && event.data["command_id"] == trigger.command.id
        })
        .expect("persist source Agent accepted event")
        .seq;
    store
        .settle(
            &trigger.command.id,
            CommandState::Rejected,
            Some(json!({"reason":"not_dispatched"})),
            now + 23,
        )
        .expect("withdraw undispatched trigger SEND");

    let valid_other_agent_send = send_request(&other_agent, 1, "ordinary-valid-send");
    let ordinary = store
        .submit(&principal, valid_other_agent_send.clone(), now + 24)
        .expect("same-generation project SEND to other Agent is ordinarily valid");
    store
        .settle(
            &ordinary.command.id,
            CommandState::Rejected,
            Some(json!({"reason":"not_dispatched"})),
            now + 25,
        )
        .expect("withdraw ordinary admission probe before dispatch");

    let admission = store
        .admit_rule_action(
            &principal,
            &state,
            event_seq,
            valid_other_agent_send,
            now + 26,
        )
        .expect("binding mismatch is a deterministic live-rule rejection");
    assert_live_send_binding_rejection(&store, admission, event_seq);
}

#[test]
fn history_project_rule_send_rejects_rebinding_old_event_to_current_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "history-generation-binding";
    let principal = format!("project:{project}");
    let checkout = "/synthetic-history-generation-binding";
    let profile = "history-generation-binding-profile";
    let (agent, workspace) = ready_managed_codex_agent(&mut store, checkout, profile, now);
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: format!("{checkout}/.git"),
                git: None,
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now + 5,
        )
        .expect("register project SEND action");
    let rule_id = "history-generation-binding";
    let initial_state = store
        .ensure_rule_state(&principal, &project_send_rule_input(rule_id), now + 6)
        .expect("seed project rule before trigger event");
    let after_seq = initial_state.current_cursor;

    let trigger = store
        .submit(
            "owner",
            send_request(&agent, 1, "trigger-history-rule"),
            now + 7,
        )
        .expect("admit generation-1 trigger SEND");
    let event_seq = trigger
        .events
        .iter()
        .find(|event| event.kind == "command.accepted")
        .expect("persist generation-1 accepted event")
        .seq;
    store
        .settle(
            &trigger.command.id,
            CommandState::Rejected,
            Some(json!({"reason":"not_dispatched"})),
            now + 8,
        )
        .expect("withdraw undispatched trigger SEND");
    let cursor = store
        .advance_rule_cursor(
            &principal,
            &initial_state,
            event_seq,
            RuleCursorOutcome::NoMatch,
            now + 9,
        )
        .expect("advance live lane over the retained trigger");
    assert!(
        cursor.events.is_empty(),
        "no-match cursor writes are silent"
    );

    install_live_custody_observations(&mut store, &agent, 1, now + 9);
    store
        .put_batch(
            SourceBatch {
                batch_id: "history-generation-binding:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "history-generation-binding:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 10,
        )
        .expect("commit generation-1 terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 10);
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store
        .submit_with_workspace("owner", resume, workspace.clone(), now + 11)
        .expect("admit managed-workspace resume");
    store
        .settle_workspace_creation(
            &resume_id,
            2,
            WorkspaceCreationOutcome::Verified {
                repo_path: workspace.repo_path.clone(),
                repo_common_dir: workspace.repo_common_dir.clone(),
                worktree_path: workspace.worktree_path.clone(),
                branch: workspace.requested_branch.clone(),
                head: workspace.base_oid.clone(),
            },
            now + 12,
        )
        .expect("verify generation-2 managed workspace");
    store
        .dispatch(&resume_id, now + 13)
        .expect("dispatch generation-2 resume");
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 13);
    store
        .put_batch(
            SourceBatch {
                batch_id: "history-generation-binding:ready".to_owned(),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "history-generation-binding:ready".to_owned(),
                    data: json!({
                        "command_id": resume_id,
                        "native_scope": format!("{checkout}:{agent}:scope"),
                        "native_ref": format!("{checkout}:{agent}:thread")
                    }),
                }],
                cursor: None,
            },
            now + 14,
        )
        .expect("commit generation-2 readiness");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 15)
        .expect("settle generation-2 resume");
    let current = store
        .get_agent(&agent)
        .expect("read current Agent")
        .expect("Agent exists");
    assert_eq!(current.generation, 2);
    assert_eq!(current.state, AgentState::Idle);

    let valid_current_generation_send = send_request(&agent, 2, "ordinary-valid-generation-2");
    let ordinary = store
        .submit(&principal, valid_current_generation_send.clone(), now + 16)
        .expect("generation-2 project SEND is ordinarily valid");
    store
        .settle(
            &ordinary.command.id,
            CommandState::Rejected,
            Some(json!({"reason":"not_dispatched"})),
            now + 17,
        )
        .expect("withdraw ordinary admission probe before dispatch");

    let catch_up_request =
        RuleCatchUpRequest::new(Ulid::new().to_string(), rule_id, after_seq, Some(event_seq))
            .in_project(project);
    let admission = store
        .admit_rule_catch_up(
            &principal,
            &catch_up_request,
            &cursor.value,
            event_seq,
            &[RuleCatchUpAction::command(
                event_seq,
                valid_current_generation_send,
            )],
            now + 18,
        )
        .expect("old-event rebinding is a deterministic history rejection");
    assert_history_send_binding_rejection(&store, admission, event_seq);
    assert_eq!(
        store
            .get_rule_state(&principal, rule_id)
            .expect("read rule cursor")
            .expect("rule remains")
            .current_cursor,
        event_seq,
        "history rejection does not rewind or move the live cursor",
    );
}

#[test]
fn project_resume_rules_use_retained_binding_for_live_and_history_and_require_send_for_text() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-resume-rules";
    let principal = format!("project:{project}");
    let checkout = "/synthetic-project-resume-rules";
    let profile = "project-resume-profile";
    let (live_agent, live_workspace) =
        ready_managed_codex_agent(&mut store, checkout, profile, now);
    let (history_agent, history_workspace) =
        ready_managed_codex_agent(&mut store, checkout, profile, now + 10);
    let registration = |actions: Vec<String>| ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: format!("{checkout}/.git"),
        git: None,
        profiles: vec![profile.to_owned()],
        actions,
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration(
            "owner",
            &registration(vec!["resume".to_owned(), "send".to_owned()]),
            now + 20,
        )
        .expect("register Project resume and linked SEND");

    let make_rule = |id: &str| {
        let value = json!({
            "id": id,
            "enabled": true,
            "on": {"event": "session.ended"},
            "do": [{
                "action": "resume",
                "target": "${event.agent}",
                "expected_generation": "${event.generation}",
                "text": "continue retained Project session",
            }],
            "limit": {"max": 1, "per": "agent", "window_ms": 60_000},
        });
        RuleDefinitionInput {
            id: id.to_owned(),
            canonical_definition: serde_json::to_string(&value)
                .expect("encode Project resume rule"),
            definition_digest: eliot_core::canonical_json_sha256(&value)
                .expect("digest Project resume rule"),
            enabled: true,
        }
    };
    let live_rule = make_rule("project-live-resume");
    let history_rule = make_rule("project-history-resume");
    let no_send_rule = make_rule("project-resume-no-send");
    let live_state = store
        .ensure_rule_state(&principal, &live_rule, now + 21)
        .expect("seed live Project rule before its event");
    let no_send_state = store
        .ensure_rule_state(&principal, &no_send_rule, now + 23)
        .expect("seed send-grant Project rule before its event");

    install_live_custody_observations(&mut store, &live_agent, 1, now + 23);
    let live_ended = store
        .put_batch(
            SourceBatch {
                batch_id: "project-live-resume:ended".to_owned(),
                agent_id: live_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "project-live-resume:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 24,
        )
        .expect("commit live Project terminal fact");
    let live_event = live_ended
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .expect("persist live Project event")
        .seq;
    simulate_normal_stop_proof(&mut store, &live_agent, 1, now + 24);
    install_live_custody_observations(&mut store, &history_agent, 1, now + 24);
    let history_ended = store
        .put_batch(
            SourceBatch {
                batch_id: "project-history-resume:ended".to_owned(),
                agent_id: history_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "project-history-resume:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 25,
        )
        .expect("commit history Project terminal fact");
    let history_event = history_ended
        .events
        .iter()
        .find(|event| event.kind == "session.ended")
        .expect("persist history Project event")
        .seq;
    simulate_normal_stop_proof(&mut store, &history_agent, 1, now + 25);
    let history_state = store
        .ensure_rule_state(&principal, &history_rule, now + 26)
        .expect("seed history Project cursor after its captured events");
    let captured_through = history_state.current_cursor;
    assert!(history_event <= captured_through);

    let resume_request = |agent: &str, text: &str| {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "resume");
        request.target = Some(agent.to_owned());
        request.expected_generation = Some(1);
        request.args = json!({"text": text});
        request
    };
    let live = store
        .admit_rule_action(
            &principal,
            &live_state,
            live_event,
            resume_request(&live_agent, "live Project input"),
            now + 26,
        )
        .expect("admit live Project resume through ordinary Store path");
    let live_child = live.child.as_ref().expect("live Project resume child");
    let live_resume_id = live_child.command.id.clone();
    let live_send_id = live_child
        .linked_command
        .as_ref()
        .expect("live Project resume has linked input")
        .id
        .clone();
    assert_eq!(
        live_child.command.target.as_deref(),
        Some(live_agent.as_str())
    );
    assert_eq!(live_child.command.expected_generation, Some(1));
    assert_eq!(live_child.command.generation, Some(1));
    assert_eq!(
        live_child
            .linked_command
            .as_ref()
            .unwrap()
            .expected_generation,
        Some(2)
    );
    store
        .settle_workspace_creation(
            &live_resume_id,
            2,
            WorkspaceCreationOutcome::Verified {
                repo_path: live_workspace.repo_path.clone(),
                repo_common_dir: live_workspace.repo_common_dir.clone(),
                worktree_path: live_workspace.worktree_path.clone(),
                branch: live_workspace.requested_branch.clone(),
                head: live_workspace.base_oid.clone(),
            },
            now + 27,
        )
        .expect("verify retained Project workspace for resume");
    store
        .dispatch(&live_resume_id, now + 28)
        .expect("dispatch live Project resume");
    install_fixture_sources(&mut store, &live_agent, &live_resume_id, 2, now + 28);
    let live_scope = format!("{checkout}:{live_agent}:scope");
    let live_ref = format!("{checkout}:{live_agent}:thread");
    store
        .put_batch(
            SourceBatch {
                batch_id: "project-live-resume:ready".to_owned(),
                agent_id: live_agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "project-live-resume:ready".to_owned(),
                    data: json!({
                        "command_id": live_resume_id.as_str(),
                        "native_scope": live_scope.as_str(),
                        "native_ref": live_ref.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 29,
        )
        .expect("retain exact live Project native subject");
    store
        .settle(&live_resume_id, CommandState::Applied, None, now + 30)
        .expect("settle live Project resume readiness");
    store
        .dispatch(&live_send_id, now + 31)
        .expect("dispatch linked Project input against next-generation binding");
    let live_turn = Ulid::new().to_string();
    store
        .put_batch(
            SourceBatch {
                batch_id: "project-live-resume:turn".to_owned(),
                agent_id: live_agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "project-live-resume:turn-started".to_owned(),
                    data: json!({
                        "command_id": live_send_id.as_str(),
                        "turn_id": live_turn.as_str(),
                    }),
                }],
                cursor: None,
            },
            now + 32,
        )
        .expect("associate the exact Project input");
    assert_eq!(
        store.get_command(&live_send_id).unwrap().unwrap().state,
        CommandState::Applied
    );

    let history_request = RuleCatchUpRequest::new(
        Ulid::new().to_string(),
        history_rule.id.as_str(),
        history_event - 1,
        Some(captured_through),
    )
    .in_project(project);
    let history = store
        .admit_rule_catch_up(
            &principal,
            &history_request,
            &history_state,
            captured_through,
            &[RuleCatchUpAction::command(
                history_event,
                resume_request(&history_agent, "history Project input"),
            )],
            now + 33,
        )
        .expect("admit Project history resume through the same ordinary path");
    assert_eq!(history.children.len(), 1);
    assert_eq!(history.children[0].command.verb, "resume");
    assert_eq!(
        history.children[0].command.target.as_deref(),
        Some(history_agent.as_str())
    );
    assert_eq!(history.children[0].command.expected_generation, Some(1));
    let history_resume_id = history.children[0].command.id.clone();
    let history_send_id = history.children[0]
        .linked_command
        .as_ref()
        .expect("history Project resume has linked input")
        .id
        .clone();
    assert_eq!(history_send_id, format!("{history_resume_id}/send"));
    assert_eq!(
        history.children[0]
            .linked_command
            .as_ref()
            .unwrap()
            .generation,
        Some(2)
    );
    let history_workspace_row: (String, i64, String, String) = store
        .connection
        .query_row(
            "SELECT phase, target_generation, start_command_id, worktree_path
             FROM workspaces WHERE agent = ?1",
            [eliot_core::parse_agent_id(&history_agent).unwrap()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("read retained Project workspace after catch-up resume admission");
    assert_eq!(history_workspace_row.0, "creating");
    assert_eq!(history_workspace_row.1, 2);
    assert_eq!(history_workspace_row.2, history_resume_id);
    assert_eq!(history_workspace_row.3, history_workspace.worktree_path);

    store
        .upsert_project_registration("owner", &registration(vec!["resume".to_owned()]), now + 34)
        .expect("remove the Project SEND grant");
    let denied = store
        .admit_rule_action(
            &principal,
            &no_send_state,
            history_event,
            resume_request(&history_agent, "send grant is required"),
            now + 35,
        )
        .expect("missing Project SEND grant is a deterministic rejection");
    let denied_parent = denied.parent.unwrap();
    assert_eq!(denied_parent.state, CommandState::Rejected);
    assert_eq!(
        denied_parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert!(denied.child.is_none());
    let denied_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [&denied_parent.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(denied_children, 0);
    assert_eq!(
        store.get_command(&history_send_id).unwrap().unwrap().state,
        CommandState::Queued,
        "a failed SEND grant cannot change the separately retained history input",
    );
}

#[test]
fn project_fake_interrupt_requires_explicit_action_approval() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "fake-interrupt-approval";
    let principal = format!("project:{project}");
    let agent = ready_agent(&mut store, project, now);
    let send = send_request(&agent, 1, "open project approval");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 1).unwrap();
    store.dispatch(&send_id, now + 2).unwrap();
    let turn_id = format!("project-reply-turn-{send_id}");
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
            now + 3,
        )
        .unwrap();
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: "synthetic-workspace".to_owned(),
        repo_common_dir: "synthetic-workspace/.git".to_owned(),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["send".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 4)
        .expect("register Fake profile with SEND only");

    let mut denied = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    denied.target = Some(agent.clone());
    denied.args = json!({});
    denied.expected_generation = Some(1);
    denied.expected_turn_id = Some(turn_id.clone());
    let denied_id = denied.id.clone();
    let before_denial = store.high_water().expect("read event high water");
    assert_eq!(
        store.submit(&principal, denied, now + 5).unwrap_err().code,
        "not_allowed"
    );
    assert!(store.get_command(&denied_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), before_denial);

    registration.actions = vec!["interrupt".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 6)
        .expect("approve the interrupt action explicitly");
    let mut approved = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    approved.target = Some(agent);
    approved.args = json!({});
    approved.expected_generation = Some(1);
    approved.expected_turn_id = Some(turn_id.clone());
    let approved_id = approved.id.clone();
    let admission = store
        .submit(&principal, approved, now + 7)
        .expect("admit explicitly approved project interrupt");
    assert_eq!(admission.command.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(
        store.dispatch(&approved_id, now + 8).unwrap().value.state,
        CommandState::Dispatched
    );
}

#[test]
fn project_fake_steer_requires_explicit_approval_and_keeps_exact_turn_binding() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-fake-steer-approval";
    let principal = format!("project:{project}");
    let agent = ready_agent(&mut store, project, now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);
    let checkout = "synthetic-workspace";
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: format!("{checkout}/.git"),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["send".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 2)
        .expect("register project without STEER approval");

    let mut denied = SubmitCommand::new(Ulid::new().to_string(), "steer");
    denied.target = Some(agent.clone());
    denied.args = json!({"text":"project steer"});
    denied.expected_generation = Some(1);
    denied.expected_turn_id = Some(turn_id.clone());
    let denied_id = denied.id.clone();
    let high_water_before_denial = store.high_water().unwrap();
    assert_eq!(
        store.submit(&principal, denied, now + 3).unwrap_err().code,
        "not_allowed"
    );
    assert!(store.get_command(&denied_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), high_water_before_denial);

    registration.actions.push("steer".to_owned());
    store
        .upsert_project_registration("owner", &registration, now + 4)
        .expect("approve project STEER explicitly");
    let mut approved = SubmitCommand::new(Ulid::new().to_string(), "steer");
    approved.target = Some(agent.clone());
    approved.args = json!({"text":"project steer"});
    approved.expected_generation = Some(1);
    approved.expected_turn_id = Some(turn_id.clone());
    let approved_id = approved.id.clone();
    let admission = store
        .submit(&principal, approved, now + 5)
        .expect("admit project STEER with its approved exact turn");
    assert_eq!(admission.command.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(admission.command.args, json!({"text":"project steer"}));
    assert_eq!(
        store.dispatch(&approved_id, now + 6).unwrap().value.state,
        CommandState::Dispatched
    );
}

#[test]
fn project_send_requires_the_approved_action_and_rejects_unknown_registration_actions() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let profile = "project-send-profile";
    let checkout = "/synthetic-denied-send";
    let (agent, _) = ready_managed_codex_agent(&mut store, checkout, profile, now);
    let common = format!("{checkout}/.git");
    let unsupported = store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: "unsupported-project-action".to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: common.clone(),
                git: None,
                profiles: vec![profile.to_owned()],
                actions: vec!["unexpected".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now + 5,
        )
        .expect_err("Store must reject unimplemented project actions");
    assert_eq!(unsupported.code, "unsupported");

    let project = "send-not-approved";
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: project.to_owned(),
                checkout: checkout.to_owned(),
                repo_common_dir: common,
                git: None,
                profiles: vec![profile.to_owned()],
                actions: vec!["stop".to_owned()],
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now + 6,
        )
        .expect("register project with send denied");
    let events_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events before denied send");
    let send_id = Ulid::new().to_string();
    let error = store
        .submit(
            &format!("project:{project}"),
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent),
                selector: None,
                verb: "send".to_owned(),
                args: json!({"text":"denied-project-send"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 7,
        )
        .expect_err("project cannot send without the owner's send allowlist");
    assert_eq!(error.code, "not_allowed");
    assert!(
        store
            .get_command(&send_id)
            .expect("read denied send")
            .is_none()
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after denied send"),
        events_before
    );
}

#[test]
fn project_event_launch_requires_both_grants_and_only_linked_send_uses_next_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let project = "project-launch-contract";
    let rule_id = "project-launch-contract-rule";
    let checkout_path = std::env::temp_dir().join("eliot-store-project-launch-contract");
    let checkout = checkout_path.display().to_string();
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.clone(),
        repo_common_dir: checkout_path.join(".git").display().to_string(),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["launch".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now)
        .expect("register default-deny launch project");
    let principal = format!("project:{project}");
    let state = store
        .ensure_rule_state(
            &principal,
            &rule_input_with_limit(rule_id, 5, &checkout),
            now,
        )
        .expect("ensure project launch rule");
    let trigger_agent = ready_agent(&mut store, "project-launch-trigger", now + 1);
    let launch_event_seq = launch_event_seq_for_agent(&store, &trigger_agent);
    let agents_before = store.agent_count().unwrap();

    registration.actions.clear();
    store
        .upsert_project_registration("owner", &registration, now + 2)
        .expect("revoke Launch before testing missing grant");
    let missing_launch = store
        .admit_rule_action(
            &principal,
            &state,
            launch_event_seq,
            targetless_rule_launch_request(&checkout, &checkout, None),
            now + 3,
        )
        .expect("record missing launch grant");
    let missing_launch_parent = missing_launch.parent.as_ref().unwrap();
    assert_eq!(missing_launch_parent.state, CommandState::Rejected);
    assert_eq!(
        missing_launch_parent.outcome.as_ref().unwrap()["code"],
        "not_allowed"
    );
    assert_eq!(
        missing_launch_parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert!(missing_launch.child.is_none());
    assert_eq!(store.agent_count().unwrap(), agents_before);

    registration.actions = vec!["launch".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 4)
        .expect("approve launch without SEND");
    let trigger_send = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "project SEND-grant event"),
            now + 5,
        )
        .expect("admit trigger SEND event");
    let send_event = trigger_send
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(trigger_send.command.id.as_str())
        })
        .expect("trigger SEND command event");
    let missing_send = store
        .admit_rule_action(
            &principal,
            &missing_launch.state,
            send_event.seq,
            targetless_rule_launch_request(&checkout, &checkout, Some("unapproved input")),
            now + 6,
        )
        .expect("record missing SEND grant");
    let missing_send_parent = missing_send.parent.as_ref().unwrap();
    assert_eq!(missing_send_parent.state, CommandState::Rejected);
    assert_eq!(
        missing_send_parent.outcome.as_ref().unwrap()["code"],
        "not_allowed"
    );
    assert_eq!(
        missing_send_parent.outcome.as_ref().unwrap()["children"],
        json!([])
    );
    assert!(missing_send.child.is_none());
    assert_eq!(store.agent_count().unwrap(), agents_before);

    registration.actions = vec!["launch".to_owned(), "send".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 7)
        .expect("approve launch and SEND");
    let second_trigger_send = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "approved project launch event"),
            now + 8,
        )
        .expect("admit second trigger SEND event");
    let approved_event = second_trigger_send
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(second_trigger_send.command.id.as_str())
        })
        .expect("approved trigger command event");
    let approved = store
        .admit_rule_action(
            &principal,
            &missing_send.state,
            approved_event.seq,
            targetless_rule_launch_request(
                &checkout,
                &checkout,
                Some("approved project initial input"),
            ),
            now + 9,
        )
        .expect("admit project launch with both grants");
    assert!(approved.is_new);
    let parent = approved.parent.as_ref().expect("project launch parent");
    let action = approved.child.as_ref().expect("project launch action");
    let launch = &action.command;
    let linked = action
        .linked_command
        .as_ref()
        .expect("SEND grant admits linked initial input");
    let launch_id = launch.id.clone();
    let linked_id = linked.id.clone();
    let target = launch.target.clone().expect("allocated project Agent");
    assert_eq!(parent.principal, principal);
    assert_eq!(launch.principal, principal);
    assert_eq!(linked.principal, principal);
    assert_ne!(target, trigger_agent);
    assert_eq!(launch.args["workspace"], checkout);
    assert_eq!(launch.expected_generation, Some(0));
    assert_eq!(launch.generation, Some(0));
    assert_eq!(linked.target.as_deref(), Some(target.as_str()));
    assert_eq!(linked.expected_generation, Some(1));
    assert_eq!(linked.generation, Some(1));
    assert_eq!(linked.args["text"], "approved project initial input");
    assert_eq!(linked.state, CommandState::Queued);
    let target_num = eliot_core::parse_agent_id(&target).unwrap();
    let stored_generation: i64 = store
        .connection
        .query_row(
            "SELECT generation FROM agents WHERE id = ?1",
            [target_num],
            |row| row.get(0),
        )
        .expect("read created project Agent generation");
    assert_eq!(stored_generation, 0);

    let ordinary_future_send = send_request(&target, 1, "ordinary future-generation SEND");
    let ordinary_future_send_id = ordinary_future_send.id.clone();
    let high_water_before_ordinary_send = store.high_water().unwrap();
    assert_eq!(
        store
            .submit(&principal, ordinary_future_send, now + 10)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert!(
        store
            .get_command(&ordinary_future_send_id)
            .expect("read rejected future SEND")
            .is_none()
    );
    assert_eq!(store.high_water().unwrap(), high_water_before_ordinary_send);

    store
        .dispatch(&launch_id, now + 11)
        .expect("dispatch project launch");
    install_fixture_sources(&mut store, &target, &launch_id, 1, now + 11);
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: target.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":launch_id.clone()}),
                }],
                cursor: None,
            },
            now + 12,
        )
        .expect("commit project launch readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 13)
        .expect("settle project launch");
    assert_eq!(
        store.dispatch(&linked_id, now + 14).unwrap().value.state,
        CommandState::Dispatched,
        "only the linked initial SEND crosses the stored generation-0 to generation-1 boundary"
    );
}

#[test]
fn recover_rule_children_byte_trims_launch_prefix_and_paginates_each_link_once() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "large-recovery-launches";
    let launch_count = 12_usize;
    let launch_text = "x".repeat(60_000);
    assert!(launch_count * 2 * launch_text.len() > crate::MAX_PAGE_BYTES);
    let rule_workspace = "synthetic-recovery-action";
    let mut state = store
        .ensure_rule_state(
            "owner",
            &rule_input_with_limit(
                rule_id,
                u64::try_from(launch_count + 1).unwrap(),
                rule_workspace,
            ),
            now,
        )
        .expect("ensure paginated recovery rule");
    let mut expected_launch_ids = Vec::with_capacity(launch_count);
    let mut expected_send_ids = Vec::with_capacity(launch_count);

    for index in 0..launch_count {
        let trigger_workspace = format!("synthetic-recovery-trigger-{index}");
        let trigger = store
            .submit(
                "owner",
                targetless_rule_launch_request(&trigger_workspace, &trigger_workspace, None),
                now + i64::try_from(index * 3).unwrap() + 1,
            )
            .expect("admit external trigger launch");
        let trigger_event = trigger
            .events
            .iter()
            .find(|event| {
                event.kind == "command.accepted"
                    && event
                        .data
                        .get("command_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(trigger.command.id.as_str())
            })
            .expect("trigger launch command event");
        let workspace = format!("synthetic-recovery-action-{index}");
        let request = targetless_rule_launch_request(&workspace, &workspace, Some(&launch_text));
        let admission = store
            .admit_rule_action(
                "owner",
                &state,
                trigger_event.seq,
                request,
                now + i64::try_from(index * 3).unwrap() + 2,
            )
            .expect("admit large linked launch action");
        let action = admission.child.as_ref().expect("launch action");
        expected_launch_ids.push(action.command.id.clone());
        expected_send_ids.push(
            action
                .linked_command
                .as_ref()
                .expect("large launch input is linked")
                .id
                .clone(),
        );
        state = admission.state;
    }

    let mut seen_launch_ids = Vec::new();
    let mut seen_send_ids = Vec::new();
    let first_page = store
        .recover_rule_children(None, 100)
        .expect("return bounded nonempty recovery prefix");
    assert!(
        first_page.len() < launch_count,
        "byte bound must split the page"
    );
    let mut after = Some(
        first_page
            .last()
            .expect("first recovery page is nonempty")
            .command
            .id
            .clone(),
    );
    let cursor_id = after.as_ref().unwrap().clone();
    assert_ne!(
        cursor_id,
        first_page
            .last()
            .unwrap()
            .linked_command
            .as_ref()
            .unwrap()
            .id,
        "recovery cursor remains the launch ULID, not the linked SEND ID"
    );
    record_recovered_launch_page(
        &first_page,
        &expected_launch_ids,
        &expected_send_ids,
        &mut seen_launch_ids,
        &mut seen_send_ids,
        &launch_text,
    );

    let mut page_count = 1;
    loop {
        let page = store
            .recover_rule_children(after.as_deref(), 100)
            .expect("continue after the last returned launch ULID");
        if page.is_empty() {
            break;
        }
        page_count += 1;
        after = Some(page.last().unwrap().command.id.clone());
        record_recovered_launch_page(
            &page,
            &expected_launch_ids,
            &expected_send_ids,
            &mut seen_launch_ids,
            &mut seen_send_ids,
            &launch_text,
        );
    }
    assert!(page_count > 1, "recovery exercised multiple bounded pages");
    assert_eq!(seen_launch_ids.len(), launch_count);
    assert_eq!(seen_send_ids.len(), launch_count);
    assert!(
        expected_launch_ids
            .iter()
            .all(|id| seen_launch_ids.contains(id))
    );
    assert!(
        expected_send_ids
            .iter()
            .all(|id| seen_send_ids.contains(id))
    );
}

#[test]
fn recover_rule_children_returns_valid_oversized_launch_pair_as_singleton() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "oversized-single-recovery-launch";
    let workspace = "synthetic-oversized-recovery-action";
    let state = store
        .ensure_rule_state("owner", &rule_input_with_limit(rule_id, 2, workspace), now)
        .expect("ensure oversized recovery rule");
    let trigger_workspace = "synthetic-oversized-recovery-trigger";
    let trigger = store
        .submit(
            "owner",
            targetless_rule_launch_request(trigger_workspace, trigger_workspace, None),
            now + 1,
        )
        .expect("admit external trigger launch");
    let trigger_event = trigger
        .events
        .iter()
        .find(|event| {
            event.kind == "command.accepted"
                && event
                    .data
                    .get("command_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(trigger.command.id.as_str())
        })
        .expect("trigger launch event");

    let launch_text = "i".repeat(60_000);
    let mut request = targetless_rule_launch_request(workspace, workspace, Some(&launch_text));
    request.args["profile_snapshot"]["recovery_fixture"] = json!("p".repeat(820_000));
    assert!(
        serde_json::to_vec(&request).unwrap().len() <= crate::MAX_FRAME_BYTES,
        "launch request remains inside the existing admission frame bound"
    );
    let admission = store
        .admit_rule_action("owner", &state, trigger_event.seq, request, now + 2)
        .expect("admit valid large launch and linked SEND");
    let launch_id = admission.child.as_ref().unwrap().command.id.clone();
    let send_id = admission
        .child
        .as_ref()
        .unwrap()
        .linked_command
        .as_ref()
        .unwrap()
        .id
        .clone();

    let page = store
        .recover_rule_children(None, 100)
        .expect("preserve a valid oversized singleton");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].command.id, launch_id);
    assert_eq!(page[0].linked_command.as_ref().unwrap().id, send_id);
    let launch_bytes = serde_json::to_vec(&page[0].command).unwrap().len();
    let send_bytes = serde_json::to_vec(page[0].linked_command.as_ref().unwrap())
        .unwrap()
        .len();
    assert!(launch_bytes <= crate::MAX_FRAME_BYTES);
    assert!(send_bytes <= crate::MAX_FRAME_BYTES);
    assert!(
        recovered_admission_page_bytes(&page) > crate::MAX_PAGE_BYTES,
        "single valid Launch+SEND pair exceeds the recovery batch target"
    );
    assert!(
        store
            .recover_rule_children(Some(&launch_id), 100)
            .expect("resume after launch cursor")
            .is_empty(),
        "recovery cursor remains the launch ULID"
    );
}

#[test]
fn project_run_registration_roundtrips_and_rechecks_targetless_approval() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "project-run-registration";
    let program = std::env::current_exe()
        .expect("current executable path")
        .to_string_lossy()
        .into_owned();
    let interpreter = ScriptInterpreter {
        program,
        argv_prefix: vec!["-NoProfile".to_owned()],
    };
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: "/synthetic/project-run".to_owned(),
        repo_common_dir: "/synthetic/project-run/.git".to_owned(),
        git: None,
        profiles: Vec::new(),
        actions: Vec::new(),
        interpreters: BTreeMap::from([("pwsh".to_owned(), interpreter.clone())]),
        run_env: vec!["CI".to_owned()],
    };
    let default_deny = store
        .upsert_project_registration("owner", &registration, now)
        .expect("register project with no approved actions")
        .value;
    assert!(default_deny.actions.is_empty());
    assert_eq!(default_deny.run_env, vec!["CI"]);
    assert_eq!(
        store
            .get_project_registration("owner", project)
            .expect("read default-deny project"),
        Some(default_deny.clone())
    );
    assert_eq!(
        store
            .upsert_project_registration("owner", &registration, now + 1)
            .expect("repeat unchanged registration")
            .value
            .revision,
        default_deny.revision,
        "an identical approval snapshot does not create a new revision"
    );

    let principal = format!("project:{project}");
    let request = SubmitCommand::new(Ulid::new().to_string(), "run");
    let payload_digest = [0_u8; 32];
    let command = crate::CommandInsert {
        id: &request.id,
        principal: &principal,
        method: "command.submit",
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
        verb: &request.verb,
        args: &request.args,
        state: CommandState::Accepted,
        outcome: None,
        created_at: now,
        updated_at: now,
    };
    let denied = {
        let tx = store
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("open registration read transaction");
        crate::projects::capture_project_command_binding_tx(&tx, &command)
            .expect_err("run must be denied without owner action approval")
    };
    assert_eq!(denied.code, "not_allowed");

    registration.actions.push("run".to_owned());
    let approved = store
        .upsert_project_registration("owner", &registration, now + 2)
        .expect("approve project run")
        .value;
    assert!(approved.revision > default_deny.revision);
    assert_eq!(approved.interpreters.get("pwsh"), Some(&interpreter));
    assert_eq!(approved.actions, vec!["run"]);
    assert_eq!(
        store
            .list_project_registrations("owner", None, 10)
            .expect("read approved registration list"),
        vec![approved.clone()]
    );

    let binding = {
        let tx = store
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("open targetless binding transaction");
        crate::projects::capture_project_command_binding_tx(&tx, &command)
            .expect("capture approved targetless project run")
            .expect("project principal binding")
    };
    let binding: serde_json::Value =
        serde_json::from_str(&binding).expect("decode project run binding");
    assert_eq!(binding["registration_revision"], json!(approved.revision));
    assert!(binding["profile"].is_null());
    assert!(binding["workspace"].is_null());
    assert!(binding["repo_path"].is_null());
    assert!(binding["repo_common_dir"].is_null());
    assert!(binding["workspace_phase"].is_null());
    assert!(binding["workspace_target_generation"].is_null());
    assert_eq!(command.target, None);
    assert_eq!(command.generation, None);

    registration
        .interpreters
        .get_mut("pwsh")
        .expect("approved interpreter")
        .argv_prefix
        .push("-NoLogo".to_owned());
    let interpreter_changed = store
        .upsert_project_registration("owner", &registration, now + 3)
        .expect("update approved interpreter prefix")
        .value;
    assert!(interpreter_changed.revision > approved.revision);
    assert_eq!(
        interpreter_changed.interpreters["pwsh"].argv_prefix,
        vec!["-NoProfile", "-NoLogo"]
    );

    registration.run_env.push("BUILD_ID".to_owned());
    let revised = store
        .upsert_project_registration("owner", &registration, now + 4)
        .expect("update approved run environment")
        .value;
    assert!(revised.revision > interpreter_changed.revision);
    assert_eq!(revised.run_env, vec!["BUILD_ID", "CI"]);
    assert_eq!(
        store
            .get_project_registration("owner", project)
            .expect("read revised project registration"),
        Some(revised.clone())
    );

    registration.actions.clear();
    let revoked = store
        .upsert_project_registration("owner", &registration, now + 5)
        .expect("revoke project run approval")
        .value;
    assert!(revoked.revision > revised.revision);
    assert!(revoked.actions.is_empty());
    let error = {
        let tx = store
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("open revoked binding transaction");
        crate::projects::capture_project_command_binding_tx(&tx, &command)
            .expect_err("revoked run approval must reject a new binding")
    };
    assert_eq!(error.code, "not_allowed");
}

#[test]
fn project_registration_migration_defaults_legacy_run_authority_to_deny() {
    let path = capacity_store_path();
    {
        let legacy = rusqlite::Connection::open(&path).expect("create version 14 Store fixture");
        legacy
            .execute_batch(
                r#"CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
                   INSERT INTO schema_migrations(version) VALUES
                       (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14);
                   CREATE TABLE writes (
                       id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
                       payload_digest BLOB NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL
                   );
                   CREATE TABLE agents (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       generation INTEGER NOT NULL DEFAULT 0,
                       name TEXT UNIQUE,
                       project TEXT NOT NULL,
                       harness TEXT NOT NULL,
                       profile TEXT NOT NULL,
                       workspace TEXT,
                       token TEXT UNIQUE,
                       state TEXT NOT NULL,
                       active_turn TEXT,
                       created_at INTEGER NOT NULL,
                       updated_at INTEGER NOT NULL,
                       profile_snapshot TEXT NOT NULL DEFAULT '{}',
                       native_scope TEXT,
                       native_ref TEXT,
                       effective TEXT,
                       pending TEXT NOT NULL DEFAULT '[]',
                       origin TEXT NOT NULL DEFAULT 'eliot'
                           CHECK(origin IN ('eliot','native','external')),
                       observe_only INTEGER NOT NULL DEFAULT 0
                           CHECK(observe_only IN (0,1) AND (state <> 'unknown' OR observe_only = 1)
                               AND (observe_only = 0 OR token IS NULL)),
                       capacity_generation INTEGER
                           CHECK(capacity_generation IS NULL OR capacity_generation > 0)
                   );
                   CREATE INDEX agents_state ON agents(state);
                   CREATE UNIQUE INDEX agents_native
                       ON agents(native_scope, native_ref)
                       WHERE native_scope IS NOT NULL AND native_ref IS NOT NULL;
                   CREATE INDEX agents_capacity_reserved ON agents(capacity_generation)
                       WHERE capacity_generation IS NOT NULL;
                   CREATE TABLE commands (
                       id TEXT PRIMARY KEY,
                       principal TEXT NOT NULL,
                       method TEXT NOT NULL,
                       payload_digest BLOB NOT NULL,
                       target INTEGER REFERENCES agents(id),
                       expected_generation INTEGER,
                       generation INTEGER,
                       expected_turn_id TEXT,
                       parent_command TEXT REFERENCES commands(id),
                       link_kind TEXT,
                       ordinal INTEGER,
                       turn_id TEXT,
                       verb TEXT NOT NULL,
                       args TEXT NOT NULL,
                       state TEXT NOT NULL,
                       outcome TEXT,
                       created_at INTEGER NOT NULL,
                       updated_at INTEGER NOT NULL,
                       request_id TEXT,
                       probe INTEGER NOT NULL DEFAULT 0 CHECK(probe IN (0,1)),
                       cause TEXT,
                       project_binding TEXT,
                       CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL)
                          OR (parent_command IS NOT NULL AND link_kind IS NOT NULL
                              AND ordinal IS NOT NULL AND ordinal >= 0))
                   );
                   CREATE INDEX commands_target ON commands(target, created_at);
                   CREATE INDEX commands_open ON commands(state)
                       WHERE state IN ('accepted','queued','dispatched','unknown');
                   CREATE UNIQUE INDEX commands_children
                       ON commands(parent_command, ordinal)
                       WHERE parent_command IS NOT NULL;
                   CREATE INDEX commands_request_open
                       ON commands(target, generation, request_id, state)
                       WHERE request_id IS NOT NULL;
                   CREATE INDEX commands_rule_action_window ON commands(created_at)
                       WHERE link_kind = 'rule_action';
                   CREATE TABLE turns(
                       agent INTEGER NOT NULL REFERENCES agents(id),
                       generation INTEGER NOT NULL,
                       turn_id TEXT NOT NULL,
                       state TEXT NOT NULL,
                       status TEXT,
                       final_text TEXT,
                       started_at INTEGER,
                       completed_at INTEGER,
                       PRIMARY KEY(agent, generation, turn_id)
                   );
                   CREATE TABLE processes (
                       host_boot_id TEXT NOT NULL CHECK(length(host_boot_id) > 0),
                       pid INTEGER NOT NULL CHECK(pid > 0 AND pid <= 4294967295),
                       start_time INTEGER NOT NULL CHECK(start_time >= 0),
                       owner TEXT NOT NULL,
                       role TEXT NOT NULL,
                       state TEXT NOT NULL CHECK(state IN ('running', 'exited', 'unknown')),
                       exit_code INTEGER,
                       updated_at INTEGER NOT NULL,
                       PRIMARY KEY(host_boot_id, pid, start_time),
                       CHECK(exit_code IS NULL OR state = 'exited')
                   );
                   CREATE INDEX processes_owner ON processes(owner, state);
                   CREATE TABLE workspaces (
                       agent INTEGER PRIMARY KEY REFERENCES agents(id),
                       target_generation INTEGER NOT NULL CHECK(target_generation > 0),
                       start_command_id TEXT NOT NULL REFERENCES commands(id),
                       project TEXT NOT NULL,
                       repo_path TEXT NOT NULL,
                       repo_common_dir TEXT NOT NULL,
                       requested_branch TEXT NOT NULL,
                       base_ref TEXT,
                       base_oid TEXT NOT NULL,
                       worktree_path TEXT NOT NULL UNIQUE,
                       frozen_setup TEXT NOT NULL,
                       phase TEXT NOT NULL CHECK(phase IN ('creating','setup_ready','setup_running','ready','unknown','failed','removing','removed')),
                       setup_index INTEGER NOT NULL DEFAULT 0 CHECK(setup_index >= 0),
                       outcome TEXT,
                       updated_at INTEGER NOT NULL
                   );
                   CREATE TABLE events(
                       seq INTEGER PRIMARY KEY AUTOINCREMENT,
                       ts INTEGER NOT NULL,
                       kind TEXT NOT NULL,
                       family TEXT,
                       agent INTEGER,
                       generation INTEGER,
                       source TEXT NOT NULL,
                       native_id TEXT,
                       cursor TEXT,
                       payload_digest BLOB,
                       data TEXT NOT NULL,
                       subject TEXT,
                       cause TEXT
                   );
                   CREATE TABLE project_registrations(
                       revision INTEGER PRIMARY KEY AUTOINCREMENT,
                       id TEXT NOT NULL UNIQUE,
                       checkout TEXT NOT NULL,
                       repo_common_dir TEXT NOT NULL,
                       profiles TEXT NOT NULL,
                       actions TEXT NOT NULL
                   );
                   INSERT INTO project_registrations(
                       id, checkout, repo_common_dir, profiles, actions
                   ) VALUES (
                       'legacy-run', '/legacy-run', '/legacy-run/.git', '[]', '["stop"]'
                   );"#,
            )
            .expect("insert pre-migration project registration");
    }

    let store = Store::open(&path).expect("apply registration and Git migration 18");
    let version: i64 = store
        .connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("read Store migration version");
    assert_eq!(version, crate::schema::CURRENT_SCHEMA_VERSION);
    let legacy = store
        .get_project_registration("owner", "legacy-run")
        .expect("read migrated registration")
        .expect("legacy registration remains present");
    assert_eq!(legacy.actions, vec!["stop"]);
    assert!(legacy.interpreters.is_empty());
    assert!(legacy.run_env.is_empty());
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn project_registration_list_rejects_oversized_whole_value_without_truncation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let profile_body = "p".repeat(470 * 1024);
    for id in ["large-project-a", "large-project-b"] {
        let registration = ProjectRegistrationInput {
            id: id.to_owned(),
            checkout: format!("/synthetic/{id}"),
            repo_common_dir: format!("/synthetic/{id}/.git"),
            git: None,
            profiles: vec![format!("{profile_body}-{id}")],
            actions: vec!["stop".to_owned()],
            interpreters: BTreeMap::new(),
            run_env: Vec::new(),
        };
        store
            .upsert_project_registration("owner", &registration, 0)
            .expect("each registration fits by itself");
    }

    let error = store
        .list_project_registrations("owner", None, 1_000)
        .expect_err("the complete result exceeds the response bound");
    assert_eq!(error.code, "frame_too_large");
}
