use super::{
    BatchCommit, ProjectRegistrationInput, RuleCatchUpAction, RuleCatchUpAdmission,
    RuleCursorOutcome, RuleDefinitionInput, RuleEventAdmission, Store, contains_secret_marker,
};
use eliot_core::api::{ProviderClearHoldRequest, ProviderCorrectiveEvidence, RuleCatchUpRequest};
use eliot_core::{
    AgentState, CommandState, EventCause, NativeEvent, NativeStoreBinding, ProviderHoldState,
    ProviderRestrictionKind, SourceBatch, SourceGap, SubmitCommand, TurnState,
};
use eliot_core::{
    WorkspaceCancelOutcome, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparationInput,
};
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use ulid::Ulid;

fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace"}),
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
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit session ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle launch");
    agent
}

fn ready_managed_codex_agent(
    store: &mut Store,
    checkout: &str,
    profile: &str,
    now: i64,
) -> (String, WorkspacePreparationInput) {
    let start_id = Ulid::new().to_string();
    let suffix = Ulid::new().to_string().to_ascii_lowercase();
    let branch = format!("codex/{profile}-{suffix}");
    let worktree_path = format!("{checkout}-worktree-{suffix}");
    let repo_common_dir = format!("{checkout}/.git");
    let base_oid = "d".repeat(40);
    let input = WorkspacePreparationInput {
        project: checkout.to_owned(),
        repo_path: checkout.to_owned(),
        repo_common_dir: repo_common_dir.clone(),
        requested_branch: branch.clone(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: base_oid.clone(),
        worktree_path: worktree_path.clone(),
        frozen_setup: Vec::new(),
    };
    let launch = store
        .submit_with_workspace(
            "owner",
            SubmitCommand {
                id: start_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({
                    "project": checkout,
                    "workspace": checkout,
                    "worktree": branch,
                    "harness": "codex",
                    "profile": profile,
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
        .expect("admit managed L3 launch");
    let agent = launch.command.target.expect("managed Agent");
    let native_scope = format!("{checkout}:{agent}:scope");
    let native_ref = format!("{checkout}:{agent}:thread");
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
        .expect("verify managed L3 workspace");
    store
        .dispatch(&start_id, now + 2)
        .expect("dispatch managed L3 launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({
                        "command_id": start_id,
                        "native_scope": native_scope,
                        "native_ref": native_ref,
                    }),
                }],
                cursor: None,
            },
            now + 3,
        )
        .expect("commit managed Codex session readiness");
    store
        .settle(&launch.command.id, CommandState::Applied, None, now + 4)
        .expect("settle managed L3 launch");
    (agent, input)
}

fn send_request(agent: &str, generation: i64, text: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "send");
    request.target = Some(agent.to_owned());
    request.args = json!({"text":text});
    request.expected_generation = Some(generation);
    request
}

fn start_fake_turn(store: &mut Store, agent: &str, now: i64) -> String {
    let send = send_request(agent, 1, "fake interrupt turn");
    let send_id = send.id.clone();
    store
        .submit("owner", send, now)
        .expect("admit Fake turn input");
    store
        .dispatch(&send_id, now + 1)
        .expect("dispatch Fake turn input");
    let turn_id = format!("fake-turn-{send_id}");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("fake-interrupt-{send_id}"),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("fake-interrupt-{send_id}:started"),
                    data: json!({"command_id":send_id,"turn_id":turn_id.clone()}),
                }],
                cursor: None,
            },
            now + 2,
        )
        .expect("commit Fake active turn");
    turn_id
}

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

fn capacity_launch_request(
    project: &str,
    profile: &str,
    scopes: serde_json::Value,
) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.args = json!({
        "project": project,
        "harness": "codex",
        "profile": profile,
        "workspace": format!("synthetic-capacity-{project}"),
        "profile_snapshot": {"capacity_scopes": scopes},
    });
    request.expected_generation = Some(0);
    request
}

fn capacity_generation(store: &Store, agent: &str) -> Option<i64> {
    let agent = eliot_core::parse_agent_id(agent).expect("valid Agent id");
    store
        .connection
        .query_row(
            "SELECT capacity_generation FROM agents WHERE id = ?1",
            [agent],
            |row| row.get(0),
        )
        .expect("read capacity generation")
}

fn capacity_store_path() -> PathBuf {
    std::env::temp_dir().join(format!("eliot-capacity-{}.sqlite", Ulid::new()))
}

fn remove_capacity_store_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let _ = fs::remove_file(PathBuf::from(candidate));
    }
}

fn ready_capacity_agent(store: &mut Store, project: &str, now: i64) -> (String, String) {
    let launch = capacity_launch_request(
        project,
        "resume-profile",
        json!([{"scope":"profile:resume-profile","max_live":1}]),
    );
    let launch_id = launch.id.clone();
    let admission = store
        .submit("owner", launch, now)
        .expect("admit capacity launch");
    let agent = admission.command.target.expect("launch target");
    store
        .dispatch(&launch_id, now)
        .expect("dispatch capacity launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready"),
                    data: json!({
                        "command_id": launch_id,
                        "native_scope": format!("capacity-scope:{project}"),
                        "native_ref": format!("capacity-ref:{project}")
                    }),
                }],
            },
            now,
        )
        .expect("commit capacity session ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle capacity launch");
    (agent, launch_id)
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
        profiles: vec!["fake".to_owned()],
        actions: vec!["stop".to_owned()],
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
                verb: "launch".to_owned(),
                args: json!({
                    "project": checkout,
                    "workspace": checkout,
                    "worktree": branch,
                    "harness": "fake",
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
        profiles: vec!["fake".to_owned()],
        actions: vec!["stop".to_owned()],
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
        profiles: vec!["project-profile".to_owned()],
        actions: vec!["stop".to_owned()],
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
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
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
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
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
                profiles: vec![profile.to_owned()],
                actions: vec!["send".to_owned()],
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
                        "native_scope": "history-generation-binding:scope-2",
                        "native_ref": "history-generation-binding:thread-2"
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
fn project_fake_interrupt_requires_explicit_action_approval() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let project = "fake-interrupt-approval";
    let principal = format!("project:{project}");
    let agent = ready_agent(&mut store, project, now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: "synthetic-workspace".to_owned(),
        repo_common_dir: "synthetic-workspace/.git".to_owned(),
        profiles: vec!["fake".to_owned()],
        actions: vec!["send".to_owned()],
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
fn fake_interrupt_rejects_stale_bindings_and_keeps_other_controls_unsupported() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "fake-interrupt-stale-turn", now);
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

    for verb in ["steer", "approve", "deny", "answer"] {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), verb);
        request.target = Some(agent.clone());
        request.args = if verb == "steer" {
            json!({"text":"unsupported Fake steer"})
        } else {
            json!({})
        };
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        let command_id = request.id.clone();
        let before_unsupported = store.high_water().unwrap();
        assert_eq!(
            store.submit("owner", request, now + 6).unwrap_err().code,
            "unsupported",
            "Fake {verb} remains unsupported",
        );
        assert!(store.get_command(&command_id).unwrap().is_none());
        assert_eq!(store.high_water().unwrap(), before_unsupported);
    }
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
                profiles: vec![profile.to_owned()],
                actions: vec!["resume".to_owned()],
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
                profiles: vec![profile.to_owned()],
                actions: vec!["stop".to_owned()],
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
fn project_registration_list_rejects_oversized_whole_value_without_truncation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let profile_body = "p".repeat(470 * 1024);
    for id in ["large-project-a", "large-project-b"] {
        let registration = ProjectRegistrationInput {
            id: id.to_owned(),
            checkout: format!("/synthetic/{id}"),
            repo_common_dir: format!("/synthetic/{id}/.git"),
            profiles: vec![format!("{profile_body}-{id}")],
            actions: vec!["stop".to_owned()],
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

#[test]
fn session_end_keeps_dispatched_stop_pending_until_exact_reply() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "stop-session-ended", now);
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
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

    let settled = store
        .settle(
            &stop_id,
            CommandState::Applied,
            Some(json!({"state":"applied"})),
            now + 3,
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
        id: start_id.clone(), target: None, verb: "launch".to_owned(),
        args: json!({"project":input.project,"workspace":input.repo_path,"worktree":input.requested_branch,
            "harness":"fake","profile":"fake",
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
                verb: "launch".to_owned(),
                args: json!({"project":"send-event", "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit fake launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
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
    store
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
            now,
        )
        .expect("commit synthetic secret question");

    let marker = "synthetic-answer-secret-marker";
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: answer_id.clone(),
                target: Some(agent_id),
                verb: "answer".to_owned(),
                args: json!({"text": marker}),
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
}

#[test]
fn native_request_cleanup_does_not_acknowledge_reply() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system time in milliseconds fits in i64");
    let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
    let command = |verb: &str, target, args, request_id| SubmitCommand {
        id: Ulid::new().to_string(),
        target,
        verb: verb.to_owned(),
        args,
        expected_generation: None,
        expected_turn_id: None,
        request_id,
        probe: false,
    };
    let launch = command(
        "launch",
        None,
        json!({
            "project": "native-request-cleanup",
            "workspace": "synthetic-workspace",
            "harness": "codex",
            "profile": "synthetic-codex",
            "profile_snapshot": {}
        }),
        None,
    );
    let launch_id = launch.id.clone();
    let agent_id = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch returns an Agent");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    let batch = |kind: &str, data| SourceBatch {
        batch_id: Ulid::new().to_string(),
        agent_id: agent_id.clone(),
        generation: 1,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: kind.to_owned(),
            native_id: Ulid::new().to_string(),
            data,
        }],
        cursor: None,
    };
    store
        .put_batch(batch("session.ready", json!({})), now)
        .expect("commit session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle launch");
    let send = command(
        "send",
        Some(agent_id.clone()),
        json!({"text": "open native requests"}),
        None,
    );
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit input");
    store.dispatch(&send_id, now).expect("dispatch input");
    store
        .put_batch(
            batch(
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    for request_id in ["cleanup-only", "uncertain-reply"] {
        store
            .put_batch(
                batch(
                    "request.opened",
                    json!({
                        "command_id": send_id,
                        "request_id": request_id,
                        "turn_id": "turn-1",
                        "kind": "approval",
                        "details": {}
                    }),
                ),
                now,
            )
            .expect("commit pending request");
    }
    let reply = command(
        "approve",
        Some(agent_id.clone()),
        json!({}),
        Some("uncertain-reply".to_owned()),
    );
    let reply_id = reply.id.clone();
    store.submit("owner", reply, now).expect("admit reply");
    store.dispatch(&reply_id, now).expect("dispatch reply");
    let unknown_reply = store
        .settle(
            &reply_id,
            CommandState::Unknown,
            Some(json!({"reason": "native reply outcome unknown"})),
            now,
        )
        .expect("retain unknown reply outcome")
        .value;

    let close_data = json!({"request_id": "cleanup-only", "turn_id": "turn-1"});
    let mut wrong_source = batch("request.closed", close_data.clone());
    wrong_source.source = "unowned".to_owned();
    let mut wrong_generation = batch("request.closed", close_data.clone());
    wrong_generation.generation = 2;
    let before_rejections = store.high_water().expect("read event high water");
    for (invalid, code) in [
        (wrong_source, "source_integrity"),
        (wrong_generation, "stale_generation"),
        (
            batch(
                "request.closed",
                json!({"request_id": "missing", "turn_id": "turn-1"}),
            ),
            "source_integrity",
        ),
        (
            batch(
                "request.closed",
                json!({"request_id": "cleanup-only", "turn_id": "other-turn"}),
            ),
            "source_integrity",
        ),
        (batch("request.resolved", close_data.clone()), "invalid"),
        (
            batch(
                "request.resolved",
                json!({
                    "request_id": "cleanup-only", "turn_id": "turn-1",
                    "command_id": send_id
                }),
            ),
            "source_integrity",
        ),
        (
            batch(
                "request.closed",
                json!({
                    "request_id": "cleanup-only", "turn_id": "turn-1",
                    "command_id": reply_id
                }),
            ),
            "invalid",
        ),
    ] {
        let error = store
            .put_batch(invalid, now)
            .expect_err("reject wrong binding");
        assert_eq!(error.code, code);
    }
    assert_eq!(
        store.high_water().expect("read event high water"),
        before_rejections
    );

    store
        .record_source_gap(
            SourceGap {
                gap_id: "independent-gap".to_owned(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                families: vec!["request".to_owned()],
                reason: "synthetic source gap".to_owned(),
                from: None,
                to: None,
            },
            now,
        )
        .expect("record independent source gap");
    let source_before = store
        .read_source_state(&agent_id, 1, "stream")
        .expect("read source gap before cleanup");
    for (request_id, expected_state, remaining) in [
        ("cleanup-only", AgentState::Waiting, 1),
        ("uncertain-reply", AgentState::Busy, 0),
    ] {
        let cleanup = store
            .put_batch(
                batch(
                    "request.closed",
                    json!({"request_id": request_id, "turn_id": "turn-1"}),
                ),
                now,
            )
            .expect("commit native request cleanup without a reply acknowledgement");
        assert_eq!(cleanup.events.len(), 1);
        assert_eq!(cleanup.events[0].kind, "request.closed");
        assert!(cleanup.events[0].data.get("command_id").is_none());
        let agent = store
            .get_agent(&agent_id)
            .expect("read Agent")
            .expect("Agent exists");
        assert_eq!(agent.state, expected_state);
        assert_eq!(agent.pending.len(), remaining);
        assert!(!agent.pending.iter().any(|pending| pending.id == request_id));
        assert_eq!(agent.active_turn.as_deref(), Some("turn-1"));
        assert_eq!(
            store.get_command(&reply_id).expect("read reply"),
            Some(unknown_reply.clone())
        );

        let late_reply = command(
            "approve",
            Some(agent_id.clone()),
            json!({}),
            Some(request_id.to_owned()),
        );
        let late_reply_id = late_reply.id.clone();
        let error = store
            .submit("owner", late_reply, now)
            .expect_err("durable pending row must already be resolved");
        assert_eq!(error.code, "source_integrity");
        assert!(
            store
                .get_command(&late_reply_id)
                .expect("read late reply")
                .is_none()
        );
    }
    let source_after = store
        .read_source_state(&agent_id, 1, "stream")
        .expect("read source gap after cleanup");
    for field in ["gap_id", "gap_digest", "gap_seq", "health"] {
        assert_eq!(source_after[field], source_before[field]);
    }
}

fn native_store_request() -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.args = json!({
        "project":"native-store-binding", "workspace":"/synthetic-workspace",
        "harness":"codex", "profile":"codex-fixture",
        "profile_snapshot":{
            "harness":"codex", "model":"fixture", "pass_env":["CODEX_HOME"],
            "pass_env_values":{"CODEX_HOME":" raw-home ","CODEX_SQLITE_HOME":null}
        }
    });
    request
}

fn native_store_binding() -> NativeStoreBinding {
    NativeStoreBinding {
        home: "/verified-home".to_owned(),
        root: "/verified-store".to_owned(),
        native_scope: "verified-scope".to_owned(),
    }
}

fn provider_send_request(agent: &str, id: &str, probe: bool) -> SubmitCommand {
    SubmitCommand {
        id: id.to_owned(),
        target: Some(agent.to_owned()),
        verb: "send".to_owned(),
        args: json!({"text":"synthetic provider hold input"}),
        expected_generation: Some(1),
        expected_turn_id: None,
        request_id: None,
        probe,
    }
}

fn provider_native_event(
    store: &mut Store,
    agent: &str,
    native_id: &str,
    kind: &str,
    data: serde_json::Value,
    now: i64,
) -> BatchCommit {
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("provider-batch-{native_id}"),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    kind: kind.to_owned(),
                    native_id: native_id.to_owned(),
                    data,
                    ts: None,
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit provider test event")
}

fn start_provider_turn(
    store: &mut Store,
    agent: &str,
    command_id: &str,
    probe: bool,
    now: i64,
) -> String {
    let admission = store
        .submit(
            "owner",
            provider_send_request(agent, command_id, probe),
            now,
        )
        .expect("admit provider test send");
    assert_eq!(admission.command.probe, probe);
    store
        .dispatch(command_id, now + 1)
        .expect("dispatch provider test send");
    let turn_id = format!("turn-{command_id}");
    provider_native_event(
        store,
        agent,
        &format!("{command_id}-started"),
        "turn.started",
        json!({"command_id":command_id,"turn_id":turn_id}),
        now + 2,
    );
    turn_id
}

fn complete_provider_turn(
    store: &mut Store,
    agent: &str,
    command_id: &str,
    turn_id: &str,
    status: &str,
    restriction: Option<(ProviderRestrictionKind, Option<i64>)>,
    now: i64,
) -> BatchCommit {
    let mut data = json!({"command_id":command_id,"turn_id":turn_id,"status":status});
    if let Some((kind, retry_after)) = restriction {
        data["provider_restriction"] = json!({
            "kind": kind.as_str(),
            "retry_after": retry_after,
        });
    }
    provider_native_event(
        store,
        agent,
        &format!("{command_id}-completed"),
        "turn.completed",
        data,
        now,
    )
}

fn clear_provider_hold_request(
    scope: &str,
    expected_probe_command: Option<String>,
) -> ProviderClearHoldRequest {
    ProviderClearHoldRequest {
        id: Ulid::new().to_string(),
        scope: scope.to_owned(),
        corrective_evidence: ProviderCorrectiveEvidence::OwnerVerified,
        expected_probe_command,
    }
}

fn ready_steer_agent(store: &mut Store, project: &str, now: i64) -> String {
    let launch = capacity_launch_request(project, "steer-profile", json!([]));
    let launch_id = launch.id.clone();
    let admission = store
        .submit("owner", launch, now)
        .expect("admit Codex steer test Agent");
    let agent = admission.command.target.expect("steer test Agent id");
    store
        .dispatch(&launch_id, now)
        .expect("dispatch Codex steer test Agent");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready"),
                    data: json!({
                        "command_id": launch_id,
                        "native_scope": format!("provider-steer:{project}"),
                        "native_ref": format!("provider-thread:{project}"),
                    }),
                    ts: None,
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit Codex steer test readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle Codex steer test launch");
    agent
}

#[test]
fn store_probe_false_preserves_legacy_digest_and_probe_toggle_conflicts() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-digest", now);
    let request = provider_send_request(&agent, &Ulid::new().to_string(), false);
    let request_id = request.id.clone();
    let admitted = store.submit("owner", request.clone(), now + 1).unwrap();
    assert!(!admitted.command.probe);

    let expected_payload = json!({
        "target": request.target,
        "verb": request.verb,
        "args": request.args,
        "expected_generation": request.expected_generation,
        "expected_turn_id": request.expected_turn_id,
    });
    let expected_digest = super::canonical::digest(&expected_payload).unwrap();
    let stored_digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM commands WHERE id = ?1",
            [&request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_digest, expected_digest);

    let mut changed_probe = request;
    changed_probe.probe = true;
    assert_eq!(
        store
            .submit("owner", changed_probe, now + 2)
            .unwrap_err()
            .code,
        "id_conflict"
    );
}

#[test]
fn provider_probe_claim_survives_reopen_and_exact_success_clears_hold() {
    let path = capacity_store_path();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let (agent, probe_id, pending_agent, pending_send_id);
    {
        let mut store = Store::open(&path).unwrap();
        agent = ready_agent(&mut store, "provider-probe-reopen", now);
        pending_agent = ready_agent(&mut store, "provider-pending-dispatch", now + 1);

        pending_send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                provider_send_request(&pending_agent, &pending_send_id, false),
                now + 2,
            )
            .expect("admit send before hold arrives");

        let first_id = Ulid::new().to_string();
        let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 3);
        let restriction_batch = complete_provider_turn(
            &mut store,
            &agent,
            &first_id,
            &first_turn,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 6,
        );
        assert!(
            restriction_batch
                .events
                .iter()
                .any(|event| event.kind == "provider.hold_updated")
        );
        let blocked = store.dispatch(&pending_send_id, now + 7).unwrap_err();
        assert_eq!(blocked.code, "provider_held");
        assert_eq!(
            store.get_command(&pending_send_id).unwrap().unwrap().state,
            CommandState::Accepted
        );

        let rejected_id = Ulid::new().to_string();
        assert_eq!(
            store
                .submit(
                    "owner",
                    provider_send_request(&agent, &rejected_id, false),
                    now + 8,
                )
                .unwrap_err()
                .code,
            "provider_held"
        );
        assert!(store.get_command(&rejected_id).unwrap().is_none());

        let count_before_launch = store.agent_count().unwrap();
        let launch_id = Ulid::new().to_string();
        let mut launch = SubmitCommand::new(launch_id, "launch");
        launch.args =
            json!({"project":"provider-held-launch", "workspace":"synthetic-held-launch"});
        launch.expected_generation = Some(0);
        assert_eq!(
            store.submit("owner", launch, now + 9).unwrap_err().code,
            "provider_held"
        );
        assert_eq!(store.agent_count().unwrap(), count_before_launch);

        probe_id = Ulid::new().to_string();
        let probe_request = provider_send_request(&agent, &probe_id, true);
        let admitted = store
            .submit("owner", probe_request.clone(), now + 10)
            .expect("claim eligible provider probe");
        assert!(admitted.command.probe);
        assert!(
            !store
                .submit("owner", probe_request.clone(), now + 11)
                .expect("retain duplicate probe admission")
                .is_new
        );
        let mut changed_probe = probe_request;
        changed_probe.probe = false;
        assert_eq!(
            store
                .submit("owner", changed_probe, now + 11)
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert!(store.get_command(&probe_id).unwrap().unwrap().probe);
        store.dispatch(&probe_id, now + 12).unwrap();
        store
            .settle(&probe_id, CommandState::Unknown, None, now + 13)
            .expect("unknown probe keeps its claim");

        let clear = clear_provider_hold_request("profile:fake", Some(probe_id.clone()));
        assert_eq!(
            store
                .clear_provider_hold("owner", &clear, now + 14)
                .unwrap_err()
                .code,
            "bad_state"
        );
    }

    {
        let mut store = Store::open(&path).unwrap();
        let hold = store
            .list_provider_holds(None, 10)
            .unwrap()
            .holds
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(hold.state, ProviderHoldState::Probing);
        assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));

        let turn_id = format!("turn-{probe_id}");
        provider_native_event(
            &mut store,
            &agent,
            &format!("{probe_id}-started"),
            "turn.started",
            json!({"command_id":probe_id,"turn_id":turn_id}),
            now + 15,
        );
        let completed = complete_provider_turn(
            &mut store,
            &agent,
            &probe_id,
            &turn_id,
            "completed",
            None,
            now + 16,
        );
        assert!(
            completed
                .events
                .iter()
                .any(|event| event.kind == "provider.hold_cleared")
        );
        assert!(
            store
                .list_provider_holds(None, 10)
                .unwrap()
                .holds
                .is_empty()
        );
    }
    remove_capacity_store_files(&path);
}

#[test]
fn failed_probe_command_without_exact_terminal_keeps_probe_claim() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-failed-command", now);
    let first_id = Ulid::new().to_string();
    let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 1);
    complete_provider_turn(
        &mut store,
        &agent,
        &first_id,
        &first_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 4,
    );

    let probe_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            provider_send_request(&agent, &probe_id, true),
            now + 5,
        )
        .expect("admit probe");
    store.dispatch(&probe_id, now + 6).unwrap();
    store
        .settle(
            &probe_id,
            CommandState::Failed,
            Some(json!({"reason":"known failure may have partial effects"})),
            now + 7,
        )
        .expect("retain known failed command result");

    let command = store.get_command(&probe_id).unwrap().unwrap();
    assert_eq!(command.state, CommandState::Failed);
    assert!(command.probe);
    let hold = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
    assert_eq!(hold.kind, ProviderRestrictionKind::RateLimit);
    assert_eq!(hold.state, ProviderHoldState::Probing);
    assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));
}

#[test]
fn applied_probe_with_running_turn_is_not_treated_as_terminal() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-running-turn", now);
    let first_id = Ulid::new().to_string();
    let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 1);
    complete_provider_turn(
        &mut store,
        &agent,
        &first_id,
        &first_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 4,
    );

    let probe_id = Ulid::new().to_string();
    let turn_id = start_provider_turn(&mut store, &agent, &probe_id, true, now + 5);
    assert_eq!(
        store.get_command(&probe_id).unwrap().unwrap().state,
        CommandState::Applied
    );
    assert_eq!(
        store.get_turn(&agent, 1, &turn_id).unwrap().unwrap().state,
        TurnState::Running
    );
    assert_eq!(
        store
            .settle(
                &probe_id,
                CommandState::Failed,
                Some(json!({"reason":"turn is still running"})),
                now + 8,
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    let hold = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
    assert_eq!(hold.state, ProviderHoldState::Probing);
    assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));
}

#[test]
fn provider_hold_gates_steer_at_admission_and_dispatch_only() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let busy_agent = ready_steer_agent(&mut store, "provider-steer-busy", now);
    let hold_agent = ready_steer_agent(&mut store, "provider-steer-hold", now + 1);
    let active_send = Ulid::new().to_string();
    let turn_id = start_provider_turn(&mut store, &busy_agent, &active_send, false, now + 2);

    let accepted_steer_id = Ulid::new().to_string();
    let mut accepted_steer = SubmitCommand::new(accepted_steer_id.clone(), "steer");
    accepted_steer.target = Some(busy_agent.clone());
    accepted_steer.args = json!({"text":"synthetic steer"});
    accepted_steer.expected_generation = Some(1);
    accepted_steer.expected_turn_id = Some(turn_id.clone());
    store
        .submit("owner", accepted_steer, now + 5)
        .expect("admit steer before scope becomes held");

    let restriction_send = Ulid::new().to_string();
    let restriction_turn =
        start_provider_turn(&mut store, &hold_agent, &restriction_send, false, now + 6);
    complete_provider_turn(
        &mut store,
        &hold_agent,
        &restriction_send,
        &restriction_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 9,
    );

    let rejected_steer_id = Ulid::new().to_string();
    let mut rejected_steer = SubmitCommand::new(rejected_steer_id.clone(), "steer");
    rejected_steer.target = Some(busy_agent.clone());
    rejected_steer.args = json!({"text":"synthetic steer"});
    rejected_steer.expected_generation = Some(1);
    rejected_steer.expected_turn_id = Some(turn_id.clone());
    let before_rejected_steer = store.high_water().unwrap();
    assert_eq!(
        store
            .submit("owner", rejected_steer, now + 10)
            .unwrap_err()
            .code,
        "provider_held"
    );
    assert!(store.get_command(&rejected_steer_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), before_rejected_steer);

    let before_blocked_dispatch = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch(&accepted_steer_id, now + 11)
            .unwrap_err()
            .code,
        "provider_held"
    );
    assert_eq!(store.high_water().unwrap(), before_blocked_dispatch);
    let retained_steer = store.get_command(&accepted_steer_id).unwrap().unwrap();
    assert_eq!(retained_steer.state, CommandState::Accepted);
    assert_eq!(retained_steer.expected_generation, Some(1));
    assert_eq!(retained_steer.turn_id.as_deref(), Some(turn_id.as_str()));

    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(busy_agent);
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some(turn_id.clone());
    let interrupt_id = interrupt.id.clone();
    store
        .submit("owner", interrupt, now + 12)
        .expect("interrupt remains available under a provider hold");
    assert_eq!(
        store.dispatch(&interrupt_id, now + 12).unwrap().value.state,
        CommandState::Dispatched
    );

    let clear = clear_provider_hold_request("profile:steer-profile", None);
    store
        .clear_provider_hold("owner", &clear, now + 13)
        .unwrap();
    assert_eq!(
        store
            .dispatch(&accepted_steer_id, now + 14)
            .unwrap()
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn same_scope_restriction_during_probe_survives_exact_probe_completion() {
    for (index, (kind, expected_kind)) in [
        (
            ProviderRestrictionKind::Quota,
            ProviderRestrictionKind::Quota,
        ),
        (ProviderRestrictionKind::Auth, ProviderRestrictionKind::Auth),
        (
            ProviderRestrictionKind::ModelRemoved,
            ProviderRestrictionKind::ModelRemoved,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let agent_a = ready_agent(&mut store, &format!("provider-interleave-a-{index}"), now);
        let agent_b = ready_agent(
            &mut store,
            &format!("provider-interleave-b-{index}"),
            now + 1,
        );
        let agent_probe = ready_agent(
            &mut store,
            &format!("provider-interleave-p-{index}"),
            now + 2,
        );
        let command_a = Ulid::new().to_string();
        let turn_a = start_provider_turn(&mut store, &agent_a, &command_a, false, now + 3);
        let command_b = Ulid::new().to_string();
        let turn_b = start_provider_turn(&mut store, &agent_b, &command_b, false, now + 3);

        complete_provider_turn(
            &mut store,
            &agent_a,
            &command_a,
            &turn_a,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 6,
        );
        let probe_id = Ulid::new().to_string();
        let turn_probe = start_provider_turn(&mut store, &agent_probe, &probe_id, true, now + 7);
        let probing_clear = clear_provider_hold_request("profile:fake", Some(probe_id.clone()));
        assert_eq!(
            store
                .clear_provider_hold("owner", &probing_clear, now + 8)
                .unwrap_err()
                .code,
            "bad_state"
        );

        complete_provider_turn(
            &mut store,
            &agent_b,
            &command_b,
            &turn_b,
            "failed",
            Some((kind, None)),
            now + 9,
        );
        let during_probe = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
        assert_eq!(during_probe.kind, expected_kind);
        assert_eq!(during_probe.state, ProviderHoldState::Probing);
        assert_eq!(
            during_probe.probe_command.as_deref(),
            Some(probe_id.as_str())
        );

        complete_provider_turn(
            &mut store,
            &agent_probe,
            &probe_id,
            &turn_probe,
            "completed",
            None,
            now + 10,
        );
        let after_probe = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
        assert_eq!(after_probe.kind, expected_kind);
        assert_eq!(after_probe.state, ProviderHoldState::Held);
        assert_eq!(after_probe.probe_command, None);
        if matches!(
            expected_kind,
            ProviderRestrictionKind::Auth | ProviderRestrictionKind::ModelRemoved
        ) {
            let forbidden_probe = Ulid::new().to_string();
            assert_eq!(
                store
                    .submit(
                        "owner",
                        provider_send_request(&agent_probe, &forbidden_probe, true),
                        now + 11,
                    )
                    .unwrap_err()
                    .code,
                "provider_held"
            );
            assert!(store.get_command(&forbidden_probe).unwrap().is_none());
        }

        let clear = clear_provider_hold_request("profile:fake", None);
        let cleared = store
            .clear_provider_hold("owner", &clear, now + 12)
            .unwrap();
        assert!(cleared.value.cleared);
        assert_eq!(cleared.events.len(), 1);
        assert_eq!(cleared.events[0].kind, "provider.hold_cleared");
        let replay = store
            .clear_provider_hold("owner", &clear, now + 13)
            .unwrap();
        assert_eq!(replay.value, cleared.value);
        assert!(replay.events.is_empty());
        let stale_clear =
            clear_provider_hold_request("profile:fake", Some("stale-probe-command".to_owned()));
        let absent = store
            .clear_provider_hold("owner", &stale_clear, now + 13)
            .unwrap();
        assert!(!absent.value.cleared);
        assert!(absent.events.is_empty());

        let renewed_id = Ulid::new().to_string();
        let renewed_turn =
            start_provider_turn(&mut store, &agent_probe, &renewed_id, false, now + 13);
        complete_provider_turn(
            &mut store,
            &agent_probe,
            &renewed_id,
            &renewed_turn,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 16,
        );
        let stale_replay = store
            .clear_provider_hold("owner", &clear, now + 17)
            .unwrap();
        assert_eq!(stale_replay.value, cleared.value);
        assert!(stale_replay.events.is_empty());
        let renewed_hold = store.list_provider_holds(None, 10).unwrap();
        assert_eq!(renewed_hold.holds.len(), 1);
        assert_eq!(
            renewed_hold.holds[0].kind,
            ProviderRestrictionKind::RateLimit
        );
    }
}

#[test]
fn native_store_binding_commits_without_changing_admitted_input_or_retry_binding() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM commands WHERE id = ?1",
            [&request.id],
            |row| row.get(0),
        )
        .unwrap();
    let binding = native_store_binding();
    let dispatched = store
        .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &binding)
        .unwrap();
    assert_eq!(dispatched.value.state, CommandState::Dispatched);
    assert_eq!(dispatched.value.args, admitted.command.args);
    let agent = store.get_agent(id).unwrap().unwrap();
    assert_eq!(agent.generation, 1);
    assert_eq!(agent.state, AgentState::Starting);
    assert_eq!(
        agent.profile_snapshot["native_store_binding"],
        json!(binding)
    );
    let mut input = agent.profile_snapshot.clone();
    input
        .as_object_mut()
        .unwrap()
        .remove("native_store_binding");
    assert_eq!(input, expected.profile_snapshot);
    assert_eq!(
        digest,
        store
            .connection
            .query_row(
                "SELECT payload_digest FROM commands WHERE id = ?1",
                [&request.id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap()
    );
    let before = store.high_water().unwrap();
    let token = store.get_agent_token(id, 1).unwrap();
    let retained = store.submit("owner", request.clone(), now + 2).unwrap();
    assert!(!retained.is_new);
    assert!(retained.events.is_empty());
    assert_eq!(retained.command, dispatched.value);
    assert_eq!(
        store
            .dispatch_with_native_store_binding(&request.id, now + 2, &expected, &binding)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    assert_eq!(store.get_agent_token(id, 1).unwrap(), token);

    for (kind, native_id, data) in [
        (
            "session.ready",
            "ready",
            json!({"command_id":request.id,"native_scope":binding.native_scope,"native_ref":"thread"}),
        ),
        ("session.ended", "ended", json!({})),
    ] {
        store
            .put_batch(
                SourceBatch {
                    batch_id: native_id.to_owned(),
                    agent_id: id.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    cursor: None,
                    events: vec![NativeEvent {
                        kind: kind.to_owned(),
                        native_id: native_id.to_owned(),
                        data,
                        ts: None,
                    }],
                },
                now + 3,
            )
            .unwrap();
    }
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(id.to_owned());
    resume.expected_generation = Some(1);
    store.submit("owner", resume.clone(), now + 4).unwrap();
    store.dispatch(&resume.id, now + 4).unwrap();
    assert_eq!(
        store.get_agent(id).unwrap().unwrap().profile_snapshot,
        agent.profile_snapshot
    );
}

#[test]
fn native_store_binding_rechecks_probe_identity_and_snapshot_before_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let before = store.high_water().unwrap();
    for (field, value) in [
        ("id", json!("a999")),
        ("workspace", json!("/changed")),
        ("profile_snapshot", json!({})),
        ("profile", json!("changed")),
        ("harness", json!("fake")),
        ("generation", json!(1)),
        ("state", json!("stopped")),
    ] {
        let mut changed = serde_json::to_value(&expected).unwrap();
        changed[field] = value;
        let changed = serde_json::from_value(changed).unwrap();
        assert_eq!(
            store
                .dispatch_with_native_store_binding(
                    &request.id,
                    now + 1,
                    &changed,
                    &native_store_binding()
                )
                .unwrap_err()
                .code,
            "stale_generation",
            "{field}"
        );
        assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
        assert_eq!(store.high_water().unwrap(), before);
    }
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap(),
        admitted.command
    );
    assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
    let sources: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM source_state WHERE generation=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sources, 0);
}

#[test]
fn native_store_binding_preserves_workspace_readiness_and_cancellation_fences() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let mut request = native_store_request();
    let input = WorkspacePreparationInput {
        project: "/synthetic-owner".to_owned(),
        repo_path: "/synthetic-owner".to_owned(),
        repo_common_dir: "/synthetic-owner/.git".to_owned(),
        requested_branch: "codex/binding".to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: "a".repeat(40),
        worktree_path: "/synthetic-worktree".to_owned(),
        frozen_setup: Vec::new(),
    };
    request.args["project"] = json!(input.project);
    request.args["workspace"] = json!(input.repo_path);
    request.args["worktree"] = json!(input.requested_branch);
    let admitted = store
        .submit_with_workspace("owner", request.clone(), input.clone(), now)
        .unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(
                &request.id,
                now,
                &expected,
                &native_store_binding()
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    store
        .settle_workspace_creation(
            &request.id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path,
                repo_common_dir: input.repo_common_dir,
                worktree_path: input.worktree_path,
                branch: input.requested_branch,
                head: input.base_oid,
            },
            now + 1,
        )
        .unwrap();
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(id.to_owned());
    stop.expected_generation = Some(0);
    store.submit("owner", stop.clone(), now + 2).unwrap();
    store.dispatch(&stop.id, now + 2).unwrap();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(
                &request.id,
                now + 3,
                &expected,
                &native_store_binding()
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    let current = store.get_agent(id).unwrap().unwrap();
    assert_eq!(current.generation, 0);
    assert_eq!(current.profile_snapshot, expected.profile_snapshot);
    assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
    assert!(store.list_source_states(id, 1).unwrap().is_empty());
}

#[test]
fn native_store_binding_is_derived_only_after_new_admission() {
    for verb in ["launch", "attach"] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let mut request = native_store_request();
        if verb == "attach" {
            request.verb = verb.to_owned();
            request.args.as_object_mut().unwrap().remove("workspace");
            request.args["mode"] = json!("observe");
            request.args["native_scope"] = json!("observe-scope");
            request.args["native_ref"] = json!("thread");
        }
        let admitted = store.submit("owner", request.clone(), now).unwrap();
        let before = store.high_water().unwrap();
        for binding in [json!(native_store_binding()), serde_json::Value::Null] {
            let mut supplied = request.clone();
            supplied.id = Ulid::new().to_string();
            supplied.args["profile_snapshot"]["native_store_binding"] = binding;
            assert_eq!(
                store
                    .submit("owner", supplied.clone(), now + 1)
                    .unwrap_err()
                    .code,
                "invalid",
                "{verb}"
            );
            assert!(store.get_command(&supplied.id).unwrap().is_none());
            assert_eq!(store.agent_count().unwrap(), 1);
            assert_eq!(store.high_water().unwrap(), before);
        }
        let retained = store.submit("owner", request.clone(), now + 2).unwrap();
        assert!(!retained.is_new);
        assert_eq!(retained.command, admitted.command);
        if verb == "attach" {
            // Attach's immutable semantic payload excludes the derived snapshot;
            // retained-ID resolution must still win before new-admission checks.
            request.args["profile_snapshot"]["native_store_binding"] =
                json!(native_store_binding());
            let retained = store.submit("owner", request, now + 2).unwrap();
            assert!(!retained.is_new);
            assert_eq!(retained.command, admitted.command);
            assert!(retained.events.is_empty());
        }
        assert_eq!(store.high_water().unwrap(), before);
    }
}

#[test]
fn native_store_binding_retains_identical_internal_binding_and_rejects_replacement() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let binding = native_store_binding();
    // Seed retained internal metadata, never a caller-supplied admission field.
    let mut snapshot = request.args["profile_snapshot"].clone();
    snapshot["native_store_binding"] = json!(binding);
    store
        .connection
        .execute(
            "UPDATE agents SET profile_snapshot=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&snapshot).unwrap(),
                eliot_core::parse_agent_id(id).unwrap()
            ],
        )
        .unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let mut replacement = binding.clone();
    replacement.root = "/different-store".to_owned();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &replacement)
            .unwrap_err()
            .code,
        "source_integrity"
    );
    assert_eq!(store.high_water().unwrap(), before);
    assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
    store
        .dispatch_with_native_store_binding(&request.id, now + 2, &expected, &binding)
        .unwrap();
    assert_eq!(
        store.get_agent(id).unwrap().unwrap().profile_snapshot,
        snapshot
    );
}

#[test]
fn native_store_binding_frame_and_persistence_failures_roll_back_entire_dispatch() {
    for oversized in [false, true] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let mut request = native_store_request();
        let mut binding = native_store_binding();
        if oversized {
            binding.root = "r".repeat(1024);
            let mut enriched = request.args["profile_snapshot"].clone();
            enriched["native_store_binding"] = json!(binding);
            enriched["padding"] = json!("");
            let padding =
                super::MAX_FRAME_BYTES - 128 - serde_json::to_vec(&enriched).unwrap().len();
            request.args["profile_snapshot"]["padding"] = json!("p".repeat(padding));
            enriched["padding"] = request.args["profile_snapshot"]["padding"].clone();
            assert!(serde_json::to_vec(&enriched).unwrap().len() <= super::MAX_FRAME_BYTES);
        }
        let admitted = store.submit("owner", request.clone(), now).unwrap();
        let id = admitted.command.target.as_deref().unwrap();
        let expected = store.get_agent(id).unwrap().unwrap();
        if !oversized {
            store.connection.execute_batch("CREATE TRIGGER fail_native_store_dispatch BEFORE INSERT ON events WHEN NEW.kind='command.dispatched' BEGIN SELECT RAISE(ABORT,'synthetic persistence failure'); END;").unwrap();
        }
        let before = store.high_water().unwrap();
        let error = store
            .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &binding)
            .unwrap_err();
        assert_eq!(
            error.code,
            if oversized {
                "frame_too_large"
            } else {
                "internal"
            }
        );
        assert_eq!(store.high_water().unwrap(), before);
        assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
        assert_eq!(
            store.get_command(&request.id).unwrap().unwrap(),
            admitted.command
        );
        assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
        let sources: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM source_state WHERE generation=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sources, 0);
    }
}

#[test]
fn configured_capacity_scopes_are_reserved_atomically_and_retries_are_idempotent() {
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut account_store = Store::open(Path::new(":memory:")).expect("open Store");
    let first = capacity_launch_request(
        "capacity-account-first",
        "profile-a",
        json!([
            {"scope":"profile:profile-a","max_live":null},
            {"scope":"account:shared-account","max_live":1},
            {"scope":"proxy:route-a","max_live":null}
        ]),
    );
    let first_admission = account_store
        .submit("owner", first.clone(), now)
        .expect("reserve all configured scopes at admission");
    let first_agent = first_admission.command.target.as_deref().unwrap();
    assert_eq!(capacity_generation(&account_store, first_agent), Some(1));
    let high_water = account_store.high_water().unwrap();
    assert!(
        !account_store
            .submit("owner", first, now + 1)
            .expect("retained id lookup precedes capacity reservation")
            .is_new
    );
    assert_eq!(account_store.high_water().unwrap(), high_water);
    let account_blocked = capacity_launch_request(
        "capacity-account-second",
        "profile-b",
        json!([
            {"scope":"profile:profile-b","max_live":null},
            {"scope":"account:shared-account","max_live":1},
            {"scope":"proxy:route-b","max_live":null}
        ]),
    );
    let account_blocked_id = account_blocked.id.clone();
    assert_eq!(
        account_store
            .submit("owner", account_blocked, now + 2)
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );
    assert_eq!(account_store.agent_count().unwrap(), 1);
    assert!(
        account_store
            .get_command(&account_blocked_id)
            .unwrap()
            .is_none()
    );

    let mut fake_store = Store::open(Path::new(":memory:")).expect("open Store");
    let mut named_fake = capacity_launch_request(
        "capacity-named-fake",
        "fake-small",
        json!([{"scope":"profile:fake-small","max_live":1}]),
    );
    named_fake.args["harness"] = json!("fake");
    let named_fake_agent = fake_store
        .submit("owner", named_fake, now)
        .expect("Store admits a configured named fake profile")
        .command
        .target
        .expect("named fake Agent");
    let named_fake_agent = fake_store.get_agent(&named_fake_agent).unwrap().unwrap();
    assert_eq!(named_fake_agent.harness, "fake");
    assert_eq!(named_fake_agent.profile, "fake-small");

    let mut proxy_store = Store::open(Path::new(":memory:")).expect("open Store");
    proxy_store
        .submit(
            "owner",
            capacity_launch_request(
                "capacity-proxy-first",
                "profile-c",
                json!([
                    {"scope":"profile:profile-c","max_live":null},
                    {"scope":"account:account-c","max_live":null},
                    {"scope":"proxy:shared-route","max_live":1}
                ]),
            ),
            now,
        )
        .expect("admit first proxy-scoped Agent");
    assert_eq!(
        proxy_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-proxy-second",
                    "profile-d",
                    json!([
                        {"scope":"profile:profile-d","max_live":null},
                        {"scope":"account:account-d","max_live":null},
                        {"scope":"proxy:shared-route","max_live":1}
                    ]),
                ),
                now + 1,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut profile_store = Store::open(Path::new(":memory:")).expect("open Store");
    profile_store
        .submit(
            "owner",
            capacity_launch_request(
                "capacity-profile-first",
                "profile-limited",
                json!([
                    {"scope":"profile:profile-limited","max_live":1},
                    {"scope":"account:account-e","max_live":null},
                    {"scope":"proxy:route-e","max_live":null}
                ]),
            ),
            now,
        )
        .expect("admit first profile-scoped Agent");
    assert_eq!(
        profile_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-profile-second",
                    "profile-limited",
                    json!([
                        {"scope":"profile:profile-limited","max_live":1},
                        {"scope":"account:account-f","max_live":null},
                        {"scope":"proxy:route-f","max_live":null}
                    ]),
                ),
                now + 1,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut disabled_store = Store::open(Path::new(":memory:")).expect("open Store");
    assert_eq!(
        disabled_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-disabled",
                    "profile-disabled",
                    json!([{"scope":"profile:profile-disabled","max_live":0}]),
                ),
                now,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut unlimited_store = Store::open(Path::new(":memory:")).expect("open Store");
    for (project, profile) in [
        ("capacity-unlimited-one", "profile-unlimited-one"),
        ("capacity-unlimited-two", "profile-unlimited-two"),
    ] {
        unlimited_store
            .submit(
                "owner",
                capacity_launch_request(
                    project,
                    profile,
                    json!([
                        {"scope":format!("profile:{profile}"),"max_live":null},
                        {"scope":"account:unlimited","max_live":null},
                        {"scope":"proxy:unlimited","max_live":null}
                    ]),
                ),
                now,
            )
            .expect("None capacity is unlimited");
    }
}

#[test]
fn unknown_start_capacity_survives_recovery_and_store_reopen() {
    let path = capacity_store_path();
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let first = capacity_launch_request(
        "capacity-unknown-first",
        "profile-unknown-first",
        json!([
            {"scope":"profile:profile-unknown-first","max_live":null},
            {"scope":"account:unknown-shared","max_live":1}
        ]),
    );
    let first_id = first.id.clone();
    let first_agent;
    {
        let mut store = Store::open(&path).expect("open persistent Store");
        first_agent = store
            .submit("owner", first, now)
            .expect("admit launch")
            .command
            .target
            .expect("launch Agent");
        store.dispatch(&first_id, now + 1).expect("dispatch launch");
        store
            .settle(
                &first_id,
                CommandState::Unknown,
                Some(json!({"reason":"start_readback_unknown"})),
                now + 2,
            )
            .expect("retain uncertain native start");
        store
            .recover_on_restart(now + 3)
            .expect("restart keeps uncertain start");
        assert_eq!(capacity_generation(&store, &first_agent), Some(1));
    }
    {
        let mut reopened = Store::open(&path).expect("reopen persistent Store");
        assert_eq!(capacity_generation(&reopened, &first_agent), Some(1));
        assert_eq!(
            reopened
                .submit(
                    "owner",
                    capacity_launch_request(
                        "capacity-unknown-second",
                        "profile-unknown-second",
                        json!([
                            {"scope":"profile:profile-unknown-second","max_live":null},
                            {"scope":"account:unknown-shared","max_live":1}
                        ]),
                    ),
                    now + 4,
                )
                .unwrap_err()
                .code,
            "capacity_unavailable"
        );
    }
    remove_capacity_store_files(&path);
}

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
fn exact_undispatched_resume_rejection_releases_only_its_next_generation_slot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "resume-withdrawal", now);
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
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store.submit("owner", resume, now + 2).unwrap();
    assert_eq!(capacity_generation(&store, &agent), Some(2));
    store
        .settle(
            &resume_id,
            CommandState::Rejected,
            Some(json!({"reason":"native_not_dispatched"})),
            now + 3,
        )
        .expect("exact no-session rejection withdraws this resume");
    assert_eq!(capacity_generation(&store, &agent), None);

    let mut next = SubmitCommand::new(Ulid::new().to_string(), "resume");
    next.target = Some(agent.clone());
    next.expected_generation = Some(1);
    store
        .submit("owner", next, now + 4)
        .expect("a withdrawn resume no longer blocks a new exact resume");
    assert_eq!(capacity_generation(&store, &agent), Some(2));
}

#[test]
fn capacity_migration_uses_commands_and_physical_facts_without_rewriting_snapshots() {
    let path = capacity_store_path();
    let pending_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:pending","max_live":1}],"keep":"pending"}"#;
    let rejected_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:rejected","max_live":1}],"keep":"rejected"}"#;
    let unknown_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:unknown","max_live":1}],"keep":"unknown"}"#;
    let ended_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:ended","max_live":1}],"keep":"ended"}"#;
    let closed_only_snapshot = r#"{"capacity_scopes":[{"scope":"profile:closed-only","max_live":1}],"keep":"closed-only"}"#;
    let owned_exit_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:owned-exit","max_live":1}],"keep":"owned-exit"}"#;
    let rejected_dispatched_snapshot = r#"{"capacity_scopes":[{"scope":"profile:rejected-dispatched","max_live":1}],"keep":"rejected-dispatched"}"#;
    let rejected_resume_snapshot = r#"{"capacity_scopes":[{"scope":"profile:rejected-resume","max_live":1}],"keep":"rejected-resume"}"#;
    let pending_resume_snapshot = r#"{"capacity_scopes":[{"scope":"profile:pending-resume","max_live":1}],"keep":"pending-resume"}"#;
    {
        let old = rusqlite::Connection::open(&path).expect("create v8 fixture");
        old.execute_batch(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
             INSERT INTO schema_migrations VALUES (1),(2),(3),(4),(5),(6),(7),(8);
             CREATE TABLE agents(
                 id INTEGER PRIMARY KEY, generation INTEGER NOT NULL,
                 observe_only INTEGER NOT NULL, profile TEXT NOT NULL,
                 profile_snapshot TEXT NOT NULL
             );
             CREATE TABLE commands(
                 id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL, target INTEGER REFERENCES agents(id),
                 expected_generation INTEGER, generation INTEGER, expected_turn_id TEXT,
                 parent_command TEXT REFERENCES commands(id), link_kind TEXT, ordinal INTEGER,
                 turn_id TEXT, request_id TEXT, verb TEXT NOT NULL, args TEXT NOT NULL,
                 state TEXT NOT NULL, outcome TEXT, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL)
                    OR (parent_command IS NOT NULL AND link_kind IS NOT NULL
                        AND ordinal IS NOT NULL AND ordinal >= 0))
             );
             CREATE INDEX commands_target ON commands(target, created_at);
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             CREATE UNIQUE INDEX commands_children
                 ON commands(parent_command, ordinal)
                 WHERE parent_command IS NOT NULL;
             CREATE INDEX commands_request_open
                 ON commands(target, generation, request_id, state)
                 WHERE request_id IS NOT NULL;
             CREATE TABLE events(
                 agent INTEGER, generation INTEGER, family TEXT,
                 kind TEXT, source TEXT, data TEXT
             );
             CREATE TABLE source_state(
                 agent INTEGER, generation INTEGER, source TEXT, state TEXT
             );
             CREATE TABLE processes(owner TEXT, role TEXT, state TEXT);
             CREATE TABLE workspaces(start_command_id TEXT, target_generation INTEGER);",
        )
        .expect("create v8 tables");
        for (id, generation, profile, snapshot) in [
            (1_i64, 0_i64, "pending", pending_snapshot),
            (2, 0, "rejected", rejected_snapshot),
            (3, 1, "unknown", unknown_snapshot),
            (4, 1, "ended", ended_snapshot),
            (5, 1, "closed-only", closed_only_snapshot),
            (6, 1, "owned-exit", owned_exit_snapshot),
            (7, 0, "unadmitted", "{}"),
            (8, 1, "rejected-dispatched", rejected_dispatched_snapshot),
            (9, 1, "rejected-resume", rejected_resume_snapshot),
            (10, 1, "pending-resume", pending_resume_snapshot),
        ] {
            old.execute(
                "INSERT INTO agents(id, generation, observe_only, profile, profile_snapshot)
                 VALUES (?1, ?2, 0, ?3, ?4)",
                rusqlite::params![id, generation, profile, snapshot],
            )
            .expect("insert v8 Agent");
        }
        old.execute(
            "INSERT INTO commands(
                 id, principal, method, payload_digest, target, generation,
                 verb, args, state, created_at, updated_at
             ) VALUES
                 ('pending-launch','owner','command.submit',zeroblob(32),1,0,'launch','{}','accepted',1,1),
                 ('rejected-launch','owner','command.submit',zeroblob(32),2,0,'launch','{}','rejected',2,2),
                 ('unknown-launch','owner','command.submit',zeroblob(32),3,1,'launch','{}','unknown',3,3),
                 ('rejected-dispatched-start','owner','command.submit',zeroblob(32),8,1,'launch','{}','rejected',4,4),
                 ('rejected-resume','owner','command.submit',zeroblob(32),9,1,'resume','{}','rejected',5,5),
                 ('pending-resume','owner','command.submit',zeroblob(32),10,1,'resume','{}','accepted',6,6)",
            [],
        )
        .expect("insert retained start commands");
        old.execute(
            "INSERT INTO events(agent, generation, family, kind, source, data)
             VALUES (4,1,'session','session.ended','stream','{}'),
                    (10,1,'session','session.ended','stream','{}'),
                    (8,1,'agent','agent.starting','daemon','{\"command_id\":\"rejected-dispatched-start\"}')",
            [],
        )
        .expect("insert terminal native fact");
        old.execute(
            "INSERT INTO source_state(agent, generation, source, state)
             VALUES (5,1,'stream','closed'),
                    (6,1,'stream','closed'),
                    (6,1,'os','closed')",
            [],
        )
        .expect("insert source facts");
        old.execute(
            "INSERT INTO processes(owner, role, state)
             VALUES ('agent:a6#1','plugin','exited')",
            [],
        )
        .expect("insert owned process exit fact");
    }

    let store = Store::open(&path).expect("migrate v8 Store");
    assert_eq!(capacity_generation(&store, "a1"), Some(1));
    assert_eq!(capacity_generation(&store, "a2"), None);
    assert_eq!(capacity_generation(&store, "a3"), Some(1));
    assert_eq!(capacity_generation(&store, "a4"), None);
    assert_eq!(
        capacity_generation(&store, "a5"),
        Some(1),
        "closed reader source alone is not process-exit proof"
    );
    assert_eq!(capacity_generation(&store, "a6"), None);
    assert_eq!(capacity_generation(&store, "a7"), None);
    assert_eq!(capacity_generation(&store, "a8"), None);
    assert_eq!(
        capacity_generation(&store, "a9"),
        Some(1),
        "pre-dispatch resume rejection does not prove the prior generation ended"
    );
    assert_eq!(
        capacity_generation(&store, "a10"),
        Some(2),
        "retained resume admission reserves its next generation after terminal proof"
    );
    let retained_snapshot: String = store
        .connection
        .query_row(
            "SELECT profile_snapshot FROM agents WHERE id = 5",
            [],
            |row| row.get(0),
        )
        .expect("read legacy snapshot");
    assert_eq!(retained_snapshot, closed_only_snapshot);
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn pre_dispatch_initial_launch_rejection_is_terminal_cancels_linked_input_and_frees_drain_slot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut request = capacity_launch_request(
        "readiness-failure",
        "readiness-profile",
        json!([{"scope":"profile:readiness-profile","max_live":1}]),
    );
    request.args["text"] = json!("launch input never reaches the native process");
    let start_id = request.id.clone();
    let admission = store
        .submit("owner", request, now)
        .expect("admit initial launch");
    let agent = admission.command.target.clone().expect("launch target");
    let linked_id = admission
        .linked_command
        .as_ref()
        .expect("linked launch input")
        .id
        .clone();
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    let settled = store
        .settle(
            &start_id,
            CommandState::Rejected,
            Some(json!({"reason":"readiness_failed"})),
            now + 1,
        )
        .expect("record proven pre-dispatch readiness rejection");
    let failed = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(failed.generation, 0);
    assert_eq!(failed.state, AgentState::Failed);
    assert!(
        failed.is_terminal(),
        "the never-started Agent no longer blocks drain"
    );
    assert_eq!(store.get_agent_token(&agent, 0).unwrap(), None);
    assert_eq!(capacity_generation(&store, &agent), None);
    assert_eq!(
        store.get_command(&linked_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert!(settled.events.iter().any(|event| {
        event.kind == "agent.failed" && event.data["reason"] == "launch_not_dispatched"
    }));
    assert!(settled.events.iter().any(|event| {
        event.kind == "command.cancelled" && event.data["command_id"] == linked_id
    }));

    store
        .submit(
            "owner",
            capacity_launch_request(
                "readiness-retry-new-id",
                "readiness-profile",
                json!([{"scope":"profile:readiness-profile","max_live":1}]),
            ),
            now + 2,
        )
        .expect("failed generation-0 launch no longer blocks drain or capacity");
}

#[test]
fn stop_and_source_closure_alone_do_not_release_capacity_or_fail_host_settlement() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "capacity-stop-proof", now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install OS process source");

    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 2,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 2).unwrap();
    store
        .settle(&stop_id, CommandState::Applied, None, now + 3)
        .expect("stop acknowledgement does not prove process exit");
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    store
        .set_process_source_status(&agent, 1, "ok", true, now + 4)
        .expect("persist source closure without an observed process exit");
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 5)
            .expect("insufficient evidence must not block Lost/source settlement")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    assert_eq!(
        store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-stop-blocked",
                    "resume-profile",
                    json!([{"scope":"profile:resume-profile","max_live":1}]),
                ),
                now + 6,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let numeric_agent = eliot_core::parse_agent_id(&agent).unwrap();
    let process_owner = format!("agent:{agent}#1");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-stop-boot', 81002, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![process_owner, now + 7],
        )
        .expect("persist exact exited process record");
    store
        .close_native_sources(&agent, 1, now + 8)
        .expect("persist native reader drain independently");
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let events = store
        .record_owned_control_exit(&agent, 1, now + 9)
        .expect("matching OS, process, and reader facts release capacity");
    assert!(events.iter().any(|event| event.kind == "capacity.released"));
    assert_eq!(capacity_generation(&store, &agent), None);
    assert!(numeric_agent > 0);
}

#[test]
fn event_subject_is_persisted_at_write_time_without_changing_native_data_digest() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let project = "event-subject-snapshot";
    let launch_id = Ulid::new().to_string();
    let mut request = SubmitCommand::new(launch_id.clone(), "launch");
    request.args = json!({"project":project, "workspace":"synthetic-workspace"});
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
    assert_eq!(created_subject.harness, "fake");
    assert_eq!(created_subject.state, AgentState::Created);

    let dispatched = store.dispatch(&launch_id, now + 1).unwrap();
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
        super::canonical::digest(&native.semantic_value()).unwrap()
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
    let mut admission =
        super::submit_tx(&tx, "owner", request.clone(), None, now + 1, false).unwrap();
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
            depth: 0,
        })
    );

    let cause = EventCause {
        command_id: request.id.clone(),
        rule: Some("stop-on-result".to_owned()),
        depth: 1,
    };
    super::annotate_new_command_cause_tx(
        &tx,
        &request.id,
        &mut admission.events[event_index],
        cause.clone(),
    )
    .unwrap();
    super::commit_transaction(tx, &store.write_faulted).unwrap();

    assert_eq!(admission.events[event_index].cause, Some(cause.clone()));
    assert_eq!(
        super::command_cause_tx(&store.connection, &request.id).unwrap(),
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
        super::command_cause_tx(&store.connection, &request.id).unwrap(),
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
        super::canonical::digest(&native.semantic_value()).unwrap()
    );
    let reread = store
        .read_events(started.seq - 1, 1, Some(started.seq))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reread.cause, started.cause);
}
