use super::{
    BatchCommit, ProjectRegistrationInput, RuleCatchUpAction, RuleCatchUpAdmission,
    RuleCursorOutcome, RuleDefinitionInput, RuleEventAdmission, Store, contains_secret_marker,
};
use eliot_core::api::{
    ProviderClearHoldRequest, ProviderCorrectiveEvidence, RuleCatchUpRequest, ScriptInterpreter,
};
use eliot_core::{
    AgentState, CommandState, EventCause, NativeEvent, NativeStoreBinding, ProviderHoldState,
    ProviderRestrictionKind, SourceBatch, SourceGap, SubmitCommand, TurnState,
};
use eliot_core::{
    WorkspaceCancelOutcome, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparationInput,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
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

#[test]
fn initial_source_families_match_fake_and_codex_harness_boundaries() {
    assert_eq!(
        super::source_families_for_harness("fake").unwrap(),
        &["session", "turn", "message", "request"][..]
    );
    assert_eq!(
        super::source_families_for_harness("codex").unwrap(),
        &["session", "turn", "message", "tool", "request"][..]
    );
    assert_eq!(
        super::source_families_for_harness("other")
            .unwrap_err()
            .code,
        "unsupported"
    );
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

fn rule_input_with_limit(id: &str, max: u64, workspace: &str) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "on": {"event":"command.accepted"},
        "do": [{"action":"launch","profile":"fake","workspace":workspace}],
        "limit": {"max":max,"per":"rule","window_ms":60_000}
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).expect("encode limited rule"),
        definition_digest: eliot_core::canonical_json_sha256(&value).expect("digest limited rule"),
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

fn targetless_rule_launch_request(
    project: &str,
    workspace: &str,
    text: Option<&str>,
) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.args = json!({
        "project": project,
        "harness": "fake",
        "profile": "fake",
        "profile_snapshot": {},
        "workspace": workspace,
    });
    if let Some(text) = text {
        request.args["text"] = json!(text);
    }
    request.expected_generation = Some(0);
    request
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

fn recovered_admission_page_bytes(page: &[super::Admission]) -> usize {
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
    page: &[super::Admission],
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
    assert!(recovered_admission_page_bytes(page) <= super::MAX_PAGE_BYTES);
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
        json!(["message", "request", "session", "turn"])
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
    assert!(launch_count * 2 * launch_text.len() > super::MAX_PAGE_BYTES);
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
        serde_json::to_vec(&request).unwrap().len() <= super::MAX_FRAME_BYTES,
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
    assert!(launch_bytes <= super::MAX_FRAME_BYTES);
    assert!(send_bytes <= super::MAX_FRAME_BYTES);
    assert!(
        recovered_admission_page_bytes(&page) > super::MAX_PAGE_BYTES,
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
    let command = super::CommandInsert {
        id: &request.id,
        principal: &principal,
        method: "command.submit",
        payload_digest: &payload_digest,
        target: None,
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
        super::projects::capture_project_command_binding_tx(&tx, &command)
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
        super::projects::capture_project_command_binding_tx(&tx, &command)
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
        super::projects::capture_project_command_binding_tx(&tx, &command)
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

    let store = Store::open(&path).expect("apply registration migration 15");
    let version: i64 = store
        .connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("read Store migration version");
    assert_eq!(version, 15);
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

#[test]
fn owner_event_launch_links_one_send_counts_once_and_replays_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "owner-launch-linked-input";
    let workspace = "synthetic-owner-rule-launch";
    let state = store
        .ensure_rule_state("owner", &rule_input_with_limit(rule_id, 2, workspace), now)
        .expect("ensure per-rule launch limit");
    let trigger_agent = ready_agent(&mut store, "launch-trigger", now + 1);
    let trigger_agent_num = eliot_core::parse_agent_id(&trigger_agent).expect("trigger Agent ID");
    let trigger_event_seq: i64 = store
        .connection
        .query_row(
            "SELECT seq FROM events
             WHERE agent = ?1 AND kind = 'command.accepted'
               AND json_extract(data, '$.verb') = 'launch'
             ORDER BY seq DESC LIMIT 1",
            [trigger_agent_num],
            |row| row.get(0),
        )
        .expect("read trigger launch event");

    let request =
        targetless_rule_launch_request(workspace, workspace, Some("initial launch input"));
    assert!(request.target.is_none());
    assert_eq!(request.expected_generation, Some(0));
    let first = store
        .admit_rule_action("owner", &state, trigger_event_seq, request.clone(), now + 2)
        .expect("admit targetless owner launch");
    assert!(first.is_new);
    let parent = first.parent.as_ref().expect("rule firing parent");
    assert_eq!(parent.principal, "owner");
    assert_eq!(parent.state, CommandState::Applied);
    let action = first.child.as_ref().expect("launch action admission");
    assert!(
        action.events.is_empty(),
        "events belong to the outer admission"
    );
    let launch = &action.command;
    let linked = action
        .linked_command
        .as_ref()
        .expect("nonblank launch text creates one linked SEND");
    let launch_id = launch.id.clone();
    let linked_id = linked.id.clone();
    let launch_target = launch.target.clone().expect("Store allocated new Agent");
    assert_eq!(launch.principal, "owner");
    assert_ne!(launch_target, trigger_agent);
    assert_eq!(launch.verb, "launch");
    assert_eq!(launch.expected_generation, Some(0));
    assert_eq!(launch.generation, Some(0));
    assert_eq!(launch.args["project"], workspace);
    assert_eq!(linked.id, format!("{launch_id}/send"));
    assert_eq!(linked.principal, "owner");
    assert_eq!(linked.target.as_deref(), Some(launch_target.as_str()));
    assert_eq!(linked.verb, "send");
    assert_eq!(linked.expected_generation, Some(1));
    assert_eq!(linked.generation, Some(1));
    assert_eq!(linked.args, json!({"text":"initial launch input"}));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(
        parent.outcome.as_ref().unwrap()["children"],
        json!([launch_id])
    );

    let (launch_parent, launch_link, launch_ordinal): (
        Option<String>,
        Option<String>,
        Option<i64>,
    ) = store
        .connection
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [&launch_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read rule launch link");
    assert_eq!(launch_parent.as_deref(), Some(parent.id.as_str()));
    assert_eq!(launch_link.as_deref(), Some("rule_action"));
    assert_eq!(launch_ordinal, Some(0));
    let (send_parent, send_link, send_ordinal): (Option<String>, Option<String>, Option<i64>) =
        store
            .connection
            .query_row(
                "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
                [&linked_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read linked initial SEND link");
    assert_eq!(send_parent.as_deref(), Some(launch_id.as_str()));
    assert_eq!(send_link.as_deref(), Some("send"));
    assert_eq!(send_ordinal, Some(0));

    let command_cause = |command_id: &str| {
        first
            .events
            .iter()
            .find(|event| {
                event
                    .data
                    .get("command_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(command_id)
            })
            .and_then(|event| event.cause.as_ref())
            .expect("command event has its own causal identity")
            .clone()
    };
    for command_id in [&parent.id, &launch_id, &linked_id] {
        let cause = command_cause(command_id);
        assert_eq!(cause.command_id, *command_id);
        assert_eq!(cause.rule.as_deref(), Some(rule_id));
        assert_eq!(cause.depth, 1);
    }

    let configured_action_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [&parent.id],
            |row| row.get(0),
        )
        .expect("count configured launch action children");
    let global_action_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("count global configured action children");
    assert_eq!(configured_action_children, 1);
    assert_eq!(global_action_children, 1);

    let queued_recovery = store
        .recover_rule_children(None, 100)
        .expect("recover accepted launch action");
    assert_eq!(
        queued_recovery
            .iter()
            .find(|command| command.command.id == launch_id)
            .expect("launch is recoverable while accepted")
            .command
            .state,
        CommandState::Accepted
    );
    assert!(
        !queued_recovery
            .iter()
            .any(|command| command.command.id == linked_id),
        "the linked SEND is carried by its launch admission, not its own recovery cursor"
    );

    let trigger_send = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "next rule event"),
            now + 3,
        )
        .expect("create second trigger event");
    let second_event = trigger_send
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
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(trigger_agent.clone());
    stop.expected_generation = Some(1);
    let second_action = store
        .admit_rule_action("owner", &first.state, second_event.seq, stop, now + 4)
        .expect("per-rule limit counts the launch once, not its linked SEND");
    assert!(second_action.is_new);
    assert_eq!(second_action.child.as_ref().unwrap().command.verb, "stop");
    store
        .settle(
            &second_action.child.as_ref().unwrap().command.id,
            CommandState::Rejected,
            Some(json!({"reason": "close stop counter fixture"})),
            now + 5,
        )
        .expect("close second configured action");

    let commands_before_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .expect("count commands before retained replay");
    let events_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action(
            "owner",
            &second_action.state,
            trigger_event_seq,
            request.clone(),
            now + 6,
        )
        .expect("replay retained launch before current gates");
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    let replay_action = replay.child.as_ref().expect("retained launch admission");
    assert!(replay_action.events.is_empty());
    assert_eq!(replay_action.command.id, launch_id);
    assert_eq!(replay_action.linked_command.as_ref().unwrap().id, linked_id);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_replay);

    store
        .dispatch(&launch_id, now + 7)
        .expect("dispatch admitted launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: launch_target.clone(),
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
            now + 8,
        )
        .expect("commit launch readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 9)
        .expect("settle launch readiness");
    let applied_recovery = store
        .recover_rule_children(None, 100)
        .expect("recover applied launch with queued linked SEND");
    assert_eq!(
        applied_recovery
            .iter()
            .find(|command| command.command.id == launch_id)
            .expect("applied launch remains recoverable while linked SEND is queued")
            .command
            .state,
        CommandState::Applied
    );
    store
        .dispatch(&linked_id, now + 10)
        .expect("dispatch linked SEND");
    store
        .settle(
            &linked_id,
            CommandState::Unknown,
            Some(json!({"reason": "fixture uncertain initial input"})),
            now + 11,
        )
        .expect("retain uncertain linked SEND");

    let commands_before_unknown_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_unknown_replay = store.high_water().unwrap();
    let unknown_replay = store
        .admit_rule_action(
            "owner",
            &second_action.state,
            trigger_event_seq,
            request,
            now + 12,
        )
        .expect("replay launch with retained Unknown linked SEND");
    assert!(!unknown_replay.is_new);
    assert!(unknown_replay.events.is_empty());
    let retained = unknown_replay.child.as_ref().unwrap();
    assert_eq!(retained.command.state, CommandState::Applied);
    assert_eq!(
        retained.linked_command.as_ref().unwrap().state,
        CommandState::Unknown
    );
    let recovery_after_unknown = store.recover_rule_children(None, 100).unwrap();
    assert!(
        !recovery_after_unknown
            .iter()
            .any(|command| command.command.id == launch_id || command.command.id == linked_id)
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_unknown_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_unknown_replay);
}
#[test]
fn targetless_launch_does_not_borrow_trigger_agent_for_agent_limit() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "targetless-agent-limit";
    let value = json!({
        "id": rule_id,
        "enabled": true,
        "limit": {"max":1,"per":"agent","window_ms":60_000}
    });
    let definition = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: eliot_core::canonical_json_sha256(&value).unwrap(),
        enabled: true,
    };
    let state = store
        .ensure_rule_state("owner", &definition, now)
        .expect("ensure per-agent rule limit");
    let trigger_agent = ready_agent(&mut store, "targetless-agent-trigger", now + 1);

    let first_trigger = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "seed trigger-agent bucket"),
            now + 2,
        )
        .expect("admit first trigger event");
    let first_event = first_trigger
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(first_trigger.command.id.as_str())
        })
        .expect("first trigger command event");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(trigger_agent.clone());
    stop.expected_generation = Some(1);
    let prior = store
        .admit_rule_action("owner", &state, first_event.seq, stop, now + 3)
        .expect("admit trigger-agent stop");
    let prior_child = prior.child.as_ref().unwrap().command.id.clone();
    store
        .settle(
            &prior_child,
            CommandState::Rejected,
            Some(json!({"reason": "seed target bucket"})),
            now + 4,
        )
        .expect("close trigger-agent bucket action");

    let second_trigger = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "targetless launch event"),
            now + 5,
        )
        .expect("admit second trigger event");
    let second_event = second_trigger
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(second_trigger.command.id.as_str())
        })
        .expect("second trigger command event");
    let request = targetless_rule_launch_request(
        "synthetic-agent-limit-launch",
        "synthetic-agent-limit-launch",
        Some("linked input"),
    );
    assert!(request.target.is_none());
    let launch = store
        .admit_rule_action("owner", &prior.state, second_event.seq, request, now + 6)
        .expect("targetless launch has no Agent bucket to borrow");
    assert!(launch.is_new);
    let launch_action = launch.child.as_ref().expect("admitted targetless launch");
    let launch_command = &launch_action.command;
    let trigger_agent_num = eliot_core::parse_agent_id(&trigger_agent).unwrap();
    assert_ne!(
        launch_command.target.as_deref(),
        Some(trigger_agent.as_str())
    );
    let trigger_bucket_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL AND target = ?1",
            [trigger_agent_num],
            |row| row.get(0),
        )
        .expect("read existing trigger-agent rule bucket");
    assert_eq!(trigger_bucket_count, 1);
    assert_eq!(
        launch_action
            .linked_command
            .as_ref()
            .expect("targetless launch linked SEND")
            .generation,
        Some(1)
    );
}
#[test]
fn event_rule_replies_bind_open_request_turn_and_replay_without_competing_reply() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 100;
    let (agent_id, _) =
        ready_managed_codex_agent(&mut store, "/synthetic/event-replies", "codex-fixture", now);
    let send = send_request(&agent_id, 1, "open request replies");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 5).unwrap();
    store.dispatch(&send_id, now + 6).unwrap();
    let turn_id = format!("request-turn-{send_id}");
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
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":send_id, "turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit exact request turn");

    let rule_id = "event-request-replies";
    let rule_value = json!({"id":rule_id,"enabled":true});
    let rule_state = store
        .ensure_rule_state(
            "owner",
            &RuleDefinitionInput {
                id: rule_id.to_owned(),
                canonical_definition: serde_json::to_string(&rule_value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
                enabled: true,
            },
            now + 8,
        )
        .unwrap();
    let open_request = |native_id: &str, request_id: &str, kind: &str, details| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id":send_id,
            "request_id":request_id,
            "turn_id":turn_id,
            "kind":kind,
            "details":details,
        }),
    };
    let opened = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    open_request("reply-forged", "request-forged", "approval", json!({})),
                    open_request("reply-approve", "request-approve", "approval", json!({})),
                    open_request("reply-deny", "request-deny", "approval", json!({})),
                    open_request(
                        "reply-answer",
                        "request-answer",
                        "question",
                        json!({"questions":[{"id":"q1","is_secret":false}]}),
                    ),
                    open_request("reply-closed", "request-closed", "approval", json!({})),
                ],
                cursor: None,
            },
            now + 9,
        )
        .expect("commit pending request events");
    let event_seq = |request_id: &str| {
        opened
            .events
            .iter()
            .find(|event| event.kind == "request.opened" && event.data["request_id"] == request_id)
            .expect("request.opened event is retained")
            .seq
    };
    let reply = |verb: &str, request_id: &str, text: Option<&str>| {
        let mut request = SubmitCommand::new(String::new(), verb);
        request.target = Some(agent_id.clone());
        request.args = text.map_or_else(|| json!({}), |text| json!({"text":text}));
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        request.request_id = Some(request_id.to_owned());
        request
    };

    let forged = store
        .admit_rule_action(
            "owner",
            &rule_state,
            event_seq("request-forged"),
            reply("approve", "request-approve", None),
            now + 10,
        )
        .unwrap();
    assert!(forged.child.is_none());
    assert_eq!(
        forged.parent.unwrap().outcome.unwrap()["code"],
        "rule_action_binding_invalid"
    );

    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.closed".to_owned(),
                    native_id: "reply-closed-native".to_owned(),
                    data: json!({"request_id":"request-closed","turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 11,
        )
        .expect("close the captured request");
    let closed = store
        .admit_rule_action(
            "owner",
            &forged.state,
            event_seq("request-closed"),
            reply("approve", "request-closed", None),
            now + 12,
        )
        .unwrap_err();
    assert_eq!(closed.code, "source_integrity");

    let approved = store
        .admit_rule_action(
            "owner",
            &forged.state,
            event_seq("request-approve"),
            reply("approve", "request-approve", None),
            now + 13,
        )
        .unwrap();
    let approved_parent_id = approved.parent.as_ref().unwrap().id.clone();
    let approved_child = approved.child.as_ref().unwrap().command.clone();
    assert_eq!(approved_child.verb, "approve");
    assert_eq!(
        approved_child.request_id.as_deref(),
        Some("request-approve")
    );
    assert_eq!(approved_child.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(approved_child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(approved_child.generation, Some(1));

    store.dispatch(&approved_child.id, now + 14).unwrap();
    store
        .settle(
            &approved_child.id,
            CommandState::Unknown,
            Some(json!({"reason":"reply result unknown"})),
            now + 15,
        )
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action(
            "owner",
            &approved.state,
            event_seq("request-approve"),
            reply("approve", "request-approve", None),
            now + 16,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.parent.as_ref().unwrap().id, approved_parent_id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, approved_child.id);
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);

    let mut competing = reply("approve", "request-approve", None);
    competing.id = Ulid::new().to_string();
    let competing_id = competing.id.clone();
    assert_eq!(
        store.submit("owner", competing, now + 17).unwrap_err().code,
        "bad_state"
    );
    assert!(store.get_command(&competing_id).unwrap().is_none());

    let denied = store
        .admit_rule_action(
            "owner",
            &replay.state,
            event_seq("request-deny"),
            reply("deny", "request-deny", None),
            now + 18,
        )
        .unwrap();
    assert_eq!(denied.child.as_ref().unwrap().command.verb, "deny");
    assert_eq!(
        denied.child.as_ref().unwrap().command.request_id.as_deref(),
        Some("request-deny")
    );

    let answered = store
        .admit_rule_action(
            "owner",
            &denied.state,
            event_seq("request-answer"),
            reply("answer", "request-answer", Some("ordinary answer")),
            now + 19,
        )
        .unwrap();
    assert_eq!(answered.child.as_ref().unwrap().command.verb, "answer");
    assert_eq!(
        answered
            .child
            .as_ref()
            .unwrap()
            .command
            .request_id
            .as_deref(),
        Some("request-answer")
    );
    assert_eq!(
        answered.child.as_ref().unwrap().command.args,
        json!({"text":"ordinary answer"})
    );
}

#[test]
fn project_event_reply_requires_the_explicit_per_verb_grant() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 100;
    let project = "project-event-reply-grant";
    let principal = format!("project:{project}");
    let agent = ready_agent(&mut store, project, now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);
    let send_id = turn_id
        .strip_prefix("fake-turn-")
        .expect("Fake turn id retains its source send id");
    let checkout = "synthetic-workspace";
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: format!("{checkout}/.git"),
        profiles: vec!["fake".to_owned()],
        actions: vec!["approve".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 2)
        .unwrap();
    let rule_id = "project-reply-rule";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event":"request.opened"},
        "do": [{"action":"approve"}]
    });
    store
        .ensure_rule_state(
            &principal,
            &RuleDefinitionInput {
                id: rule_id.to_owned(),
                canonical_definition: serde_json::to_string(&rule_value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
                enabled: true,
            },
            now + 3,
        )
        .unwrap();
    let open_request = |native_id: &str, request_id: &str| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id": send_id,
            "request_id": request_id,
            "turn_id": turn_id,
            "kind": "approval",
            "details": {}
        }),
    };
    let first_open = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![open_request("project-approval-1", "project-request-1")],
                cursor: None,
            },
            now + 5,
        )
        .unwrap()
        .events
        .into_iter()
        .find(|event| event.kind == "request.opened")
        .unwrap();
    registration.actions = vec!["stop".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 6)
        .unwrap();

    let make_approval = |request_id: &str| {
        let mut request = SubmitCommand::new(String::new(), "approve");
        request.target = Some(agent.clone());
        request.args = json!({});
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        request.request_id = Some(request_id.to_owned());
        request
    };
    let denied = store
        .admit_rule_action(
            &principal,
            &store.get_rule_state(&principal, rule_id).unwrap().unwrap(),
            first_open.seq,
            make_approval("project-request-1"),
            now + 7,
        )
        .unwrap();
    assert!(denied.child.is_none());
    assert_eq!(
        denied.parent.unwrap().outcome.unwrap()["code"],
        "not_allowed"
    );

    registration.actions = vec!["stop".to_owned(), "approve".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 8)
        .unwrap();
    let second_open = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![open_request("project-approval-2", "project-request-2")],
                cursor: None,
            },
            now + 9,
        )
        .unwrap()
        .events
        .into_iter()
        .find(|event| event.kind == "request.opened")
        .unwrap();
    let current = store.get_rule_state(&principal, rule_id).unwrap().unwrap();
    let approved = store
        .admit_rule_action(
            &principal,
            &current,
            second_open.seq,
            make_approval("project-request-2"),
            now + 10,
        )
        .unwrap();
    let child = approved
        .child
        .expect("approved project reply child")
        .command;
    assert_eq!(child.verb, "approve");
    assert_eq!(child.principal, principal);
    assert_eq!(child.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(child.request_id.as_deref(), Some("project-request-2"));
    assert_eq!(
        store.dispatch(&child.id, now + 11).unwrap().value.state,
        CommandState::Dispatched
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
    let linked_cause = super::command_cause_tx(&store.connection, &linked_id)
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
