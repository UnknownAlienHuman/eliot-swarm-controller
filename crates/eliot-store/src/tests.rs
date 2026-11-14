use super::{
    BatchCommit, ProjectRegistrationInput, RuleCatchUpAction, RuleCatchUpAdmission,
    RuleCursorOutcome, RuleDefinitionInput, RuleEventAdmission, Store, contains_secret_marker,
};
use eliot_core::api::{
    ProviderClearHoldRequest, ProviderCorrectiveEvidence, RuleCatchUpRequest, ScriptInterpreter,
    TurnResolveRequest, TurnResolveStatus,
};
use eliot_core::{
    AgentControl, AgentState, CommandState, EventCause, NativeEvent, NativeStoreBinding,
    ProcessIdentity, ProcessRecord, ProcessState, ProviderHoldState, ProviderRestrictionKind,
    SourceBatch, SourceGap, SubmitCommand, TurnOutcome, TurnState,
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
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":{}}),
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
    install_fixture_sources(store, &agent, &launch_id, 1, now);
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
                selector: None,
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
    install_fixture_sources(store, &agent, &start_id, 1, now + 2);
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
        "harness": "codex",
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
    install_fixture_sources(store, &agent, &launch_id, 1, now);
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

/// Simulated owned-plugin identity for capacity fixtures: clearly synthetic,
/// unique per (agent, generation) within a Store, and stable across the
/// running-to-exited lifecycle of one generation so observations upsert
/// instead of conflicting.
fn simulated_plugin_identity(agent: &str, generation: i64) -> ProcessIdentity {
    ProcessIdentity {
        host_boot_id: format!("simulated-os-{agent}"),
        pid: 81001,
        start_time: u64::try_from(generation).expect("positive fixture generation"),
    }
}

/// Install the fixture native source plan after dispatch, before any native
/// batch. Fixture counterpart of the PluginHost manifest validation
/// (`install_plugin_sources` after manifest check, before `agent.start`);
/// the ownership gate refuses every native family until this plan commits.
/// Uses the common five-family stream plan so every native kind used in
/// fixtures has an owner.
pub(crate) fn install_fixture_sources(
    store: &mut Store,
    agent: &str,
    start_command_id: &str,
    generation: i64,
    now: i64,
) {
    let plan: BTreeMap<String, String> = ["session", "turn", "message", "tool", "request"]
        .into_iter()
        .map(|family| (family.to_owned(), "stream".to_owned()))
        .collect();
    store
        .install_plugin_sources(agent, generation, start_command_id, &plan, now)
        .expect("install fixture source plan");
}

/// Install OS process ownership and the running owned-plugin observation
/// while the generation is live. Fixture counterpart of daemon spawn plus
/// monitor observation; must precede the terminal session fact because the
/// live guard rejects installs on terminal generations.
pub(crate) fn install_live_custody_observations(
    store: &mut Store,
    agent: &str,
    generation: i64,
    now: i64,
) {
    store
        .ensure_process_source(agent, generation, now)
        .expect("install OS process source while live");
    store
        .observe_processes(
            agent,
            generation,
            &[ProcessRecord {
                identity: simulated_plugin_identity(agent, generation),
                owner: format!("agent:{agent}#{generation}"),
                role: "plugin".to_owned(),
                state: ProcessState::Running,
                exit_code: None,
                updated_at: now,
            }],
            now,
        )
        .expect("observe running owned plugin");
}

/// Complete the owned-exit proof after the terminal fact, in host order:
/// the owned plugin's exit observation, native source drain/closure, OS
/// source closure, then the owned-control-exit record that releases the
/// reservation through the existing gate. Idempotent: already-closed
/// sources are kept closed and recorded exits are kept, never reopened.
pub(crate) fn simulate_normal_stop_proof(
    store: &mut Store,
    agent: &str,
    generation: i64,
    now: i64,
) {
    store
        .observe_processes(
            agent,
            generation,
            &[ProcessRecord {
                identity: simulated_plugin_identity(agent, generation),
                owner: format!("agent:{agent}#{generation}"),
                role: "plugin".to_owned(),
                state: ProcessState::Exited,
                exit_code: Some(0),
                updated_at: now,
            }],
            now,
        )
        .expect("observe owned plugin exit");
    store
        .close_native_sources(agent, generation, now)
        .expect("close drained native sources");
    store
        .set_process_source_status(agent, generation, "ok", true, now)
        .expect("close OS process source after exit");
    store
        .record_owned_control_exit(agent, generation, now)
        .expect("record owned control exit proof");
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
    let command = super::CommandInsert {
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
    assert_eq!(version, super::schema::CURRENT_SCHEMA_VERSION);
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
    install_fixture_sources(&mut store, &launch_target, &launch_id, 1, now + 7);
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
        // Rule-driven answers carry the structured #327 answer form.
        request.args = match (verb, text) {
            ("answer", Some(text)) => json!({"answers": {"q1": {"answers": [text]}}}),
            (_, text) => text.map_or_else(|| json!({}), |text| json!({"text":text})),
        };
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
        json!({"answers": {"q1": {"answers": ["ordinary answer"]}}})
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
        git: None,
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
        selector: None,
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
    install_fixture_sources(&mut store, &agent_id, &launch_id, 1, now);
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
        selector: None,
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
    install_fixture_sources(store, &agent, &launch_id, 1, now);
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
        launch.args = json!({"project":"provider-held-launch", "workspace":"synthetic-held-launch", "harness":"codex", "profile":"fake", "profile_snapshot":{}});
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
    install_fixture_sources(&mut store, id, &request.id, 1, now + 1);
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
        if kind == "session.ready" {
            install_live_custody_observations(&mut store, id, 1, now + 3);
        }
    }
    simulate_normal_stop_proof(&mut store, id, 1, now + 3);
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

    let mut named_store = Store::open(Path::new(":memory:")).expect("open Store");
    let named_small = capacity_launch_request(
        "capacity-named-small",
        "fixture-small",
        json!([{"scope":"profile:fixture-small","max_live":1}]),
    );
    let named_small_agent = named_store
        .submit("owner", named_small, now)
        .expect("Store admits a configured named small profile")
        .command
        .target
        .expect("named small Agent");
    let named_small_agent = named_store.get_agent(&named_small_agent).unwrap().unwrap();
    assert_eq!(named_small_agent.harness, "codex");
    assert_eq!(named_small_agent.profile, "fixture-small");

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
fn restart_readback_skips_exited_children_and_bounds_live_records() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch = capacity_launch_request(
        "restart-heavy",
        "restart-heavy",
        json!([{"scope": "profile:restart-heavy", "max_live": null}]),
    );
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch Agent");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    store
        .settle(
            &launch_id,
            CommandState::Unknown,
            Some(json!({"reason":"start_readback_unknown"})),
            now + 2,
        )
        .expect("retain uncertain native start");
    store.recover_on_restart(now + 3).expect("recover");
    let owner = format!("agent:{agent}#1");
    let insert_on = |store: &Store, boot: &str, pid: i64, role: &str, state: &str| {
        store
            .connection
            .execute(
                "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
                 VALUES (?6, ?1, 1, ?2, ?3, ?4, ?5, 1)",
                rusqlite::params![
                    pid,
                    owner,
                    role,
                    state,
                    (state == "exited").then_some(0),
                    boot
                ],
            )
            .expect("record process");
    };
    let insert = |store: &Store, pid: i64, role: &str, state: &str| {
        insert_on(store, "boot-a", pid, role, state)
    };
    // A worker that ran compilers leaves far more exited children than one
    // page; only one exited witness per boot is custody evidence and startup
    // must not fail.
    insert(&store, 1, "plugin", "exited");
    for pid in 2..=(crate::MAX_PAGE_ENTRIES as i64 + 600) {
        insert(&store, pid, "child", "exited");
    }
    insert(&store, 9_000_000, "child", "running");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert!(!retained[0].processes_truncated);
    let roles: Vec<_> = retained[0]
        .processes
        .iter()
        .map(|process| (process.role.as_str(), process.exited))
        .collect();
    assert_eq!(roles, [("plugin", true), ("child", true), ("child", false)]);
    assert_eq!(retained[0].processes[1].identity.pid, 2);

    // Boot presence survives: an exited child on another boot still yields one
    // witness for that boot, so Linux custody sees the same boots as before.
    insert_on(&store, "boot-b", 5, "child", "exited");
    insert_on(&store, "boot-b", 6, "child", "exited");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    let boots: Vec<_> = retained[0]
        .processes
        .iter()
        .map(|process| (process.identity.host_boot_id.as_str(), process.identity.pid))
        .collect();
    assert_eq!(
        boots,
        [
            ("boot-a", 1),
            ("boot-a", 2),
            ("boot-a", 9_000_000),
            ("boot-b", 5)
        ]
    );

    // An overflow of records that still need a liveness check keeps only this
    // generation retained, without failing the readback.
    for pid in 10_000_000..(10_000_000 + crate::MAX_PAGE_ENTRIES as i64) {
        insert(&store, pid, "child", "running");
    }
    let retained = store
        .restart_retained_generations(None)
        .expect("bounded readback never fails startup");
    assert!(retained[0].processes_truncated);
    assert_eq!(retained[0].processes.len(), crate::MAX_PAGE_ENTRIES);
}

#[test]
fn verified_restart_closure_releases_only_the_retained_lost_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let scopes = |profile: &str| {
        json!([
            {"scope": format!("profile:{profile}"), "max_live": null},
            {"scope": "account:restart-shared", "max_live": 1}
        ])
    };
    let launch = capacity_launch_request("restart-first", "restart-first", scopes("restart-first"));
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch Agent");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    store
        .settle(
            &launch_id,
            CommandState::Unknown,
            Some(json!({"reason":"start_readback_unknown"})),
            now + 2,
        )
        .expect("retain uncertain native start");
    store.recover_on_restart(now + 3).expect("recover");

    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert_eq!(
        (retained[0].agent.as_str(), retained[0].generation),
        (agent.as_str(), 1)
    );
    assert!(retained[0].named_custody);
    assert!(retained[0].processes.is_empty());
    assert!(
        store
            .restart_retained_generations(Some(&agent))
            .expect("read next page")
            .is_empty()
    );

    let notice = store
        .record_restart_capacity_retained(&agent, 1, "windows_job_live", now + 5)
        .expect("record retained reservation");
    assert_eq!(notice.len(), 1);
    assert_eq!(notice[0].kind, "capacity.retained");
    assert_eq!(notice[0].data["missing"], "windows_job_live");

    // Closure for any other generation cannot touch the retained slot.
    assert!(
        store
            .release_restart_closed_generation(&agent, 2, "windows_job_closed", false, now + 6)
            .expect("stale closure")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let other =
        || capacity_launch_request("restart-other", "restart-other", scopes("restart-other"));
    assert_eq!(
        store.submit("owner", other(), now + 7).unwrap_err().code,
        "capacity_unavailable"
    );

    let released = store
        .release_restart_closed_generation(&agent, 1, "unix_group_empty", true, now + 8)
        .expect("release verified closure");
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].kind, "capacity.released");
    assert_eq!(released[0].data["reason"], "restart_custody_closed");
    assert_eq!(released[0].data["evidence"], "unix_group_empty");
    assert_eq!(released[0].data["orphan_possible"], true);
    assert_eq!(capacity_generation(&store, &agent), None);
    assert!(
        store
            .restart_retained_generations(None)
            .expect("nothing retained")
            .is_empty()
    );
    assert!(
        store
            .release_restart_closed_generation(&agent, 1, "unix_group_empty", true, now + 9)
            .expect("repeated closure")
            .is_empty()
    );
    store
        .submit("owner", other(), now + 10)
        .expect("shared scope admits after verified closure");
}

#[test]
fn generations_started_before_the_custody_namespace_are_not_named() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).expect("open persistent Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch = capacity_launch_request(
        "restart-legacy",
        "restart-legacy",
        json!([{"scope":"profile:restart-legacy","max_live":null}]),
    );
    let launch_id = launch.id.clone();
    store.submit("owner", launch, now).expect("admit launch");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    // Return the file to schema 16, as an older binary left it, and upgrade.
    // Drop the v37 digest, v34 config and v33 session-usage tables first and
    // undo the v36/v35 column adds, so the reopen exercises their migration
    // blocks, then the v32 attention-wake table. Drop the
    // v29/v30 activity artifacts next: the v30 events triggers would
    // otherwise fire during the table rebuilds below while their
    // native_child_registrations reads no longer resolve (#418). Drop the
    // v31 fleet indexes too, or the parent-column drop below fails and the
    // reopen would recreate existing indexes.
    store
        .connection
        .execute_batch(
            "DROP TABLE completion_digests;
             DELETE FROM schema_migrations WHERE version = 37;
             DROP TABLE agent_config;
             ALTER TABLE agents DROP COLUMN profile_inherited;
             DELETE FROM schema_migrations WHERE version = 36;
             DELETE FROM schema_migrations WHERE version = 35;
             DELETE FROM schema_migrations WHERE version = 34;
             ALTER TABLE commands DROP COLUMN config_revision;
             ALTER TABLE turns DROP COLUMN config_revision;
             DROP TABLE session_usage;
             DELETE FROM schema_migrations WHERE version = 33;
             DROP TABLE attention_wakes;
             DELETE FROM schema_migrations WHERE version = 32;
             DROP INDEX agents_list_state;
             DROP INDEX agents_list_project;
             DROP INDEX agents_list_harness;
             DROP INDEX agents_list_parent;
             DROP INDEX agents_list_root;
             DELETE FROM schema_migrations WHERE version = 31;
             DROP TRIGGER native_child_activity_revision;
             DROP TRIGGER native_child_progress_pruned;
             DROP INDEX native_children_producer;
             DROP INDEX turns_native_activity;
             DROP INDEX events_native_progress_time;
             DROP INDEX events_git_progress_time;
             ALTER TABLE agents DROP COLUMN native_child_revision;
             DELETE FROM schema_migrations WHERE version = 30;
             ALTER TABLE turns DROP COLUMN outcome;
             ALTER TABLE turns DROP COLUMN native_status;
             DELETE FROM schema_migrations WHERE version = 29;
             DROP INDEX agents_parent; DROP INDEX agents_root;
             ALTER TABLE agents DROP COLUMN parent; ALTER TABLE agents DROP COLUMN root;
             ALTER TABLE agents DROP COLUMN depth; ALTER TABLE agents DROP COLUMN control;
             ALTER TABLE commands DROP COLUMN selector;
             DELETE FROM schema_migrations WHERE version = 23;
             DROP TABLE store_identity; DELETE FROM schema_migrations WHERE version = 17;
             DROP INDEX commands_unsettled_created;
             DROP INDEX processes_owner_identity;
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             DELETE FROM schema_migrations WHERE version = 18;
             DROP TABLE goals; DROP TABLE goal_markers;
             DELETE FROM schema_migrations WHERE version = 19;
             DELETE FROM schema_migrations WHERE version = 20;
             DROP INDEX messages_prune_acked;
             DELETE FROM schema_migrations WHERE version = 22;
             DROP TABLE messages; DROP TABLE vars; DROP TABLE mail_notices;
             DELETE FROM schema_migrations WHERE version = 21;
             DROP TABLE claims;
             DELETE FROM schema_migrations WHERE version = 24;
             DROP TABLE owned_exits;
             DELETE FROM schema_migrations WHERE version = 25;
             DROP TABLE native_child_registrations;
             DELETE FROM schema_migrations WHERE version = 26;
             ALTER TABLE turns DROP COLUMN usage;
             DELETE FROM schema_migrations WHERE version = 27;
             DROP INDEX commands_context_pending;
             DELETE FROM schema_migrations WHERE version = 28;",
        )
        .expect("roll back to schema 16");
    drop(store);
    let mut store = Store::open(&path).expect("migrate 16 to latest");
    store.recover_on_restart(now + 2).expect("recover");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert!(!retained[0].named_custody);
    assert!(!store.custody_namespace().expect("namespace").is_empty());
    drop(store);
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
        .reject_workspace_resume(&resume.id, "project_registration_revoked", now + 10)
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
        .reject_workspace_resume(&first_resume_id, "fixture_pre_dispatch_rejection", now + 23)
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
        .reject_workspace_resume(&first_resume_id, "fixture_pre_dispatch_rejection", now + 25)
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
            .reject_workspace_resume(&resume_id, "must_not_override_unknown_stop", now + 10)
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
            .reject_workspace_resume(&resume_id, "fixture_rejection", reject_now)
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
                 profile_snapshot TEXT NOT NULL,
                 state TEXT NOT NULL, project TEXT NOT NULL, harness TEXT NOT NULL
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
                 kind TEXT, source TEXT, data TEXT,
                 ts INTEGER NOT NULL DEFAULT 0,
                 native_id TEXT
             );
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
             CREATE TABLE source_state(
                 agent INTEGER, generation INTEGER, source TEXT, state TEXT
             );
             CREATE TABLE processes(
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
                 project TEXT NOT NULL, repo_path TEXT NOT NULL, repo_common_dir TEXT NOT NULL,
                 requested_branch TEXT NOT NULL, base_ref TEXT, base_oid TEXT NOT NULL,
                 worktree_path TEXT NOT NULL UNIQUE, frozen_setup TEXT NOT NULL,
                 phase TEXT NOT NULL CHECK(phase IN ('creating','setup_ready','setup_running','ready','unknown','failed','removing','removed')),
                 setup_index INTEGER NOT NULL DEFAULT 0 CHECK(setup_index >= 0),
                 outcome TEXT, updated_at INTEGER NOT NULL
             );",
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
                "INSERT INTO agents(id, generation, observe_only, profile, profile_snapshot,
                                    state, project, harness)
                 VALUES (?1, ?2, 0, ?3, ?4, 'idle', 'capacity-test', 'test-harness')",
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
             VALUES (4,1,'stream','closed'),
                    (4,1,'os','closed'),
                    (5,1,'stream','closed'),
                    (6,1,'stream','closed'),
                    (6,1,'os','closed'),
                    (10,1,'stream','closed'),
                    (10,1,'os','closed')",
            [],
        )
        .expect("insert source facts");
        old.execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state,
                 exit_code, updated_at)
             VALUES ('boot',1,0,'agent:a4#1','plugin','exited',NULL,1),
                    ('boot',2,0,'agent:a6#1','plugin','exited',NULL,1),
                    ('boot',3,0,'agent:a10#1','plugin','exited',NULL,1)",
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

/// A force-stopped generation that never committed `session.ended`, with its
/// plugin process recorded as exited; returns the Agent id. The stop already
/// closed its native sources administratively.
fn stopped_capacity_agent_without_session_end(
    store: &mut Store,
    project: &str,
    now: i64,
) -> String {
    let (agent, _) = ready_capacity_agent(store, project, now);
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
                selector: None,
                verb: "stop".to_owned(),
                args: json!({"force": true}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 2,
        )
        .expect("admit forced stop");
    store.dispatch(&stop_id, now + 2).unwrap();
    store
        .settle(&stop_id, CommandState::Applied, None, now + 3)
        .expect("forced stop applied without session.ended");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-order-boot', 81003, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![format!("agent:{agent}#1"), now + 4],
        )
        .expect("persist exact exited plugin process");
    assert_eq!(capacity_generation(store, &agent), Some(1));
    agent
}

#[test]
fn owned_exit_capacity_releases_when_the_os_source_closes_last() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = stopped_capacity_agent_without_session_end(&mut store, "capacity-os-last", now);
    // The host proves exit and drain before the monitor's next sweep closes `os`.
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 5)
            .expect("owned exit with OS still open")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 6)
        .expect("monitor closes the OS source");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "capacity.released")
            .count(),
        1,
        "the last exit fact releases the reservation: {events:?}"
    );
    assert_eq!(capacity_generation(&store, &agent), None);
    let repeated = store
        .set_process_source_status(&agent, 1, "ok", true, now + 7)
        .expect("repeated closure");
    assert!(
        repeated
            .iter()
            .all(|event| event.kind != "capacity.released")
    );
}

#[test]
fn os_closure_without_a_host_drain_proof_keeps_capacity() {
    // A forced stop whose reader never drained: the host records no owned-exit
    // proof, so closing `os` must not release although the stop closed the
    // native sources administratively.
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = stopped_capacity_agent_without_session_end(&mut store, "capacity-no-drain", now);
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 5)
        .expect("monitor closes the OS source");
    assert!(events.iter().all(|event| event.kind != "capacity.released"));
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let released = store
        .record_owned_control_exit(&agent, 1, now + 6)
        .expect("the host drain proof arrives last");
    assert!(
        released
            .iter()
            .any(|event| event.kind == "capacity.released")
    );
    assert_eq!(capacity_generation(&store, &agent), None);
}

#[test]
fn restart_reconciles_a_stopped_generation_that_kept_its_reservation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent =
        stopped_capacity_agent_without_session_end(&mut store, "capacity-restart-stopped", now);
    store.recover_on_restart(now + 5).expect("recover");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(
        retained.len(),
        1,
        "a stopped generation still holding capacity is retained"
    );
    assert_eq!(
        (retained[0].agent.as_str(), retained[0].generation),
        (agent.as_str(), 1)
    );
    let released = store
        .release_restart_closed_generation(&agent, 1, "windows_job_closed", false, now + 6)
        .expect("release verified closure");
    assert!(
        released
            .iter()
            .any(|event| event.kind == "capacity.released")
    );
    assert_eq!(capacity_generation(&store, &agent), None);
}

#[test]
fn session_end_alone_retains_capacity_until_exact_owned_exit_proof_settles() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "capacity-regression", now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install OS process source");
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    // End alone keeps reservation.
    store
        .put_batch(
            SourceBatch {
                batch_id: "regression:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "regression:ended".to_owned(),
                    data: json!({}),
                }],
            },
            now + 2,
        )
        .expect("commit session ended");
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().state,
        AgentState::Stopped
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "session.ended alone must retain capacity reservation"
    );
    assert_eq!(
        store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-regression-blocked",
                    "resume-profile",
                    json!([{"scope":"profile:resume-profile","max_live":1}]),
                ),
                now + 3,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable",
        "configured capacity is still held by generation 1"
    );

    // Owned-exit proof without all source/process closure still keeps it.
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 4)
            .expect("owned-exit proof before physical closure is accepted")
            .is_empty(),
        "unclosed physical facts must not emit capacity.released"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "reservation remains held without physical process/source closure"
    );

    let process_owner = format!("agent:{agent}#1");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-regression-boot', 81099, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![process_owner, now + 5],
        )
        .expect("persist exact exited process record");
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "reservation remains held while OS source is still open"
    );

    // Full exact proof releases it even if OS closure arrives later.
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 6)
        .expect("late monitor closure of OS process source");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "capacity.released")
            .count(),
        1,
        "late OS closure completes the proof and releases reservation: {events:?}"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        None,
        "reservation is now released"
    );

    // Existing stale-generation isolation must remain.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    store
        .submit("owner", resume, now + 7)
        .expect("admit resume for generation 2 into released capacity");
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 8)
            .expect("stale generation 1 exit proof is harmless")
            .is_empty(),
        "stale generation exit proof must emit nothing"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(2),
        "stale generation-1 exit cannot release generation-2 reservation"
    );

    let repeated_os = store
        .set_process_source_status(&agent, 1, "ok", true, now + 9)
        .expect("repeated stale os closure");
    assert!(
        repeated_os
            .iter()
            .all(|event| event.kind != "capacity.released"),
        "stale OS closure cannot release generation-2 reservation"
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));
}

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
    let mut admission = super::admission::submit_tx(
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
        super::canonical::digest(&native.semantic_value()).unwrap()
    );
    let reread = store
        .read_events(started.seq - 1, 1, Some(started.seq))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reread.cause, started.cause);
}

fn explain_plan(store: &Store, sql: &str, parameters: impl rusqlite::Params) -> String {
    let mut statement = store
        .connection
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .expect("prepare EXPLAIN QUERY PLAN");
    let rows = statement
        .query_map(parameters, |row| row.get::<_, String>(3))
        .expect("read query plan");
    let mut plan = Vec::new();
    for row in rows {
        plan.push(row.expect("plan row"));
    }
    plan.join("\n")
}

fn assert_ordered_index_plan(plan: &str, table: &str, access: &str) {
    assert!(
        plan.contains(access),
        "query plan should contain {access}, got:\n{plan}"
    );
    for line in plan.lines() {
        assert!(
            !(line.contains(&format!("SCAN {table}")) && !line.contains("USING INDEX")),
            "query plan should not table-scan {table}, got:\n{plan}"
        );
    }
    assert!(
        !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
        "query plan should not sort, got:\n{plan}"
    );
}

fn insert_command(store: &Store, id: &str, target: i64, verb: &str, state: &str, created_at: i64) {
    store
        .connection
        .execute(
            "INSERT INTO commands(id, principal, method, payload_digest, target, verb, args,
                 state, created_at, updated_at)
             VALUES (?1, 'owner', 'command.submit', x'00', ?2, ?3, '{}', ?4, ?5, ?5)",
            rusqlite::params![id, target, verb, state, created_at],
        )
        .expect("insert command");
}

#[test]
fn ordered_store_reads_install_v18_indexes() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let found: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index'
               AND name IN ('commands_unsettled_created', 'processes_owner_identity')",
            [],
            |row| row.get(0),
        )
        .expect("count v18 indexes");
    assert_eq!(found, 2);
    // The unordered commands_open partial index is redundant with
    // commands_unsettled_created and would keep the old sort plan.
    let dropped: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'commands_open'",
            [],
            |row| row.get(0),
        )
        .expect("count dropped index");
    assert_eq!(dropped, 0);
}

#[test]
fn ordered_store_reads_keep_unsettled_order_and_observer_exclusions() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    store
        .connection
        .execute(
            "INSERT INTO agents(project, harness, profile, state, created_at, updated_at)
             VALUES ('proj', 'fake', 'fake', 'idle', 1000, 1000)",
            [],
        )
        .expect("insert agent");
    let agent = store.connection.last_insert_rowid();
    let mut observers = Vec::new();
    for _ in 0..2 {
        store
            .connection
            .execute(
                "INSERT INTO agents(project, harness, profile, state, origin, observe_only,
                     created_at, updated_at)
                 VALUES ('proj', 'codex', 'codex', 'unknown', 'external', 1, 1000, 1000)",
                [],
            )
            .expect("insert observer");
        observers.push(store.connection.last_insert_rowid());
    }
    let observed_with_source = observers[0];
    let observed_without_source = observers[1];
    store
        .connection
        .execute(
            "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
             VALUES (?1, 0, 'stream', 'open', 'ok', 1000)",
            [observed_with_source],
        )
        .expect("insert open source");

    insert_command(&store, "cmd-applied", agent, "send", "applied", 100);
    insert_command(&store, "cmd-rejected", agent, "send", "rejected", 150);
    insert_command(&store, "cmd-accepted", agent, "send", "accepted", 200);
    insert_command(&store, "cmd-queued", agent, "send", "queued", 250);
    insert_command(&store, "cmd-dispatched", agent, "send", "dispatched", 300);
    insert_command(&store, "cmd-tie-first", agent, "send", "queued", 400);
    insert_command(&store, "cmd-tie-second", agent, "send", "accepted", 400);
    insert_command(
        &store,
        "cmd-unknown-send",
        observed_with_source,
        "send",
        "unknown",
        500,
    );
    insert_command(
        &store,
        "cmd-unknown-attach-hidden",
        observed_without_source,
        "attach",
        "unknown",
        550,
    );
    insert_command(
        &store,
        "cmd-unknown-attach-visible",
        observed_with_source,
        "attach",
        "unknown",
        600,
    );
    insert_command(
        &store,
        "cmd-unknown-detach-managed",
        agent,
        "detach",
        "unknown",
        700,
    );

    let expected = [
        "cmd-accepted",
        "cmd-queued",
        "cmd-dispatched",
        "cmd-tie-first",
        "cmd-tie-second",
        "cmd-unknown-send",
        "cmd-unknown-attach-visible",
        "cmd-unknown-detach-managed",
    ];
    let listed = store.list_unsettled_commands(100).expect("list unsettled");
    let ids: Vec<&str> = listed.iter().map(|command| command.id.as_str()).collect();
    assert_eq!(ids, expected);
    let first_page = store.list_unsettled_commands(3).expect("first page");
    let first_ids: Vec<&str> = first_page
        .iter()
        .map(|command| command.id.as_str())
        .collect();
    assert_eq!(first_ids, expected[..3]);

    let list_sql = format!(
        "{} WHERE {} ORDER BY created_at, rowid LIMIT ?1",
        super::LIST_UNSETTLED_COMMANDS_COLUMNS,
        super::UNSETTLED_COMMANDS_WHERE
    );
    let plan = explain_plan(&store, &list_sql, rusqlite::params![100]);
    assert_ordered_index_plan(
        &plan,
        "commands",
        "SCAN commands USING INDEX commands_unsettled_created",
    );
    store
        .connection
        .execute_batch("ANALYZE")
        .expect("analyze Store");
    let analyzed_plan = explain_plan(&store, &list_sql, rusqlite::params![100]);
    assert_ordered_index_plan(
        &analyzed_plan,
        "commands",
        "SCAN commands USING INDEX commands_unsettled_created",
    );
}

fn insert_process(store: &Store, boot: &str, pid: u32, start_time: i64, owner: &str, state: &str) {
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state,
                 exit_code, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'child', ?5, NULL, 1000)",
            rusqlite::params![boot, pid, start_time, owner, state],
        )
        .expect("insert process");
}

/// Pre-split `list_processes` statement: the optional-cursor OR form. Kept as the
/// page-identity oracle for the split first-page/cursor-range queries.
fn legacy_process_page(
    store: &Store,
    owner: &str,
    after: Option<&ProcessIdentity>,
    limit: i64,
) -> Vec<ProcessRecord> {
    const LEGACY_SQL: &str =
        "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
             FROM processes WHERE owner = ?1
               AND (?2 IS NULL OR (host_boot_id, pid, start_time) > (?2, ?3, ?4))
             ORDER BY host_boot_id, pid, start_time LIMIT ?5";
    let start = after
        .map(super::validate_process_identity)
        .transpose()
        .expect("valid cursor");
    let mut statement = store
        .connection
        .prepare(LEGACY_SQL)
        .expect("prepare legacy page");
    let rows = statement
        .query_map(
            rusqlite::params![
                owner,
                after.map(|id| id.host_boot_id.as_str()),
                after.map(|id| id.pid),
                start,
                limit
            ],
            super::process_record_from_row,
        )
        .expect("read legacy page");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect legacy page")
}

#[test]
fn live_process_owners_reports_only_non_exited_custody_with_honest_totals() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    insert_process(&store, "boot", 1, 1, "agent:a1#1", "running");
    insert_process(&store, "boot", 2, 1, "agent:a1#1", "exited");
    insert_process(&store, "boot", 3, 1, "agent:a2#3", "unknown");
    insert_process(&store, "boot", 4, 1, "workspace:a3#1", "running");
    insert_process(&store, "boot", 5, 1, "workspace:a4#2", "exited");
    insert_process(&store, "boot", 6, 1, "bogus-owner", "running");
    let (owners, total) = store.live_process_owners(100).expect("live owners");
    // Four distinct owners retain non-exited records; the malformed owner
    // counts toward the total but never fabricates custody.
    assert_eq!(total, 4);
    assert_eq!(owners.len(), 3);
    assert!(!owners.iter().any(|owner| owner.agent == "bogus-owner"));
    let plugin: Vec<(&str, i64)> = owners
        .iter()
        .filter(|owner| !owner.workspace)
        .map(|owner| (owner.agent.as_str(), owner.generation))
        .collect();
    assert_eq!(plugin, [("a1", 1), ("a2", 3)]);
    let workspace: Vec<(&str, i64)> = owners
        .iter()
        .filter(|owner| owner.workspace)
        .map(|owner| (owner.agent.as_str(), owner.generation))
        .collect();
    assert_eq!(workspace, [("a3", 1)]);
    let (bounded, bounded_total) = store.live_process_owners(2).expect("bounded owners");
    assert_eq!(bounded_total, 4);
    assert_eq!(bounded.len(), 2);
}

#[test]
fn ordered_store_reads_paginate_processes_by_owner_identity() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let owner = "owner-a";
    for (boot, pid, start) in [
        ("boot-b", 100, 10),
        ("boot-a", 300, 30),
        ("boot-a", 100, 20),
        ("boot-a", 100, 10),
        ("boot-b", 50, 5),
    ] {
        insert_process(&store, boot, pid, start, owner, "running");
    }
    insert_process(&store, "boot-a", 1, 1, "owner-b", "running");
    let expected = [
        ("boot-a", 100, 10),
        ("boot-a", 100, 20),
        ("boot-a", 300, 30),
        ("boot-b", 50, 5),
        ("boot-b", 100, 10),
    ]
    .map(|(boot, pid, start)| ProcessIdentity {
        host_boot_id: boot.to_owned(),
        pid,
        start_time: start,
    });

    let full = store.list_processes(owner, None, 100).expect("list all");
    let ids: Vec<ProcessIdentity> = full.iter().map(|row| row.identity.clone()).collect();
    assert_eq!(ids, expected);

    let mut walked = Vec::new();
    let mut after: Option<ProcessIdentity> = None;
    loop {
        let page = store
            .list_processes(owner, after.as_ref(), 2)
            .expect("walk page");
        if page.is_empty() {
            break;
        }
        after = Some(page.last().expect("nonempty page").identity.clone());
        walked.extend(page);
    }
    assert_eq!(walked, full);

    let other = store
        .list_processes("owner-b", None, 100)
        .expect("other owner");
    assert_eq!(other.len(), 1);

    // A cursor between rows seeks the range start; a tail cursor ends the walk.
    let between = store
        .list_processes(
            owner,
            Some(&ProcessIdentity {
                host_boot_id: "boot-a".to_owned(),
                pid: 100,
                start_time: 15,
            }),
            100,
        )
        .expect("between page");
    assert_eq!(between.first().expect("between row").identity, expected[1]);
    let past_tail = store
        .list_processes(owner, Some(&expected[4]), 100)
        .expect("past tail page");
    assert!(past_tail.is_empty());

    // Identical pages before/after the split on every cursor position.
    let mut cursors: Vec<Option<ProcessIdentity>> = vec![None];
    cursors.extend(expected.iter().cloned().map(Some));
    for cursor in &cursors {
        for limit in [1usize, 2, 100] {
            assert_eq!(
                store
                    .list_processes(owner, cursor.as_ref(), limit)
                    .expect("split page"),
                legacy_process_page(&store, owner, cursor.as_ref(), limit as i64),
                "split page differs at cursor {cursor:?} limit {limit}"
            );
        }
    }
    assert_eq!(
        store
            .list_processes("owner-b", None, 10)
            .expect("split other"),
        legacy_process_page(&store, "owner-b", None, 10)
    );

    assert_eq!(
        store
            .list_processes(
                owner,
                Some(&ProcessIdentity {
                    host_boot_id: String::new(),
                    pid: 1,
                    start_time: 0,
                }),
                10,
            )
            .expect_err("empty boot id")
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .list_processes(owner, Some(&expected[0]), 0)
            .expect_err("empty limit")
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .list_processes("", None, 10)
            .expect_err("empty owner")
            .code,
        "invalid"
    );

    let first_plan = explain_plan(
        &store,
        super::LIST_PROCESSES_FIRST_PAGE_SQL,
        rusqlite::params![owner, 2],
    );
    assert_ordered_index_plan(
        &first_plan,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
    store
        .connection
        .execute_batch("ANALYZE")
        .expect("analyze Store");
    let analyzed_first = explain_plan(
        &store,
        super::LIST_PROCESSES_FIRST_PAGE_SQL,
        rusqlite::params![owner, 2],
    );
    assert_ordered_index_plan(
        &analyzed_first,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
    let analyzed_tail = explain_plan(
        &store,
        super::LIST_PROCESSES_CURSOR_PAGE_SQL,
        rusqlite::params![owner, "boot-b", 100u32, 10i64, 2],
    );
    assert_ordered_index_plan(
        &analyzed_tail,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
}

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

/// Launch a `muse`-harness Agent through generation-1 readiness, returning
/// the Agent, its launch command, and its retained native subject. The
/// session stays live; tests end it with `end_muse_session` when they need a
/// terminal old generation.
fn ready_muse_agent(
    store: &mut Store,
    project: &str,
    profile: &str,
    workspace: &str,
    now: i64,
) -> (String, String, String, String) {
    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": project,
        "harness": "muse",
        "profile": profile,
        "profile_snapshot": {"harness": "muse", "model": "muse-test-model"},
        "workspace": workspace,
    });
    launch.expected_generation = Some(0);
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit muse launch")
        .command
        .target
        .expect("muse launch Agent");
    let native_scope = format!("{project}:{agent}:scope");
    let native_ref = format!("{project}:{agent}:session");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch muse launch");
    install_fixture_sources(store, &agent, &launch_id, 1, now + 1);
    store
        .install_plugin_sources(
            &agent,
            1,
            &launch_id,
            &BTreeMap::from([
                ("session".to_owned(), "stream".to_owned()),
                ("turn".to_owned(), "stream".to_owned()),
                ("message".to_owned(), "stream".to_owned()),
                ("tool".to_owned(), "stream".to_owned()),
                ("request".to_owned(), "stream".to_owned()),
            ]),
            now + 1,
        )
        .expect("install muse stream source plan");
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
        .expect("commit muse session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 3)
        .expect("settle muse launch");
    (agent, launch_id, native_scope, native_ref)
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
fn enforce_page_bytes_keeps_longest_fitting_prefix() {
    // Reference quadratic sizing: reserialize each shrinking prefix.
    fn reference_keep(items: &[String]) -> usize {
        let mut keep = items.len();
        while serde_json::to_vec(&items[..keep])
            .expect("encode prefix")
            .len()
            > super::MAX_PAGE_BYTES
        {
            keep -= 1;
            if keep == 0 {
                break;
            }
        }
        keep
    }

    // Mixed entry sizes, including bytes that must be escaped in JSON
    // (quotes, backslashes, control characters) so the byte count covers
    // escaped output rather than in-memory length.
    let small = "a\"b\\c\n\u{1f600}".to_owned();
    let medium = "m".repeat(1_000);
    let large = "L\"\\".repeat(200_000);
    let tail = "t".repeat(500);
    let candidates = vec![small.clone(), medium.clone(), large.clone(), tail.clone()];
    let expected = reference_keep(&candidates);

    let mut sized = candidates.clone();
    super::enforce_page_bytes(&mut sized).expect("mixed page fits a prefix");
    assert_eq!(
        sized.len(),
        expected,
        "linear sizing must keep the same prefix as reserializing each prefix"
    );
    assert_eq!(&sized, &candidates[..expected]);
    let encoded = serde_json::to_vec(&sized).expect("encode kept prefix");
    assert!(encoded.len() <= super::MAX_PAGE_BYTES);
    if expected < candidates.len() {
        let mut extended = sized.clone();
        extended.push(candidates[expected].clone());
        assert!(
            serde_json::to_vec(&extended)
                .expect("encode overflow")
                .len()
                > super::MAX_PAGE_BYTES,
            "kept prefix must be the longest fitting one"
        );
    }
}

#[test]
fn enforce_page_bytes_rejects_oversized_first_entry() {
    let oversized = "x".repeat(super::MAX_PAGE_BYTES);
    let mut items = vec![oversized];
    let error = super::enforce_page_bytes(&mut items).expect_err("first entry cannot fit");
    assert_eq!(error.code, "frame_too_large");
    assert!(items.is_empty());

    let mut empty: Vec<String> = Vec::new();
    super::enforce_page_bytes(&mut empty).expect("empty page stays empty");
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
fn agent_list_filters_use_indexes_and_pages_stay_ordered() {
    use super::{AgentListFilter, agent_list_select};

    fn fleet_agent(store: &mut Store, project: &str, now: i64) -> String {
        let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"harness": "codex", "profile": "codex-test",
            "profile_snapshot": {}, "project": project, "workspace": "synthetic-workspace"});
        let command = launch.id.clone();
        let id = store
            .submit("owner", launch, now)
            .expect("admit fleet launch")
            .command
            .target
            .expect("launch target");
        store
            .dispatch(&command, now)
            .expect("dispatch fleet launch");
        id
    }

    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let first = fleet_agent(&mut store, "fleet-a", now);
    let second = fleet_agent(&mut store, "fleet-a", now + 1);
    let third = fleet_agent(&mut store, "fleet-b", now + 2);
    // Shape one child link so subtree filters have a descendant to find.
    let first_num = eliot_core::parse_agent_id(&first).expect("parse fleet root");
    let second_num = eliot_core::parse_agent_id(&second).expect("parse fleet child");
    store
        .connection
        .execute(
            "UPDATE agents SET parent = ?1, root = ?1, depth = 1 WHERE id = ?2",
            [first_num, second_num],
        )
        .expect("link fleet child");

    // Every new filter rides its composite index.
    for (filter, index) in [
        (
            AgentListFilter {
                state: Some(AgentState::Idle),
                ..Default::default()
            },
            "agents_list_state",
        ),
        (
            AgentListFilter {
                project: Some("fleet-a".to_owned()),
                ..Default::default()
            },
            "agents_list_project",
        ),
        (
            AgentListFilter {
                harness: Some("codex".to_owned()),
                ..Default::default()
            },
            "agents_list_harness",
        ),
        (
            AgentListFilter {
                parent: Some(first.clone()),
                ..Default::default()
            },
            "agents_list_parent",
        ),
        (
            AgentListFilter {
                root: Some(first.clone()),
                ..Default::default()
            },
            "agents_list_root",
        ),
    ] {
        let select =
            agent_list_select(&filter, None, Some(0), Some(2), false).expect("build fleet query");
        let mut statement = store
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {}", select.sql))
            .expect("explain fleet query");
        let plan = statement
            .query_map(rusqlite::params_from_iter(select.params.iter()), |row| {
                row.get::<_, String>(3)
            })
            .expect("read fleet plan")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect fleet plan")
            .join(" | ");
        assert!(
            plan.contains(&format!("USING INDEX {index}")),
            "filter must use {index}: {plan}"
        );
    }

    // Ordered continuation across pages with honest counts.
    let filter = AgentListFilter::default();
    let mut after = None;
    let mut seen = Vec::new();
    for last in [false, false, true] {
        let page = store
            .list_agent_summaries(&filter, after.as_deref(), 1, now + 9)
            .expect("page the fleet");
        assert_eq!(page.total, 3);
        assert_eq!(page.truncated, !last);
        assert_eq!(page.summaries.len(), 1);
        assert_eq!(page.next.as_deref(), Some(page.summaries[0].id.as_str()));
        assert!(seen.iter().all(|id| id != &page.summaries[0].id));
        if last {
            assert!(page.summaries[0].id == third);
        }
        after = page.next;
        seen.push(page.summaries.into_iter().next().unwrap().id);
    }
    assert!(seen.windows(2).all(|pair| pair[0] < pair[1]));
    let drained = store
        .list_agent_summaries(&filter, after.as_deref(), 1, now + 9)
        .expect("drain the fleet");
    assert!(drained.summaries.is_empty());
    assert_eq!(drained.total, 3);
    assert!(!drained.truncated);

    // Filtered totals ignore the cursor.
    let project = store
        .list_agent_summaries(
            &AgentListFilter {
                project: Some("fleet-a".to_owned()),
                ..Default::default()
            },
            Some(seen[1].as_str()),
            10,
            now + 9,
        )
        .expect("filter the fleet by project");
    assert_eq!(project.total, 2);
    assert!(!project.truncated);
    let root = store
        .list_agent_summaries(
            &AgentListFilter {
                root: Some(first.clone()),
                ..Default::default()
            },
            None,
            10,
            now + 9,
        )
        .expect("filter the fleet by root");
    assert_eq!(root.total, 2);
    let parent = store
        .list_agent_summaries(
            &AgentListFilter {
                parent: Some(first.clone()),
                ..Default::default()
            },
            None,
            10,
            now + 9,
        )
        .expect("filter the fleet by parent");
    assert_eq!(parent.total, 1);
    assert_eq!(parent.summaries[0].id, second);
    assert_eq!(parent.summaries[0].root, first);

    // Compact rows carry freshness without fabricating outcomes.
    let probe = &root.summaries[0];
    assert_eq!(probe.last_outcome, None);
    assert_eq!(probe.last_turn_id, None);
    assert_eq!(probe.last_progress_at, None);
    assert_eq!(probe.progress_age_ms, None);
    assert!(probe.updated_at >= now);

    // Scopes: self sees one row, tree sees the subtree, project the label.
    let agent = store
        .get_agent(&first)
        .expect("read fleet root")
        .expect("fleet root");
    let mut tree = agent.clone();
    tree.control = AgentControl::Tree;
    let scoped = store
        .list_agent_summaries_scoped(&tree, &AgentListFilter::default(), None, 10, now + 9)
        .expect("list the fleet subtree");
    assert_eq!(scoped.total, 2);
    let mut project_caller = agent.clone();
    project_caller.control = AgentControl::Project;
    let scoped = store
        .list_agent_summaries_scoped(
            &project_caller,
            &AgentListFilter::default(),
            None,
            10,
            now + 9,
        )
        .expect("list the fleet project");
    assert_eq!(scoped.total, 2);
    let scoped = store
        .list_agent_summaries_scoped(&agent, &AgentListFilter::default(), None, 10, now + 9)
        .expect("list the fleet self row");
    assert_eq!(scoped.total, 1);
    assert_eq!(scoped.summaries[0].id, first);
}

#[test]
fn tree_scope_listing_shares_bounded_depth_with_direct_reads() {
    use super::hierarchy::ANCESTOR_WALK_CAP;
    use super::{AgentListFilter, agent_list_select};

    // Listing, totals and direct reads share one bounded scope rule: the
    // tree CTE carries the same walk cap that gates parent-chain direct
    // reads, so a tree caller cannot list or count an Agent its `agent.get`
    // would deny.
    let mut caller = eliot_core::Agent::new("a1", "p", "codex", "fixture", 7);
    caller.control = eliot_core::AgentControl::Tree;
    let filter = AgentListFilter::default();
    for count in [false, true] {
        let select = agent_list_select(&filter, Some(&caller), None, Some(2), count)
            .expect("build tree scope query");
        assert!(
            select.sql.contains(&ANCESTOR_WALK_CAP.to_string()),
            "tree scope must bound depth with the shared cap: {}",
            select.sql
        );
        assert!(
            select.sql.contains("subtree(id, depth)"),
            "tree scope must track CTE depth: {}",
            select.sql
        );
    }
    // The selective structural index wins over state when both are present,
    // so `state=idle,parent=a42` totals do not scan all idle Agents.
    let combined = AgentListFilter {
        state: Some(eliot_core::AgentState::Idle),
        parent: Some("a42".to_owned()),
        ..Default::default()
    };
    let select = agent_list_select(&combined, None, Some(0), Some(2), false)
        .expect("build combined filter query");
    assert!(
        select.sql.contains("INDEXED BY agents_list_parent"),
        "combined filters must prefer the parent index: {}",
        select.sql
    );
    assert!(
        !select.sql.contains("INDEXED BY agents_list_state"),
        "combined filters must not force the state index: {}",
        select.sql
    );
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

fn ready_attention_agent(store: &mut Store, project: &str, now: i64) -> String {
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace", "harness":"codex", "profile":"test", "profile_snapshot":{}}),
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
    let plan = ["session", "turn", "message"]
        .into_iter()
        .map(|family| (family.to_owned(), "stream".to_owned()))
        .collect();
    store
        .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
        .expect("install plugin sources");
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
fn attention_commit_binds_agent_generation_and_subject_atomically() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-bind", now);
    let generation = store.get_agent(&agent).unwrap().unwrap().generation;
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"probe": true}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit attention emit");
    store
        .dispatch(&id, now + 1)
        .expect("dispatch attention emit");
    let report = json!({
        "schema": 1,
        "incident_id": "attention-test",
        "phase": "incident",
        "reason": "source_unhealthy",
        "agent_id": agent,
        "generation": generation,
    });
    let change = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": report}),
            now + 2,
        )
        .expect("atomic attention commit");
    assert_eq!(change.value.state, CommandState::Applied);
    assert_eq!(change.events.len(), 2);
    let event = &change.events[0];
    assert_eq!(event.kind, "x.user.agent.attention");
    assert_eq!(event.agent.as_deref(), Some(agent.as_str()));
    assert_eq!(event.generation, Some(generation));
    // The wire shape stays compatible: the report rides at data.data.
    assert_eq!(
        event
            .data
            .get("data")
            .and_then(|data| data.get("agent_id"))
            .and_then(|id| id.as_str()),
        Some(agent.as_str())
    );
    let expected_state = store.get_agent(&agent).unwrap().unwrap().state;
    assert_eq!(
        event
            .subject
            .as_ref()
            .expect("attention carries a subject")
            .state,
        expected_state
    );
    assert_eq!(change.events[1].kind, "command.applied");
    // An exact retry returns the recorded result without a new event.
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events after atomic commit");
    let retry = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": report}),
            now + 3,
        )
        .expect("idempotent attention retry");
    assert_eq!(retry.value.state, CommandState::Applied);
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
fn attention_commit_rejects_stale_evidence_without_a_binding() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-stale", now);
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"probe": true}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit attention emit");
    store
        .dispatch(&id, now + 1)
        .expect("dispatch attention emit");
    let event_count: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .expect("count events before stale commit");
    let error = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": {"agent_id": agent, "generation": 999}}),
            now + 2,
        )
        .expect_err("stale generation is rejected");
    assert_eq!(error.code, "stale_generation");
    assert_eq!(
        store.get_command(&id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .expect("count events after stale commit"),
        event_count
    );
}

#[test]
fn public_emit_cannot_use_the_reserved_attention_topic() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_attention_agent(&mut store, "attention-reserved", now);
    // A public `emit` naming the reserved topic is rejected at admission, so
    // it can never acquire an authoritative subject binding.
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: Ulid::new().to_string(),
                target: None,
                selector: None,
                verb: "emit".to_owned(),
                args: json!({"topic": "agent.attention", "data": {"agent_id": agent, "generation": 1}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect_err("reserved topic is rejected");
    assert_eq!(error.code, "invalid");
    // The internal verb admits only the reserved topic.
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: Ulid::new().to_string(),
                target: None,
                selector: None,
                verb: "attention.emit".to_owned(),
                args: json!({"topic": "agent.other", "data": {}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect_err("internal verb carries only the reserved topic");
    assert_eq!(error.code, "invalid");
    // Even a directly admitted public emit can never reach the bound commit:
    // the bound commit requires the internal verb.
    let id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: id.clone(),
                target: None,
                selector: None,
                verb: "emit".to_owned(),
                args: json!({"topic": "agent.probe", "data": {}}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit ordinary emit");
    store.dispatch(&id, now + 1).expect("dispatch emit");
    let error = store
        .commit_attention_event_applied(
            &id,
            "x.user.agent.attention",
            json!({"data": {"agent_id": agent, "generation": 1}}),
            now + 2,
        )
        .expect_err("public emit cannot use the bound commit");
    assert_eq!(error.code, "invalid");
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
fn session_usage_keeps_only_the_latest_snapshot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "session-usage", now);
    assert!(
        store
            .get_session_usage(&agent, 1)
            .expect("read absent session usage")
            .is_none()
    );

    let commit =
        |store: &mut Store, batch_id: &str, native_id: &str, usage: serde_json::Value, at: i64| {
            store
                .put_batch(
                    SourceBatch {
                        batch_id: batch_id.to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.usage".to_owned(),
                            native_id: native_id.to_owned(),
                            data: json!({"usage": usage}),
                        }],
                        cursor: None,
                    },
                    at,
                )
                .expect("commit session usage");
        };

    // First snapshot commits with the caller timestamp as observed_at.
    commit(
        &mut store,
        "session-usage:1",
        "session-usage:n1",
        json!({
            "context_used_tokens": 10,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
        }),
        now + 1,
    );
    let first = store
        .get_session_usage(&agent, 1)
        .expect("read session usage")
        .expect("snapshot retained");
    assert_eq!(first.context_used_tokens, Some(10));
    assert_eq!(
        first.source.as_deref(),
        Some("codex:thread/tokenUsage/updated")
    );

    // A newer snapshot replaces the retained one.
    commit(
        &mut store,
        "session-usage:2",
        "session-usage:n2",
        json!({
            "context_used_tokens": 20,
            "context_window_tokens": 1000,
            "cumulative_cost": {"amount": 0.5, "unit": "usd_estimate"},
            "source": "antigravity:usage_update",
        }),
        now + 2,
    );
    let second = store
        .get_session_usage(&agent, 1)
        .expect("read session usage")
        .expect("snapshot replaced");
    assert_eq!(second.context_used_tokens, Some(20));
    assert_eq!(second.source.as_deref(), Some("antigravity:usage_update"));

    // An older snapshot never regresses the readback.
    commit(
        &mut store,
        "session-usage:3",
        "session-usage:n3",
        json!({
            "context_used_tokens": 5,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
            "observed_at": now,
        }),
        now + 3,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second.clone()),
        "older snapshot must not replace a newer one"
    );

    // An equal-timestamp snapshot with different content never replaces.
    commit(
        &mut store,
        "session-usage:3b",
        "session-usage:n3b",
        json!({
            "context_used_tokens": 99,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
            "observed_at": now + 2,
        }),
        now + 5,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second.clone()),
        "equal timestamp must not replace the retained snapshot"
    );

    // A byte-identical duplicate is ignored, even on a newer commit.
    commit(
        &mut store,
        "session-usage:4",
        "session-usage:n4",
        serde_json::to_value(&second).expect("encode snapshot"),
        now + 4,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second),
        "duplicate snapshot must not replace the retained one"
    );
}

/// Idle first-generation Agent with an explicit frozen profile snapshot,
/// ready for `configure` without a native harness.
fn ready_configure_agent(
    store: &mut Store,
    project: &str,
    harness: &str,
    snapshot: serde_json::Value,
    now: i64,
) -> String {
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace", "harness":harness, "profile":"fake", "profile_snapshot":snapshot}),
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
    install_fixture_sources(store, &agent, &launch_id, 1, now);
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

fn configure_request(agent: &str, generation: i64, args: serde_json::Value) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "configure");
    request.target = Some(agent.to_owned());
    request.args = args;
    request.expected_generation = Some(generation);
    request
}

#[test]
fn configure_applies_overlay_and_stamps_monotonic_revisions() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let snapshot = json!({"model": "m0", "effort": "low"});
    let agent = ready_configure_agent(
        &mut store,
        "configure-basic",
        "codex",
        snapshot.clone(),
        now,
    );

    let first = configure_request(&agent, 1, json!({"model": "m1"}));
    let first_id = first.id.clone();
    store
        .submit("owner", first, now + 1)
        .expect("admit configure");
    store
        .dispatch(&first_id, now + 2)
        .expect("dispatch configure");
    let applied = store
        .apply_agent_config(&first_id, now + 3)
        .expect("apply configure");
    assert_eq!(applied.revision, 1, "first overlay is revision 1");
    assert_eq!(applied.requested_model.as_deref(), Some("m1"));
    assert_eq!(applied.requested_effort, None);
    assert_eq!(applied.model.as_deref(), Some("m1"), "requested model wins");
    assert_eq!(
        applied.effort.as_deref(),
        Some("low"),
        "unrequested effort falls back to the snapshot"
    );
    assert_eq!(applied.application, "next_turn");
    store
        .settle(&first_id, CommandState::Applied, None, now + 4)
        .expect("settle configure");

    let second = configure_request(&agent, 1, json!({"effort": "high"}));
    let second_id = second.id.clone();
    store
        .submit("owner", second, now + 5)
        .expect("admit second configure");
    store
        .dispatch(&second_id, now + 6)
        .expect("dispatch second configure");
    let reapplied = store
        .apply_agent_config(&second_id, now + 7)
        .expect("apply second configure");
    assert_eq!(reapplied.revision, 2, "revisions increase monotonically");
    assert_eq!(reapplied.requested_effort.as_deref(), Some("high"));
    assert_eq!(
        reapplied.model.as_deref(),
        Some("m0"),
        "unrequested model falls back to the snapshot"
    );

    // Reapplying the same command is idempotent, never a new revision.
    let replayed = store
        .apply_agent_config(&second_id, now + 8)
        .expect("reapply configure");
    assert_eq!(
        replayed.revision, 2,
        "reapply returns the retained revision"
    );
    let current = store
        .get_agent_config_current(&agent, 1)
        .expect("read current overlay")
        .expect("overlay exists");
    assert_eq!(current.revision, 2);

    // The frozen profile snapshot is unchanged by both overlays.
    let stored: eliot_core::Agent = store
        .get_agent(&agent)
        .expect("load agent")
        .expect("agent exists");
    assert_eq!(
        stored.profile_snapshot, snapshot,
        "snapshot is never rewritten"
    );
}

#[test]
fn configure_rejects_authority_unknown_and_malformed_fields() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_configure_agent(
        &mut store,
        "configure-rejections",
        "codex",
        json!({"model": "m0"}),
        now,
    );
    for (args, code) in [
        (json!({"permissions": {"scope": "all"}}), "not_allowed"),
        (json!({"approval": "auto"}), "not_allowed"),
        (json!({"mcp": {}}), "not_allowed"),
        (json!({"env": {}}), "not_allowed"),
        (json!({"route_binding": "x"}), "not_allowed"),
        (json!({"account": "other"}), "not_allowed"),
        (json!({"bogus": "x"}), "unsupported"),
        (json!({"model": "m1", "bogus": "x"}), "unsupported"),
        (json!({}), "invalid"),
        (json!({"model": 1}), "invalid"),
        (json!({"model": ""}), "invalid"),
        (json!({"model": "   "}), "invalid"),
        (json!({"model": null}), "invalid"),
    ] {
        let error = store
            .submit("owner", configure_request(&agent, 1, args), now + 1)
            .unwrap_err();
        assert_eq!(error.code, code, "configure args must be refused honestly");
    }
    // Explicit JSON nulls are absent fields, not values.
    store
        .submit(
            "owner",
            configure_request(&agent, 1, json!({"effort": "low", "model": null})),
            now + 2,
        )
        .expect("null model with a real effort configures effort");
    assert_eq!(
        store
            .get_agent_config_current(&agent, 1)
            .expect("read overlay"),
        None,
        "refused configures store nothing"
    );
}

#[test]
fn configure_gates_harness_generation_state_and_principal() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let codex = ready_configure_agent(
        &mut store,
        "configure-gates",
        "codex",
        json!({"model": "m0"}),
        now,
    );
    let muse = ready_configure_agent(
        &mut store,
        "configure-gates-muse",
        "muse",
        json!({"model": "m0", "effort": "low"}),
        now + 1,
    );
    let claude = ready_configure_agent(
        &mut store,
        "configure-gates-claude",
        "claude",
        json!({"model": "m0"}),
        now + 2,
    );
    let opencode = ready_configure_agent(
        &mut store,
        "configure-gates-opencode",
        "opencode",
        json!({"model": "m0"}),
        now + 3,
    );
    let antigravity = ready_configure_agent(
        &mut store,
        "configure-gates-antigravity",
        "antigravity",
        json!({"model": "m0"}),
        now + 4,
    );

    // Muse effort is a per-turn override; a Muse model change needs a new
    // native session, so it is refused as unsupported and stores nothing.
    store
        .submit(
            "owner",
            configure_request(&muse, 1, json!({"effort": "high"})),
            now + 10,
        )
        .expect("admit Muse effort configure");
    assert_eq!(
        store
            .submit(
                "owner",
                configure_request(&muse, 1, json!({"model": "m9"})),
                now + 11
            )
            .unwrap_err()
            .code,
        "unsupported",
        "Muse model needs a new native session"
    );
    assert_eq!(
        store
            .submit(
                "owner",
                configure_request(&muse, 1, json!({"model": "m9", "effort": "high"})),
                now + 12
            )
            .unwrap_err()
            .code,
        "unsupported",
        "mixed Muse configure is atomic: nothing applies"
    );
    for harness_agent in [&claude, &opencode, &antigravity] {
        assert_eq!(
            store
                .submit(
                    "owner",
                    configure_request(harness_agent, 1, json!({"effort": "high"})),
                    now + 13
                )
                .unwrap_err()
                .code,
            "unsupported",
            "adapters without a verified mechanism refuse configure"
        );
    }
    assert_eq!(
        store
            .get_agent_config_current(&muse, 1)
            .expect("read Muse overlay"),
        None,
        "refused Muse model configure stores nothing"
    );

    // Stale bindings, non-Owner principals and dead sessions are rejected.
    assert_eq!(
        store
            .submit(
                "owner",
                configure_request(&codex, 99, json!({"effort": "high"})),
                now + 14
            )
            .unwrap_err()
            .code,
        "stale_generation"
    );
    assert_eq!(
        store
            .submit(
                "agent:a9#1",
                configure_request(&codex, 1, json!({"effort": "high"})),
                now + 15
            )
            .unwrap_err()
            .code,
        "not_allowed",
        "configure requires the Owner principal"
    );
    store
        .connection
        .execute(
            "UPDATE agents SET state = 'stopped' WHERE id = ?1",
            [eliot_core::parse_agent_id(&codex).expect("parse agent")],
        )
        .expect("stop Agent");
    assert_eq!(
        store
            .submit(
                "owner",
                configure_request(&codex, 1, json!({"effort": "high"})),
                now + 16
            )
            .unwrap_err()
            .code,
        "bad_state",
        "configure requires a live Agent generation"
    );
}

#[test]
fn sends_run_under_their_dispatch_revision_and_turns_inherit_it() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_configure_agent(
        &mut store,
        "configure-freeze",
        "codex",
        json!({"model": "m0", "effort": "low"}),
        now,
    );

    // First send dispatched under the frozen snapshot, before any overlay.
    let send_one = send_request(&agent, 1, "first input");
    let send_one_id = send_one.id.clone();
    store
        .submit("owner", send_one, now + 1)
        .expect("admit first send");
    store
        .dispatch(&send_one_id, now + 2)
        .expect("dispatch first send");

    // The overlay is admitted while the first send is still unassociated:
    // dispatch queues it on the held settings lane instead of applying.
    let configure = configure_request(&agent, 1, json!({"model": "m1", "effort": "high"}));
    let configure_id = configure.id.clone();
    store
        .submit("owner", configure, now + 3)
        .expect("admit configure");
    store
        .dispatch(&configure_id, now + 4)
        .expect("queue configure");
    assert_eq!(
        store
            .get_command(&configure_id)
            .expect("load configure")
            .expect("configure exists")
            .state,
        CommandState::Queued,
        "unassociated send input holds the settings lane"
    );

    // A later send is admitted while the overlay is still pending.
    let send_two = send_request(&agent, 1, "second input");
    let send_two_id = send_two.id.clone();
    store
        .submit("owner", send_two, now + 6)
        .expect("admit second send");

    // Complete the first turn so both lanes free, then let the queued
    // configure apply before the second send dispatches.
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-freeze:frozen".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-freeze:frozen-turn:started".to_owned(),
                        data: json!({"command_id": send_one_id, "turn_id": "frozen-turn"}),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "message.completed".to_owned(),
                        native_id: "configure-freeze:frozen-turn:message".to_owned(),
                        data: json!({"command_id": send_one_id, "turn_id": "frozen-turn", "text": "first reply"}),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: "configure-freeze:frozen-turn:completed".to_owned(),
                        data: json!({"command_id": send_one_id, "turn_id": "frozen-turn", "status": "completed"}),
                    },
                ],
                cursor: None,
            },
            now + 7,
        )
        .expect("complete frozen turn");
    store
        .dispatch(&configure_id, now + 8)
        .expect("dispatch queued configure");
    let applied = store
        .apply_agent_config(&configure_id, now + 9)
        .expect("apply configure");
    assert_eq!(applied.revision, 1);
    // The Router settles applied right after the Store commit; only then
    // does the lane release for later input.
    store
        .settle(&configure_id, CommandState::Applied, None, now + 9)
        .expect("settle configure");

    // Association for the first send still binds the snapshot even though
    // the association lands after the overlay: a send committed before an
    // overlay never runs under newer configuration.
    assert_eq!(
        store
            .send_effective_config(&send_one_id)
            .expect("resolve first send"),
        None,
        "pre-overlay send runs under the frozen snapshot"
    );
    // The second send stamps its revision at its own dispatch, after the
    // overlay applied.
    store
        .dispatch(&send_two_id, now + 10)
        .expect("dispatch second send");
    let effective = store
        .send_effective_config(&send_two_id)
        .expect("resolve second send")
        .expect("overlay applies");
    assert_eq!(effective.revision, 1);
    assert_eq!(effective.model.as_deref(), Some("m1"));
    assert_eq!(effective.effort.as_deref(), Some("high"));
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-freeze:overlay".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "configure-freeze:overlay-turn:started".to_owned(),
                    data: json!({"command_id": send_two_id, "turn_id": "overlay-turn"}),
                }],
                cursor: None,
            },
            now + 9,
        )
        .expect("associate overlay turn");
    let frozen = store
        .get_turn(&agent, 1, "frozen-turn")
        .expect("load frozen turn")
        .expect("turn exists");
    assert_eq!(
        frozen.config_revision, 0,
        "turn records the snapshot revision"
    );
    let overlay = store
        .get_turn(&agent, 1, "overlay-turn")
        .expect("load overlay turn")
        .expect("turn exists");
    assert_eq!(
        overlay.config_revision, 1,
        "turn records the revision it ran under"
    );
    let overlay_outcome = store
        .get_command(&send_two_id)
        .expect("load second send")
        .expect("send exists")
        .outcome
        .expect("send has an outcome");
    assert_eq!(overlay_outcome["config_revision"], json!(1));
    assert_eq!(
        overlay_outcome["model"],
        json!("m1"),
        "first overlay turn shows its effective model"
    );
    assert_eq!(overlay_outcome["effort"], json!("high"));
    let frozen_outcome = store
        .get_command(&send_one_id)
        .expect("load first send")
        .expect("send exists")
        .outcome
        .expect("send has an outcome");
    assert!(
        frozen_outcome.get("config_revision").is_none(),
        "snapshot sends keep their exact outcome shape"
    );
}

#[test]
fn configure_and_send_share_one_admission_order() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_configure_agent(
        &mut store,
        "configure-lane",
        "codex",
        json!({"model": "m0", "effort": "low"}),
        now,
    );
    let state_of = |store: &Store, id: &str| {
        store
            .get_command(id)
            .expect("load command")
            .expect("command exists")
            .state
    };
    let batch =
        |batch_id: &str, kind: &str, native_id: &str, data: serde_json::Value| SourceBatch {
            batch_id: batch_id.to_owned(),
            agent_id: agent.clone(),
            generation: 1,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                ts: None,
                kind: kind.to_owned(),
                native_id: native_id.to_owned(),
                data,
            }],
            cursor: None,
        };

    // Occupy the Agent with an associated active turn.
    let active = send_request(&agent, 1, "occupying turn");
    let active_id = active.id.clone();
    store
        .submit("owner", active, now + 1)
        .expect("admit occupying send");
    store
        .dispatch(&active_id, now + 2)
        .expect("dispatch occupying send");
    store
        .put_batch(
            batch(
                "configure-lane:occupying",
                "turn.started",
                "configure-lane:occupying:started",
                json!({"command_id": active_id, "turn_id": "lane-turn-0"}),
            ),
            now + 3,
        )
        .expect("associate occupying turn");

    // Admit SEND S, then configure C, on the busy Agent.
    let send = send_request(&agent, 1, "queued input");
    let send_id = send.id.clone();
    store
        .submit("owner", send, now + 4)
        .expect("admit queued send");
    let configure = configure_request(&agent, 1, json!({"model": "m1"}));
    let configure_id = configure.id.clone();
    store
        .submit("owner", configure, now + 5)
        .expect("admit configure");

    // A configure never overtakes an earlier-admitted send: C queues behind
    // the still-queued S instead of applying immediately.
    store
        .dispatch(&configure_id, now + 6)
        .expect("queue configure");
    assert_eq!(
        state_of(&store, &configure_id),
        CommandState::Queued,
        "configure waits for the earlier queued send"
    );

    // Free the Agent; the earlier send dispatches first and freezes the
    // snapshot revision even though a configure is pending.
    store
        .put_batch(
            batch(
                "configure-lane:free",
                "turn.completed",
                "configure-lane:free:completed",
                json!({"command_id": active_id, "turn_id": "lane-turn-0", "status": "completed"}),
            ),
            now + 7,
        )
        .expect("complete occupying turn");
    store
        .dispatch(&send_id, now + 8)
        .expect("dispatch earlier send");
    assert_eq!(
        state_of(&store, &send_id),
        CommandState::Dispatched,
        "earlier send passes the later configure"
    );
    assert!(
        store
            .send_effective_config(&send_id)
            .expect("resolve earlier send")
            .is_none(),
        "earlier send runs under the frozen snapshot"
    );

    // C still waits for S's turn association, then applies as revision 1.
    store
        .dispatch(&configure_id, now + 9)
        .expect("redispatch configure");
    assert_eq!(
        state_of(&store, &configure_id),
        CommandState::Queued,
        "configure waits for the earlier unassociated send"
    );
    store
        .put_batch(
            batch(
                "configure-lane:associated",
                "turn.started",
                "configure-lane:associated:started",
                json!({"command_id": send_id, "turn_id": "lane-turn-1"}),
            ),
            now + 10,
        )
        .expect("associate earlier send");
    store
        .dispatch(&configure_id, now + 11)
        .expect("dispatch configure");
    let applied = store
        .apply_agent_config(&configure_id, now + 12)
        .expect("apply configure");
    assert_eq!(applied.revision, 1);
    store
        .settle(&configure_id, CommandState::Applied, None, now + 12)
        .expect("settle configure");

    // A later input waits for the earlier configure while otherwise ready,
    // then runs under it.
    store
        .put_batch(
            batch(
                "configure-lane:done",
                "turn.completed",
                "configure-lane:done:completed",
                json!({"command_id": send_id, "turn_id": "lane-turn-1", "status": "completed"}),
            ),
            now + 13,
        )
        .expect("complete earlier turn");
    let configure_two = configure_request(&agent, 1, json!({"effort": "high"}));
    let configure_two_id = configure_two.id.clone();
    store
        .submit("owner", configure_two, now + 14)
        .expect("admit second configure");
    store
        .dispatch(&configure_two_id, now + 15)
        .expect("dispatch second configure");
    let later = send_request(&agent, 1, "later input");
    let later_id = later.id.clone();
    store
        .submit("owner", later, now + 16)
        .expect("admit later send");
    store
        .dispatch(&later_id, now + 17)
        .expect("queue later send");
    assert_eq!(
        state_of(&store, &later_id),
        CommandState::Queued,
        "later input waits for the earlier configure"
    );
    let applied_two = store
        .apply_agent_config(&configure_two_id, now + 18)
        .expect("apply second configure");
    assert_eq!(applied_two.revision, 2);
    store
        .settle(&configure_two_id, CommandState::Applied, None, now + 18)
        .expect("settle second configure");
    store
        .dispatch(&later_id, now + 19)
        .expect("dispatch later send");
    assert_eq!(state_of(&store, &later_id), CommandState::Dispatched);
    let effective = store
        .send_effective_config(&later_id)
        .expect("resolve later send")
        .expect("overlay applies");
    assert_eq!(effective.revision, 2, "next input runs under C");
}

#[test]
fn turn_and_generation_readback_show_resolved_native_effort() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    // The profile omits effort while native startup resolves it (e.g. low).
    let agent = ready_configure_agent(
        &mut store,
        "configure-effort",
        "codex",
        json!({"model": "m0"}),
        now,
    );

    // A model-only configure stores no effective effort of its own.
    let configure = configure_request(&agent, 1, json!({"model": "m1"}));
    let configure_id = configure.id.clone();
    store
        .submit("owner", configure, now + 1)
        .expect("admit configure");
    store
        .dispatch(&configure_id, now + 2)
        .expect("dispatch configure");
    let applied = store
        .apply_agent_config(&configure_id, now + 3)
        .expect("apply configure");
    assert_eq!(applied.revision, 1);
    assert_eq!(applied.effort, None, "no effort was requested or retained");
    assert_eq!(applied.requested_effort, None);
    store
        .settle(&configure_id, CommandState::Applied, None, now + 3)
        .expect("settle configure");

    // The next send runs under the overlay; the native request carries the
    // resolved effort, which the plugin reports on the turn association.
    let send = send_request(&agent, 1, "first turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 4).expect("admit send");
    store.dispatch(&send_id, now + 5).expect("dispatch send");
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-effort:turn".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "configure-effort:turn:started".to_owned(),
                    data: json!({"command_id": send_id, "turn_id": "effort-turn", "effort": "low"}),
                }],
                cursor: None,
            },
            now + 6,
        )
        .expect("associate turn");
    let outcome = store
        .get_command(&send_id)
        .expect("load send")
        .expect("send exists")
        .outcome
        .expect("send has an outcome");
    assert_eq!(
        outcome["effort"],
        json!("low"),
        "turn shows the effort actually sent"
    );
    assert_eq!(outcome["model"], json!("m1"));
    let current = store
        .get_agent_config_current(&agent, 1)
        .expect("read overlay")
        .expect("overlay exists");
    assert_eq!(
        current.effort.as_deref(),
        Some("low"),
        "generation readback shows the resolved effort"
    );
    assert_eq!(
        current.requested_effort, None,
        "requested override stays absent"
    );
    let turn = store
        .get_turn(&agent, 1, "effort-turn")
        .expect("load turn")
        .expect("turn exists");
    assert_eq!(turn.config_revision, 1);

    // A later send under the same revision reads back the recorded effort.
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-effort:done".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.completed".to_owned(),
                    native_id: "configure-effort:done:completed".to_owned(),
                    data: json!({"command_id": send_id, "turn_id": "effort-turn", "status": "completed"}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("complete turn");
    let second = send_request(&agent, 1, "second turn");
    let second_id = second.id.clone();
    store
        .submit("owner", second, now + 8)
        .expect("admit second send");
    store
        .dispatch(&second_id, now + 9)
        .expect("dispatch second send");
    let effective = store
        .send_effective_config(&second_id)
        .expect("resolve second send")
        .expect("overlay applies");
    assert_eq!(
        effective.effort.as_deref(),
        Some("low"),
        "recorded effort carries to the next input"
    );
}

#[test]
fn resume_inherits_latest_overlay_as_revision_one_with_provenance() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let snapshot = json!({"model": "m0", "effort": "low"});
    let agent = ready_configure_agent(
        &mut store,
        "configure-resume-inherit",
        "codex",
        snapshot.clone(),
        now,
    );
    let configure = configure_request(&agent, 1, json!({"model": "m1"}));
    let configure_id = configure.id.clone();
    store
        .submit("owner", configure, now + 1)
        .expect("admit configure");
    store
        .dispatch(&configure_id, now + 2)
        .expect("dispatch configure");
    let applied = store
        .apply_agent_config(&configure_id, now + 3)
        .expect("apply configure");
    assert_eq!(applied.revision, 1);
    assert_eq!(applied.provenance, "configure");
    store
        .settle(&configure_id, CommandState::Applied, None, now + 3)
        .expect("settle configure");

    // Retain a native subject, then retire generation 1 through the honest
    // terminal fact so `resume` admits.
    let agent_num = eliot_core::parse_agent_id(&agent).expect("Agent id");
    store
        .connection
        .execute(
            "UPDATE agents SET native_scope = ?1, native_ref = ?2 WHERE id = ?3",
            rusqlite::params![
                "configure-resume-inherit:scope",
                "configure-resume-inherit:thread",
                agent_num
            ],
        )
        .expect("retain a native subject for resume");
    install_live_custody_observations(&mut store, &agent, 1, now + 4);
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-resume-inherit:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "configure-resume-inherit:ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 5,
        )
        .expect("commit terminal session fact");
    simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    let resume_id = resume.id.clone();
    store
        .submit("owner", resume, now + 6)
        .expect("admit resume");
    store
        .dispatch(&resume_id, now + 6)
        .expect("dispatch resume");
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists")
            .generation,
        2,
        "resume starts the next generation"
    );

    // The new generation explicitly inherits the previous overlay as its
    // own revision 1 with `inherited_from` provenance; the frozen snapshot
    // itself is unchanged.
    let inherited = store
        .get_agent_config_current(&agent, 2)
        .expect("read inherited overlay")
        .expect("resume inherits an overlay");
    assert_eq!(inherited.revision, 1);
    assert_eq!(inherited.provenance, "inherited_from");
    assert_eq!(inherited.requested_model.as_deref(), Some("m1"));
    assert_eq!(inherited.model.as_deref(), Some("m1"));
    assert_eq!(inherited.effort.as_deref(), Some("low"));
    assert_eq!(inherited.command_id, resume_id);
    let stored = store
        .get_agent(&agent)
        .expect("load agent")
        .expect("agent exists");
    assert_eq!(
        stored.profile_snapshot, snapshot,
        "snapshot never rewritten"
    );
    let resume_command = store
        .get_command(&resume_id)
        .expect("load resume")
        .expect("resume exists");
    assert_eq!(resume_command.generation, Some(2));
    assert_eq!(
        resume_command.config_revision, 1,
        "resume runs under its inherited revision"
    );

    // The previous generation's overlay is untouched.
    let previous = store
        .get_agent_config(&agent, 1, 1)
        .expect("read previous overlay")
        .expect("previous overlay retained");
    assert_eq!(previous.provenance, "configure");
    assert_eq!(previous.command_id, configure_id);

    // Complete the resume lifecycle so the new generation can take input.
    install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 6);
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-resume-inherit:ready".to_owned(),
                agent_id: agent.clone(),
                generation: 2,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "configure-resume-inherit:ready".to_owned(),
                    data: json!({"command_id": resume_id,
                        "native_scope": "configure-resume-inherit:scope",
                        "native_ref": "configure-resume-inherit:thread"}),
                }],
                cursor: None,
            },
            now + 6,
        )
        .expect("commit resumed session ready");
    store
        .settle(&resume_id, CommandState::Applied, None, now + 6)
        .expect("settle resume");

    // The next send in the new generation runs under the inherited overlay.
    let send = send_request(&agent, 2, "after resume");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 7).expect("admit send");
    store.dispatch(&send_id, now + 8).expect("dispatch send");
    let effective = store
        .send_effective_config(&send_id)
        .expect("resolve send")
        .expect("inherited overlay applies");
    assert_eq!(effective.revision, 1);
    assert_eq!(effective.model.as_deref(), Some("m1"));
}

#[test]
fn retained_config_survives_config_file_reload_for_pending_and_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let snapshot = json!({"model": "m0", "effort": "low"});
    let agent = ready_configure_agent(
        &mut store,
        "configure-reload-proof",
        "codex",
        snapshot.clone(),
        now,
    );
    let configure = configure_request(&agent, 1, json!({"model": "m1", "effort": "high"}));
    let configure_id = configure.id.clone();
    store
        .submit("owner", configure, now + 1)
        .expect("admit configure");
    store
        .dispatch(&configure_id, now + 2)
        .expect("dispatch configure");
    store
        .apply_agent_config(&configure_id, now + 3)
        .expect("apply configure");
    store
        .settle(&configure_id, CommandState::Applied, None, now + 3)
        .expect("settle configure");

    // A first send dispatched under the overlay, plus a pending send admitted
    // while its turn is still unassociated.
    let first = send_request(&agent, 1, "first");
    let first_id = first.id.clone();
    store.submit("owner", first, now + 4).expect("admit first");
    store.dispatch(&first_id, now + 5).expect("dispatch first");
    let pending = send_request(&agent, 1, "pending");
    let pending_id = pending.id.clone();
    store
        .submit("owner", pending, now + 6)
        .expect("admit pending");

    // A config-file reload edits the profile: a new launch resolves the
    // edited snapshot, while the retained generation keeps its frozen one.
    let reloaded_snapshot = json!({"model": "m-edited", "effort": "low"});
    let launch_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":"configure-reload-proof-new", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":reloaded_snapshot.clone()}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 7,
        )
        .expect("admit reloaded launch");
    let reloaded_target = store
        .get_command(&launch_id)
        .expect("load reloaded launch")
        .expect("launch exists")
        .target
        .expect("launch target");
    let reloaded_row = store
        .get_agent(&reloaded_target)
        .expect("load reloaded agent")
        .expect("agent exists");
    assert_eq!(
        reloaded_row.profile_snapshot, reloaded_snapshot,
        "new launches resolve the reloaded profile"
    );

    let retained = store
        .get_agent(&agent)
        .expect("load retained agent")
        .expect("agent exists");
    assert_eq!(
        retained.profile_snapshot, snapshot,
        "reload never rewrites a retained generation"
    );
    assert_eq!(
        store
            .send_effective_config(&first_id)
            .expect("resolve first")
            .expect("overlay applies")
            .model
            .as_deref(),
        Some("m1"),
        "dispatched send keeps its original configuration"
    );

    // The pending send dispatches after the reload and still runs under the
    // retained overlay, not the reloaded profile. Complete the first turn
    // first so the send lane frees; the lane holds a configure-pending send
    // on its unassociated input.
    store
        .put_batch(
            SourceBatch {
                batch_id: "configure-reload-proof:turn".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-reload-proof:turn:started".to_owned(),
                        data: json!({"command_id": first_id, "turn_id": "reload-turn"}),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "message.completed".to_owned(),
                        native_id: "configure-reload-proof:turn:message".to_owned(),
                        data: json!({"command_id": first_id, "turn_id": "reload-turn", "text": "first reply"}),
                    },
                    NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: "configure-reload-proof:turn:completed".to_owned(),
                        data: json!({"command_id": first_id, "turn_id": "reload-turn", "status": "completed"}),
                    },
                ],
                cursor: None,
            },
            now + 8,
        )
        .expect("complete first turn");
    store
        .dispatch(&pending_id, now + 9)
        .expect("dispatch pending");
    let pending_command = store
        .get_command(&pending_id)
        .expect("load pending")
        .expect("send exists");
    assert_eq!(
        pending_command.config_revision, 1,
        "pending send dispatched under its retained revision"
    );
    assert_eq!(
        store
            .command_effective_config(&pending_id, &["send"])
            .expect("resolve pending")
            .expect("overlay applies")
            .model
            .as_deref(),
        Some("m1"),
        "pending command still reads back its original configuration"
    );

    // An unknown outcome still resolves to the retained configuration: the
    // revision stamped at dispatch is read back, never the reloaded file.
    store
        .recover_on_restart(now + 10)
        .expect("fence dispatched send as unknown");
    let unknown = store
        .get_command(&pending_id)
        .expect("load unknown")
        .expect("send exists");
    assert_eq!(unknown.state, CommandState::Unknown);
    assert_eq!(unknown.config_revision, 1);
    assert_eq!(
        store
            .command_effective_config(&pending_id, &["send"])
            .expect("resolve unknown")
            .expect("overlay applies")
            .model
            .as_deref(),
        Some("m1"),
        "unknown-outcome resolution keeps the original configuration"
    );
    assert_eq!(
        store
            .turn_effective_config(&agent, 1, "reload-turn")
            .expect("resolve turn")
            .expect("turn overlay retained")
            .model
            .as_deref(),
        Some("m1"),
        "retained turn keeps its original configuration"
    );
}

#[test]
fn retained_command_and_turn_views_survive_later_revisions() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let snapshot = json!({"model": "m0", "effort": "low"});
    let agent = ready_configure_agent(
        &mut store,
        "configure-view-history",
        "codex",
        snapshot.clone(),
        now,
    );

    // Revision 1, then a send and a completed turn under it.
    let first_configure = configure_request(&agent, 1, json!({"model": "m1"}));
    let first_configure_id = first_configure.id.clone();
    store
        .submit("owner", first_configure, now + 1)
        .expect("admit first configure");
    store
        .dispatch(&first_configure_id, now + 2)
        .expect("dispatch first configure");
    store
        .apply_agent_config(&first_configure_id, now + 3)
        .expect("apply first configure");
    store
        .settle(&first_configure_id, CommandState::Applied, None, now + 3)
        .expect("settle first configure");

    let first = send_request(&agent, 1, "first turn");
    let first_id = first.id.clone();
    store
        .submit("owner", first, now + 4)
        .expect("admit first send");
    store
        .dispatch(&first_id, now + 5)
        .expect("dispatch first send");
    for (kind, native_id, data) in [
        (
            "turn.started",
            "configure-view-history:turn:started",
            json!({"command_id": first_id, "turn_id": "view-turn"}),
        ),
        (
            "message.completed",
            "configure-view-history:turn:message",
            json!({"command_id": first_id, "turn_id": "view-turn", "text": "first reply"}),
        ),
        (
            "turn.completed",
            "configure-view-history:turn:completed",
            json!({"command_id": first_id, "turn_id": "view-turn", "status": "completed"}),
        ),
    ] {
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("configure-view-history:{native_id}"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: kind.to_owned(),
                        native_id: native_id.to_owned(),
                        data,
                    }],
                    cursor: None,
                },
                now + 6,
            )
            .expect("commit first turn event");
    }

    let agent_row = store
        .get_agent(&agent)
        .expect("load agent")
        .expect("agent exists");
    let first_command = store
        .get_command(&first_id)
        .expect("load first send")
        .expect("send exists");
    let first_view = store
        .command_config_view(&first_command)
        .expect("resolve first command view")
        .expect("revision 1 view exists");
    assert_eq!(first_view.revision, 1);
    assert_eq!(first_view.provenance, "configure");
    assert_eq!(first_view.profile, "fake");
    assert_eq!(first_view.requested_model.as_deref(), Some("m1"));
    assert_eq!(first_view.model.as_deref(), Some("m1"));
    assert_eq!(first_view.effort.as_deref(), Some("low"));
    assert_eq!(first_view.original_model.as_deref(), Some("m0"));

    let first_turn = store
        .get_turn(&agent, 1, "view-turn")
        .expect("load turn")
        .expect("turn exists");
    assert_eq!(first_turn.config_revision, 1);
    let turn_view = store
        .turn_config_view(&agent_row, &first_turn)
        .expect("resolve turn view")
        .expect("revision 1 turn view exists");
    assert_eq!(turn_view, first_view);
    assert!(!turn_view.profile_inherited);

    // Revision 2 applies afterwards: the retained revision-1 command and
    // turn still read back revision 1, while a new send reads revision 2.
    let second_configure = configure_request(&agent, 1, json!({"model": "m2"}));
    let second_configure_id = second_configure.id.clone();
    store
        .submit("owner", second_configure, now + 7)
        .expect("admit second configure");
    store
        .dispatch(&second_configure_id, now + 8)
        .expect("dispatch second configure");
    store
        .apply_agent_config(&second_configure_id, now + 9)
        .expect("apply second configure");
    store
        .settle(&second_configure_id, CommandState::Applied, None, now + 9)
        .expect("settle second configure");

    let retained_command = store
        .get_command(&first_id)
        .expect("reload first send")
        .expect("send exists");
    assert_eq!(
        store
            .command_config_view(&retained_command)
            .expect("resolve retained command view")
            .expect("revision 1 view survives"),
        first_view,
        "later revisions never rewrite a retained command view"
    );
    let retained_turn = store
        .get_turn(&agent, 1, "view-turn")
        .expect("reload turn")
        .expect("turn exists");
    assert_eq!(
        store
            .turn_config_view(&agent_row, &retained_turn)
            .expect("resolve retained turn view")
            .expect("revision 1 turn view survives"),
        first_view,
        "later revisions never rewrite a retained turn view"
    );

    let second = send_request(&agent, 1, "second turn");
    let second_id = second.id.clone();
    store
        .submit("owner", second, now + 10)
        .expect("admit second send");
    store
        .dispatch(&second_id, now + 11)
        .expect("dispatch second send");
    let second_command = store
        .get_command(&second_id)
        .expect("load second send")
        .expect("send exists");
    let second_view = store
        .command_config_view(&second_command)
        .expect("resolve second command view")
        .expect("revision 2 view exists");
    assert_eq!(second_view.revision, 2);
    assert_eq!(second_view.model.as_deref(), Some("m2"));
    assert_eq!(second_view.profile, "fake");

    // Commands without a native configuration expose no view.
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    stop.expected_generation = Some(1);
    let stop_id = stop.id.clone();
    store.submit("owner", stop, now + 12).expect("admit stop");
    let stop_command = store
        .get_command(&stop_id)
        .expect("load stop")
        .expect("stop exists");
    assert_eq!(
        store
            .command_config_view(&stop_command)
            .expect("resolve stop view"),
        None,
        "non-native commands expose no configuration view"
    );
}
