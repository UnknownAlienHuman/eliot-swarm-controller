use super::*;
use serde_json::Value;

pub(crate) fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
    ready_agent_with_native_identity(store, project, now, None)
}

pub(crate) fn ready_agent_with_native_identity(
    store: &mut Store,
    project: &str,
    now: i64,
    native_identity: Option<(&str, &str)>,
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
    let data = match native_identity {
        Some((scope, reference)) => json!({
            "command_id": launch_id,
            "native_scope": scope,
            "native_ref": reference,
        }),
        None => json!({"command_id": launch_id}),
    };
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
                    data,
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

pub(super) fn ready_managed_codex_agent(
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

pub(crate) fn send_request(agent: &str, generation: i64, text: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "send");
    request.target = Some(agent.to_owned());
    request.args = json!({"text":text});
    request.expected_generation = Some(generation);
    request
}

pub(crate) fn native_batch(agent: &str, generation: i64, kind: &str, data: Value) -> SourceBatch {
    SourceBatch {
        batch_id: Ulid::new().to_string(),
        agent_id: agent.to_owned(),
        generation,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: kind.to_owned(),
            native_id: Ulid::new().to_string(),
            data,
        }],
        cursor: None,
    }
}

pub(super) fn start_fake_turn(store: &mut Store, agent: &str, now: i64) -> String {
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

pub(super) fn rule_input_with_limit(id: &str, max: u64, workspace: &str) -> RuleDefinitionInput {
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

pub(super) fn targetless_rule_launch_request(
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

pub(super) fn capacity_launch_request(
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

pub(super) fn capacity_generation(store: &Store, agent: &str) -> Option<i64> {
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

pub(super) fn capacity_store_path() -> PathBuf {
    std::env::temp_dir().join(format!("eliot-capacity-{}.sqlite", Ulid::new()))
}

pub(super) fn remove_capacity_store_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let _ = fs::remove_file(PathBuf::from(candidate));
    }
}

pub(super) fn ready_capacity_agent(store: &mut Store, project: &str, now: i64) -> (String, String) {
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
pub(super) fn simulated_plugin_identity(agent: &str, generation: i64) -> ProcessIdentity {
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

pub(super) fn native_store_request() -> SubmitCommand {
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

pub(super) fn native_store_binding() -> NativeStoreBinding {
    NativeStoreBinding {
        home: "/verified-home".to_owned(),
        root: "/verified-store".to_owned(),
        native_scope: "verified-scope".to_owned(),
    }
}

pub(super) fn ready_steer_agent(store: &mut Store, project: &str, now: i64) -> String {
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

/// Launch a `muse`-harness Agent through generation-1 readiness, returning
/// the Agent, its launch command, and its retained native subject. The
/// session stays live; tests end it with `end_muse_session` when they need a
/// terminal old generation.
pub(super) fn ready_muse_agent(
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

pub(super) fn ready_attention_agent(store: &mut Store, project: &str, now: i64) -> String {
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

pub(super) fn insert_process(
    store: &Store,
    boot: &str,
    pid: u32,
    start_time: i64,
    owner: &str,
    state: &str,
) {
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
