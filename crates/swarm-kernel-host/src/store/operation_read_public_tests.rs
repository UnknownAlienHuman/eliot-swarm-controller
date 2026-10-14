use super::super::{Store, StoreOwner};
use crate::{
    config::{Config, McpProfileConfig, McpToolProfile, Route, RouteAdmissionPolicy},
    forge::ForgeProject,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    policy::OWNER_POLICY_V1_ID,
    workspace::WorkspaceProjectConfig,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PROJECT: &str = "operation-read-bounded-public";
const OPERATION_POSITION_STREAM: &str = "controller:read-position:operation";
const RELATION_PROJECT: &str = "operation-read-retained-relations";
const RELATION_ROUTE: &str = "operation-read-relation-route";
const SCRIPT_PROJECT: &str = "operation-read-script-relations";
const SCRIPT_ID: &str = "operation_read_relation_script";

struct Fixture {
    owner: StoreOwner,
    directory: PathBuf,
    operator: Principal,
    manager_a: Principal,
    manager_b: Principal,
}

async fn start() -> Fixture {
    let directory = std::env::temp_dir().join(format!("swarm-operation-read-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let manager_a = register_manager(&owner.store, &operator, "operation-read-manager-a").await;
    let manager_b = register_manager(&owner.store, &operator, "operation-read-manager-b").await;
    Fixture {
        owner,
        directory,
        operator,
        manager_a,
        manager_b,
    }
}

fn git_executable() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
        .map(|directory| directory.join(if cfg!(windows) { "git.exe" } else { "git" }))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
        .expect("Git is available for the isolated workspace fixture")
}

async fn start_relation_fixture() -> Fixture {
    let directory = std::env::temp_dir().join(format!(
        "swarm-operation-read-relations-{}",
        model::new_id()
    ));
    let repository = directory.join("repository");
    let workspace_root = directory.join("workspaces");
    let data_directory = directory.join("store");
    for path in [&repository, &workspace_root, &data_directory] {
        std::fs::create_dir_all(path).expect("create isolated relation fixture directory");
    }
    let repository = std::fs::canonicalize(repository).expect("canonicalize fixture repository");
    let workspace_root =
        std::fs::canonicalize(workspace_root).expect("canonicalize fixture workspaces");
    let root = DataRoot::acquire(&data_directory).expect("acquire relation fixture Store root");
    let credential = bootstrap_credential(&root.path).expect("bootstrap local Operator");
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    config.forge.enabled = true;
    config.forge.git_executable = git_executable();
    let connection_file = directory.join("unused-connection.json");
    config.forge.projects.insert(
        RELATION_PROJECT.to_owned(),
        ForgeProject {
            canonical_repository: format!("github.com/owner/{RELATION_PROJECT}"),
            repository_path: repository.clone(),
            remote_name: "origin".to_owned(),
            policy_revision: OWNER_POLICY_V1_ID.to_owned(),
            target_refs: vec!["refs/heads/main".to_owned()],
        },
    );
    config.workspace.projects.insert(
        RELATION_PROJECT.to_owned(),
        WorkspaceProjectConfig {
            allowed_roots: vec![workspace_root],
        },
    );
    config.routes.push(Route {
        admission_policy: Some(RouteAdmissionPolicy {
            max_concurrent_roots: 1,
        }),
        workspace_option: None,
        owned_service: None,
        alias: RELATION_ROUTE.to_owned(),
        runtime: "opencode_v2".to_owned(),
        module_artifact_id: "eliot-opencode-v2.http.2".to_owned(),
        enabled: true,
        native_options: json!({
            "service_id":"operation-read-relation-service",
            "connection_file":connection_file,
            "expected_version":"1",
            "directory":repository,
            "model":{"id":"m1","providerID":"prov-a","variant":"v"},
        }),
    });
    config.mcp.profiles.insert(
        "operation-read-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "operation-read-participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    config.scripts.executor =
        Some(super::super::o6_taskless_path_fixture::store_only_script_executor_pin());
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .expect("start relation fixture Store");
    let operator = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate relation fixture Operator");
    owner
        .store
        .initialize_workspace_authority()
        .await
        .expect("retain the isolated workspace registration");
    let manager_a = register_manager(&owner.store, &operator, "operation-read-relation-a").await;
    let manager_b = register_manager(&owner.store, &operator, "operation-read-relation-b").await;
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":manager_a.client_id,
            }),
        )
        .await
        .expect("handover current-GM authority through Store");
    Fixture {
        owner,
        directory,
        operator,
        manager_a,
        manager_b,
    }
}

fn relation_task_spec(label: &str) -> Value {
    json!({
        "objective":format!("Operation read relation fixture {label}"),
        "phase":"implementation",
        "requirements":[{"id":"R1","statement":"Retain exact operation relations"}],
        "scope":{"initial_paths":["src/target.rs"]},
        "owner_policy_id":OWNER_POLICY_V1_ID,
    })
}

async fn create_relation_task(
    store: &Store,
    manager: &Principal,
    project_id: &str,
    label: &str,
) -> Value {
    call(
        store,
        manager,
        "task.create",
        json!({
            "client_request_id":model::new_id(),
            "project_id":project_id,
            "origin_key":format!("operation-read-relation-{label}-{}", model::new_id()),
            "spec":relation_task_spec(label),
        }),
    )
    .await
}

fn powershell_path() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
        .map(|directory| directory.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" }))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
        .expect("pwsh is available for validating the Store-only ScriptRun fixture")
}

fn relation_script_bundle() -> Value {
    json!({
        "script_id":SCRIPT_ID,
        "interpreter_kind":"powershell",
        "interpreter_path":powershell_path(),
        "entrypoint":"main.ps1",
        "argv":[],
        "trust":"trusted_local",
        "inherit_environment":[],
        "controller_effects":[],
        "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
        "result_schema":{"type":"null"},
        "files":[{"path":"main.ps1","content_base64":STANDARD.encode(b"Write-Output {}")}],
    })
}

impl Fixture {
    async fn close(self) {
        self.owner.close().await.unwrap();
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("operation-read-token-{client_id}");
    store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":client_id,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .unwrap();
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

fn task_spec(label: &str) -> Value {
    let mut spec: Value =
        serde_json::from_str(include_str!("../../config/task.example.json")).unwrap();
    spec["objective"] = json!(format!("Bounded Operation read fixture {label}"));
    spec
}

async fn create(store: &Store, manager: &Principal, label: &str) -> Value {
    store
        .call(
            manager.clone(),
            "task.create".into(),
            json!({
                "client_request_id":model::new_id(),
                "project_id":PROJECT,
                "origin_key":format!("operation-read-{label}"),
                "spec":task_spec(label),
            }),
        )
        .await
        .unwrap()
}

async fn claim(store: &Store, manager: &Principal, task: &Value) -> Value {
    store
        .call(
            manager.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":model::new_id(),
                "task_id":task["task_id"],
                "expected_revision":task["revision"],
                "start_owner":"native_manager",
            }),
        )
        .await
        .unwrap()
}

async fn call(store: &Store, principal: &Principal, method: &str, request: Value) -> Value {
    store
        .call(principal.clone(), method.into(), request)
        .await
        .unwrap_or_else(|error| panic!("{method}: {error:?}"))
}

async fn max_observation_id(store: &Store) -> i64 {
    store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn max_operation_position(store: &Store) -> i64 {
    store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE source_stream_id=?1",
                [OPERATION_POSITION_STREAM],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn operation_positions_after(store: &Store, after: i64, limit: usize) -> Vec<i64> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT observation_id FROM observations WHERE source_stream_id=?1 AND observation_id>?2 ORDER BY observation_id LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![OPERATION_POSITION_STREAM, after, limit as i64],
                |row| row.get(0),
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<i64>>>()?)
        })
        .await
        .unwrap()
}

async fn operation_event_positions_for_prefix(
    store: &Store,
    prefix: String,
    after: i64,
) -> Vec<i64> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT observation.observation_id FROM observations AS observation JOIN operations AS operation ON operation.operation_id=observation.operation_id WHERE operation.client_request_id LIKE ?1 AND observation.observation_id>?2 AND observation.source_stream_id NOT IN ('controller:messages','controller:read-position:operation','controller:read-position:task') ORDER BY observation.observation_id",
            )?;
            let rows = statement.query_map(params![format!("{prefix}-%"), after], |row| {
                row.get(0)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<i64>>>()?)
        })
        .await
        .unwrap()
}

async fn operation_ids_for_prefix(store: &Store, prefix: String) -> Vec<String> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id FROM operations WHERE client_request_id LIKE ?1 ORDER BY client_request_id",
            )?;
            let rows = statement.query_map([format!("{prefix}-%")], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
        })
        .await
        .unwrap()
}

fn assert_no_hidden_identity(
    page: &Value,
    hidden_operation_ids: &[String],
    hidden_prefix: &str,
    hidden_task_id: &str,
    mismatched_attempt_id: &str,
    damaged_operation_id: &str,
) {
    let encoded = model::canonical(page).unwrap();
    for identity in hidden_operation_ids.iter().map(String::as_str).chain([
        hidden_prefix,
        hidden_task_id,
        mismatched_attempt_id,
        damaged_operation_id,
    ]) {
        assert!(
            !encoded.contains(identity),
            "hidden Operation identity escaped a bounded page: {encoded}"
        );
    }
    assert!(page.get("filtered_count").is_none());
}

#[tokio::test]
async fn store_authenticated_operation_lists_and_delta_preserve_bounded_gaps_and_cursor_tail() {
    let f = start().await;
    let store = &f.owner.store;
    let owned_task = create(store, &f.manager_a, "owner").await;
    let owned_attempt = claim(store, &f.manager_a, &owned_task).await;
    let owned_attempt_id = owned_attempt["attempt_id"].as_str().unwrap().to_owned();
    let list_after = max_operation_position(store).await;
    let delta_after = max_observation_id(store).await;
    let scan_limit = super::super::projection::MAX_PROJECTED_ITEMS;
    let hidden_prefix = format!("operation-read-hidden-{}", model::new_id());

    for index in 0..scan_limit {
        let error = store
            .call(
                f.manager_b.clone(),
                "task.claim".into(),
                json!({
                    "client_request_id":format!("{hidden_prefix}-{index}"),
                    "task_id":owned_task["task_id"],
                    "expected_revision":owned_task["revision"],
                    "start_owner":"native_manager",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "TASK_ALREADY_OWNED");
    }

    let hidden_operation_ids = operation_ids_for_prefix(store, hidden_prefix.clone()).await;
    assert_eq!(hidden_operation_ids.len(), scan_limit);
    let hidden_positions = operation_positions_after(store, list_after, scan_limit).await;
    assert_eq!(hidden_positions.len(), scan_limit);
    let expected_list_cursor = *hidden_positions.last().unwrap();
    let hidden_event_positions =
        operation_event_positions_for_prefix(store, hidden_prefix.clone(), delta_after).await;
    assert!(hidden_event_positions.len() >= scan_limit);
    let expected_delta_cursor = hidden_event_positions[scan_limit - 1];

    let damaged_task = create(store, &f.manager_b, "damaged-relation").await;
    let damaged_operation_id = damaged_task["operation_id"].as_str().unwrap().to_owned();
    let damaged_operation_for_update = damaged_operation_id.clone();
    let mismatched_attempt_for_update = owned_attempt_id.clone();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE operations SET attempt_id=?1 WHERE operation_id=?2",
                params![mismatched_attempt_for_update, damaged_operation_for_update],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);

    let later_task = create(store, &f.manager_a, "visible-tail").await;
    let later_operation_id = later_task["operation_id"].as_str().unwrap().to_owned();
    let hidden_task_id = damaged_task["task_id"].as_str().unwrap();

    let first_list = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":list_after,"limit":1}),
    )
    .await;
    assert!(first_list["items"].as_array().unwrap().is_empty());
    assert_eq!(first_list["next_after"], expected_list_cursor);
    assert_eq!(first_list["has_newer"], true);
    assert_eq!(first_list["coverage_complete"], true);
    assert_eq!(first_list["gap_count"], 0);
    assert_no_hidden_identity(
        &first_list,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let first_delta = call(
        store,
        &f.manager_a,
        "report.delta",
        json!({"after":delta_after,"limit":1}),
    )
    .await;
    assert!(first_delta["items"].as_array().unwrap().is_empty());
    assert_eq!(first_delta["next_cursor"], expected_delta_cursor);
    assert_eq!(first_delta["projection"]["has_newer"], true);
    assert_eq!(first_delta["projection"]["coverage_complete"], true);
    assert_eq!(first_delta["projection"]["gap_count"], 0);
    assert!(
        first_delta["projection"]["range"]
            .get("filtered_count")
            .is_none()
    );
    assert_no_hidden_identity(
        &first_delta,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let list_gap = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":first_list["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(list_gap["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        list_gap["items"][0]["gap"]["reason"],
        "retained_relation_damaged"
    );
    assert_eq!(
        list_gap["items"][0]["gap"]["error_code"],
        "OBJECT_SCOPE_DAMAGED"
    );
    assert!(list_gap["items"][0].get("operation_id").is_none());
    assert_eq!(list_gap["coverage_complete"], false);
    assert_eq!(list_gap["gap_count"], 1);
    assert_eq!(list_gap["has_newer"], true);
    assert_eq!(list_gap["next_after"], list_gap["items"][0]["cursor"]);
    assert_no_hidden_identity(
        &list_gap,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let list_tail = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":list_gap["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(list_tail["items"].as_array().unwrap().len(), 1);
    assert_eq!(list_tail["items"][0]["operation_id"], later_operation_id);
    assert_eq!(list_tail["coverage_complete"], true);
    assert_eq!(list_tail["gap_count"], 0);
    assert_no_hidden_identity(
        &list_tail,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let delta_gap = call(
        store,
        &f.manager_a,
        "report.delta",
        json!({"after":first_delta["next_cursor"],"limit":1}),
    )
    .await;
    assert_eq!(delta_gap["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        delta_gap["items"][0]["gap"]["reason"],
        "retained_relation_damaged"
    );
    assert_eq!(
        delta_gap["items"][0]["gap"]["error_code"],
        "OBJECT_SCOPE_DAMAGED"
    );
    assert!(delta_gap["items"][0].get("operation_id").is_none());
    assert!(delta_gap["items"][0].get("payload").is_none());
    assert!(delta_gap["items"][0].get("kind").is_none());
    assert_eq!(delta_gap["projection"]["coverage_complete"], false);
    assert_eq!(delta_gap["projection"]["gap_count"], 1);
    assert_eq!(delta_gap["projection"]["has_newer"], true);
    assert!(
        delta_gap["projection"]["range"]
            .get("filtered_count")
            .is_none()
    );
    assert_eq!(delta_gap["next_cursor"], delta_gap["items"][0]["cursor"]);
    assert_no_hidden_identity(
        &delta_gap,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let mut cursor = delta_gap["next_cursor"].as_i64().unwrap();
    let cursor_for_count = cursor;
    let remaining_observations = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM observations WHERE observation_id>?1",
                [cursor_for_count],
                |row| row.get::<_, i64>(0),
            )? as usize)
        })
        .await
        .unwrap();
    let mut recovered = false;
    for _ in 0..=remaining_observations {
        let page = call(
            store,
            &f.manager_a,
            "report.delta",
            json!({"after":cursor,"limit":1}),
        )
        .await;
        assert_no_hidden_identity(
            &page,
            &hidden_operation_ids,
            &hidden_prefix,
            hidden_task_id,
            &owned_attempt_id,
            &damaged_operation_id,
        );
        if page["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["operation_id"] == later_operation_id)
        {
            assert_eq!(page["projection"]["coverage_complete"], true);
            assert_eq!(page["projection"]["gap_count"], 0);
            recovered = true;
            break;
        }
        let next = page["next_cursor"].as_i64().unwrap();
        assert!(
            next > cursor,
            "delta cursor stopped before the visible tail"
        );
        cursor = next;
    }
    assert!(
        recovered,
        "report.delta cursor skipped the first later visible Operation"
    );
    f.close().await;
}

#[tokio::test]
async fn malformed_public_operation_relations_fail_closed() {
    let f = start_relation_fixture().await;
    let store = &f.owner.store;

    let script_task = create_relation_task(store, &f.manager_b, SCRIPT_PROJECT, "script").await;
    let script_attempt = claim(store, &f.manager_b, &script_task).await;
    call(
        store,
        &f.manager_b,
        "script.register",
        json!({
            "client_request_id":model::new_id(),
            "bundle":relation_script_bundle(),
        }),
    )
    .await;
    call(
        store,
        &f.manager_b,
        "script.activate",
        json!({
            "client_request_id":model::new_id(),
            "script_id":SCRIPT_ID,
            "revision":1,
        }),
    )
    .await;
    let script_run = call(
        store,
        &f.manager_b,
        "script.run",
        json!({
            "client_request_id":model::new_id(),
            "script_id":SCRIPT_ID,
            "expected_script_revision":1,
            "attempt_id":script_attempt["attempt_id"],
            "expected_task_revision":script_task["revision"],
            "input":{},
        }),
    )
    .await;
    let script_operation_id = script_run["operation_id"].as_str().unwrap().to_owned();

    let launch_task = create_relation_task(store, &f.manager_a, RELATION_PROJECT, "launch").await;
    let launch_attempt = claim(store, &f.manager_a, &launch_task).await;
    let task_b = create_relation_task(store, &f.manager_b, SCRIPT_PROJECT, "unrelated").await;
    let task_b_attempt = claim(store, &f.manager_b, &task_b).await;
    let changes = json!([{
        "automation_id":"operation-read-work-dispatch",
        "expected_revision":0,
        "include_existing":true,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":{
                "route":RELATION_ROUTE,
                "agent_profile":"operation-read-agent",
                "mcp_profile":"operation-read-participant",
                "mcp_surface":"participant-core",
                "workspace_policy":"manager_owned_worktree",
                "requested_model":null,
                "requested_effort":null,
                "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
                "stop_conditions":[],
                "purpose":"implementation",
            },
        },
    }]);
    let preview = call(
        store,
        &f.manager_a,
        "automation.config.preview",
        json!({"project_id":RELATION_PROJECT,"changes":changes}),
    )
    .await;
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = call(
        store,
        &f.manager_a,
        "automation.config.apply",
        json!({
            "client_request_id":model::new_id(),
            "project_id":RELATION_PROJECT,
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    let admitted = applied["work_dispatch"][0]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_id"] == launch_task["task_id"])
        .unwrap_or_else(|| panic!("the retained Task source was not dispatched: {applied}"));
    assert_eq!(admitted["disposition"], "admitted");
    let launch_operation_id = admitted["operation_id"].as_str().unwrap().to_owned();

    let launch_for_readback = launch_operation_id.clone();
    let config = f.owner.store.config.clone();
    let (lease_id, link_key, original_link) = store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let actor = super::super::launcher::launch_actor(&tx, &launch_for_readback)?;
            let plan = super::super::launcher::launch_workspace_plan(
                &tx,
                &actor,
                &launch_for_readback,
                &config,
            )?;
            super::super::workspace::reserve_lease_for_launch(
                &tx,
                &actor,
                &plan,
                model::now_ms()?,
            )?;
            let lease: String = tx.query_row(
                "SELECT lease_id FROM workspace_leases WHERE operation_id=?1",
                [&launch_for_readback],
                |row| row.get(0),
            )?;
            let link_json: String = tx.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [format!(
                    "work-dispatch:v1:operation-link:{launch_for_readback}"
                )],
                |row| row.get(0),
            )?;
            tx.commit()?;
            Ok((
                lease,
                format!("work-dispatch:v1:operation-link:{launch_for_readback}"),
                link_json,
            ))
        })
        .await
        .expect("Store admitted the exact workspace lease and retained launch link");

    let parent = call(
        store,
        &f.manager_a,
        "operation.get",
        json!({"operation_id":launch_operation_id}),
    )
    .await;
    assert_eq!(parent["method"], "swarm.launch");
    let lease_for_damage = lease_id.clone();
    let other_manager = f.manager_b.client_id.clone();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE workspace_leases SET owner_client_id=?1 WHERE lease_id=?2",
                params![other_manager, lease_for_damage],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);
    let damaged_workspace = store
        .call(
            f.manager_b.clone(),
            "operation.get".into(),
            json!({"operation_id":launch_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(damaged_workspace.code, "NOT_FOUND");
    let lease_for_restore = lease_id.clone();
    let owner_id = f.manager_a.client_id.clone();
    store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE workspace_leases SET owner_client_id=?1 WHERE lease_id=?2",
                params![owner_id, lease_for_restore],
            )?)
        })
        .await
        .unwrap();

    let changed_link_key = link_key.clone();
    let changed_task_id = task_b["task_id"].as_str().unwrap().to_owned();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE meta SET value_json=json_set(value_json,'$.record.task_id',?1) WHERE key=?2",
                params![changed_task_id, changed_link_key],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);
    let damaged_on_behalf = store
        .call(
            f.manager_a.clone(),
            "operation.get".into(),
            json!({"operation_id":launch_operation_id}),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            damaged_on_behalf.code.as_str(),
            "AUTOMATION_LINK_CORRUPT" | "AUTOMATION_RECORD_CORRUPT"
        ),
        "{damaged_on_behalf:?}"
    );
    let link_key_for_restore = link_key.clone();
    store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE meta SET value_json=?1 WHERE key=?2",
                params![original_link, link_key_for_restore],
            )?)
        })
        .await
        .unwrap();

    let launch_for_damage = launch_operation_id.clone();
    let other_task = task_b["task_id"].as_str().unwrap().to_owned();
    let other_attempt = task_b_attempt["attempt_id"].as_str().unwrap().to_owned();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE operations SET task_id=?1,attempt_id=?2 WHERE operation_id=?3",
                params![other_task, other_attempt, launch_for_damage],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);
    let damaged_launch = store
        .call(
            f.manager_b.clone(),
            "operation.get".into(),
            json!({"operation_id":launch_operation_id}),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            damaged_launch.code.as_str(),
            "AUTOMATION_LINK_CORRUPT" | "AUTOMATION_RECORD_CORRUPT"
        ),
        "{damaged_launch:?}"
    );

    let launch_for_script_damage = script_operation_id.clone();
    let launch_task_id = launch_task["task_id"].as_str().unwrap().to_owned();
    let launch_attempt_id = launch_attempt["attempt_id"].as_str().unwrap().to_owned();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE operations SET task_id=?1,attempt_id=?2 WHERE operation_id=?3",
                params![launch_task_id, launch_attempt_id, launch_for_script_damage],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);
    let damaged_script = store
        .call(
            f.manager_a.clone(),
            "operation.get".into(),
            json!({"operation_id":script_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(damaged_script.code, "OBJECT_SCOPE_DAMAGED");
    f.close().await;
}

#[tokio::test]
async fn script_run_missing_row_cannot_grant_repointed_task_and_caller_replay_survives() {
    let f = start_relation_fixture().await;
    let store = &f.owner.store;
    let manager_c_id = format!("operation-read-damaged-run-target-{}", model::new_id());
    let manager_c = register_manager(store, &f.operator, &manager_c_id).await;
    let caller_task = create_relation_task(store, &f.manager_b, SCRIPT_PROJECT, "run-caller").await;
    let caller_attempt = claim(store, &f.manager_b, &caller_task).await;
    let target_task = create_relation_task(store, &manager_c, SCRIPT_PROJECT, "run-target").await;
    let target_attempt = claim(store, &manager_c, &target_task).await;

    call(
        store,
        &f.manager_b,
        "script.register",
        json!({
            "client_request_id":model::new_id(),
            "bundle":relation_script_bundle(),
        }),
    )
    .await;
    call(
        store,
        &f.manager_b,
        "script.activate",
        json!({
            "client_request_id":model::new_id(),
            "script_id":SCRIPT_ID,
            "revision":1,
        }),
    )
    .await;

    let queued_request = json!({
        "client_request_id":format!("script-run-missing-queued-{}", model::new_id()),
        "script_id":SCRIPT_ID,
        "expected_script_revision":1,
        "attempt_id":caller_attempt["attempt_id"],
        "expected_task_revision":caller_task["revision"],
        "input":{},
    });
    let queued = call(store, &f.manager_b, "script.run", queued_request.clone()).await;
    let settled_request = json!({
        "client_request_id":format!("script-run-missing-settled-{}", model::new_id()),
        "script_id":SCRIPT_ID,
        "expected_script_revision":1,
        "attempt_id":caller_attempt["attempt_id"],
        "expected_task_revision":caller_task["revision"],
        "input":{},
    });
    let settled = call(store, &f.manager_b, "script.run", settled_request).await;
    assert_ne!(queued["operation_id"], settled["operation_id"]);
    assert_ne!(queued["run_id"], settled["run_id"]);

    let queued_operation_id = queued["operation_id"].as_str().unwrap().to_owned();
    let settled_operation_id = settled["operation_id"].as_str().unwrap().to_owned();
    for operation_id in [&queued_operation_id, &settled_operation_id] {
        let error = store
            .call(
                manager_c.clone(),
                "operation.get".into(),
                json!({"operation_id":operation_id}),
            )
            .await
            .expect_err("an unrelated Task owner cannot read the retained ScriptRun");
        assert_eq!(error.code, "NOT_FOUND");
    }

    let queued_for_damage = queued_operation_id.clone();
    let settled_for_damage = settled_operation_id.clone();
    let target_task_id = target_task["task_id"].as_str().unwrap().to_owned();
    let target_attempt_id = target_attempt["attempt_id"].as_str().unwrap().to_owned();
    let settled_at_ms = model::now_ms().unwrap();
    store
        .run(move |db| {
            db.execute(
                "DELETE FROM script_runs WHERE operation_id=?1",
                [&queued_for_damage],
            )?;
            db.execute(
                "DELETE FROM script_runs WHERE operation_id=?1",
                [&settled_for_damage],
            )?;
            db.execute(
                "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
                params![queued_for_damage, target_task_id, target_attempt_id],
            )?;
            db.execute(
                "UPDATE operations SET state='settled',settled_at_ms=?4,task_id=?2,attempt_id=?3 WHERE operation_id=?1",
                params![
                    settled_for_damage,
                    target_task_id,
                    target_attempt_id,
                    settled_at_ms,
                ],
            )?;
            Ok(())
        })
        .await
        .expect("simulate loss of both retained runs and forged Operation scope");

    for (operation_id, state) in [
        (&queued_operation_id, "queued"),
        (&settled_operation_id, "settled"),
    ] {
        let error = store
            .call(
                manager_c.clone(),
                "operation.get".into(),
                json!({"operation_id":operation_id}),
            )
            .await
            .expect_err("missing ScriptRun identity cannot grant Task/Attempt scope");
        assert_eq!(error.code, "OBJECT_SCOPE_DAMAGED", "{state}: {error:?}");

        let caller_receipt = call(
            store,
            &f.manager_b,
            "operation.get",
            json!({"operation_id":operation_id}),
        )
        .await;
        assert_eq!(
            caller_receipt["operation_id"].as_str(),
            Some(operation_id.as_str())
        );
        assert_eq!(caller_receipt["state"], state);
    }

    assert_eq!(
        call(store, &f.manager_b, "script.run", queued_request).await,
        queued,
        "exact caller replay still reuses its bounded retained receipt"
    );
    f.close().await;
}
