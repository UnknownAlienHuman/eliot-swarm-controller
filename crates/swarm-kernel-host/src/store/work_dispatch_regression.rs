//! Behavioral regression for a WorkDispatch launch sharing the manual launch
//! slot while retaining the original alias request and live manager scope.

use super::{Store, StoreOwner};
use crate::{
    automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
    config::{Config, McpProfileConfig, McpToolProfile, Route, RouteAdmissionPolicy},
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    policy::OWNER_POLICY_V1_ID,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

async fn start_store() -> (StoreOwner, PathBuf, Principal) {
    let directory = std::env::temp_dir().join(format!(
        "swarm-work-dispatch-regression-{}",
        model::new_id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.routes.push(Route {
        admission_policy: Default::default(),
        workspace_option: None,
        owned_service: None,
        alias: "regression-route".to_owned(),
        runtime: "regression-runtime".to_owned(),
        module_artifact_id: "regression-artifact".to_owned(),
        enabled: true,
        native_options: json!({}),
    });
    config.mcp.profiles.insert(
        "work-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    (owner, directory, operator)
}

async fn write(store: &Store, principal: &Principal, method: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("work-dispatch-test-{client_id}-{}", model::new_id());
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(token.as_bytes()),
        }),
    )
    .await;
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn work_dispatch_and_manual_launch_share_one_immutable_manager_scoped_slot() {
    let (owner, directory, operator) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "work-manager").await;
    let outsider = register_manager(&owner.store, &operator, "other-manager").await;

    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;

    // This real settled Task Operation and its immutable observation are the
    // source facts consumed by the actual WorkDispatch entry reconciliation.
    let task = write(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":"regression-project",
            "spec":{
                "objective":"Exercise shared manual and automatic launch admission",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Keep one exact semantic launch slot"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();

    let settings = json!({
        "route":"regression-route",
        "agent_profile":"regression-agent",
        "mcp_profile":"work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    });
    let changes = json!([{
        "automation_id":"work-dispatch-regression",
        "expected_revision":0,
        "include_existing":true,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":settings,
        }
    }]);
    let automation_preview = owner
        .store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":"regression-project","changes":changes}),
        )
        .await
        .unwrap();
    assert_eq!(automation_preview["valid"], true);
    let applied = write(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":"regression-project",
            "changes":changes,
            "preview_digest":automation_preview["plan_sha256"],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true);
    let recent = applied["work_dispatch"][0]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_id"] == task_id)
        .expect("the committed task.create fact is consumed");
    assert_eq!(recent["disposition"], "admitted");
    let operation_id = recent["operation_id"].as_str().unwrap().to_owned();
    let task_source_operation_id = task["operation_id"].as_str().unwrap().to_owned();
    let retained_link = {
        let operation_id = operation_id.clone();
        owner
            .store
            .run(move |db| super::automation_work_dispatch::operation_link(db, &operation_id))
            .await
            .unwrap()
            .expect("automatic admission retains its typed WorkDispatch context")
    };
    assert_eq!(
        retained_link.technical_requester_id,
        AUTOMATION_TECHNICAL_REQUESTER_ID
    );
    assert_eq!(retained_link.effective_manager_id, manager.client_id);
    assert_eq!(retained_link.task_id, task_id);
    assert_eq!(retained_link.action, "swarm.launch");
    assert_eq!(retained_link.source["event_kind"], "task.create");
    assert_eq!(
        retained_link.source["operation_id"],
        task_source_operation_id
    );

    // The retained Operation is on behalf of the registered current Manager.
    let visible = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    // The private retained link pins the technical requester; the Manager's
    // public Operation projection remains Receipt-only.
    assert!(visible.get("caller_id").is_none());
    assert_eq!(visible["task_id"], task_id);
    // Exercise SQL visibility before LIMIT/OFFSET as well as the exact read.
    // A hidden queued launch must not consume the outsider's first page.
    let manager_page = owner
        .store
        .call(
            manager.clone(),
            "operation.list".to_owned(),
            json!({"state":"queued","limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(manager_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(manager_page["items"][0]["operation_id"], operation_id);
    let outsider_page = owner
        .store
        .call(
            outsider.clone(),
            "operation.list".to_owned(),
            json!({"state":"queued","limit":1}),
        )
        .await
        .unwrap();
    assert!(outsider_page["items"].as_array().unwrap().is_empty());
    assert!(
        outsider_page["next_after"]
            .as_i64()
            .is_some_and(|cursor| cursor > 0),
        "{outsider_page}"
    );
    let hidden = owner
        .store
        .call(
            outsider,
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(hidden.code, "NOT_FOUND");

    // A second manual request uses the exact same preview choices. Its
    // caller-owned request ID becomes an immutable alias for the admitted
    // WorkDispatch Operation; the preview remains read-only after admission.
    let mut preview_params = settings.clone();
    preview_params["task_id"] = json!(task_id);
    preview_params["expected_task_revision"] = json!(1);
    let preview = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch.preview".to_owned(),
            preview_params.clone(),
        )
        .await
        .unwrap();
    let mut alias_request = preview_params;
    alias_request["client_request_id"] = json!("manual-alias-regression");
    alias_request["plan_digest"] = preview["plan_digest"].clone();
    let alias_receipt = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch".to_owned(),
            alias_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(alias_receipt["semantic_reuse"], true);
    assert_eq!(alias_receipt["operation_id"], operation_id);

    let exact_retry = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch".to_owned(),
            alias_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(exact_retry, alias_receipt);

    let mut conflicting_retry = alias_request.clone();
    conflicting_retry["purpose"] = json!("changed-purpose");
    let conflict = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch".to_owned(),
            conflicting_retry,
        )
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "REQUEST_ID_CONFLICT");
    let exact_retry_after_conflict = owner
        .store
        .call(manager, "swarm.launch".to_owned(), alias_request)
        .await
        .unwrap();
    assert_eq!(exact_retry_after_conflict, alias_receipt);

    let (launch_count, operation_caller) = owner
        .store
        .run(move |db| {
            let launch_count: i64 = db.query_row(
                "SELECT count(*) FROM operations WHERE method='swarm.launch'",
                [],
                |row| row.get(0),
            )?;
            let operation_caller: String = db.query_row(
                "SELECT caller_id FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            Ok((launch_count, operation_caller))
        })
        .await
        .unwrap();
    assert_eq!(launch_count, 1);
    assert_eq!(operation_caller, AUTOMATION_TECHNICAL_REQUESTER_ID);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

fn fixture_git_executable() -> PathBuf {
    let name = if cfg!(windows) { "git.exe" } else { "git" };
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
        .expect("Git executable is available for the configured Forge mapping")
}

fn run_fixture_git(git: &Path, repository: &Path, hooks: &Path, args: &[&str]) -> String {
    let output = Command::new(git)
        .arg("-c")
        .arg(format!("core.hooksPath={}", hooks.display()))
        .arg("-C")
        .arg(repository)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("run fixture Git command");
    assert!(
        output.status.success(),
        "fixture Git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("fixture Git output is UTF-8")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn work_dispatch_rechecks_root_admission_after_committing_a_verified_workspace() {
    const PROJECT_ID: &str = "workspace-admission-regression";
    const ROUTE_ALIAS: &str = "workspace-admission-opencode";
    const SERVICE_ID: &str = "workspace-admission-service";
    const CANONICAL_REPOSITORY: &str = "github.com/owner/workspace-admission";
    const REMOTE_URL: &str = "https://github.com/owner/workspace-admission.git";

    let fixture_root =
        std::env::temp_dir().join(format!("swarm-workspace-admission-{}", model::new_id()));
    let repository = fixture_root.join("repository");
    let workspace_root = fixture_root.join("workspaces");
    let empty_hooks = fixture_root.join("empty-hooks");
    let data_directory = fixture_root.join("store");
    for directory in [&repository, &workspace_root, &empty_hooks, &data_directory] {
        std::fs::create_dir_all(directory).expect("create the isolated fixture directory");
        crate::platform::private_permissions(directory, true)
            .expect("restrict the fixture directory to its workspace owner");
    }
    let repository = std::fs::canonicalize(repository).expect("canonicalize fixture repository");
    let workspace_root =
        std::fs::canonicalize(workspace_root).expect("canonicalize fixture workspace root");

    let git = fixture_git_executable();
    run_fixture_git(&git, &repository, &empty_hooks, &["init", "--quiet"]);
    run_fixture_git(
        &git,
        &repository,
        &empty_hooks,
        &["config", "user.name", "Workspace Admission Fixture"],
    );
    run_fixture_git(
        &git,
        &repository,
        &empty_hooks,
        &[
            "config",
            "user.email",
            "workspace-admission@example.invalid",
        ],
    );
    std::fs::create_dir_all(repository.join("src")).expect("create fixture source directory");
    std::fs::write(repository.join("src/target.rs"), "pub fn target() {}\n")
        .expect("write the scoped fixture source");
    run_fixture_git(&git, &repository, &empty_hooks, &["add", "src/target.rs"]);
    run_fixture_git(
        &git,
        &repository,
        &empty_hooks,
        &["commit", "--quiet", "-m", "workspace admission fixture"],
    );
    run_fixture_git(&git, &repository, &empty_hooks, &["branch", "-M", "main"]);
    run_fixture_git(
        &git,
        &repository,
        &empty_hooks,
        &["remote", "add", "origin", REMOTE_URL],
    );
    let source_commit = run_fixture_git(
        &git,
        &repository,
        &empty_hooks,
        &["rev-parse", "--verify", "HEAD^{commit}"],
    );

    let route = Route {
        alias: ROUTE_ALIAS.to_owned(),
        runtime: "opencode_v2".to_owned(),
        module_artifact_id: "eliot-opencode-v2.http.2".to_owned(),
        enabled: true,
        native_options: json!({
            "service_id":SERVICE_ID,
            "connection_file":fixture_root.join("unused-connection.json"),
            "expected_version":"1",
            "directory":repository,
            "model":{"id":"m1","providerID":"prov-a","variant":"v"}
        }),
        workspace_option: None,
        owned_service: None,
        admission_policy: Some(RouteAdmissionPolicy {
            max_concurrent_roots: 1,
        }),
    };
    let route_value = serde_json::to_value(&route).unwrap();
    let scope_key = super::capacity::ScopeKey::from_route(&route_value, "");
    assert_eq!(
        scope_key.as_str(),
        "opencode_v2:workspace-admission-service"
    );

    let mut config = Config::default();
    config.storage.data_dir = data_directory.clone();
    config.forge.enabled = true;
    config.forge.git_executable = git;
    config.forge.projects.insert(
        PROJECT_ID.to_owned(),
        crate::forge::ForgeProject {
            canonical_repository: CANONICAL_REPOSITORY.to_owned(),
            repository_path: repository.clone(),
            remote_name: "origin".to_owned(),
            policy_revision: OWNER_POLICY_V1_ID.to_owned(),
            target_refs: vec!["refs/heads/main".to_owned()],
        },
    );
    config.workspace.projects.insert(
        PROJECT_ID.to_owned(),
        crate::workspace::WorkspaceProjectConfig {
            allowed_roots: vec![workspace_root.clone()],
        },
    );
    config.routes.push(route);
    config.mcp.profiles.insert(
        "work-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );

    let root = DataRoot::acquire(&data_directory).expect("acquire isolated Store root");
    let credential = bootstrap_credential(&root.path).expect("create local Operator credential");
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .expect("start Store with the exact Forge and workspace mappings");
    let operator = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate local Operator");
    owner
        .store
        .initialize_workspace_authority()
        .await
        .expect("retain the configured workspace registration");

    let manager = register_manager(&owner.store, &operator, "workspace-admission-manager").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;
    let task = write(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":PROJECT_ID,
            "spec":{
                "objective":"Verify fresh admission after workspace proof",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Hold before any binding when capacity becomes unknown"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let settings = json!({
        "route":ROUTE_ALIAS,
        "agent_profile":"workspace-admission-agent",
        "mcp_profile":"work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    });
    let changes = json!([{
        "automation_id":"workspace-admission-work-dispatch",
        "expected_revision":0,
        "include_existing":true,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":settings,
        }
    }]);
    let preview = owner
        .store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":PROJECT_ID,"changes":changes}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = write(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":PROJECT_ID,
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    let recent = applied["work_dispatch"][0]["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["task_id"] == task_id)
        .expect("the real Task source is consumed by WorkDispatch");
    assert_eq!(recent["disposition"], "admitted", "{recent}");
    let operation_id = recent["operation_id"].as_str().unwrap().to_owned();

    // The trigger fires only when this exact operation's real Git evidence is
    // committed as a held lease. The malformed exact-scope ledger is a
    // deterministic damaged-capacity fault, not a fabricated provider fact.
    let trigger = format!(
        "CREATE TRIGGER workspace_admission_capacity_damage \
         AFTER UPDATE OF state ON workspace_leases \
         WHEN OLD.state='preparing' AND NEW.state='held' \
           AND NEW.operation_id='{operation_id}' \
         BEGIN \
           INSERT INTO meta(key,value_json) \
             VALUES('capacity:{}','{{}}') \
             ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json; \
         END;",
        scope_key.as_str()
    );
    owner
        .store
        .run(move |db| {
            db.execute_batch(&trigger)?;
            Ok(())
        })
        .await
        .unwrap();

    let progress = owner.store.reconcile_launches_once().await.unwrap();
    let operation = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(
        progress["progressed"], 1,
        "progress={progress}; operation={operation}"
    );
    assert_eq!(
        progress["outcome_unknown"], 0,
        "progress={progress}; operation={operation}"
    );
    assert_eq!(operation["state"], "queued");
    assert_eq!(operation["binding_id"], Value::Null);
    assert_eq!(operation["attempt_id"], Value::Null);

    let task_id_for_readback = task_id.clone();
    let operation_id_for_readback = operation_id.clone();
    let scope_key_for_readback = scope_key.as_str().to_owned();
    let retained = owner
        .store
        .run(move |db| {
            let (result_json, effective_request_json): (String, String) = db.query_row(
                "SELECT result_json,effective_request_json FROM operations WHERE operation_id=?1",
                [&operation_id_for_readback],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let result: Value = serde_json::from_str(&result_json)?;
            let effective_request: Value = serde_json::from_str(&effective_request_json)?;
            let result_admission = result["current_route_admission"].clone();
            let effective_admission =
                effective_request["launch_manifest"]["current_route_admission"].clone();
            let (lease_id, state, baseline_commit, workspace_path, clean_state_json, attempt_id): (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
            ) = db.query_row(
                "SELECT lease_id,state,baseline_commit,workspace_path,clean_state_json,attempt_id \
                 FROM workspace_leases WHERE operation_id=?1",
                [&operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )?;
            let clean_state: Value = serde_json::from_str(&clean_state_json)?;
            let attempt_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM attempts WHERE task_id=?1",
                [&task_id_for_readback],
                |row| row.get(0),
            )?;
            let binding_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM bindings WHERE lane_id=?1",
                [format!("launch-{lease_id}")],
                |row| row.get(0),
            )?;
            let open_child_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='agent.open' AND prerequisite_operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            let capacity_raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [format!("capacity:{scope_key_for_readback}")],
                |row| row.get(0),
            )?;
            let (trusted_repository, registered_path): (String, String) = db.query_row(
                "SELECT trusted_repository,repository_path FROM workspace_registrations WHERE project_id=?1",
                [PROJECT_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok(json!({
                "lease_id":lease_id,
                "state":state,
                "baseline_commit":baseline_commit,
                "workspace_path":workspace_path,
                "clean_state":clean_state,
                "lease_attempt_id":attempt_id,
                "result_admission":result_admission,
                "effective_admission":effective_admission,
                "attempt_count":attempt_count,
                "binding_count":binding_count,
                "open_child_count":open_child_count,
                "capacity_raw":capacity_raw,
                "trusted_repository":trusted_repository,
                "registered_path":registered_path,
            }))
        })
        .await
        .unwrap();
    let result_admission = &retained["result_admission"];
    assert_eq!(
        result_admission["decision"]["decision"], "hold",
        "{retained}"
    );
    assert_eq!(
        result_admission["decision"]["code"], "ROUTE_CAPACITY_UNKNOWN",
        "{retained}"
    );
    assert_eq!(
        result_admission["root_limit"]["status"], "capacity_unknown",
        "{retained}"
    );
    assert_eq!(result_admission, &retained["effective_admission"]);
    assert_eq!(retained["state"], "held", "{retained}");
    assert_eq!(retained["baseline_commit"], source_commit);
    assert_eq!(retained["clean_state"]["status"], "verified_clean");
    assert_eq!(retained["clean_state"]["source_head_rechecked"], true);
    assert_eq!(retained["lease_attempt_id"], Value::Null);
    assert_eq!(retained["attempt_count"], 0);
    assert_eq!(retained["binding_count"], 0);
    assert_eq!(retained["open_child_count"], 0);
    assert_eq!(retained["capacity_raw"], "{}");
    assert_eq!(retained["trusted_repository"], CANONICAL_REPOSITORY);
    assert_eq!(
        retained["registered_path"],
        repository.to_string_lossy().as_ref()
    );
    assert!(PathBuf::from(retained["workspace_path"].as_str().unwrap()).is_dir());
    assert!(
        PathBuf::from(retained["workspace_path"].as_str().unwrap()).starts_with(&workspace_root)
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture_root).unwrap();
}
