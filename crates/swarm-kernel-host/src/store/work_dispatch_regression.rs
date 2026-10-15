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
use swarm_contracts::{
    module_catalog::{
        ActivationPolicy, ArtifactId, ArtifactVersion, LaunchSpec, LifecycleOwnership, ModuleId,
        ProtectedRef, RestartPolicy, Sha256Digest,
    },
    module_contract::ModuleContractTemplate,
};

#[cfg(test)]
#[path = "work_dispatch_authority_regression.rs"]
mod pr74_remaining;

fn regression_config(directory: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = directory.to_path_buf();
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
    config
}

async fn start_store_with_root(
    root: DataRoot,
    directory: &Path,
    credential: Credential,
) -> (StoreOwner, Principal) {
    let owner = StoreOwner::start(
        root,
        Arc::new(regression_config(directory)),
        credential.clone(),
    )
    .await
    .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    (owner, operator)
}

async fn start_store() -> (StoreOwner, PathBuf, Principal, Credential) {
    let directory = std::env::temp_dir().join(format!(
        "swarm-work-dispatch-regression-{}",
        model::new_id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let (owner, operator) = start_store_with_root(root, &directory, credential.clone()).await;
    (owner, directory, operator, credential)
}

async fn start_store_with_config(
    directory: PathBuf,
    mut config: Config,
) -> (StoreOwner, Principal, Credential) {
    std::fs::create_dir_all(&directory).unwrap();
    config.storage.data_dir = directory.clone();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential.clone()).await.unwrap();
    (owner, operator, credential)
}

async fn write(store: &Store, principal: &Principal, method: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}

fn public_work_dispatch_settings(route_alias: &str) -> Value {
    json!({
        "route":route_alias,
        "agent_profile":"regression-agent",
        "mcp_profile":"work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    })
}

async fn create_public_work_dispatch_task(
    store: &Store,
    operator: &Principal,
    project_id: &str,
    objective: &str,
) -> String {
    let task = write(
        store,
        operator,
        "task.create",
        json!({
            "project_id":project_id,
            "spec":{
                "objective":objective,
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Keep WorkDispatch admission exact"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    task["task_id"].as_str().unwrap().to_owned()
}

async fn apply_public_work_dispatch(
    store: &Store,
    manager: &Principal,
    project_id: &str,
    automation_id: &str,
    route_alias: &str,
) -> Value {
    let changes = json!([{
        "automation_id":automation_id,
        "expected_revision":0,
        "include_existing":true,
        "patch":{
            "enabled":true,
            "steps":["work_dispatch"],
            "work_dispatch":public_work_dispatch_settings(route_alias),
        }
    }]);
    let preview = store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":project_id,"changes":changes}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true, "{preview}");
    let applied = write(
        store,
        manager,
        "automation.config.apply",
        json!({
            "project_id":project_id,
            "changes":changes,
            "preview_digest":preview["plan_sha256"],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true, "{applied}");
    applied
}

async fn public_work_dispatch_effect_snapshot(store: &Store) -> Value {
    store
        .run(|db| {
            let launch_operations: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='swarm.launch'",
                [],
                |row| row.get(0),
            )?;
            let agent_open_operations: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='agent.open'",
                [],
                |row| row.get(0),
            )?;
            let semantic_slots: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key GLOB 'launch:v1:semantic-slot:*'",
                [],
                |row| row.get(0),
            )?;
            let operation_links: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key GLOB 'work-dispatch:v1:operation-link:*'",
                [],
                |row| row.get(0),
            )?;
            let entry_links: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key GLOB 'work-dispatch:v1:entry-operation:*'",
                [],
                |row| row.get(0),
            )?;
            let workspace_leases: i64 =
                db.query_row("SELECT COUNT(*) FROM workspace_leases", [], |row| {
                    row.get(0)
                })?;
            let attempts: i64 =
                db.query_row("SELECT COUNT(*) FROM attempts", [], |row| row.get(0))?;
            let bindings: i64 =
                db.query_row("SELECT COUNT(*) FROM bindings", [], |row| row.get(0))?;
            let native_operations: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method GLOB 'native.*'",
                [],
                |row| row.get(0),
            )?;
            let native_observations: i64 = db.query_row(
                "SELECT COUNT(*) FROM observations WHERE source_stream_id='native' \
                 OR kind GLOB 'native.*' OR kind GLOB 'native_*'",
                [],
                |row| row.get(0),
            )?;
            Ok(json!({
                "launch_operations":launch_operations,
                "agent_open_operations":agent_open_operations,
                "semantic_slots":semantic_slots,
                "operation_links":operation_links,
                "entry_links":entry_links,
                "workspace_leases":workspace_leases,
                "attempts":attempts,
                "bindings":bindings,
                "native_operations":native_operations,
                "native_observations":native_observations,
            }))
        })
        .await
        .unwrap()
}

async fn seed_existing_root_binding(store: &Store, route: Route) {
    let route_json = model::canonical(&serde_json::to_value(&route).unwrap()).unwrap();
    let state_json = model::canonical(&json!({
        "execution":"not_observed",
        "waiting_for":"runtime_adapter",
        "family_completeness":"unknown",
    }))
    .unwrap();
    let module_artifact_id = route.module_artifact_id;
    store
        .run_fixture(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,\
                 state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,?2,'pr64-root-instance',?3,'ready',NULL,NULL,?4,?5,?6)",
                rusqlite::params![
                    "pr64-existing-root-binding",
                    "pr64-existing-root-lane",
                    module_artifact_id,
                    route_json,
                    state_json,
                    model::now_ms()?,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn reconcile_pending_fixture<F>(
    store: &Store,
    owner_manager_id: String,
    state_key: String,
    budget: usize,
    admit: F,
) -> (Value, Value)
where
    F: for<'db> FnMut(
            &rusqlite::Transaction<'db>,
            &super::automation_work_dispatch::PreparedWorkDispatch,
        )
            -> crate::error::Result<super::automation_work_dispatch::WorkDispatchOutcome>
        + Send
        + 'static,
{
    store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let entry = crate::automation::config::load_entry(
                &tx,
                &owner_manager_id,
                "work-dispatch-fairness-project",
                "work-dispatch-fairness",
            )?
            .ok_or_else(|| {
                crate::error::Error::new(
                    "AUTOMATION_WORK_CURSOR_MISSING",
                    "fairness fixture automation entry disappeared",
                )
            })?;
            let result = super::automation_work_dispatch::reconcile_entry(
                &tx,
                &entry,
                entry.work_dispatch.as_ref(),
                budget,
                model::now_ms()?,
                admit,
            )?;
            let state = crate::automation::config::read_record(
                &tx,
                &state_key,
                "work-dispatch fairness fixture state",
            )?
            .ok_or_else(|| {
                crate::error::Error::new(
                    "AUTOMATION_WORK_CURSOR_MISSING",
                    "fairness fixture state disappeared after reconciliation",
                )
            })?;
            tx.commit()?;
            Ok((result, state))
        })
        .await
        .unwrap()
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    register_manager_with_credential(store, operator, client_id)
        .await
        .0
}

async fn register_manager_with_credential(
    store: &Store,
    operator: &Principal,
    client_id: &str,
) -> (Principal, Credential) {
    let token = format!("work-dispatch-test-{client_id}-{}", model::new_id());
    let credential = Credential {
        client_id: client_id.to_owned(),
        token,
    };
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(credential.token.as_bytes()),
        }),
    )
    .await;
    let principal = store
        .authenticate(Credential {
            client_id: credential.client_id.clone(),
            token: credential.token.clone(),
        })
        .await
        .unwrap();
    (principal, credential)
}

async fn select_regression_descriptor(store: &Store, manager: &Principal, route_alias: &str) {
    let mut template = ModuleContractTemplate::codex_rust_controller_v5().unwrap();
    template.module_id = ModuleId::new("regression-runtime").unwrap();
    template.artifact.artifact_id = ArtifactId::new("regression-artifact").unwrap();
    template.artifact.version = ArtifactVersion::new("5").unwrap();
    let descriptor = template
        .descriptor(
            LaunchSpec {
                executable: if cfg!(windows) {
                    PathBuf::from(r"C:\modules\regression.exe")
                } else {
                    PathBuf::from("/modules/regression")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: Some(ProtectedRef::new("credential:regression-fixture").unwrap()),
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: Some(Sha256Digest::new("d".repeat(64)).unwrap()),
            },
            LifecycleOwnership::OwnedService,
            ActivationPolicy::OnDemand,
            true,
            RestartPolicy::default(),
        )
        .unwrap();
    let registered = store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let registered = super::module_handshake::register_trusted_descriptor(&tx, descriptor)?;
            tx.commit()?;
            Ok(registered)
        })
        .await
        .unwrap();
    write(
        store,
        manager,
        "module.route.select",
        json!({
            "route_alias":route_alias,
            "module_id":"regression-runtime",
            "artifact_id":"regression-artifact",
            "version":"5",
            "expected_catalog_revision":registered["catalog_revision"],
        }),
    )
    .await;
}

async fn seed_unknown_launch(
    store: &Store,
    operator: &Principal,
    manager: &Principal,
    project_id: &str,
    route_alias: &str,
    ordinal: usize,
) -> String {
    let task = write(
        store,
        operator,
        "task.create",
        json!({
            "project_id":project_id,
            "spec":{
                "objective":format!("Retain bounded launch candidate {ordinal}"),
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Keep this unrelated launch independently valid"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let mut request = json!({
        "task_id":task_id,
        "expected_task_revision":1,
        "route":route_alias,
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
    let preview = store
        .call(
            manager.clone(),
            "swarm.launch.preview".to_owned(),
            request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(preview["hard_blocks"], json!([]), "{preview}");
    request["client_request_id"] = json!(format!("launch-fairness-{ordinal}-{}", model::new_id()));
    request["plan_digest"] = preview["plan_digest"].clone();
    let receipt = store
        .call(manager.clone(), "swarm.launch".to_owned(), request)
        .await
        .unwrap();
    assert_eq!(receipt["launch_state"], "pending_workspace", "{receipt}");
    let operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
    let failure_id = operation_id.clone();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::launcher::fail_launch(
                &tx,
                &failure_id,
                "workspace_effect_unknown",
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    operation_id
}

/// Seed a pending-workspace launch that stays route-held: its isolated
/// hold scope is pre-damaged (`capacity:<hold-scope>` = `{}`), so every
/// examination retains admission (refreshing `updated_at_ms`) and holds
/// without progress or leases. `created_at_ms` never moves, so an immutable
/// `(created_at_ms, operation_id)` cursor still reaches later launches.
async fn seed_hold_launch(
    store: &Store,
    operator: &Principal,
    manager: &Principal,
    project_id: &str,
    route_alias: &str,
    ordinal: usize,
) -> String {
    let task = write(
        store,
        operator,
        "task.create",
        json!({
            "project_id":project_id,
            "spec":{
                "objective":format!("Retain route-held launch candidate {ordinal}"),
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Hold without progress while keeping fair order"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let mut request = json!({
        "task_id":task_id,
        "expected_task_revision":1,
        "route":route_alias,
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
    let preview = store
        .call(
            manager.clone(),
            "swarm.launch.preview".to_owned(),
            request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(preview["hard_blocks"], json!([]), "{preview}");
    request["client_request_id"] = json!(format!("launch-hold-{ordinal}-{}", model::new_id()));
    request["plan_digest"] = preview["plan_digest"].clone();
    let receipt = store
        .call(manager.clone(), "swarm.launch".to_owned(), request)
        .await
        .unwrap();
    assert_eq!(receipt["launch_state"], "pending_workspace", "{receipt}");
    receipt["operation_id"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn work_dispatch_and_manual_launch_share_one_immutable_manager_scoped_slot() {
    let (owner, directory, operator, _credential) = start_store().await;
    let manager = register_manager(&owner.store, &operator, "work-manager").await;
    let outsider = register_manager(&owner.store, &operator, "other-manager").await;

    // Automatic admission requires the same exact trusted route selection as
    // manual launch. This fixture never executes the descriptor's placeholder.
    select_regression_descriptor(&owner.store, &manager, "regression-route").await;

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
        .unwrap_or_else(|| panic!("the committed task.create fact is consumed: {applied}"));
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

#[tokio::test]
async fn work_dispatch_public_root_limit_refuses_before_reserving_launch_or_effects() {
    const PROJECT_ID: &str = "pr64-public-root-limit-project";

    let directory = std::env::temp_dir().join(format!(
        "swarm-work-dispatch-public-root-limit-{}",
        model::new_id()
    ));
    let mut config = regression_config(&directory);
    config.routes[0].admission_policy = Some(RouteAdmissionPolicy {
        max_concurrent_roots: 1,
    });
    let route = config.routes[0].clone();
    let (owner, operator, _credential) = start_store_with_config(directory.clone(), config).await;
    let manager = register_manager(&owner.store, &operator, "pr64-root-limit-manager").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;
    select_regression_descriptor(&owner.store, &manager, &route.alias).await;
    seed_existing_root_binding(&owner.store, route.clone()).await;

    let route_for_claims = route.clone();
    let claims_before = owner
        .store
        .run(move |db| super::capacity::root_claims_for_route(db, &route_for_claims, None, None))
        .await
        .unwrap();
    assert_eq!(
        claims_before,
        Some(1),
        "fixture is exactly at the configured limit"
    );

    let task_id = create_public_work_dispatch_task(
        &owner.store,
        &operator,
        PROJECT_ID,
        "Refuse public WorkDispatch at the exact route root limit",
    )
    .await;
    let before = public_work_dispatch_effect_snapshot(&owner.store).await;
    let applied = apply_public_work_dispatch(
        &owner.store,
        &manager,
        PROJECT_ID,
        "pr64-public-root-limit",
        &route.alias,
    )
    .await;
    let dispatch = &applied["work_dispatch"][0];
    assert_eq!(dispatch["processed"], 1, "{dispatch}");
    assert_eq!(
        dispatch["pending"].as_array().unwrap().len(),
        1,
        "{dispatch}"
    );
    assert_eq!(dispatch["pending"][0]["task_id"], task_id, "{dispatch}");
    assert_eq!(
        dispatch["pending"][0]["reason"], "ROUTE_ROOT_LIMIT_REACHED",
        "{dispatch}"
    );
    let after = public_work_dispatch_effect_snapshot(&owner.store).await;
    assert_eq!(
        after, before,
        "root-limit refusal must not reserve launch effects"
    );

    let route_for_claims = route.clone();
    let claims_after = owner
        .store
        .run(move |db| super::capacity::root_claims_for_route(db, &route_for_claims, None, None))
        .await
        .unwrap();
    assert_eq!(claims_after, Some(1));

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn work_dispatch_public_disabled_route_refuses_before_reserving_launch_or_effects() {
    const PROJECT_ID: &str = "pr64-public-disabled-route-project";

    let directory = std::env::temp_dir().join(format!(
        "swarm-work-dispatch-public-disabled-route-{}",
        model::new_id()
    ));
    let mut config = regression_config(&directory);
    config.routes[0].enabled = false;
    config.routes[0].admission_policy = Some(RouteAdmissionPolicy {
        max_concurrent_roots: 1,
    });
    let route = config.routes[0].clone();
    let (owner, operator, _credential) = start_store_with_config(directory.clone(), config).await;
    let manager = register_manager(&owner.store, &operator, "pr64-disabled-route-manager").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;

    let task_id = create_public_work_dispatch_task(
        &owner.store,
        &operator,
        PROJECT_ID,
        "Refuse public WorkDispatch for a disabled route",
    )
    .await;
    let before = public_work_dispatch_effect_snapshot(&owner.store).await;
    let applied = apply_public_work_dispatch(
        &owner.store,
        &manager,
        PROJECT_ID,
        "pr64-public-disabled-route",
        &route.alias,
    )
    .await;
    let dispatch = &applied["work_dispatch"][0];
    assert_eq!(dispatch["processed"], 1, "{dispatch}");
    assert_eq!(
        dispatch["pending"].as_array().unwrap().len(),
        1,
        "{dispatch}"
    );
    assert_eq!(dispatch["pending"][0]["task_id"], task_id, "{dispatch}");
    assert_eq!(
        dispatch["pending"][0]["reason"], "route_disabled",
        "{dispatch}"
    );
    let after = public_work_dispatch_effect_snapshot(&owner.store).await;
    assert_eq!(
        after, before,
        "disabled-route refusal must precede launch effects"
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn manual_launch_final_gate_rejects_new_provider_condition_before_workspace_or_binding() {
    let directory = std::env::temp_dir().join(format!(
        "swarm-manual-provider-final-gate-{}",
        model::new_id()
    ));
    let mut config = regression_config(&directory);
    config.routes[0].native_options = json!({
        "service_id":"manual-provider-service",
        "account_id":"provider-account",
        "model":{"id":"m1","providerID":"prov-a","variant":"v"}
    });
    let route = config.routes[0].clone();
    let route_value = serde_json::to_value(&route).unwrap();
    let (owner, operator, _credential) = start_store_with_config(directory.clone(), config).await;
    let manager = register_manager(&owner.store, &operator, "manual-provider-manager").await;
    select_regression_descriptor(&owner.store, &manager, &route.alias).await;
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
            "project_id":"manual-provider-project",
            "spec":{
                "objective":"Reject manual launch when provider state changes after preview",
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Do not reserve workspace or root after provider denial"}],
                "scope":{"initial_paths":["src/target.rs"]},
                "owner_policy_id":OWNER_POLICY_V1_ID,
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let preview_request = json!({
        "task_id":task_id,
        "expected_task_revision":1,
        "route":route.alias,
        "agent_profile":"manual-provider-agent",
        "mcp_profile":"work-participant",
        "mcp_surface":"participant-core",
        "workspace_policy":"manager_owned_worktree",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"implementation",
    });
    let preview = owner
        .store
        .call(
            manager.clone(),
            "swarm.launch.preview".to_owned(),
            preview_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(preview["hard_blocks"], json!([]), "{preview}");
    assert_eq!(
        preview["route"]["admission"]["decision"]["decision"], "admit",
        "the read-only preview sees the initial unobserved condition: {preview}"
    );

    let mut launch_request = preview_request;
    launch_request["client_request_id"] = json!("manual-provider-condition-change");
    launch_request["plan_digest"] = preview["plan_digest"].clone();
    let launch = owner
        .store
        .call(manager.clone(), "swarm.launch".to_owned(), launch_request)
        .await
        .unwrap();
    assert_eq!(launch["launch_state"], "pending_workspace", "{launch}");
    let operation_id = launch["operation_id"].as_str().unwrap().to_owned();

    let source_binding_id = "manual-provider-condition-source";
    let source_lane_id = "manual-provider-condition-source-lane";
    let source_scope_key = "manual-provider-native-scope";
    let source_root_id = "manual-provider-native-root";
    let source_epoch = "manual-provider-source-boot";
    owner
        .store
        .run({
            let route_value = route_value.clone();
            move |db| {
                let tx = db.transaction_with_behavior(
                    rusqlite::TransactionBehavior::Immediate,
                )?;
                let snapshot: swarm_contracts::native_usage::NativeUsageSnapshot =
                    serde_json::from_value(json!({
                        "schema_version":1,
                        "service":"codex",
                        "connection_id":"manual-provider-connection",
                        "auth_context_ref":"b".repeat(64),
                        "ordinary_usage_allowed":null,
                        "collection_issue":null,
                        "collected_at_ms":2_000,
                        "freshness":"current",
                        "completeness":"full",
                        "evidence":{
                            "method":"account/rateLimits/read",
                            "revision":1,
                            "native_observed_at_ms":null
                        },
                        "buckets":[{
                            "id":"primary",
                            "normal_model_slug":{"value":"m1","collected_at_ms":1_700},
                            "provider_condition":{
                                "condition":{"kind":"auth_required"},
                                "collected_at_ms":1_700,
                                "evidence":{
                                    "method":"account/rateLimits/read",
                                    "revision":1,
                                    "native_observed_at_ms":null
                                }
                            }
                        }]
                    }))?;
                let native_usage = json!({"boot_id":source_epoch,"snapshot":snapshot.clone()});
                let state = json!({
                    "execution":"not_observed",
                    "waiting_for":"runtime_adapter",
                    "family_completeness":"unknown",
                    "connection":"connected",
                    "bridge_boot_id":source_epoch,
                    "native_usage":native_usage.clone(),
                    "observed_at_ms":2_000,
                });
                tx.execute(
                    "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                     VALUES(?1,1,?2,'manual-provider-instance',?3,'ready',?4,?5,?6,?7,2000)",
                    rusqlite::params![
                        source_binding_id,
                        source_lane_id,
                        route.module_artifact_id,
                        source_scope_key,
                        source_root_id,
                        model::canonical(&route_value)?,
                        model::canonical(&state)?,
                    ],
                )?;
                let payload = model::canonical(&json!({"native_usage":native_usage}))?;
                tx.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) \
                     VALUES(?1,'manual-provider-condition-v1',?2,1,'runtime.state',?3,2000)",
                    rusqlite::params![
                        format!("runtime-state:{source_binding_id}:1"),
                        source_binding_id,
                        payload,
                    ],
                )?;
                let observation_id = tx.last_insert_rowid();
                let binding = json!({
                    "binding_id":source_binding_id,
                    "generation":1,
                    "route":route_value,
                    "native_scope_key":source_scope_key,
                    "released_at_ms":null,
                    "state":"ready",
                    "observation":{"bridge_boot_id":source_epoch}
                });
                super::provider_conditions::observe_usage(
                    &tx,
                    &binding,
                    &snapshot,
                    1,
                    observation_id,
                )?;
                tx.commit()?;
                Ok(())
            }
        })
        .await
        .unwrap();

    owner.store.reconcile_launches_once().await.unwrap();
    let final_operation = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(final_operation["state"], "settled", "{final_operation}");
    assert_eq!(final_operation["result"]["launch_state"], "blocked");
    assert_eq!(
        final_operation["result"]["failure"]["code"],
        "route_admission_unavailable"
    );
    let operation_id_for_readback = operation_id.clone();
    let task_id_for_readback = task_id.clone();
    let (workspace_leases, attempts, launch_bindings, open_children): (i64, i64, i64, i64) = owner
        .store
        .run(move |db| {
            Ok((
                db.query_row(
                    "SELECT COUNT(*) FROM workspace_leases WHERE operation_id=?1",
                    [&operation_id_for_readback],
                    |row| row.get(0),
                )?,
                db.query_row(
                    "SELECT COUNT(*) FROM attempts WHERE task_id=?1",
                    [&task_id_for_readback],
                    |row| row.get(0),
                )?,
                db.query_row(
                    "SELECT COUNT(*) FROM bindings WHERE lane_id LIKE 'launch-%'",
                    [],
                    |row| row.get(0),
                )?,
                db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='agent.open' AND prerequisite_operation_id=?1",
                    [&operation_id_for_readback],
                    |row| row.get(0),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(workspace_leases, 0);
    assert_eq!(attempts, 0);
    assert_eq!(launch_bindings, 0);
    assert_eq!(open_children, 0);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

fn fixture_git_executable() -> PathBuf {
    let name = if cfg!(windows) { "git.exe" } else { "git" };
    let git = std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .expect("Git executable is available for the configured Forge mapping");
    // Exercise the configured native engine on Windows. The cmd/bin launcher
    // normalizes its own child argv and would hide a verbatim argv[0] defect.
    #[cfg(windows)]
    let git = git
        .parent()
        .and_then(Path::parent)
        .and_then(|root| {
            ["ucrt64", "mingw64"]
                .into_iter()
                .map(|runtime| root.join(runtime).join("bin/git.exe"))
                .find(|candidate| candidate.is_file())
        })
        .unwrap_or(git);
    git.canonicalize()
        .expect("canonical Git executable is available for the configured Forge mapping")
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
    const DUMMY_ROUTE_ALIAS: &str = "workspace-admission-fairness-dummy";
    const SERVICE_ID: &str = "workspace-admission-service";
    const HOLD_ROUTE_ALIAS: &str = "workspace-admission-hold";
    const HOLD_SERVICE_ID: &str = "workspace-admission-hold-service";
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
        crate::platform::private_owned_directory(directory)
            .expect("establish the exact fixture directory owner and private ACL");
    }
    let repository = std::fs::canonicalize(repository).expect("canonicalize fixture repository");
    let workspace_root =
        std::fs::canonicalize(workspace_root).expect("canonicalize fixture workspace root");

    let git = fixture_git_executable();
    eprintln!("workspace_fixture_git_executable={}", git.display());
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
    let hold_route = Route {
        alias: HOLD_ROUTE_ALIAS.to_owned(),
        runtime: "opencode_v2".to_owned(),
        module_artifact_id: "eliot-opencode-v2.http.2".to_owned(),
        enabled: true,
        native_options: json!({
            "service_id":HOLD_SERVICE_ID,
            "connection_file":fixture_root.join("unused-connection.json"),
            "expected_version":"1",
            "directory":repository.clone(),
            "model":{"id":"m1","providerID":"prov-a","variant":"v"}
        }),
        workspace_option: None,
        owned_service: None,
        admission_policy: Some(RouteAdmissionPolicy {
            // Admit all eight fixtures before the explicit damaged-ledger Hold.
            max_concurrent_roots: 8,
        }),
    };
    let hold_route_value = serde_json::to_value(&hold_route).unwrap();
    let hold_scope_key = super::capacity::ScopeKey::from_route(&hold_route_value, "");
    assert_eq!(
        hold_scope_key.as_str(),
        "opencode_v2:workspace-admission-hold-service"
    );
    config.routes.push(hold_route);
    config.routes.push(Route {
        alias: DUMMY_ROUTE_ALIAS.to_owned(),
        runtime: "regression-runtime".to_owned(),
        module_artifact_id: "regression-artifact".to_owned(),
        enabled: true,
        native_options: json!({}),
        workspace_option: None,
        owned_service: None,
        admission_policy: None,
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

    let root = DataRoot::acquire(&data_directory).expect("acquire isolated Store root");
    let credential = bootstrap_credential(&root.path).expect("create local Operator credential");
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config.clone(), credential.clone())
        .await
        .expect("start Store with the exact Forge and workspace mappings");
    let operator = owner
        .store
        .authenticate(credential.clone())
        .await
        .expect("authenticate local Operator");
    owner
        .store
        .initialize_workspace_authority()
        .await
        .expect("retain the configured workspace registration");

    let (manager, manager_credential) =
        register_manager_with_credential(&owner.store, &operator, "workspace-admission-manager")
            .await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;
    select_regression_descriptor(&owner.store, &manager, DUMMY_ROUTE_ALIAS).await;
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

    // Eight route-held launches precede the ninth valid target: the
    // hold ledger is damaged, so every examination retains admission
    // (moving `updated_at_ms`) and holds without progress or leases. The
    // immutable `(created_at_ms, operation_id)` cursor still reaches the
    // ninth valid launch and leaves the hard-error tail unprocessed. The
    // tail remains an exact Unknown/no-ticket readback case on an unrelated
    // route, keeping the target's root-capacity admission independent.
    let mut hold_ids = Vec::with_capacity(8);
    for ordinal in 0..8 {
        hold_ids.push(
            seed_hold_launch(
                &owner.store,
                &operator,
                &manager,
                PROJECT_ID,
                HOLD_ROUTE_ALIAS,
                ordinal,
            )
            .await,
        );
    }
    let tail_id = seed_unknown_launch(
        &owner.store,
        &operator,
        &manager,
        PROJECT_ID,
        DUMMY_ROUTE_ALIAS,
        8,
    )
    .await;
    hold_ids.sort();
    let target_id_for_order = operation_id.clone();
    let tail_id_for_order = tail_id.clone();
    let ordered_hold_ids = hold_ids.clone();
    owner
        .store
        .run(move |db| {
            let base = model::now_ms()?.saturating_sub(100_000);
            for operation_id in ordered_hold_ids {
                db.execute(
                    "UPDATE operations SET created_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",
                    rusqlite::params![operation_id, base],
                )?;
            }
            db.execute(
                "UPDATE operations SET created_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",
                rusqlite::params![target_id_for_order, base.saturating_add(1)],
            )?;
            db.execute(
                "UPDATE operations SET created_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",
                rusqlite::params![tail_id_for_order, 9_000_000_000_000_000_000i64],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Damage the hold scope before any reconciliation: the eight
    // hold launches then retain a Hold admission on every examination.
    let hold_capacity_key = format!("capacity:{}", hold_scope_key.as_str());
    owner
        .store
        .run(move |db| {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,'{}') ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                [hold_capacity_key],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let hold_ids_for_stamps = hold_ids.clone();
    let hold_stamps_before = owner
        .store
        .run(move |db| {
            let mut stamps = Vec::with_capacity(hold_ids_for_stamps.len());
            for operation_id in hold_ids_for_stamps {
                let (created_at_ms, updated_at_ms): (i64, i64) = db.query_row(
                    "SELECT created_at_ms,updated_at_ms FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                stamps.push(json!({"operation_id":operation_id,"created_at_ms":created_at_ms,"updated_at_ms":updated_at_ms}));
            }
            Ok(json!(stamps))
        })
        .await
        .unwrap();

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
    assert_eq!(progress["examined"], 8, "{progress}");
    assert_eq!(progress["progressed"], 0, "{progress}");
    assert_eq!(progress["held"], 8, "{progress}");
    assert_eq!(progress["outcome_unknown"], 0, "{progress}");
    let hold_ids_for_after = hold_ids.clone();
    let hold_stamps_after = owner
        .store
        .run(move |db| {
            let mut stamps = Vec::with_capacity(hold_ids_for_after.len());
            for operation_id in hold_ids_for_after {
                let (created_at_ms, updated_at_ms): (i64, i64) = db.query_row(
                    "SELECT created_at_ms,updated_at_ms FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                stamps.push(json!({"operation_id":operation_id,"created_at_ms":created_at_ms,"updated_at_ms":updated_at_ms}));
            }
            Ok(json!(stamps))
        })
        .await
        .unwrap();
    assert_eq!(
        hold_stamps_before.as_array().unwrap().len(),
        8,
        "{hold_stamps_before}"
    );
    assert_eq!(
        hold_stamps_after.as_array().unwrap().len(),
        8,
        "{hold_stamps_after}"
    );
    for (before, after) in hold_stamps_before
        .as_array()
        .unwrap()
        .iter()
        .zip(hold_stamps_after.as_array().unwrap())
    {
        assert_eq!(
            before["operation_id"], after["operation_id"],
            "{before} {after}"
        );
        assert_eq!(
            before["created_at_ms"], after["created_at_ms"],
            "immutable creation order never moves {before} {after}"
        );
        assert!(
            after["updated_at_ms"].as_i64().unwrap() > before["updated_at_ms"].as_i64().unwrap(),
            "route-held admission refresh must move updated_at_ms {before} {after}"
        );
    }
    let selected_cursor = owner
        .store
        .run(|db| {
            let value = super::meta(db, "launcher:reconcile_launches:v2:cursor")?
                .expect("the considered page persists its cursor");
            Ok((
                value["after"]["operation_id"].as_str().unwrap().to_owned(),
                value["through"]["operation_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            ))
        })
        .await
        .unwrap();
    assert_eq!(
        selected_cursor.0,
        *hold_ids.last().unwrap(),
        "{selected_cursor:?}"
    );
    assert_eq!(
        selected_cursor.1, tail_id,
        "immutable ceiling still reaches the future tail"
    );

    // A malformed ledger makes fleet capacity unknown conservatively, including
    // the target runtime. Remove this first-stage fault before the target's
    // independent post-workspace fault, so the latter actually reaches Git.
    let hold_capacity_key = format!("capacity:{}", hold_scope_key.as_str());
    owner
        .store
        .run(move |db| {
            db.execute("DELETE FROM meta WHERE key=?1", [hold_capacity_key])?;
            Ok(())
        })
        .await
        .unwrap();

    let cursor_failure = format!(
        "CREATE TRIGGER launch_cursor_checkpoint_hard_failure \
         BEFORE INSERT ON meta \
         WHEN NEW.key='launcher:reconcile_launches:v2:cursor' \
           AND json_extract(NEW.value_json,'$.after.operation_id')='{operation_id}' \
         BEGIN SELECT RAISE(ABORT,'fixture cursor checkpoint failure'); END;"
    );
    owner
        .store
        .run(move |db| {
            db.execute_batch(&cursor_failure)?;
            Ok(())
        })
        .await
        .unwrap();
    let hard_failure = owner.store.reconcile_launches_once().await.unwrap_err();
    assert!(!hard_failure.code.is_empty(), "{hard_failure}");
    let operation = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    let diagnostic_id = operation_id.clone();
    let diagnostic = owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [&diagnostic_id],
                |row| row.get(0),
            )?;
            let retained: Value = serde_json::from_str(&raw)?;
            Ok(json!({
                "manifest_state":retained["launch_manifest"]["state"],
                "failure":retained["launch_manifest"]["failure"],
                "progress":retained["launch_manifest"]["progress"],
                "route_admission":retained["launch_manifest"]["current_route_admission"],
                "retained_failure":super::meta(db, &format!("launcher:failure:{diagnostic_id}"))?,
                "workspace_lease":rusqlite::OptionalExtension::optional(db.query_row(
                    "SELECT json_object('state',state,'clean_state',json(clean_state_json),\
                     'attempt_id',attempt_id) FROM workspace_leases WHERE operation_id=?1",
                    [&diagnostic_id],
                    |row| row.get::<_, String>(0),
                ))?.map(|raw| serde_json::from_str::<Value>(&raw)).transpose()?,
            }))
        })
        .await
        .unwrap();
    assert_eq!(
        operation["state"], "queued",
        "checkpoint_error={hard_failure}; retained={diagnostic}"
    );
    assert_eq!(operation["binding_id"], Value::Null);
    assert_eq!(operation["attempt_id"], Value::Null);
    let tail_id_for_check = tail_id.clone();
    let cursor_after_failure = owner
        .store
        .run(move |db| {
            let cursor = super::meta(db, "launcher:reconcile_launches:v2:cursor")?
                .expect("hard checkpoint failure preserves the prior cursor");
            let (state, effective_raw, lease_count): (String, String, i64) = db.query_row(
                "SELECT state,effective_request_json, \
                        (SELECT COUNT(*) FROM workspace_leases WHERE operation_id=?1) \
                 FROM operations WHERE operation_id=?1",
                [&tail_id_for_check],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let effective: Value = serde_json::from_str(&effective_raw)?;
            Ok(json!({
                "after":cursor["after"]["operation_id"],
                "tail_state":state,
                "tail_manifest_state":effective["launch_manifest"]["state"],
                "tail_lease_count":lease_count,
            }))
        })
        .await
        .unwrap();
    assert_eq!(cursor_after_failure["after"], *hold_ids.last().unwrap());
    assert_eq!(cursor_after_failure["tail_state"], "outcome_unknown");
    assert_eq!(
        cursor_after_failure["tail_manifest_state"],
        "outcome_unknown"
    );
    assert_eq!(cursor_after_failure["tail_lease_count"], 0);

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
                [&operation_id_for_readback],
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
                [&operation_id_for_readback],
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

    let capacity_key = format!("capacity:{}", scope_key.as_str());
    owner
        .store
        .run(move |db| {
            db.execute_batch(
                "DROP TRIGGER workspace_admission_capacity_damage; \
                 DROP TRIGGER launch_cursor_checkpoint_hard_failure;",
            )?;
            db.execute("DELETE FROM meta WHERE key=?1", [capacity_key])?;
            Ok(())
        })
        .await
        .unwrap();
    owner.close().await.unwrap();

    // The saved scan ceiling and last considered tuple survive a real Store
    // restart. The next pass resumes at the ninth valid launch and reaches the
    // existing held-lease path, which performs one native open admission.
    let root = DataRoot::acquire(&data_directory).expect("reacquire Store root after close");
    let owner = StoreOwner::start(root, config.clone(), credential)
        .await
        .expect("reopen Store with the same local authority and workspace mapping");
    let manager = owner
        .store
        .authenticate(manager_credential)
        .await
        .expect("reauthenticate the retained launch actor");
    let admission_operation_id = operation_id.clone();
    let admission_config = config.clone();
    let resumed_admission = owner
        .store
        .run(move |db| {
            let admission = super::launcher::current_launch_admission(
                db,
                &admission_operation_id,
                &admission_config,
            )?;
            Ok(serde_json::to_value(admission)?)
        })
        .await
        .unwrap();
    assert_eq!(
        resumed_admission["decision"]["decision"], "admit",
        "fresh route admission before resumed launch: {resumed_admission}"
    );
    assert_eq!(
        resumed_admission["root_limit"]["status"], "within_limit",
        "fresh root capacity before resumed launch: {resumed_admission}"
    );
    assert_eq!(
        resumed_admission["root_limit"]["current_claims"], 0,
        "the already-admitted target excludes its own root claim: {resumed_admission}"
    );
    let resumed = owner.store.reconcile_launches_once().await.unwrap();
    assert_eq!(resumed["examined"], 2, "{resumed}");
    assert_eq!(resumed["progressed"], 1, "{resumed}");
    assert_eq!(resumed["held"], 1, "{resumed}");
    // The unchanged unknown tail is counted in both independent dimensions:
    // it was held this pass and remains persisted as outcome_unknown.
    assert_eq!(resumed["outcome_unknown"], 1, "{resumed}");
    let admitted = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(admitted["state"], "queued", "{admitted}");
    assert!(admitted["attempt_id"].as_str().is_some(), "{admitted}");
    assert!(admitted["binding_id"].as_str().is_some(), "{admitted}");
    let task_id_for_admission = task_id.clone();
    let operation_id_for_admission = operation_id.clone();
    let admitted_counts = owner
        .store
        .run(move |db| {
            let attempts: i64 = db.query_row(
                "SELECT COUNT(*) FROM attempts WHERE task_id=?1",
                [&task_id_for_admission],
                |row| row.get(0),
            )?;
            let bindings: i64 = db.query_row(
                "SELECT COUNT(*) FROM bindings WHERE lane_id=( \
                   SELECT 'launch-' || lease_id FROM workspace_leases WHERE operation_id=?1)",
                [&operation_id_for_admission],
                |row| row.get(0),
            )?;
            let open_children: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='agent.open' \
                 AND prerequisite_operation_id=?1",
                [&operation_id_for_admission],
                |row| row.get(0),
            )?;
            Ok(json!({"attempts":attempts,"bindings":bindings,"open_children":open_children}))
        })
        .await
        .unwrap();
    assert_eq!(admitted_counts["attempts"], 1, "{admitted_counts}");
    assert_eq!(admitted_counts["bindings"], 1, "{admitted_counts}");
    assert_eq!(admitted_counts["open_children"], 1, "{admitted_counts}");

    // Restore the first-stage hold only after the target was admitted. The
    // exhausted scan wraps over those eight retained rows without new work.
    let hold_capacity_key = format!("capacity:{}", hold_scope_key.as_str());
    owner
        .store
        .run(move |db| {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,'{}')",
                [hold_capacity_key],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // A following
    // pass can revisit the admitted launch without creating another child.
    let wrapped = owner.store.reconcile_launches_once().await.unwrap();
    assert_eq!(wrapped["examined"], 8, "{wrapped}");
    assert_eq!(wrapped["held"], 8, "{wrapped}");
    let after_wrap = owner.store.reconcile_launches_once().await.unwrap();
    assert!(
        after_wrap["examined"].as_u64().unwrap() <= 8,
        "{after_wrap}"
    );
    let operation_id_for_wrap = operation_id.clone();
    let open_children_after_wrap = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='agent.open' \
                 AND prerequisite_operation_id=?1",
                [&operation_id_for_wrap],
                |row| row.get::<_, i64>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(open_children_after_wrap, 1);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture_root).unwrap();
}

#[tokio::test]
async fn work_dispatch_pending_rechecks_rotate_across_restart_and_reject_changed_assignment() {
    const PROJECT_ID: &str = "work-dispatch-fairness-project";
    const AUTOMATION_ID: &str = "work-dispatch-fairness";

    let (owner, directory, operator, operator_credential) = start_store().await;
    let (manager, manager_credential) =
        register_manager_with_credential(&owner.store, &operator, "fairness-manager").await;
    select_regression_descriptor(&owner.store, &manager, "regression-route").await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager.client_id}),
    )
    .await;

    let mut subjects = Vec::new();
    for index in 0..9 {
        let task = write(
            &owner.store,
            &operator,
            "task.create",
            json!({
                "project_id":PROJECT_ID,
                "spec":{
                    "objective":format!("Retain WorkDispatch fairness subject {index}"),
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Preserve exact pending subject authority"}],
                    "scope":{"initial_paths":[format!("src/fairness-{index}.rs")]},
                    "owner_policy_id":OWNER_POLICY_V1_ID,
                }
            }),
        )
        .await;
        subjects.push((
            task["task_id"].as_str().unwrap().to_owned(),
            task["operation_id"].as_str().unwrap().to_owned(),
        ));
    }

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
        "automation_id":AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
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
    assert_eq!(preview["valid"], true);
    write(
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

    let state_subjects = subjects.clone();
    let owner_manager_id = manager.client_id.clone();
    let state_key = owner
        .store
        .run(move |db| {
            let tx = db.unchecked_transaction()?;
            let state_key: String = tx.query_row(
                "SELECT key FROM meta \
                 WHERE key LIKE 'automation:v1:work-dispatch:state:%' \
                 ORDER BY key LIMIT 1",
                [],
                |row| row.get(0),
            )?;
            let mut state = crate::automation::config::read_record(
                &tx,
                &state_key,
                "work-dispatch fairness fixture state",
            )?
            .unwrap();
            let mut pending = Vec::with_capacity(state_subjects.len());
            for (task_id, source_operation_id) in state_subjects {
                let observation_id: i64 = tx.query_row(
                    "SELECT observation_id FROM observations \
                     WHERE operation_id=?1 AND source_stream_id='controller' \
                       AND kind='task.create'",
                    [&source_operation_id],
                    |row| row.get(0),
                )?;
                pending.push(json!({
                    "observation_id":observation_id,
                    "source_kind":"task.create",
                    "source_operation_id":source_operation_id,
                    "task_id":task_id,
                    "task_revision":1,
                    "attempt_id":null,
                    "reason":"fixture_initial_pending",
                    "wake_when":["fixture_prerequisite_changed"],
                    "first_seen_at_ms":0,
                    "last_checked_at_ms":0,
                    "held":false,
                }));
            }
            state["pending"] = json!(pending);
            state.as_object_mut().unwrap().remove("recheck_cursor");
            crate::automation::config::write_record(&tx, &state_key, &state)?;
            tx.commit()?;
            Ok((state_key, owner_manager_id))
        })
        .await
        .unwrap();
    let (state_key, owner_manager_id) = state_key;

    let (first_pass, first_state) = reconcile_pending_fixture(
        &owner.store,
        owner_manager_id.clone(),
        state_key.clone(),
        8,
        |_, _| {
            Ok(
                super::automation_work_dispatch::WorkDispatchOutcome::Pending {
                    reason: "fixture_prerequisite_still_pending".to_owned(),
                    wake_when: vec!["fixture_prerequisite_changed".to_owned()],
                },
            )
        },
    )
    .await;
    assert_eq!(first_pass["processed"], 8);
    assert_eq!(first_state["pending"].as_array().unwrap().len(), 9);
    assert_eq!(first_state["recheck_cursor"], 8);
    for pending in &first_state["pending"].as_array().unwrap()[..8] {
        assert!(pending["last_checked_at_ms"].as_i64().unwrap() > 0);
    }
    assert_eq!(first_state["pending"][8]["last_checked_at_ms"], 0);

    owner.close().await.unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let (owner, _operator_after_restart) =
        start_store_with_root(root, &directory, operator_credential).await;
    let manager = owner.store.authenticate(manager_credential).await.unwrap();

    let tail_task_id = subjects[8].0.clone();
    let (second_pass, second_state) = reconcile_pending_fixture(
        &owner.store,
        owner_manager_id.clone(),
        state_key.clone(),
        8,
        move |_, prepared| {
            if prepared.context().subject().task_id() == tail_task_id.as_str() {
                Ok(
                    super::automation_work_dispatch::WorkDispatchOutcome::Admitted {
                        operation_id: "fixture-eligible-tail".to_owned(),
                    },
                )
            } else {
                Ok(
                    super::automation_work_dispatch::WorkDispatchOutcome::Pending {
                        reason: "fixture_prerequisite_still_pending".to_owned(),
                        wake_when: vec!["fixture_prerequisite_changed".to_owned()],
                    },
                )
            }
        },
    )
    .await;
    assert_eq!(second_pass["processed"], 8);
    assert_eq!(second_state["recheck_cursor"], 7);
    assert!(
        second_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item["task_id"] == subjects[8].0 && item["disposition"] == "admitted" })
    );
    assert_eq!(second_state["pending"].as_array().unwrap().len(), 8);
    for (task_id, _) in subjects.iter().take(8) {
        assert!(
            second_state["pending"]
                .as_array()
                .unwrap()
                .iter()
                .any(|pending| pending["task_id"] == *task_id)
        );
    }

    let changed_task_id = subjects[7].0.clone();
    let claim = write(
        &owner.store,
        &manager,
        "task.claim",
        json!({"task_id":changed_task_id.clone(),"expected_revision":1}),
    )
    .await;
    assert_eq!(claim["task_id"].as_str(), Some(changed_task_id.as_str()));
    let (authority_pass, authority_state) = reconcile_pending_fixture(
        &owner.store,
        owner_manager_id,
        state_key,
        1,
        |_, prepared| {
            Ok(
                super::automation_work_dispatch::WorkDispatchOutcome::Admitted {
                    operation_id: format!(
                        "unexpected-stale-admission-{}",
                        prepared.context().subject().task_id()
                    ),
                },
            )
        },
    )
    .await;
    assert_eq!(authority_pass["processed"], 1);
    assert!(
        authority_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| { item["task_id"] == changed_task_id && item["disposition"] == "skipped" })
    );
    assert!(
        !authority_state["recent"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| {
                item["task_id"] == changed_task_id && item["disposition"] == "admitted"
            })
    );
    assert_eq!(authority_state["pending"].as_array().unwrap().len(), 7);
    for (task_id, _) in subjects.iter().take(7) {
        assert!(
            authority_state["pending"]
                .as_array()
                .unwrap()
                .iter()
                .any(|pending| pending["task_id"] == *task_id)
        );
    }

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(test)]
mod detached_preview_authority_qualification {
    use super::*;

    fn public_preview_request(task_id: &str, expected_task_revision: i64) -> Value {
        json!({
            "task_id":task_id,
            "expected_task_revision":expected_task_revision,
            "route":"regression-route",
            "agent_profile":"workspace-admission-agent",
            "mcp_profile":"work-participant",
            "mcp_surface":"participant-core",
            "workspace_policy":"manager_owned_worktree",
            "requested_model":null,
            "requested_effort":null,
            "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
            "stop_conditions":[],
            "purpose":"implementation",
        })
    }

    async fn public_preview(store: &Store, manager: &Principal, request: Value) -> Value {
        store
            .call(manager.clone(), "swarm.launch.preview".to_owned(), request)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn oversized_detached_public_preview_cannot_replace_launch_authority() {
        let (owner, directory, operator, _) = start_store().await;
        let manager = register_manager(&owner.store, &operator, "preview-authority-manager").await;
        select_regression_descriptor(&owner.store, &manager, "regression-route").await;
        write(
            &owner.store,
            &operator,
            "gm.handover",
            json!({"client_id":manager.client_id}),
        )
        .await;

        let mut dependencies = Vec::new();
        for index in 0..64 {
            let phase = format!("phase-{index}-{}", "p".repeat(4_096));
            let dependency = write(
                &owner.store,
                &operator,
                "task.create",
                json!({"project_id":"preview-authority-project","spec":{
                    "objective":"Retained accepted dependency for the preview fixture",
                    "phase":phase,
                    "requirements":[{"id":"R1","statement":"Accepted fixture dependency"}],
                    "owner_policy_id":OWNER_POLICY_V1_ID,
                }}),
            )
            .await;
            let dependency_task_id = dependency["task_id"].as_str().unwrap().to_owned();
            dependencies.push(json!({
                "task_id":dependency_task_id,
                "required_revision":1,
                "required_phase":phase,
            }));
            // The planner consumes retained original acceptance facts. Seed
            // those facts; this fixture does not qualify task.accept itself.
            let caller_id = operator.client_id.clone();
            owner.store.run(move |db| {
                let acceptance_id = model::new_id();
                let result = json!({
                    "outcome":"applied",
                    "acceptance_operation_id":acceptance_id,
                    "task_id":dependency_task_id,
                    "task_revision":1,
                    "phase":phase,
                });
                db.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                     VALUES(?1,?2,?1,'task.accept','{}','{}',?3,'settled',?4,1,1,1,1)",
                    rusqlite::params![acceptance_id,caller_id,dependency_task_id,model::canonical(&result)?],
                )?;
                // A newer Task revision preserves the pinned historical
                // acceptance; no incomplete current acceptance anchor is set.
                db.execute("UPDATE tasks SET revision=2,updated_at_ms=2 WHERE task_id=?1",[dependency_task_id])?;
                Ok(())
            }).await.unwrap();
        }
        let spec = json!({
            "objective":"Keep private launch authority independent of preview size",
            "phase":"implementation",
            "requirements":[{"id":"R1","statement":"Retain exact Task authority"}],
            "dependencies":dependencies.clone(),
            "scope":{"initial_paths":["src/launch-authority.rs"]},
            "owner_policy_id":OWNER_POLICY_V1_ID,
        });
        let task = write(
            &owner.store,
            &operator,
            "task.create",
            json!({"project_id":"preview-authority-project","spec":spec}),
        )
        .await;
        let task_id = task["task_id"].as_str().unwrap().to_owned();
        let original_request = public_preview_request(&task_id, 1);

        let preview = public_preview(&owner.store, &manager, original_request.clone()).await;
        assert_eq!(preview["hard_blocks"], json!([]), "{preview}");
        assert_eq!(preview["authority_projection"], "detached", "{preview}");
        assert_eq!(
            preview["context_detached"]["status"], "detached",
            "{preview}"
        );
        assert!(
            preview["context_detached"]["serialized_bytes"]
                .as_u64()
                .is_some_and(
                    |bytes| bytes as usize > super::super::projection::MAX_SERIALIZED_BYTES
                ),
            "fixture did not exceed the full-preview byte boundary: {preview}"
        );
        assert!(
            model::canonical(&preview).unwrap().len()
                <= super::super::projection::MAX_SERIALIZED_BYTES,
            "detached public preview exceeded its byte boundary"
        );
        for omitted_authority_field in ["candidate_scope", "workspace", "route", "mcp"] {
            assert!(
                preview.get(omitted_authority_field).is_none(),
                "detached presentation unexpectedly included {omitted_authority_field}"
            );
        }

        let preview_task_id = task_id.clone();
        let launch_count_after_preview = owner
            .store
            .run(move |db| {
                db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='swarm.launch' AND task_id=?1",
                    [preview_task_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(
            launch_count_after_preview, 0,
            "preview retained a launch Operation"
        );

        let changed_spec = json!({
            "objective":"Revised source fact for the complete private launch authority",
            "phase":"implementation",
            "requirements":[{
                "id":"R1",
                "statement":"Retain exact Task authority",
            }],
            "dependencies":dependencies,
            "scope":{"initial_paths":["src/launch-authority.rs"]},
            "owner_policy_id":OWNER_POLICY_V1_ID,
        });
        write(
            &owner.store,
            &operator,
            "task.revise",
            json!({"task_id":task_id,"expected_revision":1,"spec":changed_spec}),
        )
        .await;

        let stale_request_id = format!("pr74-stale-preview-{}", model::new_id());
        let mut stale_launch = original_request;
        stale_launch["client_request_id"] = json!(stale_request_id);
        stale_launch["plan_digest"] = preview["plan_digest"].clone();
        let stale_error = owner
            .store
            .call(manager.clone(), "swarm.launch".to_owned(), stale_launch)
            .await
            .expect_err("changed Task authority must invalidate the detached preview digest");
        assert_eq!(stale_error.code, "STALE_LAUNCH_PLAN", "{stale_error}");

        let manager_id = manager.client_id.clone();
        let stale_request_id_for_query = stale_request_id.clone();
        let stale_operation = owner
            .store
            .run(move |db| {
                db.query_row(
                    "SELECT state,task_id,attempt_id,effective_request_json,result_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                    rusqlite::params![manager_id, stale_request_id_for_query],
                    |row| Ok((row.get::<_, String>(0)?,row.get::<_, Option<String>>(1)?,row.get::<_, Option<String>>(2)?,row.get::<_, String>(3)?,row.get::<_, String>(4)?)),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        // Public writes retain their rejected receipt outside the effect
        // savepoint. Digest drift must retain no admitted launch authority.
        assert_eq!(stale_operation.0, "rejected");
        assert_eq!(stale_operation.1, None);
        assert_eq!(stale_operation.2, None);
        let stale_effective: Value = serde_json::from_str(&stale_operation.3).unwrap();
        let stale_result: Value = serde_json::from_str(&stale_operation.4).unwrap();
        assert_eq!(stale_result["code"], "STALE_LAUNCH_PLAN");
        assert_eq!(stale_effective["receipt"]["ok"], false);
        assert!(stale_effective.get("launch_manifest").is_none());
        assert!(stale_effective.get("launch_plan_authority").is_none());

        let fresh_request = public_preview_request(&task_id, 2);
        let fresh_preview = public_preview(&owner.store, &manager, fresh_request.clone()).await;
        assert_eq!(
            fresh_preview["authority_projection"], "detached",
            "{fresh_preview}"
        );
        assert_ne!(fresh_preview["plan_digest"], preview["plan_digest"]);

        let mut launch_request = fresh_request;
        launch_request["client_request_id"] =
            json!(format!("pr74-fresh-preview-{}", model::new_id()));
        launch_request["plan_digest"] = fresh_preview["plan_digest"].clone();
        let receipt = owner
            .store
            .call(manager.clone(), "swarm.launch".to_owned(), launch_request)
            .await
            .unwrap();
        assert_eq!(receipt["launch_state"], "pending_workspace", "{receipt}");

        let operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
        let retained = owner
            .store
            .run(move |db| {
                db.query_row(
                    "SELECT state,task_id,attempt_id,effective_request_json FROM operations WHERE operation_id=?1",
                    [operation_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                        ))
                    },
                )
                .map_err(Into::into)
            })
            .await
            .unwrap();
        assert_eq!(retained.0, "queued");
        assert_eq!(retained.1, task_id);
        let effective: Value = serde_json::from_str(&retained.3).unwrap();
        let manifest = &effective["launch_manifest"];
        let authority = &effective["launch_plan_authority"];

        assert_eq!(manifest["plan_digest"], fresh_preview["plan_digest"]);
        assert_eq!(manifest["request"], authority["requested_plan_facts"]);
        assert_eq!(manifest["runtime"]["route"], authority["route"]);
        assert_eq!(manifest["mcp"], authority["mcp"]);
        assert_eq!(
            manifest["task"]["candidate_scope"],
            authority["candidate_scope"]
        );
        assert_eq!(
            manifest["workspace"]["policy"],
            authority["workspace"]["policy"]
        );
        assert_eq!(manifest["task"]["task_id"], authority["task"]["task_id"]);
        assert_eq!(
            manifest["task"]["project_id"],
            authority["task"]["project_id"]
        );
        assert_eq!(
            manifest["task"]["observed_revision"],
            authority["task"]["revision"]
        );
        assert_eq!(
            manifest["task"]["expected_revision"],
            authority["task"]["expected_revision"]
        );
        assert_eq!(
            manifest["task"]["attempt_action"],
            authority["attempt_action"]
        );
        assert_eq!(
            manifest["task"]["attempt_id"],
            authority["current_attempt"]["attempt_id"]
        );
        assert_eq!(authority["route"]["alias"], "regression-route");
        assert_eq!(authority["mcp"]["profile_name"], "work-participant");
        assert_eq!(authority["workspace"]["policy"], "manager_owned_worktree");
        assert_eq!(
            retained.2,
            manifest["task"]["attempt_id"].as_str().map(str::to_owned)
        );
        assert_eq!(manifest["runtime"]["native_effect"], "not_attempted");

        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
