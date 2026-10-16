use super::StoreOwner;
use crate::{
    config::{Config, Route},
    error::{Error, Result},
    model::{self, Principal, Role},
    platform::{DataRoot, bootstrap_credential},
};
use rusqlite::{Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use swarm_contracts::{
    module_catalog::{ActivationPolicy, LaunchSpec, LifecycleOwnership, RestartPolicy},
    module_contract::ModuleContractTemplate,
};

const MODULE_CLIENT_ID: &str = "scoped-shutdown-module";
const MODULE_LINK_ID: &str = "scoped-shutdown-link";
const PRIMARY_BINDING_ID: &str = "scoped-shutdown-primary";
const PRIMARY_GENERATION: i64 = 4;
const OTHER_BINDING_ID: &str = "scoped-shutdown-other";
const OTHER_GENERATION: i64 = 7;
const UNKNOWN_OPERATION_ID: &str = "scoped-shutdown-old-unknown";

fn codex_route() -> Route {
    Route {
        alias: "scoped-shutdown-codex".to_owned(),
        runtime: "codex".to_owned(),
        module_artifact_id: swarm_contracts::module_contract::CODEX_RUST_CONTROLLER_ARTIFACT_ID
            .to_owned(),
        enabled: true,
        native_options: json!({
            "modelProvider":"fixture",
            "model":"fixture-model",
            "workspaceRoot":if cfg!(windows) { r"C:\workspace" } else { "/workspace" }
        }),
        workspace_option: Some("workspaceRoot".to_owned()),
        admission_policy: None,
        owned_service: None,
    }
}

fn codex_descriptor() -> swarm_contracts::module_catalog::ModuleDescriptor {
    let template = ModuleContractTemplate::codex_rust_controller_v8().unwrap();
    template
        .descriptor(
            LaunchSpec {
                executable: if cfg!(windows) {
                    PathBuf::from(r"C:\modules\codex-rust-controller.exe")
                } else {
                    PathBuf::from("/modules/codex-rust-controller")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: None,
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: None,
            },
            LifecycleOwnership::OwnedService,
            ActivationPolicy::OnDemand,
            true,
            RestartPolicy::default(),
        )
        .unwrap()
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep both exact binding identities explicit at the SQLite fixture boundary."
)]
fn insert_binding(
    tx: &Transaction<'_>,
    binding_id: &str,
    generation: i64,
    lane_id: &str,
    module_instance_id: &str,
    native_scope_key: &str,
    native_root_id: &str,
    route: &Value,
    observation: &Value,
) -> Result<()> {
    tx.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
         VALUES(?1,?2,?3,?4,?5,'ready',?6,?7,?8,?9,1)",
        params![
            binding_id,
            generation,
            lane_id,
            module_instance_id,
            route["module_artifact_id"].as_str().unwrap_or_default(),
            native_scope_key,
            native_root_id,
            model::canonical(route)?,
            model::canonical(observation)?,
        ],
    )?;
    Ok(())
}

fn insert_unknown_operation(tx: &Transaction<'_>, caller_id: &str) -> Result<()> {
    let request = json!({
        "client_request_id":"scoped-shutdown-old-unknown-request",
        "binding_id":PRIMARY_BINDING_ID,
        "generation":PRIMARY_GENERATION,
        "delivery":"next_turn",
        "text":"Previously sent input; application is unknown"
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,sent_at_ms,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'scoped-shutdown-old-unknown-request','agent.send',?3,'{}',?4,?5,'outcome_unknown',3,0,1,1)",
        params![
            UNKNOWN_OPERATION_ID,
            caller_id,
            model::canonical(&request)?,
            PRIMARY_BINDING_ID,
            PRIMARY_GENERATION,
        ],
    )?;
    Ok(())
}

#[tokio::test]
async fn scoped_shutdown_dispatches_while_next_turn_remains_blocked_by_unknown() {
    let directory = std::env::temp_dir().join(format!(
        "swarm-scoped-shutdown-dispatch-{}",
        model::new_id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let route = codex_route();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.routes.push(route.clone());
    let owner = StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let module = Principal {
        link_id: MODULE_LINK_ID.to_owned(),
        client_id: MODULE_CLIENT_ID.to_owned(),
        role: Role::Module,
    };

    let descriptor = codex_descriptor();
    let route_value = serde_json::to_value(&route).unwrap();
    let config_for_fixture = config.clone();
    let operator_for_fixture = operator.clone();
    owner
        .store
        .run_fixture(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let registration =
                super::module_handshake::register_trusted_descriptor(&tx, descriptor.clone())?;
            super::module_handshake::select_route(
                &tx,
                &operator_for_fixture,
                &json!({
                    "route_alias":route.alias.clone(),
                    "module_id":descriptor.module_id,
                    "artifact_id":descriptor.artifact.artifact_id,
                    "version":descriptor.artifact.version,
                    "expected_catalog_revision":registration["catalog_revision"]
                }),
                &config_for_fixture,
                "scoped-shutdown-route-selection",
            )?;
            let selector = super::module_handshake::selection_for_new_binding(
                &tx,
                &operator_for_fixture.client_id,
                &route.alias,
                &route.runtime,
                &route.module_artifact_id,
            )?
            .ok_or_else(|| Error::new("MODULE_CONTRACT_REQUIRED", "Codex selector missing"))?;
            let primary_observation = json!({
                "module_client_id":MODULE_CLIENT_ID,
                "module_link_id":MODULE_LINK_ID,
                "connection":"connected",
                "recovery_required":false,
                "module_contract_selector":selector.clone(),
                "module_contract_selection_owner_manager_id":operator_for_fixture.client_id.clone()
            });
            let other_observation = json!({
                "module_client_id":"scoped-shutdown-other-module",
                "module_link_id":"scoped-shutdown-other-link",
                "connection":"connected",
                "module_contract_selector":selector,
                "module_contract_selection_owner_manager_id":operator_for_fixture.client_id.clone()
            });
            super::set_meta(
                &tx,
                &format!("client:{MODULE_CLIENT_ID}"),
                &json!({
                    "role":"module",
                    "disabled":false,
                    "binding_id":PRIMARY_BINDING_ID,
                    "binding_generation":PRIMARY_GENERATION
                }),
            )?;
            super::set_meta(&tx, "execution_mode", &json!({"new_work":"enabled"}))?;
            insert_binding(
                &tx,
                PRIMARY_BINDING_ID,
                PRIMARY_GENERATION,
                "scoped-shutdown-primary-lane",
                "scoped-shutdown-primary-instance",
                "codex:/fixture/primary",
                "thread_primary",
                &route_value,
                &primary_observation,
            )?;
            insert_binding(
                &tx,
                OTHER_BINDING_ID,
                OTHER_GENERATION,
                "scoped-shutdown-other-lane",
                "scoped-shutdown-other-instance",
                "codex:/fixture/other",
                "thread_other",
                &route_value,
                &other_observation,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let next_turn = owner
        .store
        .call(
            operator.clone(),
            "agent.send".to_owned(),
            json!({
                "client_request_id":"scoped-shutdown-blocked-next-turn",
                "binding_id":PRIMARY_BINDING_ID,
                "generation":PRIMARY_GENERATION,
                "delivery":"next_turn",
                "text":"This next turn must stay queued"
            }),
        )
        .await
        .unwrap();
    let next_turn_id = next_turn["operation_id"].as_str().unwrap().to_owned();

    let unknown_caller = operator.client_id.clone();
    owner
        .store
        .run_fixture(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            insert_unknown_operation(&tx, &unknown_caller)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let shutdown = owner
        .store
        .call(
            operator.clone(),
            "agent.shutdown".to_owned(),
            json!({
                "client_request_id":"scoped-shutdown-primary-request",
                "binding_id":PRIMARY_BINDING_ID,
                "generation":PRIMARY_GENERATION,
                "owned_scope":"owned_sdk_transport"
            }),
        )
        .await
        .unwrap();
    let shutdown_id = shutdown["operation_id"].as_str().unwrap().to_owned();
    let other_shutdown = owner
        .store
        .call(
            operator.clone(),
            "agent.shutdown".to_owned(),
            json!({
                "client_request_id":"scoped-shutdown-other-request",
                "binding_id":OTHER_BINDING_ID,
                "generation":OTHER_GENERATION,
                "owned_scope":"owned_sdk_transport"
            }),
        )
        .await
        .unwrap();
    let other_shutdown_id = other_shutdown["operation_id"].as_str().unwrap().to_owned();

    let dispatch = owner
        .store
        .call(module.clone(), "module.next".to_owned(), json!({}))
        .await
        .unwrap();
    assert_eq!(dispatch["command"]["operation_id"], shutdown_id);
    assert_eq!(dispatch["command"]["method"], "agent.shutdown");
    assert_eq!(dispatch["command"]["binding_id"], PRIMARY_BINDING_ID);
    assert_eq!(dispatch["command"]["generation"], PRIMARY_GENERATION);
    assert_eq!(
        dispatch["command"]["input"]["owned_scope"],
        "owned_sdk_transport"
    );

    let check_module = module.clone();
    let check_config = config.clone();
    let next_dispatch = owner
        .store
        .run_fixture(move |db| super::runtime::next_with_config(db, &check_module, &check_config))
        .await
        .unwrap();
    assert!(next_dispatch["command"].is_null());

    let next_turn_id_for_check = next_turn_id.clone();
    let shutdown_id_for_check = shutdown_id.clone();
    let other_shutdown_id_for_check = other_shutdown_id.clone();
    let after = owner
        .store
        .run_fixture(move |db| {
            let unknown: (String, Option<String>) = db.query_row(
                "SELECT state,result_json FROM operations WHERE operation_id=?1",
                [UNKNOWN_OPERATION_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let next_turn_state: String = db.query_row(
                "SELECT state FROM operations WHERE operation_id=?1",
                [next_turn_id_for_check],
                |row| row.get(0),
            )?;
            let shutdown_state: String = db.query_row(
                "SELECT state FROM operations WHERE operation_id=?1",
                [shutdown_id_for_check],
                |row| row.get(0),
            )?;
            let other_shutdown_state: String = db.query_row(
                "SELECT state FROM operations WHERE operation_id=?1",
                [other_shutdown_id_for_check],
                |row| row.get(0),
            )?;
            let other_binding: (String, i64, Option<i64>) = db.query_row(
                "SELECT state,generation,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
                params![OTHER_BINDING_ID, OTHER_GENERATION],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            Ok(json!({
                "unknown_state":unknown.0,
                "unknown_result":unknown.1,
                "next_turn_state":next_turn_state,
                "shutdown_state":shutdown_state,
                "other_shutdown_state":other_shutdown_state,
                "other_binding_state":other_binding.0,
                "other_generation":other_binding.1,
                "other_released_at_ms":other_binding.2
            }))
        })
        .await
        .unwrap();
    assert_eq!(after["unknown_state"], "outcome_unknown");
    assert!(after["unknown_result"].is_null());
    assert_eq!(after["next_turn_state"], "queued");
    assert_eq!(after["shutdown_state"], "sending");
    assert_eq!(after["other_shutdown_state"], "queued");
    assert_eq!(after["other_binding_state"], "ready");
    assert_eq!(after["other_generation"], OTHER_GENERATION);
    assert!(after["other_released_at_ms"].is_null());

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
