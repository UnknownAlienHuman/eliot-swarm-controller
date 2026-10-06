//! Behavioral regression for a WorkDispatch launch sharing the manual launch
//! slot while retaining the original alias request and live manager scope.

use super::{Store, StoreOwner};
use crate::{
    automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
    config::{Config, McpProfileConfig, McpToolProfile, Route},
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    policy::OWNER_POLICY_V1_ID,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

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
    assert_eq!(visible["caller_id"], AUTOMATION_TECHNICAL_REQUESTER_ID);
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
    assert_eq!(outsider_page["next_after"], 0);
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
