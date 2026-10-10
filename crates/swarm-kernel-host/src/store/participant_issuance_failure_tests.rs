use super::*;
use crate::{
    config::{Config, McpProfileConfig, McpToolProfile},
    model::{self, Credential, Principal},
    participant_credentials::{InboundPolicy, IssueRequest, ParticipationBasis},
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use rusqlite::{TransactionBehavior, params};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const MANAGER_ID: &str = "participant-issuance-failure-manager";
const FIRST_LAUNCH_ID: &str = "participant-issuance-failure-launch-one";
const SECOND_LAUNCH_ID: &str = "participant-issuance-failure-launch-two";
const PRIVATE_PATH_MARKER: &str = "participant-issuance-private-path-marker-7319";
const TASK_ID: &str = "issuance-task";
const ATTEMPT_ID: &str = "issuance-attempt";
const BINDING_ID: &str = "issuance-binding";
const PROJECT_ID: &str = "participant-issuance-project";

async fn start_store() -> (StoreOwner, PathBuf, Arc<Config>, Principal) {
    let directory =
        std::env::temp_dir().join(format!("participant-issuance-failure-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();

    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.mcp.profiles.insert(
        "work-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "participant-issuance-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config.clone(), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let token = format!("participant-issuance-test-{}", model::new_id());
    owner
        .store
        .call(
            operator.clone(),
            "client.register".to_owned(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":MANAGER_ID,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .unwrap();
    let manager = owner
        .store
        .authenticate(Credential {
            client_id: MANAGER_ID.to_owned(),
            token,
        })
        .await
        .unwrap();
    owner
        .store
        .call(
            operator,
            "gm.handover".to_owned(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":MANAGER_ID,
            }),
        )
        .await
        .unwrap();
    (owner, directory, config, manager)
}

async fn seed_issuance_subject(store: &Store) {
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,'open','{}',1,1)",
                params![TASK_ID, PROJECT_ID],
            )?;
            let route = json!({
                "alias":"issuance-opencode",
                "runtime":"opencode_v2",
                "native_options":{"service_id":"opencode","model":{"id":"hosted/model","providerID":"opencode-go"}},
            });
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'issuance-lane','issuance-module','eliot-opencode-v2.http.1','ready',?2,'{}',1)",
                params![BINDING_ID, model::canonical(&route)?],
            )?;
            tx.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,'{\"revision\":1}',?3,'controller',?4,1,'running','[]',1,1)",
                params![ATTEMPT_ID, TASK_ID, MANAGER_ID, BINDING_ID],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_launch(store: &Store, operation_id: &'static str) -> String {
    let result = json!({
        "operation_id":operation_id,
        "state":"queued",
        "launch_state":"awaiting_participant_credential",
        "dispatch_permitted":false,
        "task_dispatch":"not_started",
    });
    let effective = json!({
        "launch_manifest":{
            "state":"awaiting_participant_credential",
            "task":{"task_id":TASK_ID,"observed_revision":1,"attempt_id":ATTEMPT_ID},
        }
    });
    let result_json = model::canonical(&result).unwrap();
    let effective_json = model::canonical(&effective).unwrap();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO operations(
                    operation_id,caller_id,client_request_id,method,original_request_json,
                    effective_request_json,task_id,attempt_id,binding_id,binding_generation,
                    state,result_json,due_at_ms,created_at_ms,updated_at_ms
                 ) VALUES(?1,?2,?3,'swarm.launch','{}',?4,?5,?6,?7,1,'queued',?8,1,1,1)",
                params![
                    operation_id,
                    MANAGER_ID,
                    format!("request-{operation_id}"),
                    effective_json,
                    TASK_ID,
                    ATTEMPT_ID,
                    BINDING_ID,
                    result_json,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    model::canonical(&result).unwrap()
}

async fn launch_snapshot(store: &Store, operation_id: &'static str) -> String {
    store
        .run(move |db| {
            db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

async fn launch_result_snapshot(store: &Store, operation_id: &'static str) -> String {
    store
        .run(move |db| {
            db.query_row(
                "SELECT result_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

fn issue_request(operation_id: &str) -> IssueRequest {
    IssueRequest {
        launch_operation_id: operation_id.to_owned(),
        client_request_id: format!("launch:{operation_id}:participant"),
        task_id: TASK_ID.to_owned(),
        task_revision: 1,
        attempt_id: ATTEMPT_ID.to_owned(),
        binding_id: BINDING_ID.to_owned(),
        binding_generation: 1,
        participation_basis: ParticipationBasis::AttemptOwner,
        mcp_profile: "work-participant".to_owned(),
        mcp_surface: "participant-core".to_owned(),
        display_alias: None,
        inbound_policy: Some(InboundPolicy::PullOnly),
        native_session_id: None,
    }
}

async fn private_artifact_failure(
    store: &Store,
    config: &Config,
    manager: &Principal,
    operation_id: &str,
) -> crate::error::Error {
    match crate::participant_credentials::issue_for_launch(
        store,
        crate::store::launcher::LaunchActor::Direct(manager.clone()),
        config,
        issue_request(operation_id),
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("a file at the credential-directory path must reject issuance"),
    }
}

async fn retained_effective(store: &Store, operation_id: &'static str) -> Value {
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| row.get(0),
            )?;
            Ok(serde_json::from_str(&raw)?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn pre_registration_artifact_failure_is_manager_visible_and_cas_bound() {
    let (owner, directory, config, manager) = start_store().await;
    seed_issuance_subject(&owner.store).await;
    std::fs::write(
        directory.join("participant-credentials"),
        PRIVATE_PATH_MARKER,
    )
    .unwrap();

    let first_result_json = seed_launch(&owner.store, FIRST_LAUNCH_ID).await;
    let second_result_json = seed_launch(&owner.store, SECOND_LAUNCH_ID).await;
    let first_snapshot = launch_snapshot(&owner.store, FIRST_LAUNCH_ID).await;
    let second_snapshot = launch_snapshot(&owner.store, SECOND_LAUNCH_ID).await;

    let error = private_artifact_failure(&owner.store, &config, &manager, FIRST_LAUNCH_ID).await;
    assert_eq!(error.code, "PRIVATE_ARTIFACT_PATH");
    let first_recorded = owner
        .store
        .record_participant_issuance_failure(
            FIRST_LAUNCH_ID.to_owned(),
            Some(first_snapshot),
            IssuanceFailureStage::CredentialIssue,
            &error,
        )
        .await
        .unwrap();
    assert!(first_recorded);

    let visible = owner
        .store
        .call(
            manager.clone(),
            "operation.get".to_owned(),
            json!({"operation_id":FIRST_LAUNCH_ID}),
        )
        .await
        .unwrap();
    assert_eq!(visible["operation_id"], FIRST_LAUNCH_ID);
    assert_eq!(visible["task_id"], TASK_ID);
    assert_eq!(visible["attempt_id"], ATTEMPT_ID);
    assert_eq!(visible["binding_id"], BINDING_ID);
    assert_eq!(visible["binding_generation"], 1);
    let failure = &visible["participant_issuance"]["latest_failure"];
    assert_eq!(failure["schema_version"], 1);
    assert_eq!(failure["code"], "PRIVATE_ARTIFACT_PATH");
    assert_eq!(failure["stage"], "participant_credential_issue");
    assert_eq!(failure["category"], "scoped_artifact_unavailable");
    assert!(failure["recorded_at_ms"].as_i64().unwrap_or_default() > 0);
    assert_eq!(failure.as_object().unwrap().len(), 5);
    assert_eq!(visible["state"], "queued");
    assert_eq!(
        visible["result"],
        json!({"operation_id":FIRST_LAUNCH_ID,"state":"queued"})
    );
    assert_eq!(
        launch_result_snapshot(&owner.store, FIRST_LAUNCH_ID).await,
        first_result_json,
        "the safe diagnostic projection leaves the raw admission result byte-identical"
    );
    let public_json = serde_json::to_string(&visible).unwrap();
    assert!(!public_json.contains(PRIVATE_PATH_MARKER));
    assert!(!public_json.contains(&directory.display().to_string()));

    let first_effective = retained_effective(&owner.store, FIRST_LAUNCH_ID).await;
    assert_eq!(
        first_effective["launch_manifest"]["state"],
        "awaiting_participant_credential"
    );
    assert_eq!(
        first_effective["launch_manifest"]["participant_issuance_latest_failure"],
        failure.clone()
    );
    let child_count = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT count(*) FROM operations WHERE client_request_id=?1 \
                 AND method='coordination.participant.register'",
                [format!("launch:{FIRST_LAUNCH_ID}:participant")],
                |row| row.get::<_, i64>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(child_count, 0);

    let second_error =
        private_artifact_failure(&owner.store, &config, &manager, SECOND_LAUNCH_ID).await;
    assert_eq!(second_error.code, "PRIVATE_ARTIFACT_PATH");
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let raw: String = tx.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [SECOND_LAUNCH_ID],
                |row| row.get(0),
            )?;
            let mut effective: Value = serde_json::from_str(&raw)?;
            effective["launch_manifest"]["state"] = json!("awaiting_native_mcp");
            tx.execute(
                "UPDATE operations SET effective_request_json=?2,updated_at_ms=2 \
                 WHERE operation_id=?1 AND state='queued'",
                params![SECOND_LAUNCH_ID, model::canonical(&effective)?],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let stale_recorded = owner
        .store
        .record_participant_issuance_failure(
            SECOND_LAUNCH_ID.to_owned(),
            Some(second_snapshot),
            IssuanceFailureStage::CredentialIssue,
            &second_error,
        )
        .await
        .unwrap();
    assert!(!stale_recorded);
    let second_effective = retained_effective(&owner.store, SECOND_LAUNCH_ID).await;
    assert_eq!(
        second_effective["launch_manifest"]["state"],
        "awaiting_native_mcp"
    );
    assert!(second_effective["launch_manifest"]["participant_issuance_latest_failure"].is_null());
    assert_eq!(
        launch_result_snapshot(&owner.store, SECOND_LAUNCH_ID).await,
        second_result_json,
        "the stale compare-and-set leaves the raw admission result byte-identical"
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
