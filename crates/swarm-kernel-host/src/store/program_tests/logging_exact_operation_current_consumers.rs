//! Public Store consumers for exact Operation logging ownership.
use super::*;
use rusqlite::params;
use serde_json::json;

const OWNER_ID: &str = "review-owner-v2";
const FOREIGN_OWNER_ID: &str = "unrelated-manager";
const BINDING_ID: &str = "app140-exact-operation-binding";
const STALE_BINDING_ID: &str = "app140-stale-operation-binding";
const OWNED_OPERATION_ID: &str = "app140-owned-taskless-operation";
const MISMATCH_OPERATION_ID: &str = "app140-owned-mismatch-operation";
const STALE_OPERATION_ID: &str = "app140-owned-stale-operation";
const FOREIGN_OPERATION_ID: &str = "app140-foreign-operation-0";

#[tokio::test]
async fn public_logging_get_uses_exact_operation_owner_without_weakening_binding_gates() {
    let (owner, directory, _) = start_store("app140-exact-operation-logging").await;
    seed_clients(&owner.store).await;

    owner
        .store
        .run(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            for (binding_id, released_at_ms) in
                [(BINDING_ID, None), (STALE_BINDING_ID, Some(5_i64))]
            {
                tx.execute(
                    "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms,released_at_ms) \
                     VALUES(?1,1,?2,?3,'app140-fixture-module',CASE WHEN ?4 IS NULL THEN 'ready' ELSE 'closed' END,'{}','{}',1,?4)",
                    params![
                        binding_id,
                        format!("{binding_id}-lane"),
                        format!("{binding_id}-instance"),
                        released_at_ms,
                    ],
                )?;
            }

            let mut operations = vec![
                (
                    OWNED_OPERATION_ID.to_owned(),
                    OWNER_ID.to_owned(),
                    1_i64,
                    BINDING_ID.to_owned(),
                ),
                (
                    MISMATCH_OPERATION_ID.to_owned(),
                    OWNER_ID.to_owned(),
                    2_i64,
                    BINDING_ID.to_owned(),
                ),
                (
                    STALE_OPERATION_ID.to_owned(),
                    OWNER_ID.to_owned(),
                    3_i64,
                    STALE_BINDING_ID.to_owned(),
                ),
            ];
            for index in 0_i64..16 {
                let operation_id = if index == 0 {
                    FOREIGN_OPERATION_ID.to_owned()
                } else {
                    format!("app140-foreign-operation-{index}")
                };
                operations.push((
                    operation_id,
                    FOREIGN_OWNER_ID.to_owned(),
                    100 + index,
                    BINDING_ID.to_owned(),
                ));
            }

            for (operation_id, caller_id, created_at_ms, binding_id) in operations {
                let request_id = format!("app140-request-{operation_id}");
                tx.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                     VALUES(?1,?2,?3,'coordination.consult','{}','{}',?4,1,'settled','{}',1,?5,?5,?5)",
                    params![operation_id, caller_id, request_id, binding_id, created_at_ms],
                )?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let manager = principal(OWNER_ID, Role::Manager);
    let exact = owner
        .store
        .call(
            manager.clone(),
            "logging.get".into(),
            json!({"operation_id":OWNED_OPERATION_ID}),
        )
        .await
        .unwrap();
    assert_eq!(exact["scope"]["operation_id"], OWNED_OPERATION_ID);
    assert_eq!(exact["scope"]["binding_id"], BINDING_ID);
    assert_eq!(exact["scope"]["binding_generation"], 1);
    assert!(exact["scope"]["task_id"].is_null());

    let foreign_operation = owner
        .store
        .call(
            manager.clone(),
            "logging.get".into(),
            json!({"operation_id":FOREIGN_OPERATION_ID}),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_operation.code, "FORBIDDEN");

    let direct_binding = owner
        .store
        .call(
            manager.clone(),
            "logging.get".into(),
            json!({"binding_id":BINDING_ID,"binding_generation":1}),
        )
        .await
        .unwrap_err();
    assert_eq!(direct_binding.code, "FORBIDDEN");

    let mismatched_generation = owner
        .store
        .call(
            manager.clone(),
            "logging.get".into(),
            json!({
                "operation_id":MISMATCH_OPERATION_ID,
                "binding_id":BINDING_ID,
                "binding_generation":2
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(mismatched_generation.code, "LOGGING_SCOPE_MISMATCH");

    let stale_generation = owner
        .store
        .call(
            manager,
            "logging.get".into(),
            json!({"operation_id":STALE_OPERATION_ID}),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_generation.code, "LOGGING_SCOPE_STALE");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
