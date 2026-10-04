use super::*;
use rusqlite::{Connection, TransactionBehavior, params};
use serde_json::{Value, json};

const LAUNCH_ID: &str = "native-mcp-failure-launch";

fn invalid_scope_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(super::super::SCHEMA).unwrap();

    let manifest = json!({
        "state":"awaiting_native_mcp",
        "runtime":{"dispatch_permitted":false},
        "progress":{"task_dispatch":"not_started"},
        "actor":{
            "kind":"direct",
            "client_id":"different-from-operation-caller",
        },
    });
    let effective = json!({"launch_manifest":manifest});
    db.execute(
        "INSERT INTO operations(
             operation_id,caller_id,client_request_id,method,original_request_json,
             effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms
         ) VALUES(?1,'current-manager','native-mcp-failure-request','swarm.launch',
                  '{}',?2,'queued',1,1,1)",
        params![LAUNCH_ID, model::canonical(&effective).unwrap()],
    )
    .unwrap();
    db
}

fn retained_manifest(db: &Connection) -> Value {
    let raw: String = db
        .query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [LAUNCH_ID],
            |row| row.get(0),
        )
        .unwrap();
    serde_json::from_str::<Value>(&raw).unwrap()["launch_manifest"].clone()
}

#[test]
fn invalid_scope_readback_persists_safe_code_and_stage_across_marker_update() {
    let mut db = invalid_scope_db();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let outcome = claim_next_readback(&tx, 100, &Config::default()).unwrap();
    let ClaimOutcome::Deferred(result) = outcome else {
        panic!("invalid launch scope should defer readback");
    };
    assert_eq!(result["state"], "retry_wait");
    assert_eq!(result["last_error_code"], "FORBIDDEN");
    assert_eq!(result["last_error_stage"], "launch_snapshot_validate");
    tx.commit().unwrap();

    let initial = retained_manifest(&db);
    assert_eq!(
        initial["native_mcp_readback"]["last_error_code"],
        "FORBIDDEN"
    );
    assert_eq!(
        initial["native_mcp_readback"]["last_error_stage"],
        "launch_snapshot_validate"
    );
    assert_eq!(
        initial["native_mcp_latest_failure"],
        json!({
            "schema_version":1,
            "code":"FORBIDDEN",
            "stage":"launch_snapshot_validate",
            "recorded_at_ms":100,
            "category":"assignment_scope_unavailable",
        })
    );

    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let (row, mut manifest) = load_launch_manifest(&tx, LAUNCH_ID).unwrap();
    manifest["native_mcp_readback"] = json!({
        "state":"observed_partial",
        "attempts":1,
        "dispatch_permitted":false,
    });
    persist_manifest(&tx, &row, &manifest, 101).unwrap();
    tx.commit().unwrap();

    let updated = retained_manifest(&db);
    assert_eq!(updated["native_mcp_readback"]["state"], "observed_partial");
    assert_eq!(
        updated["native_mcp_latest_failure"],
        initial["native_mcp_latest_failure"]
    );
}
