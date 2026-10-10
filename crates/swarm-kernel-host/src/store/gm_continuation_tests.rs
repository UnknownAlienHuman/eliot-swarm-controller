//! Offline Store regression for current-GM control of an existing Attempt.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const TASK_ID: &str = "task-gm-continuation";
const ATTEMPT_ID: &str = "attempt-gm-continuation";
const BINDING_ID: &str = "binding-gm-continuation";
const MODULE_ID: &str = "module-gm-continuation";
const MODULE_LINK: &str = "module-link-gm-continuation";

struct StoredOperationScope {
    caller_id: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
}

struct RetainedAttemptScope {
    owner_id: String,
    start_operation_id: Option<String>,
    binding_generation: Option<i64>,
    queued_dispatches: i64,
}

fn principal(client_id: &str, role: Role, link_id: &str) -> Principal {
    Principal {
        link_id: link_id.to_owned(),
        client_id: client_id.to_owned(),
        role,
    }
}

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(PROVIDER_CONDITION_SCHEMA).unwrap();

    for client_id in ["old-gm", "successor-gm", "replacement-gm", "unrelated"] {
        set_meta(
            &db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(
        &db,
        &format!("client:{MODULE_ID}"),
        &json!({
            "role":"module",
            "disabled":false,
            "binding_id":BINDING_ID,
            "binding_generation":1
        }),
    )
    .unwrap();
    set_meta(&db, "gm", &json!({"client_id":"old-gm","epoch":1})).unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();

    let route = json!({
        "alias":"gm-fixture",
        "runtime":"muse",
        "module_artifact_id":"muse-sdk-1.3.0-bridge.5",
        "enabled":true,
        "native_options":{"workspaceRoot":"C:\\fixture","modelId":"fixture-model"}
    });
    let binding_state = json!({
        "connection":"connected",
        "module_client_id":MODULE_ID,
        "module_link_id":MODULE_LINK,
        "bridge_boot_id":"fixture-boot"
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'gm-fixture-lane','gm-fixture-module','muse-sdk-1.3.0-bridge.5','ready',?2,?3,1)",
        params![
            BINDING_ID,
            model::canonical(&route).unwrap(),
            model::canonical(&binding_state).unwrap()
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'gm-fixture-project',1,'open','{}',1,1)",
        [TASK_ID],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,'{}','old-gm','controller',?3,1,'reserved',1,1)",
        params![ATTEMPT_ID, TASK_ID, BINDING_ID],
    )
    .unwrap();
    db
}

fn reply_request(request_id: &str) -> Value {
    json!({
        "client_request_id":request_id,
        "binding_id":BINDING_ID,
        "generation":1,
        "reply":{"text":"continue the retained task"}
    })
}

#[test]
fn successor_gm_continues_exact_attempt_without_restarting_dispatch() {
    let mut db = fixture();
    let mut config = Config::default();
    config.routes.push(
        serde_json::from_value(json!({
            "alias":"gm-fixture",
            "runtime":"muse",
            "module_artifact_id":"muse-sdk-1.3.0-bridge.5",
            "enabled":true,
            "native_options":{"workspaceRoot":r"C:\fixture","modelId":"fixture-model"}
        }))
        .unwrap(),
    );
    let old_gm = principal("old-gm", Role::Manager, "old-link");
    let successor = principal("successor-gm", Role::Manager, "successor-link");
    let replacement = principal("replacement-gm", Role::Manager, "replacement-link");
    let unrelated = principal("unrelated", Role::Manager, "unrelated-link");
    let module = principal(MODULE_ID, Role::Module, MODULE_LINK);

    let first_dispatch = mutate(
        &mut db,
        &old_gm,
        "task.dispatch",
        &json!({
            "client_request_id":"dispatch-original",
            "attempt_id":ATTEMPT_ID,
            "text":"start the retained Attempt"
        }),
        &config,
    )
    .unwrap();
    let start_operation_id = first_dispatch["operation_id"].as_str().unwrap().to_owned();
    assert_eq!(first_dispatch["state"], "queued");

    mutate(
        &mut db,
        &old_gm,
        "gm.handover",
        &json!({"client_request_id":"handover-successor","client_id":"successor-gm"}),
        &config,
    )
    .unwrap();

    let coalesced = mutate(
        &mut db,
        &successor,
        "task.dispatch",
        &json!({
            "client_request_id":"dispatch-successor-retry",
            "attempt_id":ATTEMPT_ID,
            "text":"start the retained Attempt"
        }),
        &config,
    )
    .unwrap();
    assert_eq!(coalesced["coalesced"], true);
    assert_eq!(coalesced["semantic_reuse"], true);
    assert_eq!(coalesced["start_operation_id"], start_operation_id);
    assert_eq!(coalesced["start_operation_state_at_receipt"], "queued");
    assert_eq!(coalesced["native_effect"], "not_repeated");
    let reuse_operation_id = coalesced["operation_id"].as_str().unwrap().to_owned();
    assert_ne!(reuse_operation_id, start_operation_id);

    let reuse_raw: String = db
        .query_row(
            r#"SELECT json_object(
                'state',state,
                'task_id',task_id,
                'attempt_id',attempt_id,
                'binding_id',binding_id,
                'binding_generation',binding_generation,
                'effective',json(effective_request_json),
                'result',json(result_json)
            )
            FROM operations
            WHERE operation_id=?1"#,
            [&reuse_operation_id],
            |row| row.get(0),
        )
        .unwrap();
    let reuse: Value = serde_json::from_str(&reuse_raw).unwrap();
    assert_eq!(reuse["state"], "settled");
    assert_eq!(reuse["task_id"], TASK_ID);
    assert_eq!(reuse["attempt_id"], ATTEMPT_ID);
    assert!(reuse["binding_id"].is_null());
    assert!(reuse["binding_generation"].is_null());
    assert_eq!(
        reuse["effective"]["semantic_reuse"]["start_operation_id"],
        start_operation_id
    );
    assert_eq!(
        reuse["effective"]["semantic_reuse"]["native_effect"],
        "not_repeated"
    );
    assert!(reuse["effective"].get("route").is_none());
    assert!(reuse["effective"].get("input").is_none());
    assert!(reuse["effective"].get("task_snapshot").is_none());
    assert!(reuse["effective"].get("launch_dispatch_packet").is_none());
    assert_eq!(reuse["result"]["operation_id"], reuse_operation_id);
    assert_eq!(reuse["result"]["start_operation_id"], start_operation_id);
    let observation_payload: String = db
        .query_row(
            "SELECT payload_json FROM observations              WHERE operation_id=?1 AND kind='task.dispatch'",
            [&reuse_operation_id],
            |row| row.get(0),
        )
        .unwrap();
    let observation: Value = serde_json::from_str(&observation_payload).unwrap();
    assert_eq!(observation["operation_id"], reuse_operation_id);
    assert_eq!(observation["start_operation_id"], start_operation_id);

    let successor_reply = mutate(
        &mut db,
        &successor,
        "agent.reply",
        &reply_request("reply-by-successor"),
        &config,
    )
    .unwrap();
    let successor_operation_id = successor_reply["operation_id"].as_str().unwrap();
    let stored = db
        .query_row(
            "SELECT caller_id,task_id,attempt_id,binding_id,binding_generation FROM operations WHERE operation_id=?1",
            [successor_operation_id],
            |row| {
                Ok(StoredOperationScope {
                    caller_id: row.get(0)?,
                    task_id: row.get(1)?,
                    attempt_id: row.get(2)?,
                    binding_id: row.get(3)?,
                    binding_generation: row.get(4)?,
                })
            },
        )
        .unwrap();
    assert_eq!(stored.caller_id, "successor-gm");
    assert_eq!(stored.task_id.as_deref(), Some(TASK_ID));
    assert_eq!(stored.attempt_id.as_deref(), Some(ATTEMPT_ID));
    assert_eq!(stored.binding_id.as_deref(), Some(BINDING_ID));
    assert_eq!(stored.binding_generation, Some(1));

    mutate(
        &mut db,
        &successor,
        "gm.handover",
        &json!({"client_request_id":"handover-replacement","client_id":"replacement-gm"}),
        &config,
    )
    .unwrap();
    let stale_begin = super::runtime::next_with_config(&mut db, &module, &config).unwrap();
    assert!(stale_begin["command"].is_null());
    assert_eq!(stale_begin["rejected_operation_id"], successor_operation_id);
    assert_eq!(stale_begin["error"]["code"], "FORBIDDEN");

    let denied = mutate(
        &mut db,
        &unrelated,
        "agent.reply",
        &reply_request("reply-by-unrelated"),
        &config,
    )
    .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");

    let replacement_reply = mutate(
        &mut db,
        &replacement,
        "agent.reply",
        &reply_request("reply-by-replacement"),
        &config,
    )
    .unwrap();
    let replacement_operation_id = replacement_reply["operation_id"].as_str().unwrap();
    let accepted_begin = super::runtime::next_with_config(&mut db, &module, &config).unwrap();
    assert_eq!(
        accepted_begin["command"]["operation_id"],
        replacement_operation_id
    );
    assert_eq!(accepted_begin["command"]["method"], "agent.reply");

    let retained = db
        .query_row(
            "SELECT owner_id,start_operation_id,binding_generation,
                    (SELECT COUNT(*) FROM operations WHERE method='task.dispatch' AND state='queued') \
             FROM attempts WHERE attempt_id=?1",
            [ATTEMPT_ID],
            |row| {
                Ok(RetainedAttemptScope {
                    owner_id: row.get(0)?,
                    start_operation_id: row.get(1)?,
                    binding_generation: row.get(2)?,
                    queued_dispatches: row.get(3)?,
                })
            },
        )
        .unwrap();
    assert_eq!(retained.owner_id, "old-gm");
    assert_eq!(
        retained.start_operation_id.as_deref(),
        Some(start_operation_id.as_str())
    );
    assert_eq!(retained.binding_generation, Some(1));
    assert_eq!(retained.queued_dispatches, 1);
}
