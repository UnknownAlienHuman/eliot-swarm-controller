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
        "progress":{
            "task_dispatch":"not_started",
            "participant_credential":"registered_and_refs_retained",
        },
        "mcp":{
            "identity":{"status":"assignment_template","role":"participant"},
            "capability_state":"unknown",
            "runtime_loaded":"unknown",
        },
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

fn changed_participant_registration_db() -> Connection {
    const MANAGER_ID: &str = "legacy-scope-manager";
    const PARTICIPANT_ID: &str = "legacy-scope-participant";
    const TASK_ID: &str = "legacy-scope-task";
    const ATTEMPT_ID: &str = "legacy-scope-attempt";
    const BINDING_ID: &str = "legacy-scope-binding";

    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(super::super::SCHEMA).unwrap();
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![
            format!("client:{MANAGER_ID}"),
            model::canonical(&json!({"role":"manager","disabled":false})).unwrap(),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![
            format!("client:{PARTICIPANT_ID}"),
            model::canonical(&json!({
                "role":"participant",
                "disabled":true,
                "task_id":TASK_ID,
                "task_revision":1,
                "attempt_id":ATTEMPT_ID,
                "binding_id":BINDING_ID,
                "binding_generation":1,
                "created_by":MANAGER_ID,
                "created_operation_id":"legacy-scope-registration",
                "grant_revision":1,
                "participation_basis":{
                    "kind":"attempt_owner",
                    "assignment_id":null,
                    "review_scope":null,
                },
                "native_session_id":null,
            }))
            .unwrap(),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'legacy-scope-project',1,'open','{}',1,1)",
        [TASK_ID],
    )
    .unwrap();
    let route = json!({
        "alias":"legacy-scope-opencode",
        "runtime":"opencode_v2",
        "module_artifact_id":crate::runtime::opencode_v2::ARTIFACT_ID,
        "native_options":{"service_id":"opencode","model":{"id":"test/model","providerID":"opencode-go"}},
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'legacy-scope-lane','legacy-scope-module',?2,'ready',?3,'{}',1)",
        params![
            BINDING_ID,
            crate::runtime::opencode_v2::ARTIFACT_ID,
            model::canonical(&route).unwrap(),
        ],
    )
    .unwrap();

    let credential_ref = "participant-credential:00000000-0000-4000-8000-000000000001";
    let profile_config_ref = "participant-mcp-config:00000000-0000-4000-8000-000000000002";
    let registration_operation_id = "legacy-scope-registration";
    let manifest = json!({
        "state":"awaiting_native_mcp",
        "runtime":{"dispatch_permitted":false},
        "progress":{
            "task_dispatch":"not_started",
            "participant_credential":"registered_and_refs_retained",
        },
        "actor":{
            "kind":"direct",
            "client_id":MANAGER_ID,
            "role":"manager",
            "link_id":"legacy-scope-manager-link",
        },
        "task":{
            "task_id":TASK_ID,
            "project_id":"legacy-scope-project",
            "observed_revision":1,
            "attempt_id":ATTEMPT_ID,
        },
        "binding":{
            "operation_id":"legacy-scope-open",
            "binding_id":BINDING_ID,
            "generation":1,
            "state":"ready",
        },
        "participant":{
            "client_id":PARTICIPANT_ID,
            "role":"participant",
            "task_id":TASK_ID,
            "task_revision":1,
            "attempt_id":ATTEMPT_ID,
            "binding_id":BINDING_ID,
            "binding_generation":1,
            "grant_revision":1,
            "participation_basis":{
                "kind":"attempt_owner",
                "assignment_id":null,
                "review_scope":null,
            },
            "native_session_id":null,
            "registration_operation_id":registration_operation_id,
            "credential_ref":credential_ref,
            "profile_config_ref":profile_config_ref,
        },
        "request":{"mcp_profile":"participant","mcp_surface":"task"},
        "mcp":{
            "identity":{"status":"assignment_template","role":"participant"},
            "participant_client_id":PARTICIPANT_ID,
            "credential_ref":credential_ref,
            "profile_config_ref":profile_config_ref,
            "profile_name":"participant",
            "surface":"task",
            "surface_facts":{},
            "hard_profile":"participant",
            "status":"validated_against_static_catalog",
            "capability_state":"unknown",
            "runtime_loaded":"unknown",
        },
    });
    let effective = json!({"launch_manifest":manifest});
    db.execute(
        "INSERT INTO operations(
             operation_id,caller_id,client_request_id,method,original_request_json,
             effective_request_json,task_id,binding_id,binding_generation,state,
             due_at_ms,created_at_ms,updated_at_ms
         ) VALUES(?1,?2,'legacy-scope-request','swarm.launch','{}',?3,?4,?5,1,
                  'queued',1,1,1)",
        params![
            LAUNCH_ID,
            MANAGER_ID,
            model::canonical(&effective).unwrap(),
            TASK_ID,
            BINDING_ID,
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,'{}',?3,'controller',?4,1,'reserved','[]',1,1)",
        params![ATTEMPT_ID, TASK_ID, MANAGER_ID, BINDING_ID],
    )
    .unwrap();
    db.execute(
        "UPDATE operations SET attempt_id=?2 WHERE operation_id=?1",
        params![LAUNCH_ID, ATTEMPT_ID],
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
    assert_eq!(initial["mcp"]["identity"]["status"], "assignment_template");
    assert_eq!(initial["mcp"]["identity"]["role"], "participant");
    assert_eq!(initial["mcp"]["capability_state"], "unknown");
    assert_eq!(initial["mcp"]["runtime_loaded"], "unknown");
    assert_eq!(initial["runtime"]["dispatch_permitted"], false);
    assert_eq!(initial["progress"]["task_dispatch"], "not_started");
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

#[test]
fn committed_readback_failure_is_a_closed_any_event_and_rolls_back_with_its_marker() {
    let mut db = invalid_scope_db();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let outcome = claim_next_readback(&tx, 100, &Config::default()).unwrap();
    let ClaimOutcome::Deferred(result) = outcome else {
        panic!("invalid launch scope should defer readback");
    };
    assert_eq!(result["last_error_code"], "FORBIDDEN");
    tx.commit().unwrap();

    let (observation_id, source_id, source_event_key, operation_id, event_kind, recorded_at_ms, raw):
        (i64, String, String, String, String, i64, String) = db
        .query_row(
            "SELECT observation_id,source_stream_id,source_event_key,operation_id,kind,recorded_at_ms,payload_json \
             FROM observations WHERE source_stream_id='controller:native-mcp' AND kind='native.mcp.failure'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(source_id, "controller:native-mcp");
    assert_eq!(operation_id, LAUNCH_ID);
    assert_eq!(event_kind, "native.mcp.failure");
    assert_eq!(recorded_at_ms, 100);
    let payload: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(payload["schema_version"], 1);
    assert_eq!(payload["phase"], "native_mcp_failure");
    assert_eq!(payload["status"], "failed");
    assert_eq!(payload["error_code"], "FORBIDDEN");
    assert_eq!(payload["failure_category"], "assignment_scope_unavailable");
    assert_eq!(payload["failed_supervisor"], "native_mcp_readback");
    assert_eq!(payload["failure_kind"], "readback_retry");
    assert_eq!(payload["attempt"], 1);
    assert_eq!(payload.as_object().unwrap().len(), 9);
    assert_eq!(
        Some(source_event_key.as_str()),
        payload["occurrence_id"].as_str()
    );

    let event = crate::automation::intake::ObservedEvent {
        observation_id,
        source_id: source_id.clone(),
        event_kind: event_kind.clone(),
        operation_id: Some(operation_id),
        recorded_at_ms,
    };
    let projection = super::super::automation_intake::safe_event_projection(&db, &event).unwrap();
    assert_eq!(
        projection.status,
        Some(crate::automation::event_rules::EventStatus::Failed)
    );
    assert_eq!(projection.error_code.as_deref(), Some("FORBIDDEN"));
    assert_eq!(
        projection.failure_category.as_deref(),
        Some("assignment_scope_unavailable")
    );
    assert_eq!(
        projection.failed_supervisor.as_deref(),
        Some("native_mcp_readback")
    );
    assert_eq!(
        projection.occurrence_phase.as_deref(),
        Some("native_mcp_failure")
    );
    let any_rule = crate::automation::event_rules::EventRule {
        source: None,
        predicate: None,
        source_id: Some(source_id.clone()),
        event_kind: Some(event_kind.clone()),
        status: None,
        action: crate::automation::event_rules::EventRuleAction::ScriptRun,
    };
    let completed_rule = crate::automation::event_rules::EventRule {
        status: Some(crate::automation::event_rules::EventStatus::Completed),
        ..any_rule.clone()
    };
    assert!(any_rule.matches_safe_event(&source_id, &event_kind, projection.status,));
    assert!(!completed_rule.matches_safe_event(&source_id, &event_kind, projection.status,));

    let mut rollback_db = invalid_scope_db();
    rollback_db
        .execute_batch(
            "CREATE TRIGGER reject_native_mcp_failure BEFORE INSERT ON observations \
             WHEN NEW.source_stream_id='controller:native-mcp' AND NEW.kind='native.mcp.failure' \
             BEGIN SELECT RAISE(ABORT,'fixture event rejection'); END;",
        )
        .unwrap();
    let tx = rollback_db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(claim_next_readback(&tx, 100, &Config::default()).is_err());
    tx.rollback().unwrap();
    assert!(retained_manifest(&rollback_db)["native_mcp_readback"].is_null());
    let event_count: i64 = rollback_db
        .query_row(
            "SELECT count(*) FROM observations WHERE source_stream_id='controller:native-mcp' \
             AND kind='native.mcp.failure'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 0);
}

#[test]
fn legacy_template_is_not_promoted_after_participant_registration_changes() {
    let mut db = changed_participant_registration_db();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let outcome = claim_next_readback(&tx, 100, &Config::default()).unwrap();
    let ClaimOutcome::Deferred(result) = outcome else {
        panic!("changed Participant registration should defer readback");
    };
    assert_eq!(result["last_error_code"], "NATIVE_MCP_SCOPE_MISMATCH");
    assert_eq!(result["last_error_stage"], "launch_snapshot_validate");
    tx.commit().unwrap();

    let retained = retained_manifest(&db);
    assert_eq!(retained["mcp"]["identity"]["status"], "assignment_template");
    assert_eq!(retained["mcp"]["identity"]["role"], "participant");
    assert_eq!(retained["mcp"]["capability_state"], "unknown");
    assert_eq!(retained["mcp"]["runtime_loaded"], "unknown");
    assert_eq!(retained["runtime"]["dispatch_permitted"], false);
    assert_eq!(retained["progress"]["task_dispatch"], "not_started");
}
