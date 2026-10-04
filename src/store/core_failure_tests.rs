//! Public Store readback for owned-service startup failures.
use super::*;
use rusqlite::params;

const ORIGINAL_GM: &str = "failure-original-gm";
const SUCCESSOR_GM: &str = "failure-successor-gm";
const OPERATOR: &str = "failure-local-operator";
const TASK_ID: &str = "task-owned-service-failure";
const ATTEMPT_ID: &str = "attempt-owned-service-failure";
const BINDING_ID: &str = "binding-owned-service-failure";
const LAUNCH_OPERATION_ID: &str = "launch-owned-service-failure";
const OPEN_OPERATION_ID: &str = "open-owned-service-failure";
const DIAGNOSTIC_MARKER: &str = "DO_NOT_EXPOSE_CORRUPT_DIAGNOSTIC_MARKER";
const PRIVATE_SERVICE_PROOF_MARKER: &str = "DO_NOT_EXPOSE_OWNED_SERVICE_PROOF_MARKER";

struct Fixture {
    db: Connection,
    admission_result: Value,
    admission_result_json: String,
    observation_id: i64,
}

fn principal(client_id: &str, role: Role) -> Principal {
    Principal {
        link_id: format!("test-link-{client_id}"),
        client_id: client_id.to_owned(),
        role,
    }
}

fn fixture() -> Fixture {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    db.execute_batch(SCRIPT_SCHEMA).unwrap();
    db.execute_batch(GITHUB_SCHEMA).unwrap();

    for client_id in [ORIGINAL_GM, SUCCESSOR_GM] {
        set_meta(
            &db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(
        &db,
        &format!("client:{OPERATOR}"),
        &json!({"role":"operator","disabled":false}),
    )
    .unwrap();
    set_meta(&db, LOCAL_OPERATOR_CLIENT_ID_KEY, &json!(OPERATOR)).unwrap();
    set_meta(
        &db,
        "gm",
        &json!({
            "client_id":ORIGINAL_GM,
            "binding_id":null,
            "binding_generation":null,
            "epoch":1,
        }),
    )
    .unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();

    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'failure-projection-project',1,'open','{}',1,1)",
        [TASK_ID],
    )
    .unwrap();
    let route = json!({
        "alias":"failure-opencode",
        "runtime":"opencode_v2",
        "native_options":{"service_id":"opencode","model":{"id":"hosted/model","providerID":"opencode-go"}},
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'failure-lane','failure-module','eliot-opencode-v2.http.1','ready',?2,'{}',1)",
        params![BINDING_ID, model::canonical(&route).unwrap()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,'{}',?3,'controller',?4,1,'running','[]',1,1)",
        params![ATTEMPT_ID, TASK_ID, ORIGINAL_GM, BINDING_ID],
    )
    .unwrap();

    let admission_result = json!({
        "operation_id":LAUNCH_OPERATION_ID,
        "state":"queued",
        "launch_state":"awaiting_capability",
        "task_dispatch":"not_started",
        "capability_state":"unknown",
        "binding":{"state":"ready"},
    });
    let admission_result_json = model::canonical(&admission_result).unwrap();
    let launch_request = json!({"client_request_id":"request-owned-service-failure"});
    let launch_effective = json!({
        "launch_manifest":{
            "binding":{
                "operation_id":OPEN_OPERATION_ID,
                "binding_id":BINDING_ID,
                "generation":1,
            }
        }
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,result_json,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'request-owned-service-failure','swarm.launch',?3,?4,?5,?6,?7,1,'outcome_unknown',?8,1,1,1)",
        params![
            LAUNCH_OPERATION_ID,
            ORIGINAL_GM,
            model::canonical(&launch_request).unwrap(),
            model::canonical(&launch_effective).unwrap(),
            TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
            admission_result_json,
        ],
    )
    .unwrap();
    let open_request = json!({"client_request_id":"open-owned-service-failure"});
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,prerequisite_operation_id,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'request-open-owned-service-failure','agent.open',?3,'{}',?4,?5,?6,1,?7,'outcome_unknown',1,1,1)",
        params![
            OPEN_OPERATION_ID,
            ORIGINAL_GM,
            model::canonical(&open_request).unwrap(),
            TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
            LAUNCH_OPERATION_ID,
        ],
    )
    .unwrap();

    db.execute(
        "INSERT INTO workspace_registrations(registration_id,project_id,trusted_repository,repository_path,allowed_roots_json,registration_digest,generation,authorized_by,state,created_at_ms,updated_at_ms) \
         VALUES('failure-registration','failure-projection-project','fixture-repository','fixture-repository','[]',?1,1,?2,'active',1,1)",
        params!["d".repeat(64), ORIGINAL_GM],
    )
    .unwrap();
    db.execute(
        "INSERT INTO workspace_leases(lease_id,registration_id,registration_generation,project_id,task_id,task_revision,operation_id,plan_digest,owner_client_id,attempt_id,allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,worktree_handle,workspace_path,clean_state_json,generation,binding_digest,state,created_at_ms,updated_at_ms) \
         VALUES('failure-lease','failure-registration',1,'failure-projection-project',?1,1,?2,?3,?4,?5,'[\"README.md\"]','[]','baseline','refs/heads/main','failure-worktree','fixture-worktree','{}',1,?6,'held',1,1)",
        params![
            TASK_ID,
            LAUNCH_OPERATION_ID,
            "e".repeat(64),
            ORIGINAL_GM,
            ATTEMPT_ID,
            "b".repeat(64),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO owned_service_starts(launch_operation_id,open_operation_id,binding_id,binding_generation,task_id,task_revision,attempt_id,lease_id,lease_generation,technical_requester_id,effective_manager_id,service_id,service_version,route_digest,binding_digest,intent_nonce,intent_digest,state,proof_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,1,?4,1,?5,'failure-lease',1,?6,?6,'opencode','2.0.7',?7,?8,'12345678-1234-4234-8234-1234567890ab',?9,'outcome_unknown','{}',1,2)",
        params![
            LAUNCH_OPERATION_ID,
            OPEN_OPERATION_ID,
            BINDING_ID,
            TASK_ID,
            ATTEMPT_ID,
            ORIGINAL_GM,
            "a".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
        ],
    )
    .unwrap();
    let diagnostic = json!({
        "schema_version":2,
        "status":"startup_failed_unknown",
        "stage":"bootstrap",
        "error_code":"NATIVE_REJECTED",
        "native_effect":"unknown",
        "request_phase":"provider_key_post",
        "http_status":403,
    });
    let event_key = format!("owned-service-start-failure:{LAUNCH_OPERATION_ID}");
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller:owned-service',?1,?2,1,?3,'owned_service.start_failure',?4,2)",
        params![
            event_key,
            BINDING_ID,
            LAUNCH_OPERATION_ID,
            model::canonical(&diagnostic).unwrap(),
        ],
    )
    .unwrap();
    let observation_id = db.last_insert_rowid();

    Fixture {
        db,
        admission_result,
        admission_result_json,
        observation_id,
    }
}

fn set_native_mcp_manifest(fixture: &Fixture, marker: Value, latest_failure: Value) {
    let effective = json!({
        "launch_manifest":{
            "state":"awaiting_native_mcp",
            "binding":{
                "operation_id":OPEN_OPERATION_ID,
                "binding_id":BINDING_ID,
                "generation":1,
            },
            "native_mcp_readback":marker,
            "native_mcp_latest_failure":latest_failure,
        }
    });
    fixture
        .db
        .execute(
            "UPDATE operations SET effective_request_json=?1 WHERE operation_id=?2",
            params![model::canonical(&effective).unwrap(), LAUNCH_OPERATION_ID],
        )
        .unwrap();
}

fn store_read(db: &Connection, caller: Principal, method: &str, params: Value) -> Value {
    let current = super::current_principal(db, caller).unwrap();
    super::read(db, &current, method, &params, &Config::default()).unwrap()
}

fn store_mutate(db: &mut Connection, caller: Principal, method: &str, params: Value) -> Value {
    let current = super::current_principal(db, caller).unwrap();
    super::mutate(db, &current, method, &params, &Config::default()).unwrap()
}

fn assert_failure_action(action: &Value, observation_id: i64) {
    assert_eq!(action["status"], "required");
    assert_eq!(action["kind"], "owned_service_start_failure");
    assert_eq!(action["manager_actionable"], true);
    assert_eq!(action["launch_operation_id"], LAUNCH_OPERATION_ID);
    assert_eq!(action["open_operation_id"], OPEN_OPERATION_ID);
    assert_eq!(action["binding_id"], BINDING_ID);
    assert_eq!(action["binding_generation"], 1);
    assert_eq!(action["task_id"], TASK_ID);
    assert_eq!(action["attempt_id"], ATTEMPT_ID);
    assert_eq!(action["schema_version"], 2);
    assert_eq!(action["failure_status"], "startup_failed_unknown");
    assert_eq!(action["stage"], "bootstrap");
    assert_eq!(action["error_code"], "NATIVE_REJECTED");
    assert_eq!(action["native_effect"], "unknown");
    assert_eq!(action["request_phase"], "provider_key_post");
    assert_eq!(action["http_status"], 403);
    assert_eq!(action["retry_authorized"], false);
    assert_eq!(
        action["source_observation"]["observation_id"],
        observation_id
    );
    assert_eq!(
        action["source_observation"]["source_stream_id"],
        "controller:owned-service"
    );
    assert_eq!(
        action["source_observation"]["kind"],
        "owned_service.start_failure"
    );
}

fn assert_failure_visible_through_store_reads(fixture: &Fixture, caller: Principal) {
    let operation = store_read(
        &fixture.db,
        caller.clone(),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(operation["result"], fixture.admission_result);
    assert!(operation.get("runtime_dispatch_action_required").is_none());
    let operation_action = &operation["manager_action_required"];
    assert_failure_action(operation_action, fixture.observation_id);

    let exceptions = store_read(
        &fixture.db,
        caller,
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert_eq!(
        exceptions["host_lifecycle"]["required_readback"],
        "operation.get before retrying admitted work"
    );
    let feed = &exceptions["manager_action_required"];
    assert_eq!(feed["status"], "required");
    assert_eq!(feed["total_items"], 1);
    assert_eq!(feed["returned_items"], 1);
    assert_eq!(feed["has_more"], false);
    let feed_items = feed["items"].as_array().unwrap();
    assert_eq!(feed_items.len(), 1);
    assert_failure_action(&feed_items[0], fixture.observation_id);
    assert_eq!(&feed_items[0], operation_action);
    let dispatch_feed = &exceptions["runtime_dispatch_action_required"];
    assert_eq!(dispatch_feed["status"], "clear");
    assert_eq!(dispatch_feed["total_items"], 0);
    assert_eq!(dispatch_feed["returned_items"], 0);
    assert!(dispatch_feed["items"].as_array().unwrap().is_empty());
}

fn stored_admission_result(db: &Connection) -> String {
    db.query_row(
        "SELECT result_json FROM operations WHERE operation_id=?1",
        [LAUNCH_OPERATION_ID],
        |row| row.get(0),
    )
    .unwrap()
}

fn native_mcp_tools_meta_key(prefix: &str, operation_id: &str) -> String {
    format!("{prefix}{}", model::digest(operation_id.as_bytes()))
}

fn handover_to_successor(fixture: &mut Fixture) {
    let receipt = store_mutate(
        &mut fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "gm.handover",
        json!({"client_request_id":"handover-failure-projection","client_id":SUCCESSOR_GM}),
    );
    assert_eq!(receipt["client_id"], SUCCESSOR_GM);
}

#[test]
fn current_gm_and_local_operator_read_the_linked_failure_without_changing_receipt() {
    let fixture = fixture();
    assert_eq!(
        stored_admission_result(&fixture.db),
        fixture.admission_result_json
    );

    assert_failure_visible_through_store_reads(&fixture, principal(ORIGINAL_GM, Role::Manager));
    assert_failure_visible_through_store_reads(&fixture, principal(OPERATOR, Role::Operator));

    assert_eq!(
        stored_admission_result(&fixture.db),
        fixture.admission_result_json
    );
}

#[test]
fn successor_gm_keeps_failure_readback_while_former_gm_loses_action_authority() {
    let mut fixture = fixture();
    let original_receipt = stored_admission_result(&fixture.db);
    handover_to_successor(&mut fixture);

    assert_failure_visible_through_store_reads(&fixture, principal(SUCCESSOR_GM, Role::Manager));

    let former_operation = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(former_operation["result"], fixture.admission_result);
    assert!(former_operation.get("manager_action_required").is_none());
    let former_exceptions = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert!(former_exceptions.get("manager_action_required").is_none());
    assert!(former_exceptions.get("host_lifecycle").is_none());
    assert_eq!(stored_admission_result(&fixture.db), original_receipt);
}

fn prepare_queued_owned_open_dispatch_failure(fixture: &mut Fixture) {
    let manifest = json!({
        "launch_manifest":{
            "state":"awaiting_capability",
            "binding":{
                "operation_id":OPEN_OPERATION_ID,
                "binding_id":BINDING_ID,
                "generation":1,
            },
            "task":{
                "task_id":TASK_ID,
                "observed_revision":1,
                "attempt_id":ATTEMPT_ID,
                "attempt_action":"use_existing",
            },
        }
    });
    let route = json!({
        "alias":"failure-opencode",
        "runtime":"opencode_v2",
        "native_options":{"service_id":"opencode","model":{"id":"hosted/model","providerID":"opencode-go"}},
        "owned_service":{"service_id":"opencode","service_version":"2.0.7"},
    });
    let birth_token = "b".repeat(64);
    let executable_sha256 = "d".repeat(64);
    let proof = json!({
        "schema_version":1,
        "status":"ready",
        "process":{
            "pid":43210,
            "birth_token":birth_token,
            "binary_sha256":executable_sha256,
        },
        "private_marker":PRIVATE_SERVICE_PROOF_MARKER,
    });
    fixture
        .db
        .execute(
            "UPDATE owned_service_starts
             SET state='service_observed',process_id=43210,process_birth_token=?1,
                 executable_sha256=?2,proof_json=?3,updated_at_ms=3
             WHERE launch_operation_id=?4",
            params![
                birth_token,
                executable_sha256,
                model::canonical(&proof).unwrap(),
                LAUNCH_OPERATION_ID,
            ],
        )
        .unwrap();
    fixture
        .db
        .execute(
            "UPDATE bindings SET state='opening',route_json=?1,state_json='{}',
                 native_root_id=NULL,native_scope_key=NULL,released_at_ms=NULL
             WHERE binding_id=?2 AND generation=1",
            params![model::canonical(&route).unwrap(), BINDING_ID],
        )
        .unwrap();
    fixture
        .db
        .execute(
            "UPDATE operations SET state='queued',effective_request_json=?1,due_at_ms=1,updated_at_ms=3
             WHERE operation_id=?2",
            params![model::canonical(&manifest).unwrap(), LAUNCH_OPERATION_ID],
        )
        .unwrap();
    fixture
        .db
        .execute(
            "UPDATE operations SET state='queued',due_at_ms=1,updated_at_ms=3
             WHERE operation_id=?1",
            [OPEN_OPERATION_ID],
        )
        .unwrap();
}

fn assert_dispatch_failure_action(action: &Value, observation_id: i64) {
    assert_eq!(action["status"], "required");
    assert_eq!(action["kind"], "owned_service_dispatch_failure");
    assert_eq!(action["manager_actionable"], true);
    assert_eq!(action["launch_operation_id"], LAUNCH_OPERATION_ID);
    assert_eq!(action["open_operation_id"], OPEN_OPERATION_ID);
    assert_eq!(action["binding_id"], BINDING_ID);
    assert_eq!(action["binding_generation"], 1);
    assert_eq!(action["task_id"], TASK_ID);
    assert_eq!(action["attempt_id"], ATTEMPT_ID);
    assert_eq!(action["failure_status"], "selection_error");
    assert_eq!(action["stage"], "runtime_command_select");
    assert_eq!(action["error_code"], "INVALID_PARAMS");
    assert_eq!(action["dispatch_state"], "queued");
    assert_eq!(action["native_effect"], "not_dispatched");
    assert_eq!(action["retry_authorized"], false);
    assert_eq!(
        action["source_observation"]["observation_id"],
        observation_id
    );
    assert_eq!(
        action["source_observation"]["source_stream_id"],
        "controller:owned-service"
    );
    assert_eq!(
        action["source_observation"]["kind"],
        "owned_service.dispatch_failure"
    );
}

#[test]
fn queued_open_selection_failure_is_deduplicated_and_survives_handover_and_departure() {
    let mut fixture = fixture();
    prepare_queued_owned_open_dispatch_failure(&mut fixture);
    let original_receipt = stored_admission_result(&fixture.db);
    assert_eq!(original_receipt, fixture.admission_result_json);

    assert!(
        super::operations::record_owned_open_dispatch_failure(
            &fixture.db,
            BINDING_ID,
            1,
            "runtime_command_select",
            "selection_error",
            "INVALID_PARAMS",
            "queued",
        )
        .unwrap()
    );
    assert!(
        !super::operations::record_owned_open_dispatch_failure(
            &fixture.db,
            BINDING_ID,
            1,
            "runtime_command_select",
            "selection_error",
            "INVALID_PARAMS",
            "queued",
        )
        .unwrap()
    );

    let event_key = format!("owned-service-dispatch-failure:{LAUNCH_OPERATION_ID}");
    let (source_stream, operation_id, binding_id, generation, kind, payload_json): (
        String,
        String,
        String,
        i64,
        String,
        String,
    ) = fixture
        .db
        .query_row(
            "SELECT source_stream_id,operation_id,binding_id,binding_generation,kind,payload_json
             FROM observations WHERE source_event_key=?1",
            [&event_key],
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
        )
        .unwrap();
    assert_eq!(source_stream, "controller:owned-service");
    assert_eq!(operation_id, LAUNCH_OPERATION_ID);
    assert_eq!(binding_id, BINDING_ID);
    assert_eq!(generation, 1);
    assert_eq!(kind, "owned_service.dispatch_failure");
    let payload: Value = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(payload.as_object().unwrap().len(), 6);
    assert_eq!(payload["schema_version"], 1);
    assert_eq!(payload["status"], "selection_error");
    assert_eq!(payload["stage"], "runtime_command_select");
    assert_eq!(payload["error_code"], "INVALID_PARAMS");
    assert_eq!(payload["native_effect"], "not_dispatched");
    assert_eq!(payload["retry_authorized"], false);
    let observation_id: i64 = fixture
        .db
        .query_row(
            "SELECT observation_id FROM observations WHERE source_event_key=?1",
            [&event_key],
            |row| row.get(0),
        )
        .unwrap();
    let observation_count: i64 = fixture
        .db
        .query_row(
            "SELECT count(*) FROM observations WHERE source_event_key=?1",
            [&event_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(observation_count, 1);

    let manager = principal(ORIGINAL_GM, Role::Manager);
    let operation = store_read(
        &fixture.db,
        manager.clone(),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(operation["result"], fixture.admission_result);
    let operation_action = &operation["runtime_dispatch_action_required"];
    assert_dispatch_failure_action(operation_action, observation_id);
    let exceptions = store_read(
        &fixture.db,
        manager,
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    let exception_feed = &exceptions["runtime_dispatch_action_required"];
    assert_eq!(exception_feed["status"], "required");
    assert_eq!(exception_feed["total_items"], 1);
    assert_eq!(exception_feed["returned_items"], 1);
    assert_eq!(exception_feed["items"].as_array().unwrap().len(), 1);
    let exception_action = &exception_feed["items"][0];
    assert_dispatch_failure_action(exception_action, observation_id);
    assert_eq!(exception_action, operation_action);
    assert!(
        !serde_json::to_string(&(&operation, &exceptions))
            .unwrap()
            .contains(PRIVATE_SERVICE_PROOF_MARKER)
    );

    handover_to_successor(&mut fixture);
    let successor = principal(SUCCESSOR_GM, Role::Manager);
    let successor_operation = store_read(
        &fixture.db,
        successor.clone(),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(successor_operation["result"], fixture.admission_result);
    let successor_action = &successor_operation["runtime_dispatch_action_required"];
    assert_dispatch_failure_action(successor_action, observation_id);
    assert_eq!(successor_action, operation_action);
    let former_operation = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(former_operation["result"], fixture.admission_result);
    assert!(
        former_operation
            .get("runtime_dispatch_action_required")
            .is_none()
    );
    let former_exceptions = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert!(
        former_exceptions
            .get("runtime_dispatch_action_required")
            .is_none()
    );

    fixture
        .db
        .execute(
            "UPDATE owned_service_starts SET state='service_departed',updated_at_ms=4
             WHERE launch_operation_id=?1",
            [LAUNCH_OPERATION_ID],
        )
        .unwrap();
    fixture
        .db
        .execute(
            "UPDATE operations SET state='rejected',settled_at_ms=4,updated_at_ms=4 WHERE operation_id=?1",
            [LAUNCH_OPERATION_ID],
        )
        .unwrap();
    let departed_operation = store_read(
        &fixture.db,
        successor.clone(),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(departed_operation["result"], fixture.admission_result);
    assert_eq!(departed_operation["state"], "rejected");
    let departed_action = &departed_operation["runtime_dispatch_action_required"];
    assert_dispatch_failure_action(departed_action, observation_id);
    let departed_exceptions = store_read(
        &fixture.db,
        successor,
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    let departed_feed = &departed_exceptions["runtime_dispatch_action_required"];
    assert_eq!(departed_feed["total_items"], 1);
    assert_eq!(&departed_feed["items"][0], departed_action);
    assert!(
        !serde_json::to_string(&(&departed_operation, &departed_exceptions))
            .unwrap()
            .contains(PRIVATE_SERVICE_PROOF_MARKER)
    );

    let rejected_result = json!({"code":"INVALID_PARAMS","message":"safe selector rejection"});
    fixture
        .db
        .execute(
            "UPDATE operations SET state='rejected',sent_at_ms=5,settled_at_ms=5,result_json=?1,updated_at_ms=5
             WHERE operation_id=?2",
            params![
                model::canonical(&rejected_result).unwrap(),
                OPEN_OPERATION_ID,
            ],
        )
        .unwrap();
    let sent_rejection = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    let uncertain_action = &sent_rejection["runtime_dispatch_action_required"];
    assert_eq!(uncertain_action["dispatch_state"], "rejected");
    assert_eq!(uncertain_action["error_code"], "INVALID_PARAMS");
    assert_eq!(uncertain_action["native_effect"], "unknown");
    assert_eq!(uncertain_action["retry_authorized"], false);
    let uncertain_exceptions = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert_eq!(
        &uncertain_exceptions["runtime_dispatch_action_required"]["items"][0],
        uncertain_action
    );

    fixture
        .db
        .execute(
            "UPDATE operations SET state='settled',settled_at_ms=6,updated_at_ms=6
             WHERE operation_id=?1",
            [OPEN_OPERATION_ID],
        )
        .unwrap();
    let settled = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert!(settled.get("runtime_dispatch_action_required").is_none());
    let settled_exceptions = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert_eq!(
        settled_exceptions["runtime_dispatch_action_required"]["total_items"],
        0
    );
    assert!(
        settled_exceptions["runtime_dispatch_action_required"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let retained_event_count: i64 = fixture
        .db
        .query_row(
            "SELECT count(*) FROM observations WHERE source_event_key=?1",
            [&event_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_event_count, 1);
    assert_eq!(stored_admission_result(&fixture.db), original_receipt);
}

#[test]
fn corrupt_optional_startup_diagnostic_returns_a_safe_gap_without_breaking_reads() {
    let mut fixture = fixture();
    handover_to_successor(&mut fixture);
    let original_receipt = stored_admission_result(&fixture.db);
    let corrupt_payload = json!({
        "schema_version":2,
        "status":"startup_failed_unknown",
        "stage":"bootstrap",
        "error_code":"NATIVE_REJECTED",
        "native_effect":"unknown",
        "request_phase":"provider_key_post",
        "http_status":403,
        "unrecognized_private_field":DIAGNOSTIC_MARKER,
    });
    fixture
        .db
        .execute(
            "UPDATE observations SET payload_json=?2 WHERE observation_id=?1",
            params![
                fixture.observation_id,
                model::canonical(&corrupt_payload).unwrap(),
            ],
        )
        .unwrap();

    let successor = principal(SUCCESSOR_GM, Role::Manager);
    let operation = store_read(
        &fixture.db,
        successor.clone(),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(operation["result"], fixture.admission_result);
    let gap = &operation["manager_action_required"];
    assert_eq!(gap["status"], "readback_required");
    assert_eq!(gap["kind"], "owned_service_start_diagnostic_gap");
    assert_eq!(gap["manager_actionable"], true);
    assert_eq!(gap["source_operation_id"], LAUNCH_OPERATION_ID);
    assert_eq!(gap["error_code"], "OWNED_SERVICE_START_DIAGNOSTIC_CORRUPT");
    assert_eq!(gap["native_effect"], "unknown");
    assert_eq!(gap["retry_authorized"], false);
    assert!(gap.get("stage").is_none());
    assert!(gap.get("http_status").is_none());
    assert!(serde_json::to_vec(gap).unwrap().len() <= 2048);
    assert!(
        !serde_json::to_string(&operation)
            .unwrap()
            .contains(DIAGNOSTIC_MARKER)
    );

    let exceptions = store_read(
        &fixture.db,
        successor,
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    let feed = &exceptions["manager_action_required"];
    let feed_gap = feed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["source_operation_id"] == LAUNCH_OPERATION_ID)
        .unwrap();
    assert_eq!(feed_gap, gap);
    assert!(
        !serde_json::to_string(&exceptions)
            .unwrap()
            .contains(DIAGNOSTIC_MARKER)
    );
    assert_eq!(stored_admission_result(&fixture.db), original_receipt);
}

#[test]
fn native_mcp_failure_survives_successful_readback_and_successor_gm_handover() {
    const RAW_MESSAGE: &str = "RAW_NATIVE_MCP_MESSAGE_private_9137";
    const RAW_AUTH: &str = "RAW_NATIVE_MCP_AUTH_private_2841";
    const RAW_PATH: &str = "C:/private/native-mcp/credential-file";

    let mut fixture = fixture();
    let original_result = stored_admission_result(&fixture.db);
    let latest_failure = json!({
        "schema_version":1,
        "code":"NATIVE_MCP_READBACK_TIMEOUT",
        "stage":"native_capability_readback",
        "recorded_at_ms":7,
        "category":"native_service_unavailable",
        "message":RAW_MESSAGE,
        "authorization":RAW_AUTH,
        "path":RAW_PATH,
    });
    let retry_marker = json!({
        "state":"retry_wait",
        "attempts":2,
        "last_attempt_at_ms":7,
        "next_retry_at_ms":60_007,
        "last_failure_category":"native_service_unavailable",
        "last_error_code":"NATIVE_MCP_READBACK_TIMEOUT",
        "last_error_stage":"native_capability_readback",
        "dispatch_permitted":false,
    });
    set_native_mcp_manifest(&fixture, retry_marker, latest_failure.clone());

    let original_before_readback = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(original_before_readback["result"], fixture.admission_result);
    assert_eq!(
        original_before_readback["native_mcp_readback"]["state"],
        "retry_wait"
    );
    assert_eq!(
        original_before_readback["native_mcp_readback"]["latest_failure"],
        json!({
            "schema_version":1,
            "code":"NATIVE_MCP_READBACK_TIMEOUT",
            "stage":"native_capability_readback",
            "recorded_at_ms":7,
            "category":"native_service_unavailable",
        })
    );

    // A successful partial MCP list read replaces the retry marker while the
    // producer's sibling latest-failure fact remains durable.
    let observed_marker = json!({
        "state":"observed_partial",
        "attempts":2,
        "first_observed_at_ms":7,
        "last_observed_at_ms":12,
        "next_retry_at_ms":60_012,
        "observation_id":42,
        "semantic_digest":format!("sha256:{}", "a".repeat(64)),
        "dispatch_permitted":false,
    });
    set_native_mcp_manifest(&fixture, observed_marker, latest_failure);
    let original_after_readback = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(original_after_readback["result"], fixture.admission_result);
    assert_eq!(
        original_after_readback["native_mcp_readback"]["state"],
        "observed_partial"
    );
    assert_eq!(
        original_after_readback["native_mcp_readback"]["latest_failure"],
        original_before_readback["native_mcp_readback"]["latest_failure"]
    );

    handover_to_successor(&mut fixture);
    let successor_operation = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(successor_operation["result"], fixture.admission_result);
    assert_eq!(
        successor_operation["native_mcp_readback"],
        original_after_readback["native_mcp_readback"]
    );
    let former_operation = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(former_operation["result"], fixture.admission_result);
    assert!(former_operation.get("native_mcp_readback").is_none());
    assert!(former_operation.get("manager_action_required").is_none());
    assert!(
        former_operation
            .get("runtime_dispatch_action_required")
            .is_none()
    );

    for public in [
        &original_before_readback,
        &original_after_readback,
        &successor_operation,
        &former_operation,
    ] {
        let encoded = serde_json::to_string(public).unwrap();
        assert!(!encoded.contains(RAW_MESSAGE));
        assert!(!encoded.contains(RAW_AUTH));
        assert!(!encoded.contains(RAW_PATH));
    }

    // Malformed optional failure data is a bounded diagnostic, not a failed
    // Operation read and not a channel for private native details.
    let malformed_failure = json!({
        "schema_version":9,
        "code":"private malformed code",
        "stage":"unknown private stage",
        "recorded_at_ms":-1,
        "category":"unknown private category",
        "message":RAW_MESSAGE,
        "authorization":RAW_AUTH,
        "path":RAW_PATH,
    });
    let observed_marker = json!({
        "state":"observed_partial",
        "attempts":2,
        "next_retry_at_ms":60_012,
        "dispatch_permitted":false,
    });
    set_native_mcp_manifest(&fixture, observed_marker, malformed_failure);
    let corrupted = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(corrupted["result"], fixture.admission_result);
    assert_eq!(
        corrupted["native_mcp_readback"]["state"],
        "observed_partial"
    );
    assert_eq!(
        corrupted["native_mcp_readback"]["latest_failure"],
        json!({"schema_version":1,"code":"NATIVE_MCP_DIAGNOSTIC_CORRUPT"})
    );
    let encoded = serde_json::to_string(&corrupted).unwrap();
    assert!(!encoded.contains(RAW_MESSAGE));
    assert!(!encoded.contains(RAW_AUTH));
    assert!(!encoded.contains(RAW_PATH));
    assert_eq!(stored_admission_result(&fixture.db), original_result);
}

#[test]
fn native_mcp_tools_preflight_failure_is_bounded_and_visible_to_successor_gm() {
    const RAW_MESSAGE: &str = "RAW_C8_ERROR_MESSAGE_private_6312";
    const RAW_AUTH: &str = "RAW_C8_AUTH_private_9124";
    const RAW_CONFIG: &str = "RAW_C8_CONFIG_private_7721";
    const RAW_ENDPOINT: &str = "https://private.example.invalid/mcp";
    const RAW_SCHEMA: &str = "RAW_C8_TOOL_SCHEMA_private_0835";

    let mut fixture = fixture();
    let original_receipt = stored_admission_result(&fixture.db);
    let launch_digest = format!("sha256:{}", "d".repeat(64));
    let record_key =
        native_mcp_tools_meta_key("launcher:native_mcp_tools:v1:", LAUNCH_OPERATION_ID);
    let schedule_key = native_mcp_tools_meta_key(
        "launcher:native_mcp_tools:supervisor:v1:",
        LAUNCH_OPERATION_ID,
    );
    let before_c8 = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert!(before_c8.get("native_mcp_tools_readback").is_none());
    let record = json!({
        "schema_version":1,
        "kind":"launcher_native_mcp_tools",
        "operation_id":LAUNCH_OPERATION_ID,
        "launch_identity_digest":launch_digest,
        "service":{"endpoint":RAW_ENDPOINT,"authorization":RAW_AUTH},
        "challenge":{"metadata":{"nonce":RAW_CONFIG}},
        "tools_readback":{"native_discovered":{"tools":[{"schema":RAW_SCHEMA}]}},
        "last_error":{
            "stage":"challenge_preflight",
            "code":"NATIVE_MCP_PROOF_SOURCE",
            "recorded_at_ms":41,
            "message":RAW_MESSAGE,
            "authorization":RAW_AUTH,
            "config":RAW_CONFIG,
            "endpoint":RAW_ENDPOINT,
            "tool_schema":RAW_SCHEMA,
        },
    });
    let schedule = json!({
        "schema_version":1,
        "kind":"launcher_native_mcp_tools_supervisor",
        "operation_id":LAUNCH_OPERATION_ID,
        "launch_identity_digest":launch_digest,
        "state":"retry_wait",
        "claim_generation":1,
        "started_at_ms":null,
        "next_retry_at_ms":15_041,
        "finished_at_ms":41,
        "failure_attempts":1,
        "last_error_code":"NATIVE_MCP_PROOF_SOURCE",
    });
    set_meta(&fixture.db, &record_key, &record).unwrap();
    set_meta(&fixture.db, &schedule_key, &schedule).unwrap();

    let original_operation = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    let expected = json!({
        "schema_version":1,
        "state":"retry_wait",
        "failure_attempts":1,
        "next_retry_at_ms":15_041,
        "last_error_code":"NATIVE_MCP_PROOF_SOURCE",
        "latest_failure":{
            "schema_version":1,
            "code":"NATIVE_MCP_PROOF_SOURCE",
            "stage":"challenge_preflight",
            "recorded_at_ms":41,
        },
        "model_consumed":"unknown",
        "dispatch_permitted":false,
    });
    assert_eq!(original_operation["result"], fixture.admission_result);
    assert_eq!(original_operation["native_mcp_tools_readback"], expected);
    let original_json = serde_json::to_string(&original_operation).unwrap();
    for marker in [RAW_MESSAGE, RAW_AUTH, RAW_CONFIG, RAW_ENDPOINT, RAW_SCHEMA] {
        assert!(!original_json.contains(marker));
    }

    handover_to_successor(&mut fixture);
    let successor_operation = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(successor_operation["result"], fixture.admission_result);
    assert_eq!(successor_operation["native_mcp_tools_readback"], expected);
    let former_operation = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(former_operation["result"], fixture.admission_result);
    assert!(former_operation.get("native_mcp_tools_readback").is_none());
    let successor_json = serde_json::to_string(&successor_operation).unwrap();
    for marker in [RAW_MESSAGE, RAW_AUTH, RAW_CONFIG, RAW_ENDPOINT, RAW_SCHEMA] {
        assert!(!successor_json.contains(marker));
    }

    // Invalid optional record shape and schedule fields must leave Operation
    // readback available while returning only the closed corruption marker.
    let invalid_record = model::canonical(&json!({
        "schema_version":1,
        "kind":"wrong_private_record_kind",
        "operation_id":LAUNCH_OPERATION_ID,
        "message":RAW_MESSAGE,
        "authorization":RAW_AUTH,
    }))
    .unwrap();
    fixture
        .db
        .execute(
            "UPDATE meta SET value_json=?1 WHERE key=?2",
            params![invalid_record, record_key],
        )
        .unwrap();
    let malformed_record = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(malformed_record["result"], fixture.admission_result);
    assert_eq!(
        malformed_record["native_mcp_tools_readback"],
        json!({
            "schema_version":1,
            "state":"unknown",
            "latest_failure":{
                "schema_version":1,
                "code":"NATIVE_MCP_TOOLS_DIAGNOSTIC_CORRUPT",
            },
            "model_consumed":"unknown",
            "dispatch_permitted":false,
        })
    );
    let public_record = serde_json::to_string(&malformed_record).unwrap();
    assert!(!public_record.contains(RAW_MESSAGE));
    assert!(!public_record.contains(RAW_AUTH));

    set_meta(&fixture.db, &record_key, &record).unwrap();
    set_meta(
        &fixture.db,
        &schedule_key,
        &json!({
            "schema_version":1,
            "kind":"launcher_native_mcp_tools_supervisor",
            "operation_id":LAUNCH_OPERATION_ID,
            "launch_identity_digest":launch_digest,
            "state":"retry_wait",
            "claim_generation":1,
            "started_at_ms":null,
            "next_retry_at_ms":-1,
            "finished_at_ms":41,
            "failure_attempts":1,
            "last_error_code":"NATIVE_MCP_PROOF_SOURCE",
            "message":RAW_MESSAGE,
        }),
    )
    .unwrap();
    let malformed_schedule = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(malformed_schedule["result"], fixture.admission_result);
    assert_eq!(
        malformed_schedule["native_mcp_tools_readback"]["latest_failure"]["code"],
        "NATIVE_MCP_TOOLS_DIAGNOSTIC_CORRUPT"
    );
    assert!(
        !serde_json::to_string(&malformed_schedule)
            .unwrap()
            .contains(RAW_MESSAGE)
    );

    // A preparation failure can be retained by the supervisor before a C8
    // private record exists. Expose only its safe code and retry status; do not
    // invent a stage or timestamp.
    fixture
        .db
        .execute("DELETE FROM meta WHERE key=?1", [&record_key])
        .unwrap();
    set_meta(
        &fixture.db,
        &schedule_key,
        &json!({
            "schema_version":1,
            "kind":"launcher_native_mcp_tools_supervisor",
            "operation_id":LAUNCH_OPERATION_ID,
            "launch_identity_digest":launch_digest,
            "state":"retry_wait",
            "claim_generation":1,
            "started_at_ms":null,
            "next_retry_at_ms":30_000,
            "finished_at_ms":41,
            "failure_attempts":2,
            "last_error_code":"PRIVATE_ARTIFACT_REFERENCE",
            "message":RAW_MESSAGE,
        }),
    )
    .unwrap();
    let schedule_only = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(schedule_only["result"], fixture.admission_result);
    assert_eq!(
        schedule_only["native_mcp_tools_readback"],
        json!({
            "schema_version":1,
            "state":"retry_wait",
            "failure_attempts":2,
            "next_retry_at_ms":30_000,
            "last_error_code":"PRIVATE_ARTIFACT_REFERENCE",
            "latest_failure":null,
            "model_consumed":"unknown",
            "dispatch_permitted":false,
        })
    );
    assert!(
        !serde_json::to_string(&schedule_only)
            .unwrap()
            .contains(RAW_MESSAGE)
    );
    assert_eq!(stored_admission_result(&fixture.db), original_receipt);
}

fn persist_prior_host_interruption(fixture: &mut Fixture) {
    set_meta(&fixture.db, "host_epoch", &json!(2)).unwrap();
    set_meta(
        &fixture.db,
        "host:lifecycle:v1",
        &json!({
            "schema_version":1,
            "host_epoch":1,
            "state":"running",
            "started_at_ms":1,
            "updated_at_ms":2,
        }),
    )
    .unwrap();
    let tx = fixture
        .db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    super::host_lifecycle::start(&tx, 3).unwrap();
    super::host_lifecycle::ready(&tx, 4).unwrap();
    tx.commit().unwrap();
}

#[test]
fn successor_manager_gets_readback_action_for_unknown_start_after_host_interruption() {
    let mut fixture = fixture();
    let original_result = stored_admission_result(&fixture.db);
    fixture
        .db
        .execute(
            "DELETE FROM observations WHERE observation_id=?1",
            [fixture.observation_id],
        )
        .unwrap();
    let terminal_failure_count: i64 = fixture
        .db
        .query_row(
            "SELECT count(*) FROM observations WHERE source_event_key=?1 \
             OR (operation_id=?2 AND kind='owned_service.start_failure')",
            params![
                format!("owned-service-start-failure:{LAUNCH_OPERATION_ID}"),
                LAUNCH_OPERATION_ID,
            ],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(terminal_failure_count, 0);
    let (start_state, process_id, process_birth_token, executable_sha256, proof_json): (
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        String,
    ) = fixture
        .db
        .query_row(
            "SELECT state,process_id,process_birth_token,executable_sha256,proof_json \
             FROM owned_service_starts WHERE launch_operation_id=?1",
            [LAUNCH_OPERATION_ID],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(start_state, "outcome_unknown");
    assert!(process_id.is_none());
    assert!(process_birth_token.is_none());
    assert!(executable_sha256.is_none());
    assert_eq!(proof_json, "{}");

    // A pending unknown start without a matching interruption stays unresolved
    // and does not acquire a fabricated crash diagnosis.
    let live_pending = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(live_pending["result"], fixture.admission_result);
    assert!(live_pending.get("manager_action_required").is_none());
    let pending_exceptions = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert!(pending_exceptions["host_lifecycle"]["latest_failure"].is_null());
    assert_eq!(
        pending_exceptions["manager_action_required"]["status"],
        "clear"
    );
    assert!(
        pending_exceptions["manager_action_required"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    persist_prior_host_interruption(&mut fixture);
    let original_launch = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(original_launch["result"], fixture.admission_result);
    let action = &original_launch["manager_action_required"];
    assert_eq!(action["status"], "required");
    assert_eq!(action["kind"], "owned_service_start_readback_required");
    assert_eq!(action["manager_actionable"], true);
    assert_eq!(action["launch_operation_id"], LAUNCH_OPERATION_ID);
    assert_eq!(action["open_operation_id"], OPEN_OPERATION_ID);
    assert_eq!(action["binding_id"], BINDING_ID);
    assert_eq!(action["binding_generation"], 1);
    assert_eq!(action["task_id"], TASK_ID);
    assert_eq!(action["attempt_id"], ATTEMPT_ID);
    assert_eq!(action["start_state"], "outcome_unknown");
    assert_eq!(action["native_effect"], "unknown");
    assert_eq!(action["retry_authorized"], false);
    assert_eq!(
        action["host_interruption"],
        json!({
            "source":"host_lifecycle.latest_failure",
            "host_epoch":1,
            "observed_at_ms":3,
        })
    );
    assert_eq!(
        action["next_readback"],
        json!({
            "method":"operation.get",
            "params":{"operation_id":LAUNCH_OPERATION_ID},
        })
    );

    let original_exceptions = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert_eq!(
        original_exceptions["host_lifecycle"]["latest_failure"]["error_code"],
        "HOST_INTERRUPTED"
    );
    let original_feed = &original_exceptions["manager_action_required"];
    assert_eq!(original_feed["status"], "required");
    let original_feed_item = original_feed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["launch_operation_id"] == LAUNCH_OPERATION_ID)
        .unwrap();
    assert_eq!(original_feed_item, action);

    handover_to_successor(&mut fixture);
    let successor_launch = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(successor_launch["result"], fixture.admission_result);
    assert_eq!(successor_launch["manager_action_required"], action.clone());
    let successor_open = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":OPEN_OPERATION_ID}),
    );
    assert_eq!(successor_open["manager_action_required"], action.clone());
    let successor_exceptions = store_read(
        &fixture.db,
        principal(SUCCESSOR_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    let successor_feed = &successor_exceptions["manager_action_required"];
    assert_eq!(successor_feed["status"], "required");
    let successor_feed_item = successor_feed["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["launch_operation_id"] == LAUNCH_OPERATION_ID)
        .unwrap();
    assert_eq!(successor_feed_item, action);

    let former_launch = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "operation.get",
        json!({"operation_id":LAUNCH_OPERATION_ID}),
    );
    assert_eq!(former_launch["result"], fixture.admission_result);
    assert!(former_launch.get("manager_action_required").is_none());
    let former_exceptions = store_read(
        &fixture.db,
        principal(ORIGINAL_GM, Role::Manager),
        "swarm.exceptions.get",
        json!({"after":0,"limit":32}),
    );
    assert!(former_exceptions.get("manager_action_required").is_none());
    assert!(former_exceptions.get("host_lifecycle").is_none());
    assert_eq!(stored_admission_result(&fixture.db), original_result);
}
