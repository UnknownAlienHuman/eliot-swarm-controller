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
}

fn stored_admission_result(db: &Connection) -> String {
    db.query_row(
        "SELECT result_json FROM operations WHERE operation_id=?1",
        [LAUNCH_OPERATION_ID],
        |row| row.get(0),
    )
    .unwrap()
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
