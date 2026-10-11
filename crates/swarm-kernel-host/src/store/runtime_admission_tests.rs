//! Regression coverage for pre-dispatch launch admission failures.
use super::*;
use crate::model::{Principal, Role};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const MODULE_ID: &str = "runtime-admission-module";
const MODULE_LINK_ID: &str = "runtime-admission-link";
const MANAGER_ID: &str = "runtime-admission-manager";
const BINDING_ID: &str = "runtime-admission-binding";
const TASK_ID: &str = "runtime-admission-task";
const OTHER_TASK_ID: &str = "runtime-admission-other-task";
const ATTEMPT_ID: &str = "runtime-admission-attempt";
const LAUNCH_ID: &str = "runtime-admission-launch";
const OPEN_ID: &str = "runtime-admission-open";

fn module_principal() -> Principal {
    Principal {
        link_id: MODULE_LINK_ID.to_owned(),
        client_id: MODULE_ID.to_owned(),
        role: Role::Module,
    }
}

fn manager_principal() -> Principal {
    Principal {
        link_id: "runtime-admission-manager-link".to_owned(),
        client_id: MANAGER_ID.to_owned(),
        role: Role::Manager,
    }
}

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    db.execute_batch(SCRIPT_SCHEMA).unwrap();
    db.execute_batch(GITHUB_SCHEMA).unwrap();

    set_meta(
        &db,
        &format!("client:{MODULE_ID}"),
        &json!({
            "role":"module",
            "disabled":false,
            "binding_id":BINDING_ID,
            "binding_generation":1,
        }),
    )
    .unwrap();
    set_meta(
        &db,
        &format!("client:{MANAGER_ID}"),
        &json!({"role":"manager","disabled":false}),
    )
    .unwrap();
    set_meta(
        &db,
        "gm",
        &json!({
            "client_id":MANAGER_ID,
            "binding_id":null,
            "binding_generation":null,
            "epoch":1,
        }),
    )
    .unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();

    let route = json!({
        "alias":"runtime-admission-opencode",
        "runtime":"opencode_v2",
        "module_artifact_id":"eliot-opencode-v2.http.1",
        "enabled":true,
        "native_options":{"service_id":"opencode","model":{"id":"test/model","providerID":"test-provider"}},
    });
    let observation = json!({
        "module_client_id":MODULE_ID,
        "module_link_id":MODULE_LINK_ID,
        "connection":"connected",
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'runtime-admission-lane','runtime-admission-instance','eliot-opencode-v2.http.1','opening',?2,?3,1)",
        params![
            BINDING_ID,
            model::canonical(&route).unwrap(),
            model::canonical(&observation).unwrap(),
        ],
    )
    .unwrap();

    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'runtime-admission-project',1,'open','{}',1,1), \
               (?2,'runtime-admission-project',1,'open','{}',1,1)",
        params![TASK_ID, OTHER_TASK_ID],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,'{}',?3,'controller',?4,1,'reserved','[]',1,1)",
        params![ATTEMPT_ID, TASK_ID, MANAGER_ID, BINDING_ID],
    )
    .unwrap();

    db
}

fn insert_corrupted_launcher_open(db: &Connection) {
    let launch_effective = json!({
        "launch_manifest":{
            "binding":{"operation_id":OPEN_ID},
        },
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'runtime-admission-launch-request','swarm.launch','{}',?3,?4,?5,?6,1,'queued','{}',0,1,1)",
        params![
            LAUNCH_ID,
            MANAGER_ID,
            model::canonical(&launch_effective).unwrap(),
            TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
        ],
    )
    .unwrap();

    let open_request = json!({"client_request_id":"runtime-admission-open-request"});
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,prerequisite_operation_id,state,native_refs_json,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'runtime-admission-open-request','agent.open',?3,'{}',?4,?5,?6,1,?7,'queued','{}',0,1,1)",
        params![
            OPEN_ID,
            MANAGER_ID,
            model::canonical(&open_request).unwrap(),
            OTHER_TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
            LAUNCH_ID,
        ],
    )
    .unwrap();
}

fn insert_launch_parent_with_route_mismatch(db: &Connection) {
    let manifest = json!({
        "state":"awaiting_native_mcp",
        "progress":{"task_dispatch":"not_started"},
        "runtime":{
            "dispatch_permitted":false,
            "route":{"alias":"another-route"},
        },
        "task":{
            "task_id":TASK_ID,
            "observed_revision":1,
            "attempt_id":ATTEMPT_ID,
        },
        "binding":{"binding_id":BINDING_ID,"generation":1},
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,'runtime-admission-launch-parent-request','swarm.launch','{}',?3,?4,?5,?6,1,'queued','{}',0,1,1)",
        params![
            LAUNCH_ID,
            MANAGER_ID,
            model::canonical(&json!({"launch_manifest":manifest})).unwrap(),
            TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
        ],
    )
    .unwrap();

    db.execute(
        "UPDATE bindings SET state='ready',state_json=json_set(state_json,'$.connection','connected') WHERE binding_id=?1 AND generation=1",
        [BINDING_ID],
    )
    .unwrap();
}

#[test]
fn module_link_disconnect_retains_exact_reason_and_transition_in_one_transaction() {
    let mut db = fixture();

    assert!(
        runtime::disconnected(&mut db, &module_principal(), DisconnectReason::PeerEof).unwrap()
    );

    let (binding_id, generation, recorded_at_ms, payload_json): (String, i64, i64, String) = db
        .query_row(
            "SELECT binding_id,binding_generation,recorded_at_ms,payload_json \
             FROM observations WHERE kind='module.link_disconnected'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    let payload: Value = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(binding_id, BINDING_ID);
    assert_eq!(generation, 1);
    assert!(recorded_at_ms > 0);
    assert_eq!(payload["module_link_id"], MODULE_LINK_ID);
    assert_eq!(payload["reason"], "peer_eof");
    assert_eq!(payload["prior_state"], "opening");
    assert_eq!(payload["transitioned"], true);
    assert_eq!(payload["disconnected_at_ms"], recorded_at_ms);
    assert_eq!(payload.as_object().unwrap().len(), 5);

    let binding = operations::get_binding(&db, BINDING_ID, 1).unwrap();
    assert_eq!(binding["state"], "reconciling");
    assert_eq!(binding["observation"]["connection"], "disconnected");
}

#[test]
fn stale_module_link_does_not_mutate_or_observe_new_binding_generation() {
    let mut db = fixture();
    let new_link = "runtime-admission-link-generation-two";
    let route_json: String = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id=?1 AND generation=1",
            [BINDING_ID],
            |row| row.get(0),
        )
        .unwrap();
    let state_json = model::canonical(&json!({
        "module_client_id":MODULE_ID,
        "module_link_id":new_link,
        "connection":"connected",
    }))
    .unwrap();
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,2,'runtime-admission-lane-two','runtime-admission-instance','eliot-opencode-v2.http.1','ready',?2,?3,2)",
        params![BINDING_ID, route_json, state_json],
    )
    .unwrap();
    set_meta(
        &db,
        &format!("client:{MODULE_ID}"),
        &json!({
            "role":"module",
            "disabled":false,
            "binding_id":BINDING_ID,
            "binding_generation":2,
        }),
    )
    .unwrap();

    assert!(
        !runtime::disconnected(&mut db, &module_principal(), DisconnectReason::PeerEof).unwrap()
    );

    let binding = operations::get_binding(&db, BINDING_ID, 2).unwrap();
    assert_eq!(binding["state"], "ready");
    assert_eq!(binding["observation"]["module_link_id"], new_link);
    assert_eq!(binding["observation"]["connection"], "connected");
    let observations: i64 = db
        .query_row(
            "SELECT count(*) FROM observations WHERE kind='module.link_disconnected'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(observations, 0);
}

#[test]
fn host_stop_is_retained_as_a_distinct_disconnect_reason() {
    let mut db = fixture();
    assert!(
        runtime::disconnected(&mut db, &module_principal(), DisconnectReason::HostStopping)
            .unwrap()
    );
    let payload_json: String = db
        .query_row(
            "SELECT payload_json FROM observations WHERE kind='module.link_disconnected'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let payload: Value = serde_json::from_str(&payload_json).unwrap();
    assert_eq!(payload["reason"], "host_stopping");
    assert_eq!(payload["prior_state"], "opening");
}

#[test]
fn scope_predicate_is_retained_after_mutation_rejection_savepoint() {
    let mut db = fixture();
    // Earlier admission-only fixtures stop before provider-condition reads.
    // This mutation exercises the real enabled-route admission path as well.
    db.execute_batch(PROVIDER_CONDITION_SCHEMA).unwrap();
    insert_launch_parent_with_route_mismatch(&db);
    let binding = operations::get_binding(&db, BINDING_ID, 1).unwrap();
    // Current route enablement is checked before the parent tuple. Use the
    // retained fixture route so this test reaches the intended stale-scope guard.
    let config = Config {
        routes: vec![serde_json::from_value(binding["route"].clone()).unwrap()],
        ..Config::default()
    };
    let admission = runtime::binding_route_admission(&db, &binding, Some(&config), None).unwrap();
    assert!(matches!(
        admission.decision,
        provider_conditions::RouteAdmissionDecision::Admit
    ));
    let request = json!({
        "client_request_id":"runtime-admission-dispatch-savepoint",
        "attempt_id":ATTEMPT_ID,
        "text":"fixture dispatch input",
        "launch_operation_id":LAUNCH_ID,
    });
    let error = mutate(
        &mut db,
        &manager_principal(),
        "task.dispatch",
        &request,
        &config,
    )
    .expect_err("mismatched launch route is durably rejected");
    assert_eq!(error.code, "STALE_LAUNCH_DISPATCH_SCOPE");
    assert_eq!(
        error.message,
        "launch dispatch scope failed predicate: binding.route.alias_matches_launch_manifest"
    );

    let (state, result_json): (String, String) = db
        .query_row(
            "SELECT state,result_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![MANAGER_ID, "runtime-admission-dispatch-savepoint"],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let retained: Value = serde_json::from_str(&result_json).unwrap();
    assert_eq!(state, "rejected");
    assert_eq!(retained["code"], "STALE_LAUNCH_DISPATCH_SCOPE");
    assert_eq!(
        retained["message"],
        "launch dispatch scope failed predicate: binding.route.alias_matches_launch_manifest"
    );
}

#[test]
fn corrupted_launcher_open_tuple_is_durably_rejected_before_dispatch() {
    let mut db = fixture();
    insert_corrupted_launcher_open(&db);

    // The selected agent.open has a valid parent pointer and binding, but its
    // Task differs from the retained swarm.launch tuple. This is a deterministic
    // FORBIDDEN validation failure before the ordinary dispatch guard begins.
    let dispatched = runtime::next(&mut db, &module_principal()).unwrap();
    assert_eq!(dispatched["command"], Value::Null);
    assert_eq!(dispatched["rejected_operation_id"], OPEN_ID);
    assert_eq!(dispatched["error"]["code"], "FORBIDDEN");
    assert_eq!(
        dispatched["error"]["message"],
        "opening launch validation failed before dispatch"
    );

    let (state, result_json, sent_at_ms, settled_at_ms, native_refs_json): (
        String,
        String,
        Option<i64>,
        Option<i64>,
        String,
    ) = db
        .query_row(
            "SELECT state,result_json,sent_at_ms,settled_at_ms,native_refs_json \
             FROM operations WHERE operation_id=?1",
            [OPEN_ID],
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
    assert_eq!(state, "rejected");
    assert!(sent_at_ms.is_none());
    assert!(settled_at_ms.is_some());
    assert_eq!(native_refs_json, "{}");
    let retained_error: Value = serde_json::from_str(&result_json).unwrap();
    assert_eq!(retained_error["code"], "FORBIDDEN");
    assert_eq!(
        retained_error["message"],
        "opening launch validation failed before dispatch"
    );

    let binding = operations::get_binding(&db, BINDING_ID, 1).unwrap();
    assert_eq!(binding["state"], "opening");
    assert!(binding["native_root_id"].is_null());
    assert!(binding["native_scope_key"].is_null());
    let dispatch_failures: i64 = db
        .query_row(
            "SELECT count(*) FROM observations \
             WHERE operation_id=?1 AND kind='owned_service.dispatch_failure'",
            [LAUNCH_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(dispatch_failures, 0);

    let next_poll = runtime::next(&mut db, &module_principal()).unwrap();
    assert_eq!(next_poll, json!({"command":null}));
    let final_state: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [OPEN_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(final_state, "rejected");
}

#[test]
fn idle_poll_stays_a_noop_and_store_errors_propagate() {
    let mut db = fixture();

    let idle = runtime::next(&mut db, &module_principal()).unwrap();
    assert_eq!(idle, json!({"command":null}));

    // A missing core table is a real SQLite/Store failure, not an idle poll or
    // an admission rejection. The runtime boundary must return STORE_ERROR.
    db.execute_batch("DROP TABLE operations").unwrap();
    let error = runtime::next(&mut db, &module_principal()).unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
}

#[test]
fn standalone_opencode_hello_projects_v3_options_and_preserves_retained_route() {
    let mut db = fixture();
    let directory = std::env::temp_dir();
    let options = json!({
        "service_id":"hello-external",
        "connection_file":directory.join("hello-connection.json"),
        "expected_version":"2.0.7",
        "directory":directory,
        "model":{"id":"step-5-preview-free","providerID":"opencode","variant":"high"},
    });
    let route = json!({
        "alias":"runtime-admission-opencode",
        "runtime":"module",
        "module_artifact_id":crate::config::OPENCODE_RUST_ARTIFACT_ID,
        "workspace_option":"directory",
        "enabled":true,
        "native_options":options,
    });
    db.execute(
        "UPDATE bindings SET module_artifact_id=?2,route_json=?3 WHERE binding_id=?1",
        params![
            BINDING_ID,
            crate::config::OPENCODE_RUST_ARTIFACT_ID,
            model::canonical(&route).unwrap()
        ],
    )
    .unwrap();
    let hello = json!({"boot_id":"hello-projection-boot","module_artifact_id":crate::config::OPENCODE_RUST_ARTIFACT_ID,"native_ready":false});
    let plan = runtime::hello_plan(&db, &module_principal(), &hello).unwrap();
    let reply = runtime::hello_with_config(
        &mut db,
        &module_principal(),
        &hello,
        &plan,
        &Config::default(),
    )
    .unwrap();
    let mut expected = options;
    expected.as_object_mut().unwrap().remove("expected_version");
    assert_eq!(reply["route"]["native_options"], expected);
    assert_eq!(
        operations::get_binding(&db, BINDING_ID, 1).unwrap()["route"],
        route
    );

    let before = operations::get_binding(&db, BINDING_ID, 1).unwrap();
    let mut owned_route = route;
    owned_route["owned_service"] = json!({"origin":"fresh_owned_service"});
    owned_route["native_options"] = json!({"directory":directory});
    db.execute(
        "UPDATE bindings SET route_json=?2 WHERE binding_id=?1",
        params![BINDING_ID, model::canonical(&owned_route).unwrap()],
    )
    .unwrap();
    let mut replacement = hello;
    replacement["boot_id"] = json!("hello-projection-replacement");
    // OS custody preflight is a supplied fixture fact at this Store seam.
    replacement["managed_owner"] =
        json!({"token":"12345678-1234-4234-8234-1234567890ab","process":{"purpose":"module"}});
    let mut plan = runtime::hello_plan(&db, &module_principal(), &replacement).unwrap();
    plan["departed"] = json!(true);
    assert_eq!(
        runtime::hello_with_config(
            &mut db,
            &module_principal(),
            &replacement,
            &plan,
            &Config::default()
        )
        .unwrap_err()
        .code,
        "OWNED_SERVICE_INTENT_MISSING"
    );
    let after = operations::get_binding(&db, BINDING_ID, 1).unwrap();
    assert_eq!(after["observation"], before["observation"]);
    assert_eq!(after["state"], before["state"]);
}
