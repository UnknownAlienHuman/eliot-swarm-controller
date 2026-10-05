//! Offline Store regression for Manager readback after a verified external
//! bridge-owner departure. The synthetic verified fact is fixture input; this
//! test does not inspect or start a native process.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};

struct RouteCase {
    runtime: &'static str,
    artifact: &'static str,
    target_method: &'static str,
}

fn principal(client_id: &str, role: Role, link_id: &str) -> Principal {
    Principal {
        client_id: client_id.to_owned(),
        link_id: link_id.to_owned(),
        role,
    }
}

fn database() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    db.execute_batch(SCRIPT_SCHEMA).unwrap();
    db.execute_batch(GITHUB_SCHEMA).unwrap();
    for client_id in ["old-manager", "successor-manager"] {
        set_meta(
            &db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(&db, "gm", &json!({"client_id":"old-manager","epoch":1})).unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();
    db
}

fn seed_target(db: &Connection, case: &RouteCase) -> (Value, String, String, String) {
    let runtime_suffix = case.runtime.replace('.', "-");
    let binding_id = format!("binding-recovery-{runtime_suffix}");
    let operation_id = format!("operation-recovery-{runtime_suffix}");
    let module_client_id = format!("module-recovery-{runtime_suffix}");
    let old_module_link = format!("old-module-link-{runtime_suffix}");
    let route = json!({
        "alias":"bridge-recovery-fixture",
        "runtime":case.runtime,
        "module_artifact_id":case.artifact,
        "enabled":true,
        "native_options":{}
    });
    let previous_owner = json!({"pid":431,"birth_token":"prior-owner"});
    let binding_state = json!({
        "connection":"connected",
        "module_client_id":module_client_id,
        "module_link_id":old_module_link,
        "bridge_boot_id":"boot-before",
        "managed_owner":previous_owner
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,?2,?3,?4,'ready',?5,?6,1)",
        params![
            binding_id,
            format!("lane-{runtime_suffix}"),
            format!("instance-{runtime_suffix}"),
            case.artifact,
            model::canonical(&route).unwrap(),
            model::canonical(&binding_state).unwrap(),
        ],
    )
    .unwrap();
    set_meta(
        db,
        &format!("client:{module_client_id}"),
        &json!({
            "role":"module",
            "disabled":false,
            "binding_id":binding_id,
            "binding_generation":1
        }),
    )
    .unwrap();

    let (task_id, attempt_id) = if case.target_method == "task.dispatch" {
        let task_id = format!("task-recovery-{runtime_suffix}");
        let attempt_id = format!("attempt-recovery-{runtime_suffix}");
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
             VALUES(?1,'recovery-project',1,'open','{}',1,1)",
            [&task_id],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,1,'{}','old-manager','controller',?3,1,'running',1,1)",
            params![attempt_id, task_id, binding_id],
        )
        .unwrap();
        (Some(task_id), Some(attempt_id))
    } else {
        (None, None)
    };
    let original_request = if let Some(attempt_id) = attempt_id.as_deref() {
        json!({
            "client_request_id":format!("request-{runtime_suffix}"),
            "binding_id":binding_id,
            "generation":1,
            "attempt_id":attempt_id,
            "text":"fixture input with uncertain native effect"
        })
    } else {
        json!({
            "client_request_id":format!("request-{runtime_suffix}"),
            "binding_id":binding_id,
            "generation":1,
            "delivery":"next_turn",
            "text":"fixture input with uncertain native effect"
        })
    };
    let effective_request = json!({"route":route,"native_root_id":null});
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,'old-manager',?2,?3,?4,?5,?6,?7,?8,1,'sending',1,2,2,2)",
        params![
            operation_id,
            format!("request-{runtime_suffix}"),
            case.target_method,
            model::canonical(&original_request).unwrap(),
            model::canonical(&effective_request).unwrap(),
            task_id,
            attempt_id,
            binding_id,
        ],
    )
    .unwrap();

    (route, binding_id, operation_id, module_client_id)
}

fn operation_read(db: &Connection, principal: &Principal, operation_id: &str) -> Value {
    read(
        db,
        principal,
        "operation.get",
        &json!({"operation_id":operation_id}),
        &Config::default(),
    )
    .unwrap()
}

#[test]
fn verified_bridge_boot_change_projects_readback_to_current_and_successor_manager() {
    let cases = [
        RouteCase {
            runtime: crate::runtime::batch::COMMAND_RUNTIME,
            artifact: crate::runtime::batch::COMMAND_PREVIOUS_ARTIFACT_ID,
            target_method: "task.dispatch",
        },
        RouteCase {
            runtime: crate::runtime::batch::COMMAND_RUNTIME,
            artifact: crate::runtime::batch::COMMAND_ARTIFACT_ID,
            target_method: "task.dispatch",
        },
        RouteCase {
            runtime: crate::runtime::codex::RUNTIME,
            artifact: crate::runtime::codex::ARTIFACT_ID,
            target_method: "agent.send",
        },
        RouteCase {
            runtime: crate::runtime::warm_stream::RUNTIME,
            artifact: crate::runtime::warm_stream::ARTIFACT_ID,
            target_method: "agent.send",
        },
    ];
    let command_route = json!({
        "runtime":crate::runtime::batch::COMMAND_RUNTIME,
        "module_artifact_id":crate::runtime::batch::COMMAND_ARTIFACT_ID
    });
    assert!(operations::exact_module_recovery_contract(&command_route, "task.dispatch").is_some());
    assert!(operations::exact_module_recovery_contract(&command_route, "agent.send").is_none());

    for case in cases {
        let mut db = database();
        let (route, binding_id, operation_id, module_client_id) = seed_target(&db, &case);
        let old_manager = principal("old-manager", Role::Manager, "old-manager-link");
        let successor = principal("successor-manager", Role::Manager, "successor-link");
        let previous_owner = json!({"pid":431,"birth_token":"prior-owner"});
        let module = principal(&module_client_id, Role::Module, "new-module-link");
        let hello = json!({
            "boot_id":"boot-after",
            "module_artifact_id":case.artifact,
            "native_root_id":null,
            "native_scope_key":null,
            "native_ready":true,
            "managed_owner":{"pid":982,"birth_token":"successor-owner"}
        });
        let verified = json!({
            "old_boot":"boot-before",
            "owner":previous_owner,
            "departed":true
        });
        let transition = runtime::hello(&mut db, &module, &hello, &verified).unwrap();
        assert!(
            transition["recovery_required"].as_bool().unwrap(),
            "{}",
            case.runtime
        );

        let before_handover = operation_read(&db, &old_manager, &operation_id);
        let action = &before_handover["module_recovery_action_required"];
        assert_eq!(before_handover["state"], "outcome_unknown");
        assert_eq!(action["operation_id"], operation_id);
        assert_eq!(action["operation_method"], case.target_method);
        assert_eq!(action["binding_id"], binding_id);
        assert_eq!(action["binding_generation"], 1);
        assert_eq!(action["runtime"], case.runtime);
        assert_eq!(action["module_artifact_id"], case.artifact);
        assert!(
            action["verified_transition"]["previous_owner_departed"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            action["verified_transition"]["from_bridge_boot_id"],
            "boot-before"
        );
        assert_eq!(
            action["verified_transition"]["to_bridge_boot_id"],
            "boot-after"
        );
        assert_eq!(action["cause"], "unknown");
        assert_eq!(action["native_effect"], "unknown");
        assert!(!action["retry_authorized"].as_bool().unwrap());
        assert_eq!(action["readback"]["method"], "agent.reconcile");
        assert!(
            action["readback"]["supported_on_exact_route"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(action["readback"]["binding_id"], binding_id);
        assert_eq!(action["readback"]["generation"], 1);
        assert_eq!(action["readback"]["operation_id"], operation_id);
        assert!(!action["readback"]["native_replay"].as_bool().unwrap());

        let retained_before_handover: (String, String, String, Option<String>) = db
            .query_row(
                "SELECT state,original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(retained_before_handover.0, "outcome_unknown");
        assert!(retained_before_handover.3.is_none());

        mutate(
            &mut db,
            &old_manager,
            "gm.handover",
            &json!({
                "client_request_id":"handover-to-successor",
                "client_id":"successor-manager"
            }),
            &Config::default(),
        )
        .unwrap();
        let after_handover = operation_read(&db, &successor, &operation_id);
        assert_eq!(
            after_handover["module_recovery_action_required"],
            before_handover["module_recovery_action_required"]
        );
        let former_manager = operation_read(&db, &old_manager, &operation_id);
        assert!(
            former_manager
                .get("module_recovery_action_required")
                .is_none()
        );

        let retained_after_handover: (String, String, String, Option<String>) = db
            .query_row(
                "SELECT state,original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(retained_after_handover, retained_before_handover);
        assert_eq!(route["runtime"], case.runtime);
    }
}
