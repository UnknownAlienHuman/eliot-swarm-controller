//! Offline Store regression for Manager readback after a verified external
//! bridge-owner departure. The synthetic verified fact is fixture input; this
//! test does not inspect or start a native process.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

const PROMPT_RECOVERY_BINDING: &str = "binding-prompt-recovery";
const PROMPT_RECOVERY_OPERATION: &str = "operation-prompt-recovery";
const PROMPT_RECOVERY_TASK: &str = "task-prompt-recovery";
const PROMPT_RECOVERY_ATTEMPT: &str = "attempt-prompt-recovery";
const PROMPT_RECOVERY_MODULE: &str = "module-prompt-recovery";
const PROMPT_RECOVERY_SOURCE: &str = "Apply the exact selected source for this Task.";
const PROMPT_RECOVERY_TEXT: &str = "Implement the frozen Task using its selected source.";

fn initialize_database(db: &Connection) {
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    db.execute_batch(SCRIPT_SCHEMA).unwrap();
    db.execute_batch(GITHUB_SCHEMA).unwrap();
    db.execute_batch(PROVIDER_CONDITION_SCHEMA).unwrap();
    for client_id in ["old-manager", "successor-manager", "task-owner"] {
        set_meta(
            db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(db, "gm", &json!({"client_id":"old-manager","epoch":1})).unwrap();
    set_meta(db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();
}

fn durable_prompt_store() -> (PathBuf, Principal, Principal) {
    let path = std::env::temp_dir().join(format!(
        "eliot-task-prompt-handoff-{}.sqlite",
        model::new_id()
    ));
    let db = Connection::open(&path).unwrap();
    initialize_database(&db);

    let binding_id = PROMPT_RECOVERY_BINDING;
    let module_client_id = PROMPT_RECOVERY_MODULE;
    let route = json!({
        "alias":"zed-prompt-recovery",
        "runtime":crate::runtime::zed::RUNTIME,
        "module_artifact_id":crate::runtime::zed::ARTIFACT_ID,
        "enabled":true,
        "native_options":{}
    });
    let previous_owner = json!({"pid":431,"birth_token":"prompt-owner-before"});
    let binding_state = json!({
        "connection":"connected",
        "module_client_id":module_client_id,
        "module_link_id":"prompt-link-before",
        "bridge_boot_id":"prompt-boot-before",
        "managed_owner":previous_owner
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'lane-prompt-recovery','instance-prompt-recovery',?2,'ready',?3,?4,1)",
        params![
            binding_id,
            crate::runtime::zed::ARTIFACT_ID,
            model::canonical(&route).unwrap(),
            model::canonical(&binding_state).unwrap(),
        ],
    )
    .unwrap();
    set_meta(
        &db,
        &format!("client:{module_client_id}"),
        &json!({
            "role":"module",
            "disabled":false,
            "binding_id":binding_id,
            "binding_generation":1
        }),
    )
    .unwrap();

    let snapshot = json!({
        "spec":{
            "objective":"Use the exact selected Task source",
            "phase":"implementation",
            "source_refs":["issue:17/body"],
            "source_index":[{
                "source_ref":"issue:17/body",
                "revision":"issue-revision-4",
                "content_sha256":model::digest(PROMPT_RECOVERY_SOURCE.as_bytes()),
                "text":PROMPT_RECOVERY_SOURCE,
                "status":"selected",
                "gap_reason":null
            }]
        },
        "revision":1,
        "dependency_acceptances":[],
        "baseline_candidate":{"status":"wide","reason":"baseline_not_configured"},
        "brief":{"objective":"Use the exact selected Task source","phase":"implementation"}
    });
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'prompt-recovery-project',1,'open','{}',1,1)",
        [PROMPT_RECOVERY_TASK],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,?3,'old-manager','controller',?4,1,'reserved','[]',1,1)",
        params![
            PROMPT_RECOVERY_ATTEMPT,
            PROMPT_RECOVERY_TASK,
            model::canonical(&snapshot).unwrap(),
            binding_id,
        ],
    )
    .unwrap();
    let attempt = super::tasks::get_attempt(&db, PROMPT_RECOVERY_ATTEMPT).unwrap();
    let prompt = super::task_prompt::build(&attempt, PROMPT_RECOVERY_TEXT, None).unwrap();
    let original_request = json!({
        "client_request_id":"prompt-recovery-request",
        "binding_id":binding_id,
        "generation":1,
        "attempt_id":PROMPT_RECOVERY_ATTEMPT,
        "text":PROMPT_RECOVERY_TEXT
    });
    let effective_request = json!({
        "route":route,
        "input":PROMPT_RECOVERY_TEXT,
        "task_snapshot":snapshot,
        "task_prompt":prompt,
        "operation_contract":{"task_prompt":{
            "contract_revision":swarm_contracts::task_prompt::TASK_PROMPT_CONTRACT_REVISION
        }}
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,'old-manager','prompt-recovery-request','task.dispatch',?2,?3,?4,?5,?6,1,'queued',1,1,1)",
        params![
            PROMPT_RECOVERY_OPERATION,
            model::canonical(&original_request).unwrap(),
            model::canonical(&effective_request).unwrap(),
            PROMPT_RECOVERY_TASK,
            PROMPT_RECOVERY_ATTEMPT,
            binding_id,
        ],
    )
    .unwrap();
    db.execute(
        "UPDATE attempts SET start_operation_id=?2 WHERE attempt_id=?1",
        params![PROMPT_RECOVERY_ATTEMPT, PROMPT_RECOVERY_OPERATION],
    )
    .unwrap();

    let before = principal(module_client_id, Role::Module, "prompt-link-before");
    let after = principal(module_client_id, Role::Module, "prompt-link-after");
    drop(db);
    (path, before, after)
}

fn initial_prompt_command(path: &Path, module: &Principal) -> Value {
    let mut db = Connection::open(path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    let result = runtime::next(&mut db, module).unwrap();
    assert_eq!(result["command"]["method"], "task.dispatch");
    assert_eq!(result["command"]["operation_id"], PROMPT_RECOVERY_OPERATION);
    assert_eq!(
        result["command"]["input"]["task_dispatch_context"]["worker_boot_id"],
        "prompt-boot-before"
    );
    result
}

fn replace_prompt_worker(
    db: &mut Connection,
    module: &Principal,
    proof_boot: &str,
    proof_owner: &Value,
) -> crate::error::Result<Value> {
    let hello = json!({
        "boot_id":"prompt-boot-after",
        "module_artifact_id":crate::runtime::zed::ARTIFACT_ID,
        "native_root_id":null,
        "native_scope_key":null,
        "native_ready":true,
        "managed_owner":{"pid":982,"birth_token":"prompt-owner-after"}
    });
    let plan = runtime::hello_plan(db, module, &hello)?;
    let verified = json!({
        "old_boot":proof_boot,
        "owner":proof_owner,
        "departed":true,
        "module_contract_negotiation":plan["module_contract_negotiation"]
    });
    runtime::hello(db, module, &hello, &verified)
}

fn recovered_prompt_admission(db: &Connection, command: &Value) -> Value {
    let command = &command["command"];
    let operation_id = command["operation_id"].as_str().unwrap();
    let input = &command["input"];
    let context = input["task_dispatch_context"].clone();
    let request_raw: String = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .unwrap();
    let request: Value = serde_json::from_str(&request_raw).unwrap();
    let module_receipt = json!({
        "schema_version":1,
        "module_id":"builtin.zed",
        "artifact":{
            "artifact_id":crate::runtime::zed::ARTIFACT_ID,
            "version":crate::runtime::zed::CONTRACT_REVISION,
            "build_id":null
        },
        "protocol":{"major":1,"minor":0},
        "binding_id":context["binding_id"],
        "binding_generation":context["binding_generation"],
        "operation_id":operation_id,
        "input_sha256":model::digest(model::canonical(&request).unwrap().as_bytes())
    });
    let prompt = &input["task_prompt"];
    let admission = json!({
        "schema_version":1,
        "module_receipt":module_receipt,
        "operation_id":operation_id,
        "binding_id":context["binding_id"],
        "binding_generation":context["binding_generation"],
        "worker_boot_id":context["worker_boot_id"],
        "attempt_id":context["attempt_id"],
        "task_id":context["task_id"],
        "task_revision":context["task_revision"],
        "task_snapshot_sha256":context["task_snapshot_sha256"],
        "source_text_sha256":context["source_text_sha256"],
        "source_text_bytes":context["source_text_bytes"],
        "native_payload_sha256":prompt["prompt_sha256"],
        "native_payload_bytes":prompt["prompt_bytes"],
        "native_input_id":null
    });
    let facts = crate::runtime::batch::command_receipt_facts(
        operation_id,
        prompt["prompt"].as_str().unwrap(),
    );
    json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_root_id":null,
        "native_scope_key":null,
        "turn_id":null,
        "native_input_id":null,
        "details":{
            "dispatch_admission":admission,
            "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
            "completion_condition":"native_result_observed",
            "exit_code":0,
            "result_subtype":"success",
            "native_result":{"status":"completed"},
            "batch_run_id":facts.batch_run_id,
            "signal":null,
            "spawn_error_observed":false,
            "timed_out":false
        }
    })
}

fn stored_operation_state(db: &Connection) -> (String, i64) {
    let state: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [PROMPT_RECOVERY_OPERATION],
            |row| row.get(0),
        )
        .unwrap();
    let count: i64 = db
        .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
        .unwrap();
    (state, count)
}

#[test]
fn task_prompt_dispatch_context_survives_store_restart_and_worker_replacement() {
    let (path, before, after) = durable_prompt_store();
    let initial_command = initial_prompt_command(&path, &before);

    // A new SQLite connection models Store restart. The verified boot change
    // marks the original dispatch unknown; the replacement gets no replay.
    let mut db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    let old_owner = json!({"pid":431,"birth_token":"prompt-owner-before"});
    let transition =
        replace_prompt_worker(&mut db, &after, "prompt-boot-before", &old_owner).unwrap();
    assert_eq!(transition["recovery_required"], true);

    let no_replay = runtime::next(&mut db, &after).unwrap();
    assert!(no_replay["command"].is_null());
    let (state, operation_count) = stored_operation_state(&db);
    assert_eq!(state, "outcome_unknown");
    assert_eq!(operation_count, 1);

    let effective_raw: String = db
        .query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [PROMPT_RECOVERY_OPERATION],
            |row| row.get(0),
        )
        .unwrap();
    let effective: Value = serde_json::from_str(&effective_raw).unwrap();
    assert_eq!(
        effective["task_dispatch_context"],
        initial_command["command"]["input"]["task_dispatch_context"]
    );
    assert_eq!(
        effective["task_dispatch_context"]["worker_boot_id"],
        "prompt-boot-before"
    );
    assert_eq!(
        effective["task_prompt"],
        initial_command["command"]["input"]["task_prompt"]
    );
    let attempt = super::tasks::get_attempt(&db, PROMPT_RECOVERY_ATTEMPT).unwrap();
    let retained_prompt =
        super::task_prompt::load(&effective, &attempt, PROMPT_RECOVERY_TEXT).unwrap();
    assert_eq!(
        retained_prompt,
        serde_json::from_value(initial_command["command"]["input"]["task_prompt"].clone()).unwrap()
    );

    // This synthetic receipt exercises Store validation only; no native
    // executor is started by the test.
    let admission = recovered_prompt_admission(&db, &initial_command);
    let recorded = runtime::outcome(&mut db, &after, &admission).unwrap();
    assert_eq!(recorded["recorded"], true);
    let (state, operation_count) = stored_operation_state(&db);
    assert_eq!(state, "settled");
    assert_eq!(operation_count, 1);

    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn immutable_prompt_recovery_mutations_do_not_create_operations_or_commands() {
    // Once the caller is neither the current GM nor the retained Attempt
    // owner, the queued dispatch cannot return a native command.
    let (path, before, _) = durable_prompt_store();
    let mut db = Connection::open(&path).unwrap();
    let old_manager = principal("old-manager", Role::Manager, "old-manager-link");
    mutate(
        &mut db,
        &old_manager,
        "gm.handover",
        &json!({"client_request_id":"prompt-owner-handover","client_id":"successor-manager"}),
        &Config::default(),
    )
    .unwrap();
    db.execute(
        "UPDATE attempts SET owner_id='successor-manager' WHERE attempt_id=?1",
        [PROMPT_RECOVERY_ATTEMPT],
    )
    .unwrap();
    let (_, operations_before_dispatch) = stored_operation_state(&db);
    let rejected = runtime::next(&mut db, &before).unwrap();
    assert!(rejected["command"].is_null());
    assert_eq!(rejected["rejected_operation_id"], PROMPT_RECOVERY_OPERATION);
    let (state, operation_count) = stored_operation_state(&db);
    assert_eq!(state, "rejected");
    assert_eq!(operation_count, operations_before_dispatch);
    drop(db);
    std::fs::remove_file(path).unwrap();

    // Worker replacement requires the exact old boot and native-owner proof.
    for proof_mutation in ["worker_boot", "managed_owner"] {
        let (path, before, after) = durable_prompt_store();
        let _initial_command = initial_prompt_command(&path, &before);
        let mut db = Connection::open(&path).unwrap();
        let verified_owner = if proof_mutation == "managed_owner" {
            json!({"pid":431,"birth_token":"forged-owner"})
        } else {
            json!({"pid":431,"birth_token":"prompt-owner-before"})
        };
        let verified_boot = if proof_mutation == "worker_boot" {
            "forged-prior-boot"
        } else {
            "prompt-boot-before"
        };
        let error =
            replace_prompt_worker(&mut db, &after, verified_boot, &verified_owner).unwrap_err();
        assert_eq!(error.code, "STALE_RECOVERY", "{proof_mutation}");
        let no_command = runtime::next(&mut db, &before).unwrap();
        assert!(no_command["command"].is_null());
        let (state, operation_count) = stored_operation_state(&db);
        assert_eq!(state, "sending");
        assert_eq!(operation_count, 1);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    // Revalidate the retained admission after a valid worker replacement.
    // Each mutation starts from a fresh durable fixture and must leave the
    // original uncertain Operation unchanged when rejected.
    for (mutation, expected_code) in [
        ("worker_boot", "TASK_DISPATCH_ADMISSION_INVALID"),
        ("task_revision", "TASK_DISPATCH_ADMISSION_INVALID"),
        ("source_cause", "TASK_DISPATCH_ADMISSION_INVALID"),
        ("source_text", "TASK_DISPATCH_ADMISSION_INVALID"),
        ("prompt_digest", "TASK_PROMPT_INVALID"),
    ] {
        let (path, before, after) = durable_prompt_store();
        let initial_command = initial_prompt_command(&path, &before);
        let mut db = Connection::open(&path).unwrap();
        let old_owner = json!({"pid":431,"birth_token":"prompt-owner-before"});
        replace_prompt_worker(&mut db, &after, "prompt-boot-before", &old_owner).unwrap();
        let no_replay = runtime::next(&mut db, &after).unwrap();
        assert!(no_replay["command"].is_null());

        match mutation {
            "worker_boot" => {
                db.execute(
                    "UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.task_dispatch_context.worker_boot_id','forged-boot') WHERE operation_id=?1",
                    [PROMPT_RECOVERY_OPERATION],
                )
                .unwrap();
            }
            "task_revision" => {
                db.execute(
                    "UPDATE attempts SET task_revision=2 WHERE attempt_id=?1",
                    [PROMPT_RECOVERY_ATTEMPT],
                )
                .unwrap();
            }
            "source_cause" => {
                let raw: String = db
                    .query_row(
                        "SELECT task_snapshot_json FROM attempts WHERE attempt_id=?1",
                        [PROMPT_RECOVERY_ATTEMPT],
                        |row| row.get(0),
                    )
                    .unwrap();
                let mut snapshot: Value = serde_json::from_str(&raw).unwrap();
                snapshot["spec"]["source_index"][0]["text"] = json!("a different selected source");
                snapshot["spec"]["source_index"][0]["content_sha256"] =
                    json!(model::digest(b"a different selected source"));
                db.execute(
                    "UPDATE attempts SET task_snapshot_json=?2 WHERE attempt_id=?1",
                    params![
                        PROMPT_RECOVERY_ATTEMPT,
                        model::canonical(&snapshot).unwrap()
                    ],
                )
                .unwrap();
            }
            "source_text" => {
                let raw: String = db
                    .query_row(
                        "SELECT original_request_json FROM operations WHERE operation_id=?1",
                        [PROMPT_RECOVERY_OPERATION],
                        |row| row.get(0),
                    )
                    .unwrap();
                let mut request: Value = serde_json::from_str(&raw).unwrap();
                request["text"] = json!("a different original source text");
                db.execute(
                    "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
                    params![
                        PROMPT_RECOVERY_OPERATION,
                        model::canonical(&request).unwrap()
                    ],
                )
                .unwrap();
            }
            "prompt_digest" => {
                db.execute(
                    "UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.task_prompt.prompt_sha256',?2) WHERE operation_id=?1",
                    params![PROMPT_RECOVERY_OPERATION, model::digest(b"forged prompt digest")],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }

        let admission = recovered_prompt_admission(&db, &initial_command);
        let error = runtime::outcome(&mut db, &after, &admission).unwrap_err();
        assert_eq!(error.code, expected_code, "{mutation}: {error}");
        let (state, operation_count) = stored_operation_state(&db);
        assert_eq!(state, "outcome_unknown", "{mutation}");
        assert_eq!(operation_count, 1, "{mutation}");
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}

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
    initialize_database(&db);
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
             VALUES(?1,?2,1,?4,'task-owner','controller',?3,1,'running',1,1)",
            params![attempt_id, task_id, binding_id, model::canonical(&json!({"revision":1})).unwrap()],
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
    let caller_id = if attempt_id.is_some() {
        // Keep the Manager under test distinct from the exact Operation caller;
        // this forces current and successor reads through registered Task scope.
        "task-owner"
    } else {
        "old-manager"
    };
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?9,?2,?3,?4,?5,?6,?7,?8,1,'sending',1,2,2,2)",
        params![
            operation_id,
            format!("request-{runtime_suffix}"),
            case.target_method,
            model::canonical(&original_request).unwrap(),
            model::canonical(&effective_request).unwrap(),
            task_id,
            attempt_id,
            binding_id,
            caller_id,
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
        let plan = runtime::hello_plan(&db, &module, &hello).unwrap();
        let verified = json!({
            "old_boot":"boot-before",
            "owner":previous_owner,
            "departed":true,
            "module_contract_negotiation":plan["module_contract_negotiation"]
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
        if case.artifact == crate::runtime::batch::COMMAND_PREVIOUS_ARTIFACT_ID {
            assert!(
                operations::exact_module_recovery_contract(&route, case.target_method).is_none()
            );
            assert!(
                before_handover
                    .get("module_recovery_action_required")
                    .is_none()
            );
            assert!(
                before_handover
                    .get("module_outcome_readback_required")
                    .is_none()
            );
            continue;
        }
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
        if case.target_method == "task.dispatch" {
            let after_handover = operation_read(&db, &successor, &operation_id);
            assert_eq!(
                after_handover["module_recovery_action_required"],
                before_handover["module_recovery_action_required"]
            );
            let former_manager = read(
                &db,
                &old_manager,
                "operation.get",
                &json!({"operation_id":operation_id}),
                &Config::default(),
            )
            .expect_err("former GM must lose the exact Task/project scope after handover");
            assert_eq!(former_manager.code, "NOT_FOUND");
        } else {
            let successor = read(
                &db,
                &successor,
                "operation.get",
                &json!({"operation_id":operation_id}),
                &Config::default(),
            )
            .expect_err("taskless agent.send has no successor Task scope or on-behalf link");
            assert_eq!(successor.code, "NOT_FOUND");

            // The former GM remains the exact caller for this taskless receipt,
            // but handover removes its manager-only recovery action cards.
            let former_manager = operation_read(&db, &old_manager, &operation_id);
            assert!(
                former_manager
                    .get("module_recovery_action_required")
                    .is_none()
            );
            assert!(
                former_manager
                    .get("module_outcome_readback_required")
                    .is_none()
            );
        }

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

// This retained-binding fixture tests Store Assignment and diagnostic projection only;
// it does not exercise descriptor admission or a native Command process.
fn command_diagnostic_store() -> (PathBuf, Principal) {
    let (path, module, _) = durable_prompt_store();
    let db = Connection::open(&path).unwrap();
    let route = json!({
        "alias":"command-diagnostic-retained",
        "runtime":crate::runtime::batch::COMMAND_RUNTIME,
        "module_artifact_id":crate::runtime::batch::COMMAND_ARTIFACT_ID,
        "enabled":true,
        "native_options":{"modelId":"diagnostic-fixture-model"}
    });
    db.execute(
        "UPDATE bindings SET module_artifact_id=?2,route_json=?3 WHERE binding_id=?1 AND generation=1",
        params![
            PROMPT_RECOVERY_BINDING,
            crate::runtime::batch::COMMAND_ARTIFACT_ID,
            model::canonical(&route).unwrap(),
        ],
    )
    .unwrap();
    let raw: String = db
        .query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [PROMPT_RECOVERY_OPERATION],
            |row| row.get(0),
        )
        .unwrap();
    let mut effective: Value = serde_json::from_str(&raw).unwrap();
    effective["route"] = route;
    let object = effective.as_object_mut().unwrap();
    object.remove("task_prompt");
    object.remove("operation_contract");
    db.execute(
        "UPDATE operations SET effective_request_json=?2 WHERE operation_id=?1",
        params![
            PROMPT_RECOVERY_OPERATION,
            model::canonical(&effective).unwrap(),
        ],
    )
    .unwrap();
    drop(db);
    (path, module)
}

fn command_terminal_receipt(command: &Value) -> Value {
    let command = &command["command"];
    let operation_id = command["operation_id"].as_str().unwrap();
    let instruction = crate::runtime::batch::instruction(&command["input"]).unwrap();
    let facts = crate::runtime::batch::command_receipt_facts(operation_id, &instruction);
    json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_root_id":null,
        "native_scope_key":null,
        "turn_id":null,
        "native_input_id":null,
        "details":{
            "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
            "completion_condition":"native_result_observed",
            "batch_run_id":facts.batch_run_id,
            "prompt_sha256":facts.prompt_sha256,
            "prompt_bytes":facts.prompt_bytes,
            "requested_model":command["route"]["native_options"]["modelId"],
            "result_subtype":"success",
            "exit_code":0,
            "native_result":{
                "status":"completed",
                "private_payload":"COMMAND_DIAGNOSTIC_PRIVATE_MARKER"
            },
            "private_debug":"COMMAND_DIAGNOSTIC_PRIVATE_MARKER",
            "signal":null,
            "spawn_error_observed":false,
            "timed_out":false
        }
    })
}

fn record_command_terminal(db: &mut Connection, module: &Principal) -> (String, Value) {
    let dispatch = runtime::next(db, module).unwrap();
    assert_eq!(dispatch["command"]["method"], "task.dispatch");
    assert_eq!(
        dispatch["command"]["operation_id"],
        PROMPT_RECOVERY_OPERATION
    );
    let receipt = command_terminal_receipt(&dispatch);
    let recorded = runtime::outcome(db, module, &receipt).unwrap();
    assert_eq!(recorded["recorded"], true);
    (PROMPT_RECOVERY_OPERATION.to_owned(), receipt)
}

#[test]
fn command_task_dispatch_execution_diagnostic_projects_only_bounded_terminal_fields() {
    let (path, module) = command_diagnostic_store();
    let mut db = Connection::open(&path).unwrap();
    let (operation_id, _receipt) = record_command_terminal(&mut db, &module);

    let producers_raw: String = db
        .query_row(
            "SELECT producers_json FROM attempts WHERE attempt_id=?1",
            [PROMPT_RECOVERY_ATTEMPT],
            |row| row.get(0),
        )
        .unwrap();
    let producers: Value = serde_json::from_str(&producers_raw).unwrap();
    assert_eq!(producers.as_array().unwrap().len(), 1);
    assert_eq!(producers[0]["assignment_id"], operation_id);
    assert_eq!(producers[0]["dispatch_operation_id"], operation_id);
    assert_eq!(
        producers[0]["execution_shape"],
        crate::runtime::batch::EXECUTION_SHAPE
    );
    assert_eq!(
        producers[0]["terminal_evidence"]["completion_condition"],
        "native_result_observed"
    );
    assert_eq!(
        producers[0]["terminal_evidence"]["result_subtype"],
        "success"
    );
    assert_eq!(producers[0]["terminal_evidence"]["exit_code"], 0);

    let owner = principal("old-manager", Role::Manager, "owner-link");
    let owner_projection = operation_read(&db, &owner, &operation_id);
    assert_eq!(
        owner_projection["execution_diagnostic"],
        json!({"result_subtype":"success","exit_code":0})
    );
    assert!(
        !model::canonical(&owner_projection)
            .unwrap()
            .contains("COMMAND_DIAGNOSTIC_PRIVATE_MARKER")
    );

    mutate(
        &mut db,
        &owner,
        "gm.handover",
        &json!({"client_request_id":"command-diagnostic-handover","client_id":"successor-manager"}),
        &Config::default(),
    )
    .unwrap();
    let current_gm = principal("successor-manager", Role::Manager, "successor-link");
    let gm_projection = operation_read(&db, &current_gm, &operation_id);
    assert_eq!(
        gm_projection["execution_diagnostic"],
        json!({"result_subtype":"success","exit_code":0})
    );
    assert!(
        !model::canonical(&gm_projection)
            .unwrap()
            .contains("COMMAND_DIAGNOSTIC_PRIVATE_MARKER")
    );

    drop(db);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn command_task_dispatch_execution_diagnostic_withholds_corrupt_assignment_binding_and_outcome() {
    for mutation in ["assignment", "binding_generation", "outcome_tuple"] {
        let (path, module) = command_diagnostic_store();
        let mut db = Connection::open(&path).unwrap();
        let (operation_id, _receipt) = record_command_terminal(&mut db, &module);

        match mutation {
            "assignment" | "binding_generation" => {
                let raw: String = db
                    .query_row(
                        "SELECT producers_json FROM attempts WHERE attempt_id=?1",
                        [PROMPT_RECOVERY_ATTEMPT],
                        |row| row.get(0),
                    )
                    .unwrap();
                let mut producers: Value = serde_json::from_str(&raw).unwrap();
                if mutation == "assignment" {
                    producers[0]["assignment_id"] = json!("another-operation");
                } else {
                    // Damage optional retained Assignment evidence while the
                    // base Attempt and its foreign-key binding remain valid.
                    producers[0]["binding_generation"] = json!(2);
                }
                db.execute(
                    "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                    params![
                        PROMPT_RECOVERY_ATTEMPT,
                        model::canonical(&producers).unwrap(),
                    ],
                )
                .unwrap();
            }
            "outcome_tuple" => {
                let raw: String = db
                    .query_row(
                        "SELECT result_json FROM operations WHERE operation_id=?1",
                        [&operation_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                let mut result: Value = serde_json::from_str(&raw).unwrap();
                result["details"]["exit_code"] = json!(7);
                db.execute(
                    "UPDATE operations SET result_json=?2 WHERE operation_id=?1",
                    params![operation_id, model::canonical(&result).unwrap()],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }

        let owner = principal("old-manager", Role::Manager, "owner-link");
        let projection = operation_read(&db, &owner, &operation_id);
        assert!(
            projection.get("execution_diagnostic").is_none(),
            "{mutation}"
        );
        assert_eq!(
            projection["diagnostic_gaps"],
            json!([{
                "card":"execution_diagnostic",
                "reason_code":"OBJECT_SCOPE_DAMAGED"
            }]),
            "{mutation}"
        );

        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
