//! Transaction checks for prepared sessions and CLI terminal receipts.
use super::*;
use crate::runtime::{EffectOutcome, RuntimeOutcome, prepared, warm_stream};

fn database(runtime_name: &str, artifact: &str, rooted: bool) -> (Connection, Principal) {
    let db = Connection::open_in_memory().unwrap();
    db.pragma_update(None, "foreign_keys", true).unwrap();
    db.execute_batch(SCHEMA).unwrap();
    let route = json!({"runtime":runtime_name,"module_artifact_id":artifact});
    let state = json!({"module_client_id":"module","module_link_id":"link",
        "bridge_boot_id":"boot","connection":"connected",
        "opening_evidence":{"completion_condition":"native_executor_prepared",
            "native_session_state":"prepared","bridge_boot_id":"boot"}});
    db.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_root_id,native_scope_key,route_json,state_json,created_at_ms) VALUES('binding',1,'lane','instance',?1,'ready',?2,?3,?4,?5,1)",
        params![artifact, rooted.then_some("session"), rooted.then_some("scope"), model::canonical(&route).unwrap(), model::canonical(&state).unwrap()]).unwrap();
    set_meta(
        &db,
        "client:module",
        &json!({"role":"module","binding_id":"binding","binding_generation":1,"disabled":false}),
    )
    .unwrap();
    db.execute("INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task','project',1,'open','{}',1,1)",[]).unwrap();
    db.execute(r#"INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) VALUES('attempt','task',1,'{"objective":"frozen"}','operator','controller','binding',1,'reserved',1,1)"#,[]).unwrap();
    db.execute(r#"INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) VALUES('dispatch','operator','request','task.dispatch','{"text":"reply"}','{}','task','attempt','binding',1,'sending',1,1,1,1)"#,[]).unwrap();
    (
        db,
        Principal {
            link_id: "link".into(),
            client_id: "module".into(),
            role: Role::Module,
        },
    )
}

fn warm_receipt(db: &mut Connection, status: &str) -> RuntimeOutcome {
    let fingerprint = json!({"input_operation_id":"dispatch","native_conversation_id":"session",
        "bridge_boot_id":"boot","result_ordinal":1,"response_sha256":model::digest(b"reply"),"status":status});
    let observation = json!({"boot_id":"boot","native_root_id":"session","native_scope_key":"scope",
        "local_execution_results":[fingerprint]});
    let receipt = runtime::observe(
        db,
        &Principal {
            link_id: "link".into(),
            client_id: "module".into(),
            role: Role::Module,
        },
        &json!({"event_id":"terminal","sequence":1,"state":observation}),
    )
    .unwrap();
    let mut fingerprint = fingerprint;
    fingerprint["observation_id"] = receipt["observation_id"].clone();
    RuntimeOutcome {
        operation_id: "dispatch".into(),
        outcome: if status == "SUCCESS" {
            EffectOutcome::Applied
        } else {
            EffectOutcome::Rejected
        },
        native_root_id: Some("session".into()),
        native_scope_key: Some("scope".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({"completion_condition":"native_terminal_result_observed","local_execution_ref":fingerprint}),
    }
}

#[test]
fn warm_terminal_receipts_settle_execution_without_accepting_task_or_inventing_run() {
    for (status, disposition, operation_state) in [
        ("SUCCESS", "completed", "settled"),
        ("ERROR", "failed", "rejected"),
        ("CANCELED", "cancelled", "rejected"),
    ] {
        let (mut db, p) = database(warm_stream::RUNTIME, warm_stream::ARTIFACT_ID, true);
        let outcome = warm_receipt(&mut db, status);
        runtime::outcome(&mut db, &p, &serde_json::to_value(outcome).unwrap()).unwrap();
        let attempt = tasks::get_attempt(&db, "attempt").unwrap();
        assert_eq!(attempt["producers"][0]["disposition"], disposition);
        assert!(attempt["producers"][0]["native_run_id"].is_null());
        assert!(attempt["producers"][0]["native_input_id"].is_null());
        let operation = operations::get_operation(&db, "dispatch").unwrap();
        assert_eq!(operation["state"], operation_state);
        assert!(operation["native_refs"]["local_execution_ref"].is_object());
        assert_eq!(tasks::get_task(&db, "task").unwrap()["state"], "open");
    }
}

#[test]
fn warm_forged_observation_receipt_rolls_back_operation_and_producer() {
    let (mut db, p) = database(warm_stream::RUNTIME, warm_stream::ARTIFACT_ID, true);
    let mut outcome = warm_receipt(&mut db, "SUCCESS");
    outcome.details["local_execution_ref"]["observation_id"] = json!(999);
    let error = runtime::outcome(&mut db, &p, &serde_json::to_value(outcome).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "dispatch").unwrap()["state"],
        "sending"
    );
    assert_eq!(
        tasks::get_attempt(&db, "attempt").unwrap()["producers"],
        json!([])
    );
}

fn first_claude_receipt() -> RuntimeOutcome {
    let snapshot = json!({"objective":"frozen"});
    let canonical = model::canonical(&snapshot).unwrap();
    let prompt = format!("Task specification: {canonical}\n\nreply");
    RuntimeOutcome {
        operation_id: "dispatch".into(),
        outcome: EffectOutcome::Applied,
        native_root_id: Some("session".into()),
        native_scope_key: Some("scope".into()),
        turn_id: None,
        native_input_id: Some("actual-uuid".into()),
        details: json!({
            "completion_condition":"native_input_admitted","initial_task_dispatch":true,
            "evidence":"native_frame_echo","execution_complete":false,"user_message_uuid":"actual-uuid",
            "bridge_boot_id":"boot","system_init_session_id":"session","native_frame_session_id":"session",
            "native_scope_key":"scope","prompt_sha256":model::digest(prompt.as_bytes()),"prompt_bytes":prompt.len(),
            "task_snapshot_sha256":model::digest(canonical.as_bytes())}),
    }
}

fn insert_claude_send(db: &Connection, delivery: &str, text: &str) {
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) VALUES('send','operator','send-request','agent.send',?1,'{}','binding',1,'sending',1,1,1,1)",
        [model::canonical(&json!({"delivery":delivery,"text":text})).unwrap()],
    )
    .unwrap();
}

fn claude_send_receipt(text: &str) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: "send".into(),
        outcome: EffectOutcome::Applied,
        native_root_id: Some("session".into()),
        native_scope_key: Some("scope".into()),
        turn_id: None,
        native_input_id: Some("send-uuid".into()),
        details: json!({
            "completion_condition":"native_input_admitted",
            "evidence":"native_frame_echo",
            "execution_complete":false,
            "user_message_uuid":"send-uuid",
            "initial_task_dispatch":false,
            "bridge_boot_id":"boot",
            "native_frame_session_id":"session",
            "native_scope_key":"scope",
            "prompt_sha256":model::digest(text.as_bytes()),
            "prompt_bytes":text.len(),
            "task_snapshot_sha256":Value::Null
        }),
    }
}

#[test]
fn prepared_next_turn_send_requires_exact_request_and_native_echo_receipt() {
    let text = "continue with the verified input 🐇";
    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "next_turn", text);
    runtime::outcome(
        &mut db,
        &p,
        &serde_json::to_value(claude_send_receipt(text)).unwrap(),
    )
    .unwrap();
    let operation = operations::get_operation(&db, "send").unwrap();
    assert_eq!(operation["state"], "settled");
    assert_eq!(operation["native_refs"]["input_id"], "send-uuid");
    assert_eq!(tasks::get_task(&db, "task").unwrap()["state"], "open");

    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "next_turn", text);
    let mut forged = claude_send_receipt(text);
    forged.details["prompt_sha256"] = json!(model::digest(b"different text"));
    let error = runtime::outcome(&mut db, &p, &serde_json::to_value(forged).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );

    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "next_turn", text);
    let mut wrong_length = claude_send_receipt(text);
    wrong_length.details["prompt_bytes"] = json!(text.len() - 1);
    let error =
        runtime::outcome(&mut db, &p, &serde_json::to_value(wrong_length).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );

    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "next_turn", text);
    let mut missing_snapshot_marker = claude_send_receipt(text);
    missing_snapshot_marker
        .details
        .as_object_mut()
        .unwrap()
        .remove("task_snapshot_sha256");
    let error = runtime::outcome(
        &mut db,
        &p,
        &serde_json::to_value(missing_snapshot_marker).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );

    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "steer", text);
    let error = runtime::outcome(
        &mut db,
        &p,
        &serde_json::to_value(claude_send_receipt(text)).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );

    let (mut db, p) = database(
        prepared::CLAUDE_RUNTIME,
        prepared::CLAUDE_ARTIFACT_ID,
        false,
    );
    insert_claude_send(&db, "next_turn", text);
    let error = runtime::outcome(
        &mut db,
        &p,
        &serde_json::to_value(claude_send_receipt(text)).unwrap(),
    )
    .unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );

    let (mut db, p) = database(prepared::CLAUDE_RUNTIME, prepared::CLAUDE_ARTIFACT_ID, true);
    insert_claude_send(&db, "next_turn", text);
    let mut synthetic_turn = claude_send_receipt(text);
    synthetic_turn.turn_id = Some("invented-turn".into());
    let error =
        runtime::outcome(&mut db, &p, &serde_json::to_value(synthetic_turn).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "send").unwrap()["state"],
        "sending"
    );
}

#[test]
fn prepared_first_input_adopts_actual_identity_and_replays_without_duplicate_producer() {
    let (mut db, p) = database(
        prepared::CLAUDE_RUNTIME,
        prepared::CLAUDE_ARTIFACT_ID,
        false,
    );
    let outcome = serde_json::to_value(first_claude_receipt()).unwrap();
    runtime::outcome(&mut db, &p, &outcome).unwrap();
    let binding = operations::get_binding(&db, "binding", 1).unwrap();
    assert_eq!(binding["native_root_id"], "session");
    assert_eq!(binding["native_scope_key"], "scope");
    assert_eq!(
        tasks::get_attempt(&db, "attempt").unwrap()["producers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        runtime::outcome(&mut db, &p, &outcome).unwrap()["replayed"],
        true
    );
    assert_eq!(
        tasks::get_attempt(&db, "attempt").unwrap()["producers"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(tasks::get_task(&db, "task").unwrap()["state"], "open");
}

#[test]
fn forged_first_prompt_cannot_partially_adopt_native_identity() {
    let (mut db, p) = database(
        prepared::CLAUDE_RUNTIME,
        prepared::CLAUDE_ARTIFACT_ID,
        false,
    );
    let mut outcome = first_claude_receipt();
    outcome.details["prompt_sha256"] = json!(model::digest(b"other prompt"));
    let error = runtime::outcome(&mut db, &p, &serde_json::to_value(outcome).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    let binding = operations::get_binding(&db, "binding", 1).unwrap();
    assert!(binding["native_root_id"].is_null());
    assert!(binding["native_scope_key"].is_null());
    assert_eq!(
        operations::get_operation(&db, "dispatch").unwrap()["state"],
        "sending"
    );
}

#[test]
fn recorded_claude_input_terminal_disposes_producer_without_accepting_task() {
    let (mut db, p) = database(
        prepared::CLAUDE_RUNTIME,
        prepared::CLAUDE_ARTIFACT_ID,
        false,
    );
    runtime::outcome(
        &mut db,
        &p,
        &serde_json::to_value(first_claude_receipt()).unwrap(),
    )
    .unwrap();
    let state = json!({"native_root_id":"session","native_scope_key":"scope","boot_id":"boot",
        "input_executions":[{"native_input_id":"actual-uuid","native_session_id":"session",
            "native_scope_key":"scope","bridge_boot_id":"boot","correlation":"unique",
            "user_message_uuids":["actual-uuid"],"terminal_status":"completed",
            "result_frame_uuid":"actual-result-frame","result_index":0,"effective_model":"claude-sonnet-5",
            "result_sha256":model::digest(b"reply"),"result_bytes":5,"result_subtype":"success",
            "is_error":false}]});
    let observed = runtime::observe(
        &mut db,
        &p,
        &json!({"event_id":"claude-terminal","sequence":1,"state":state}),
    )
    .unwrap();
    let attempt = tasks::get_attempt(&db, "attempt").unwrap();
    assert_eq!(attempt["producers"][0]["disposition"], "completed");
    assert_eq!(
        attempt["producers"][0]["terminal_evidence"]["observation_id"],
        observed["observation_id"]
    );
    assert!(attempt["producers"][0]["native_run_id"].is_null());
    assert_eq!(tasks::get_task(&db, "task").unwrap()["state"], "open");
}
