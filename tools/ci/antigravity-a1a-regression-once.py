from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/module_bridge_recovery_tests.rs")
text = path.read_text(encoding="utf-8")
start_marker = "#[test]\nfn antigravity_terminal_receipt_needs_store_observation_and_keeps_task_unresolved() {"
if text.count(start_marker) != 1:
    raise SystemExit("expected exactly one stale Antigravity regression")

prefix = text[: text.index(start_marker)]
replacement = r'''#[test]
fn antigravity_terminal_receipt_is_self_contained_and_keeps_task_unresolved() {
    let child_mode = std::env::var(ANTIGRAVITY_CHILD_MARKER_ENV)
        .map(|marker| marker == ANTIGRAVITY_CHILD_MARKER_VALUE)
        .unwrap_or(false);
    if child_mode {
        println!("{ANTIGRAVITY_CHILD_STARTED_MARKER}");
        run_antigravity_terminal_receipt_regression();
        return;
    }

    let encoded_claim = antigravity_encoded_contract_claim();
    let test_module_path = module_path!()
        .split_once("::")
        .map(|(_, module_path)| module_path)
        .unwrap_or(module_path!());
    let exact_test_name = format!(
        "{test_module_path}::antigravity_terminal_receipt_is_self_contained_and_keeps_task_unresolved"
    );
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", exact_test_name.as_str(), "--nocapture"])
        .env(ANTIGRAVITY_CONTRACT_ENV, encoded_claim)
        .env(ANTIGRAVITY_CHILD_MARKER_ENV, ANTIGRAVITY_CHILD_MARKER_VALUE)
        .output()
        .expect("spawn isolated Antigravity regression child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolated Antigravity regression child failed with {}:\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains(ANTIGRAVITY_CHILD_STARTED_MARKER),
        "isolated test executable skipped the exact child test:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

fn run_antigravity_terminal_receipt_regression() {
    let (path, module) = antigravity_dispatch_store();
    assert_eq!(
        std::env::var(ANTIGRAVITY_CONTRACT_ENV).unwrap(),
        antigravity_encoded_contract_claim()
    );
    let mut db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();

    let dispatch = runtime::next(&mut db, &module).unwrap();
    assert_eq!(dispatch["command"]["method"], "task.dispatch");
    assert_eq!(
        dispatch["command"]["operation_id"],
        PROMPT_RECOVERY_OPERATION
    );
    let command: swarm_contracts::runtime::RuntimeCommand =
        serde_json::from_value(dispatch["command"].clone()).unwrap();
    let context = &dispatch["command"]["input"]["task_dispatch_context"];
    let prompt = &dispatch["command"]["input"]["task_prompt"];
    assert_eq!(
        command.native_root_id.as_deref(),
        Some(ANTIGRAVITY_TEST_ROOT)
    );
    assert_eq!(context["task_id"], PROMPT_RECOVERY_TASK);
    assert_eq!(context["task_revision"], 1);
    assert_eq!(
        context["source_text_sha256"],
        model::digest(PROMPT_RECOVERY_TEXT.as_bytes())
    );
    assert_eq!(context["source_text_bytes"], PROMPT_RECOVERY_TEXT.len());

    let mut controller = Controller::new(
        context["worker_boot_id"].as_str().unwrap().to_owned(),
        ANTIGRAVITY_TEST_SCOPE.to_owned(),
        PROMPT_RECOVERY_BINDING.to_owned(),
        1,
        Some(ANTIGRAVITY_TEST_ROOT.to_owned()),
        true,
    );
    controller.consume_line(
        &serde_json::to_vec(&json!({
            "event":"init",
            "conversation_id":ANTIGRAVITY_TEST_ROOT,
            "init":{"model":"fixture-model","tools":[]}
        }))
        .unwrap(),
    );
    let replayed_response = "prior terminal result replay";
    controller.consume_line(&antigravity_result_line(
        ANTIGRAVITY_TEST_ROOT,
        replayed_response,
    ));
    assert!(controller.settle_terminal().is_none());
    let _native_prompt_line = controller.begin_prompt(&command).unwrap();
    assert!(controller.settle_terminal().is_none());
    controller.consume_line(&antigravity_result_line(
        "foreign-conversation",
        "foreign result",
    ));
    assert!(controller.settle_terminal().is_none());
    assert!(controller.pending_receipts().is_empty());

    // The CLI has no native parent edge. This exact local sequential result can
    // settle the admitted Operation, but it still does not prove Task completion.
    controller.consume_line(&antigravity_result_line(
        ANTIGRAVITY_TEST_ROOT,
        replayed_response,
    ));
    let terminal = controller.settle_terminal().unwrap();
    assert_eq!(
        terminal.outcome,
        swarm_contracts::runtime::EffectOutcome::Applied
    );
    assert_eq!(
        terminal.details["completion_condition"],
        "native_terminal_result_observed"
    );
    assert_eq!(terminal.details["execution_complete"], false);
    assert_eq!(terminal.details["task_completion"], "unknown");
    let admission = terminal.details["dispatch_admission"].clone();
    let admission_receipt: swarm_contracts::runtime::TaskDispatchAdmissionReceipt =
        serde_json::from_value(admission.clone()).unwrap();
    let dispatch_context: swarm_contracts::runtime::TaskDispatchContext =
        serde_json::from_value(context.clone()).unwrap();
    assert_eq!(admission_receipt.context(), dispatch_context);
    assert_eq!(
        admission_receipt.native_payload_sha256,
        prompt["prompt_sha256"].as_str().unwrap()
    );
    assert_eq!(
        Some(admission_receipt.native_payload_bytes),
        prompt["prompt_bytes"].as_u64()
    );
    assert_eq!(admission_receipt.native_input_id, None);

    let receipt = controller.pending_receipts().into_iter().next().unwrap();
    let module_receipt = &receipt["details"]["module_receipt"];
    assert_eq!(admission["module_receipt"], *module_receipt);
    assert_eq!(module_receipt["module_id"], "antigravity");
    assert_eq!(
        module_receipt["artifact"]["artifact_id"],
        swarm_antigravity_adapter::wire::ARTIFACT_ID
    );
    assert_eq!(module_receipt["operation_id"], command.operation_id);
    assert_eq!(module_receipt["binding_id"], command.binding_id);
    assert_eq!(module_receipt["binding_generation"], command.generation);
    assert_eq!(
        module_receipt["input_sha256"],
        command.input_sha256.as_deref().unwrap()
    );
    assert_eq!(
        admission_receipt.module_receipt.input_sha256,
        command.input_sha256.as_deref().unwrap()
    );

    let execution_ref = &receipt["details"]["local_execution_ref"];
    assert_eq!(execution_ref["input_operation_id"], command.operation_id);
    assert_eq!(
        execution_ref["native_conversation_id"],
        ANTIGRAVITY_TEST_ROOT
    );
    assert_eq!(execution_ref["bridge_boot_id"], context["worker_boot_id"]);
    assert_eq!(
        execution_ref["response_sha256"],
        model::digest(replayed_response.as_bytes())
    );
    assert_eq!(execution_ref["status"], "SUCCESS");
    assert!(execution_ref.get("observation_id").is_none());

    // The actual current boot remains a terminal identity guard.
    let mut forged_receipt = receipt.clone();
    forged_receipt["details"]["local_execution_ref"]["bridge_boot_id"] =
        json!("forged-boot");
    let error = runtime::outcome(&mut db, &module, &forged_receipt).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    let operation_state: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [PROMPT_RECOVERY_OPERATION],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(operation_state, "sending");
    let producers_raw: String = db
        .query_row(
            "SELECT producers_json FROM attempts WHERE attempt_id=?1",
            [PROMPT_RECOVERY_ATTEMPT],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&producers_raw).unwrap(),
        json!([])
    );

    // Store accepts the original terminal outcome before any observation is
    // recorded. Observation remains independent current-state data.
    let recorded = runtime::outcome(&mut db, &module, &receipt).unwrap();
    assert_eq!(recorded["recorded"], true);
    controller.acknowledge_outcome(&command.operation_id);
    assert!(controller.pending_receipts().is_empty());

    let pending_observation = controller.prepare_observation();
    let observed_state = pending_observation.state.clone();
    assert!(observed_state.get("local_execution_results").is_none());
    let observation_ack = runtime::observe(
        &mut db,
        &module,
        &json!({
            "event_id":pending_observation.event_id,
            "sequence":pending_observation.sequence,
            "state":pending_observation.state
        }),
    )
    .unwrap();
    assert_eq!(observation_ack["recorded"], true);
    let observation_id = observation_ack["observation_id"].as_i64().unwrap();
    assert_eq!(
        controller.acknowledge_observation(&observation_ack),
        Some(observation_id)
    );
    controller.finish_observation();

    let producers_raw: String = db
        .query_row(
            "SELECT producers_json FROM attempts WHERE attempt_id=?1",
            [PROMPT_RECOVERY_ATTEMPT],
            |row| row.get(0),
        )
        .unwrap();
    let producers: Value = serde_json::from_str(&producers_raw).unwrap();
    assert_eq!(producers.as_array().unwrap().len(), 1);
    assert_eq!(producers[0]["assignment_id"], command.operation_id);
    assert_eq!(producers[0]["dispatch_operation_id"], command.operation_id);
    assert_eq!(producers[0]["native_session_id"], ANTIGRAVITY_TEST_ROOT);
    assert_eq!(producers[0]["disposition"], "completed");
    let producer_ref = &producers[0]["local_execution_ref"];
    assert_eq!(producer_ref["kind"], "antigravity_warm_result");
    assert_eq!(producer_ref["input_operation_id"], command.operation_id);
    assert_eq!(producer_ref["native_conversation_id"], ANTIGRAVITY_TEST_ROOT);
    assert_eq!(producer_ref["bridge_boot_id"], context["worker_boot_id"]);
    assert_eq!(
        producer_ref["response_sha256"],
        model::digest(replayed_response.as_bytes())
    );
    assert_eq!(producer_ref["status"], "SUCCESS");
    assert!(producer_ref.get("observation_id").is_none());

    let task_state: String = db
        .query_row(
            "SELECT state FROM tasks WHERE task_id=?1",
            [PROMPT_RECOVERY_TASK],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(task_state, "open");
    let stored_observation_id: i64 = db
        .query_row(
            "SELECT json_extract(state_json,'$.native_observation_id') FROM bindings WHERE binding_id=?1 AND generation=1",
            [PROMPT_RECOVERY_BINDING],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_observation_id, observation_id);

    drop(db);
    std::fs::remove_file(path).unwrap();
}
'''

path.write_text(prefix + replacement, encoding="utf-8")
