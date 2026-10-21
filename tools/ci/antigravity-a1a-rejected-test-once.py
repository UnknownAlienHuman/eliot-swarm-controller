from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/module_bridge_recovery_tests.rs")
text = path.read_text(encoding="utf-8")
old = '''    if child_mode {
        println!("{ANTIGRAVITY_CHILD_STARTED_MARKER}");
        run_antigravity_terminal_receipt_regression();
        return;
    }
'''
new = '''    if child_mode {
        println!("{ANTIGRAVITY_CHILD_STARTED_MARKER}");
        run_antigravity_terminal_receipt_regression();
        run_antigravity_rejected_terminal_regression("ERROR", "failed");
        run_antigravity_rejected_terminal_regression("CANCELED", "cancelled");
        return;
    }
'''
if text.count(old) != 1:
    raise SystemExit("generated Antigravity child block changed")
text = text.replace(old, new, 1)
append = r'''

fn run_antigravity_rejected_terminal_regression(status: &str, expected_disposition: &str) {
    let (path, module) = antigravity_dispatch_store();
    let mut db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();

    let dispatch = runtime::next(&mut db, &module).unwrap();
    let command: swarm_contracts::runtime::RuntimeCommand =
        serde_json::from_value(dispatch["command"].clone()).unwrap();
    let context = &dispatch["command"]["input"]["task_dispatch_context"];
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
    let _native_prompt_line = controller.begin_prompt(&command).unwrap();
    let response = format!("terminal {status}");
    controller.consume_line(
        &serde_json::to_vec(&json!({
            "event":"result",
            "conversation_id":ANTIGRAVITY_TEST_ROOT,
            "result":{
                "status":status,
                "conversation_id":ANTIGRAVITY_TEST_ROOT,
                "response":response
            }
        }))
        .unwrap(),
    );
    let terminal = controller.settle_terminal().unwrap();
    assert_eq!(
        terminal.outcome,
        swarm_contracts::runtime::EffectOutcome::Rejected
    );
    let receipt = controller.pending_receipts().into_iter().next().unwrap();
    let recorded = runtime::outcome(&mut db, &module, &receipt).unwrap();
    assert_eq!(recorded["recorded"], true);

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
    assert_eq!(producers[0]["disposition"], expected_disposition);
    assert_eq!(
        producers[0]["completion_condition"],
        "native_terminal_result_observed"
    );
    assert_eq!(producers[0]["execution_complete"], false);
    assert_eq!(producers[0]["task_completion"], "unknown");
    assert_eq!(
        producers[0]["local_execution_ref"]["status"],
        status
    );
    assert!(
        producers[0]["local_execution_ref"]
            .get("observation_id")
            .is_none()
    );
    let operation_state: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [PROMPT_RECOVERY_OPERATION],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(operation_state, "rejected");
    let task_state: String = db
        .query_row(
            "SELECT state FROM tasks WHERE task_id=?1",
            [PROMPT_RECOVERY_TASK],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(task_state, "open");

    drop(db);
    std::fs::remove_file(path).unwrap();
}
'''
path.write_text(text.rstrip() + append + "\n", encoding="utf-8")
