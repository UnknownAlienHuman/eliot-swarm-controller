use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, process::Command};

const TASK_ID: &str = "task-prompt-offline-claude";
const ATTEMPT_ID: &str = "attempt-task-prompt-offline-claude";
const OPERATION_ID: &str = "operation-task-prompt-offline-claude";
const BINDING_ID: &str = "binding-task-prompt-offline-claude";
const SOURCE_TEXT: &str = "Implement the pinned Task source: café 🐇";
const BOOT_ID: &str = "claude-task-prompt-offline-boot";
const INPUT_ID: &str = "d34d0055-5555-4555-8555-555555555555";
const NATIVE_ROOT_ID: &str = "ses_claude_task_prompt_offline";
const CLAUDE_ARTIFACT_ID: &str = "claude-agent-sdk-0.3.287-bridge.5";

#[test]
fn rust_store_task_prompt_v1_crosses_into_claude_bridge5_offline_helpers() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(super::super::super::SCHEMA).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();

    let snapshot = super::frozen_snapshot();
    let spec = snapshot["spec"].clone();
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'task-prompt-offline-project',3,'open',?2,1,1)",
        params![TASK_ID, crate::model::canonical(&spec).unwrap()],
    )
    .unwrap();

    let route = json!({
        "alias":"claude-offline",
        "runtime":"module",
        "module_artifact_id":CLAUDE_ARTIFACT_ID,
        "enabled":true,
        "native_options":{}
    });
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
         VALUES(?1,1,'lane-task-prompt-offline','instance-task-prompt-offline',?2,'ready',?3,'{}',1)",
        params![
            BINDING_ID,
            CLAUDE_ARTIFACT_ID,
            crate::model::canonical(&route).unwrap(),
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,3,?3,'owner-task-prompt-offline','controller',?4,1,'reserved','[]',1,1)",
        params![
            ATTEMPT_ID,
            TASK_ID,
            crate::model::canonical(&snapshot).unwrap(),
            BINDING_ID,
        ],
    )
    .unwrap();

    // Use the existing Store fixture's frozen Attempt projection and the real
    // TaskPrompt producer, then read the persisted envelope as runtime::next does.
    let attempt = super::super::super::tasks::get_attempt(&db, ATTEMPT_ID).unwrap();
    let produced = super::super::build(&attempt, SOURCE_TEXT, None).unwrap();
    let original_request = json!({
        "client_request_id":"task-prompt-offline-request",
        "attempt_id":ATTEMPT_ID,
        "text":SOURCE_TEXT
    });
    let effective = json!({
        "route":route,
        "input":SOURCE_TEXT,
        "task_snapshot":snapshot,
        "task_prompt":serde_json::to_value(&produced).unwrap(),
        "operation_contract":{"task_prompt":{
            "contract_revision":swarm_contracts::task_prompt::TASK_PROMPT_CONTRACT_REVISION
        }}
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,'owner-task-prompt-offline','task-prompt-offline-request','task.dispatch',?2,?3,?4,?5,?6,1,'queued',1,1,1)",
        params![
            OPERATION_ID,
            crate::model::canonical(&original_request).unwrap(),
            crate::model::canonical(&effective).unwrap(),
            TASK_ID,
            ATTEMPT_ID,
            BINDING_ID,
        ],
    )
    .unwrap();

    let effective_raw: String = db
        .query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [OPERATION_ID],
            |row| row.get(0),
        )
        .unwrap();
    let effective: Value = serde_json::from_str(&effective_raw).unwrap();
    let envelope = super::super::load(&effective, &attempt, SOURCE_TEXT).unwrap();
    assert_eq!(envelope, produced);

    let envelope_bytes = crate::model::canonical(&effective["task_prompt"])
        .unwrap()
        .into_bytes();
    let envelope_json_sha256 = crate::model::digest(&envelope_bytes);
    let snapshot_sha256 = crate::model::digest(
        crate::model::canonical(&attempt["task_snapshot"])
            .unwrap()
            .as_bytes(),
    );
    let context = json!({
        "schema_version":1,
        "operation_id":OPERATION_ID,
        "binding_id":BINDING_ID,
        "binding_generation":1,
        "worker_boot_id":BOOT_ID,
        "attempt_id":ATTEMPT_ID,
        "task_id":TASK_ID,
        "task_revision":3,
        "task_snapshot_sha256":snapshot_sha256,
        "source_text_sha256":crate::model::digest(SOURCE_TEXT.as_bytes()),
        "source_text_bytes":SOURCE_TEXT.len() as u64
    });
    let input = json!({
        "text":SOURCE_TEXT,
        "attempt_id":ATTEMPT_ID,
        "task_dispatch_context":context
    });
    let input_sha256 = crate::model::digest(
        crate::model::canonical(&original_request)
            .unwrap()
            .as_bytes(),
    );
    let dispatch = json!({
        "bootId":BOOT_ID,
        "inputId":INPUT_ID,
        "nativeRootId":NATIVE_ROOT_ID,
        "expected_envelope_json_sha256":envelope_json_sha256,
        "moduleContract":{
            "module_id":"claude",
            "artifact":{"artifact_id":CLAUDE_ARTIFACT_ID,"version":"5"},
            "protocol":{"major":1,"minor":0}
        },
        "command":{
            "method":"task.dispatch",
            "operation_id":OPERATION_ID,
            "binding_id":BINDING_ID,
            "generation":1,
            "input_sha256":input_sha256,
            "route":{"runtime":"module","module_artifact_id":CLAUDE_ARTIFACT_ID},
            "input":input
        }
    });

    let fixture_dir = std::env::temp_dir().join(format!(
        "eliot-task-prompt-v5-offline-{}",
        crate::model::new_id()
    ));
    fs::create_dir(&fixture_dir).unwrap();
    fs::write(fixture_dir.join("envelope.json"), &envelope_bytes).unwrap();
    fs::write(
        fixture_dir.join("dispatch.json"),
        serde_json::to_vec(&dispatch).unwrap(),
    )
    .unwrap();

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("swarm-kernel-host must be inside the workspace")
        .to_path_buf();
    let node_test = workspace_root
        .join("modules/claude/task-prompt-v5-offline.test.mjs")
        .canonicalize()
        .unwrap();
    let output = Command::new("node")
        .arg("--test")
        .arg("--test-name-pattern=Rust Store TaskPrompt v1 crosses into Claude bridge.5 consumer helpers")
        .arg(node_test)
        .current_dir(&workspace_root)
        .env("ELIOT_TASK_PROMPT_V5_FIXTURE_DIR", &fixture_dir)
        .output()
        .expect("Node.js must be available for the offline Rust-to-Claude fixture");
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let _ = fs::remove_dir_all(&fixture_dir);
        panic!("offline Claude consumer fixture failed\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }

    let admission: Value =
        serde_json::from_slice(&fs::read(fixture_dir.join("admission.json")).unwrap()).unwrap();
    assert_eq!(admission["operation_id"], OPERATION_ID);
    assert_eq!(admission["binding_id"], BINDING_ID);
    assert_eq!(admission["binding_generation"], 1);
    assert_eq!(admission["worker_boot_id"], BOOT_ID);
    assert_eq!(admission["attempt_id"], ATTEMPT_ID);
    assert_eq!(admission["task_id"], TASK_ID);
    assert_eq!(admission["task_revision"], 3);
    assert_eq!(admission["task_snapshot_sha256"], snapshot_sha256);
    assert_eq!(admission["native_payload_sha256"], envelope.prompt_sha256);
    assert_eq!(admission["native_payload_bytes"], envelope.prompt_bytes);
    assert_eq!(admission["native_input_id"], INPUT_ID);
    assert_eq!(admission["module_receipt"]["input_sha256"], input_sha256);

    fs::remove_dir_all(&fixture_dir).unwrap();
}
