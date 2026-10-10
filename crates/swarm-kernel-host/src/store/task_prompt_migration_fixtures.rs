use crate::{
    config::Route,
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome, opencode_v2},
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::path::PathBuf;
use swarm_contracts::{
    module_catalog::{
        ActivationPolicy, ArtifactId, LaunchSpec, LifecycleOwnership, ModuleId, ProtectedRef,
        RestartPolicy, Sha256Digest,
    },
    module_contract::ModuleContractTemplate,
};

const COMMAND_ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
const COMMAND_ACP_ARTIFACT_ID: &str = "eliot-command.acp-rust.1";

fn route(runtime: &str, artifact_id: &str) -> Route {
    Route {
        alias: "task-prompt-migration".to_owned(),
        runtime: runtime.to_owned(),
        module_artifact_id: artifact_id.to_owned(),
        enabled: true,
        native_options: json!({}),
        workspace_option: None,
        owned_service: None,
        admission_policy: None,
    }
}

fn current_task_prompt_command() -> (RuntimeCommand, String) {
    let source_text = "Caller source: café 🐇".to_owned();
    let prompt = "Store-owned UTF-8 prompt: café 🌿".to_owned();
    let snapshot_sha256 = "a".repeat(64);
    let original_request = json!({
        "attempt_id":"attempt-task-prompt-current",
        "text":source_text
    });
    let input_sha256 = model::digest(model::canonical(&original_request).unwrap().as_bytes());
    let command = RuntimeCommand {
        operation_id: "operation-task-prompt-current".to_owned(),
        method: "task.dispatch".to_owned(),
        created_at_ms: 1,
        binding_id: "binding-task-prompt-current".to_owned(),
        generation: 3,
        native_root_id: Some("ses_task_prompt_current".to_owned()),
        route: json!({
            "runtime":opencode_v2::RUNTIME,
            "module_artifact_id":opencode_v2::TASK_PROMPT_ARTIFACT_ID
        }),
        input: json!({
            "attempt_id":"attempt-task-prompt-current",
            "text":source_text,
            "task_prompt":{
                "schema_id":"swarm.task_prompt",
                "schema_version":1,
                "task_id":"task-task-prompt-current",
                "task_revision":7,
                "attempt_id":"attempt-task-prompt-current",
                "task_snapshot_sha256":snapshot_sha256,
                "prompt_sha256":model::digest(prompt.as_bytes()),
                "prompt_bytes":prompt.as_bytes().len() as u64,
                "prompt":prompt
            },
            "task_dispatch_context":{
                "schema_version":1,
                "operation_id":"operation-task-prompt-current",
                "binding_id":"binding-task-prompt-current",
                "binding_generation":3,
                "worker_boot_id":"builtin-opencode-boot",
                "attempt_id":"attempt-task-prompt-current",
                "task_id":"task-task-prompt-current",
                "task_revision":7,
                "task_snapshot_sha256":snapshot_sha256,
                "source_text_sha256":model::digest(source_text.as_bytes()),
                "source_text_bytes":source_text.as_bytes().len() as u64
            }
        }),
        input_sha256: Some(input_sha256),
        target_input_sha256: None,
    };
    (command, prompt)
}

fn trusted_batch_v3() -> (Connection, Route, Value) {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);")
        .unwrap();
    let tx = db.unchecked_transaction().unwrap();

    let mut template = ModuleContractTemplate::codex_rust_controller_v3().unwrap();
    template.module_id = ModuleId::new("runtime.command").unwrap();
    template.artifact.artifact_id = ArtifactId::new(COMMAND_ARTIFACT_ID).unwrap();
    let descriptor = template
        .descriptor(
            LaunchSpec {
                executable: if cfg!(windows) {
                    PathBuf::from(r"C:\modules\command.exe")
                } else {
                    PathBuf::from("/modules/command")
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: Some(ProtectedRef::new("credential:command-fixture").unwrap()),
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: Some(Sha256Digest::new("b".repeat(64)).unwrap()),
            },
            LifecycleOwnership::OwnedService,
            ActivationPolicy::OnDemand,
            true,
            RestartPolicy::default(),
        )
        .unwrap();
    super::super::module_handshake::register_trusted_descriptor(&tx, descriptor.clone()).unwrap();
    let selector = json!({
        "schema_version":1,
        "module_id":descriptor.module_id,
        "artifact":descriptor.artifact,
        "registered_revision":1,
        "selected_revision":1
    });
    tx.commit().unwrap();
    (db, route("module", COMMAND_ARTIFACT_ID), selector)
}

#[test]
fn builtin_task_prompt_uses_store_bytes_and_emits_the_exact_dispatch_receipt() {
    let (command, expected_prompt) = current_task_prompt_command();
    let (prompt, receipt) =
        opencode_v2::task_prompt_migration_fixture_projection(&command).unwrap();
    let receipt = receipt.expect("current built-in TaskPrompt should produce normalized admission");

    assert_eq!(prompt, expected_prompt);
    assert_eq!(
        receipt["native_payload_sha256"],
        model::digest(prompt.as_bytes())
    );
    assert_eq!(
        receipt["native_payload_bytes"].as_u64(),
        Some(prompt.as_bytes().len() as u64)
    );
    assert_eq!(
        receipt["module_receipt"]["artifact"]["artifact_id"],
        opencode_v2::TASK_PROMPT_ARTIFACT_ID
    );
    assert_eq!(
        receipt["module_receipt"]["artifact"]["version"],
        opencode_v2::TASK_PROMPT_ARTIFACT_VERSION
    );
    assert_eq!(
        receipt["module_receipt"]["input_sha256"].as_str(),
        command.input_sha256.as_deref()
    );
    assert_eq!(
        receipt["native_input_id"],
        opencode_v2::input_id(&command.operation_id)
    );

    let original_request = json!({
        "attempt_id":"attempt-task-prompt-current",
        "text":"Caller source: café 🐇"
    });
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE operations(
            operation_id TEXT NOT NULL,
            binding_id TEXT NOT NULL,
            binding_generation INTEGER NOT NULL,
            original_request_json TEXT NOT NULL
        );",
    )
    .unwrap();
    db.execute(
        "INSERT INTO operations VALUES (?1,?2,?3,?4)",
        rusqlite::params![
            command.operation_id,
            command.binding_id,
            command.generation,
            model::canonical(&original_request).unwrap()
        ],
    )
    .unwrap();
    let binding = json!({
        "module_artifact_id":opencode_v2::TASK_PROMPT_ARTIFACT_ID,
        "route":{"runtime":opencode_v2::RUNTIME},
        "observation":{}
    });
    let outcome = RuntimeOutcome {
        operation_id: command.operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: Some(opencode_v2::input_id(&command.operation_id)),
        details: json!({"dispatch_admission":receipt}),
    };
    let module_receipt = super::super::runtime::validate_module_receipt_for_operation(
        &db,
        &command.binding_id,
        command.generation,
        &binding,
        &outcome,
    )
    .unwrap();
    assert_eq!(
        module_receipt.artifact.artifact_id.as_str(),
        opencode_v2::TASK_PROMPT_ARTIFACT_ID
    );

    let mut tampered_outcome = outcome;
    tampered_outcome.details["dispatch_admission"]["module_receipt"]["artifact"]["version"] =
        json!("1");
    assert_eq!(
        super::super::runtime::validate_module_receipt_for_operation(
            &db,
            &command.binding_id,
            command.generation,
            &binding,
            &tampered_outcome
        )
        .unwrap_err()
        .code,
        "MODULE_RECEIPT_INVALID"
    );

    let root = command.native_root_id.as_deref().unwrap();
    let item = json!({
        "id":opencode_v2::input_id(&command.operation_id),
        "sessionID":root,
        "type":"user",
        "delivery":"queue",
        "payload":{
            "text":expected_prompt,
            "metadata":{"eliot":{
                "binding":command.binding_id,
                "generation":command.generation,
                "operation":command.operation_id,
                "native_delivery":"queue"
            }}
        }
    });
    assert!(
        opencode_v2::task_prompt_migration_fixture_inbox_matches(&command, root, &item).unwrap()
    );
    let mut changed = item;
    changed["payload"]["text"] = json!("changed native prompt");
    assert!(
        !opencode_v2::task_prompt_migration_fixture_inbox_matches(&command, root, &changed)
            .unwrap()
    );
}

#[test]
fn builtin_task_prompt_rejects_digest_length_context_and_snapshot_fallback_changes() {
    let (command, prompt) = current_task_prompt_command();

    let mut bad_digest = command.clone();
    bad_digest.input["task_prompt"]["prompt_sha256"] = json!("0".repeat(64));
    assert_eq!(
        opencode_v2::task_prompt_migration_fixture_projection(&bad_digest)
            .unwrap_err()
            .code,
        "TASK_PROMPT_INVALID"
    );

    let mut bad_utf8_length = command.clone();
    bad_utf8_length.input["task_prompt"]["prompt_bytes"] = json!(prompt.chars().count() as u64);
    assert_eq!(
        opencode_v2::task_prompt_migration_fixture_projection(&bad_utf8_length)
            .unwrap_err()
            .code,
        "TASK_PROMPT_INVALID"
    );

    let mut bad_context = command.clone();
    bad_context.input["task_dispatch_context"]["operation_id"] = json!("another-operation");
    assert_eq!(
        opencode_v2::task_prompt_migration_fixture_projection(&bad_context)
            .unwrap_err()
            .code,
        "TASK_PROMPT_INVALID"
    );

    let mut raw_snapshot = command;
    raw_snapshot.input["task_snapshot"] = json!({"frozen":"snapshot"});
    assert_eq!(
        opencode_v2::task_prompt_migration_fixture_projection(&raw_snapshot)
            .unwrap_err()
            .code,
        "TASK_PROMPT_INVALID"
    );
}

#[test]
fn retained_builtin_and_batch_v3_keep_their_old_prompt_and_decoder_identity() {
    let legacy_prompt = "Implement v1\n\nELIOT immutable task snapshot:\n{\"brief\":{\"objective\":\"Keep .1 prompt\"},\"task_id\":\"task-migration-retained\"}";
    let legacy_command = RuntimeCommand {
        operation_id: "operation-retained-v1".to_owned(),
        method: "task.dispatch".to_owned(),
        created_at_ms: 1,
        binding_id: "binding-retained-v1".to_owned(),
        generation: 1,
        native_root_id: Some("ses_retained_v1".to_owned()),
        route: json!({
            "runtime":opencode_v2::RUNTIME,
            "module_artifact_id":opencode_v2::ARTIFACT_ID
        }),
        input: json!({
            "text":"Implement v1",
            "task_snapshot":{
                "brief":{"objective":"Keep .1 prompt"},
                "task_id":"task-migration-retained"
            }
        }),
        input_sha256: None,
        target_input_sha256: None,
    };
    let (projected, receipt) =
        opencode_v2::task_prompt_migration_fixture_projection(&legacy_command).unwrap();
    assert_eq!(projected, legacy_prompt);
    assert_eq!(
        model::digest(projected.as_bytes()),
        "b77efa27439bf06250c1ddc8e3add4b19c957b44f4387a1374bbfa650d44bfa7"
    );
    let retained_item = json!({
        "id":opencode_v2::input_id(&legacy_command.operation_id),
        "sessionID":"ses_retained_v1",
        "type":"user",
        "delivery":"queue",
        "payload":{
            "text":legacy_prompt,
            "metadata":{"eliot":{
                "binding":legacy_command.binding_id,
                "generation":legacy_command.generation,
                "operation":legacy_command.operation_id,
                "native_delivery":"queue"
            }}
        }
    });
    assert!(
        opencode_v2::task_prompt_migration_fixture_inbox_matches(
            &legacy_command,
            "ses_retained_v1",
            &retained_item
        )
        .unwrap()
    );
    assert!(receipt.is_none());

    let db = Connection::open_in_memory().unwrap();
    let legacy_binding = json!({
        "module_artifact_id":opencode_v2::ARTIFACT_ID,
        "route":{"runtime":opencode_v2::RUNTIME}
    });
    let current_binding = json!({
        "module_artifact_id":opencode_v2::TASK_PROMPT_ARTIFACT_ID,
        "route":{"runtime":opencode_v2::RUNTIME}
    });
    assert!(!super::selected(&db, &legacy_binding).unwrap());
    assert!(super::selected(&db, &current_binding).unwrap());
    assert_eq!(
        super::require_new_binding(&route(opencode_v2::RUNTIME, opencode_v2::ARTIFACT_ID), None)
            .unwrap_err()
            .code,
        "ARTIFACT_RETIRED"
    );
    super::require_new_binding(
        &route(opencode_v2::RUNTIME, opencode_v2::TASK_PROMPT_ARTIFACT_ID),
        None,
    )
    .unwrap();

    let (db, command_v3_route, selector) = trusted_batch_v3();
    let retained = super::super::module_handshake::retained_contract_identity(
        &db,
        COMMAND_ARTIFACT_ID,
        Some(&selector),
    )
    .unwrap()
    .unwrap();
    assert_eq!(retained.artifact.version.as_str(), "3");
    let retained_binding = json!({
        "module_artifact_id":COMMAND_ARTIFACT_ID,
        "route":{"runtime":"module"},
        "observation":{"module_contract_selector":selector}
    });
    assert!(!super::selected(&db, &retained_binding).unwrap());
    assert_eq!(
        super::super::module_handshake::selected_native_command_supported(
            &db,
            COMMAND_ARTIFACT_ID,
            Some(&selector),
            "task.dispatch",
            &json!({"attempt_id":"attempt-retained-v3","text":"legacy snapshot"})
        )
        .unwrap(),
        Some(true)
    );
    assert_eq!(
        super::require_new_binding(&command_v3_route, Some(&selector))
            .unwrap_err()
            .code,
        "ARTIFACT_RETIRED"
    );
}

#[test]
fn command_batch_v4_and_current_acp_selectors_remain_admissible() {
    let batch_v4 = route("module", COMMAND_ARTIFACT_ID);
    let batch_v4_selector = json!({
        "artifact":{"artifact_id":COMMAND_ARTIFACT_ID,"version":"4"}
    });
    super::require_new_binding(&batch_v4, Some(&batch_v4_selector)).unwrap();

    let current_acp = route("module", COMMAND_ACP_ARTIFACT_ID);
    let acp_selector = json!({
        "artifact":{"artifact_id":COMMAND_ACP_ARTIFACT_ID,"version":"1"}
    });
    super::require_new_binding(&current_acp, Some(&acp_selector)).unwrap();

    let later_batch = json!({
        "artifact":{"artifact_id":COMMAND_ARTIFACT_ID,"version":"5"}
    });
    super::require_new_binding(&batch_v4, Some(&later_batch)).unwrap();
}
