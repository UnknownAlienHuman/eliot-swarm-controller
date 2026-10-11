//! Same-process causal response retention through the public adapter APIs.
use serde_json::json;
use sha2::{Digest, Sha256};
use swarm_antigravity_adapter::stream::{
    MAX_RESPONSE_CHARS, MAX_RETAINED_RESPONSES, RetainedResponseMiss, StreamState,
};
use swarm_antigravity_adapter::{contract, controller::Controller, module_receipt, result_page};
use swarm_contracts::{
    module_contract::{MODULE_PROTOCOL_V1, ModuleContractClaim},
    runtime::{NormalizedResultOriginContext, NormalizedResultProducerOrigin, RuntimeCommand},
};
const BINDING: &str = "binding-seq";
const GENERATION: i64 = 7;
const ROOT: &str = "conv-seq";
const BOOT: &str = "boot-seq";
const PAGE_BYTES: usize = 65_536;

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

// Configure a child before startup; parallel libtest threads never mutate env.
fn with_contract(name: &str, case: impl FnOnce()) {
    if std::env::var("ELIOT_AG_SEQ_CASE").as_deref() == Ok(name) {
        case();
        return;
    }
    let template = contract::template().unwrap();
    let claim = ModuleContractClaim {
        schema_version: 1,
        module_id: template.module_id,
        artifact: template.artifact,
        protocol: MODULE_PROTOCOL_V1,
        capabilities: template.capabilities.into_iter().collect(),
        config_schema: template.config_schema,
        pre_input_open: template.pre_input_open,
        command_schemas: template.command_schemas.into_iter().collect(),
        event_schemas: template.event_schemas.into_iter().collect(),
    };
    claim.validate().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("ELIOT_AG_SEQ_CASE", name)
        .env(
            "ELIOT_SWARM_MODULE_CONTRACT",
            serde_json::to_string(&claim).unwrap(),
        )
        .status()
        .unwrap();
    assert!(status.success(), "isolated case {name} failed");
}
fn init() -> Vec<u8> {
    serde_json::to_vec(&json!({"event":"init","conversation_id":ROOT,
        "init":{"model":"gemini-3.8","tools":[]}}))
    .unwrap()
}
fn result(root: &str, text: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"event":"result","result":{
        "status":"SUCCESS","conversation_id":root,"response":text}}))
    .unwrap()
}
fn state() -> StreamState {
    let mut state = StreamState::new(0);
    state.consume_line(&init());
    state
}
fn controller() -> Controller {
    let mut controller = Controller::new(
        BOOT.into(),
        "scope-seq".into(),
        BINDING.into(),
        GENERATION,
        Some(ROOT.into()),
        true,
    );
    controller.consume_line(&init());
    controller
}
fn dispatch(id: &str) -> RuntimeCommand {
    let text = "source";
    let prompt = "perform the exact fixture task";
    let snapshot = hash(b"snapshot");
    RuntimeCommand {
        operation_id: id.into(),
        method: "task.dispatch".into(),
        created_at_ms: 1,
        binding_id: BINDING.into(),
        generation: GENERATION,
        native_root_id: Some(ROOT.into()),
        route: json!({"runtime":"antigravity","module_artifact_id":"eliot-antigravity.rust-headless.1"}),
        input: json!({"text":text,
            "task_prompt":{"schema_id":"swarm.task_prompt","schema_version":1,
                "task_id":"task-seq","task_revision":1,"attempt_id":"attempt-seq",
                "task_snapshot_sha256":snapshot,"prompt_sha256":hash(prompt.as_bytes()),
                "prompt_bytes":prompt.len(),"prompt":prompt},
            "task_dispatch_context":{"schema_version":1,"operation_id":id,"binding_id":BINDING,
                "binding_generation":GENERATION,"worker_boot_id":BOOT,"task_id":"task-seq",
                "task_revision":1,"attempt_id":"attempt-seq","task_snapshot_sha256":snapshot,
                "source_text_sha256":hash(text.as_bytes()),"source_text_bytes":text.len()}
        }),
        // Same digest deliberately cannot distinguish two Operation identities.
        input_sha256: Some(hash(b"identical source request")),
        target_input_sha256: None,
    }
}
fn origin(command: &RuntimeCommand) -> NormalizedResultOriginContext {
    let receipt = module_receipt::for_command(command).unwrap();
    let snapshot = hash(b"snapshot");
    let origin = NormalizedResultOriginContext {
        schema_version: 1,
        binding_id: BINDING.into(),
        binding_generation: GENERATION,
        task_id: "task-seq".into(),
        task_revision: 1,
        task_snapshot_sha256: snapshot.clone(),
        attempt_id: "attempt-seq".into(),
        target_operation_id: command.operation_id.clone(),
        target_input_sha256: receipt.input_sha256.clone(),
        selector_sha256: hash(b"selector"),
        producer: NormalizedResultProducerOrigin {
            assignment_id: command.operation_id.clone(),
            dispatch_operation_id: command.operation_id.clone(),
            attempt_id: "attempt-seq".into(),
            task_id: "task-seq".into(),
            task_revision: 1,
            task_snapshot_sha256: snapshot,
            source_text_sha256: hash(b"source"),
            source_text_bytes: 6,
            native_payload_sha256: hash(b"perform the exact fixture task"),
            native_payload_bytes: b"perform the exact fixture task".len() as u64,
            completion_condition: "native_turn_completed".into(),
            execution_complete: true,
            task_completion: "unknown".into(),
            disposition: "completed".into(),
            native_input_id: None,
            module_receipt: receipt,
        },
    };
    origin.validate().unwrap();
    origin
}
fn settle(controller: &mut Controller, command: &RuntimeCommand, text: &str) {
    controller.begin_prompt(command).unwrap();
    controller.consume_line(&result(ROOT, text));
    let outcome = controller
        .settle_terminal()
        .expect("exact next result settles");
    assert_eq!(outcome.operation_id, command.operation_id);
    controller.acknowledge_outcome(&command.operation_id);
    assert!(controller.pending_receipts().is_empty());
}
fn page_command(target: &RuntimeCommand, offset: u64, length: u64) -> RuntimeCommand {
    RuntimeCommand {
        operation_id: format!("read-{}-{offset}", target.operation_id),
        method: "agent.result".into(),
        created_at_ms: 2,
        binding_id: BINDING.into(),
        generation: GENERATION,
        native_root_id: Some(ROOT.into()),
        route: target.route.clone(),
        input: json!({"selector":{"kind":"normalized_response","input_operation_id":target.operation_id,
            "session_id":ROOT},"normalized_result_origin":origin(target),
            "offset_bytes":offset,"length_bytes":length}),
        input_sha256: Some(hash(format!("read-{offset}-{length}").as_bytes())),
        target_input_sha256: target.input_sha256.clone(),
    }
}

#[test]
fn exact_body_is_private_and_lookup_is_conversation_generation_fenced() {
    let mut state = state();
    let secret = "private-body-with-unicode-ответ";
    state.consume_line(&result(ROOT, secret));
    let retained = state.retained_response_for(1, ROOT, 0).unwrap();
    assert_eq!(retained.body, secret);
    assert_eq!(retained.response_sha256, hash(secret.as_bytes()));
    assert!(!state.snapshot().to_string().contains(secret));
    assert!(
        !serde_json::to_string(state.turns.back().unwrap())
            .unwrap()
            .contains(secret)
    );
    assert_eq!(
        state.retained_response_for(2, ROOT, 0).unwrap_err(),
        RetainedResponseMiss::Absent
    );
    assert_eq!(
        state.retained_response_for(1, "foreign", 0).unwrap_err(),
        RetainedResponseMiss::ConversationMismatch
    );
    assert_eq!(
        state.retained_response_for(1, ROOT, 1).unwrap_err(),
        RetainedResponseMiss::StaleOwner
    );
}
#[test]
fn oversized_body_is_not_published_as_truncated_text() {
    let mut state = state();
    state.consume_line(&result(ROOT, &"x".repeat(MAX_RESPONSE_CHARS + 1)));
    assert_eq!(
        state.retained_response_for(1, ROOT, 0).unwrap_err(),
        RetainedResponseMiss::Absent
    );
}
#[test]
fn failed_stream_invalidates_retained_body() {
    let mut state = state();
    state.consume_line(&result(ROOT, "answer"));
    assert!(state.retained_response_for(1, ROOT, 0).is_ok());
    state.note_stream_failure();
    assert_eq!(
        state.retained_response_for(1, ROOT, 0).unwrap_err(),
        RetainedResponseMiss::Absent
    );
}
#[test]
fn normalized_pages_survive_ack_and_bind_full_body_and_ranges() {
    with_contract(
        "normalized_pages_survive_ack_and_bind_full_body_and_ranges",
        || {
            let mut controller = controller();
            let target = dispatch("op-pages");
            let text = "я".repeat(PAGE_BYTES / 2 + 2048);
            settle(&mut controller, &target, &text);
            let first =
                result_page::build_normalized(&page_command(&target, 0, u64::MAX), &controller)
                    .unwrap();
            let second = result_page::build_normalized(
                &page_command(&target, PAGE_BYTES as u64, PAGE_BYTES as u64),
                &controller,
            )
            .unwrap();
            assert_eq!(first["page"]["byte_length"], PAGE_BYTES);
            assert_eq!(first["page"]["eof"], false);
            assert_eq!(second["page"]["byte_length"], 4096);
            assert_eq!(second["page"]["eof"], true);
            for page in [&first, &second] {
                assert_eq!(
                    page["page"]["source"]["payload_sha256"],
                    hash(text.as_bytes())
                );
                assert_eq!(page["page"]["source"]["payload_bytes"], text.len());
                assert_eq!(page["page"]["source"]["execution_complete"], false);
                assert_eq!(page["page"]["source"]["task_completion"], "unknown");
                assert_eq!(page["page"]["source"]["native_replay"], false);
                assert!(
                    page["page"]["source"]
                        .get("native_response_identity")
                        .is_none()
                );
            }
            assert_eq!(
                first["page"]["page_sha256"],
                hash(&text.as_bytes()[..PAGE_BYTES])
            );
            assert_eq!(
                second["page"]["page_sha256"],
                hash(&text.as_bytes()[PAGE_BYTES..])
            );
            assert_eq!(
                result_page::build_normalized(
                    &page_command(&target, text.len() as u64 + 1, 1),
                    &controller
                )
                .unwrap_err()
                .code,
                "RESULT_RANGE_INVALID"
            );
            assert_eq!(
                result_page::build_normalized(&page_command(&target, 0, 0), &controller)
                    .unwrap_err()
                    .code,
                "RESULT_RANGE_INVALID"
            );
            assert!(
                !controller
                    .prepare_observation()
                    .state
                    .to_string()
                    .contains(&text)
            );
        },
    );
}
#[test]
fn same_digest_other_operation_and_mismatched_receipt_cannot_borrow_body() {
    with_contract(
        "same_digest_other_operation_and_mismatched_receipt_cannot_borrow_body",
        || {
            let mut controller = controller();
            let settled = dispatch("op-settled");
            let other = dispatch("op-other");
            settle(&mut controller, &settled, "answer");
            let own = origin(&settled);
            assert_eq!(
                controller
                    .retained_response_for_operation("op-settled", &own)
                    .unwrap()
                    .body,
                "answer"
            );
            assert_eq!(
                controller
                    .retained_response_for_operation("op-settled", &origin(&other))
                    .unwrap_err(),
                "NATIVE_RESULT_BODY_ORIGIN_MISMATCH"
            );
            let mut wrong = own;
            wrong.producer.module_receipt.artifact.build_id = Some("different-build".into());
            assert_eq!(
                controller
                    .retained_response_for_operation("op-settled", &wrong)
                    .unwrap_err(),
                "NATIVE_RESULT_BODY_ORIGIN_MISMATCH"
            );
            assert_eq!(
                result_page::build_normalized(&page_command(&other, 0, 100), &controller)
                    .unwrap_err()
                    .code,
                "RESULT_BODY_UNAVAILABLE"
            );
            let mut foreign = page_command(&settled, 0, 100);
            foreign.native_root_id = Some("foreign".into());
            foreign.input["selector"]["session_id"] = json!("foreign");
            assert_eq!(
                result_page::build_normalized(&foreign, &controller)
                    .unwrap_err()
                    .code,
                "RESULT_PROVENANCE_INVALID"
            );
        },
    );
}
#[test]
fn completed_mapping_evicts_oldest_ordinal_with_reverse_lexical_ids() {
    with_contract(
        "completed_mapping_evicts_oldest_ordinal_with_reverse_lexical_ids",
        || {
            let mut controller = controller();
            for index in 0..=MAX_RETAINED_RESPONSES {
                let target = dispatch(&format!("op-{:04}", MAX_RETAINED_RESPONSES - index));
                settle(&mut controller, &target, &format!("answer {index}"));
            }
            let newest = dispatch("op-0000");
            assert_eq!(
                controller
                    .retained_response_for_operation(&newest.operation_id, &origin(&newest))
                    .unwrap()
                    .body,
                format!("answer {MAX_RETAINED_RESPONSES}")
            );
            let oldest = dispatch(&format!("op-{MAX_RETAINED_RESPONSES:04}"));
            assert_eq!(
                controller
                    .retained_response_for_operation(&oldest.operation_id, &origin(&oldest))
                    .unwrap_err(),
                "NATIVE_RESULT_BODY_UNAVAILABLE"
            );
        },
    );
}
#[test]
fn skipped_ordinal_foreign_conversation_and_stale_generation_do_not_settle() {
    with_contract(
        "skipped_ordinal_foreign_conversation_and_stale_generation_do_not_settle",
        || {
            let target = dispatch("op-fenced");
            for invalid in ["ordinal", "conversation", "generation"] {
                let mut controller = controller();
                controller.begin_prompt(&target).unwrap();
                assert_eq!(
                    controller
                        .begin_prompt(&dispatch("op-next"))
                        .unwrap_err()
                        .code,
                    "NATIVE_TURN_PENDING"
                );
                match invalid {
                    "ordinal" => {
                        controller.consume_line(&result(ROOT, "first"));
                        controller.consume_line(&result(ROOT, "second"));
                    }
                    "conversation" => controller.consume_line(&result("foreign", "answer")),
                    _ => {
                        controller.stream_mut().stream_generation += 1;
                        controller.consume_line(&result(ROOT, "answer"));
                    }
                }
                assert!(
                    controller.settle_terminal().is_none(),
                    "{invalid} settled a different prompt"
                );
                assert_eq!(
                    controller
                        .retained_response_for_operation(&target.operation_id, &origin(&target))
                        .unwrap_err(),
                    "NATIVE_RESULT_BODY_UNAVAILABLE"
                );
            }
        },
    );
}
