//! Exact, bounded status pages for a Command dispatch.
//!
//! Command's one-shot JSON result does not carry a native assistant-message
//! identity. This projection reports only the saved Operation state and
//! checked terminal facts; it never publishes response text or task completion.

use super::{meta, operations, results, runtime, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_contracts::{
    module_command::{
        CommandCaptureArtifactPage, CommandCaptureOutputCoverage, CommandCaptureReleaseEvidence,
    },
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};

const COMMAND_ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
const COMMAND_ARTIFACT_VERSION: &str = "3";
const COMMAND_MODULE_ID: &str = "runtime.command";
const RESULT_CAPABILITY: &str = "agent.result";
const MAX_PAGE_BYTES: usize = crate::artifacts::MAX_PAGE_BYTES;

/// Validate the Command v3 result selector at manager admission. The
/// descriptor capability is compatibility metadata; normal Store method,
/// principal, binding and Attempt checks remain authoritative.
pub(super) fn validate_request(db: &Connection, binding: &Value, request: &Value) -> Result<Value> {
    require_result_capability(db, binding)?;
    let selector = request
        .get("selector")
        .ok_or_else(|| Error::invalid("Command result selector is required"))?;
    let kind = model::text(selector, "kind")?;
    match kind {
        "command_status" => model::fields(selector, &["kind", "input_operation_id"])?,
        "command_output" => {
            model::fields(selector, &["kind", "input_operation_id", "native_output"])?
        }
        _ => {
            return Err(Error::new(
                "RESULT_SELECTOR_UNSUPPORTED",
                "Command v3 supports only exact dispatch status and captured output pages",
            ));
        }
    }
    let target_id = model::text(selector, "input_operation_id")?;
    let offset = request
        .get("offset_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let length = request
        .get("length_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(MAX_PAGE_BYTES as u64);
    if length == 0 || length > MAX_PAGE_BYTES as u64 || offset > i64::MAX as u64 {
        return Err(Error::invalid("Command status page range is invalid"));
    }
    let snapshot = if kind == "command_status" {
        target_snapshot(db, binding, target_id)?
    } else {
        target_output_snapshot(
            db,
            binding,
            target_id,
            model::text(selector, "native_output")?,
        )?
    };
    let total_bytes = if kind == "command_status" {
        page_bytes(&json!({"target_operation_status":snapshot}))?.len() as u64
    } else {
        snapshot["stored_bytes"]
            .as_u64()
            .ok_or_else(|| Error::invalid("Command captured size is invalid"))?
    };
    if offset > total_bytes {
        return Err(Error::new(
            "RESULT_RANGE_INVALID",
            "Command result offset exceeds the captured body",
        ));
    }
    Ok(snapshot)
}

/// Build the Store-authoritative target snapshot injected into `module.next`.
/// Terminal dispatches retain their typed receipt. An `outcome_unknown`
/// dispatch can be shown as Store status without a module receipt; it never
/// becomes terminal or authorizes a retry.
pub(super) fn target_snapshot(
    db: &Connection,
    binding: &Value,
    target_operation_id: &str,
) -> Result<Value> {
    require_result_capability(db, binding)?;
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let target = operations::get_operation(db, target_operation_id)?;
    let state = model::text(&target, "state")?;
    let unknown = state == "outcome_unknown";
    if target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || target["method"] != "task.dispatch"
        || (!unknown && !matches!(state, "settled" | "rejected"))
    {
        return Err(Error::new(
            "RESULT_TARGET_NOT_TERMINAL",
            "Command status requires a terminal dispatch on this binding generation",
        ));
    }

    let raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
            params![target_operation_id, binding_id, generation],
            |row| row.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| {
        Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command status target is absent from this binding generation",
        )
    })?;
    let request: Value = serde_json::from_str(&raw)?;
    let input_sha256 = model::digest(model::canonical(&request)?.as_bytes());

    let (
        outcome_name,
        expected_state,
        completion,
        diagnostic_code,
        result_subtype,
        exit_code,
        timed_out,
        signal_observed,
        receipt,
    ) = if unknown && target["result"].is_null() {
        (
            "unknown",
            "outcome_unknown",
            json!("module_outcome_not_retained"),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        )
    } else {
        let outcome: RuntimeOutcome =
            serde_json::from_value(target["result"].clone()).map_err(|_| {
                Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command dispatch has no typed retained outcome",
                )
            })?;
        if outcome.operation_id != target_operation_id
            || outcome.native_root_id.is_some()
            || outcome.native_scope_key.is_some()
        {
            return Err(Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "Command receipt differs from its retained Operation",
            ));
        }
        if unknown {
            if !matches!(&outcome.outcome, EffectOutcome::Unknown)
                || outcome.details["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
                || outcome.details["task_acceptance_claimed"] != false
                || outcome.details["result_page_available"] != false
            {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "unknown Command status contains incompatible retained facts",
                ));
            }
            let receipt = runtime::validate_module_receipt_for_operation(
                db, binding_id, generation, binding, &outcome,
            )?;
            if receipt.input_sha256 != input_sha256 {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command receipt is not bound to the exact original request",
                ));
            }
            (
                "unknown",
                "outcome_unknown",
                json!("native_result_unconfirmed"),
                safe_diagnostic(&outcome.details["diagnostic_code"]),
                Value::Null,
                Value::Null,
                outcome
                    .details
                    .get("timed_out")
                    .filter(|value| value.is_boolean())
                    .cloned()
                    .unwrap_or(Value::Null),
                outcome
                    .details
                    .get("signal")
                    .map(|value| json!(!value.is_null()))
                    .unwrap_or(Value::Null),
                json!(receipt),
            )
        } else {
            let (outcome_name, expected_state) = match outcome.outcome {
                EffectOutcome::Applied => ("applied", "settled"),
                EffectOutcome::Rejected => ("rejected", "rejected"),
                EffectOutcome::Accepted | EffectOutcome::Unknown => {
                    return Err(Error::new(
                        "RESULT_TARGET_NOT_TERMINAL",
                        "Command status target has no terminal outcome",
                    ));
                }
            };
            if state != expected_state {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command terminal state differs from its retained outcome",
                ));
            }
            let receipt = runtime::validate_module_receipt_for_operation(
                db, binding_id, generation, binding, &outcome,
            )?;
            if receipt.input_sha256 != input_sha256 {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command receipt is not bound to the exact original request",
                ));
            }
            let details = &outcome.details;
            let completion = model::text(details, "completion_condition")?;
            let (diagnostic_code, result_subtype, exit_code, timed_out, signal_observed) =
                terminal_facts(details, outcome_name)?;
            (
                outcome_name,
                expected_state,
                json!(completion),
                diagnostic_code,
                result_subtype,
                exit_code,
                json!(timed_out),
                json!(signal_observed),
                json!(receipt),
            )
        }
    };
    Ok(json!({
        "schema_version":1,
        "operation_id":target_operation_id,
        "method":"task.dispatch",
        "operation_state":expected_state,
        "operation_outcome":outcome_name,
        "completion_condition":completion,
        "diagnostic_code":diagnostic_code,
        "result_subtype":result_subtype,
        "exit_code":exit_code,
        "timed_out":timed_out,
        "signal_observed":signal_observed,
        "input_sha256":input_sha256,
        "module_receipt":receipt,
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false
    }))
}

/// Capture facts are available only when Store retained the typed Command
/// outcome that named the exact bounded native stream and process receipt.
/// An `outcome_unknown` Operation without that receipt is not output evidence.
pub(super) fn target_output_snapshot(
    db: &Connection,
    binding: &Value,
    target_operation_id: &str,
    native_output: &str,
) -> Result<Value> {
    require_result_capability(db, binding)?;
    validate_native_output_name(native_output)?;
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let target = operations::get_operation(db, target_operation_id)?;
    let state = model::text(&target, "state")?;
    if target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || target["method"] != "task.dispatch"
        || !matches!(state, "settled" | "rejected" | "outcome_unknown")
    {
        return Err(Error::new(
            "BATCH_OUTPUT_UNAVAILABLE",
            "captured output requires this binding's retained dispatch",
        ));
    }
    let outcome: RuntimeOutcome =
        serde_json::from_value(target["result"].clone()).map_err(|_| {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "the exact typed Command outcome is not retained",
            )
        })?;
    let expected_state = match outcome.outcome {
        EffectOutcome::Applied if state == "settled" => "applied",
        EffectOutcome::Rejected if state == "rejected" => "rejected",
        EffectOutcome::Unknown if state == "outcome_unknown" => "unknown",
        _ => {
            return Err(Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "Command dispatch state differs from its typed outcome",
            ));
        }
    };
    if outcome.operation_id != target_operation_id
        || outcome.native_root_id.is_some()
        || outcome.native_scope_key.is_some()
        || outcome.details["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
        || outcome.details["task_acceptance_claimed"] != false
        || outcome.details["result_page_available"] != false
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command output target has incompatible retained native facts",
        ));
    }
    let raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='task.dispatch'",
            params![target_operation_id, binding_id, generation],
            |row| row.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| {
        Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command output target is absent from this binding generation",
        )
    })?;
    let request: Value = serde_json::from_str(&raw)?;
    let input_sha256 = model::digest(model::canonical(&request)?.as_bytes());
    let receipt = runtime::validate_module_receipt_for_operation(
        db, binding_id, generation, binding, &outcome,
    )?;
    if receipt.input_sha256 != input_sha256
        || outcome.details["input_sha256"].as_str() != Some(input_sha256.as_str())
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command output receipt differs from the exact dispatch request",
        ));
    }
    let normalized_dispatch = normalized_dispatch_enabled(db, binding)?;
    let worker_boot_id = outcome.details["dispatch_admission"]["worker_boot_id"].as_str();
    let dispatch_context = dispatch_context_projection(
        db,
        normalized_dispatch,
        binding_id,
        generation,
        target_operation_id,
        &request,
        worker_boot_id,
    )?;
    let dispatch_admission = retained_dispatch_admission(
        &outcome,
        normalized_dispatch,
        &dispatch_context,
        &receipt,
        binding_id,
        generation,
    )?;
    let stream = if native_output == "stdout.ndjson" {
        "stdout"
    } else {
        "stderr"
    };
    let facts = capture_facts(&outcome.details, stream)?;
    let native_child = outcome
        .details
        .get("native_child")
        .filter(|value| value.is_object())
        .or_else(|| outcome.details.get("direct_child"))
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "the native process receipt is not retained",
            )
        })?;
    validate_native_child(&native_child)?;
    let (stream_bytes, stored_bytes, stream_sha256, stored_sha256, truncated, read_error) = facts;
    Ok(json!({
        "schema_version":1,
        "operation_id":target_operation_id,
        "method":"task.dispatch",
        "operation_state":state,
        "operation_outcome":expected_state,
        "completion_condition":outcome.details["completion_condition"],
        "input_sha256":input_sha256,
        "module_receipt":receipt,
        "task_dispatch_context":dispatch_context,
        "dispatch_admission":dispatch_admission,
        "native_output":native_output,
        "stream_bytes":stream_bytes,
        "stored_bytes":stored_bytes,
        "stream_sha256":stream_sha256,
        "stored_sha256":stored_sha256,
        "truncated":truncated,
        "read_error":read_error,
        "native_child":native_child,
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false
    }))
}

fn normalized_dispatch_enabled(db: &Connection, binding: &Value) -> Result<bool> {
    let Some(selector) = binding["observation"].get("module_contract_selector") else {
        return Ok(false);
    };
    let artifact_id = model::text(binding, "module_artifact_id")?;
    let retained =
        super::module_handshake::retained_contract_identity(db, artifact_id, Some(selector))?
            .ok_or_else(|| {
                Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command dispatch descriptor is no longer retained",
                )
            })?;
    let context_schema = swarm_contracts::module_contract::task_dispatch_context_schema();
    let admission_schema = swarm_contracts::module_contract::task_dispatch_admission_schema();
    let declares_context = retained.command_schemas.contains(&context_schema);
    let declares_admission = retained.event_schemas.contains(&admission_schema);
    if declares_context != declares_admission {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "normalized Command dispatch requires both context and admission schemas",
        ));
    }
    Ok(declares_context)
}

fn dispatch_context_projection(
    db: &Connection,
    normalized: bool,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    request: &Value,
    worker_boot_id: Option<&str>,
) -> Result<Value> {
    if !normalized {
        return Ok(Value::Null);
    }
    let attempt_id = model::text(request, "attempt_id")?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let source_text = model::text(request, "text")?;
    if attempt["binding_id"] != binding_id
        || attempt["binding_generation"] != generation
        || attempt["start_operation_id"] != operation_id
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "dispatch context differs from its exact retained Attempt owner",
        ));
    }
    if worker_boot_id.is_some_and(|id| id.trim().is_empty() || id.len() > 256) {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "normalized dispatch boot identity is invalid",
        ));
    }
    Ok(json!({
        "schema_version":1,
        "operation_id":operation_id,
        "binding_id":binding_id,
        "binding_generation":generation,
        "worker_boot_id":worker_boot_id,
        "attempt_id":attempt_id,
        "task_id":model::text(&attempt, "task_id")?,
        "task_revision":model::positive(&attempt, "task_revision")?,
        "task_snapshot_sha256":model::digest(model::canonical(&attempt["task_snapshot"])?.as_bytes()),
        "source_text_sha256":model::digest(source_text.as_bytes()),
        "source_text_bytes":u64::try_from(source_text.len()).map_err(|_| Error::invalid("task dispatch text length is out of range"))?
    }))
}

fn retained_dispatch_admission(
    outcome: &RuntimeOutcome,
    normalized: bool,
    context: &Value,
    module_receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    binding_id: &str,
    generation: i64,
) -> Result<Value> {
    let raw = &outcome.details["dispatch_admission"];
    if !normalized || !matches!(&outcome.outcome, EffectOutcome::Applied) {
        if !raw.is_null() {
            return Err(Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "dispatch admission receipt is not permitted for this retained outcome",
            ));
        }
        return Ok(Value::Null);
    }
    if context.is_null() {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "normalized dispatch context is unavailable",
        ));
    }
    let receipt: TaskDispatchAdmissionReceipt =
        serde_json::from_value(raw.clone()).map_err(|_| {
            Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "known normalized dispatch outcome lacks its typed admission receipt",
            )
        })?;
    receipt.validate().map_err(|_| {
        Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "normalized dispatch admission receipt is invalid",
        )
    })?;
    if !dispatch_context_matches(context, &receipt.context())
        || receipt.module_receipt != *module_receipt
        || receipt.operation_id.as_str() != model::text(context, "operation_id")?
        || receipt.binding_id != binding_id
        || receipt.binding_generation != generation
        || outcome.native_input_id != receipt.native_input_id
        || outcome.details["prompt_sha256"].as_str() != Some(receipt.native_payload_sha256.as_str())
        || outcome.details["prompt_bytes"].as_u64() != Some(receipt.native_payload_bytes)
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "dispatch admission receipt differs from its exact native invocation",
        ));
    }
    Ok(serde_json::to_value(receipt)?)
}

fn dispatch_context_matches(projection: &Value, context: &TaskDispatchContext) -> bool {
    projection["schema_version"] == context.schema_version
        && projection["operation_id"] == context.operation_id
        && projection["binding_id"] == context.binding_id
        && projection["binding_generation"] == context.binding_generation
        && (projection["worker_boot_id"].is_null()
            || projection["worker_boot_id"] == context.worker_boot_id)
        && projection["attempt_id"] == context.attempt_id
        && projection["task_id"] == context.task_id
        && projection["task_revision"] == context.task_revision
        && projection["task_snapshot_sha256"] == context.task_snapshot_sha256
        && projection["source_text_sha256"] == context.source_text_sha256
        && projection["source_text_bytes"] == context.source_text_bytes
}

fn capture_facts(details: &Value, stream: &str) -> Result<(u64, u64, String, String, bool, bool)> {
    let stream_bytes = capture_fact(details, stream, "bytes", "bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::new("BATCH_OUTPUT_UNAVAILABLE", "stream size is not retained"))?;
    let stored_bytes = capture_fact(details, stream, "stored_bytes", "stored_bytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "stored stream size is not retained",
            )
        })?;
    let stream_sha256 = capture_fact(details, stream, "sha256", "sha256")
        .and_then(Value::as_str)
        .filter(|value| valid_digest(value))
        .ok_or_else(|| Error::new("BATCH_OUTPUT_UNAVAILABLE", "stream digest is not retained"))?
        .to_owned();
    let stored_sha256 = capture_fact(details, stream, "stored_sha256", "stored_sha256")
        .and_then(Value::as_str)
        .filter(|value| valid_digest(value))
        .ok_or_else(|| {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "stored stream digest is not retained",
            )
        })?
        .to_owned();
    let truncated = capture_fact(details, stream, "truncated", "truncated")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "truncation fact is not retained",
            )
        })?;
    let read_error = capture_fact(details, stream, "read_error", "read_error")
        .and_then(Value::as_bool)
        .ok_or_else(|| Error::new("BATCH_OUTPUT_UNAVAILABLE", "read status is not retained"))?;
    let native_output = if stream == "stdout" {
        "stdout.ndjson"
    } else {
        "stderr.txt"
    };
    validate_capture_values(
        native_output,
        stream_bytes,
        stored_bytes,
        &stream_sha256,
        &stored_sha256,
        truncated,
        read_error,
    )?;
    Ok((
        stream_bytes,
        stored_bytes,
        stream_sha256,
        stored_sha256,
        truncated,
        read_error,
    ))
}

fn capture_fact<'a>(
    details: &'a Value,
    stream: &str,
    top_suffix: &str,
    nested_name: &str,
) -> Option<&'a Value> {
    let top_name = format!("{stream}_{top_suffix}");
    details
        .get(top_name.as_str())
        .or_else(|| details.get(stream).and_then(|value| value.get(nested_name)))
}

fn validate_capture_values(
    native_output: &str,
    stream_bytes: u64,
    stored_bytes: u64,
    stream_sha256: &str,
    stored_sha256: &str,
    truncated: bool,
    read_error: bool,
) -> Result<()> {
    let maximum = if native_output == "stdout.ndjson" {
        16 * 1024 * 1024
    } else {
        256 * 1024
    };
    if !valid_digest(stream_sha256)
        || !valid_digest(stored_sha256)
        || stored_bytes > maximum
        || stream_bytes < stored_bytes
        || (stream_bytes > stored_bytes && !truncated)
        || (!truncated && !read_error && stream_bytes != stored_bytes)
        || (!truncated && !read_error && stream_sha256 != stored_sha256)
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command capture metadata is internally inconsistent",
        ));
    }
    Ok(())
}

fn validate_native_output_name(native_output: &str) -> Result<()> {
    if matches!(native_output, "stdout.ndjson" | "stderr.txt") {
        Ok(())
    } else {
        Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "Command output must select stdout.ndjson or stderr.txt",
        ))
    }
}

fn validate_native_child(child: &Value) -> Result<()> {
    model::fields(
        child,
        &[
            "spawn_returned_pid",
            "birth_identity",
            "exit_observed_through_child_handle",
            "exit",
            "family_departure_claimed",
            "manager_group_drain_required",
        ],
    )?;
    if child["family_departure_claimed"] != false || child["manager_group_drain_required"] != true {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "native process receipt exceeds the Command direct-child contract",
        ));
    }
    Ok(())
}

/// Return the snapshot sealed when the Manager admitted this result Operation.
/// The dispatch may be reconciled later; acknowledgement retries must validate
/// the same historical status bytes rather than silently changing the page.
/// Older queued terminal-only pages predate this field and can be reconstructed
/// safely because their terminal Operation state cannot change.
pub(super) fn admitted_target_snapshot(
    db: &Connection,
    result_operation_id: &str,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
) -> Result<Value> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='agent.result'",
            params![result_operation_id, binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((original, effective)) = row else {
        return Err(Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command result Operation is absent from this binding generation",
        ));
    };
    let original: Value = serde_json::from_str(&original)?;
    if original["selector"]["kind"] != "command_status"
        || original["selector"]["input_operation_id"] != target_operation_id
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command result Operation selector differs from its retained target",
        ));
    }
    let effective: Value = serde_json::from_str(&effective)?;
    let snapshot = match effective.get("command_status_target_snapshot") {
        Some(snapshot) if snapshot.is_object() => snapshot.clone(),
        Some(_) => {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "sealed Command status snapshot is malformed",
            ));
        }
        None => {
            // Compatibility for a queued v3 Operation admitted before this
            // snapshot field existed. Such an Operation can only have passed
            // the old terminal-only admission check.
            let binding = operations::get_binding(db, binding_id, generation)?;
            let snapshot = target_snapshot(db, &binding, target_operation_id)?;
            if snapshot["operation_state"] == "outcome_unknown" {
                return Err(Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "unknown Command status requires a Manager-admission snapshot",
                ));
            }
            snapshot
        }
    };
    validate_frozen_snapshot(db, binding_id, generation, target_operation_id, &snapshot)?;
    Ok(snapshot)
}

/// Return a Store scope for an already-admitted Command status result. This
/// deliberately ignores client disablement, binding release, and link changes
/// only after checking the immutable result Operation and its sealed target.
pub(super) fn admitted_status_scope(
    db: &Connection,
    principal: &Principal,
    result_operation_id: &str,
) -> Result<Option<(String, i64, Value)>> {
    if principal.role != Role::Module {
        return Ok(None);
    }
    let client = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "module is not registered"))?;
    if client["role"] != "module" {
        return Err(Error::new("UNAUTHORIZED", "client is not a module"));
    }
    let binding_id = model::text(&client, "binding_id")?;
    let generation = model::positive(&client, "binding_generation")?;
    let binding = operations::get_binding(db, binding_id, generation)?;
    if binding["observation"]["module_client_id"] != principal.client_id {
        return Err(Error::new(
            "MODULE_OWNER_MISMATCH",
            "credential does not own this Command binding",
        ));
    }
    if !crate::runtime::batch::is_rust_command_route(&binding["route"]) {
        return Ok(None);
    }

    let operation = operations::get_operation(db, result_operation_id)?;
    if operation["method"] != "agent.result" {
        return Ok(None);
    }
    let original_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [result_operation_id],
        |row| row.get(0),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let selector = &original["selector"];
    if selector["kind"] != "command_status" {
        return Ok(None);
    }
    model::fields(selector, &["kind", "input_operation_id"])?;
    let target_operation_id = model::text(selector, "input_operation_id")?;
    if !matches!(
        operation["state"].as_str(),
        Some("sending" | "native_accepted" | "outcome_unknown" | "settled")
    ) {
        return Err(Error::conflict(
            "Command status page does not belong to an admitted result Operation",
        ));
    }

    if operation["binding_id"] != binding_id || operation["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "Command status result belongs to another binding generation",
        ));
    }
    let target = operations::get_operation(db, target_operation_id)?;
    if target["method"] != "task.dispatch"
        || target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || operation["task_id"] != target["task_id"]
        || operation["attempt_id"] != target["attempt_id"]
    {
        return Err(Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command status result does not name its exact retained Task dispatch",
        ));
    }
    admitted_target_snapshot(
        db,
        result_operation_id,
        binding_id,
        generation,
        target_operation_id,
    )?;
    Ok(Some((binding_id.to_owned(), generation, binding)))
}

pub(super) fn admitted_output_snapshot(
    db: &Connection,
    result_operation_id: &str,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
    native_output: &str,
) -> Result<Value> {
    validate_native_output_name(native_output)?;
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='agent.result'",
            params![result_operation_id, binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((original, effective)) = row else {
        return Err(Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command output Operation is absent from this binding generation",
        ));
    };
    let original: Value = serde_json::from_str(&original)?;
    if original["selector"]["kind"] != "command_output"
        || original["selector"]["input_operation_id"] != target_operation_id
        || original["selector"]["native_output"] != native_output
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command output selector differs from its retained target",
        ));
    }
    let effective: Value = serde_json::from_str(&effective)?;
    let snapshot = effective
        .get("command_output_target_snapshot")
        .filter(|snapshot| snapshot.is_object())
        .cloned()
        .ok_or_else(|| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "Command output has no Manager-admission snapshot",
            )
        })?;
    validate_frozen_output_snapshot(
        db,
        binding_id,
        generation,
        target_operation_id,
        native_output,
        &snapshot,
    )?;
    Ok(snapshot)
}

/// Build the closed release proof from settled result Operations and their
/// immutable Store artifact receipts. The caller must verify every returned
/// ArtifactRecord against its file and re-run this projection in the same
/// transaction that admits the release Operation.
pub(super) fn command_capture_release_readback(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
) -> Result<(CommandCaptureReleaseEvidence, Vec<ArtifactRecord>)> {
    let target = operations::get_operation(db, target_operation_id)?;
    if target["method"] != "task.dispatch"
        || target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || !matches!(
            target["state"].as_str(),
            Some("settled" | "rejected" | "outcome_unknown")
        )
        || !target["result"].is_object()
        || target["result"]["operation_id"] != target_operation_id
    {
        return Err(Error::new(
            "COMMAND_RELEASE_TARGET_UNRESOLVED",
            "Command capture release requires the exact recorded dispatch outcome",
        ));
    }
    let target_input_sha256 = request_digest(db, target_operation_id, binding_id, generation)?;
    let target_outcome_sha256 = model::digest(model::canonical(&target["result"])?.as_bytes());
    let retained_evidence = retained_command_release_evidence(
        db,
        binding_id,
        generation,
        target_operation_id,
        &target,
        &target_input_sha256,
        &target_outcome_sha256,
    )?;
    let mut outputs = [
        CommandCaptureOutputCoverage {
            native_output: "stdout.ndjson".to_owned(),
            target_command_output: Value::Null,
            pages: Vec::new(),
        },
        CommandCaptureOutputCoverage {
            native_output: "stderr.txt".to_owned(),
            target_command_output: Value::Null,
            pages: Vec::new(),
        },
    ];
    let mut artifacts = Vec::new();
    let mut statement = db.prepare(
        "SELECT operation_id,original_request_json,state,result_json FROM operations \
         WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.result' \
         ORDER BY created_at_ms,operation_id",
    )?;
    let rows = statement
        .query_map(params![binding_id, generation], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    for (result_operation_id, original_raw, state, result_raw) in rows {
        if let Some(retained) = &retained_evidence
            && !retained.outputs.iter().any(|output| {
                output
                    .pages
                    .iter()
                    .any(|page| page.result_operation_id == result_operation_id)
            })
        {
            // Once Store has admitted the immutable release Operation, later
            // artifact-backed reads are served from its retained proof. They
            // must not widen or overlap the coverage that authorized release.
            continue;
        }
        let original: Value = serde_json::from_str(&original_raw)?;
        let selector = &original["selector"];
        if selector["input_operation_id"] != target_operation_id {
            continue;
        }
        // A terminally rejected read published no successful artifact and is
        // no longer a competing read. Later result Operations may retry it;
        // full immutable coverage below still gates release.
        if state == "rejected" {
            continue;
        }
        if state != "settled" {
            return Err(Error::new(
                "COMMAND_RELEASE_RESULT_UNRESOLVED",
                "Command capture release is blocked by an unresolved result read",
            ));
        }
        let result: Value = serde_json::from_str(result_raw.as_deref().ok_or_else(|| {
            Error::new(
                "COMMAND_RELEASE_RESULT_UNRESOLVED",
                "settled Command result Operation has no stored receipt",
            )
        })?)?;
        if result["outcome"] != "applied" {
            return Err(Error::new(
                "COMMAND_RELEASE_RESULT_UNRESOLVED",
                "Command result Operation has no successful artifact receipt",
            ));
        }
        match selector["kind"].as_str() {
            Some("command_status") => continue,
            Some("command_output") => {}
            _ => {
                return Err(Error::new(
                    "COMMAND_RELEASE_RESULT_INVALID",
                    "result read for the Command dispatch has an unsupported selector",
                ));
            }
        }
        let native_output = selector["native_output"].as_str().ok_or_else(|| {
            Error::new(
                "COMMAND_RELEASE_RESULT_INVALID",
                "Command output result has no exact stream selector",
            )
        })?;
        validate_native_output_name(native_output)?;
        let output_index = match native_output {
            "stdout.ndjson" => 0,
            "stderr.txt" => 1,
            _ => {
                return Err(Error::new(
                    "COMMAND_RELEASE_RESULT_INVALID",
                    "Command output result names an unsupported stream",
                ));
            }
        };
        let snapshot = admitted_output_snapshot(
            db,
            &result_operation_id,
            binding_id,
            generation,
            target_operation_id,
            native_output,
        )?;
        if outputs[output_index].target_command_output.is_null() {
            outputs[output_index].target_command_output = snapshot.clone();
        } else if outputs[output_index].target_command_output != snapshot {
            return Err(Error::new(
                "COMMAND_RELEASE_RESULT_CONFLICT",
                "result pages do not share one immutable Command capture snapshot",
            ));
        }
        let artifact_ref = result["details"]["artifact_ref"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_ARTIFACT_UNACKNOWLEDGED",
                    "settled Command output result has no artifact reference",
                )
            })?;
        let artifact =
            results::command_result_page_readback(db, &result_operation_id, artifact_ref)?;
        let source = &artifact.metadata["source"];
        let result_input_sha256 = model::digest(model::canonical(&original)?.as_bytes());
        let standard_source_matches = source["kind"] == "command_output"
            && source["result_operation_id"] == result_operation_id
            && source["result_input_sha256"] == result_input_sha256
            && source["input_operation_id"] == target_operation_id
            && source["target_input_sha256"] == target_input_sha256
            && source["target_command_output"] == snapshot;
        let normalized_source_matches = source["schema_id"]
            == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
            && source["result_operation_id"] == result_operation_id
            && source["result_input_sha256"] == result_input_sha256
            && source["origin"]["binding_id"] == binding_id
            && source["origin"]["binding_generation"] == generation
            && source["origin"]["target_operation_id"] == target_operation_id
            && source["origin"]["target_input_sha256"] == target_input_sha256
            && source["payload_sha256"] == snapshot["stored_sha256"]
            && source["payload_bytes"] == snapshot["stored_bytes"];
        let details = &result["details"];
        let offset_bytes = artifact.metadata["offset_bytes"].as_u64();
        let byte_length = artifact.metadata["byte_length"].as_u64();
        let total_bytes = artifact.metadata["total_bytes"].as_u64();
        let page_sha256 = artifact.metadata["page_sha256"].as_str();
        let eof = artifact.metadata["eof"].as_bool();
        if artifact.metadata["selector"] != selector.clone()
            || artifact.metadata["operation_id"] != result_operation_id
            || artifact.metadata["target_command_output"]
                .as_object()
                .is_some_and(|saved| Value::Object(saved.clone()) != snapshot)
            || (!standard_source_matches && !normalized_source_matches)
            || offset_bytes != details["offset_bytes"].as_u64()
            || byte_length != Some(artifact.byte_length)
            || total_bytes != details["total_bytes"].as_u64()
            || eof != details["eof"].as_bool()
            || page_sha256 != Some(artifact.content_digest.as_str())
            || details["page_sha256"] != artifact.content_digest
        {
            return Err(Error::new(
                "COMMAND_RELEASE_ARTIFACT_INVALID",
                "Command output artifact metadata differs from its exact result Operation",
            ));
        }
        let artifact_page = CommandCaptureArtifactPage {
            result_operation_id,
            result_input_sha256,
            artifact_ref: artifact_ref.to_owned(),
            artifact_sha256: artifact.content_digest.clone(),
            artifact_byte_length: artifact.byte_length,
            offset_bytes: offset_bytes.unwrap_or(u64::MAX),
            byte_length: byte_length.unwrap_or(u64::MAX),
            total_bytes: total_bytes.unwrap_or(u64::MAX),
            page_sha256: page_sha256.unwrap_or_default().to_owned(),
            eof: eof.unwrap_or(false),
        };
        outputs[output_index].pages.push(artifact_page);
        artifacts.push(artifact);
    }
    for output in &mut outputs {
        output.pages.sort_by_key(|page| page.offset_bytes);
        let mut unique_pages: Vec<CommandCaptureArtifactPage> = Vec::new();
        for page in output.pages.drain(..) {
            if let Some(previous) = unique_pages.last() {
                if previous.offset_bytes == page.offset_bytes {
                    if previous.total_bytes == page.total_bytes
                        && previous.byte_length == page.byte_length
                        && previous.page_sha256 == page.page_sha256
                        && previous.artifact_sha256 == page.artifact_sha256
                        && previous.eof == page.eof
                    {
                        // Re-reading an already persisted exact range creates
                        // another immutable artifact, not new stream coverage.
                        continue;
                    }
                    return Err(Error::new(
                        "COMMAND_RELEASE_COVERAGE_INVALID",
                        "overlapping Command result pages disagree on their exact range",
                    ));
                }
                let previous_end = previous
                    .offset_bytes
                    .checked_add(previous.byte_length)
                    .ok_or_else(|| {
                        Error::new("COMMAND_RELEASE_COVERAGE_INVALID", "page range overflow")
                    })?;
                if page.offset_bytes < previous_end {
                    return Err(Error::new(
                        "COMMAND_RELEASE_COVERAGE_INVALID",
                        "overlapping Command result pages do not identify one exact range",
                    ));
                }
            }
            unique_pages.push(page);
        }
        output.pages = unique_pages;
        let total = output.target_command_output["stored_bytes"]
            .as_u64()
            .ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_RESULT_MISSING",
                    "capture snapshot is missing",
                )
            })?;
        if output.target_command_output["operation_id"] != target_operation_id
            || output.target_command_output["input_sha256"] != target_input_sha256
            || output.target_command_output["native_output"] != output.native_output
            || output.pages.is_empty()
        {
            return Err(Error::new(
                "COMMAND_RELEASE_RESULT_MISSING",
                "both exact Command output streams require persisted result pages",
            ));
        }
        let mut covered = 0u64;
        let mut has_gap = false;
        for (index, page) in output.pages.iter().enumerate() {
            let end = page
                .offset_bytes
                .checked_add(page.byte_length)
                .ok_or_else(|| {
                    Error::new("COMMAND_RELEASE_COVERAGE_INVALID", "page range overflow")
                })?;
            if end > total
                || page.total_bytes != total
                || page.artifact_byte_length != page.byte_length
                || page.artifact_sha256 != page.page_sha256
                || page.eof != (end == total)
                || (page.byte_length == 0 && !(total == 0 && output.pages.len() == 1 && index == 0))
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "immutable Command result artifacts do not form exact output coverage",
                ));
            }
            has_gap |= page.offset_bytes != covered;
            covered = end;
        }
        if has_gap || covered != total {
            return Err(Error::new(
                if retained_evidence.is_some() {
                    "COMMAND_RELEASE_COVERAGE_INVALID"
                } else {
                    "COMMAND_RELEASE_RESULT_MISSING"
                },
                "immutable Command result artifacts leave captured bytes uncovered",
            ));
        }
    }
    let evidence = CommandCaptureReleaseEvidence {
        schema_version: 1,
        target_operation_id: target_operation_id.to_owned(),
        target_input_sha256,
        target_outcome_sha256,
        outputs: outputs.into(),
    };
    if retained_evidence
        .as_ref()
        .is_some_and(|retained| retained != &evidence)
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "retained release Operation differs from its exact result artifact readback",
        ));
    }
    Ok((evidence, artifacts))
}

/// Validate the adapter's release ACK against the same immutable coverage that
/// authorized the release. Its typed module receipt is checked by the caller.
pub(super) fn validate_capture_release_outcome(
    db: &Connection,
    binding: &Value,
    operation: &Value,
    outcome: &RuntimeOutcome,
) -> Result<()> {
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    if !crate::runtime::batch::is_rust_command_route(&binding["route"])
        || binding["module_artifact_id"] != crate::runtime::batch::COMMAND_RUST_ARTIFACT_ID
        || operation["method"] != swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD
        || operation["binding_id"] != binding_id
        || operation["binding_generation"] != generation
        || operation["operation_id"] != outcome.operation_id
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "release ACK is outside its exact Rust Command operation",
        ));
    }
    let original_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [&outcome.operation_id],
        |row| row.get(0),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let target_id = model::text(&original, "target_operation_id")?;
    let target = operations::get_operation(db, target_id)?;
    let (evidence, _) = command_capture_release_readback(db, binding_id, generation, target_id)?;
    if original["client_request_id"] != capture_release_client_request_id(target_id)
        || operation["caller_id"] != target["caller_id"]
        || operation["attempt_id"] != target["attempt_id"]
        || original[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD]
            != serde_json::to_value(&evidence)?
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "release ACK does not bind its retained Store artifact authority",
        ));
    }
    let artifact_refs: Vec<&str> = evidence
        .outputs
        .iter()
        .flat_map(|output| output.pages.iter().map(|page| page.artifact_ref.as_str()))
        .collect();
    let expected = json!({
        "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
        "completion_condition":"command_capture_release_recorded",
        "release_input_sha256":model::digest(model::canonical(&original)?.as_bytes()),
        "target_operation_id":target_id,
        "target_input_sha256":evidence.target_input_sha256,
        "target_outcome_sha256":evidence.target_outcome_sha256,
        "coverage_sha256":model::digest(model::canonical(&serde_json::to_value(&evidence)?)?.as_bytes()),
        "artifact_refs":artifact_refs,
        "native_capture_released":true,
        "native_replay":false
    });
    let mut details = outcome.details.clone();
    if let Some(object) = details.as_object_mut() {
        object.remove("module_receipt");
    }
    if !matches!(outcome.outcome, EffectOutcome::Applied)
        || outcome.native_scope_key.is_some()
        || outcome.native_root_id.is_some()
        || outcome.turn_id.is_some()
        || outcome.native_input_id.is_some()
        || details != expected
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "release ACK differs from its exact retained proof or claims native execution",
        ));
    }
    Ok(())
}

fn retained_command_release_evidence(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
    target: &Value,
    target_input_sha256: &str,
    target_outcome_sha256: &str,
) -> Result<Option<CommandCaptureReleaseEvidence>> {
    let caller_id = model::text(target, "caller_id")?;
    let client_request_id = capture_release_client_request_id(target_operation_id);
    let retained: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,original_request_json FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2 AND method=?3",
            params![
                caller_id,
                client_request_id,
                swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((release_operation_id, original_raw)) = retained else {
        return Ok(None);
    };
    let release_operation = operations::get_operation(db, &release_operation_id)?;
    let original: Value = serde_json::from_str(&original_raw)?;
    model::fields(
        &original,
        &[
            "client_request_id",
            "binding_id",
            "generation",
            "target_operation_id",
            "attempt_id",
            swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD,
        ],
    )?;
    let expected_attempt = target["attempt_id"].as_str();
    if release_operation["method"]
        != swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD
        || release_operation["binding_id"] != binding_id
        || release_operation["binding_generation"] != generation
        || release_operation["attempt_id"].as_str() != expected_attempt
        || original["client_request_id"] != client_request_id
        || original["binding_id"] != binding_id
        || original["generation"] != generation
        || original["target_operation_id"] != target_operation_id
        || original.get("attempt_id").and_then(Value::as_str) != expected_attempt
        || (expected_attempt.is_none() && original.get("attempt_id").is_some())
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "retained release Operation does not match its exact Command dispatch authority",
        ));
    }
    let evidence: CommandCaptureReleaseEvidence = serde_json::from_value(
        original[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD].clone(),
    )
    .map_err(|_| {
        Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "retained release Operation has malformed artifact coverage evidence",
        )
    })?;
    if evidence.schema_version != 1
        || evidence.target_operation_id != target_operation_id
        || evidence.target_input_sha256 != target_input_sha256
        || evidence.target_outcome_sha256 != target_outcome_sha256
        || evidence.outputs.len() != 2
        || evidence.outputs[0].native_output != "stdout.ndjson"
        || evidence.outputs[1].native_output != "stderr.txt"
        || evidence
            .outputs
            .iter()
            .any(|output| output.pages.is_empty())
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "retained release proof does not bind both exact Command output streams",
        ));
    }
    let mut page_operations: Vec<String> = Vec::new();
    for page in evidence.outputs.iter().flat_map(|output| &output.pages) {
        if page_operations.contains(&page.result_operation_id) {
            return Err(Error::new(
                "COMMAND_RELEASE_EVIDENCE_INVALID",
                "retained release proof reuses a result Operation across output pages",
            ));
        }
        page_operations.push(page.result_operation_id.clone());
    }
    Ok(Some(evidence))
}

#[derive(Clone)]
struct CaptureReleaseProjection {
    principal: Principal,
    caller_id: String,
    target_operation_id: String,
    binding_id: String,
    generation: i64,
    attempt_id: Option<String>,
    route: Value,
    evidence: CommandCaptureReleaseEvidence,
    artifacts: Vec<ArtifactRecord>,
}

/// Admit the one Store-created release Operation after immutable Command
/// output pages cover both saved streams. The caller supplies only the exact
/// target Operation ID; evidence and binding authority are read from Store.
impl super::Store {
    /// Manager's post-`module.result` hook. It resolves the target only from
    /// the settled Store result Operation, then delegates to the closed
    /// target-only admission path below.
    pub(super) async fn admit_command_capture_release_for_result(
        &self,
        result_operation_id: &str,
    ) -> Result<Option<Value>> {
        if result_operation_id.is_empty() {
            return Err(Error::invalid("result Operation ID is required"));
        }
        let result_operation_id = result_operation_id.to_owned();
        let target_operation_id = self
            .run(move |db| capture_release_target_for_result_operation(db, &result_operation_id))
            .await?;
        let Some(target_operation_id) = target_operation_id else {
            return Ok(None);
        };
        self.admit_command_capture_release_for_target(&target_operation_id)
            .await
    }

    pub(super) async fn admit_command_capture_release_for_target(
        &self,
        target_operation_id: &str,
    ) -> Result<Option<Value>> {
        if target_operation_id.is_empty() {
            return Err(Error::invalid("Command target Operation ID is required"));
        }
        let target_operation_id = target_operation_id.to_owned();
        let first_target = target_operation_id.clone();
        let config = self.config.clone();
        let first_config = config.clone();
        let first = match self
            .run(move |db| capture_release_projection(db, &first_config, &first_target))
            .await
        {
            Ok(projection) => projection,
            Err(error) if capture_release_is_pending(&error.code) => return Ok(None),
            Err(error) => return Err(error),
        };

        let evidence = first.evidence.clone();
        let artifacts = first.artifacts.clone();
        self.file_io(move |files| verify_capture_release_artifacts(&files, &evidence, &artifacts))
            .await?;

        let expected = first.clone();
        let target_for_recheck = target_operation_id.clone();
        let recheck_config = config.clone();
        let admission = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let current =
                    capture_release_projection(&tx, &recheck_config, &target_for_recheck)?;
                if !same_capture_release_projection(&expected, &current) {
                    return Err(Error::conflict(
                        "Command result coverage changed while release artifacts were verified",
                    ));
                }
                let request = capture_release_operation_input(&current)?;
                let admission = super::mutate_in_transaction_with_check_plan(
                    &tx,
                    &current.principal,
                    swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD,
                    &request,
                    &config,
                    model::now_ms()?,
                    None,
                )?;
                tx.commit()?;
                admission
            })
            .await;
        let admission = match admission {
            Err(error) if capture_release_is_pending(&error.code) => return Ok(None),
            result => result,
        };
        if let Ok(receipt) = &admission
            && receipt
                .get("operation_id")
                .and_then(Value::as_str)
                .is_some()
        {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        admission.map(Some)
    }
}

fn capture_release_target_for_result_operation(
    db: &Connection,
    result_operation_id: &str,
) -> Result<Option<String>> {
    let result = operations::get_operation(db, result_operation_id)?;
    if result["method"] != "agent.result"
        || result["state"] != "settled"
        || result["result"]["outcome"] != "applied"
        || result["result"]["details"]["completion_condition"] != "result_page_persisted"
    {
        return Ok(None);
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND method='agent.result'",
        [result_operation_id],
        |row| row.get(0),
    )?;
    let original: Value = serde_json::from_str(&raw)?;
    if original["selector"]["kind"] != "command_output" {
        return Ok(None);
    }
    let target_operation_id = model::text(&original["selector"], "input_operation_id")?;
    let artifact_ref = model::text(&result["result"]["details"], "artifact_ref")?;
    let artifact = results::command_result_page_readback(db, result_operation_id, artifact_ref)?;
    if artifact.metadata["selector"] != original["selector"] {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "settled result page selector differs from its retained Operation",
        ));
    }
    Ok(Some(target_operation_id.to_owned()))
}

fn capture_release_projection(
    db: &Connection,
    config: &crate::config::Config,
    target_operation_id: &str,
) -> Result<CaptureReleaseProjection> {
    let target = operations::get_operation(db, target_operation_id)?;
    let binding_id = model::text(&target, "binding_id")?.to_owned();
    let generation = model::positive(&target, "binding_generation")?;
    let binding = operations::get_binding(db, &binding_id, generation)?;
    let route_alias = model::text(&binding["route"], "alias")?;
    let configured_command_route = config.routes.iter().any(|route| {
        route.alias == route_alias
            && route.runtime == "command"
            && route.module_artifact_id == COMMAND_ARTIFACT_ID
    });
    if target["method"] != "task.dispatch"
        || !crate::runtime::batch::is_rust_command_route(&binding["route"])
        || binding["module_artifact_id"] != COMMAND_ARTIFACT_ID
        || !configured_command_route
        || !binding["released_at_ms"].is_null()
        || binding["state"] != "ready"
    {
        return Err(Error::new(
            "COMMAND_RELEASE_TARGET_INVALID",
            "capture release requires the current exact Rust Command binding and dispatch",
        ));
    }
    let (caller_id, original_raw): (String, String) = db.query_row(
        "SELECT caller_id,original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='task.dispatch'",
        params![target_operation_id, binding_id, generation],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    super::module_handshake::require_selected_native_command(
        db,
        &binding_id,
        (
            model::text(&binding["route"], "runtime")?,
            model::text(&binding, "module_artifact_id")?,
        ),
        binding["observation"].get("module_contract_selector"),
        "task.dispatch",
        &original,
    )?;
    let principal = operation_caller_principal(db, &caller_id)?;
    let (evidence, artifacts) =
        command_capture_release_readback(db, &binding_id, generation, target_operation_id)?;
    Ok(CaptureReleaseProjection {
        principal,
        caller_id,
        target_operation_id: target_operation_id.to_owned(),
        binding_id,
        generation,
        attempt_id: target["attempt_id"].as_str().map(str::to_owned),
        route: binding["route"].clone(),
        evidence,
        artifacts,
    })
}

fn operation_caller_principal(db: &Connection, caller_id: &str) -> Result<Principal> {
    let registration = meta(db, &format!("client:{caller_id}"))?.ok_or_else(|| {
        Error::new(
            "UNAUTHORIZED",
            "original Command caller is no longer registered",
        )
    })?;
    let role: Role = serde_json::from_value(registration["role"].clone())?;
    super::current_principal(
        db,
        Principal {
            link_id: String::new(),
            client_id: caller_id.to_owned(),
            role,
        },
    )
}

fn capture_release_operation_input(projection: &CaptureReleaseProjection) -> Result<Value> {
    let mut request = json!({
        "client_request_id":capture_release_client_request_id(&projection.target_operation_id),
        "binding_id":projection.binding_id,
        "generation":projection.generation,
        "target_operation_id":projection.target_operation_id,
    });
    request[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD] =
        serde_json::to_value(&projection.evidence)?;
    if let Some(attempt_id) = &projection.attempt_id {
        request["attempt_id"] = json!(attempt_id);
    }
    if request["target_operation_id"]
        != request[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD]["target_operation_id"]
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "Store release request target differs from its derived artifact proof",
        ));
    }
    Ok(request)
}

fn capture_release_client_request_id(target_operation_id: &str) -> String {
    format!(
        "command-capture-release-{}",
        model::digest(target_operation_id.as_bytes())
    )
}

fn capture_release_is_pending(code: &str) -> bool {
    matches!(
        code,
        "COMMAND_RELEASE_RESULT_UNRESOLVED"
            | "COMMAND_RELEASE_RESULT_MISSING"
            | "COMMAND_RELEASE_ARTIFACT_UNACKNOWLEDGED"
    )
}

fn same_capture_release_projection(
    first: &CaptureReleaseProjection,
    second: &CaptureReleaseProjection,
) -> bool {
    first.caller_id == second.caller_id
        && first.target_operation_id == second.target_operation_id
        && first.binding_id == second.binding_id
        && first.generation == second.generation
        && first.attempt_id == second.attempt_id
        && first.route == second.route
        && first.evidence == second.evidence
        && same_artifact_records(&first.artifacts, &second.artifacts)
}

fn same_artifact_records(first: &[ArtifactRecord], second: &[ArtifactRecord]) -> bool {
    first.len() == second.len()
        && first.iter().zip(second).all(|(left, right)| {
            left.kind == right.kind
                && left.artifact_id == right.artifact_id
                && left.relative_path == right.relative_path
                && left.byte_length == right.byte_length
                && left.content_digest == right.content_digest
                && left.metadata == right.metadata
        })
}

fn verify_capture_release_artifacts(
    files: &ArtifactFiles,
    evidence: &CommandCaptureReleaseEvidence,
    artifacts: &[ArtifactRecord],
) -> Result<()> {
    for output in &evidence.outputs {
        let stored_bytes = output.target_command_output["stored_bytes"]
            .as_u64()
            .ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "stored byte count is absent",
                )
            })?;
        let mut covered = 0u64;
        let mut stream = Sha256::new();
        for page in &output.pages {
            let record = artifacts
                .iter()
                .find(|record| record.artifact_id == page.artifact_ref)
                .ok_or_else(|| {
                    Error::new(
                        "COMMAND_RELEASE_ARTIFACT_INVALID",
                        "Store artifact projection omitted a referenced page",
                    )
                })?;
            if record.kind != "native_result_page"
                || record.byte_length != page.artifact_byte_length
                || record.content_digest != page.artifact_sha256
                || page.offset_bytes != covered
                || page.total_bytes != stored_bytes
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "Store artifact record differs from its ordered release coverage entry",
                ));
            }
            let bytes = read_verified_result_page(files, record)?;
            let end = page
                .offset_bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| {
                    Error::new("COMMAND_RELEASE_COVERAGE_INVALID", "page range overflow")
                })?;
            if bytes.len() as u64 != page.byte_length
                || model::digest(&bytes) != page.page_sha256
                || page.artifact_sha256 != page.page_sha256
                || page.eof != (end == stored_bytes)
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "verified result page bytes differ from their exact digest and range",
                ));
            }
            stream.update(&bytes);
            covered = end;
        }
        let stored_sha256 = format!("{:x}", stream.finalize());
        if covered != stored_bytes
            || output.target_command_output["stored_sha256"].as_str()
                != Some(stored_sha256.as_str())
        {
            return Err(Error::new(
                "COMMAND_RELEASE_ARTIFACT_INVALID",
                "ordered Store artifact bytes do not reproduce the whole retained capture digest",
            ));
        }
        let complete = output.target_command_output["truncated"] == false
            && output.target_command_output["read_error"] == false
            && output.target_command_output["stream_bytes"] == json!(stored_bytes);
        if complete && output.target_command_output["stream_sha256"] != stored_sha256 {
            return Err(Error::new(
                "COMMAND_RELEASE_ARTIFACT_INVALID",
                "complete Command stream digest differs from its persisted artifact bytes",
            ));
        }
    }
    Ok(())
}

fn read_verified_result_page(files: &ArtifactFiles, record: &ArtifactRecord) -> Result<Vec<u8>> {
    if record.byte_length > MAX_PAGE_BYTES as u64 {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "persisted result page exceeds the Store artifact read bound",
        ));
    }
    if !files.verify_existing(record)? {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_MISSING",
            "acknowledged Command result artifact file is absent",
        ));
    }
    if record.byte_length == 0 {
        return Ok(Vec::new());
    }
    let length = usize::try_from(record.byte_length).map_err(|_| {
        Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "persisted result page length exceeds the address space",
        )
    })?;
    let readback = files.read(record, 0, length)?;
    if readback["artifact_id"] != record.artifact_id
        || readback["offset_bytes"] != 0
        || readback["byte_length"].as_u64() != Some(record.byte_length)
        || readback["eof"] != true
        || readback["artifact_sha256"] != record.content_digest
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "Store artifact reader returned a different page identity or range",
        ));
    }
    let encoded = readback["content"].as_str().ok_or_else(|| {
        Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "Store artifact reader omitted the verified page body",
        )
    })?;
    let bytes = match readback["encoding"].as_str() {
        Some("utf8") => encoded.as_bytes().to_vec(),
        Some("base64") => {
            let bytes = STANDARD.decode(encoded).map_err(|_| {
                Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "Store artifact reader returned invalid base64 page bytes",
                )
            })?;
            if STANDARD.encode(&bytes) != encoded {
                return Err(Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "Store artifact reader returned noncanonical base64 page bytes",
                ));
            }
            bytes
        }
        _ => {
            return Err(Error::new(
                "COMMAND_RELEASE_ARTIFACT_INVALID",
                "Store artifact reader returned an unknown page encoding",
            ));
        }
    };
    if bytes.len() as u64 != record.byte_length || model::digest(&bytes) != record.content_digest {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "Store artifact reader body differs from its immutable record digest",
        ));
    }
    Ok(bytes)
}

fn validate_frozen_snapshot(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
    snapshot: &Value,
) -> Result<()> {
    model::fields(
        snapshot,
        &[
            "schema_version",
            "operation_id",
            "method",
            "operation_state",
            "operation_outcome",
            "completion_condition",
            "diagnostic_code",
            "result_subtype",
            "exit_code",
            "timed_out",
            "signal_observed",
            "input_sha256",
            "module_receipt",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    if snapshot["operation_id"] != target_operation_id
        || snapshot["schema_version"] != 1
        || snapshot["method"] != "task.dispatch"
        || !matches!(
            (
                snapshot["operation_state"].as_str(),
                snapshot["operation_outcome"].as_str()
            ),
            (Some("settled"), Some("applied"))
                | (Some("rejected"), Some("rejected"))
                | (Some("outcome_unknown"), Some("unknown"))
        )
        || snapshot["native_response_identity"] != "unavailable"
        || snapshot["execution_complete"] != false
        || snapshot["task_completion"] != "unknown"
        || snapshot["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command status snapshot has an invalid identity or authority claim",
        ));
    }
    let raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='task.dispatch'",
            params![target_operation_id, binding_id, generation],
            |row| row.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| {
        Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command status target is absent from this binding generation",
        )
    })?;
    let request: Value = serde_json::from_str(&raw)?;
    let expected = model::digest(model::canonical(&request)?.as_bytes());
    if snapshot["input_sha256"].as_str() != Some(expected.as_str()) {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command status snapshot no longer matches the exact target request",
        ));
    }
    if snapshot["module_receipt"].is_null() {
        if snapshot["operation_state"] != "outcome_unknown"
            || snapshot["operation_outcome"] != "unknown"
        {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "only an unknown Command status may omit its module receipt",
            ));
        }
    } else {
        let binding = operations::get_binding(db, binding_id, generation)?;
        let outcome = RuntimeOutcome {
            operation_id: target_operation_id.to_owned(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({"module_receipt":snapshot["module_receipt"]}),
        };
        let receipt = runtime::validate_module_receipt_for_operation(
            db, binding_id, generation, &binding, &outcome,
        )?;
        if receipt.input_sha256 != expected {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "sealed Command module receipt differs from its exact target request",
            ));
        }
    }
    Ok(())
}

fn validate_frozen_output_snapshot(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
    native_output: &str,
    snapshot: &Value,
) -> Result<()> {
    model::fields(
        snapshot,
        &[
            "schema_version",
            "operation_id",
            "method",
            "operation_state",
            "operation_outcome",
            "completion_condition",
            "input_sha256",
            "module_receipt",
            "task_dispatch_context",
            "dispatch_admission",
            "native_output",
            "stream_bytes",
            "stored_bytes",
            "stream_sha256",
            "stored_sha256",
            "truncated",
            "read_error",
            "native_child",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    if snapshot["schema_version"] != 1
        || snapshot["operation_id"] != target_operation_id
        || snapshot["method"] != "task.dispatch"
        || snapshot["native_output"] != native_output
        || !matches!(
            (
                snapshot["operation_state"].as_str(),
                snapshot["operation_outcome"].as_str()
            ),
            (Some("settled"), Some("applied"))
                | (Some("rejected"), Some("rejected"))
                | (Some("outcome_unknown"), Some("unknown"))
        )
        || snapshot["native_response_identity"] != "unavailable"
        || snapshot["execution_complete"] != false
        || snapshot["task_completion"] != "unknown"
        || snapshot["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command output snapshot has an invalid identity or authority claim",
        ));
    }
    let raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND method='task.dispatch'",
            params![target_operation_id, binding_id, generation],
            |row| row.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| {
        Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Command output target is absent from this binding generation",
        )
    })?;
    let request: Value = serde_json::from_str(&raw)?;
    let expected = model::digest(model::canonical(&request)?.as_bytes());
    if snapshot["input_sha256"].as_str() != Some(expected.as_str()) {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command output snapshot differs from the exact target request",
        ));
    }
    let binding = operations::get_binding(db, binding_id, generation)?;
    let module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(snapshot["module_receipt"].clone()).map_err(|_| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "sealed Command output module receipt is malformed",
            )
        })?;
    module_receipt.validate().map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command output module receipt is invalid",
        )
    })?;
    let dispatch_admission = if snapshot["dispatch_admission"].is_null() {
        None
    } else {
        Some(
            serde_json::from_value::<TaskDispatchAdmissionReceipt>(
                snapshot["dispatch_admission"].clone(),
            )
            .map_err(|_| {
                Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "sealed Command dispatch admission receipt is malformed",
                )
            })?,
        )
    };
    let normalized_dispatch = normalized_dispatch_enabled(db, &binding)?;
    let boot_id = if snapshot["operation_outcome"] == "applied" {
        dispatch_admission
            .as_ref()
            .map(|receipt| receipt.worker_boot_id.as_str())
    } else {
        None
    };
    let dispatch_context = dispatch_context_projection(
        db,
        normalized_dispatch,
        binding_id,
        generation,
        target_operation_id,
        &request,
        boot_id,
    )?;
    if snapshot["task_dispatch_context"] != dispatch_context {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command output context differs from the exact dispatch request",
        ));
    }
    match (normalized_dispatch, dispatch_admission.as_ref()) {
        (true, Some(receipt)) => {
            receipt.validate().map_err(|_| {
                Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "sealed Command dispatch admission receipt is invalid",
                )
            })?;
            if snapshot["operation_outcome"] != "applied"
                || !dispatch_context_matches(&dispatch_context, &receipt.context())
                || receipt.module_receipt != module_receipt
                || receipt.operation_id != target_operation_id
                || receipt.binding_id != binding_id
                || receipt.binding_generation != generation
            {
                return Err(Error::new(
                    "RESULT_PROVENANCE_INVALID",
                    "sealed Command dispatch admission differs from its exact target receipt",
                ));
            }
        }
        (true, None) if snapshot["operation_outcome"] == "applied" => {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "known normalized dispatch is missing its admission receipt",
            ));
        }
        (false, Some(_)) => {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "legacy dispatch cannot carry normalized admission evidence",
            ));
        }
        _ => {}
    }
    let stream_bytes = snapshot["stream_bytes"].as_u64().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed stream size is malformed",
        )
    })?;
    let stored_bytes = snapshot["stored_bytes"].as_u64().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed stored size is malformed",
        )
    })?;
    let stream_sha256 = model::text(snapshot, "stream_sha256")?;
    let stored_sha256 = model::text(snapshot, "stored_sha256")?;
    let truncated = snapshot["truncated"].as_bool().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed truncation fact is malformed",
        )
    })?;
    let read_error = snapshot["read_error"].as_bool().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed read status is malformed",
        )
    })?;
    validate_capture_values(
        native_output,
        stream_bytes,
        stored_bytes,
        stream_sha256,
        stored_sha256,
        truncated,
        read_error,
    )?;
    validate_native_child(&snapshot["native_child"])?;
    let outcome = RuntimeOutcome {
        operation_id: target_operation_id.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({"module_receipt":snapshot["module_receipt"]}),
    };
    let receipt = runtime::validate_module_receipt_for_operation(
        db, binding_id, generation, &binding, &outcome,
    )?;
    if receipt.input_sha256 != expected {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed Command output receipt differs from its exact target request",
        ));
    }
    Ok(())
}

fn safe_diagnostic(value: &Value) -> Value {
    value
        .as_str()
        .filter(|code| {
            !code.is_empty()
                && code.len() <= 64
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
        .map(|code| json!(code))
        .unwrap_or(Value::Null)
}

pub(super) fn target_evidence(snapshot: &Value) -> &'static str {
    if snapshot["operation_state"] == "outcome_unknown" {
        if snapshot["module_receipt"].is_null() {
            "store_retained_operation_state_only"
        } else {
            "store_retained_module_reported_unknown_outcome"
        }
    } else {
        "store_retained_module_reported_operation_receipt"
    }
}

fn terminal_facts(details: &Value, outcome: &str) -> Result<(Value, Value, Value, bool, bool)> {
    if details["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
        || details["task_acceptance_claimed"] != false
        || details["result_page_available"] != false
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command terminal facts exceed the saved one-shot result contract",
        ));
    }
    let completion = model::text(details, "completion_condition")?;
    let diagnostic = details
        .get("diagnostic_code")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or(Value::Null);
    if !diagnostic.is_null() && !safe_code(diagnostic.as_str().unwrap_or_default()) {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command diagnostic code is outside the bounded closed form",
        ));
    }
    match completion {
        "executor_launch_rejected" if outcome == "rejected" => {
            if details["native_session_state"] != "not_started"
                || details["timed_out"] != false
                || !details["signal"].is_null()
                || details["native_result"].is_object()
            {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command pre-spawn rejection contains inconsistent native facts",
                ));
            }
            Ok((diagnostic, Value::Null, Value::Null, false, false))
        }
        "native_result_observed" => {
            let subtype = model::text(details, "result_subtype")?;
            let exit_code = details["exit_code"].as_i64().ok_or_else(|| {
                Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command terminal result has no exit code",
                )
            })?;
            let result_object = &details["native_result"];
            let transport_complete = details["timed_out"] == false
                && details["spawn_error_observed"] == false
                && details["stream_drain_timed_out"] == false
                && details["stdout_truncated"] == false
                && details["stderr_truncated"] == false
                && details["native_protocol_gap_count"] == 0
                && details["native_frames_after_result"] == 0
                && details["signal"].is_null()
                && result_object["status"] == subtype;
            let terminal_pair = match (outcome, subtype) {
                ("applied", "success") => exit_code == 0,
                ("rejected", "error") => matches!(exit_code, 1 | 3 | 4 | 5 | 6 | 7 | 9 | 10 | 130),
                ("rejected", "max_turns") => exit_code == 8,
                _ => false,
            };
            if !transport_complete || !terminal_pair {
                return Err(Error::new(
                    "RESULT_TARGET_RECEIPT_INVALID",
                    "Command result status and complete native terminal facts disagree",
                ));
            }
            Ok((Value::Null, json!(subtype), json!(exit_code), false, false))
        }
        _ => Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command status requires an observed terminal result or pre-spawn rejection",
        )),
    }
}

fn require_result_capability(db: &Connection, binding: &Value) -> Result<()> {
    if !crate::runtime::batch::is_rust_command_route(&binding["route"]) {
        return Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "Command result pages require the standalone Command route",
        ));
    }
    let artifact_id = model::text(binding, "module_artifact_id")?;
    let selector = binding["observation"]
        .get("module_contract_selector")
        .ok_or_else(|| {
            Error::new(
                "CAPABILITY_UNAVAILABLE",
                "Command result pages require a registered v3 module descriptor",
            )
        })?;
    let retained =
        super::module_handshake::retained_contract_identity(db, artifact_id, Some(selector))?
            .ok_or_else(|| {
                Error::new(
                    "CAPABILITY_UNAVAILABLE",
                    "Command result pages require a registered v3 module descriptor",
                )
            })?;
    let declares_result = retained
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == RESULT_CAPABILITY);
    if artifact_id != COMMAND_ARTIFACT_ID
        || retained.module_id.as_str() != COMMAND_MODULE_ID
        || retained.artifact.artifact_id.as_str() != COMMAND_ARTIFACT_ID
        || retained.artifact.version.as_str() != COMMAND_ARTIFACT_VERSION
        || !declares_result
    {
        return Err(Error::new(
            "CAPABILITY_UNAVAILABLE",
            "the selected Command descriptor does not declare v3 status pages",
        ));
    }
    Ok(())
}

/// Validate and augment the common result context. The shared result writer
/// still owns artifact publication and immutable Operation settlement.
pub(super) fn prepare(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
    source: &Value,
) -> Result<Value> {
    let mut context = results::prepare(db, principal, operation_id, source)?;
    let kind = model::text(&context["selector"], "kind")?;
    if kind == "command_status" {
        let target_id = model::text(&context["selector"], "input_operation_id")?.to_owned();
        let snapshot = target_snapshot_from_context(db, &context, &target_id)?;
        let result_input_sha256 = request_digest(
            db,
            operation_id,
            model::text(&context, "binding_id")?,
            model::positive(&context, "generation")?,
        )?;
        validate_source(
            db,
            model::text(&context, "binding_id")?,
            model::positive(&context, "generation")?,
            &context,
            source,
            &snapshot,
            &result_input_sha256,
        )?;
        context["result_input_sha256"] = json!(result_input_sha256);
        context["target_operation_id"] = json!(target_id);
        context["target_input_sha256"] = snapshot["input_sha256"].clone();
        context["target_operation_status"] = snapshot;
        return Ok(context);
    }
    if kind == "command_output" {
        model::fields(
            &context["selector"],
            &["kind", "input_operation_id", "native_output"],
        )?;
        let target_id = model::text(&context["selector"], "input_operation_id")?.to_owned();
        let native_output = model::text(&context["selector"], "native_output")?;
        validate_native_output_name(native_output)?;
        let snapshot = admitted_output_snapshot(
            db,
            operation_id,
            model::text(&context, "binding_id")?,
            model::positive(&context, "generation")?,
            &target_id,
            native_output,
        )?;
        let result_input_sha256 = request_digest(
            db,
            operation_id,
            model::text(&context, "binding_id")?,
            model::positive(&context, "generation")?,
        )?;
        validate_output_source(
            db,
            model::text(&context, "binding_id")?,
            model::positive(&context, "generation")?,
            &context,
            source,
            &snapshot,
            &result_input_sha256,
        )?;
        context["result_input_sha256"] = json!(result_input_sha256);
        context["target_operation_id"] = json!(target_id);
        context["target_input_sha256"] = snapshot["input_sha256"].clone();
        context["binding_generation"] = context["generation"].clone();
        context["target_command_output"] = snapshot;
        return Ok(context);
    }
    Err(Error::new(
        "RESULT_SELECTOR_UNSUPPORTED",
        "Command result page selector is invalid",
    ))
}

fn target_snapshot_from_context(
    db: &Connection,
    context: &Value,
    target_id: &str,
) -> Result<Value> {
    let binding_id = model::text(context, "binding_id")?;
    let generation = model::positive(context, "generation")?;
    admitted_target_snapshot(
        db,
        model::text(context, "operation_id")?,
        binding_id,
        generation,
        target_id,
    )
}

fn request_digest(
    db: &Connection,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
) -> Result<String> {
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![operation_id, binding_id, generation],
        |row| row.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    Ok(model::digest(model::canonical(&request)?.as_bytes()))
}

fn validate_source(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    context: &Value,
    source: &Value,
    target_snapshot: &Value,
    result_input_sha256: &str,
) -> Result<()> {
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_input_sha256",
            "target_module_receipt",
            "target_operation_status",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    let target_id = model::text(&context["selector"], "input_operation_id")?;
    if source["kind"] != "command_status"
        || source["result_operation_id"] != context["operation_id"]
        || source["result_input_sha256"] != result_input_sha256
        || source["input_operation_id"] != target_id
        || source["target_input_sha256"] != target_snapshot["input_sha256"]
        || source["target_module_receipt"] != target_snapshot["module_receipt"]
        || source["target_operation_status"] != *target_snapshot
        || source["evidence"] != target_evidence(target_snapshot)
        || source["native_response_identity"] != "unavailable"
        || source["execution_complete"] != false
        || source["task_completion"] != "unknown"
        || source["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command status page differs from the exact retained Operation receipt",
        ));
    }

    let current_outcome = RuntimeOutcome {
        operation_id: model::text(context, "operation_id")?.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: context["native_scope_key"].as_str().map(str::to_owned),
        native_root_id: context["native_root_id"].as_str().map(str::to_owned),
        turn_id: None,
        native_input_id: None,
        details: json!({"module_receipt":source["result_module_receipt"]}),
    };
    runtime::validate_module_receipt_for_operation(
        db,
        binding_id,
        generation,
        &operations::get_binding(db, binding_id, generation)?,
        &current_outcome,
    )?;
    Ok(())
}

fn validate_output_source(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    context: &Value,
    source: &Value,
    target_snapshot: &Value,
    result_input_sha256: &str,
) -> Result<()> {
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_input_sha256",
            "target_module_receipt",
            "target_command_output",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    let target_id = model::text(&context["selector"], "input_operation_id")?;
    if source["kind"] != "command_output"
        || source["result_operation_id"] != context["operation_id"]
        || source["result_input_sha256"] != result_input_sha256
        || source["input_operation_id"] != target_id
        || source["target_input_sha256"] != target_snapshot["input_sha256"]
        || source["target_module_receipt"] != target_snapshot["module_receipt"]
        || source["target_command_output"] != *target_snapshot
        || source["native_response_identity"] != "unavailable"
        || source["execution_complete"] != false
        || source["task_completion"] != "unknown"
        || source["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command output page differs from its exact retained process receipt",
        ));
    }
    let current_outcome = RuntimeOutcome {
        operation_id: model::text(context, "operation_id")?.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({"module_receipt":source["result_module_receipt"]}),
    };
    runtime::validate_module_receipt_for_operation(
        db,
        binding_id,
        generation,
        &operations::get_binding(db, binding_id, generation)?,
        &current_outcome,
    )?;
    Ok(())
}

pub(super) fn validate_output_page(
    page: &crate::artifacts::ResultPage,
    metadata: &Value,
    bytes: &[u8],
) -> Result<()> {
    let snapshot = &metadata["target_command_output"];
    let native_output = model::text(snapshot, "native_output")?;
    let total = snapshot["stored_bytes"].as_u64().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command stored output size is malformed",
        )
    })?;
    let end = page
        .offset_bytes
        .checked_add(page.byte_length)
        .ok_or_else(|| Error::invalid("Command output page range overflow"))?;
    let media_type = if native_output == "stdout.ndjson" {
        "application/x-ndjson"
    } else {
        "text/plain; charset=utf-8"
    };
    if metadata["selector"]["kind"] != "command_output"
        || metadata["selector"]["native_output"] != native_output
        || page.total_bytes != total
        || end > total
        || page.eof != (end == total)
        || page.media_type != media_type
        || bytes.len() as u64 != page.byte_length
        || page.byte_length > MAX_PAGE_BYTES as u64
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Command output page range differs from its sealed capture",
        ));
    }
    if page.offset_bytes == 0 && end == total && model::digest(bytes) != snapshot["stored_sha256"] {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "complete Command output bytes differ from the retained stored-stream digest",
        ));
    }
    Ok(())
}

pub(super) fn page_bytes(source: &Value) -> Result<Vec<u8>> {
    let status = &source["target_operation_status"];
    let body = json!({
        "schema_version":1,
        "operation_id":status["operation_id"],
        "method":status["method"],
        "operation_state":status["operation_state"],
        "operation_outcome":status["operation_outcome"],
        "completion_condition":status["completion_condition"],
        "diagnostic_code":status["diagnostic_code"],
        "result_subtype":status["result_subtype"],
        "exit_code":status["exit_code"],
        "timed_out":status["timed_out"],
        "signal_observed":status["signal_observed"],
        "input_sha256":status["input_sha256"],
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false
    });
    Ok(serde_json::to_vec(&body)?)
}

/// Revalidate the Command-specific provenance immediately before delegating
/// to the shared immutable artifact transaction. Store runs this synchronously
/// on its single DB owner thread, so the validated context cannot interleave.
pub(super) fn record(
    db: &mut rusqlite::Connection,
    principal: &Principal,
    artifact: &ArtifactRecord,
) -> Result<Value> {
    let operation_id = model::text(&artifact.metadata, "operation_id")?;
    let context = prepare(db, principal, operation_id, &artifact.metadata["source"])?;
    let context_keys: &[&str] = if context["selector"]["kind"] == "command_output" {
        &[
            "result_input_sha256",
            "target_operation_id",
            "target_input_sha256",
            "binding_generation",
            "target_command_output",
        ]
    } else {
        &[
            "result_input_sha256",
            "target_operation_id",
            "target_input_sha256",
            "target_operation_status",
        ]
    };
    for &key in context_keys {
        if context[key] != artifact.metadata[key] {
            return Err(Error::conflict(
                "Command result context changed before result registration",
            ));
        }
    }
    results::record(db, principal, artifact)
}

fn safe_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod capture_release_artifact_fixture {
    use super::*;
    use crate::{
        config::{Config, Route},
        error::Result,
        platform::{DataRoot, bootstrap_credential},
    };
    use std::sync::Arc;

    struct StoreSeed {
        module_client_id: String,
        module_link_id: String,
        binding_id: String,
        target_operation_id: String,
        attempt_id: String,
        target_input_sha256: String,
        target_outcome_sha256: String,
        target_module_receipt: Value,
    }

    struct TempArtifacts(std::path::PathBuf);

    impl TempArtifacts {
        fn new() -> Self {
            let root = std::env::temp_dir()
                .join(format!("command-store-release-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&root).expect("create isolated artifact fixture");
            Self(
                root.canonicalize()
                    .expect("canonicalize artifact fixture root"),
            )
        }
    }

    impl Drop for TempArtifacts {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn verified_partial_store_pages_build_the_adapter_classified_envelope() {
        let temp = TempArtifacts::new();
        let files = ArtifactFiles::new(&temp.0).expect("open artifact fixture");
        let target_operation_id = "command-dispatch-partial-fixture";
        let target_input_sha256 = "a".repeat(64);
        let mut artifacts = Vec::new();
        let mut outputs = Vec::new();

        for (native_output, bytes, suffix) in [
            (
                "stdout.ndjson",
                b"partial stdout retained".as_slice(),
                "stdout",
            ),
            (
                "stderr.txt",
                b"partial stderr retained".as_slice(),
                "stderr",
            ),
        ] {
            let unretained = [bytes, b" late bytes".as_slice()].concat();
            let snapshot = json!({
                "schema_version":1,
                "operation_id":target_operation_id,
                "method":"task.dispatch",
                "operation_state":"outcome_unknown",
                "operation_outcome":"unknown",
                "completion_condition":"native_result_unconfirmed",
                "input_sha256":target_input_sha256,
                "native_output":native_output,
                "stream_bytes":unretained.len() as u64,
                "stored_bytes":bytes.len() as u64,
                "stream_sha256":model::digest(&unretained),
                "stored_sha256":model::digest(bytes),
                "truncated":true,
                "read_error":true,
                "native_response_identity":"unavailable",
                "execution_complete":false,
                "task_completion":"unknown",
                "native_replay":false
            });
            let split = bytes.len() / 2;
            let mut pages = Vec::new();
            let mut offset = 0u64;
            for (index, chunk) in [&bytes[..split], &bytes[split..]].into_iter().enumerate() {
                let result_operation_id = format!("result-{suffix}-{index}");
                let record = ArtifactFiles::record(
                    &result_operation_id,
                    chunk,
                    json!({"fixture":"Store command result page"}),
                );
                files
                    .publish(&record, chunk)
                    .expect("persist immutable result page");
                let end = offset + chunk.len() as u64;
                pages.push(CommandCaptureArtifactPage {
                    result_operation_id,
                    result_input_sha256: "b".repeat(64),
                    artifact_ref: record.artifact_id.clone(),
                    artifact_sha256: record.content_digest.clone(),
                    artifact_byte_length: record.byte_length,
                    offset_bytes: offset,
                    byte_length: chunk.len() as u64,
                    total_bytes: bytes.len() as u64,
                    page_sha256: model::digest(chunk),
                    eof: end == bytes.len() as u64,
                });
                offset = end;
                artifacts.push(record);
            }
            outputs.push(CommandCaptureOutputCoverage {
                native_output: native_output.to_owned(),
                target_command_output: snapshot,
                pages,
            });
        }

        let evidence = CommandCaptureReleaseEvidence {
            schema_version: 1,
            target_operation_id: target_operation_id.to_owned(),
            target_input_sha256,
            target_outcome_sha256: "c".repeat(64),
            outputs,
        };
        verify_capture_release_artifacts(&files, &evidence, &artifacts)
            .expect("Store verifies page and whole retained-stream digests");

        let projection = CaptureReleaseProjection {
            principal: Principal {
                link_id: String::new(),
                client_id: "fixture-caller".to_owned(),
                role: Role::Manager,
            },
            caller_id: "fixture-caller".to_owned(),
            target_operation_id: target_operation_id.to_owned(),
            binding_id: "fixture-binding".to_owned(),
            generation: 1,
            attempt_id: Some("fixture-attempt".to_owned()),
            route: json!({"runtime":"command","module_artifact_id":COMMAND_ARTIFACT_ID}),
            evidence,
            artifacts,
        };
        let input = capture_release_operation_input(&projection)
            .expect("build server-owned release Operation envelope");
        assert_eq!(
            swarm_contracts::module_command::classify_runtime_command(
                swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD,
                &input,
            ),
            Ok(swarm_contracts::module_command::RuntimeCommandKind::CommandReleaseEvidence)
        );
        assert_eq!(
            input["target_operation_id"],
            input[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD]["target_operation_id"]
        );
    }

    #[tokio::test]
    async fn store_consumer_releases_capture_and_serves_verified_output_pages() -> Result<()> {
        let temp = TempArtifacts::new();
        let root = DataRoot::acquire(&temp.0).expect("acquire fixture Store root");
        let credential = bootstrap_credential(&root.path).expect("bootstrap fixture operator");
        let workspace = std::env::current_dir()
            .expect("read fixture workspace")
            .to_string_lossy()
            .into_owned();
        let route = Route {
            alias: "command-release-fixture".to_owned(),
            runtime: "command".to_owned(),
            module_artifact_id: COMMAND_ARTIFACT_ID.to_owned(),
            enabled: true,
            native_options: json!({"modelId":"fixture-model","workspaceRoot":workspace}),
            workspace_option: Some("workspaceRoot".to_owned()),
            owned_service: None,
            admission_policy: None,
        };
        let mut config = Config::default();
        config.storage.data_dir = temp.0.clone();
        config.routes.push(route.clone());
        let owner =
            super::super::StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
                .await
                .expect("start fixture Store");
        let caller = owner
            .store
            .authenticate(credential)
            .await
            .expect("authenticate fixture operator");
        let fixture_route = route.clone();
        let fixture_config = config.clone();
        let caller_id = caller.client_id.clone();
        let fixture_caller = caller.clone();
        let seed = owner
            .store
            .run_fixture(move |db| {
                let module_client_id = "fixture-command-module";
                let module_link_id = "fixture-command-module-link";
                let attempt_id = "fixture-command-attempt";
                let tx = db.transaction_with_behavior(
                    rusqlite::TransactionBehavior::Immediate,
                )?;
                let mut descriptor_value: Value = serde_json::from_str(include_str!(
                    "../../../swarm-adapter-command/module-descriptor.template.json"
                ))?;
                descriptor_value["enabled"] = json!(true);
                descriptor_value["launch"]["executable"] = json!(
                    std::env::current_exe()
                        .map_err(|error| Error::new("FIXTURE_EXE_UNAVAILABLE", error.to_string()))?
                        .to_string_lossy()
                        .into_owned()
                );
                descriptor_value["artifact"]["version"] = json!(COMMAND_ARTIFACT_VERSION);
                let descriptor: swarm_contracts::module_catalog::ModuleDescriptor =
                    serde_json::from_value(descriptor_value).map_err(|_| {
                        Error::new(
                            "MODULE_DESCRIPTOR_INVALID",
                            "fixture Command descriptor is malformed",
                        )
                    })?;
                let registered = super::super::module_handshake::register_trusted_descriptor(
                    &tx,
                    descriptor,
                )?;
                let selection = super::super::module_handshake::select_route(
                    &tx,
                    &fixture_caller,
                    &json!({
                        "route_alias":fixture_route.alias,
                        "module_id":"runtime.command",
                        "artifact_id":COMMAND_ARTIFACT_ID,
                        "version":COMMAND_ARTIFACT_VERSION,
                        "expected_catalog_revision":registered["catalog_revision"]
                    }),
                    &fixture_config,
                    "fixture-route-selection",
                )?;
                let binding_id = "fixture-command-binding";
                let target_operation_id = "fixture-command-dispatch";
                let route_json = model::canonical(&json!(fixture_route))?;
                let state_json = model::canonical(&json!({
                    "module_contract_selector":selection["selection"],
                    "module_client_id":module_client_id,
                    "module_link_id":module_link_id,
                    "bridge_boot_id":"fixture-command-boot",
                    "connection":"connected"
                }))?;
                tx.execute(
                    "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
                     VALUES(?1,1,'fixture-command-lane','fixture-command-instance',?2,'ready',?3,?4,1)",
                    params![binding_id, COMMAND_ARTIFACT_ID, route_json, state_json],
                )?;
                super::super::set_meta(
                    &tx,
                    &format!("client:{module_client_id}"),
                    &json!({
                        "role":"module",
                        "disabled":false,
                        "internal_only":false,
                        "binding_id":binding_id,
                        "binding_generation":1
                    }),
                )?;

                let task_id = "fixture-command-task";
                let spec = json!({
                    "objective":"Retain partial Command capture without claiming Task completion.",
                    "phase":"verification",
                    "requirements":[],
                });
                let task_snapshot = json!({
                    "task_id":task_id,
                    "revision":1,
                    "spec":spec
                });
                let task_snapshot_sha256 =
                    model::digest(model::canonical(&task_snapshot)?.as_bytes());
                tx.execute(
                    "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                     VALUES(?1,'fixture-command-project',1,'open',?2,1,1)",
                    params![task_id, model::canonical(&spec)?],
                )?;
                tx.execute(
                    "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
                     VALUES(?1,?2,1,?3,?4,'controller',?5,1,'running','[]',1,1)",
                    params![
                        attempt_id,
                        task_id,
                        model::canonical(&task_snapshot)?,
                        caller_id,
                        binding_id,
                    ],
                )?;
                let original_dispatch = json!({
                    "client_request_id":"fixture-dispatch-request",
                    "attempt_id":attempt_id,
                    "text":"partial capture fixture"
                });
                let target_input_sha256 =
                    model::digest(model::canonical(&original_dispatch)?.as_bytes());
                let module_receipt = json!({
                    "schema_version":1,
                    "module_id":"runtime.command",
                    "artifact":{"artifact_id":COMMAND_ARTIFACT_ID,"version":COMMAND_ARTIFACT_VERSION,"build_id":null},
                    "protocol":{"major":1,"minor":0},
                    "binding_id":binding_id,
                    "binding_generation":1,
                    "operation_id":target_operation_id,
                    "input_sha256":target_input_sha256
                });
                let stream_facts = |bytes: &[u8]| {
                    let observed = [bytes, b" bytes after read failure".as_slice()].concat();
                    json!({
                        "bytes":observed.len() as u64,
                        "stored_bytes":bytes.len() as u64,
                        "sha256":model::digest(&observed),
                        "stored_sha256":model::digest(bytes),
                        "truncated":true,
                        "read_error":true
                    })
                };
                let outcome = RuntimeOutcome {
                    operation_id: target_operation_id.to_owned(),
                    outcome: EffectOutcome::Unknown,
                    native_scope_key: None,
                    native_root_id: None,
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
                        "task_acceptance_claimed":false,
                        "result_page_available":false,
                        "completion_condition":"native_result_unconfirmed",
                        "input_sha256":target_input_sha256,
                        "module_receipt":module_receipt.clone(),
                        "native_child":{
                            "spawn_returned_pid":true,
                            "birth_identity":null,
                            "exit_observed_through_child_handle":false,
                            "exit":null,
                            "family_departure_claimed":false,
                            "manager_group_drain_required":true
                        },
                        "stdout":stream_facts(b"partial stdout retained"),
                        "stderr":stream_facts(b"partial stderr retained"),
                        "timed_out":true
                    }),
                };
                let target_outcome = serde_json::to_value(&outcome)?;
                let target_outcome_sha256 =
                    model::digest(model::canonical(&target_outcome)?.as_bytes());
                let producer = json!({
                    "assignment_id":target_operation_id,
                    "dispatch_operation_id":target_operation_id,
                    "attempt_id":attempt_id,
                    "task_id":task_id,
                    "task_revision":1,
                    "task_snapshot_sha256":task_snapshot_sha256,
                    "source_text_sha256":model::digest(model::text(&original_dispatch,"text")?.as_bytes()),
                    "source_text_bytes":model::text(&original_dispatch,"text")?.len() as u64,
                    "native_session_id":null,
                    "native_input_id":null,
                    "native_payload_sha256":task_snapshot_sha256,
                    "native_payload_bytes":model::canonical(&task_snapshot)?.len() as u64,
                    "module_receipt":module_receipt.clone(),
                    "admission_kind":"normalized_task_dispatch",
                    "completion_condition":"native_input_admitted",
                    "execution_complete":false,
                    "task_completion":"unknown",
                    "disposition":"admitted"
                });
                let producers = json!([producer]);
                tx.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,result_json,due_at_ms,created_at_ms,updated_at_ms) \
                     VALUES(?1,?2,'fixture-dispatch-request','task.dispatch',?3,'{}',?4,?5,?6,1,'outcome_unknown',?7,1,1,1)",
                    params![
                        target_operation_id,
                        caller_id,
                        model::canonical(&original_dispatch)?,
                        task_id,
                        attempt_id,
                        binding_id,
                        model::canonical(&target_outcome)?
                    ],
                )?;
                tx.execute(
                    "UPDATE attempts SET start_operation_id=?2 WHERE attempt_id=?1",
                    params![attempt_id, target_operation_id],
                )?;
                tx.execute(
                    "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                    params![attempt_id, model::canonical(&producers)?],
                )?;
                tx.commit()?;
                Ok(StoreSeed {
                    module_client_id:module_client_id.to_owned(),
                    module_link_id:module_link_id.to_owned(),
                    binding_id: binding_id.to_owned(),
                    target_operation_id: target_operation_id.to_owned(),
                    attempt_id:attempt_id.to_owned(),
                    target_input_sha256,
                    target_outcome_sha256,
                    target_module_receipt:module_receipt,
                })
            })
            .await
            .expect("seed exact Command Operation and selected descriptor");

        let module = Principal {
            link_id: seed.module_link_id.clone(),
            client_id: seed.module_client_id.clone(),
            role: Role::Module,
        };
        let mut result_operation_ids = Vec::new();
        let mut result_artifact_refs = Vec::new();
        for (native_output, bytes) in [
            ("stdout.ndjson", b"partial stdout retained".as_slice()),
            ("stderr.txt", b"partial stderr retained".as_slice()),
        ] {
            let split = bytes.len() / 2;
            let chunks = [&bytes[..split], &bytes[split..]];
            let page_order = if native_output == "stderr.txt" {
                [1, 0]
            } else {
                [0, 1]
            };
            for index in page_order {
                let chunk = chunks[index];
                let offset_bytes = if index == 0 { 0 } else { split as u64 };
                let request = json!({
                    "client_request_id":format!("fixture-result-{native_output}-{index}"),
                    "binding_id":seed.binding_id,
                    "generation":1,
                    "selector":{
                        "kind":"command_output",
                        "input_operation_id":seed.target_operation_id,
                        "native_output":native_output
                    },
                    "offset_bytes":offset_bytes,
                    "length_bytes":chunk.len()
                });
                let queued = owner
                    .store
                    .call(caller.clone(), "agent.result".to_owned(), request.clone())
                    .await
                    .expect("admit exact Command result page through Store");
                let result_operation_id = model::text(&queued, "operation_id")?.to_owned();
                let delivered = owner
                    .store
                    .call(module.clone(), "module.next".to_owned(), json!({}))
                    .await
                    .expect("deliver result page through authenticated module.next");
                assert!(
                    delivered["command"].is_object(),
                    "exact result page must be dispatched: {delivered}"
                );
                let command = delivered["command"].clone();
                assert_eq!(command["operation_id"], result_operation_id);
                assert_eq!(command["method"], "agent.result");
                assert_eq!(command["binding_id"], seed.binding_id);
                assert_eq!(command["generation"], 1);
                assert_eq!(command["input"]["selector"], request["selector"]);
                assert!(command["input"]["normalized_result_origin"].is_object());
                for field in [
                    "normalized_result_origin",
                    "normalized_result_payload_identity",
                ] {
                    let mut forged = command.clone();
                    forged["input"][field] = json!({"forged":true});
                    let error = owner
                        .store
                        .serve_released_command_output(module.clone(), &forged)
                        .await
                        .expect_err("private result enrichment must match the sealed admission");
                    assert_eq!(error.code, "RESULT_PROVENANCE_INVALID");
                }
                let snapshot = command["input"]["target_command_output"].clone();
                assert_eq!(snapshot["operation_id"], seed.target_operation_id);
                assert_eq!(snapshot["native_output"], native_output);
                let result_input_sha256 = model::text(&command, "input_sha256")?.to_owned();
                assert_eq!(
                    result_input_sha256,
                    model::digest(model::canonical(&request)?.as_bytes())
                );
                let mut result_module_receipt = seed.target_module_receipt.clone();
                result_module_receipt["operation_id"] = json!(result_operation_id);
                result_module_receipt["input_sha256"] = json!(result_input_sha256);
                let source = json!({
                    "kind":"command_output",
                    "result_operation_id":result_operation_id,
                    "result_input_sha256":result_input_sha256,
                    "result_module_receipt":result_module_receipt,
                    "input_operation_id":seed.target_operation_id,
                    "target_input_sha256":snapshot["input_sha256"],
                    "target_module_receipt":snapshot["module_receipt"],
                    "target_command_output":snapshot,
                    "native_response_identity":"unavailable",
                    "execution_complete":false,
                    "task_completion":"unknown",
                    "native_replay":false
                });
                let page = json!({
                    "source":source,
                    "offset_bytes":offset_bytes,
                    "byte_length":chunk.len(),
                    "total_bytes":bytes.len(),
                    "eof":offset_bytes + chunk.len() as u64 == bytes.len() as u64,
                    "media_type":if native_output == "stdout.ndjson" {
                        "application/x-ndjson"
                    } else {
                        "text/plain; charset=utf-8"
                    },
                    "content_base64":STANDARD.encode(chunk),
                    "page_sha256":model::digest(chunk)
                });
                let persisted = owner
                    .store
                    .call(
                        module.clone(),
                        "module.result".to_owned(),
                        json!({"operation_id":result_operation_id,"page":page}),
                    )
                    .await
                    .expect("persist immutable result page through Store module.result");
                assert_eq!(persisted["recorded"], true);
                let artifact_ref = model::text(&persisted, "artifact_ref")?.to_owned();
                result_operation_ids.push(result_operation_id.clone());
                result_artifact_refs.push((result_operation_id, artifact_ref));
                let release_count = owner
                    .store
                    .run(|db| {
                        Ok(db.query_row(
                            "SELECT COUNT(*) FROM operations WHERE method=?1",
                            [swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD],
                            |row| row.get::<_, i64>(0),
                        )?)
                    })
                    .await
                    .expect("read release admission while partial pages arrive");
                assert_eq!(release_count, i64::from(result_operation_ids.len() == 4));
            }
        }
        assert_eq!(result_operation_ids.len(), 4);

        let release_delivery = owner
            .store
            .call(module.clone(), "module.next".to_owned(), json!({}))
            .await
            .expect("deliver Store-derived release through module.next");
        let release_command = release_delivery["command"].clone();
        let release_method = model::text(&release_command, "method")?.to_owned();
        assert_eq!(
            release_method,
            swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD
        );
        assert_eq!(
            swarm_contracts::module_command::classify_runtime_command(
                &release_method,
                &release_command["input"]
            ),
            Ok(swarm_contracts::module_command::RuntimeCommandKind::CommandReleaseEvidence)
        );
        assert_eq!(release_command["binding_id"], seed.binding_id);
        assert_eq!(release_command["generation"], 1);
        assert_eq!(release_command["input"]["attempt_id"], seed.attempt_id);
        let release_operation_id = model::text(&release_command, "operation_id")?.to_owned();
        let release_input_sha256 = model::text(&release_command, "input_sha256")?.to_owned();
        assert_eq!(
            release_input_sha256,
            model::digest(model::canonical(&release_command["input"])?.as_bytes())
        );
        assert_eq!(
            release_command["input"]["client_request_id"],
            capture_release_client_request_id(&seed.target_operation_id)
        );
        let evidence: CommandCaptureReleaseEvidence = serde_json::from_value(
            release_command["input"]
                [swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD]
                .clone(),
        )
        .expect("decode exact Store-derived release evidence");
        assert_eq!(evidence.target_operation_id, seed.target_operation_id);
        assert_eq!(evidence.target_input_sha256, seed.target_input_sha256);
        assert_eq!(evidence.target_outcome_sha256, seed.target_outcome_sha256);
        assert_eq!(evidence.outputs.len(), 2);
        for (output, expected_name, expected_bytes) in [
            (
                &evidence.outputs[0],
                "stdout.ndjson",
                b"partial stdout retained".as_slice(),
            ),
            (
                &evidence.outputs[1],
                "stderr.txt",
                b"partial stderr retained".as_slice(),
            ),
        ] {
            assert_eq!(output.native_output, expected_name);
            assert_eq!(
                output.target_command_output["stored_sha256"],
                model::digest(expected_bytes)
            );
            assert_eq!(
                output.target_command_output["stored_bytes"],
                expected_bytes.len()
            );
            assert_eq!(output.pages.len(), 2);
            assert!(output.pages.iter().all(|page| {
                result_operation_ids.contains(&page.result_operation_id)
                    && result_artifact_refs
                        .iter()
                        .any(|(operation_id, artifact_ref)| {
                            operation_id == &page.result_operation_id
                                && artifact_ref == &page.artifact_ref
                        })
                    && page.total_bytes == expected_bytes.len() as u64
                    && page.eof == (page.offset_bytes + page.byte_length == page.total_bytes)
            }));
        }

        let release_query_id = release_operation_id.clone();
        let release_target_id = seed.target_operation_id.clone();
        let (release_operation, release_count) = owner
            .store
            .run_fixture(move |db| {
                let operation = operations::get_operation(db, &release_query_id)?;
                let count: i64 = db.query_row(
                    "SELECT count(*) FROM operations WHERE method=?1 AND json_extract(original_request_json,'$.target_operation_id')=?2",
                    params![
                        swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD,
                        release_target_id
                    ],
                    |row| row.get(0),
                )?;
                Ok((operation, count))
            })
            .await
            .expect("read exact release Operation and uniqueness count");
        assert_eq!(release_count, 1);
        assert_eq!(release_operation["method"], release_method);
        assert_eq!(release_operation["binding_id"], seed.binding_id);
        assert_eq!(release_operation["binding_generation"], 1);
        assert_eq!(release_operation["attempt_id"], seed.attempt_id);

        let mut artifact_refs = Vec::new();
        for output in &evidence.outputs {
            artifact_refs.extend(output.pages.iter().map(|page| page.artifact_ref.clone()));
        }
        let mut release_module_receipt = seed.target_module_receipt.clone();
        release_module_receipt["operation_id"] = json!(release_operation_id);
        release_module_receipt["input_sha256"] = json!(release_input_sha256);
        let release_outcome = RuntimeOutcome {
            operation_id: release_operation_id.clone(),
            outcome: EffectOutcome::Applied,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
                "module_receipt":release_module_receipt,
                "completion_condition":"command_capture_release_recorded",
                "release_input_sha256":release_input_sha256,
                "target_operation_id":seed.target_operation_id,
                "target_input_sha256":evidence.target_input_sha256,
                "target_outcome_sha256":evidence.target_outcome_sha256,
                "coverage_sha256":model::digest(
                    model::canonical(&serde_json::to_value(&evidence)?)?.as_bytes()
                ),
                "artifact_refs":artifact_refs,
                "native_capture_released":true,
                "native_replay":false
            }),
        };
        let release_value = serde_json::to_value(release_outcome)?;
        for (field, replacement) in [
            ("release_input_sha256", json!("0".repeat(64))),
            ("target_operation_id", json!("another-dispatch")),
            ("target_input_sha256", json!("0".repeat(64))),
            ("target_outcome_sha256", json!("0".repeat(64))),
            ("coverage_sha256", json!("0".repeat(64))),
            ("artifact_refs", json!([])),
            ("native_capture_released", json!(false)),
            ("native_replay", json!(true)),
            ("task_completion", json!("accepted")),
        ] {
            let mut forged = release_value.clone();
            forged["details"][field] = replacement;
            let error = owner
                .store
                .call(module.clone(), "module.outcome".to_owned(), forged)
                .await
                .expect_err("forged release ACK must not settle the operation");
            assert_eq!(error.code, "COMMAND_RELEASE_EVIDENCE_INVALID", "{field}");
        }
        let mut missing_receipt = release_value.clone();
        missing_receipt["details"]
            .as_object_mut()
            .expect("release details")
            .remove("module_receipt");
        let error = owner
            .store
            .call(module.clone(), "module.outcome".to_owned(), missing_receipt)
            .await
            .expect_err("release ACK requires its exact selected module receipt");
        assert_eq!(error.code, "MODULE_RECEIPT_INVALID");
        owner
            .store
            .call(module.clone(), "module.outcome".to_owned(), release_value)
            .await
            .expect("record exact release ACK through module.outcome");

        let mut manager = caller.clone();
        manager.role = Role::Manager;
        let direct_release = owner
            .store
            .call(
                manager,
                release_method.clone(),
                release_command["input"].clone(),
            )
            .await
            .expect_err("external Manager cannot replay the acknowledged release proof");
        assert_eq!(direct_release.code, "FORBIDDEN");
        let mut injected_request = json!({
            "client_request_id":"fixture-forged-release-proof",
            "binding_id":seed.binding_id,
            "generation":1,
            "selector":{
                "kind":"command_output",
                "input_operation_id":seed.target_operation_id,
                "native_output":"stdout.ndjson"
            }
        });
        injected_request[swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD] =
            release_command["input"]
                [swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_FIELD]
                .clone();
        let injected_release = owner
            .store
            .call(caller.clone(), "agent.result".to_owned(), injected_request)
            .await
            .expect_err("ordinary Manager result request cannot smuggle release proof");
        assert_eq!(injected_release.code, "FORBIDDEN");

        let future_request = json!({
            "client_request_id":"fixture-post-release-stdout-subrange",
            "binding_id":seed.binding_id,
            "generation":1,
            "selector":{
                "kind":"command_output",
                "input_operation_id":seed.target_operation_id,
                "native_output":"stdout.ndjson"
            },
            "offset_bytes":2,
            "length_bytes":7
        });
        let future_queued = owner
            .store
            .call(
                caller.clone(),
                "agent.result".to_owned(),
                future_request.clone(),
            )
            .await
            .expect("queue future command_output read through Store");
        let future_operation_id = model::text(&future_queued, "operation_id")?.to_owned();
        let sentinel_request = json!({
            "client_request_id":"fixture-post-release-next-command",
            "binding_id":seed.binding_id,
            "generation":1
        });
        let sentinel_queued = owner
            .store
            .call(caller.clone(), "agent.refresh".to_owned(), sentinel_request)
            .await
            .expect("queue a following module command to observe Store interception");
        let sentinel_operation_id = model::text(&sentinel_queued, "operation_id")?.to_owned();
        let next = owner
            .store
            .call(module.clone(), "module.next".to_owned(), json!({}))
            .await
            .expect("serve released output internally before returning the next module command");
        let sentinel_command = next["command"].clone();
        assert_eq!(sentinel_command["operation_id"], sentinel_operation_id);
        assert_eq!(sentinel_command["method"], "agent.refresh");
        assert_ne!(sentinel_command["operation_id"], future_operation_id);

        let future_query_id = future_operation_id.clone();
        let (future_operation, saved_request) = owner
            .store
            .run_fixture(move |db| {
                let operation = operations::get_operation(db, &future_query_id)?;
                let raw: String = db.query_row(
                    "SELECT original_request_json FROM operations WHERE operation_id=?1",
                    [&future_query_id],
                    |row| row.get(0),
                )?;
                Ok((operation, serde_json::from_str::<Value>(&raw)?))
            })
            .await
            .expect("read Store-persisted post-release result Operation");
        assert_eq!(future_operation["state"], "settled");
        assert_eq!(future_operation["result"]["outcome"], "applied");
        assert_eq!(future_operation["binding_id"], seed.binding_id);
        assert_eq!(future_operation["binding_generation"], 1);
        assert_eq!(saved_request, future_request);
        assert_eq!(
            future_operation["result"]["details"]["source"]["result_module_receipt"]["operation_id"],
            future_operation_id
        );
        assert_eq!(
            future_operation["result"]["details"]["source"]["native_response_identity"],
            "unavailable"
        );
        let artifact_ref =
            model::text(&future_operation["result"]["details"], "artifact_ref")?.to_owned();
        let future_page_id = future_operation_id.clone();
        let future_artifact_ref = artifact_ref.clone();
        let artifact = owner
            .store
            .run_fixture(move |db| {
                results::command_result_page_readback(db, &future_page_id, &future_artifact_ref)
            })
            .await
            .expect("verify immutable Store artifact receipt");
        let expected_slice = b"partial stdout retained"[2..9].to_vec();
        let verified_page = owner
            .store
            .file_io(move |files| files.read(&artifact, 0, MAX_PAGE_BYTES))
            .await
            .expect("read verified published artifact bytes");
        assert_eq!(
            verified_page["content"],
            String::from_utf8(expected_slice.clone()).unwrap()
        );
        assert_eq!(
            verified_page["artifact_sha256"],
            future_operation["result"]["details"]["page_sha256"]
        );
        assert_eq!(
            future_operation["result"]["details"]["byte_length"],
            expected_slice.len()
        );
        assert_eq!(future_operation["result"]["details"]["offset_bytes"], 2);

        let mut sentinel_module_receipt = seed.target_module_receipt.clone();
        sentinel_module_receipt["operation_id"] = json!(sentinel_operation_id);
        sentinel_module_receipt["input_sha256"] = sentinel_command["input_sha256"].clone();
        let sentinel_outcome = RuntimeOutcome {
            operation_id: sentinel_operation_id,
            outcome: EffectOutcome::Rejected,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
                "completion_condition":"fixture_consumer_sentinel_rejected",
                "module_receipt":sentinel_module_receipt
            }),
        };
        owner
            .store
            .call(
                module,
                "module.outcome".to_owned(),
                serde_json::to_value(sentinel_outcome)?,
            )
            .await
            .expect("close fixture sentinel Operation without a success receipt");

        let release_query_id = release_operation_id.clone();
        let (settled_release, release_count) = owner
            .store
            .run_fixture(move |db| {
                let operation = operations::get_operation(db, &release_query_id)?;
                let release_target_operation_id =
                    model::text(&operation["result"]["details"], "target_operation_id")?;
                let count: i64 = db.query_row(
                    "SELECT count(*) FROM operations WHERE method=?1 AND json_extract(original_request_json,'$.target_operation_id')=?2",
                    params![
                        swarm_contracts::module_command::COMMAND_RELEASE_EVIDENCE_METHOD,
                        release_target_operation_id
                    ],
                    |row| row.get(0),
                )?;
                Ok((operation, count))
            })
            .await
            .expect("confirm one retained release Operation after ACK");
        assert_eq!(release_count, 1);
        assert_eq!(settled_release["state"], "settled");
        assert_eq!(settled_release["result"]["outcome"], "applied");
        assert_eq!(settled_release["attempt_id"], seed.attempt_id);
        owner.close().await.expect("close fixture Store");
        Ok(())
    }
}
