//! Exact, bounded status pages for a Command dispatch.
//!
//! Command's one-shot JSON result does not carry a native assistant-message
//! identity. This projection reports only the saved Operation state and
//! checked terminal facts; it never publishes response text or task completion.

use super::{meta, operations, results, runtime, tasks};
use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use swarm_contracts::runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext};

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
