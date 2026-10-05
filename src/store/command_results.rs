//! Exact, bounded status pages for a terminal Command dispatch.
//!
//! Command's one-shot JSON result does not carry a native assistant-message
//! identity. This projection reports only the saved Operation state and
//! checked terminal facts; it never publishes response text or task completion.

use super::{operations, results, runtime};
use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model::{self, Principal},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

const COMMAND_ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
const COMMAND_ARTIFACT_VERSION: &str = "3";
const COMMAND_MODULE_ID: &str = "runtime.command";
const RESULT_CAPABILITY: &str = "agent.result";
const MAX_PAGE_BYTES: usize = crate::artifacts::MAX_PAGE_BYTES;

/// Validate the Command v3 result selector at manager admission. The
/// descriptor capability is compatibility metadata; normal Store method,
/// principal, binding and Attempt checks remain authoritative.
pub(super) fn validate_request(db: &Connection, binding: &Value, request: &Value) -> Result<()> {
    require_result_capability(db, binding)?;
    let selector = request
        .get("selector")
        .ok_or_else(|| Error::invalid("Command status selector is required"))?;
    model::fields(selector, &["kind", "input_operation_id"])?;
    if model::text(selector, "kind")? != "command_status" {
        return Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "Command v3 supports only exact dispatch status pages",
        ));
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
    let snapshot = target_snapshot(db, binding, target_id)?;
    let status_bytes = page_bytes(&json!({"target_operation_status":snapshot}))?;
    if offset > status_bytes.len() as u64 {
        return Err(Error::new(
            "RESULT_RANGE_INVALID",
            "Command status offset exceeds the exact status page",
        ));
    }
    Ok(())
}

/// Build the Store-authoritative target snapshot injected into `module.next`.
/// Only a terminal dispatch can be published; pending/unknown Operations stay
/// visible through their existing Operation readback and are never promoted.
pub(super) fn target_snapshot(
    db: &Connection,
    binding: &Value,
    target_operation_id: &str,
) -> Result<Value> {
    require_result_capability(db, binding)?;
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let target = operations::get_operation(db, target_operation_id)?;
    if target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || target["method"] != "task.dispatch"
        || !matches!(target["state"].as_str(), Some("settled" | "rejected"))
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

    let outcome: RuntimeOutcome =
        serde_json::from_value(target["result"].clone()).map_err(|_| {
            Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "terminal Command dispatch has no typed outcome",
            )
        })?;
    let (outcome_name, expected_state) = match &outcome.outcome {
        EffectOutcome::Applied => ("applied", "settled"),
        EffectOutcome::Rejected => ("rejected", "rejected"),
        EffectOutcome::Accepted | EffectOutcome::Unknown => {
            return Err(Error::new(
                "RESULT_TARGET_NOT_TERMINAL",
                "Command status target has no terminal outcome",
            ));
        }
    };
    if target["state"] != expected_state
        || outcome.operation_id != target_operation_id
        || !outcome.native_root_id.is_none()
        || !outcome.native_scope_key.is_none()
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "Command terminal receipt differs from its retained Operation",
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
            "Command status pages require the standalone Command route",
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
    if context["selector"]["kind"] != "command_status" {
        return Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "Command status page selector is invalid",
        ));
    }
    let target_id = model::text(&context["selector"], "input_operation_id")?;
    let snapshot = target_snapshot_from_context(db, &context, target_id)?;
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
    Ok(context)
}

fn target_snapshot_from_context(
    db: &Connection,
    context: &Value,
    target_id: &str,
) -> Result<Value> {
    let binding_id = model::text(context, "binding_id")?;
    let generation = model::positive(context, "generation")?;
    let binding = operations::get_binding(db, binding_id, generation)?;
    target_snapshot(db, &binding, target_id)
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
        || source["evidence"] != "store_retained_module_reported_operation_receipt"
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
    for key in [
        "result_input_sha256",
        "target_operation_id",
        "target_input_sha256",
        "target_operation_status",
    ] {
        if context[key] != artifact.metadata[key] {
            return Err(Error::conflict(
                "Command status context changed before result registration",
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
