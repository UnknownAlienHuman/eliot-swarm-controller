//! File publication precedes this transaction. No filesystem or SDK calls in Store.
use super::{operations, runtime};
use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

/// Historical input status remains available under its original contract.
/// Loop-step status additionally requires the exact selected native command
/// contract; a runtime name alone cannot admit a new result source.
pub(super) fn input_status_target_supported(
    db: &Connection,
    binding: &Value,
    target: &Value,
) -> Result<bool> {
    match target["method"].as_str() {
        Some("task.dispatch" | "agent.send") => Ok(true),
        Some("native.opencode.loop_step")
            if binding["route"]["runtime"] == crate::runtime::opencode_v2::RUNTIME =>
        {
            Ok(super::module_handshake::selected_native_command_supported(
                db,
                model::text(binding, "module_artifact_id")?,
                binding["observation"].get("module_contract_selector"),
                "native.opencode.loop_step",
                &Value::Null,
            )? == Some(true))
        }
        _ => Ok(false),
    }
}

/// Build a bounded status projection from a terminal Operation already
/// recorded by the same strict Antigravity binding. This is Store-side
/// provenance only; it deliberately contains no native response body or
/// inferred Task/execution-completion fact.
pub(super) fn antigravity_status_snapshot(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    binding: &Value,
    target_operation_id: &str,
    session_id: &str,
) -> Result<Value> {
    if binding["route"]["runtime"] != "antigravity"
        || binding["native_root_id"].as_str() != Some(session_id)
        || binding["observation"]["module_contract_selector"].is_null()
    {
        return Err(Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Antigravity status requires the exact selected binding session",
        ));
    }
    let target = operations::get_operation(db, target_operation_id)?;
    if target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || !matches!(
            target["method"].as_str(),
            Some("task.dispatch" | "agent.send")
        )
        || !matches!(target["state"].as_str(), Some("settled" | "rejected"))
    {
        return Err(Error::new(
            "RESULT_TARGET_NOT_TERMINAL",
            "status target must be a terminal dispatch or send on this binding generation",
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
            "status target is absent from this binding generation",
        )
    })?;
    let request: Value = serde_json::from_str(&raw)?;
    let target_input_sha256 = model::digest(model::canonical(&request)?.as_bytes());

    let outcome: RuntimeOutcome =
        serde_json::from_value(target["result"].clone()).map_err(|_| {
            Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "terminal status target has no typed module outcome",
            )
        })?;
    let outcome_matches_state = matches!(
        (target["state"].as_str(), &outcome.outcome),
        (Some("settled"), EffectOutcome::Applied) | (Some("rejected"), EffectOutcome::Rejected)
    );
    if outcome.operation_id != target_operation_id
        || !outcome_matches_state
        || outcome.native_root_id.as_deref() != Some(session_id)
        || outcome.native_scope_key.as_deref() != binding["native_scope_key"].as_str()
    {
        return Err(Error::new(
            "RESULT_TARGET_RECEIPT_INVALID",
            "terminal status target differs from the retained native binding receipt",
        ));
    }
    let target_module_receipt = super::runtime::validate_module_receipt_for_operation(
        db, binding_id, generation, binding, &outcome,
    )?;

    let diagnostic_code = target["result"]["details"]["diagnostic_code"]
        .as_str()
        .filter(|value| safe_diagnostic_code(value))
        .map(str::to_owned);
    let native_failure_status = antigravity_native_failure_status(
        &target["result"],
        target_operation_id,
        session_id,
        &outcome.outcome,
    );

    Ok(json!({
        "operation_id":target_operation_id,
        "method":target["method"],
        "operation_state":target["state"],
        "operation_outcome":target["result"]["outcome"],
        "diagnostic_code":diagnostic_code,
        "native_failure_status":native_failure_status,
        "target_input_sha256":target_input_sha256,
        "module_receipt":target_module_receipt,
    }))
}

fn antigravity_native_failure_status(
    result: &Value,
    operation_id: &str,
    session_id: &str,
    outcome: &EffectOutcome,
) -> Option<&'static str> {
    if !matches!(outcome, EffectOutcome::Rejected)
        || result["details"]["completion_condition"] != "native_terminal_result_observed"
    {
        return None;
    }
    let reference = &result["details"]["local_execution_ref"];
    let status = result["details"]["turn_status"].as_str()?;
    if !matches!(status, "ERROR" | "CANCELED" | "INTERRUPTED")
        || reference["input_operation_id"].as_str() != Some(operation_id)
        || reference["native_conversation_id"].as_str() != Some(session_id)
        || reference["status"].as_str() != Some(status)
        || reference["bridge_boot_id"]
            .as_str()
            .is_none_or(str::is_empty)
        || reference["result_ordinal"]
            .as_u64()
            .is_none_or(|ordinal| ordinal == 0)
        || reference["response_sha256"]
            .as_str()
            .is_none_or(|digest| !valid_sha256(digest))
    {
        return None;
    }
    Some(match status {
        "ERROR" => "ERROR",
        "CANCELED" => "CANCELED",
        "INTERRUPTED" => "INTERRUPTED",
        _ => return None,
    })
}

fn safe_diagnostic_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn validate_sealed_module_receipt(
    descriptor: &Value,
    value: &Value,
    operation_id: &str,
    binding_id: &str,
    generation: i64,
    input_sha256: &str,
) -> Result<()> {
    let receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(value.clone()).map_err(|_| {
            Error::new(
                "MODULE_RECEIPT_INVALID",
                "sealed Claude result origin has an invalid module receipt",
            )
        })?;
    receipt.validate().map_err(|_| {
        Error::new(
            "MODULE_RECEIPT_INVALID",
            "sealed Claude result module receipt is outside protocol 1.0",
        )
    })?;
    if receipt.binding_id != binding_id
        || receipt.binding_generation != generation
        || receipt.operation_id != operation_id
        || receipt.input_sha256 != input_sha256
        || serde_json::to_value(&receipt.module_id)? != descriptor["module_id"]
        || serde_json::to_value(&receipt.artifact)? != descriptor["artifact"]
        || serde_json::to_value(receipt.protocol)? != descriptor["protocol"]
    {
        return Err(Error::new(
            "MODULE_RECEIPT_INVALID",
            "sealed Claude result module receipt differs from its admitted descriptor or Operation",
        ));
    }
    Ok(())
}

pub(super) fn load_claude_result_origin(
    db: &Connection,
    operation_id: &str,
    operation: &Value,
    binding_id: &str,
    generation: i64,
    target_operation_id: &str,
    target: &Value,
) -> Result<Value> {
    if operation["operation_id"] != operation_id
        || operation["method"] != "agent.result"
        || operation["binding_id"] != binding_id
        || operation["binding_generation"] != generation
        || target["operation_id"] != target_operation_id
        || target["method"] != "task.dispatch"
        || target["binding_id"] != binding_id
        || target["binding_generation"] != generation
        || target["state"] != "settled"
        || target["result"]["outcome"] != "applied"
    {
        return Err(Error::new(
            "RESULT_TARGET_SCOPE_INVALID",
            "Claude result origin does not name the exact retained dispatch",
        ));
    }
    let effective_raw: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![operation_id, binding_id, generation],
        |row| row.get(0),
    )?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let origin = effective["claude_result_origin"].clone();
    if origin["schema_version"] != 1
        || origin["kind"] != "claude_assistant_result"
        || origin["result_operation_id"] != operation_id
        || origin["target_operation_id"] != target_operation_id
        || origin["target_method"] != "task.dispatch"
        || origin["binding_id"] != binding_id
        || origin["binding_generation"] != generation
        || origin["result_selector"]["kind"] != "claude_assistant_result"
        || origin["result_selector"]["input_operation_id"] != target_operation_id
        || !origin["result_selector"]["session_id"].is_string()
        || !origin["native_scope_key"].is_string()
        || origin["descriptor"].is_null()
        || origin["producer"].is_null()
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "Claude result Operation has no valid sealed origin",
        ));
    }
    let result_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![operation_id, binding_id, generation],
        |row| row.get(0),
    )?;
    let result_request: Value = serde_json::from_str(&result_raw)?;
    let result_input_sha256 = model::digest(model::canonical(&result_request)?.as_bytes());
    if origin["result_input_sha256"] != result_input_sha256
        || origin["result_selector"] != result_request["selector"]
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result origin differs from its immutable request",
        ));
    }
    let target_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![target_operation_id, binding_id, generation],
        |row| row.get(0),
    )?;
    let target_request: Value = serde_json::from_str(&target_raw)?;
    let target_input_sha256 = model::digest(model::canonical(&target_request)?.as_bytes());
    if origin["target_input_sha256"] != target_input_sha256
        || target["task_id"] != origin["target_task_id"]
        || target["attempt_id"] != origin["target_attempt_id"]
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result origin differs from the retained Task Attempt identity",
        ));
    }
    let target_outcome: RuntimeOutcome =
        serde_json::from_value(target["result"].clone()).map_err(|_| {
            Error::new(
                "RESULT_TARGET_RECEIPT_INVALID",
                "Claude result target has no typed retained outcome",
            )
        })?;
    if target_outcome.operation_id != target_operation_id
        || !matches!(target_outcome.outcome, EffectOutcome::Applied)
        || target_outcome.native_root_id.as_deref() != origin["native_session_id"].as_str()
        || target_outcome.native_scope_key.as_deref() != origin["native_scope_key"].as_str()
        || target_outcome.details["completion_condition"] != "native_input_admitted"
        || target_outcome.details["execution_complete"] != false
        || target_outcome.details["task_completion"] != "unknown"
        || target_outcome.details["module_receipt"] != origin["target_module_receipt"]
        || target_outcome.details["dispatch_admission"] != origin["dispatch_admission"]
    {
        return Err(Error::new(
            "RESULT_TARGET_NOT_ADMITTED",
            "Claude result target differs from the sealed native admission",
        ));
    }
    let admission: swarm_contracts::runtime::TaskDispatchAdmissionReceipt =
        serde_json::from_value(origin["dispatch_admission"].clone()).map_err(|_| {
            Error::new(
                "RESULT_TARGET_ORIGIN_INVALID",
                "sealed Claude result origin has an invalid dispatch admission",
            )
        })?;
    admission.validate().map_err(|_| {
        Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result dispatch admission is invalid",
        )
    })?;
    if serde_json::to_value(&admission.module_receipt)? != origin["target_module_receipt"]
        || admission.operation_id != target_operation_id
        || admission.binding_id != binding_id
        || admission.binding_generation != generation
        || admission.attempt_id != origin["target_attempt_id"]
        || admission.task_id != origin["target_task_id"]
        || admission.task_revision != origin["target_task_revision"]
        || admission.native_input_id != origin["native_input_id"].as_str().map(str::to_owned)
        || admission.native_payload_sha256 != origin["native_payload_sha256"]
        || admission.native_payload_bytes != origin["native_payload_bytes"]
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result origin differs from the normalized dispatch admission",
        ));
    }
    let producer = &origin["producer"];
    if producer["assignment_id"] != target_operation_id
        || producer["dispatch_operation_id"] != target_operation_id
        || producer["task_id"] != origin["target_task_id"]
        || producer["attempt_id"] != origin["target_attempt_id"]
        || producer["task_revision"] != origin["target_task_revision"]
        || producer["native_session_id"] != origin["native_session_id"]
        || producer["native_input_id"] != origin["native_input_id"]
        || producer["native_payload_sha256"] != origin["native_payload_sha256"]
        || producer["native_payload_bytes"] != origin["native_payload_bytes"]
        || producer["completion_condition"] != "native_input_admitted"
        || producer["execution_complete"] != false
        || producer["task_completion"] != "unknown"
        || producer["disposition"] != "admitted"
        || producer["admission_kind"] != "normalized_task_dispatch"
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result producer identity is inconsistent",
        ));
    }
    validate_sealed_module_receipt(
        &origin["descriptor"],
        &origin["target_module_receipt"],
        target_operation_id,
        binding_id,
        generation,
        target_input_sha256.as_str(),
    )?;
    if origin["target_task_revision"]
        .as_i64()
        .is_none_or(|revision| revision <= 0)
        || !valid_sha256(origin["native_payload_sha256"].as_str().unwrap_or_default())
        || origin["native_payload_bytes"]
            .as_u64()
            .is_none_or(|bytes| bytes == 0)
    {
        return Err(Error::new(
            "RESULT_TARGET_ORIGIN_INVALID",
            "sealed Claude result native admission identity is invalid",
        ));
    }
    if let Some(result_receipt) = operation["result"]
        .get("details")
        .and_then(|d| d.get("module_receipt"))
        && !result_receipt.is_null()
    {
        validate_sealed_module_receipt(
            &origin["descriptor"],
            result_receipt,
            operation_id,
            binding_id,
            generation,
            result_input_sha256.as_str(),
        )?;
    }
    Ok(origin)
}

pub(super) fn antigravity_status_page_bytes(metadata: &Value) -> Result<Vec<u8>> {
    let source = &metadata["source"];
    Ok(serde_json::to_vec(&json!({
        "schema_version":1,
        "operation_id":source["input_operation_id"],
        "method":source["target_method"],
        "operation_state":source["target_operation_state"],
        "operation_outcome":source["target_operation_outcome"],
        "diagnostic_code":source["target_diagnostic_code"],
        "native_failure_status":source["native_failure_status"],
        "native_session_id":source["native_session_id"],
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false,
    }))?)
}

/// Re-admit only an already-authenticated Claude result against its retained
/// binding generation and sealed dispatch origin. Historical result pages may
/// arrive after the binding or current module link has been released, so this
/// deliberately avoids the live `runtime::scope` release/link checks. The
/// module registration, result Operation, retained target and immutable origin
/// still have to agree before any page is prepared or recorded.
fn admitted_claude_result_scope(
    db: &Connection,
    p: &Principal,
    operation_id: &str,
) -> Result<Option<(String, i64, Value)>> {
    if p.role != Role::Module {
        return Ok(None);
    }
    let Some(client) = super::meta(db, &format!("client:{}", p.client_id))? else {
        return Ok(None);
    };
    // The transport authenticated this carrier. This narrow completion path
    // collects an existing result; revocation still denies new connections and
    // new commands through their ordinary admission paths.
    if client["role"] != "module" {
        return Ok(None);
    }

    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "agent.result" {
        return Ok(None);
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    if request["selector"]["kind"] != "claude_assistant_result" {
        return Ok(None);
    }

    let binding_id = model::text(&client, "binding_id")?.to_owned();
    let generation = model::positive(&client, "binding_generation")?;
    let binding = operations::get_binding(db, &binding_id, generation)?;
    if binding["observation"]["module_client_id"] != p.client_id {
        return Err(Error::new(
            "MODULE_OWNER_MISMATCH",
            "credential does not own the retained Claude result binding",
        ));
    }
    // The Manager is the result Operation's caller; the Module is its carrier.
    // Authenticate the carrier against the retained binding, not caller_id.
    if operation["binding_id"] != binding_id || operation["binding_generation"] != generation {
        return Err(Error::new(
            "FORBIDDEN",
            "Claude result is not admitted under this module credential tuple",
        ));
    }
    let target_id = model::text(&request["selector"], "input_operation_id")?;
    let target = operations::get_operation(db, target_id)?;
    load_claude_result_origin(
        db,
        operation_id,
        &operation,
        &binding_id,
        generation,
        target_id,
        &target,
    )?;
    Ok(Some((binding_id, generation, binding)))
}

pub(super) fn prepare(
    db: &Connection,
    p: &Principal,
    operation_id: &str,
    source: &Value,
) -> Result<Value> {
    let normalized_result_page =
        source["schema_id"] == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID;
    let admitted_command_status_scope = if normalized_result_page {
        None
    } else {
        super::command_results::admitted_status_scope(db, p, operation_id)?
    };
    let (id, generation, b) = if normalized_result_page {
        runtime::admitted_result_scope(db, p, operation_id)?
    } else if let Some(scope) = admitted_command_status_scope {
        scope
    } else {
        match admitted_claude_result_scope(db, p, operation_id)? {
            Some(scope) => scope,
            None => runtime::scope(db, p, true)?,
        }
    };
    let op = operations::get_operation(db, operation_id)?;
    if op["method"] != "agent.result"
        || op["binding_id"] != id
        || op["binding_generation"] != generation
    {
        return Err(Error::new(
            "FORBIDDEN",
            "result belongs to another operation or binding",
        ));
    }
    if !matches!(
        op["state"].as_str(),
        Some("sending" | "native_accepted" | "outcome_unknown" | "settled")
    ) {
        return Err(Error::conflict(
            "result read was not admitted or has already been rejected",
        ));
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |r| r.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    let mut context = json!({
        "operation_id":operation_id,
        "binding_id":id,
        "generation":generation,
        "native_root_id":b["native_root_id"],
        "native_scope_key":b["native_scope_key"],
        "selector":request["selector"],
        "requested_offset":request["offset_bytes"].as_u64().unwrap_or(0),
        "requested_length":request["length_bytes"].as_u64().unwrap_or(crate::artifacts::MAX_PAGE_BYTES as u64)
    });
    if normalized_result_page {
        let (_, origin) = super::normalized_result::validate_source(db, operation_id, source)?;
        let effective_raw: String = db.query_row(
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )?;
        let effective: Value = serde_json::from_str(&effective_raw)?;
        context["native_root_id"] = effective["native_root_id"].clone();
        context["native_scope_key"] = effective["native_scope_key"].clone();
        context["normalized_result_origin"] = origin;
    } else if request["selector"]["kind"] == "input_status" {
        model::fields(
            &request["selector"],
            &["kind", "input_operation_id", "session_id"],
        )?;
        let target_id = model::text(&request["selector"], "input_operation_id")?;
        let session_id = model::text(&request["selector"], "session_id")?;
        let target = operations::get_operation(db, target_id)?;
        if target["binding_id"] != id
            || target["binding_generation"] != generation
            || !input_status_target_supported(db, &b, &target)?
            || b["native_root_id"].as_str() != Some(session_id)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "input status must name an admitted input Operation on this exact binding session",
            ));
        }
        let target_raw: String = db.query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
            params![target_id, id, generation],
            |row| row.get(0),
        )?;
        let target_request: Value = serde_json::from_str(&target_raw)?;
        let target_input_sha256 = model::digest(model::canonical(&target_request)?.as_bytes());
        context["result_input_sha256"] =
            json!(model::digest(model::canonical(&request)?.as_bytes()));
        context["target_operation_id"] = json!(target_id);
        context["target_method"] = target["method"].clone();
        context["target_input_sha256"] = json!(target_input_sha256);
        validate_input_status_source(db, &id, generation, &b, &request, source, &context)?;
    } else if request["selector"]["kind"] == "antigravity_status" {
        model::fields(
            &request["selector"],
            &["kind", "input_operation_id", "session_id"],
        )?;
        let target_id = model::text(&request["selector"], "input_operation_id")?;
        let session_id = model::text(&request["selector"], "session_id")?;
        let target_status =
            antigravity_status_snapshot(db, &id, generation, &b, target_id, session_id)?;
        context["result_input_sha256"] =
            json!(model::digest(model::canonical(&request)?.as_bytes()));
        context["target_operation_id"] = json!(target_id);
        context["target_method"] = target_status["method"].clone();
        context["target_input_sha256"] = target_status["target_input_sha256"].clone();
        context["target_operation_status"] = target_status.clone();
        context["native_session_id"] = json!(session_id);
        validate_antigravity_status_source(db, &id, generation, &b, source, &context)?;
    } else if request["selector"]["kind"] == "claude_assistant_result" {
        model::fields(
            &request["selector"],
            &["kind", "input_operation_id", "session_id"],
        )?;
        let target_id = model::text(&request["selector"], "input_operation_id")?;
        let session_id = model::text(&request["selector"], "session_id")?;
        let target = operations::get_operation(db, target_id)?;
        let origin =
            load_claude_result_origin(db, operation_id, &op, &id, generation, target_id, &target)?;
        if origin["native_session_id"] != session_id {
            return Err(Error::new(
                "RESULT_TARGET_SCOPE_INVALID",
                "Claude result selector differs from its sealed native session",
            ));
        }
        validate_sealed_module_receipt(
            &origin["descriptor"],
            &source["result_module_receipt"],
            operation_id,
            &id,
            generation,
            origin["result_input_sha256"].as_str().unwrap_or_default(),
        )?;
        context["binding_id"] = origin["binding_id"].clone();
        context["generation"] = origin["binding_generation"].clone();
        context["native_root_id"] = origin["native_session_id"].clone();
        context["native_scope_key"] = origin["native_scope_key"].clone();
        context["result_input_sha256"] = origin["result_input_sha256"].clone();
        context["result_module_receipt"] = source["result_module_receipt"].clone();
        context["target_operation_id"] = origin["target_operation_id"].clone();
        context["target_method"] = origin["target_method"].clone();
        context["target_input_sha256"] = origin["target_input_sha256"].clone();
        context["target_module_receipt"] = origin["target_module_receipt"].clone();
        context["target_attempt_id"] = origin["target_attempt_id"].clone();
        context["target_task_id"] = origin["target_task_id"].clone();
        context["target_task_revision"] = origin["target_task_revision"].clone();
        context["native_session_id"] = origin["native_session_id"].clone();
        context["native_input_id"] = origin["native_input_id"].clone();
        context["native_payload_sha256"] = origin["native_payload_sha256"].clone();
        context["native_payload_bytes"] = origin["native_payload_bytes"].clone();
        validate_claude_assistant_result_source(&request, source, &context, &origin["producer"])?;
        for key in [
            "result_frame_uuid",
            "result_subtype",
            "result_status",
            "result_sha256",
            "result_bytes",
            "content_digest",
            "native_output",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ] {
            context[key] = source[key].clone();
        }
    }
    Ok(context)
}

pub(super) fn validate_claude_assistant_result_source(
    request: &Value,
    source: &Value,
    context: &Value,
    producer: &Value,
) -> Result<()> {
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_method",
            "target_input_sha256",
            "target_module_receipt",
            "native_session_id",
            "native_input_id",
            "native_payload_sha256",
            "native_payload_bytes",
            "result_frame_uuid",
            "result_subtype",
            "result_status",
            "result_sha256",
            "result_bytes",
            "content_digest",
            "native_output",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    let subtype = model::text(source, "result_subtype")?;
    let status = model::text(source, "result_status")?;
    let result_frame_uuid = model::text(source, "result_frame_uuid")?;
    let result_sha256 = model::text(source, "result_sha256")?;
    let content_digest = model::text(source, "content_digest")?;
    let result_bytes = source["result_bytes"]
        .as_u64()
        .filter(|bytes| *bytes <= 512_000)
        .ok_or_else(|| Error::invalid("Claude result byte length is invalid"))?;
    if source["kind"] != "claude_assistant_result"
        || source["result_operation_id"] != context["operation_id"]
        || source["result_input_sha256"] != context["result_input_sha256"]
        || source["result_module_receipt"] != context["result_module_receipt"]
        || source["input_operation_id"] != context["target_operation_id"]
        || source["target_method"] != "task.dispatch"
        || source["target_input_sha256"] != context["target_input_sha256"]
        || source["target_module_receipt"] != context["target_module_receipt"]
        || source["native_session_id"] != context["native_session_id"]
        || source["native_session_id"] != request["selector"]["session_id"]
        || source["native_input_id"] != context["native_input_id"]
        || source["native_payload_sha256"] != context["native_payload_sha256"]
        || source["native_payload_bytes"] != context["native_payload_bytes"]
        || !matches!(
            subtype,
            "success"
                | "error_during_execution"
                | "error_max_turns"
                | "error_max_budget_usd"
                | "error_max_structured_output_retries"
        )
        || !matches!(status, "completed" | "failed")
        || result_frame_uuid.len() > 128
        || !valid_sha256(result_sha256)
        || content_digest != format!("sha256:{result_sha256}")
        || source["native_output"] != "claude.assistant.result"
        || source["evidence"] != "exact_claude_sdk_assistant_result"
        || source["native_response_identity"] != "sdk_result_frame"
        || source["execution_complete"] != true
        || source["task_completion"] != "unknown"
        || source["native_replay"] != false
        || producer["native_payload_sha256"] != source["native_payload_sha256"]
        || producer["native_payload_bytes"] != source["native_payload_bytes"]
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Claude assistant result differs from the exact admitted input and SDK result frame",
        ));
    }
    let _ = result_bytes;
    Ok(())
}

fn validate_antigravity_status_source(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    binding: &Value,
    source: &Value,
    context: &Value,
) -> Result<()> {
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_method",
            "target_input_sha256",
            "target_module_receipt",
            "target_operation_state",
            "target_operation_outcome",
            "target_diagnostic_code",
            "native_failure_status",
            "native_session_id",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ],
    )?;
    let target = &context["target_operation_status"];
    if source["kind"] != "antigravity_status"
        || source["result_operation_id"] != context["operation_id"]
        || source["result_input_sha256"] != context["result_input_sha256"]
        || source["input_operation_id"] != context["target_operation_id"]
        || source["target_method"] != context["target_method"]
        || source["target_input_sha256"] != context["target_input_sha256"]
        || source["target_module_receipt"] != target["module_receipt"]
        || source["target_operation_state"] != target["operation_state"]
        || source["target_operation_outcome"] != target["operation_outcome"]
        || source["target_diagnostic_code"] != target["diagnostic_code"]
        || source["native_failure_status"] != target["native_failure_status"]
        || source["native_session_id"] != binding["native_root_id"]
        || source["evidence"] != "store_retained_module_operation_receipt"
        || source["native_response_identity"] != "unavailable"
        || source["execution_complete"] != false
        || source["task_completion"] != "unknown"
        || source["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Antigravity status page differs from the exact retained Operation receipt",
        ));
    }
    let result_outcome = RuntimeOutcome {
        operation_id: model::text(context, "operation_id")?.to_owned(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: binding["native_scope_key"].as_str().map(str::to_owned),
        native_root_id: binding["native_root_id"].as_str().map(str::to_owned),
        turn_id: None,
        native_input_id: None,
        details: json!({"module_receipt":source["result_module_receipt"]}),
    };
    super::runtime::validate_module_receipt_for_operation(
        db,
        binding_id,
        generation,
        binding,
        &result_outcome,
    )?;
    Ok(())
}

fn validate_input_status_source(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    binding: &Value,
    request: &Value,
    source: &Value,
    context: &Value,
) -> Result<()> {
    model::fields(
        source,
        &[
            "kind",
            "result_operation_id",
            "result_input_sha256",
            "result_module_receipt",
            "input_operation_id",
            "target_method",
            "target_input_sha256",
            "target_module_receipt",
            "native_session_id",
            "native_input_id",
            "input_message_sha256",
            "assistant_result_correlation",
            "assistant_result_correlation_reason",
            "evidence",
            "read_method",
            "read_consistency",
            "task_completion",
            "execution_complete",
            "native_replay",
        ],
    )?;
    if source["kind"] != "input_status"
        || source["result_operation_id"] != context["operation_id"]
        || source["result_input_sha256"] != context["result_input_sha256"]
        || source["input_operation_id"] != context["target_operation_id"]
        || source["target_method"] != context["target_method"]
        || source["target_input_sha256"] != context["target_input_sha256"]
        || source["native_session_id"] != binding["native_root_id"]
        || source["native_session_id"] != request["selector"]["session_id"]
        || source["native_input_id"]
            != format!(
                "msg_swarm_{}",
                model::digest(model::text(context, "target_operation_id")?.as_bytes())
            )
        || !valid_prefixed_digest(&source["input_message_sha256"], "sha256:")
        || source["assistant_result_correlation"] != "not_exposed"
        || source["assistant_result_correlation_reason"]
            != "assistant_message_has_no_input_parent_in_public_projection"
        || source["evidence"] != "exact_user_message_projection"
        || source["read_method"] != "session.message.get"
        || source["read_consistency"] != "repeated_equal_projection_not_atomic_snapshot"
        || source["task_completion"] != "unknown"
        || source["execution_complete"] != false
        || source["native_replay"] != false
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "input status page differs from the exact retained Operation context",
        ));
    }
    for (operation_id, receipt_key) in [
        (
            model::text(context, "operation_id")?,
            "result_module_receipt",
        ),
        (
            model::text(context, "target_operation_id")?,
            "target_module_receipt",
        ),
    ] {
        let outcome = RuntimeOutcome {
            operation_id: operation_id.to_owned(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: binding["native_scope_key"].as_str().map(str::to_owned),
            native_root_id: binding["native_root_id"].as_str().map(str::to_owned),
            turn_id: None,
            native_input_id: None,
            details: json!({"module_receipt":source[receipt_key]}),
        };
        super::runtime::validate_module_receipt_for_operation(
            db, binding_id, generation, binding, &outcome,
        )?;
    }
    Ok(())
}

fn valid_prefixed_digest(value: &Value, prefix: &str) -> bool {
    let Some(value) = value.as_str().and_then(|value| value.strip_prefix(prefix)) else {
        return false;
    };
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn record(
    db: &mut Connection,
    p: &Principal,
    artifact: &ArtifactRecord,
) -> Result<Value> {
    let eof = artifact.metadata["eof"]
        .as_bool()
        .ok_or_else(|| Error::invalid("validated result page metadata is missing its EOF flag"))?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation_id = model::text(&artifact.metadata, "operation_id")?;
    let context = prepare(&tx, p, operation_id, &artifact.metadata["source"])?;
    let mut context_keys = vec![
        "binding_id",
        "generation",
        "native_root_id",
        "native_scope_key",
        "selector",
        "requested_offset",
        "requested_length",
    ];
    if artifact.metadata["selector"]["kind"] == "input_status" {
        context_keys.extend([
            "result_input_sha256",
            "target_operation_id",
            "target_method",
            "target_input_sha256",
        ]);
    } else if artifact.metadata["selector"]["kind"] == "antigravity_status" {
        context_keys.extend([
            "result_input_sha256",
            "target_operation_id",
            "target_method",
            "target_input_sha256",
            "target_operation_status",
            "native_session_id",
        ]);
    } else if artifact.metadata["selector"]["kind"] == "claude_assistant_result" {
        context_keys.extend([
            "result_input_sha256",
            "result_module_receipt",
            "target_operation_id",
            "target_method",
            "target_input_sha256",
            "target_module_receipt",
            "target_attempt_id",
            "target_task_id",
            "target_task_revision",
            "native_session_id",
            "native_input_id",
            "native_payload_sha256",
            "native_payload_bytes",
            "result_frame_uuid",
            "result_subtype",
            "result_status",
            "result_sha256",
            "result_bytes",
            "content_digest",
            "native_output",
            "evidence",
            "native_response_identity",
            "execution_complete",
            "task_completion",
            "native_replay",
        ]);
    } else if artifact.metadata["source"]["schema_id"]
        == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
    {
        context_keys.push("normalized_result_origin");
    }
    for key in context_keys {
        if context[key] != artifact.metadata[key] {
            return Err(Error::conflict(
                "result context changed before file registration",
            ));
        }
    }
    if artifact.metadata["selector"]["kind"] == "claude_assistant_result"
        && (artifact.metadata["total_bytes"] != context["result_bytes"]
            || artifact.metadata["source"]["result_bytes"] != context["result_bytes"]
            || artifact.metadata["source"]["content_digest"]
                != format!(
                    "sha256:{}",
                    context["result_sha256"].as_str().unwrap_or_default()
                ))
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Claude result page range differs from the retained full SDK result identity",
        ));
    }
    let op = operations::get_operation(&tx, operation_id)?;
    if op["state"] == "settled" {
        let existing = get(&tx, &artifact.artifact_id)?;
        if op["result"]["details"]["artifact_ref"] != artifact.artifact_id
            || existing.content_digest != artifact.content_digest
            || existing.metadata != artifact.metadata
        {
            return Err(Error::conflict(
                "completed result cannot be replaced by different bytes or provenance",
            ));
        }
        return Ok(json!({"recorded":true,"replayed":true,"artifact_ref":artifact.artifact_id}));
    }
    let sql_length = i64::try_from(artifact.byte_length)
        .map_err(|_| Error::invalid("artifact length exceeds the SQLite integer range"))?;
    let now = model::now_ms()?;
    tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'native_result_page',?3,?4,?5,?6)",
        params![artifact.artifact_id,artifact.relative_path,sql_length,artifact.content_digest,now,model::canonical(&artifact.metadata)?])?;
    let details = json!({"completion_condition":"result_page_persisted","artifact_ref":artifact.artifact_id,
    "byte_length":artifact.byte_length,"page_sha256":artifact.content_digest,
    "source":artifact.metadata["source"],"offset_bytes":artifact.metadata["offset_bytes"],
    "total_bytes":artifact.metadata["total_bytes"],"eof":eof,
    "next_offset_bytes":if eof {None} else {
        artifact.metadata["offset_bytes"].as_u64().and_then(|n| n.checked_add(artifact.byte_length))
    }});
    let outcome = RuntimeOutcome {
        native_input_id: None,
        operation_id: operation_id.to_string(),
        outcome: EffectOutcome::Applied,
        native_root_id: context["native_root_id"].as_str().map(str::to_owned),
        native_scope_key: context["native_scope_key"].as_str().map(str::to_owned),
        turn_id: None,
        details,
    };
    let encoded = model::canonical(&json!(outcome))?;
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1", params![operation_id,encoded,now])?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.result',?6,?7)",
        params![format!("module:{}",p.client_id),format!("result:{operation_id}"),context["binding_id"].as_str(),context["generation"].as_i64(),operation_id,encoded,now])?;
    let page_status = if eof { "completed" } else { "incomplete" };
    let phase = "native_result_page_recorded";
    let occurrence_id = format!("operation:{operation_id}:{phase}");
    super::insert_safe_system_event(
        &tx,
        "controller:runtime",
        &format!("result-page:{operation_id}"),
        Some(operation_id),
        "native.result.available",
        phase,
        page_status,
        Some(&occurrence_id),
        None,
        None,
        now,
    )?;
    tx.commit()?;
    Ok(json!({"recorded":true,"artifact_ref":artifact.artifact_id}))
}

pub(super) fn get(db: &Connection, id: &str) -> Result<ArtifactRecord> {
    let row: Option<(String,String,i64,String,String)> = db.query_row(
        "SELECT kind,relative_path,byte_length,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1 AND kind IN ('native_result_page','native_result','task_submission','source_snapshot','check_result','check_output','script_bundle','script_result','script_output')",
        [id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let (kind, relative_path, byte_length, content_digest, metadata) =
        row.ok_or_else(|| Error::new("NOT_FOUND", "result artifact is not registered"))?;
    Ok(ArtifactRecord {
        kind,
        artifact_id: id.to_string(),
        relative_path,
        byte_length: u64::try_from(byte_length)
            .map_err(|_| Error::new("ARTIFACT_DAMAGED", "negative stored artifact length"))?,
        content_digest,
        metadata: serde_json::from_str(&metadata)?,
    })
}
/// Closed projection for one already-authorized artifact. This function never
/// reopens identity and never widens a resolved grant.
pub(super) fn project_authorized(authorized: &super::object_scope::AuthorizedArtifact) -> Value {
    json!({"artifact_id":authorized.record.artifact_id,"kind":authorized.record.kind,
        "byte_length":authorized.record.byte_length,
        "content_digest":authorized.record.content_digest,
        "metadata":project_metadata(&authorized.record),
        "grant":{"level":authorized.grant.level.as_str(),
            "basis":authorized.grant.basis.as_str()}})
}

/// Per-kind metadata ceiling for authorized artifact projections. Stored
/// metadata remains private provenance; arbitrary keys and nested producer data
/// are never copied into a public artifact result.
pub(super) fn project_metadata(record: &ArtifactRecord) -> Value {
    let fields: &[&str] = match record.kind.as_str() {
        "task_submission" => &[
            "task_id",
            "attempt_id",
            "task_revision",
            "candidate_ref",
            "candidate_sha256",
            "candidate_byte_length",
            "candidate_kind",
        ],
        "source_snapshot" => &[
            "task_id",
            "attempt_id",
            "task_revision",
            "commit",
            "tree",
            "coverage",
            "file_count",
        ],
        "check_result" => &["check_id", "candidate_ref", "state"],
        "check_output" => &["check_id", "candidate_ref", "state", "stream"],
        "native_result_page" => &[
            "offset_bytes",
            "byte_length",
            "total_bytes",
            "eof",
            "media_type",
            "page_sha256",
        ],
        "native_result" => &[
            "coverage",
            "byte_length",
            "sha256",
            "part_count",
            "expected_sha256",
        ],
        "script_bundle" | "script_result" | "script_output" => {
            &["script_id", "revision", "artifact_role"]
        }
        _ => &[],
    };
    let Some(metadata) = record.metadata.as_object() else {
        return json!({});
    };
    let mut projected = serde_json::Map::new();
    for field in fields {
        if let Some(value) = metadata.get(*field) {
            projected.insert((*field).to_owned(), value.clone());
        }
    }
    Value::Object(projected)
}

/// Object-authorized artifact metadata read (`artifact.get`).
pub(super) fn describe(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    if p.role == Role::Module {
        return Err(Error::new(
            "FORBIDDEN",
            "module reports results only for its binding",
        ));
    }
    model::fields(v, &["artifact_id"])?;
    let record = get(db, model::text(v, "artifact_id")?)?;
    let Some(grant) = super::object_scope::resolve_artifact_read(
        db,
        p,
        &record,
        super::object_scope::ArtifactReadLevel::Metadata,
    )?
    else {
        return Err(super::object_scope::unauthorized_artifact());
    };
    Ok(project_authorized(
        &super::object_scope::AuthorizedArtifact { record, grant },
    ))
}
