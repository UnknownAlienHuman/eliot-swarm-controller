//! Bounded Antigravity Operation status pages.
//!
//! Antigravity's public result event has no native request/assistant-message
//! identity and this adapter does not retain its response text. This module
//! publishes only the exact terminal Operation receipt supplied by Store.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{NormalizedResultOriginContext, RuntimeCommand},
};

use crate::{module_receipt, wire::OperationIdentity};

const MAX_PAGE_BYTES: usize = 65_536;

pub fn build(command: &RuntimeCommand) -> Result<Value> {
    let identity = OperationIdentity::try_from(command).map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status command identity is invalid",
        )
    })?;
    if command.method != "agent.result"
        || command.route["runtime"].as_str() != Some("antigravity")
        || command.input["selector"]["kind"] != "antigravity_status"
    {
        return Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "only Antigravity Operation status pages are supported",
        ));
    }

    let session_id = command.input["selector"]["session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "status selector has no native session identity",
            )
        })?;
    if command.native_root_id.as_deref() != Some(session_id) {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status selector differs from the admitted native session",
        ));
    }

    let target_id = identity.target_operation_id.as_deref().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status command has no exact target Operation",
        )
    })?;
    let target_digest = identity.target_input_sha256.as_deref().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status command has no exact target request digest",
        )
    })?;
    let target = &command.input["target_operation_status"];
    let target_method = target["method"].as_str().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store omitted the target Operation method",
        )
    })?;
    let target_state = target["operation_state"].as_str().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store omitted the target Operation state",
        )
    })?;
    let target_outcome = target["operation_outcome"].as_str().ok_or_else(|| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store omitted the target Operation outcome",
        )
    })?;
    let state_matches_outcome = matches!(
        (target_state, target_outcome),
        ("settled", "applied") | ("rejected", "rejected")
    );
    if target["operation_id"].as_str() != Some(target_id)
        || target["target_input_sha256"].as_str() != Some(target_digest)
        || !matches!(target_method, "task.dispatch" | "agent.send")
        || !state_matches_outcome
        || target.as_object().is_none_or(|fields| fields.len() != 8)
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store target snapshot differs from the exact terminal Operation",
        ));
    }
    let target_receipt = module_receipt::for_result_target(command)?;
    let result_receipt = module_receipt::for_command(command)?;
    if identity.operation_id != result_receipt.operation_id
        || target["module_receipt"] != serde_json::to_value(&target_receipt)?
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status page receipt identity differs from the Store snapshot",
        ));
    }

    let diagnostic_code = checked_optional_code(&target["diagnostic_code"])?;
    let native_failure_status = checked_optional_failure(&target["native_failure_status"])?;
    if !native_failure_status.is_null() && target_outcome != "rejected" {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "native failure status is inconsistent with the terminal Operation",
        ));
    }

    let source = json!({
        "kind":"antigravity_status",
        "result_operation_id":identity.operation_id,
        "result_input_sha256":result_receipt.input_sha256,
        "result_module_receipt":result_receipt,
        "input_operation_id":target_id,
        "target_method":target_method,
        "target_input_sha256":target_digest,
        "target_module_receipt":target_receipt,
        "target_operation_state":target_state,
        "target_operation_outcome":target_outcome,
        "target_diagnostic_code":diagnostic_code,
        "native_failure_status":native_failure_status,
        "native_session_id":session_id,
        "evidence":"store_retained_module_operation_receipt",
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false,
    });
    if serde_json::to_vec(&source)?.len() > 8_192 {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "status page provenance exceeds its bounded source envelope",
        ));
    }

    let body = status_body(&source);
    let body_bytes = serde_json::to_vec(&body)?;
    let offset = command.input["offset_bytes"].as_u64().unwrap_or(0);
    let requested_length = command.input["length_bytes"]
        .as_u64()
        .unwrap_or(MAX_PAGE_BYTES as u64)
        .min(MAX_PAGE_BYTES as u64);
    let total = body_bytes.len() as u64;
    if offset > total || (requested_length == 0 && offset < total) {
        return Err(Error::new(
            "RESULT_RANGE_INVALID",
            "requested status page range is outside the retained status document",
        ));
    }
    let end = offset.saturating_add(requested_length).min(total);
    let start = usize::try_from(offset)
        .map_err(|_| Error::new("RESULT_RANGE_INVALID", "status page offset is too large"))?;
    let end = usize::try_from(end)
        .map_err(|_| Error::new("RESULT_RANGE_INVALID", "status page range is too large"))?;
    let selected = &body_bytes[start..end];
    let page = json!({
        "source":source,
        "offset_bytes":offset,
        "byte_length":selected.len(),
        "total_bytes":total,
        "eof":end as u64 == total,
        "media_type":"application/json; charset=utf-8",
        "content_base64":encode_base64(selected),
        "page_sha256":sha256_hex(selected),
    });
    Ok(json!({
        "operation_id":identity.operation_id,
        "page":page,
    }))
}

/// Validate a Store-admitted normalized result request, then report the
/// native protocol's bounded body capability. Antigravity's stream result
/// has no request, item, or turn parent; conversation plus this adapter's
/// local ordinal cannot prove that response text belongs to this input.
pub fn build_normalized(command: &RuntimeCommand) -> Result<Value> {
    if command.method != "agent.result"
        || command.route["runtime"].as_str() != Some("antigravity")
        || !command.input["normalized_result_origin"].is_object()
    {
        return Err(Error::new(
            "RESULT_SELECTOR_UNSUPPORTED",
            "normalized Antigravity result origin was not admitted",
        ));
    }
    let identity = OperationIdentity::try_from(command).map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized result command identity is invalid",
        )
    })?;
    let origin: NormalizedResultOriginContext = serde_json::from_value(
        command.input["normalized_result_origin"].clone(),
    )
    .map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized result origin is malformed",
        )
    })?;
    origin.validate().map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized result origin is invalid",
        )
    })?;
    let target_id = command.input["selector"]["input_operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "normalized result selector has no exact input Operation",
            )
        })?;
    let session_id = command.input["selector"]["session_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "normalized result selector has no native session identity",
            )
        })?;
    if command.native_root_id.as_deref() != Some(session_id)
        || identity.target_operation_id.as_deref() != Some(target_id)
        || identity.target_input_sha256.as_deref() != Some(origin.target_input_sha256.as_str())
        || origin.binding_id != command.binding_id
        || origin.binding_generation != command.generation
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized result origin differs from the admitted binding or input",
        ));
    }
    Err(Error::new(
        "RESULT_BODY_UNAVAILABLE",
        "Antigravity exposes response text without a native request, item, or turn parent; conversation and local ordinal do not prove input causality",
    ))
}

fn status_body(source: &Value) -> Value {
    json!({
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
    })
}

fn checked_optional_code(value: &Value) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(code)
            if !code.is_empty()
                && code.len() <= 64
                && code.bytes().all(|byte| {
                    byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_'
                }) =>
        {
            Ok(json!(code))
        }
        _ => Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store diagnostic code is outside the bounded closed form",
        )),
    }
}

fn checked_optional_failure(value: &Value) -> Result<Value> {
    match value.as_str() {
        None if value.is_null() => Ok(Value::Null),
        Some(status @ ("ERROR" | "CANCELED" | "INTERRUPTED")) => Ok(json!(status)),
        _ => Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "Store native failure status is not a supported terminal failure",
        )),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let (chunks, remainder) = bytes.as_chunks::<3>();
    for chunk in chunks {
        encoded.push(ALPHABET[(chunk[0] >> 2) as usize] as char);
        encoded.push(ALPHABET[(((chunk[0] & 0x03) << 4) | (chunk[1] >> 4)) as usize] as char);
        encoded.push(ALPHABET[(((chunk[1] & 0x0f) << 2) | (chunk[2] >> 6)) as usize] as char);
        encoded.push(ALPHABET[(chunk[2] & 0x3f) as usize] as char);
    }
    match remainder {
        [first] => {
            encoded.push(ALPHABET[(*first >> 2) as usize] as char);
            encoded.push(ALPHABET[(((*first) & 0x03) << 4) as usize] as char);
            encoded.push_str("==");
        }
        [first, second] => {
            encoded.push(ALPHABET[(*first >> 2) as usize] as char);
            encoded.push(ALPHABET[((((*first) & 0x03) << 4) | (*second >> 4)) as usize] as char);
            encoded.push(ALPHABET[(((*second) & 0x0f) << 2) as usize] as char);
            encoded.push('=');
        }
        [] => {}
        _ => unreachable!("as_chunks remainder has at most two bytes"),
    }
    encoded
}
