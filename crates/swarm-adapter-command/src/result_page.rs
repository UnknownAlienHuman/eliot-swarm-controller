//! Bounded Command dispatch status pages.
//!
//! Command's JSON result has no native assistant-message ID. The page exposes
//! only Store-retained terminal status, never response text or task completion.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{ModuleReceiptIdentity, RuntimeCommand},
};

const MAX_PAGE_BYTES: usize = 65_536;
const MAX_SOURCE_BYTES: usize = 8_192;

pub fn build(command: &RuntimeCommand, claim: &ModuleContractClaim) -> Result<Value> {
    if command.method != "agent.result"
        || command.route["runtime"] != "command"
        || command.route["module_artifact_id"] != "eliot-command.rust-headless.1"
        || command.input["selector"]["kind"] != "command_status"
    {
        return Err(invalid("unsupported Command result selector"));
    }
    if claim.module_id.as_str() != "runtime.command"
        || claim.artifact.artifact_id.as_str() != "eliot-command.rust-headless.1"
        || claim.artifact.version.as_str() != "3"
        || !claim
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "agent.result")
    {
        return Err(invalid("Command result capability is not negotiated"));
    }
    let result_input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| is_sha256(digest))
        .ok_or_else(|| invalid("Store omitted the exact result Operation digest"))?;
    let target_id = command.input["selector"]["input_operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("status selector has no exact target Operation"))?;
    let target_input_sha256 = command
        .target_input_sha256
        .as_deref()
        .filter(|digest| is_sha256(digest))
        .ok_or_else(|| invalid("Store omitted the exact target Operation digest"))?;
    let target = &command.input["target_operation_status"];
    validate_target(
        target,
        target_id,
        target_input_sha256,
        claim,
        &command.binding_id,
        command.generation,
    )?;

    let result_receipt = receipt(command, claim, &command.operation_id, result_input_sha256)?;
    let target_receipt = target["module_receipt"].clone();
    let source = json!({
        "kind":"command_status",
        "result_operation_id":command.operation_id,
        "result_input_sha256":result_input_sha256,
        "result_module_receipt":result_receipt,
        "input_operation_id":target_id,
        "target_input_sha256":target_input_sha256,
        "target_module_receipt":target_receipt,
        "target_operation_status":target,
        "evidence":target_evidence(target),
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false
    });
    if serde_json::to_vec(&source)?.len() > MAX_SOURCE_BYTES {
        return Err(invalid("Command status provenance exceeds its size limit"));
    }
    let bytes = status_body(target)?;
    let offset = command.input["offset_bytes"].as_u64().unwrap_or(0);
    let requested = command.input["length_bytes"]
        .as_u64()
        .unwrap_or(MAX_PAGE_BYTES as u64)
        .min(MAX_PAGE_BYTES as u64);
    let total = bytes.len() as u64;
    if offset > total || (requested == 0 && offset < total) {
        return Err(Error::new(
            "RESULT_RANGE_INVALID",
            "requested Command status page range is invalid",
        ));
    }
    let end = offset.checked_add(requested).unwrap_or(u64::MAX).min(total);
    let start = usize::try_from(offset)
        .map_err(|_| Error::new("RESULT_RANGE_INVALID", "page offset is too large"))?;
    let end = usize::try_from(end)
        .map_err(|_| Error::new("RESULT_RANGE_INVALID", "page end is too large"))?;
    let selected = &bytes[start..end];
    let params = json!({
        "operation_id":command.operation_id,
        "page":{
            "source":source,
            "offset_bytes":offset,
            "byte_length":selected.len(),
            "total_bytes":total,
            "eof":end == total,
            "media_type":"application/json; charset=utf-8",
            "content_base64":encode_base64(selected),
            "page_sha256":sha256_hex(selected)
        }
    });
    validate_saved(&params, claim, &command.binding_id, command.generation)?;
    Ok(params)
}

/// Validate a persisted page before an acknowledgement retry. The bytes and
/// source must be derivable from its exact stored target snapshot and receipts.
pub fn validate_saved(
    params: &Value,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
) -> Result<()> {
    fields(params, &["operation_id", "page"])?;
    let operation_id = text(params, "operation_id")?;
    let page = params
        .get("page")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("saved Command result page is malformed"))?;
    if page.len() != 8 {
        return Err(invalid("saved Command result page has unknown fields"));
    }
    let page = Value::Object(page.clone());
    let source = &page["source"];
    fields(
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
    let target_id = text(source, "input_operation_id")?;
    let target_digest = text(source, "target_input_sha256")?;
    let result_digest = text(source, "result_input_sha256")?;
    if source["kind"] != "command_status"
        || source["result_operation_id"] != operation_id
        || source["target_operation_status"]["operation_id"] != target_id
        || source["target_operation_status"]["input_sha256"] != target_digest
        || source["target_operation_status"]["module_receipt"] != source["target_module_receipt"]
        || source["evidence"] != target_evidence(&source["target_operation_status"])
        || source["native_response_identity"] != "unavailable"
        || source["execution_complete"] != false
        || source["task_completion"] != "unknown"
        || source["native_replay"] != false
    {
        return Err(invalid("saved Command result provenance is inconsistent"));
    }
    let result_receipt: ModuleReceiptIdentity =
        serde_json::from_value(source["result_module_receipt"].clone())
            .map_err(|_| invalid("saved result receipt is malformed"))?;
    if result_receipt.operation_id != operation_id
        || result_receipt.input_sha256 != result_digest
        || result_receipt.module_id != claim.module_id
        || result_receipt.artifact != claim.artifact
        || result_receipt.protocol != claim.protocol
        || result_receipt.binding_id != binding_id
        || result_receipt.binding_generation != generation
        || claim.artifact.version.as_str() != "3"
        || !claim
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == "agent.result")
    {
        return Err(invalid("saved result receipt differs from this binding"));
    }
    result_receipt
        .validate()
        .map_err(|_| invalid("saved result receipt is invalid"))?;
    validate_target(
        &source["target_operation_status"],
        target_id,
        target_digest,
        claim,
        binding_id,
        generation,
    )?;

    let bytes = status_body(&source["target_operation_status"])?;
    let offset = page["offset_bytes"]
        .as_u64()
        .ok_or_else(|| invalid("saved page offset is invalid"))?;
    let byte_length = page["byte_length"]
        .as_u64()
        .ok_or_else(|| invalid("saved page length is invalid"))?;
    let total = page["total_bytes"]
        .as_u64()
        .ok_or_else(|| invalid("saved page total is invalid"))?;
    let end = offset
        .checked_add(byte_length)
        .ok_or_else(|| invalid("saved page range overflows"))?;
    let start = usize::try_from(offset).map_err(|_| invalid("saved page offset is too large"))?;
    let end_index = usize::try_from(end).map_err(|_| invalid("saved page end is too large"))?;
    if total != bytes.len() as u64 || end > total || end_index > bytes.len() {
        return Err(invalid("saved page range differs from its status body"));
    }
    let selected = &bytes[start..end_index];
    if page["eof"] != (end == total)
        || page["media_type"] != "application/json; charset=utf-8"
        || page["content_base64"] != encode_base64(selected)
        || page["page_sha256"] != sha256_hex(selected)
        || byte_length != selected.len() as u64
    {
        return Err(invalid("saved page bytes differ from its status body"));
    }
    Ok(())
}

fn validate_target(
    target: &Value,
    target_id: &str,
    target_digest: &str,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
) -> Result<()> {
    fields(
        target,
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
    let receipt = if target["module_receipt"].is_null() {
        None
    } else {
        Some(
            serde_json::from_value::<ModuleReceiptIdentity>(target["module_receipt"].clone())
                .map_err(|_| invalid("Store target receipt is malformed"))?,
        )
    };
    if target["schema_version"] != 1
        || target["operation_id"] != target_id
        || target["method"] != "task.dispatch"
        || !matches!(target["operation_state"].as_str(), Some("settled" | "rejected" | "outcome_unknown"))
        || !matches!(target["operation_outcome"].as_str(), Some("applied" | "rejected" | "unknown"))
        || target["input_sha256"] != target_digest
        || target["native_response_identity"] != "unavailable"
        || target["execution_complete"] != false
        || target["task_completion"] != "unknown"
        || target["native_replay"] != false
        || receipt.as_ref().is_some_and(|receipt| {
            receipt.operation_id != target_id
                || receipt.input_sha256 != target_digest
                || receipt.binding_id != binding_id
                || receipt.binding_generation != generation
                || receipt.module_id != claim.module_id
                || receipt.artifact != claim.artifact
                || receipt.protocol != claim.protocol
        })
    {
        return Err(invalid(
            "Store target snapshot differs from exact dispatch identity",
        ));
    }
    if let Some(receipt) = &receipt {
        receipt
            .validate()
            .map_err(|_| invalid("Store target receipt is invalid"))?;
    } else if target["operation_state"] != "outcome_unknown"
        || target["operation_outcome"] != "unknown"
    {
        return Err(invalid("terminal Command status is missing its module receipt"));
    }
    let diagnostic = &target["diagnostic_code"];
    if !diagnostic.is_null()
        && !diagnostic.as_str().is_some_and(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    {
        return Err(invalid(
            "Store target diagnostic is outside the closed form",
        ));
    }
    if target["operation_state"] == "outcome_unknown" {
        if target["operation_outcome"] != "unknown"
            || !matches!(
                target["completion_condition"].as_str(),
                Some("native_result_unconfirmed" | "module_outcome_not_retained")
            )
            || !target["exit_code"].is_null()
            || !target["result_subtype"].is_null()
            || (!target["timed_out"].is_null() && !target["timed_out"].is_boolean())
            || (!target["signal_observed"].is_null()
                && !target["signal_observed"].is_boolean())
            || (target["completion_condition"] == "module_outcome_not_retained"
                && !target["module_receipt"].is_null())
            || (target["completion_condition"] == "native_result_unconfirmed"
                && target["module_receipt"].is_null())
        {
            return Err(invalid("unknown Command status contains incompatible retained facts"));
        }
        return Ok(());
    }
    if target["timed_out"] != false || target["signal_observed"] != false {
        return Err(invalid("terminal Command status has inconsistent process facts"));
    }
    match (
        target["operation_state"].as_str(),
        target["operation_outcome"].as_str(),
        target["completion_condition"].as_str(),
        target["result_subtype"].as_str(),
        target["exit_code"].as_i64(),
    ) {
        (
            Some("settled"),
            Some("applied"),
            Some("native_result_observed"),
            Some("success"),
            Some(0),
        ) => {}
        (
            Some("rejected"),
            Some("rejected"),
            Some("native_result_observed"),
            Some("error"),
            Some(1 | 3 | 4 | 5 | 6 | 7 | 9 | 10 | 130),
        ) => {}
        (
            Some("rejected"),
            Some("rejected"),
            Some("native_result_observed"),
            Some("max_turns"),
            Some(8),
        ) => {}
        (Some("rejected"), Some("rejected"), Some("executor_launch_rejected"), None, None) => {}
        _ => {
            return Err(invalid(
                "Command status is not an exact terminal disposition",
            ));
        }
    }
    Ok(())
}

fn status_body(target: &Value) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&json!({
        "schema_version":1,
        "operation_id":target["operation_id"],
        "method":target["method"],
        "operation_state":target["operation_state"],
        "operation_outcome":target["operation_outcome"],
        "completion_condition":target["completion_condition"],
        "diagnostic_code":target["diagnostic_code"],
        "result_subtype":target["result_subtype"],
        "exit_code":target["exit_code"],
        "timed_out":target["timed_out"],
        "signal_observed":target["signal_observed"],
        "input_sha256":target["input_sha256"],
        "native_response_identity":"unavailable",
        "execution_complete":false,
        "task_completion":"unknown",
        "native_replay":false
    }))?)
}

fn target_evidence(target: &Value) -> &'static str {
    if target["operation_state"] == "outcome_unknown" {
        if target["module_receipt"].is_null() {
            "store_retained_operation_state_only"
        } else {
            "store_retained_module_reported_unknown_outcome"
        }
    } else {
        "store_retained_module_reported_operation_receipt"
    }
}

fn receipt(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
    operation_id: &str,
    input_sha256: &str,
) -> Result<ModuleReceiptIdentity> {
    let identity = ModuleReceiptIdentity {
        schema_version: 1,
        module_id: claim.module_id.clone(),
        artifact: claim.artifact.clone(),
        protocol: claim.protocol,
        binding_id: command.binding_id.clone(),
        binding_generation: command.generation,
        operation_id: operation_id.to_owned(),
        input_sha256: input_sha256.to_owned(),
    };
    identity
        .validate()
        .map_err(|_| invalid("Command result receipt identity is invalid"))?;
    Ok(identity)
}

fn fields(value: &Value, expected: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("Command result object is malformed"))?;
    if object.len() != expected.len() || expected.iter().any(|field| !object.contains_key(*field)) {
        return Err(invalid(
            "Command result object has missing or unknown fields",
        ));
    }
    Ok(())
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid("Command result identity is missing"))
}

fn invalid(message: &str) -> Error {
    Error::new("RESULT_PROVENANCE_INVALID", message)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    let mut chunks = bytes.chunks_exact(3);
    for chunk in &mut chunks {
        encoded.push(ALPHABET[(chunk[0] >> 2) as usize] as char);
        encoded.push(ALPHABET[(((chunk[0] & 0x03) << 4) | (chunk[1] >> 4)) as usize] as char);
        encoded.push(ALPHABET[(((chunk[1] & 0x0f) << 2) | (chunk[2] >> 6)) as usize] as char);
        encoded.push(ALPHABET[(chunk[2] & 0x3f) as usize] as char);
    }
    match chunks.remainder() {
        [first] => {
            encoded.push(ALPHABET[(*first >> 2) as usize] as char);
            encoded.push(ALPHABET[(((*first) & 0x03) << 4) as usize] as char);
            encoded.push_str("==");
        }
        [first, second] => {
            encoded.push(ALPHABET[(*first >> 2) as usize] as char);
            encoded.push(ALPHABET[(((*first & 0x03) << 4) | (*second >> 4)) as usize] as char);
            encoded.push(ALPHABET[(((*second) & 0x0f) << 2) as usize] as char);
            encoded.push('=');
        }
        [] => {}
        _ => unreachable!("three-byte chunks have at most two remainder bytes"),
    }
    encoded
}
