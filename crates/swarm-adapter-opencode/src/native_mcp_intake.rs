//! Typed admission for one Store-enriched native MCP RuntimeCommand.
//!
//! The shared [`NativeMcpCommand`] is the outer command identity.  The
//! `effect` member is a private, host-enriched handoff for the sibling native
//! executor; it is removed before DTO deserialization and is never copied to
//! a RuntimeOutcome.  Keeping the two layers explicit lets the executor keep
//! its existing prepared request/challenge validation while the adapter binds
//! that request to the authenticated operation before connecting to OpenCode.

use crate::module_runtime;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    native_mcp::{NativeMcpCommand, NativeMcpPhase},
    runtime::RuntimeCommand,
};

const EFFECT_FIELD: &str = "effect";
const MAX_EFFECT_BYTES: usize = 1_048_576;

/// The effect owner receives a command whose input is the exact private
/// effect envelope.  Its operation/binding/generation fields remain those of
/// the authenticated outer command, so the existing executor cannot silently
/// retarget a request while parsing its prepared HTTP body.
pub(crate) struct AdmittedNativeMcp {
    pub(crate) effect_command: RuntimeCommand,
}

/// Validate the descriptor opt-in, bind the typed DTO to the outer command,
/// and verify the phase-specific private artifact before any native client is
/// opened or effect is attempted.
pub(crate) fn admit(
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<AdmittedNativeMcp> {
    if !module_runtime::native_mcp_enabled(claim) {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY",
            "native MCP command requires the exact expanded descriptor claim",
        ));
    }
    let input = command.input.as_object().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command input must be an object",
        )
    })?;
    let effect = input.get(EFFECT_FIELD).cloned().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command has no private effect handoff",
        )
    })?;
    bounded_json(&effect)?;

    // Deserialize the DTO from the direct command object.  Only the one
    // host-enriched private field is removed; all DTO fields remain subject
    // to deny_unknown_fields and the shared schema's exact validation.
    let mut dto_object = input.clone();
    dto_object.remove(EFFECT_FIELD);
    let dto_value = Value::Object(dto_object);
    let native_command: NativeMcpCommand = serde_json::from_value(dto_value).map_err(|_| {
        Error::new(
            "NATIVE_MCP_ADMISSION",
            "native MCP command does not match the shared DTO",
        )
    })?;
    native_command.validate_against(command).map_err(|_| {
        Error::new(
            "NATIVE_MCP_IDENTITY",
            "native MCP command differs from its authenticated RuntimeCommand",
        )
    })?;
    validate_effect(&native_command, &effect)?;

    let mut effect_command = command.clone();
    effect_command.input = effect;
    Ok(AdmittedNativeMcp { effect_command })
}

fn validate_effect(command: &NativeMcpCommand, effect: &Value) -> Result<()> {
    let object = effect.as_object().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect handoff must be an object",
        )
    })?;
    if object.get("schema_version") != Some(&Value::from(1_u64))
        || object.get("kind").and_then(Value::as_str) != Some("swarm.native_mcp_command")
    {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect handoff schema is not swarm.native_mcp_command@1",
        ));
    }
    let expected_action = match command.phase {
        NativeMcpPhase::Install => "install",
        NativeMcpPhase::Observe => "observe",
        NativeMcpPhase::Arm => "arm",
        NativeMcpPhase::Read => "read",
    };
    if object.get("action").and_then(Value::as_str) != Some(expected_action) {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP effect action differs from the admitted phase",
        ));
    }

    let (field, reference) = match command.phase {
        NativeMcpPhase::Install | NativeMcpPhase::Observe => {
            ("prepared", command.prepared_command.as_ref())
        }
        NativeMcpPhase::Arm | NativeMcpPhase::Read => ("challenge", command.challenge.as_ref()),
    };
    let reference = reference.ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP phase has no matching protected artifact reference",
        )
    })?;
    let artifact = object.get(field).ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP effect has no matching private artifact",
        )
    })?;
    let digest = digest_json(artifact)?;
    if digest != reference.sha256.as_str() {
        return Err(Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP private artifact differs from its admitted SHA-256",
        ));
    }
    Ok(())
}

fn bounded_json(value: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|_| {
        Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP private effect is not valid JSON",
        )
    })?;
    if bytes.len() > MAX_EFFECT_BYTES {
        return Err(Error::new(
            "NATIVE_MCP_EFFECT",
            "native MCP private effect exceeds its handoff bound",
        ));
    }
    Ok(())
}

fn digest_json(value: &Value) -> Result<String> {
    let canonical = canonical_value(value);
    let bytes = serde_json::to_vec(&canonical).map_err(|_| {
        Error::new(
            "NATIVE_MCP_ARTIFACT",
            "native MCP private artifact cannot be canonicalized",
        )
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut ordered = Map::new();
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for key in keys {
                ordered.insert(key.clone(), canonical_value(&object[key]));
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_value).collect()),
        value => value.clone(),
    }
}
