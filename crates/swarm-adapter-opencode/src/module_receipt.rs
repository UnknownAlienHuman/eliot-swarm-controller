//! Host-derived operation receipts for the standalone OpenCode adapter.
//!
//! The adapter consumes the SHA-256 supplied from the retained original
//! Operation request. It never reconstructs the digest from enriched input or
//! native prompt text.

use crate::journal::OperationIntent;
use serde_json::Value;
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{ModuleReceiptIdentity, RuntimeCommand},
};

pub fn for_command(
    claim: &ModuleContractClaim,
    command: &RuntimeCommand,
) -> Result<ModuleReceiptIdentity> {
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| is_sha256(digest))
        .ok_or_else(|| {
            Error::new(
                "HOST_COMMAND_IDENTITY",
                "module command lacks the canonical retained Operation digest",
            )
        })?;
    if command.binding_id.trim().is_empty()
        || command.generation < 1
        || command.operation_id.trim().is_empty()
        || command.route["runtime"] != "module"
        || command.route["module_artifact_id"] != claim.artifact.artifact_id.as_str()
    {
        return Err(Error::new(
            "HOST_COMMAND_IDENTITY",
            "module command differs from its authenticated descriptor identity",
        ));
    }
    identity(
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )
}

/// A readback operation emits the target Operation receipt independently from
/// its own receipt. The digest comes from the host's exact target Operation
/// snapshot and must match the durable target intent before readback proceeds.
pub fn for_target_intent(
    claim: &ModuleContractClaim,
    target: &OperationIntent,
    target_operation_id: &str,
    target_input_sha256: Option<&str>,
    binding_id: &str,
    generation: i64,
) -> Result<ModuleReceiptIdentity> {
    let digest = target_input_sha256
        .filter(|digest| is_sha256(digest))
        .ok_or_else(|| {
            Error::new(
                "HOST_COMMAND_IDENTITY",
                "readback command lacks the canonical target Operation digest",
            )
        })?;
    let saved = &target.module_receipt;
    if target.operation_id != target_operation_id
        || saved.operation_id != target_operation_id
        || saved.input_sha256 != digest
        || target.binding_id != binding_id
        || target.generation != generation
        || saved.binding_id != binding_id
        || saved.binding_generation != generation
        || saved.module_id != claim.module_id
        || saved.artifact != claim.artifact
        || saved.protocol != claim.protocol
    {
        return Err(Error::new(
            "NATIVE_IDENTITY_MISMATCH",
            "saved target intent differs from the exact reconcile target identity",
        ));
    }
    saved
        .validate()
        .map_err(|_| Error::new("ADAPTER_RECEIPT", "saved target receipt is invalid"))?;
    Ok(saved.clone())
}

pub fn insert_into_details(details: &mut Value, identity: &ModuleReceiptIdentity) -> Result<()> {
    identity
        .validate()
        .map_err(|_| Error::new("ADAPTER_RECEIPT", "module receipt identity is invalid"))?;
    let object = details
        .as_object_mut()
        .ok_or_else(|| Error::new("ADAPTER_RECEIPT", "outcome details must be a JSON object"))?;
    object.insert("module_receipt".to_owned(), serde_json::to_value(identity)?);
    Ok(())
}

fn identity(
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<ModuleReceiptIdentity> {
    let identity = ModuleReceiptIdentity {
        schema_version: 1,
        module_id: claim.module_id.clone(),
        artifact: claim.artifact.clone(),
        protocol: claim.protocol,
        binding_id: binding_id.to_owned(),
        binding_generation: generation,
        operation_id: operation_id.to_owned(),
        input_sha256: input_sha256.to_owned(),
    };
    identity
        .validate()
        .map_err(|_| Error::new("ADAPTER_RECEIPT", "module receipt identity is invalid"))?;
    Ok(identity)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
