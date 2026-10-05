//! Receipts are derived only from the authenticated command and the Store's
//! canonical digest of the retained original Operation.

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
        .filter(|value| is_sha256(value))
        .ok_or_else(|| {
            Error::new(
                "HOST_COMMAND_IDENTITY",
                "module command lacks the retained Operation digest",
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
            "command differs from the authenticated module descriptor",
        ));
    }
    let receipt = ModuleReceiptIdentity {
        schema_version: 1,
        module_id: claim.module_id.clone(),
        artifact: claim.artifact.clone(),
        protocol: claim.protocol,
        binding_id: command.binding_id.clone(),
        binding_generation: command.generation,
        operation_id: command.operation_id.clone(),
        input_sha256: input_sha256.to_owned(),
    };
    receipt
        .validate()
        .map_err(|_| Error::new("ADAPTER_RECEIPT", "module receipt identity is invalid"))?;
    Ok(receipt)
}

pub fn insert(details: &mut Value, receipt: &ModuleReceiptIdentity) -> Result<()> {
    receipt
        .validate()
        .map_err(|_| Error::new("ADAPTER_RECEIPT", "module receipt is invalid"))?;
    let object = details
        .as_object_mut()
        .ok_or_else(|| Error::new("ADAPTER_RECEIPT", "outcome details must be an object"))?;
    object.insert("module_receipt".to_owned(), serde_json::to_value(receipt)?);
    Ok(())
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
