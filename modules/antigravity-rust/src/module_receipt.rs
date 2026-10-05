//! Generic immutable receipt identity used by the shared Store validator.

use serde_json::Value;
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome},
};

use crate::contract;

pub fn for_operation(
    command: &RuntimeCommand,
    operation_id: &str,
    input_sha256: &str,
) -> Result<ModuleReceiptIdentity> {
    if command.binding_id.trim().is_empty()
        || command.route["runtime"].as_str() != Some("antigravity")
        || command.route["module_artifact_id"].as_str() != Some(crate::wire::ARTIFACT_ID)
    {
        return Err(Error::new(
            "MODULE_RECEIPT_IDENTITY_INVALID",
            "command and canonical operation digest do not match this artifact",
        ));
    }
    for_identity(
        &command.binding_id,
        command.generation,
        operation_id,
        input_sha256,
    )
}

pub fn for_identity(
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<ModuleReceiptIdentity> {
    let claim = contract::claim()?;
    if binding_id.trim().is_empty()
        || generation <= 0
        || operation_id.trim().is_empty()
        || !is_lower_sha256(input_sha256)
    {
        return Err(Error::new(
            "MODULE_RECEIPT_IDENTITY_INVALID",
            "binding, Operation and canonical digest are required",
        ));
    }
    let identity = ModuleReceiptIdentity {
        schema_version: 1,
        module_id: claim.module_id,
        artifact: claim.artifact,
        protocol: claim.protocol,
        binding_id: binding_id.to_owned(),
        binding_generation: generation,
        operation_id: operation_id.to_owned(),
        input_sha256: input_sha256.to_owned(),
    };
    identity.validate().map_err(|_| {
        Error::new(
            "MODULE_RECEIPT_IDENTITY_INVALID",
            "receipt identity is outside protocol 1.0",
        )
    })?;
    Ok(identity)
}

pub fn for_command(command: &RuntimeCommand) -> Result<ModuleReceiptIdentity> {
    let digest = command.input_sha256.as_deref().ok_or_else(|| {
        Error::new(
            "MODULE_RECEIPT_IDENTITY_MISSING",
            "versioned module command has no canonical input digest",
        )
    })?;
    for_operation(command, &command.operation_id, digest)
}

pub fn for_reconcile_target(command: &RuntimeCommand) -> Result<ModuleReceiptIdentity> {
    if command.method != "agent.reconcile" {
        return Err(Error::new(
            "MODULE_RECEIPT_TARGET_INVALID",
            "target receipt is only valid for agent.reconcile",
        ));
    }
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "MODULE_RECEIPT_TARGET_INVALID",
                "reconcile target is missing",
            )
        })?;
    let digest = command.target_input_sha256.as_deref().ok_or_else(|| {
        Error::new(
            "MODULE_RECEIPT_TARGET_DIGEST_MISSING",
            "Store omitted the exact target Operation digest",
        )
    })?;
    for_operation(command, target_id, digest)
}

pub fn attach_to_outcome(value: &mut Value, identity: &ModuleReceiptIdentity) -> Result<()> {
    let details = value
        .get_mut("details")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            Error::new(
                "MODULE_RECEIPT_INVALID",
                "outcome details must be an object",
            )
        })?;
    details.insert("module_receipt".to_owned(), serde_json::to_value(identity)?);
    Ok(())
}

pub fn serialize_outcome(
    outcome: &RuntimeOutcome,
    identity: &ModuleReceiptIdentity,
) -> Result<Value> {
    if outcome.operation_id != identity.operation_id {
        return Err(Error::new(
            "MODULE_RECEIPT_INVALID",
            "outcome and immutable receipt name different Operations",
        ));
    }
    let mut value = serde_json::to_value(outcome)?;
    attach_to_outcome(&mut value, identity)?;
    Ok(value)
}

pub fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
