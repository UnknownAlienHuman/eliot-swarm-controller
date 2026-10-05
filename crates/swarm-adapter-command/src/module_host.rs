//! Host contract checks for the versioned Rust Command artifact.
//!
//! Copy this file to `crates/swarm-adapter-command/src/module_host.rs` when
//! applying `adapter-host.patch`. Descriptor capabilities are compatibility
//! metadata only; Store still authorizes each existing module RPC separately.

use std::{env, path::PathBuf};

use serde_json::Value;
use swarm_contracts::{
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    runtime::{ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome},
};

pub struct ModuleHostIdentity {
    pub claim: ModuleContractClaim,
    pub boot_id: String,
    pub binding_id: String,
    pub generation: i64,
    pub module_client_id: String,
    pub credential_file: PathBuf,
}

pub fn load_claim_and_verify_owner(owner_record: &Value) -> Result<ModuleHostIdentity> {
    let verified = swarm_process::module_owner::verify_current_adapter_from_env()?;
    if &verified.owner_record != owner_record {
        return Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            "owner record changed during adapter membership verification",
        ));
    }

    let raw = required_env("ELIOT_SWARM_MODULE_CONTRACT")?;
    if raw.len() > 64 * 1024 {
        return Err(Error::invalid("trusted descriptor claim exceeds its limit"));
    }
    let claim: ModuleContractClaim = serde_json::from_str(&raw).map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "trusted descriptor claim is malformed",
        )
    })?;
    claim.validate().map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "trusted descriptor claim is not canonical",
        )
    })?;

    let module = required_env("ELIOT_SWARM_MODULE_ID")?;
    let artifact_id = required_env("ELIOT_SWARM_MODULE_ARTIFACT_ID")?;
    let artifact_version = required_env("ELIOT_SWARM_MODULE_ARTIFACT_VERSION")?;
    let build_id = optional_env("ELIOT_SWARM_MODULE_BUILD_ID")?;
    let protocol = required_env("ELIOT_SWARM_MODULE_PROTOCOL")?;
    let binding_id = required_env("ELIOT_SWARM_MODULE_BINDING_ID")?;
    let generation = required_env("ELIOT_SWARM_MODULE_BINDING_GENERATION")?
        .parse::<i64>()
        .ok()
        .filter(|generation| *generation > 0)
        .ok_or_else(|| Error::new("MODULE_ADAPTER_MEMBERSHIP", "module generation is invalid"))?;
    let module_client_id = required_env("ELIOT_SWARM_MODULE_CLIENT_ID")?;
    let credential_file = PathBuf::from(required_env("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")?);

    if module != "runtime.command"
        || artifact_id != crate::ARTIFACT_ID
        || artifact_version != "1"
        || claim.module_id.as_str() != module
        || claim.artifact.artifact_id.as_str() != artifact_id
        || claim.artifact.version.as_str() != artifact_version
        || claim.artifact.build_id.as_deref() != build_id.as_deref()
        || protocol != "1.0"
        || claim.protocol.major != 1
        || claim.protocol.minor != 0
        || !command_capabilities_match(&claim)
        || !schemas_match(&claim)
        || claim.config_schema.is_some()
        || binding_id.trim().is_empty()
        || !credential_file.is_absolute()
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "trusted descriptor differs from this Command adapter implementation",
        ));
    }

    Ok(ModuleHostIdentity {
        claim,
        boot_id: verified.boot_id,
        binding_id,
        generation,
        module_client_id,
        credential_file,
    })
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).map_err(|_| {
        Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            format!("{name} is missing or invalid"),
        )
    })?;
    if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
        return Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            format!("{name} is outside its bounded identity format"),
        ));
    }
    Ok(value)
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value)
            if value.is_empty()
                || value.len() > 16 * 1024
                || value.chars().any(char::is_control) =>
        {
            Err(Error::new(
                "MODULE_ADAPTER_MEMBERSHIP",
                format!("{name} is outside its bounded identity format"),
            ))
        }
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(Error::new(
            "MODULE_ADAPTER_MEMBERSHIP",
            format!("{name} is not valid Unicode"),
        )),
    }
}

fn command_capabilities_match(claim: &ModuleContractClaim) -> bool {
    const EXPECTED: [&str; 4] = [
        "agent.open",
        "agent.reconcile",
        "agent.refresh",
        "task.dispatch",
    ];
    claim.capabilities.len() == EXPECTED.len()
        && claim
            .capabilities
            .iter()
            .zip(EXPECTED)
            .all(|(actual, expected)| actual.as_str() == expected)
}

fn schemas_match(claim: &ModuleContractClaim) -> bool {
    claim.command_schemas.len() == 1
        && claim.command_schemas[0].schema_id == "swarm.runtime_command"
        && claim.command_schemas[0].version == "1"
        && claim.command_schemas[0].sha256.is_none()
        && claim.event_schemas.len() == 1
        && claim.event_schemas[0].schema_id == "swarm.runtime_outcome"
        && claim.event_schemas[0].version == "1"
        && claim.event_schemas[0].sha256.is_none()
}

pub fn require_negotiated(hello: &Value, host: &ModuleHostIdentity) -> Result<()> {
    let claim = &host.claim;
    let negotiated = &hello["module_contract_negotiation"];
    let expected = serde_json::to_value(claim)?;
    if hello["binding_id"] != host.binding_id
        || hello["generation"].as_i64() != Some(host.generation)
        || hello["route"]["runtime"] != crate::RUNTIME
        || hello["route"]["module_artifact_id"] != crate::ARTIFACT_ID
        || negotiated["status"] != "negotiated"
        || negotiated["source"] != "store_registered_descriptor"
        || negotiated["descriptor_revision"]
            .as_u64()
            .is_none_or(|revision| revision == 0)
        || negotiated["effects_authorized_by_descriptor"] != false
        || negotiated["module_id"] != expected["module_id"]
        || negotiated["artifact"] != expected["artifact"]
        || negotiated["protocol"] != expected["protocol"]
        || negotiated["capabilities"] != expected["capabilities"]
        || negotiated["config_schema"] != expected["config_schema"]
        || negotiated["command_schemas"] != expected["command_schemas"]
        || negotiated["event_schemas"] != expected["event_schemas"]
    {
        return Err(Error::new(
            "MODULE_CONTRACT_NEGOTIATION_FAILED",
            "Store did not negotiate this exact registered Command descriptor and binding",
        ));
    }
    Ok(())
}

pub fn attach_receipt_identity(
    outcome: &mut RuntimeOutcome,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<()> {
    let input_sha256 = if outcome.operation_id == command.operation_id {
        command.input_sha256.as_deref()
    } else if command.method == "agent.reconcile"
        && command.input["operation_id"] == outcome.operation_id
    {
        command.target_input_sha256.as_deref()
    } else {
        None
    }
    .filter(|digest| is_sha256(digest))
    .ok_or_else(|| {
        Error::new(
            "MODULE_RECEIPT_IDENTITY_MISSING",
            "Store did not provide the exact Operation input digest",
        )
    })?;

    let identity = ModuleReceiptIdentity {
        schema_version: 1,
        module_id: claim.module_id.clone(),
        artifact: claim.artifact.clone(),
        protocol: claim.protocol,
        binding_id: command.binding_id.clone(),
        binding_generation: command.generation,
        operation_id: outcome.operation_id.clone(),
        input_sha256: input_sha256.to_owned(),
    };
    identity.validate().map_err(|_| {
        Error::new(
            "MODULE_RECEIPT_IDENTITY_INVALID",
            "receipt identity is invalid",
        )
    })?;
    let value = serde_json::to_value(identity)?;
    let details = outcome.details.as_object_mut().ok_or_else(|| {
        Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "outcome details must be an object",
        )
    })?;
    if let Some(existing) = details.get("module_receipt") {
        if existing != &value {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved receipt identity differs from the exact Store command",
            ));
        }
    } else {
        details.insert("module_receipt".to_owned(), value);
    }
    Ok(())
}

pub fn validate_saved_receipt(
    outcome: &RuntimeOutcome,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
) -> Result<()> {
    let identity: ModuleReceiptIdentity =
        serde_json::from_value(outcome.details.get("module_receipt").cloned().ok_or_else(
            || {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "saved outcome has no module receipt",
                )
            },
        )?)
        .map_err(|_| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved module receipt is malformed",
            )
        })?;
    identity.validate().map_err(|_| {
        Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved module receipt is invalid",
        )
    })?;
    if identity.module_id != claim.module_id
        || identity.artifact != claim.artifact
        || identity.protocol != claim.protocol
        || identity.binding_id != binding_id
        || identity.binding_generation != generation
        || identity.operation_id != outcome.operation_id
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved module receipt belongs to a different descriptor or binding",
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
