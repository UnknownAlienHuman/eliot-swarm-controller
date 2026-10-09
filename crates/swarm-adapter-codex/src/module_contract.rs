use std::{env, path::PathBuf};

use swarm_contracts::{
    credential::Credential,
    module_catalog::{CapabilityId, ProtocolVersion, SchemaDescriptor},
    module_contract::ModuleContractClaim,
};
use swarm_process::module_owner::{VerifiedModuleWorker, verify_current_adapter_from_env};

use crate::{ARTIFACT_ID, ARTIFACT_VERSION, AdapterError, MODULE_ID};

pub(crate) const GOAL_CONTINUATION_ADMISSION_SCHEMA_ID: &str = "swarm.goal_continuation_admission";

const CAPABILITIES: [&str; 4] = [
    "agent.open",
    "agent.reconcile",
    "agent.send",
    "task.dispatch",
];
const RESULT_CAPABILITIES: [&str; 5] = [
    "agent.open",
    "agent.reconcile",
    "agent.result",
    "agent.send",
    "task.dispatch",
];

pub(crate) struct ModuleRuntimeContext {
    pub worker: VerifiedModuleWorker,
    pub state_dir: PathBuf,
    pub binding_id: String,
    pub generation: i64,
    pub credential: Credential,
    pub claim: ModuleContractClaim,
}

pub(crate) fn load_runtime_context() -> Result<ModuleRuntimeContext, AdapterError> {
    let worker = verify_current_adapter_from_env().map_err(|_| AdapterError::Owner)?;
    let state_dir = PathBuf::from(required_env("ELIOT_SWARM_MODULE_STATE")?);
    let owner_path = PathBuf::from(required_env("ELIOT_SWARM_MODULE_OWNER")?);
    if !state_dir.is_absolute() || owner_path != state_dir.join("owner.json") {
        return Err(AdapterError::Owner);
    }

    let module_id = required_env("ELIOT_SWARM_MODULE_ID")?;
    let binding_id = required_env("ELIOT_SWARM_MODULE_BINDING_ID")?;
    let generation = required_env("ELIOT_SWARM_MODULE_BINDING_GENERATION")?
        .parse::<i64>()
        .map_err(|_| AdapterError::Owner)?;
    let module_client_id = required_env("ELIOT_SWARM_MODULE_CLIENT_ID")?;
    let artifact_id = required_env("ELIOT_SWARM_MODULE_ARTIFACT_ID")?;
    let artifact_version = required_env("ELIOT_SWARM_MODULE_ARTIFACT_VERSION")?;
    let build_id = optional_env("ELIOT_SWARM_MODULE_BUILD_ID")?;
    let claim_json = required_env("ELIOT_SWARM_MODULE_CONTRACT")?;
    let credential_path = PathBuf::from(required_env("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")?);
    if !credential_path.is_absolute() || generation <= 0 {
        return Err(AdapterError::Owner);
    }
    let credential_bytes = super::read_bounded_file(&credential_path, 64 * 1024)?;
    let credential: Credential =
        serde_json::from_slice(&credential_bytes).map_err(|_| AdapterError::Owner)?;
    let claim: ModuleContractClaim =
        serde_json::from_str(&claim_json).map_err(|_| AdapterError::Owner)?;
    claim.validate().map_err(|_| AdapterError::Owner)?;

    if module_id != MODULE_ID
        || claim.module_id.as_str() != module_id
        || artifact_id != ARTIFACT_ID
        || artifact_version != ARTIFACT_VERSION
        || claim.artifact.artifact_id.as_str() != artifact_id
        || claim.artifact.version.as_str() != artifact_version
        || claim.artifact.build_id != build_id
        || claim.protocol != (ProtocolVersion { major: 1, minor: 0 })
        || claim.config_schema.is_some()
        || {
            let capabilities = claim
                .capabilities
                .iter()
                .map(CapabilityId::as_str)
                .collect::<Vec<_>>();
            if normalized_result_enabled(&claim) {
                capabilities.as_slice() != RESULT_CAPABILITIES.as_slice()
            } else {
                capabilities.as_slice() != CAPABILITIES.as_slice()
            }
        }
        || !has_generic_schemas(&claim)
        || credential.client_id != module_client_id
    {
        return Err(AdapterError::Owner);
    }

    Ok(ModuleRuntimeContext {
        worker,
        state_dir,
        binding_id,
        generation,
        credential,
        claim,
    })
}

fn required_env(name: &str) -> Result<String, AdapterError> {
    let value = env::var(name).map_err(|_| AdapterError::Owner)?;
    if value.is_empty() || value.len() > 16 * 1024 || value.chars().any(char::is_control) {
        return Err(AdapterError::Owner);
    }
    Ok(value)
}

fn optional_env(name: &str) -> Result<Option<String>, AdapterError> {
    match env::var(name) {
        Ok(value)
            if value.is_empty()
                || value.len() > 16 * 1024
                || value.chars().any(char::is_control) =>
        {
            Err(AdapterError::Owner)
        }
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(AdapterError::Owner),
    }
}

fn has_generic_schemas(claim: &ModuleContractClaim) -> bool {
    fn schema(id: &str) -> SchemaDescriptor {
        SchemaDescriptor {
            schema_id: id.to_owned(),
            version: "1".to_owned(),
            sha256: None,
        }
    }
    task_prompt_selected(claim).is_ok_and(|enabled| enabled)
        && exact_schemas(
            &claim.command_schemas,
            &[
                schema("swarm.runtime_command"),
                schema("swarm.task_dispatch_context"),
                schema("swarm.normalized_result_context"),
                swarm_contracts::module_contract::task_prompt_schema(),
            ],
        )
        && exact_schemas(
            &claim.event_schemas,
            &[
                schema("swarm.runtime_outcome"),
                schema("swarm.task_dispatch_admission"),
                schema("swarm.normalized_result_page"),
            ],
        )
}

/// A v5 executable never accepts an unselected or unknown prompt schema.
pub(crate) fn task_prompt_selected(
    claim: &ModuleContractClaim,
) -> Result<bool, AdapterError> {
    swarm_contracts::module_contract::task_prompt_selected(
        claim.command_schemas.iter(),
        claim.event_schemas.iter(),
        claim.capabilities.iter(),
    )
    .map_err(|_| AdapterError::HostProtocol)
}

fn exact_schemas(actual: &[SchemaDescriptor], expected: &[SchemaDescriptor]) -> bool {
    actual.len() == expected.len() && expected.iter().all(|schema| actual.contains(schema))
}

pub(crate) fn normalized_dispatch_enabled(claim: &ModuleContractClaim) -> bool {
    fn schema(id: &str, version: &str) -> SchemaDescriptor {
        SchemaDescriptor {
            schema_id: id.to_owned(),
            version: version.to_owned(),
            sha256: None,
        }
    }
    claim
        .command_schemas
        .contains(&schema("swarm.task_dispatch_context", "1"))
        && claim
            .event_schemas
            .contains(&schema("swarm.task_dispatch_admission", "1"))
}

pub(crate) fn normalized_result_enabled(claim: &ModuleContractClaim) -> bool {
    fn schema(id: &str, version: &str) -> SchemaDescriptor {
        SchemaDescriptor {
            schema_id: id.to_owned(),
            version: version.to_owned(),
            sha256: None,
        }
    }
    let runtime_command = schema("swarm.runtime_command", "1");
    let runtime_outcome = schema("swarm.runtime_outcome", "1");
    let dispatch_context = schema("swarm.task_dispatch_context", "1");
    let dispatch_admission = schema("swarm.task_dispatch_admission", "1");
    let result_context = schema("swarm.normalized_result_context", "1");
    let result_page = schema("swarm.normalized_result_page", "1");
    exact_schemas(
        &claim.command_schemas,
        &[
            runtime_command,
            dispatch_context,
            result_context,
            swarm_contracts::module_contract::task_prompt_schema(),
        ],
    ) && exact_schemas(
        &claim.event_schemas,
        &[runtime_outcome, dispatch_admission, result_page],
    )
}

pub(crate) fn receipt_identity(
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<swarm_contracts::runtime::ModuleReceiptIdentity, AdapterError> {
    let identity = swarm_contracts::runtime::ModuleReceiptIdentity {
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
        .map_err(|_| AdapterError::HostProtocol)?;
    Ok(identity)
}

pub(crate) fn validate_negotiated_hello(
    response: &serde_json::Value,
    context: &ModuleRuntimeContext,
) -> Result<(), AdapterError> {
    let claim = &context.claim;
    let artifact = serde_json::to_value(&claim.artifact).map_err(|_| AdapterError::HostProtocol)?;
    let protocol = serde_json::to_value(claim.protocol).map_err(|_| AdapterError::HostProtocol)?;
    let capabilities =
        serde_json::to_value(&claim.capabilities).map_err(|_| AdapterError::HostProtocol)?;
    let config_schema =
        serde_json::to_value(&claim.config_schema).map_err(|_| AdapterError::HostProtocol)?;
    let command_schemas =
        serde_json::to_value(&claim.command_schemas).map_err(|_| AdapterError::HostProtocol)?;
    let event_schemas =
        serde_json::to_value(&claim.event_schemas).map_err(|_| AdapterError::HostProtocol)?;
    let negotiation = &response["module_contract_negotiation"];
    if response["binding_id"] != context.binding_id
        || response["generation"].as_i64() != Some(context.generation)
        || response["route"]["runtime"] != "codex"
        || response["route"]["module_artifact_id"] != ARTIFACT_ID
        || negotiation["status"] != "negotiated"
        || negotiation["source"] != "store_registered_descriptor"
        || negotiation["descriptor_revision"]
            .as_u64()
            .is_none_or(|revision| revision == 0)
        || negotiation["module_id"] != claim.module_id.as_str()
        || negotiation["artifact"] != artifact
        || negotiation["protocol"] != protocol
        || negotiation["capabilities"] != capabilities
        || negotiation["config_schema"] != config_schema
        || negotiation["command_schemas"] != command_schemas
        || negotiation["event_schemas"] != event_schemas
        || negotiation["effects_authorized_by_descriptor"] != false
    {
        return Err(AdapterError::HostProtocol);
    }
    Ok(())
}
