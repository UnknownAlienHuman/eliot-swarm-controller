use crate::config::{
    ARTIFACT_ID, ARTIFACT_VERSION, HostBootstrapConfig, HostConnectionConfig, MODULE_ID,
    read_credential, read_credential_path_from_env, read_host_config, required_env,
};
use std::{
    env,
    path::{Path, PathBuf},
};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_catalog::{CapabilityId, PreInputOpenContract, ProtocolVersion, SchemaDescriptor},
    module_contract::ModuleContractClaim,
};
use swarm_process::module_owner::{VerifiedModuleWorker, verify_current_adapter_from_env};

const CAPABILITIES: [&str; 5] = [
    "agent.open",
    "agent.reconcile",
    "agent.refresh",
    "agent.send/next_turn",
    "task.dispatch",
];

pub struct OwnedBootstrap {
    pub config: HostBootstrapConfig,
    pub credential: Credential,
    pub worker: VerifiedModuleWorker,
    pub contract: ModuleContractClaim,
}

impl OwnedBootstrap {
    pub fn from_config_path(config_path: &Path) -> Result<Self> {
        let host: HostConnectionConfig = read_host_config(config_path)?;
        let worker = verify_current_adapter_from_env()?;
        let state_dir = absolute_env_path("ELIOT_SWARM_MODULE_STATE")?;
        let owner_path = absolute_env_path("ELIOT_SWARM_MODULE_OWNER")?;
        if owner_path != state_dir.join("owner.json") {
            return Err(Error::new(
                "MODULE_ADAPTER_MEMBERSHIP",
                "module owner path does not match its assigned state directory",
            ));
        }

        let binding_id = required_env("ELIOT_SWARM_MODULE_BINDING_ID")?;
        let generation = required_env("ELIOT_SWARM_MODULE_BINDING_GENERATION")?
            .parse::<i64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::new("MODULE_BINDING_IDENTITY", "binding generation is invalid")
            })?;
        let expected_client_id = required_env("ELIOT_SWARM_MODULE_CLIENT_ID")?;
        let module_id = required_env("ELIOT_SWARM_MODULE_ID")?;
        let artifact_id = required_env("ELIOT_SWARM_MODULE_ARTIFACT_ID")?;
        let artifact_version = required_env("ELIOT_SWARM_MODULE_ARTIFACT_VERSION")?;
        let build_id = optional_env("ELIOT_SWARM_MODULE_BUILD_ID")?;
        let protocol = parse_protocol(&required_env("ELIOT_SWARM_MODULE_PROTOCOL")?)?;
        let credential_path = read_credential_path_from_env()?;
        let contract_json = required_env("ELIOT_SWARM_MODULE_CONTRACT")?;
        let contract: ModuleContractClaim = serde_json::from_str(&contract_json)
            .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "descriptor claim is malformed"))?;
        contract
            .validate()
            .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "descriptor claim is invalid"))?;

        if module_id != MODULE_ID
            || artifact_id != ARTIFACT_ID
            || artifact_version != ARTIFACT_VERSION
            || contract.module_id.as_str() != module_id
            || contract.artifact.artifact_id.as_str() != artifact_id
            || contract.artifact.version.as_str() != artifact_version
            || contract.artifact.build_id != build_id
            || contract.protocol != protocol
            || protocol != (ProtocolVersion { major: 1, minor: 0 })
            || !capabilities_match(&contract)
            || !config_schema_matches(&contract)
            || !command_event_schemas_match(&contract)
        {
            return Err(Error::new(
                "MODULE_CONTRACT_INVALID",
                "launch identity or claim differs from the Claude adapter contract",
            ));
        }

        let config = HostBootstrapConfig {
            host_data_dir: host.host_data_dir,
            credential_file: credential_path.clone(),
            state_dir: state_dir.join("claude-adapter-state"),
            binding_id,
            generation,
            ipc: host.ipc,
        };
        config.validate()?;
        let credential = read_credential(&credential_path)?;
        if credential.client_id != expected_client_id {
            return Err(Error::new(
                "MODULE_CREDENTIAL_SCOPE",
                "credential client differs from the supervisor assignment",
            ));
        }
        Ok(Self {
            config,
            credential,
            worker,
            contract,
        })
    }
}

fn capabilities_match(claim: &ModuleContractClaim) -> bool {
    let values = claim
        .capabilities
        .iter()
        .map(CapabilityId::as_str)
        .collect::<Vec<_>>();
    values.as_slice() == CAPABILITIES.as_slice()
}

fn config_schema_matches(claim: &ModuleContractClaim) -> bool {
    claim.config_schema.is_none()
        && claim.pre_input_open
            == Some(PreInputOpenContract::first_task_dispatch_exact_native_echo())
}

fn command_event_schemas_match(claim: &ModuleContractClaim) -> bool {
    fn is_schema(schema: &SchemaDescriptor, id: &str) -> bool {
        schema.schema_id == id && schema.version == "1" && schema.sha256.is_none()
    }
    claim.command_schemas.len() == 1
        && is_schema(&claim.command_schemas[0], "swarm.runtime_command")
        && claim.event_schemas.len() == 1
        && is_schema(&claim.event_schemas[0], "swarm.runtime_outcome")
}

fn parse_protocol(value: &str) -> Result<ProtocolVersion> {
    let (major, minor) = value.split_once('.').ok_or_else(|| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "selected protocol must be major.minor",
        )
    })?;
    if minor.contains('.') {
        return Err(Error::new(
            "MODULE_CONTRACT_INVALID",
            "selected protocol must contain one separator",
        ));
    }
    let major = major
        .parse::<u16>()
        .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "protocol major is invalid"))?;
    let minor = minor
        .parse::<u16>()
        .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "protocol minor is invalid"))?;
    Ok(ProtocolVersion { major, minor })
}

fn optional_env(name: &'static str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) if value.len() > 64 * 1024 || value.chars().any(char::is_control) => Err(
            Error::new("MODULE_LAUNCH_CONFIG", "optional launch value is invalid"),
        ),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "launch value is not Unicode",
        )),
    }
}

fn absolute_env_path(name: &'static str) -> Result<PathBuf> {
    let value = required_env(name)?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "supervisor path must be absolute",
        ));
    }
    Ok(path)
}
