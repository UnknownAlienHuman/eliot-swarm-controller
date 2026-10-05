//! Process-owned adapter bootstrap and typed hello identity.

use crate::config::{
    ARTIFACT_ID, ARTIFACT_VERSION, AdapterConfig, HostConnectionConfig, MODULE_ID, NativeOptions,
    read_credential, read_host_config,
};
use std::{
    env,
    path::{Path, PathBuf},
};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_catalog::{CapabilityId, ProtocolVersion, SchemaDescriptor},
    module_contract::ModuleContractClaim,
};
use swarm_process::module_owner::{VerifiedModuleWorker, verify_current_adapter_from_env};

const MAX_LAUNCH_VALUE_BYTES: usize = 64 * 1024;
const NATIVE_OPTIONS_SCHEMA_SHA256: &str =
    "d597be6bae80dc82535b658b5daaf3037a09976e5704d799a6715a673a62f662";
const CAPABILITIES: [&str; 4] = [
    "agent.open",
    "agent.reconcile",
    "agent.send/next_turn",
    "task.dispatch",
];

pub struct OwnedBootstrap {
    pub config: AdapterConfig,
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
                "module owner path does not match the assigned scope state directory",
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
        let credential_path = absolute_env_path("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")?;
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
                "launch identity or claim differs from the OpenCode adapter contract",
            ));
        }

        // The exact immutable route values arrive through descriptor-declared
        // config keys. The supervisor passes these as separate strings; it
        // does not construct vendor JSON or invent model aliases/defaults.
        let native_options = NativeOptions {
            service_id: required_env("ELIOT_SWARM_CONFIG_OPENCODE_SERVICE_ID")?,
            connection_file: absolute_config_path("ELIOT_SWARM_CONFIG_OPENCODE_CONNECTION_FILE")?,
            expected_version: required_env("ELIOT_SWARM_CONFIG_OPENCODE_EXPECTED_VERSION")?,
            directory: absolute_config_path("ELIOT_SWARM_CONFIG_OPENCODE_DIRECTORY")?,
            model: crate::config::ModelRef {
                id: required_env("ELIOT_SWARM_CONFIG_OPENCODE_MODEL_ID")?,
                provider_id: required_env("ELIOT_SWARM_CONFIG_OPENCODE_PROVIDER_ID")?,
                variant: required_env("ELIOT_SWARM_CONFIG_OPENCODE_VARIANT")?,
            },
        };
        let config = AdapterConfig {
            schema_version: 1,
            host_data_dir: host.host_data_dir,
            credential_file: credential_path.clone(),
            // Keep adapter journals in their own child directory; the helper
            // reserves the parent for owner.json, worker.json, and its lock.
            state_dir: state_dir.join("opencode-adapter-state"),
            binding_id,
            generation,
            module_artifact_id: artifact_id,
            native_options,
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
    claim.config_schema.as_ref().is_some_and(|schema| {
        schema.schema_id == "opencode-v2-native-options"
            && schema.version == "1"
            && schema.sha256.as_ref().map(|digest| digest.as_str())
                == Some(NATIVE_OPTIONS_SCHEMA_SHA256)
    })
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
            "selected protocol must contain exactly one separator",
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

fn required_env(name: &'static str) -> Result<String> {
    let value = env::var(name)
        .map_err(|_| Error::new("MODULE_LAUNCH_CONFIG", "required launch value is missing"))?;
    if value.is_empty()
        || value.len() > MAX_LAUNCH_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "launch value is empty, too large, or contains control characters",
        ));
    }
    Ok(value)
}

fn optional_env(name: &'static str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value)
            if value.is_empty()
                || value.len() > MAX_LAUNCH_VALUE_BYTES
                || value.chars().any(char::is_control) =>
        {
            Err(Error::new(
                "MODULE_LAUNCH_CONFIG",
                "optional launch value is invalid",
            ))
        }
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

fn absolute_config_path(name: &'static str) -> Result<PathBuf> {
    let value = required_env(name)?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "descriptor-configured OpenCode path must be absolute",
        ));
    }
    Ok(path)
}
