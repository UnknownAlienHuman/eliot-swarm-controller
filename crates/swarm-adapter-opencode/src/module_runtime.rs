//! Process-owned adapter bootstrap and typed hello identity.

use crate::config::{
    ARTIFACT_ID, ARTIFACT_VERSION, AdapterConfig, HostConnectionConfig, MODULE_ID, NativeOptions,
    OwnedNativeOptions, read_credential, read_host_config,
};
use crate::provider_auth::ProviderAuthOptions;
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
    "7fc3136219b20d00570b65e5d4fe533e3ea042dadf53be3fdcdfa9781cf0eb68";
const NATIVE_OPTIONS_SCHEMA_VERSION: &str = "2";
const CAPABILITIES: [&str; 9] = [
    "agent.open",
    "agent.reconcile",
    "agent.result",
    "agent.send/next_turn",
    "native.mcp.arm",
    "native.mcp.install",
    "native.mcp.observe",
    "native.mcp.read",
    "task.dispatch",
];
const COMMAND_SCHEMAS: [&str; 4] = [
    "swarm.native_mcp_command",
    "swarm.normalized_result_context",
    "swarm.runtime_command",
    "swarm.task_dispatch_context",
];
const EVENT_SCHEMAS: [&str; 3] = [
    "swarm.normalized_result_page",
    "swarm.runtime_outcome",
    "swarm.task_dispatch_admission",
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
        let owned_native = owned_native_options(&native_options)?;
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
            owned_native,
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

/// Descriptor protected references are resolved by the module owner into
/// these binding-scoped values.  The adapter accepts the complete owner set or
/// no owner at all; a partial set is a launch configuration error, never an
/// external-attach fallback.
fn owned_native_options(native: &NativeOptions) -> Result<Option<OwnedNativeOptions>> {
    let Some(origin) = optional_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_ORIGIN")? else {
        return Ok(None);
    };
    let port = required_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_PORT")?
        .parse::<u16>()
        .map_err(|_| Error::new("MODULE_LAUNCH_CONFIG", "native owner port is invalid"))?;
    let owner = OwnedNativeOptions {
        origin,
        owner_nonce: required_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_NONCE")?,
        bun_executable: absolute_config_path("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_BUN_EXECUTABLE")?,
        bun_sha256: required_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_BUN_SHA256")?,
        server_program: absolute_config_path(
            "ELIOT_SWARM_CONFIG_OPENCODE_OWNER_SERVER_PROGRAM",
        )?,
        server_program_sha256: required_env(
            "ELIOT_SWARM_CONFIG_OPENCODE_OWNER_SERVER_PROGRAM_SHA256",
        )?,
        state_root: absolute_config_path("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_STATE_ROOT")?,
        password_file: absolute_config_path(
            "ELIOT_SWARM_CONFIG_OPENCODE_OWNER_PASSWORD_FILE",
        )?,
        port,
        model_catalog: required_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_MODEL_CATALOG")?,
        provider_auth: provider_auth_options(native)?,
    };
    owner.validate_for(native)?;
    if owner.state_root == owner.password_file {
        return Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "native owner password file must be below its private state root",
        ));
    }
    Ok(Some(owner))
}

fn provider_auth_options(native: &NativeOptions) -> Result<Option<ProviderAuthOptions>> {
    let Some(source) = optional_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_PROVIDER_AUTH_FILE")?
    else {
        return Ok(None);
    };
    let source_file = absolute_config_path_value(source)?;
    let credential_ref = optional_env("ELIOT_SWARM_CONFIG_OPENCODE_OWNER_PROVIDER_CREDENTIAL_REF")?;
    let auth = ProviderAuthOptions {
        source_file,
        credential_ref,
    };
    auth.validate_for(native)?;
    Ok(Some(auth))
}

fn capabilities_match(claim: &ModuleContractClaim) -> bool {
    let values = claim
        .capabilities
        .iter()
        .map(CapabilityId::as_str)
        .collect::<Vec<_>>();
    values.len() == CAPABILITIES.len()
        && CAPABILITIES.iter().all(|capability| values.contains(capability))
}

fn config_schema_matches(claim: &ModuleContractClaim) -> bool {
    claim.config_schema.as_ref().is_some_and(|schema| {
        schema.schema_id == "opencode-v2-native-options"
            && schema.version == NATIVE_OPTIONS_SCHEMA_VERSION
            && schema.sha256.as_ref().map(|digest| digest.as_str())
                == Some(NATIVE_OPTIONS_SCHEMA_SHA256)
    })
}

fn schema_set_matches(schemas: &[SchemaDescriptor], expected: &[&str]) -> bool {
    schemas.len() == expected.len()
        && expected.iter().all(|expected_id| {
            schemas.iter().any(|schema| {
                schema.schema_id == *expected_id
                    && schema.version == "1"
                    && schema.sha256.is_none()
            })
        })
}

fn command_event_schemas_match(claim: &ModuleContractClaim) -> bool {
    schema_set_matches(&claim.command_schemas, &COMMAND_SCHEMAS)
        && schema_set_matches(&claim.event_schemas, &EVENT_SCHEMAS)
}

pub fn native_mcp_enabled(claim: &ModuleContractClaim) -> bool {
    capabilities_match(claim) && command_event_schemas_match(claim)
}

pub fn normalized_dispatch_enabled(claim: &ModuleContractClaim) -> bool {
    capabilities_match(claim) && command_event_schemas_match(claim)
}

pub fn normalized_result_enabled(claim: &ModuleContractClaim) -> bool {
    capabilities_match(claim) && command_event_schemas_match(claim)
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
    absolute_config_path_value(value)
}

fn absolute_config_path_value(value: String) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "descriptor-configured OpenCode path must be absolute",
        ));
    }
    Ok(path)
}
