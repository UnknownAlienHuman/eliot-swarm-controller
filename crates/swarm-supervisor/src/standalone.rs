//! Process boundary for the independent module supervisor.
//!
//! This module owns the supervisor-side bootstrap, resolver publication, and
//! demand reconciliation.  It deliberately depends only on contracts, the
//! authenticated client, and the existing process/owner implementation.  A
//! Store handle, database connection, controller object, or host IPC listener
//! never crosses into this crate.

use crate::control::{
    CompleteInstalledDescriptorRequest, DescriptorRegistrationReceipt,
    InstalledDescriptorCompletionOutcome, InstalledDescriptorOperationOutcome,
    InstalledDescriptorOperationState, InstalledDescriptorRequestPage,
    InstalledDescriptorRequestState, ObservationDeliveryDisposition,
};
use crate::descriptor::extend_descriptor_catalog;
use crate::{
    AdmissionState, BindingLaunchConfig, BindingMapPublication, CapabilityId, DemandCause,
    DemandLease, DescriptorCatalog, KernelFault, LaunchValue, ModuleBindingCredential,
    ModuleDemandCursor, ModuleDemandRecord, ModuleDescriptor, ModuleOwnerExecutable,
    ModuleSupervisorObservation, OperationReadback, OperationSnapshot, ProtectedResolverContext,
    ResolverMapDirectory, ServiceScope, Sha256Digest, SupervisorControlClient, SupervisorRegistry,
    SupervisorRegistryConfig, SupervisorStatus, load_installed_descriptor, module_contract_claim,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs,
    io::{BufRead, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use swarm_client::IpcConfig;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_catalog::{ArtifactSelector, ProtectedRef, ProtocolRange, ProtocolVersion},
};
use tokio::{sync::watch, time};
use uuid::Uuid;

const CONFIG_SCHEMA_VERSION: u16 = 1;
const HOST_MODULE_PROTOCOL: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
const STARTUP_ATTEMPTS: usize = 60;
const STARTUP_RETRY: Duration = Duration::from_millis(100);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
const MAX_BOOTSTRAP_BYTES: usize = 65_536;
const MAX_DESCRIPTOR_FILES: usize = 512;
const MAX_PROTECTED_FILES: usize = 128;
const MAX_LAUNCH_CONFIGS: usize = 256;
const MAX_STATUS_QUEUE: usize = 1_024;
const MODULE_SUPERVISOR_CLIENT_ID: &str = "eliot-module-supervisor-v1";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_ID: &str = "opencode-v2-native-options";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_LEGACY: &str = "1";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_OWNER: &str = "2";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V3: &str = "3";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V4: &str = "4";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V4: &str =
    "112d277fe7774bef1c05d127a60f6fb8adfef0c6c3dab0e749eef0eb07267ed8";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_LEGACY: &str =
    "d597be6bae80dc82535b658b5daaf3037a09976e5704d799a6715a673a62f662";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_OWNER: &str =
    "7fc3136219b20d00570b65e5d4fe533e3ea042dadf53be3fdcdfa9781cf0eb68";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V3: &str =
    "070d37891aed021d6a5023cd885647b1403741927e87a0cb28050f30b7c4d97e";
const OPENCODE_MODULE_ID: &str = "eliot.opencode.v2";
const OPENCODE_ARTIFACT_ID: &str = "eliot-opencode-v2.rust-http.1";
const OPENCODE_OWNED_SERVICE_VERSION: &str = "2.0.7";
const OWNER_ROUTE_KEY: &str = "__eliot_owned_service";
const OWNER_NONCE_KEY: &str = "__eliot_owner_nonce";

/// A launch configuration bound to one exact descriptor artifact and route
/// option digest. Route options themselves remain in the authenticated demand
/// exchange; this DTO carries only a prevalidated launch environment.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandaloneLaunchConfig {
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub route_native_options_sha256: String,
    pub config: BindingLaunchConfig,
}

#[cfg(test)]
mod historical_owner_obligation_tests {
    use super::StandaloneSupervisor;
    use crate::LifecycleState;

    #[test]
    fn settled_owner_and_worker_receipts_do_not_keep_the_registry_alive() {
        let completed = LifecycleState::Completed { exit_code: Some(0) };
        assert!(!StandaloneSupervisor::has_unsettled_process_custody(
            &completed, true, true
        ));

        let departed = LifecycleState::ProcessExited {
            exit_code: Some(0),
            exit_proven: true,
        };
        assert!(!StandaloneSupervisor::has_unsettled_process_custody(
            &departed, true, true
        ));

        let live = LifecycleState::ProcessRunning {
            boot_id: "boot-fixture".to_owned(),
        };
        assert!(StandaloneSupervisor::has_unsettled_process_custody(
            &live, true, false
        ));

        let uncertain = LifecycleState::ProcessExited {
            exit_code: None,
            exit_proven: false,
        };
        assert!(StandaloneSupervisor::has_unsettled_process_custody(
            &uncertain, true, true
        ));
    }
}

impl StandaloneLaunchConfig {
    fn validate(&self, module_id: &str) -> Result<()> {
        bounded_text(&self.artifact_id, "artifact_id", 256)?;
        bounded_text(&self.artifact_version, "artifact_version", 256)?;
        if let Some(build_id) = &self.build_id {
            bounded_text(build_id, "build_id", 256)?;
        }
        validate_sha256(
            &self.route_native_options_sha256,
            "route_native_options_sha256",
        )?;
        self.config.validate()?;
        if module_id.is_empty() || module_id.len() > 128 {
            return Err(Error::invalid("launch config module id is invalid"));
        }
        Ok(())
    }
}

/// Neutral configuration for an independent supervisor process. The
/// supervisor credential is accepted only in this in-memory/bootstrap DTO and
/// is never persisted in Store metadata, argv, logs, or resolver maps.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandaloneSupervisorConfig {
    pub schema_version: u16,
    pub root: PathBuf,
    pub ipc: IpcConfig,
    pub supervisor_credential: Credential,
    pub install_root: PathBuf,
    pub descriptor_files: Vec<PathBuf>,
    pub state_root: PathBuf,
    pub resolver_root: PathBuf,
    pub owner_helper: PathBuf,
    pub owner_helper_sha256: Sha256Digest,
    pub protected_files: BTreeMap<ProtectedRef, PathBuf>,
    #[serde(default)]
    pub launch_configs: BTreeMap<String, StandaloneLaunchConfig>,
    /// The host's already-validated route mapper. This enum is deliberately
    /// neutral: it carries the mapper choice across the process boundary,
    /// never route payloads or credentials.
    #[serde(default)]
    pub route_config_mapper: StandaloneRouteConfigMapper,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StandaloneRouteConfigMapper {
    #[default]
    DescriptorSchema,
    EmptyOnly,
    OpenCodeSevenField,
}

impl StandaloneSupervisorConfig {
    /// Add retained Operator-installed descriptor files to this child bootstrap.
    /// The host obtains these explicit paths from completed durable Operations.
    pub fn append_retained_descriptor_files(
        &mut self,
        paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<()> {
        let mut staged = self.descriptor_files.clone();
        let mut seen = staged.iter().cloned().collect::<BTreeSet<_>>();
        for path in paths {
            if !path.is_absolute() {
                return Err(Error::invalid("retained descriptor path must be absolute"));
            }
            if seen.insert(path.clone()) {
                staged.push(path);
            }
        }
        if staged.len() > MAX_DESCRIPTOR_FILES {
            return Err(Error::new(
                "MODULE_DESCRIPTOR_LIMIT",
                "retained descriptor bootstrap exceeds its bounded file count",
            ));
        }
        self.descriptor_files = staged;
        Ok(())
    }

    /// Hot-update restart admission: prove the combined bootstrap that the
    /// next host restart would rebuild still fits the 65_536-byte child
    /// frame. `prior_hot` carries hot paths already admitted in this process
    /// lifetime; `candidate` is the queued install under review. Uses exact
    /// serialized bytes via `SupervisorBootstrap::to_frame` (UTF-8/JSON
    /// accounting), not char counts or the 512-entry bound.
    pub fn ensure_hot_descriptor_restartable(
        &self,
        actor_instance_id: &str,
        prior_hot: &BTreeSet<PathBuf>,
        candidate: &Path,
    ) -> Result<()> {
        let mut staged = self.clone();
        if !prior_hot.is_empty() {
            staged.append_retained_descriptor_files(prior_hot.iter().cloned())?;
        }
        staged.append_retained_descriptor_files([candidate.to_path_buf()])?;
        let probe = SupervisorBootstrap {
            config: staged,
            actor_instance_id: Some(actor_instance_id.to_owned()),
        };
        probe.to_frame().map(|_| ()).map_err(|error| {
            if error.message.contains("exceeds its size bound") {
                Error::new(
                    "MODULE_BOOTSTRAP_LIMIT",
                    "retained descriptor bootstrap exceeds its serialized size bound",
                )
            } else {
                error
            }
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(Error::invalid(
                "unsupported module supervisor config version",
            ));
        }
        for (path, field) in [
            (&self.root, "root"),
            (&self.install_root, "install_root"),
            (&self.state_root, "state_root"),
            (&self.resolver_root, "resolver_root"),
            (&self.owner_helper, "owner_helper"),
        ] {
            if !path.is_absolute() || path.as_os_str().is_empty() {
                return Err(Error::invalid(format!("{field} must be an absolute path")));
            }
        }
        if self.supervisor_credential.client_id != MODULE_SUPERVISOR_CLIENT_ID
            || self.supervisor_credential.token.is_empty()
            || self.supervisor_credential.token.len() > 4_096
            || self
                .supervisor_credential
                .token
                .chars()
                .any(char::is_control)
        {
            return Err(Error::invalid(
                "standalone supervisor credential must be the exact host-issued identity",
            ));
        }
        if self.descriptor_files.is_empty() || self.descriptor_files.len() > MAX_DESCRIPTOR_FILES {
            return Err(Error::invalid("descriptor file count is out of bounds"));
        }
        if self
            .descriptor_files
            .iter()
            .any(|path| !path.is_absolute() || path.as_os_str().is_empty())
        {
            return Err(Error::invalid("descriptor files must be absolute paths"));
        }
        if self.protected_files.len() > MAX_PROTECTED_FILES {
            return Err(Error::invalid("protected file count is out of bounds"));
        }
        if self
            .protected_files
            .values()
            .any(|path| !path.is_absolute() || path.as_os_str().is_empty())
        {
            return Err(Error::invalid("protected files must be absolute paths"));
        }
        if self.launch_configs.len() > MAX_LAUNCH_CONFIGS {
            return Err(Error::invalid("launch config count is out of bounds"));
        }
        for (module_id, entry) in &self.launch_configs {
            entry.validate(module_id)?;
        }
        if self.ipc.max_frame_bytes == 0
            || self.ipc.max_frame_bytes > 16 * 1024 * 1024
            || self.ipc.max_connections == 0
            || self.ipc.max_connections > 4_096
            || self.ipc.max_inflight_per_connection == 0
            || self.ipc.max_inflight_per_connection > 128
            || self.ipc.write_timeout_seconds == 0
            || self.ipc.write_timeout_seconds > 300
        {
            return Err(Error::invalid("module supervisor IPC bounds are invalid"));
        }
        Ok(())
    }

    fn owner_executable(&self) -> ModuleOwnerExecutable {
        ModuleOwnerExecutable {
            path: self.owner_helper.clone(),
            sha256: self.owner_helper_sha256.clone(),
        }
    }
}

/// The one-frame handoff consumed by the optional process binary. The host
/// writes this over a private stdin pipe only after its IPC listener has
/// published readiness; the child never reads arbitrary command payloads.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorBootstrap {
    pub config: StandaloneSupervisorConfig,
    /// Actor UUID admitted by the host after exact prior-child disposition.
    /// `None` exists only on the host-side bootstrap template and can never
    /// cross the child pipe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_instance_id: Option<String>,
}

impl SupervisorBootstrap {
    pub fn validate(&self) -> Result<()> {
        self.config.validate()?;
        if self
            .actor_instance_id
            .as_deref()
            .is_some_and(|actor| Uuid::parse_str(actor).is_err())
        {
            return Err(Error::invalid("supervisor actor admission ID is invalid"));
        }
        Ok(())
    }

    fn validate_for_child(&self) -> Result<()> {
        self.validate()?;
        if self.actor_instance_id.is_none() {
            return Err(Error::new(
                "MODULE_ACTOR_ADMISSION_REQUIRED",
                "host did not admit an actor UUID before supervisor bootstrap",
            ));
        }
        Ok(())
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
        let mut bytes = Vec::new();
        reader
            .take((MAX_BOOTSTRAP_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(Error::invalid(
                "supervisor bootstrap exceeds its size bound",
            ));
        }
        let bootstrap: Self = serde_json::from_slice(&bytes)?;
        bootstrap.validate_for_child()?;
        Ok(bootstrap)
    }

    /// Read one bounded newline-delimited bootstrap frame. Keeping the pipe
    /// open after this frame lets the host signal graceful shutdown with EOF.
    pub fn from_frame<R: BufRead>(mut reader: R) -> Result<Self> {
        let mut line = Vec::with_capacity(MAX_BOOTSTRAP_BYTES.min(4096));
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            return Err(Error::invalid("supervisor bootstrap frame is missing"));
        }
        if line.len() > MAX_BOOTSTRAP_BYTES {
            return Err(Error::invalid(
                "supervisor bootstrap exceeds its size bound",
            ));
        }
        let bootstrap: Self = serde_json::from_reader(Cursor::new(line.as_slice()))?;
        bootstrap.validate_for_child()?;
        Ok(bootstrap)
    }

    pub fn to_frame(&self) -> Result<Vec<u8>> {
        self.validate_for_child()?;
        let mut bytes = serde_json::to_vec(self)?;
        bytes.push(b'\n');
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(Error::invalid(
                "supervisor bootstrap exceeds its size bound",
            ));
        }
        Ok(bytes)
    }

    pub fn write_frame<W: Write>(&self, mut writer: W) -> Result<()> {
        writer.write_all(&self.to_frame()?)?;
        writer.flush()?;
        Ok(())
    }

    pub fn write_to<W: Write>(&self, mut writer: W) -> Result<()> {
        self.validate()?;
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_BOOTSTRAP_BYTES {
            return Err(Error::invalid(
                "supervisor bootstrap exceeds its size bound",
            ));
        }
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

/// Host supplied route mapping. The binary uses the strict static mapper;
/// an embedding host may provide a mapper that validates its own descriptor
/// schema before returning the same bounded `BindingLaunchConfig`.
pub trait LaunchConfigProvider: Send + Sync + 'static {
    fn for_binding(
        &self,
        descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig>;
}

struct StaticLaunchConfigProvider {
    entries: BTreeMap<String, StandaloneLaunchConfig>,
    mapper: StandaloneRouteConfigMapper,
    /// Host-only reference -> pinned source path. The mapper receives paths,
    /// never file contents or provider secrets.
    protected_files: BTreeMap<ProtectedRef, PathBuf>,
}

impl LaunchConfigProvider for StaticLaunchConfigProvider {
    fn for_binding(
        &self,
        descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig> {
        let module_id = descriptor.module_id.as_str();
        let Some(entry) = self.entries.get(module_id) else {
            return map_route_config(
                self.mapper,
                descriptor,
                route_native_options,
                &self.protected_files,
            );
        };
        if entry.artifact_id != descriptor.artifact.artifact_id.as_str()
            || entry.artifact_version != descriptor.artifact.version.as_str()
            || entry.build_id != descriptor.artifact.build_id
            || entry.route_native_options_sha256 != json_sha256(route_native_options)?
        {
            return Err(Error::new(
                "MODULE_CONFIG_SCOPE_MISMATCH",
                "launch-config mapping does not match the retained descriptor and route options",
            ));
        }
        Ok(entry.config.clone())
    }
}

fn map_route_config(
    mapper: StandaloneRouteConfigMapper,
    descriptor: &ModuleDescriptor,
    route_native_options: &Value,
    protected_files: &BTreeMap<ProtectedRef, PathBuf>,
) -> Result<BindingLaunchConfig> {
    match mapper {
        StandaloneRouteConfigMapper::EmptyOnly => {
            if route_native_options
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            {
                Ok(BindingLaunchConfig::default())
            } else {
                Err(Error::new(
                    "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                    "this host has no schema-validated mapper for the binding's native options",
                ))
            }
        }
        StandaloneRouteConfigMapper::DescriptorSchema => match descriptor.config_schema.as_ref() {
            None => Ok(BindingLaunchConfig::default()),
            Some(_) if opencode_schema_supported(descriptor) => {
                open_code_route_config(descriptor, route_native_options, protected_files)
            }
            Some(_) => Err(Error::new(
                "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                "the retained descriptor names a launch config schema with no supervisor mapper",
            )),
        },
        StandaloneRouteConfigMapper::OpenCodeSevenField => {
            open_code_route_config(descriptor, route_native_options, protected_files)
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedRouteModel {
    id: String,
    #[serde(rename = "providerID")]
    provider_id: String,
    variant: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedRouteConfig {
    origin: String,
    service_id: String,
    model: OwnedRouteModel,
    model_catalog: String,
    bun_executable: PathBuf,
    bun_sha256: String,
    server_program: PathBuf,
    server_program_sha256: String,
    state_root: PathBuf,
    port: u16,
}

fn opencode_schema_supported(descriptor: &ModuleDescriptor) -> bool {
    // These schema digests are registered for this exact module artifact.
    descriptor.module_id.as_str() == OPENCODE_MODULE_ID
        && descriptor.artifact.artifact_id.as_str() == OPENCODE_ARTIFACT_ID
        && descriptor.config_schema.as_ref().is_some_and(|schema| {
            schema.schema_id == OPENCODE_NATIVE_OPTIONS_SCHEMA_ID
                && schema.sha256.as_ref().is_some_and(|digest| {
                    (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_LEGACY
                        && matches!(descriptor.artifact.version.as_str(), "0.1.0" | "0.2.0")
                        && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_LEGACY)
                        || (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_OWNER
                            && descriptor.artifact.version.as_str() == "0.3.0"
                            && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_OWNER)
                        || (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V3
                            && matches!(descriptor.artifact.version.as_str(), "0.5.0" | "0.6.0")
                            && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V3)
                        || (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V4
                            && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V4)
                })
        })
}

fn opencode_owner_schema(descriptor: &ModuleDescriptor) -> bool {
    descriptor.config_schema.as_ref().is_some_and(|schema| {
        schema.schema_id == OPENCODE_NATIVE_OPTIONS_SCHEMA_ID
            && schema.sha256.as_ref().is_some_and(|digest| {
                (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_OWNER
                    && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_OWNER)
                    || (schema.version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V3
                        && digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V3)
            })
    })
}

fn open_code_route_config(
    descriptor: &ModuleDescriptor,
    options: &Value,
    _protected_files: &BTreeMap<ProtectedRef, PathBuf>,
) -> Result<BindingLaunchConfig> {
    if !opencode_schema_supported(descriptor) {
        return Err(Error::new(
            "MODULE_CONFIG_SCHEMA_MISMATCH",
            "OpenCode route values require the exact registered native-options schema",
        ));
    }
    let object = options.as_object().ok_or_else(|| {
        Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode route options must be an object",
        )
    })?;
    let schema_version = descriptor
        .config_schema
        .as_ref()
        .map(|schema| schema.version.as_str())
        .ok_or_else(|| {
            Error::new(
                "MODULE_CONFIG_SCHEMA_MISMATCH",
                "OpenCode route values require the exact registered native-options schema",
            )
        })?;
    let has_owner_service = object.contains_key(OWNER_ROUTE_KEY);
    let has_owner_nonce = object.contains_key(OWNER_NONCE_KEY);
    if schema_version == OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V4 {
        return open_code_route_config_v4(object);
    }
    // External schema-v3 routes have no caller-supplied version pin. Retained
    // v1/v2 routes keep theirs, and fresh-owner projections keep the version
    // derived from the retained owner contract for its explicit compatibility check.
    let carries_expected_version = has_owner_service
        || !matches!(
            schema_version,
            OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V3 | OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V4
        );
    let base_allowed: &[&str] = if carries_expected_version {
        &[
            "service_id",
            "connection_file",
            "expected_version",
            "directory",
            "model",
        ]
    } else {
        &["service_id", "connection_file", "directory", "model"]
    };
    if has_owner_service != has_owner_nonce
        || object.keys().any(|key| {
            !base_allowed.contains(&key.as_str())
                && key != OWNER_ROUTE_KEY
                && key != OWNER_NONCE_KEY
        })
        || object.len() != base_allowed.len() + usize::from(has_owner_service) * 2
    {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode route options contain an unknown or incomplete owner projection",
        ));
    }
    if has_owner_service && !opencode_owner_schema(descriptor) {
        return Err(Error::new(
            "MODULE_CONFIG_SCHEMA_MISMATCH",
            "fresh owned OpenCode values require the owner-native options schema",
        ));
    }
    let model = object["model"].as_object().ok_or_else(|| {
        Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode model options must be an object",
        )
    })?;
    if model
        .keys()
        .any(|key| !["id", "providerID", "variant"].contains(&key.as_str()))
        || model.len() != 3
    {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode model options must contain id, providerID, and variant only",
        ));
    }
    let configured_service_id = required_config_string(&object["service_id"], "service_id", 128)?;
    if !valid_service_id(&configured_service_id) {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode service_id is invalid",
        ));
    }
    let configured_connection_file =
        required_config_string(&object["connection_file"], "connection_file", 4096)?;
    let expected_version = if carries_expected_version {
        Some(required_config_string(
            &object["expected_version"],
            "expected_version",
            256,
        )?)
    } else {
        None
    };
    let directory = required_config_string(&object["directory"], "directory", 4096)?;
    if !Path::new(&configured_connection_file).is_absolute() || !Path::new(&directory).is_absolute()
    {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode connection_file and directory values must be absolute paths",
        ));
    }
    let model_id = required_config_string(&model["id"], "model.id", 256)?;
    let provider_id = required_config_string(&model["providerID"], "model.providerID", 256)?;
    let variant = required_config_string(&model["variant"], "model.variant", 256)?;

    let mut values = BTreeMap::from([
        (
            "OPENCODE_SERVICE_ID".to_owned(),
            LaunchValue::Literal(configured_service_id.clone()),
        ),
        (
            "OPENCODE_CONNECTION_FILE".to_owned(),
            LaunchValue::Literal(configured_connection_file.clone()),
        ),
        (
            "OPENCODE_DIRECTORY".to_owned(),
            LaunchValue::Literal(directory.clone()),
        ),
        (
            "OPENCODE_MODEL_ID".to_owned(),
            LaunchValue::Literal(model_id.clone()),
        ),
        (
            "OPENCODE_PROVIDER_ID".to_owned(),
            LaunchValue::Literal(provider_id.clone()),
        ),
        (
            "OPENCODE_VARIANT".to_owned(),
            LaunchValue::Literal(variant.clone()),
        ),
    ]);

    // Schema v3 intentionally removes expected_version from launch config.
    if !matches!(
        schema_version,
        OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V3 | OPENCODE_NATIVE_OPTIONS_SCHEMA_VERSION_V4
    ) {
        let expected_version = expected_version.as_ref().ok_or_else(|| {
            Error::new(
                "MODULE_CONFIG_INVALID",
                "retained OpenCode schema requires expected_version",
            )
        })?;
        values.insert(
            "OPENCODE_EXPECTED_VERSION".to_owned(),
            LaunchValue::Literal(expected_version.clone()),
        );
    }

    if has_owner_service {
        let owner: OwnedRouteConfig = serde_json::from_value(object[OWNER_ROUTE_KEY].clone())
            .map_err(|_| {
                Error::new(
                    "MODULE_CONFIG_INVALID",
                    "owned OpenCode route declaration has an invalid shape",
                )
            })?;
        let owner_nonce = required_config_string(&object[OWNER_NONCE_KEY], "owner_nonce", 64)?;
        let uuid = Uuid::parse_str(&owner_nonce).ok();
        if owner.origin != "fresh_owned_service"
            || uuid.is_none_or(|value| value.hyphenated().to_string() != owner_nonce)
            || owner.model.id != model_id
            || owner.model.provider_id != provider_id
            || owner.model.variant != variant
            || !valid_provider_id(&provider_id)
            || !valid_service_id(&owner.service_id)
            || !matches!(owner.model_catalog.as_str(), "offline" | "refresh")
            || !absolute_plain_path(&owner.bun_executable)
            || !absolute_plain_path(&owner.server_program)
            || !absolute_plain_path(&owner.state_root)
            || !is_lower_sha256(&owner.bun_sha256)
            || !is_lower_sha256(&owner.server_program_sha256)
            || expected_version.as_deref() != Some(OPENCODE_OWNED_SERVICE_VERSION)
        {
            return Err(Error::new(
                "MODULE_CONFIG_INVALID",
                "owned OpenCode route does not retain the exact configured owner",
            ));
        }
        let owner_digest = sha256_hex(owner_nonce.as_bytes());
        let owner_suffix = &owner_digest[..16];
        let owner_service_id = format!(
            "{}-{owner_suffix}",
            owner.service_id.chars().take(108).collect::<String>()
        );
        let owner_state_root = owner.state_root.join("launches").join(&owner_nonce);
        let owner_connection_file = owner_state_root.join("connection.json");
        let owner_password_file = owner_state_root.join("server.password");
        if configured_service_id != owner_service_id
            || Path::new(&configured_connection_file) != owner_connection_file
        {
            return Err(Error::new(
                "MODULE_CONFIG_SCOPE_MISMATCH",
                "owned OpenCode native options differ from the retained Store owner nonce",
            ));
        }
        for (key, value) in [
            ("OPENCODE_OWNER_ORIGIN", owner.origin),
            ("OPENCODE_OWNER_NONCE", owner_nonce),
            (
                "OPENCODE_OWNER_BUN_EXECUTABLE",
                path_text(&owner.bun_executable)?,
            ),
            ("OPENCODE_OWNER_BUN_SHA256", owner.bun_sha256),
            (
                "OPENCODE_OWNER_SERVER_PROGRAM",
                path_text(&owner.server_program)?,
            ),
            (
                "OPENCODE_OWNER_SERVER_PROGRAM_SHA256",
                owner.server_program_sha256,
            ),
            ("OPENCODE_OWNER_STATE_ROOT", path_text(&owner_state_root)?),
            (
                "OPENCODE_OWNER_PASSWORD_FILE",
                path_text(&owner_password_file)?,
            ),
            ("OPENCODE_OWNER_PORT", owner.port.to_string()),
            ("OPENCODE_OWNER_MODEL_CATALOG", owner.model_catalog),
        ] {
            values.insert(key.to_owned(), LaunchValue::Literal(value));
        }
    }
    let config = BindingLaunchConfig { values };
    config.validate().map_err(module_error)?;
    Ok(config)
}

/// Current v4 external projection. Only service, connection and directory are
/// mandatory; the native harness supplies provider/model/variant defaults.
/// An explicit caller override must be a complete `model` object with exactly
/// id, providerID and variant; partial, malformed, owner or provider-auth
/// fields are rejected with no auto-fill or global-config access.
fn open_code_route_config_v4(
    object: &serde_json::Map<String, Value>,
) -> Result<BindingLaunchConfig> {
    if object.contains_key(OWNER_ROUTE_KEY) || object.contains_key(OWNER_NONCE_KEY) {
        return Err(Error::new(
            "MODULE_CONFIG_SCHEMA_MISMATCH",
            "fresh owned OpenCode values require the owner-native options schema",
        ));
    }
    if object
        .keys()
        .any(|key| !["service_id", "connection_file", "directory", "model"].contains(&key.as_str()))
        || (object.len() != 3 && object.len() != 4)
    {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode route options contain an unknown or incomplete owner projection",
        ));
    }
    let configured_service_id = required_config_string(&object["service_id"], "service_id", 128)?;
    if !valid_service_id(&configured_service_id) {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode service_id is invalid",
        ));
    }
    let configured_connection_file =
        required_config_string(&object["connection_file"], "connection_file", 4096)?;
    let directory = required_config_string(&object["directory"], "directory", 4096)?;
    if !Path::new(&configured_connection_file).is_absolute() || !Path::new(&directory).is_absolute()
    {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode connection_file and directory values must be absolute paths",
        ));
    }
    let mut values = BTreeMap::from([
        (
            "OPENCODE_SERVICE_ID".to_owned(),
            LaunchValue::Literal(configured_service_id),
        ),
        (
            "OPENCODE_CONNECTION_FILE".to_owned(),
            LaunchValue::Literal(configured_connection_file),
        ),
        (
            "OPENCODE_DIRECTORY".to_owned(),
            LaunchValue::Literal(directory),
        ),
    ]);
    if let Some(model_value) = object.get("model") {
        let model = model_value.as_object().ok_or_else(|| {
            Error::new(
                "MODULE_CONFIG_INVALID",
                "OpenCode model options must be an object",
            )
        })?;
        if model
            .keys()
            .any(|key| !["id", "providerID", "variant"].contains(&key.as_str()))
            || model.len() != 3
        {
            return Err(Error::new(
                "MODULE_CONFIG_INVALID",
                "OpenCode model options must contain id, providerID, and variant only",
            ));
        }
        let model_id = required_config_string(&model["id"], "model.id", 256)?;
        let provider_id = required_config_string(&model["providerID"], "model.providerID", 256)?;
        let variant = required_config_string(&model["variant"], "model.variant", 256)?;
        for (key, value) in [
            ("OPENCODE_MODEL_ID", model_id),
            ("OPENCODE_PROVIDER_ID", provider_id),
            ("OPENCODE_VARIANT", variant),
        ] {
            values.insert(key.to_owned(), LaunchValue::Literal(value));
        }
    }
    let config = BindingLaunchConfig { values };
    config.validate().map_err(module_error)?;
    Ok(config)
}

fn required_config_string(value: &Value, field: &str, max: usize) -> Result<String> {
    let text = value
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= max && !text.chars().any(char::is_control))
        .ok_or_else(|| {
            Error::new(
                "MODULE_CONFIG_INVALID",
                format!("OpenCode {field} is missing or invalid"),
            )
        })?;
    Ok(text.to_owned())
}

fn valid_service_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn absolute_plain_path(path: &Path) -> bool {
    path.is_absolute()
        && path.to_str().is_some_and(|value| !value.is_empty())
        && !path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn sha256_hex(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str().map(ToOwned::to_owned).ok_or_else(|| {
        Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode owner path is not valid UTF-8",
        )
    })
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct DemandKey {
    module_id: String,
    scope: ServiceScope,
    operation_id: String,
}

#[derive(Clone, PartialEq, Eq)]
struct HealthSnapshot {
    state: String,
    consecutive_failures: u32,
    error_code: Option<String>,
    retry_in_ms: Option<u64>,
}

struct PendingStatusObservation {
    event: ModuleSupervisorObservation,
    status: SupervisorStatus,
    fingerprint: String,
}

/// Independent lifecycle process. It performs the same exact credential,
/// resolver, operation-readback, and owner-helper ordering as the embedded
/// host actor while keeping all durable authority behind `SupervisorControlClient`.
pub struct StandaloneSupervisor {
    control: SupervisorControlClient,
    registry: Arc<SupervisorRegistry>,
    resolver: Arc<ResolverMapDirectory>,
    config: StandaloneSupervisorConfig,
    launch_config: Arc<dyn LaunchConfigProvider>,
    actor_instance_id: String,
    hot_retained_paths: Mutex<BTreeSet<PathBuf>>,
}

impl StandaloneSupervisor {
    pub async fn start(config: StandaloneSupervisorConfig) -> Result<Self> {
        let provider = Arc::new(StaticLaunchConfigProvider {
            entries: config.launch_configs.clone(),
            mapper: config.route_config_mapper,
            protected_files: config.protected_files.clone(),
        });
        Self::start_with_provider(config, provider).await
    }

    pub async fn start_from_bootstrap(bootstrap: SupervisorBootstrap) -> Result<Self> {
        bootstrap.validate_for_child()?;
        let actor_instance_id = bootstrap.actor_instance_id.ok_or_else(|| {
            Error::new(
                "MODULE_ACTOR_ADMISSION_REQUIRED",
                "host did not admit an actor UUID before supervisor bootstrap",
            )
        })?;
        let config = bootstrap.config;
        let provider = Arc::new(StaticLaunchConfigProvider {
            entries: config.launch_configs.clone(),
            mapper: config.route_config_mapper,
            protected_files: config.protected_files.clone(),
        });
        Self::start_with_actor(config, actor_instance_id, provider).await
    }

    pub async fn start_with_provider(
        config: StandaloneSupervisorConfig,
        launch_config: Arc<dyn LaunchConfigProvider>,
    ) -> Result<Self> {
        // This convenience entry point remains useful to embedded callers.
        // Store's retained actor fence rejects its locally generated UUID
        // unless the host has admitted that exact actor.
        Self::start_with_actor(config, Uuid::new_v4().to_string(), launch_config).await
    }

    async fn start_with_actor(
        config: StandaloneSupervisorConfig,
        actor_instance_id: String,
        launch_config: Arc<dyn LaunchConfigProvider>,
    ) -> Result<Self> {
        config.validate()?;
        if Uuid::parse_str(&actor_instance_id).is_err() {
            return Err(Error::invalid("supervisor actor admission ID is invalid"));
        }
        create_private_directory(&config.state_root)?;
        create_private_directory(&config.resolver_root)?;
        let mut catalog = DescriptorCatalog::default();
        for path in &config.descriptor_files {
            let descriptor =
                load_installed_descriptor(path, &config.install_root).map_err(|error| {
                    Error::new(
                        "MODULE_DESCRIPTOR_UNAVAILABLE",
                        format!("descriptor load failed: {}", error.code),
                    )
                })?;
            catalog = extend_descriptor_catalog(&catalog, descriptor)
                .map_err(|error| Error::new("MODULE_CATALOG_INVALID", error.to_string()))?;
        }
        let control = SupervisorControlClient::new(
            config.root.clone(),
            config.supervisor_credential.clone(),
            config.ipc.clone(),
        )?;
        let (admission, _) = watch::channel(AdmissionState::Closed {
            fault: KernelFault::StoreUnavailable,
        });
        await_host_admission(&control, &admission).await?;
        let resolver = Arc::new(ResolverMapDirectory::new(config.resolver_root.clone())?);
        let registry = Arc::new(
            SupervisorRegistry::new_with_admission(SupervisorRegistryConfig {
                catalog,
                state_root: config.state_root.clone(),
                ipc_root: config.root.clone(),
                supervisor_credential: config.supervisor_credential.clone(),
                ipc: config.ipc.clone(),
                owner_executable: config.owner_executable(),
                resolver: resolver.clone(),
                admission,
            })
            .map_err(module_error)?,
        );
        for descriptor in registry.descriptors() {
            registry
                .register_descriptor(&descriptor)
                .await
                .map_err(module_error)?;
        }
        Ok(Self {
            control,
            registry,
            resolver,
            config,
            launch_config,
            actor_instance_id,
            hot_retained_paths: Mutex::new(BTreeSet::new()),
        })
    }

    /// Run until the embedding host closes the supplied stop channel. The
    /// standalone binary supplies a channel that remains open; a host process
    /// handoff can retain the sender and stop this loop after its own listener
    /// shutdown and scope reconciliation.
    pub async fn run(&self, mut stopping: watch::Receiver<bool>) -> Result<()> {
        let mut tick = time::interval(RECONCILE_INTERVAL);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut recovered = false;
        let mut held = HashMap::<DemandKey, DemandLease>::new();
        let mut last_status = HashMap::<(String, ServiceScope), String>::new();
        let mut pending = VecDeque::<PendingStatusObservation>::new();
        let mut delivery_quarantines = HashMap::<(String, ServiceScope), String>::new();
        let mut sequence = 0_u64;
        let mut last_health = None::<HealthSnapshot>;
        let mut consecutive_failures = 0_u32;
        loop {
            if *stopping.borrow() {
                return Ok(());
            }
            let mut cycle_error = None::<String>;
            let mut scoped_error = delivery_quarantines.values().next().cloned();
            let mut saw_demand = false;
            match self.control.admission().await {
                Ok(AdmissionState::Open) => {
                    if !recovered {
                        match self.control.reconcile_recovery_page().await {
                            Ok(()) => {
                                self.registry.reopen_durable_admission_after_recovery();
                                recovered = true;
                            }
                            Err(error) => {
                                self.registry
                                    .close_durable_admission(KernelFault::StoreUnavailable);
                                cycle_error = Some(error.code.clone());
                                eprintln!("module supervisor recovery: {}", error.code);
                            }
                        }
                    }
                    if recovered {
                        match self.reconcile_demands(&mut held, &mut scoped_error).await {
                            Ok(found) => saw_demand = found,
                            Err(error) => {
                                self.registry
                                    .close_durable_admission(KernelFault::StoreUnavailable);
                                recovered = false;
                                cycle_error = Some(error.code.clone());
                                eprintln!("module supervisor demand readback: {}", error.code);
                            }
                        }
                    }
                    if recovered {
                        match self.reconcile_install_requests().await {
                            Ok(found) => saw_demand |= found,
                            Err(error) => {
                                self.registry
                                    .close_durable_admission(KernelFault::StoreUnavailable);
                                recovered = false;
                                cycle_error = Some(error.code.clone());
                                eprintln!("module supervisor install handoff: {}", error.code);
                            }
                        }
                    }
                }
                Ok(AdmissionState::Closed { fault }) => {
                    self.registry.close_durable_admission(fault);
                    recovered = false;
                    cycle_error = Some(kernel_fault_code(fault).to_owned());
                }
                Err(error) => {
                    self.registry
                        .close_durable_admission(KernelFault::StoreUnavailable);
                    recovered = false;
                    cycle_error = Some(error.code.clone());
                    eprintln!("module supervisor admission: {}", error.code);
                }
            }
            if let Some(error_code) = scoped_error {
                cycle_error.get_or_insert(error_code);
            }
            match self
                .collect_status_events(
                    &mut last_status,
                    &mut pending,
                    &mut sequence,
                    &mut delivery_quarantines,
                )
                .await
            {
                Err(error) => {
                    self.registry
                        .close_durable_admission(KernelFault::StoreUnavailable);
                    cycle_error.get_or_insert_with(|| error.code.clone());
                    eprintln!("module supervisor observation: {}", error.code);
                }
                Ok(Some(error_code)) => {
                    cycle_error.get_or_insert(error_code);
                }
                Ok(None) => {}
            }
            if let Some(ref error_code) = cycle_error {
                consecutive_failures = consecutive_failures.saturating_add(1).min(32);
                self.publish_health(
                    &mut last_health,
                    "retry_wait",
                    consecutive_failures,
                    Some(error_code),
                    Some(RECONCILE_INTERVAL),
                )
                .await?;
            } else if recovered {
                consecutive_failures = 0;
                self.publish_health(&mut last_health, "running", 0, None, None)
                    .await?;
            }
            if recovered
                && cycle_error.is_none()
                && !saw_demand
                && held.is_empty()
                && pending.is_empty()
                && !self.registry_has_obligations().await
            {
                // Registry descriptors are retained in memory for this
                // process, but no demand, owner, readback, or observation
                // obligation needs a permanent safety-only poller. The host
                // demand watch launches a fresh sibling for future demand.
                return Ok(());
            }
            tokio::select! {
                _ = tick.tick() => {}
                changed = stopping.changed() => {
                    if changed.is_err() || *stopping.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn run_forever(&self) -> Result<()> {
        let (_stop, stopping) = watch::channel(false);
        self.run(stopping).await
    }

    pub fn registry(&self) -> &SupervisorRegistry {
        &self.registry
    }

    async fn publish_health(
        &self,
        last: &mut Option<HealthSnapshot>,
        state: &str,
        consecutive_failures: u32,
        error_code: Option<&str>,
        retry: Option<Duration>,
    ) -> Result<()> {
        let safe_error_code = error_code.map(safe_health_code);
        let retry_in_ms = retry.map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        let next = HealthSnapshot {
            state: state.to_owned(),
            consecutive_failures,
            error_code: safe_error_code.clone(),
            retry_in_ms,
        };
        if last.as_ref() == Some(&next) {
            return Ok(());
        }
        match self
            .control
            .record_health(
                &next.state,
                next.consecutive_failures,
                next.error_code.as_deref(),
                next.retry_in_ms,
            )
            .await
        {
            Ok(()) => *last = Some(next),
            Err(_) => {
                if let Some(primary_code) = safe_error_code {
                    eprintln!(
                        "standalone module supervisor: {{\"error\":{{\"code\":\"{primary_code}\",\"secondary_codes\":[\"MODULE_HEALTH_WRITE_FAILED\"],\"phase\":\"module_supervisor\"}}}}"
                    );
                    return Err(Error::new(
                        primary_code,
                        "module supervisor could not persist health while reporting a reconciliation failure",
                    ));
                }
                return Err(Error::new(
                    "MODULE_HEALTH_WRITE_FAILED",
                    "standalone module supervisor could not persist health",
                ));
            }
        }
        Ok(())
    }

    /// Confirm only the exact boot retained by Store's accepted module.hello.
    /// A missing, stale, or mismatched identity leaves the registry's hello
    /// gate closed; transport connection or process presence is never enough.
    pub async fn confirm_module_hello(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
    ) -> Result<()> {
        bounded_text(module_id, "module_id", 128)?;
        bounded_text(boot_id, "boot_id", 256)?;
        scope.validate().map_err(module_error)?;
        self.registry
            .confirm_module_hello(module_id, scope, boot_id)
            .await
            .map_err(module_error)
    }

    fn check_hot_bootstrap_capacity(&self, candidate: &Path) -> Result<()> {
        let prior = self
            .hot_retained_paths
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| {
                Error::new(
                    "MODULE_SUPERVISOR_STATE_INVALID",
                    "retained bootstrap state is unavailable",
                )
            })?;
        self.config
            .ensure_hot_descriptor_restartable(&self.actor_instance_id, &prior, candidate)
    }

    fn note_hot_bootstrap_admitted(&self, candidate: &Path) {
        if let Ok(mut guard) = self.hot_retained_paths.lock() {
            guard.insert(candidate.to_path_buf());
        }
    }

    fn refresh_hot_bootstrap_installed_paths(
        &self,
        page: &InstalledDescriptorRequestPage,
    ) -> Result<()> {
        let mut retained = self.hot_retained_paths.lock().map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_STATE_INVALID",
                "retained bootstrap state is unavailable",
            )
        })?;
        retain_installed_descriptor_paths(&mut retained, page);
        Ok(())
    }

    async fn reconcile_install_requests(&self) -> Result<bool> {
        // Refresh from durable Store state every pass. A completion may have
        // committed while its reply was lost, leaving the path absent from
        // this process's local ledger. If readback fails, `?` keeps the
        // queued candidate unresolved; it is never rejected from stale data.
        let page = self.control.install_requests(true).await?;
        let queued = page
            .requests
            .iter()
            .filter(|request| request.state == InstalledDescriptorRequestState::Queued)
            .cloned()
            .collect::<Vec<_>>();
        if queued.len() > 1 {
            return Err(Error::new(
                "MODULE_INSTALL_REQUEST_LIMIT",
                "Store returned more than one pending descriptor installation",
            ));
        }
        self.refresh_hot_bootstrap_installed_paths(&page)?;
        let Some(request) = queued.into_iter().next() else {
            return Ok(false);
        };

        let descriptor =
            match load_installed_descriptor(&request.descriptor_file, &self.config.install_root) {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    self.reject_install_request(&request.operation_id, &error.code)
                        .await?;
                    return Ok(true);
                }
            };
        // P2 hot-descriptor restart byte boundary: enforce the exact
        // serialized combined-bootstrap capacity BEFORE any Store
        // registration or catalog publication. A rejected too-large update
        // settles only its own Operation; catalog and live workers stay.
        if let Err(error) = self.check_hot_bootstrap_capacity(&request.descriptor_file) {
            self.reject_install_request(&request.operation_id, &error.code)
                .await?;
            return Ok(true);
        }
        let registration = match self
            .registry
            .register_installed_descriptor(&descriptor)
            .await
        {
            Ok(value) => value,
            Err(error) if permanent_install_error(&error.code) => {
                self.reject_install_request(&request.operation_id, &error.code)
                    .await?;
                return Ok(true);
            }
            Err(error) => return Err(error),
        };
        let registration: DescriptorRegistrationReceipt = serde_json::from_value(registration)
            .map_err(|_| {
                Error::new(
                    "MODULE_SUPERVISOR_RESPONSE_INVALID",
                    "Store registration receipt did not match its typed shape",
                )
            })?;
        let completion = CompleteInstalledDescriptorRequest {
            operation_id: request.operation_id.clone(),
            outcome: InstalledDescriptorCompletionOutcome::Installed,
            registration: Some(registration),
            error_code: None,
        };
        let receipt = self.control.complete_install_request(&completion).await?;
        if receipt.operation_id != request.operation_id
            || receipt.state != InstalledDescriptorOperationState::Settled
            || !matches!(
                receipt.outcome,
                InstalledDescriptorOperationOutcome::Installed
                    | InstalledDescriptorOperationOutcome::Unchanged
            )
        {
            return Err(Error::new(
                "MODULE_SUPERVISOR_RESPONSE_INVALID",
                "Store did not settle the exact installed descriptor Operation",
            ));
        }
        // Track the admitted path so later hot updates in this process
        // lifetime see cumulative restart bytes, not just spawn-time bytes.
        self.note_hot_bootstrap_admitted(&request.descriptor_file);
        Ok(true)
    }

    async fn reject_install_request(&self, operation_id: &str, error_code: &str) -> Result<()> {
        let completion = CompleteInstalledDescriptorRequest {
            operation_id: operation_id.to_owned(),
            outcome: InstalledDescriptorCompletionOutcome::Rejected,
            registration: None,
            error_code: Some(safe_install_error_code(error_code)),
        };
        let receipt = self.control.complete_install_request(&completion).await?;
        if receipt.operation_id != operation_id
            || receipt.state != InstalledDescriptorOperationState::Rejected
            || receipt.outcome != InstalledDescriptorOperationOutcome::Rejected
        {
            return Err(Error::new(
                "MODULE_SUPERVISOR_RESPONSE_INVALID",
                "Store did not reject the exact descriptor Operation",
            ));
        }
        Ok(())
    }
    async fn reconcile_demands(
        &self,
        held: &mut HashMap<DemandKey, DemandLease>,
        scoped_error: &mut Option<String>,
    ) -> Result<bool> {
        let mut cursor = None::<ModuleDemandCursor>;
        let mut seen = HashSet::<DemandKey>::new();
        let mut saw_demand = false;
        loop {
            let page = self.control.demand_page(cursor.as_ref()).await?;
            if page.truncated && page.next_cursor.is_none() {
                return Err(Error::new(
                    "MODULE_DEMAND_CURSOR_MISSING",
                    "module demand page omitted its continuation cursor",
                ));
            }
            if !page.truncated && page.next_cursor.is_some() {
                return Err(Error::new(
                    "MODULE_DEMAND_CURSOR_UNEXPECTED",
                    "module demand page returned a cursor without truncation",
                ));
            }
            for blocked in &page.blocked {
                saw_demand = true;
                // The durable Store already owns the blocked-demand reason;
                // keep this process diagnostic bounded to its typed code.
                eprintln!(
                    "module demand blocked {}:{} {}",
                    blocked.binding_id, blocked.generation, blocked.error_code
                );
            }
            for demand in page.demands {
                saw_demand = true;
                let key = demand_key(&demand);
                seen.insert(key.clone());
                if held.contains_key(&key) {
                    let scope = key.scope.clone();
                    let readback = match self
                        .control
                        .scope_readback(
                            &key.module_id,
                            &scope.binding_id,
                            i64::try_from(scope.generation)
                                .map_err(|_| Error::invalid("binding generation overflow"))?,
                        )
                        .await
                    {
                        Ok(readback) => readback,
                        Err(error) if is_control_failure(&error) => return Err(error),
                        Err(error) => {
                            eprintln!("module scope readback: {}", error.code);
                            continue;
                        }
                    };
                    let operations = match operation_readback_for_scope(&scope, &readback) {
                        Ok(operations) => operations,
                        Err(error) => {
                            eprintln!("module operation readback: {}", error.code);
                            continue;
                        }
                    };
                    if let Err(error) = self
                        .registry
                        .apply_operation_readback(&key.module_id, &scope, operations)
                        .await
                        .map_err(module_error)
                    {
                        eprintln!("module operation apply: {}", error.code);
                    } else if let Some(boot_id) = readback.module_hello_boot_id.as_deref()
                        && let Err(error) = self
                            .confirm_module_hello(&key.module_id, &scope, boot_id)
                            .await
                    {
                        if is_control_failure(&error) {
                            return Err(error);
                        }
                        self.registry
                            .record_module_hello_failure(
                                &key.module_id,
                                &scope,
                                boot_id,
                                &error.code,
                            )
                            .await
                            .map_err(module_error)?;
                        scoped_error.get_or_insert_with(|| safe_health_code(&error.code));
                    }
                    continue;
                }
                let scope = ServiceScope {
                    binding_id: demand.binding_id.clone(),
                    generation: demand.generation,
                };
                let stored = match self
                    .control
                    .scope_readback(
                        &demand.module_id,
                        &scope.binding_id,
                        i64::try_from(scope.generation)
                            .map_err(|_| Error::invalid("binding generation overflow"))?,
                    )
                    .await
                {
                    Ok(readback) => readback,
                    Err(error) if is_control_failure(&error) => return Err(error),
                    Err(error) => {
                        eprintln!("module new-scope readback: {}", error.code);
                        continue;
                    }
                };
                let readback = match operation_readback_for_scope(&scope, &stored) {
                    Ok(readback) => readback,
                    Err(error) => {
                        eprintln!("module new-scope readback: {}", error.code);
                        continue;
                    }
                };
                if self
                    .registry
                    .status(&demand.module_id, &scope)
                    .await
                    .is_some()
                {
                    if let Err(error) = self
                        .registry
                        .apply_operation_readback(&demand.module_id, &scope, readback.clone())
                        .await
                        .map_err(module_error)
                    {
                        eprintln!("module new-scope apply: {}", error.code);
                        continue;
                    }
                    if let Some(boot_id) = stored.module_hello_boot_id.as_deref()
                        && let Err(error) = self
                            .confirm_module_hello(&demand.module_id, &scope, boot_id)
                            .await
                    {
                        if is_control_failure(&error) {
                            return Err(error);
                        }
                        self.registry
                            .record_module_hello_failure(
                                &demand.module_id,
                                &scope,
                                boot_id,
                                &error.code,
                            )
                            .await
                            .map_err(module_error)?;
                        scoped_error.get_or_insert_with(|| safe_health_code(&error.code));
                    }
                }
                match self.start_demand(&demand, readback).await {
                    Ok(lease) => {
                        held.insert(key, lease);
                    }
                    Err(error) if is_control_failure(&error) => return Err(error),
                    Err(error) => {
                        eprintln!("module demand isolated: {}", error.code);
                    }
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let stale = held
            .keys()
            .filter(|key| !seen.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in stale {
            let scope = key.scope.clone();
            let readback = match self
                .control
                .scope_readback(
                    &key.module_id,
                    &scope.binding_id,
                    i64::try_from(scope.generation)
                        .map_err(|_| Error::invalid("binding generation overflow"))?,
                )
                .await
            {
                Ok(readback) => readback,
                Err(error) if is_control_failure(&error) => return Err(error),
                Err(error) => {
                    eprintln!("module stale-scope readback: {}", error.code);
                    continue;
                }
            };
            let operations = match operation_readback_for_scope(&scope, &readback) {
                Ok(operations) => operations,
                Err(error) => {
                    eprintln!("module stale-scope readback: {}", error.code);
                    continue;
                }
            };
            if let Err(error) = self
                .registry
                .apply_operation_readback(&key.module_id, &scope, operations)
                .await
                .map_err(module_error)
            {
                eprintln!("module stale-scope apply: {}", error.code);
                continue;
            }
            if let Some(boot_id) = readback.module_hello_boot_id.as_deref()
                && let Err(error) = self
                    .confirm_module_hello(&key.module_id, &scope, boot_id)
                    .await
            {
                if is_control_failure(&error) {
                    return Err(error);
                }
                self.registry
                    .record_module_hello_failure(&key.module_id, &scope, boot_id, &error.code)
                    .await
                    .map_err(module_error)?;
                scoped_error.get_or_insert_with(|| safe_health_code(&error.code));
            }
            let pending = readback
                .operations
                .iter()
                .any(|operation| is_pending_operation(&operation.state));
            if !readback.native_identity_retained && !pending {
                let _ = held.remove(&key);
            }
        }
        Ok(saw_demand)
    }

    async fn registry_has_obligations(&self) -> bool {
        Self::registry_statuses_have_obligations(self.registry.statuses().await)
    }

    fn registry_statuses_have_obligations(
        statuses: impl IntoIterator<Item = SupervisorStatus>,
    ) -> bool {
        statuses
            .into_iter()
            .any(|status| Self::registry_status_has_obligations(&status))
    }

    fn registry_status_has_obligations(status: &SupervisorStatus) -> bool {
        Self::has_unsettled_process_custody(
            &status.lifecycle,
            status.owner.is_some(),
            status.worker.is_some(),
        ) || status.readback_required
            || status.unknown_operation_count != 0
            || !status.unknown_operation_ids.is_empty()
            || (matches!(
                status.effect_certainty,
                crate::ModuleEffectCertainty::Unknown
            ) && !Self::has_exact_departed_worker(status))
    }

    #[cfg(test)]
    pub(crate) fn registry_statuses_have_obligations_for_test(
        statuses: impl IntoIterator<Item = SupervisorStatus>,
    ) -> bool {
        Self::registry_statuses_have_obligations(statuses)
    }

    fn has_exact_departed_worker(status: &SupervisorStatus) -> bool {
        matches!(
            &status.lifecycle,
            crate::LifecycleState::Completed { .. }
                | crate::LifecycleState::ProcessExited {
                    exit_proven: true,
                    ..
                }
        ) && status.owner.is_some()
            && status.worker.is_some()
            && status
                .worker_boot_id
                .as_deref()
                .is_some_and(|boot_id| Uuid::parse_str(boot_id).is_ok())
    }

    fn has_unsettled_process_custody(
        lifecycle: &crate::LifecycleState,
        owner_retained: bool,
        worker_retained: bool,
    ) -> bool {
        (owner_retained || worker_retained)
            && !matches!(
                lifecycle,
                crate::LifecycleState::Completed { .. }
                    | crate::LifecycleState::ProcessExited {
                        exit_proven: true,
                        ..
                    }
            )
    }

    async fn start_demand(
        &self,
        demand: &ModuleDemandRecord,
        readback: OperationReadback,
    ) -> Result<DemandLease> {
        if !matches!(self.registry.admission_state(), AdmissionState::Open) {
            return Err(Error::new(
                "KERNEL_ADMISSION_CLOSED",
                "module start is blocked by closed host admission",
            ));
        }
        let launch_config = self
            .launch_config
            .for_binding(&demand.descriptor, &demand.route_native_options)?;
        let provisioned = self
            .control
            .ensure_binding_credential(
                &demand.operation_id,
                &demand.binding_id,
                i64::try_from(demand.generation)
                    .map_err(|_| Error::invalid("binding generation overflow"))?,
            )
            .await?;
        validate_provisioned(&provisioned, demand)?;
        let credential_ref = demand
            .descriptor
            .launch
            .credential_ref
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "MODULE_CREDENTIAL_REF_REQUIRED",
                    "descriptor omits a protected binding credential reference",
                )
            })?;
        if &provisioned.credential_ref != credential_ref {
            return Err(Error::new(
                "MODULE_CREDENTIAL_SCOPE_MISMATCH",
                "provisioned credential reference differs from the retained descriptor",
            ));
        }
        let claim = module_contract_claim(
            &demand.descriptor,
            ProtocolRange::exact(HOST_MODULE_PROTOCOL),
        )
        .map_err(module_error)?;
        let scope = ServiceScope {
            binding_id: demand.binding_id.clone(),
            generation: demand.generation,
        };
        let context = ProtectedResolverContext {
            module_id: demand.descriptor.module_id.clone(),
            artifact: demand.descriptor.artifact.clone(),
            scope: scope.clone(),
            protocol: HOST_MODULE_PROTOCOL,
        };
        self.resolver
            .publish_binding_map(BindingMapPublication {
                context: &context,
                descriptor: &demand.descriptor,
                launch_config: &launch_config,
                claim: &claim,
                credential_ref,
                credential_file: &provisioned.credential_file,
                credential_file_sha256: &provisioned.credential_file_sha256,
                additional_files: &self.config.protected_files,
            })
            .map_err(module_error)?;
        let ready = self
            .control
            .check_binding_credential_ready(
                &demand.operation_id,
                &demand.binding_id,
                i64::try_from(demand.generation)
                    .map_err(|_| Error::invalid("binding generation overflow"))?,
            )
            .await?;
        validate_provisioned(&ready, demand)?;
        if ready.credential_file != provisioned.credential_file
            || ready.credential_file_sha256 != provisioned.credential_file_sha256
            || ready.credential_ref != provisioned.credential_ref
        {
            return Err(Error::new(
                "MODULE_CREDENTIAL_CHANGED",
                "binding credential changed between resolver publication and helper start",
            ));
        }
        let selector = ArtifactSelector::new(
            demand.descriptor.module_id.clone(),
            demand.descriptor.artifact.artifact_id.clone(),
            demand.descriptor.artifact.version.clone(),
            demand.descriptor.launch.executable_sha256.clone(),
        );
        let required = BTreeSet::from([CapabilityId::new(demand.required_capability.clone())
            .map_err(|error| Error::new("MODULE_CAPABILITY_INVALID", error.to_string()))?]);
        self.registry
            .demand(crate::ModuleDemandRequest {
                selector,
                host_protocol: ProtocolRange::exact(HOST_MODULE_PROTOCOL),
                required_capabilities: required,
                scope,
                cause: DemandCause::Operation {
                    operation_id: demand.operation_id.clone(),
                },
                launch_config,
                module_client_id: ready.module_client_id,
                readback: Some(readback),
            })
            .await
            .map_err(module_error)
    }

    async fn collect_status_events(
        &self,
        last_status: &mut HashMap<(String, ServiceScope), String>,
        pending: &mut VecDeque<PendingStatusObservation>,
        sequence: &mut u64,
        quarantined: &mut HashMap<(String, ServiceScope), String>,
    ) -> Result<Option<String>> {
        let mut health_code = None;
        let mut overflow = false;
        for status in self.registry.observation_statuses().await {
            let key = (status.module_id.clone(), status.scope.clone());
            if quarantined.contains_key(&key) {
                continue;
            }
            let fingerprint = serde_json::to_string(&status)?;
            if last_status.get(&key) == Some(&fingerprint) {
                self.registry.acknowledge_ready_observation(&status).await;
                continue;
            }
            if pending.iter().any(|item| {
                item.event.module_id == status.module_id
                    && item.event.scope == status.scope
                    && item.fingerprint == fingerprint
            }) {
                continue;
            }
            let next = sequence.checked_add(1).ok_or_else(|| {
                Error::new(
                    "MODULE_OBSERVATION_SEQUENCE_EXHAUSTED",
                    "status sequence exhausted",
                )
            })?;
            if pending.len() >= MAX_STATUS_QUEUE {
                overflow = true;
                break;
            }
            let event =
                ModuleSupervisorObservation::from_status(&status, &self.actor_instance_id, next)?;
            *sequence = next;
            pending.push_back(PendingStatusObservation {
                event,
                status,
                fingerprint,
            });
        }
        while let Some(item) = pending.front() {
            match self.control.record_observation(&item.event).await {
                ObservationDeliveryDisposition::Committed
                | ObservationDeliveryDisposition::ExactDuplicate => {
                    if let Some(item) = pending.pop_front() {
                        let key = (item.status.module_id.clone(), item.status.scope.clone());
                        last_status.insert(key, item.fingerprint);
                        self.registry
                            .acknowledge_ready_observation(&item.status)
                            .await;
                    }
                }
                ObservationDeliveryDisposition::RetryableInfrastructureFailure { error_code } => {
                    return Err(Error::new(
                        error_code,
                        "module observation delivery is uncertain; the exact event remains queued",
                    ));
                }
                ObservationDeliveryDisposition::ScopedPermanentRejection { error_code } => {
                    let Some(front) = pending.front() else {
                        break;
                    };
                    let key = (front.status.module_id.clone(), front.status.scope.clone());
                    self.registry
                        .quarantine_scope(&key.0, &key.1, &error_code)
                        .await
                        .map_err(module_error)?;
                    if let Some(item) = pending.pop_front() {
                        last_status.insert(key.clone(), item.fingerprint);
                        quarantined.insert(key, error_code.clone());
                        health_code.get_or_insert(error_code);
                    }
                }
            }
        }
        if overflow {
            health_code.get_or_insert_with(|| "MODULE_OBSERVATION_QUEUE_FULL".to_owned());
        }
        Ok(health_code)
    }
}

fn retain_installed_descriptor_paths(
    retained: &mut BTreeSet<PathBuf>,
    page: &InstalledDescriptorRequestPage,
) {
    retained.extend(
        page.requests
            .iter()
            .filter(|request| request.state == InstalledDescriptorRequestState::Installed)
            .map(|request| request.descriptor_file.clone()),
    );
}

async fn await_host_admission(
    control: &SupervisorControlClient,
    admission: &watch::Sender<AdmissionState>,
) -> Result<()> {
    let mut last_error = None::<Error>;
    for _ in 0..STARTUP_ATTEMPTS {
        match control.admission().await {
            Ok(AdmissionState::Open) => {
                admission.send_replace(AdmissionState::Open);
                return Ok(());
            }
            Ok(state @ AdmissionState::Closed { .. }) => {
                admission.send_replace(state);
            }
            Err(error) if is_control_failure(&error) => {
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
        time::sleep(STARTUP_RETRY).await;
    }
    Err(last_error.unwrap_or_else(|| {
        Error::new(
            "MODULE_SUPERVISOR_STARTUP_TIMEOUT",
            "host IPC admission did not become open within the bounded startup window",
        )
    }))
}

fn safe_install_error_code(code: &str) -> String {
    if !code.is_empty()
        && code.len() <= 128
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code.to_owned()
    } else {
        "MODULE_INSTALL_REJECTED".to_owned()
    }
}

fn permanent_install_error(code: &str) -> bool {
    matches!(
        code,
        "MODULE_DESCRIPTOR_INVALID"
            | "MODULE_DESCRIPTOR_IMMUTABLE"
            | "MODULE_DESCRIPTOR_UNTRUSTED"
            | "MODULE_ARTIFACT_DIGEST_REQUIRED"
            | "MODULE_ARTIFACT_MISMATCH"
            | "MODULE_ARTIFACT_UNAVAILABLE"
            | "MODULE_SELECTION"
            | "MODULE_CATALOG_INVALID"
            | "MODULE_CATALOG_FULL"
            | "MODULE_DESCRIPTOR_REGISTER_RESPONSE"
            | "MODULE_DESCRIPTOR_LIMIT"
            | "MODULE_BOOTSTRAP_LIMIT"
    )
}
fn demand_key(demand: &ModuleDemandRecord) -> DemandKey {
    DemandKey {
        module_id: demand.module_id.clone(),
        scope: ServiceScope {
            binding_id: demand.binding_id.clone(),
            generation: demand.generation,
        },
        operation_id: demand.operation_id.clone(),
    }
}

fn operation_readback_for_scope(
    scope: &ServiceScope,
    stored: &crate::ModuleScopeReadback,
) -> Result<OperationReadback> {
    let operations = stored
        .operations
        .iter()
        .map(|operation| {
            Ok(OperationSnapshot {
                operation_id: operation.operation_id.clone(),
                binding_id: operation.binding_id.clone(),
                generation: operation.generation,
                state: operation.state.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let readback = OperationReadback {
        scope: scope.clone(),
        complete: true,
        operations,
    };
    readback.validate_for(scope).map_err(module_error)?;
    Ok(readback)
}

fn validate_provisioned(
    credential: &ModuleBindingCredential,
    demand: &ModuleDemandRecord,
) -> Result<()> {
    if !credential.ready
        || credential.operation_id != demand.operation_id
        || credential.binding_id != demand.binding_id
        || u64::try_from(credential.generation).ok() != Some(demand.generation)
        || credential.module_id.as_str() != demand.module_id
        || credential.artifact_id != demand.artifact_id
        || credential.artifact_version != demand.artifact_version
        || credential.build_id != demand.descriptor.artifact.build_id
        || credential.descriptor_revision != demand.descriptor_revision
        || credential.protocol != HOST_MODULE_PROTOCOL
        || demand
            .module_client_id
            .as_deref()
            .is_some_and(|id| id != credential.module_client_id)
        || demand
            .credential_ref
            .as_deref()
            .is_some_and(|reference| reference != credential.credential_ref.as_str())
    {
        return Err(Error::new(
            "MODULE_CREDENTIAL_SCOPE_MISMATCH",
            "credential readiness does not match the exact retained demand and descriptor",
        ));
    }
    Ok(())
}

fn is_pending_operation(state: &str) -> bool {
    matches!(
        state,
        "queued" | "sending" | "native_accepted" | "outcome_unknown"
    )
}

fn is_control_failure(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "HOST_UNAVAILABLE"
            | "IO_ERROR"
            | "STORE_CLOSED"
            | "STORE_ERROR"
            | "STORE_PANIC"
            | "MODULE_SUPERVISOR_RESPONSE_INVALID"
    )
}

fn kernel_fault_code(fault: KernelFault) -> &'static str {
    match fault {
        KernelFault::StoreUnavailable => "STORE_UNAVAILABLE",
        KernelFault::DurableJournalUnavailable => "DURABLE_JOURNAL_UNAVAILABLE",
    }
}

fn safe_health_code(code: &str) -> String {
    if !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code.to_owned()
    } else {
        "SUPERVISOR_ERROR".to_owned()
    }
}

fn module_error(error: crate::Error) -> Error {
    Error::new(error.code, error.message)
}

fn json_sha256(value: &Value) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value)?;
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_sha256(value: &str, field: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::invalid(format!("{field} must be SHA-256 hex")));
    }
    Ok(())
}

fn bounded_text(value: &str, field: &str, max: usize) -> Result<()> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(Error::invalid(format!("{field} is invalid")));
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::invalid("supervisor directory must be absolute"));
    }
    let mut chain = path.ancestors().collect::<Vec<_>>();
    chain.reverse();
    for component in chain
        .into_iter()
        .filter(|item| !item.as_os_str().is_empty())
    {
        match fs::symlink_metadata(component) {
            Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
                return Err(Error::new(
                    "MODULE_SUPERVISOR_PATH_INVALID",
                    "supervisor directories cannot traverse links or non-directories",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(component)?;
                let metadata = fs::symlink_metadata(component)?;
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    return Err(Error::new(
                        "MODULE_SUPERVISOR_PATH_INVALID",
                        "supervisor directory creation did not produce a regular directory",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    swarm_process::private_permissions(path, true)
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(test)]
mod opencode_schema_projection_tests {
    use super::{
        BindingLaunchConfig, Error, LaunchValue, ModuleDescriptor, OWNER_NONCE_KEY,
        OWNER_ROUTE_KEY, StandaloneRouteConfigMapper, map_route_config, sha256_hex,
    };
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
    };

    const OWNER_NONCE: &str = "11111111-1111-4111-8111-111111111111";
    const OTHER_OWNER_NONCE: &str = "22222222-2222-4222-8222-222222222222";

    fn descriptor_for_schema(version: &str) -> ModuleDescriptor {
        let (artifact_version, digest) = match version {
            "1" => (
                "0.1.0",
                "d597be6bae80dc82535b658b5daaf3037a09976e5704d799a6715a673a62f662",
            ),
            "2" => (
                "0.3.0",
                "7fc3136219b20d00570b65e5d4fe533e3ea042dadf53be3fdcdfa9781cf0eb68",
            ),
            "3" => (
                "0.5.0",
                "070d37891aed021d6a5023cd885647b1403741927e87a0cb28050f30b7c4d97e",
            ),
            "4" => ("0.7.0", super::OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V4),
            _ => panic!("unsupported test schema version"),
        };
        let mut descriptor: Value = serde_json::from_str(include_str!(
            "../../swarm-adapter-opencode/registration/descriptor.template.json"
        ))
        .unwrap();
        descriptor["artifact"]["version"] = json!(artifact_version);
        descriptor["config_schema"]["version"] = json!(version);
        descriptor["config_schema"]["sha256"] = json!(digest);
        serde_json::from_value(descriptor).unwrap()
    }

    fn fixture_root() -> PathBuf {
        std::env::temp_dir().join("eliot-swarm-supervisor-opencode-schema-tests")
    }

    fn path_text(path: &Path) -> String {
        path.to_str().unwrap().to_owned()
    }

    fn external_options() -> Value {
        let root = fixture_root();
        json!({
            "service_id": "opencode-external-fixture",
            "connection_file": path_text(&root.join("external/connection.json")),
            "expected_version": "2.0.7",
            "directory": path_text(&root.join("workspace")),
            "model": {
                "id": "openai/gpt-5",
                "providerID": "openai",
                "variant": "high"
            }
        })
    }

    fn v3_external_options() -> Value {
        let mut options = external_options();
        let _ = options.as_object_mut().unwrap().remove("expected_version");
        options
    }

    fn owned_options(owner_nonce: &str) -> Value {
        let root = fixture_root();
        let state_root = root.join("owned-state");
        let owner_service_id = format!(
            "opencode-owned-fixture-{}",
            &sha256_hex(owner_nonce.as_bytes())[..16]
        );
        let connection_file = state_root
            .join("launches")
            .join(owner_nonce)
            .join("connection.json");
        let mut options = external_options();
        options["service_id"] = json!(owner_service_id);
        options["connection_file"] = json!(path_text(&connection_file));

        let owner = json!({
            "origin": "fresh_owned_service",
            "service_id": "opencode-owned-fixture",
            "model": {
                "id": "openai/gpt-5",
                "providerID": "openai",
                "variant": "high"
            },
            "model_catalog": "offline",
            "bun_executable": path_text(&root.join("runtime/bun")),
            "bun_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "server_program": path_text(&root.join("runtime/server.js")),
            "server_program_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "state_root": path_text(&state_root),
            "port": 4317
        });
        let object = options.as_object_mut().unwrap();
        object.insert(OWNER_ROUTE_KEY.to_owned(), owner);
        object.insert(OWNER_NONCE_KEY.to_owned(), json!(owner_nonce));
        options
    }

    fn v4_defaults_options() -> Value {
        let root = fixture_root();
        json!({
            "service_id": "opencode-external-fixture",
            "connection_file": path_text(&root.join("external/connection.json")),
            "directory": path_text(&root.join("workspace"))
        })
    }

    fn project(
        descriptor: &ModuleDescriptor,
        options: &Value,
    ) -> std::result::Result<BindingLaunchConfig, Error> {
        map_route_config(
            StandaloneRouteConfigMapper::DescriptorSchema,
            descriptor,
            options,
            &BTreeMap::new(),
        )
    }

    fn literal<'a>(config: &'a BindingLaunchConfig, key: &str) -> &'a str {
        match &config.values[key] {
            LaunchValue::Literal(value) => value,
            other => panic!("{key} was not projected as a literal: {other:?}"),
        }
    }

    #[test]
    fn opencode_v4_projects_only_existing_service_values() {
        let descriptor = descriptor_for_schema("4");
        let defaults = project(&descriptor, &v4_defaults_options()).unwrap();
        assert_eq!(defaults.values.len(), 3);
        assert!(defaults.values.contains_key("OPENCODE_SERVICE_ID"));
        assert!(defaults.values.contains_key("OPENCODE_CONNECTION_FILE"));
        assert!(defaults.values.contains_key("OPENCODE_DIRECTORY"));
        assert!(
            !defaults
                .values
                .keys()
                .any(|key| key.starts_with("OPENCODE_OWNER_"))
        );
        assert!(!defaults.values.contains_key("OPENCODE_MODEL_ID"));
        assert!(!defaults.values.contains_key("OPENCODE_EXPECTED_VERSION"));

        let explicit = project(&descriptor, &v3_external_options()).unwrap();
        assert_eq!(explicit.values.len(), 6);
        assert_eq!(literal(&explicit, "OPENCODE_MODEL_ID"), "openai/gpt-5");
        assert_eq!(literal(&explicit, "OPENCODE_PROVIDER_ID"), "openai");
        assert_eq!(literal(&explicit, "OPENCODE_VARIANT"), "high");

        assert_eq!(
            project(&descriptor, &owned_options(OWNER_NONCE))
                .unwrap_err()
                .code,
            "MODULE_CONFIG_SCHEMA_MISMATCH"
        );
        assert_eq!(
            sha256_hex(include_bytes!(
                "../../swarm-adapter-opencode/registration/native-options.schema.json"
            )),
            super::OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256_V4
        );
    }

    #[test]
    fn opencode_v4_mapper_accepts_compatible_artifact_releases() {
        for artifact_version in ["0.7.1", "0.8.0"] {
            let mut descriptor = serde_json::to_value(descriptor_for_schema("4")).unwrap();
            descriptor["artifact"]["version"] = json!(artifact_version);
            let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
            let options = v4_defaults_options();
            let config = project(&descriptor, &options).unwrap();

            assert_eq!(
                config.values.len(),
                3,
                "artifact version {artifact_version}"
            );
            for (key, option) in [
                ("OPENCODE_SERVICE_ID", "service_id"),
                ("OPENCODE_CONNECTION_FILE", "connection_file"),
                ("OPENCODE_DIRECTORY", "directory"),
            ] {
                assert_eq!(
                    literal(&config, key),
                    options[option].as_str().unwrap(),
                    "artifact version {artifact_version}"
                );
            }
            assert_eq!(
                project(&descriptor, &owned_options(OWNER_NONCE))
                    .unwrap_err()
                    .code,
                "MODULE_CONFIG_SCHEMA_MISMATCH"
            );
        }
    }

    #[test]
    fn opencode_v4_mapper_rejects_wrong_identity_schema_and_digest() {
        let mut descriptor = serde_json::to_value(descriptor_for_schema("4")).unwrap();
        descriptor["artifact"]["version"] = json!("0.7.1");
        let options = v4_defaults_options();

        for (pointer, replacement) in [
            ("/module_id", json!("eliot.other.v2")),
            (
                "/artifact/artifact_id",
                json!("eliot-opencode-v2.rust-http.2"),
            ),
            ("/config_schema/schema_id", json!("other-native-options")),
            ("/config_schema/version", json!("5")),
            (
                "/config_schema/sha256",
                json!("0000000000000000000000000000000000000000000000000000000000000000"),
            ),
            ("/config_schema/sha256", Value::Null),
        ] {
            let mut wrong_descriptor = descriptor.clone();
            *wrong_descriptor.pointer_mut(pointer).unwrap() = replacement;
            let wrong_descriptor: ModuleDescriptor =
                serde_json::from_value(wrong_descriptor).unwrap();
            assert_eq!(
                project(&wrong_descriptor, &options).unwrap_err().code,
                "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                "descriptor field {pointer}"
            );
        }
    }

    #[test]
    fn opencode_legacy_mapper_retains_artifact_release_gates() {
        for schema_version in ["1", "2", "3"] {
            let mut descriptor =
                serde_json::to_value(descriptor_for_schema(schema_version)).unwrap();
            descriptor["artifact"]["version"] = json!("0.7.1");
            let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
            let options = if schema_version == "3" {
                v3_external_options()
            } else {
                external_options()
            };
            assert_eq!(
                project(&descriptor, &options).unwrap_err().code,
                "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                "schema version {schema_version}"
            );
        }
    }

    #[test]
    fn opencode_v4_preserves_exact_explicit_model_trio() {
        let descriptor = descriptor_for_schema("4");
        let mut options = v4_defaults_options();
        options["model"] = json!({
            "id": "ling-3.1-flash",
            "providerID": "inclusionai",
            "variant": "exact-selected-variant"
        });
        let config = project(&descriptor, &options).unwrap();
        assert_eq!(config.values.len(), 6);
        assert_eq!(literal(&config, "OPENCODE_MODEL_ID"), "ling-3.1-flash");
        assert_eq!(literal(&config, "OPENCODE_PROVIDER_ID"), "inclusionai");
        assert_eq!(
            literal(&config, "OPENCODE_VARIANT"),
            "exact-selected-variant"
        );
        assert_eq!(
            literal(&config, "OPENCODE_SERVICE_ID"),
            options["service_id"].as_str().unwrap()
        );
    }

    #[test]
    fn opencode_v4_rejects_partial_malformed_and_owner_provider_auth() {
        let descriptor = descriptor_for_schema("4");

        let mut partial = v4_defaults_options();
        partial["model"] = json!({"id": "openai/gpt-5", "providerID": "openai"});
        assert_eq!(
            project(&descriptor, &partial).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );

        let mut malformed = v4_defaults_options();
        malformed["model"] = json!({
            "id": "",
            "providerID": "openai",
            "variant": "high"
        });
        assert_eq!(
            project(&descriptor, &malformed).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );

        let mut extra = v4_defaults_options();
        extra["model"] = json!({
            "id": "openai/gpt-5",
            "providerID": "openai",
            "variant": "high",
            "extra": "nope"
        });
        assert_eq!(
            project(&descriptor, &extra).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );

        let mut provider_auth = v4_defaults_options();
        provider_auth["provider_auth"] = json!("secret");
        assert_eq!(
            project(&descriptor, &provider_auth).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );

        let mut versioned = v4_defaults_options();
        versioned["expected_version"] = json!("2.0.7");
        assert_eq!(
            project(&descriptor, &versioned).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );

        assert_eq!(
            project(&descriptor, &owned_options(OWNER_NONCE))
                .unwrap_err()
                .code,
            "MODULE_CONFIG_SCHEMA_MISMATCH"
        );
    }

    #[test]
    fn opencode_v3_external_projection_omits_undeclared_expected_version() {
        let descriptor = descriptor_for_schema("3");
        let config = project(&descriptor, &v3_external_options()).unwrap();

        assert!(!config.values.contains_key("OPENCODE_EXPECTED_VERSION"));
        assert_eq!(
            literal(&config, "OPENCODE_CONNECTION_FILE"),
            path_text(&fixture_root().join("external/connection.json"))
        );
        assert_eq!(
            literal(&config, "OPENCODE_DIRECTORY"),
            path_text(&fixture_root().join("workspace"))
        );
        assert_eq!(
            project(&descriptor, &external_options()).unwrap_err().code,
            "MODULE_CONFIG_INVALID"
        );
    }

    #[test]
    fn opencode_v3_owned_projection_omits_version_and_retains_owner_pin() {
        let descriptor = descriptor_for_schema("3");
        let options = owned_options(OWNER_NONCE);
        let config = project(&descriptor, &options).unwrap();
        let state_root = fixture_root()
            .join("owned-state")
            .join("launches")
            .join(OWNER_NONCE);

        assert!(!config.values.contains_key("OPENCODE_EXPECTED_VERSION"));
        assert_eq!(
            literal(&config, "OPENCODE_OWNER_ORIGIN"),
            "fresh_owned_service"
        );
        assert_eq!(literal(&config, "OPENCODE_OWNER_NONCE"), OWNER_NONCE);
        assert_eq!(
            literal(&config, "OPENCODE_SERVICE_ID"),
            options["service_id"].as_str().unwrap()
        );
        assert_eq!(
            literal(&config, "OPENCODE_CONNECTION_FILE"),
            options["connection_file"].as_str().unwrap()
        );
        assert_eq!(
            literal(&config, "OPENCODE_OWNER_STATE_ROOT"),
            path_text(&state_root)
        );
    }

    #[test]
    fn opencode_v3_mapper_rejects_wrong_schema_digest_and_artifact() {
        let options = external_options();

        let mut wrong_digest: Value = serde_json::from_str(include_str!(
            "../../swarm-adapter-opencode/registration/descriptor.template.json"
        ))
        .unwrap();
        wrong_digest["config_schema"]["sha256"] =
            json!("0000000000000000000000000000000000000000000000000000000000000000");
        let wrong_digest: ModuleDescriptor = serde_json::from_value(wrong_digest).unwrap();
        assert_eq!(
            project(&wrong_digest, &options).unwrap_err().code,
            "MODULE_CONFIG_SCHEMA_UNSUPPORTED"
        );

        let mut wrong_artifact: Value = serde_json::from_str(include_str!(
            "../../swarm-adapter-opencode/registration/descriptor.template.json"
        ))
        .unwrap();
        wrong_artifact["artifact"]["artifact_id"] = json!("eliot-opencode-v2.rust-http.2");
        let wrong_artifact: ModuleDescriptor = serde_json::from_value(wrong_artifact).unwrap();
        assert_eq!(
            project(&wrong_artifact, &options).unwrap_err().code,
            "MODULE_CONFIG_SCHEMA_UNSUPPORTED"
        );
    }

    #[test]
    fn opencode_v1_and_v2_projections_preserve_expected_version() {
        for version in ["1", "2"] {
            let descriptor = descriptor_for_schema(version);
            let config = project(&descriptor, &external_options()).unwrap();
            assert_eq!(
                literal(&config, "OPENCODE_EXPECTED_VERSION"),
                "2.0.7",
                "schema version {version}"
            );
        }

        let descriptor = descriptor_for_schema("2");
        let config = project(&descriptor, &owned_options(OWNER_NONCE)).unwrap();
        assert_eq!(literal(&config, "OPENCODE_EXPECTED_VERSION"), "2.0.7");
        assert_eq!(literal(&config, "OPENCODE_OWNER_NONCE"), OWNER_NONCE);
    }

    #[test]
    fn opencode_owned_projection_rejects_route_nonce_mismatch() {
        let descriptor = descriptor_for_schema("3");
        let mut options = owned_options(OWNER_NONCE);
        options[OWNER_NONCE_KEY] = json!(OTHER_OWNER_NONCE);

        assert_eq!(
            project(&descriptor, &options).unwrap_err().code,
            "MODULE_CONFIG_SCOPE_MISMATCH"
        );
    }
}

#[cfg(test)]
mod hot_bootstrap_byte_boundary_tests {
    use super::{StandaloneRouteConfigMapper, StandaloneSupervisorConfig, SupervisorBootstrap};
    use crate::Sha256Digest;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::{Path, PathBuf};
    use swarm_client::IpcConfig;
    use swarm_contracts::Credential;

    const HOT_ACTOR_ID: &str = "11111111-1111-4111-8111-111111111111";

    fn absolute_root(name: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(format!("C:\\tmp\\eliot-hot-bootstrap-test\\{name}"))
        } else {
            PathBuf::from(format!("/tmp/eliot-hot-bootstrap-test/{name}"))
        }
    }

    fn test_config(descriptor_files: Vec<PathBuf>) -> StandaloneSupervisorConfig {
        StandaloneSupervisorConfig {
            schema_version: 1,
            root: absolute_root("root"),
            ipc: IpcConfig {
                max_frame_bytes: 1_048_576,
                max_connections: 256,
                max_inflight_per_connection: 8,
                write_timeout_seconds: 15,
            },
            supervisor_credential: Credential {
                client_id: "eliot-module-supervisor-v1".to_owned(),
                token: "test-token".to_owned(),
            },
            install_root: absolute_root("install"),
            descriptor_files,
            state_root: absolute_root("state"),
            resolver_root: absolute_root("resolver"),
            owner_helper: absolute_root("owner-helper"),
            owner_helper_sha256: Sha256Digest::new("a".repeat(64)).unwrap(),
            protected_files: BTreeMap::new(),
            launch_configs: BTreeMap::new(),
            route_config_mapper: StandaloneRouteConfigMapper::DescriptorSchema,
        }
    }

    /// Build an absolute ASCII descriptor path with an exact UTF-8 byte length.
    /// Byte length (not char count) is what `to_frame` serializes.
    fn long_descriptor_path(index: usize, total_bytes: usize) -> PathBuf {
        let dir = if cfg!(windows) {
            "C:\\install"
        } else {
            "/install"
        };
        let sep = std::path::MAIN_SEPARATOR.to_string();
        let stem = format!("hot-{index:04}-");
        let ext = ".json";
        let base_len = dir.len() + sep.len() + stem.len() + ext.len();
        assert!(total_bytes > base_len, "total_bytes too small");
        let fill = "a".repeat(total_bytes - base_len);
        let path = PathBuf::from(format!("{dir}{sep}{stem}{fill}{ext}"));
        assert_eq!(
            path.as_os_str().as_encoded_bytes().len(),
            total_bytes,
            "long path must measure bytes, not chars"
        );
        assert!(path.is_absolute());
        path
    }

    #[test]
    fn lost_completion_reply_restores_installed_paths_before_next_capacity_check() {
        use super::retain_installed_descriptor_paths;
        use crate::control::{
            InstalledDescriptorRequestPage, InstalledDescriptorRequestRecord,
            InstalledDescriptorRequestState,
        };

        let config = test_config(vec![absolute_root("short.json")]);
        let mut committed = BTreeSet::new();
        let mut next = None;
        for index in 0..256 {
            let candidate = long_descriptor_path(index, 1024);
            if config
                .ensure_hot_descriptor_restartable(HOT_ACTOR_ID, &committed, &candidate)
                .is_err()
            {
                next = Some(candidate);
                break;
            }
            committed.insert(candidate);
        }
        let next = next.expect("a later candidate exceeds the fixed restart frame");
        let lost = committed.iter().next().unwrap().clone();
        let mut local = committed.clone();
        local.remove(&lost);
        config
            .ensure_hot_descriptor_restartable(HOT_ACTOR_ID, &local, &next)
            .expect("the incomplete local ledger would wrongly admit the candidate");

        // The Store committed the first install, but its completion reply was lost.
        // Its next complete page includes that install and the later queued request.
        let page = InstalledDescriptorRequestPage {
            requests: vec![
                InstalledDescriptorRequestRecord {
                    operation_id: "committed-with-lost-reply".to_owned(),
                    descriptor_file: lost,
                    state: InstalledDescriptorRequestState::Installed,
                },
                InstalledDescriptorRequestRecord {
                    operation_id: "later-queued-install".to_owned(),
                    descriptor_file: next.clone(),
                    state: InstalledDescriptorRequestState::Queued,
                },
            ],
        };
        retain_installed_descriptor_paths(&mut local, &page);
        assert_eq!(local, committed);
        assert!(!local.contains(&next));
        let error = config
            .ensure_hot_descriptor_restartable(HOT_ACTOR_ID, &local, &next)
            .unwrap_err();
        assert_eq!(error.code, "MODULE_BOOTSTRAP_LIMIT");
        retain_installed_descriptor_paths(&mut local, &page);
        assert_eq!(local, committed, "durable readback is idempotent");
    }

    #[test]
    fn hot_update_byte_boundary_rejects_before_catalog_publish() {
        let short = absolute_root("short.json");
        let config = test_config(vec![short]);
        let mut prior = BTreeSet::new();
        let mut admitted = 0_usize;
        let mut rejected: Option<PathBuf> = None;
        let mut rejected_code = String::new();
        for index in 0..256 {
            let candidate = long_descriptor_path(index, 1024);
            match config.ensure_hot_descriptor_restartable(HOT_ACTOR_ID, &prior, &candidate) {
                Ok(()) => {
                    prior.insert(candidate);
                    admitted += 1;
                }
                Err(error) => {
                    rejected = Some(candidate);
                    rejected_code = error.code.clone();
                    break;
                }
            }
        }
        let rejected = rejected.expect("65 x 1024-byte paths must exceed the 65_536-byte frame");
        assert_eq!(rejected_code, "MODULE_BOOTSTRAP_LIMIT");
        // The 512-entry count bound alone would still allow this update.
        assert!(
            admitted < 65,
            "byte bound must trigger before 65 x 1024-byte paths"
        );
        assert!(
            prior.len() + 1 < 512,
            "count bound alone would allow the rejected update"
        );
        // Rejection leaves the admitted set unchanged: the caller returns
        // before Store registration and catalog publication, so live workers stay.
        assert!(!prior.contains(&rejected));
        assert_eq!(prior.len(), admitted);
        // Exact serialized accounting: admitted fits, admitted plus rejected does not.
        let mut fits = config.clone();
        fits.append_retained_descriptor_files(prior.iter().cloned())
            .unwrap();
        SupervisorBootstrap {
            config: fits,
            actor_instance_id: Some(HOT_ACTOR_ID.to_owned()),
        }
        .to_frame()
        .expect("admitted set must still fit the restart frame");
        let mut overflows = config.clone();
        overflows
            .append_retained_descriptor_files(prior.iter().cloned())
            .unwrap();
        overflows
            .append_retained_descriptor_files([rejected.clone()])
            .unwrap();
        assert!(overflows.descriptor_files.len() < 512);
        let frame_error = SupervisorBootstrap {
            config: overflows,
            actor_instance_id: Some(HOT_ACTOR_ID.to_owned()),
        }
        .to_frame()
        .unwrap_err();
        assert!(frame_error.message.contains("exceeds its size bound"));
        // A duplicate of an already-admitted path adds no bytes and stays admissible.
        let duplicate = prior.iter().next().unwrap().clone();
        config
            .ensure_hot_descriptor_restartable(HOT_ACTOR_ID, &prior, &duplicate)
            .expect("duplicate retained path must not grow the restart frame");
        let _ = Path::new("placeholder");
    }
}
