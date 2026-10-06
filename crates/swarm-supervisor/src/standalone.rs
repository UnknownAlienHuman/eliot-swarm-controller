//! Process boundary for the independent module supervisor.
//!
//! This module owns the supervisor-side bootstrap, resolver publication, and
//! demand reconciliation.  It deliberately depends only on contracts, the
//! authenticated client, and the existing process/owner implementation.  A
//! Store handle, database connection, controller object, or host IPC listener
//! never crosses into this crate.

use crate::{
    AdmissionState, BindingLaunchConfig, BindingMapPublication, CapabilityId, DemandCause,
    DemandLease, DescriptorCatalog, KernelFault, LaunchValue, ModuleBindingCredential,
    ModuleDemandCursor, ModuleDemandRecord, ModuleDescriptor, ModuleOwnerExecutable,
    ModuleSupervisorObservation, OperationReadback, OperationSnapshot, ProtectedResolverContext,
    ResolverMapDirectory, ServiceScope, Sha256Digest, SupervisorControlClient, SupervisorRegistry,
    SupervisorRegistryConfig, load_installed_descriptor, module_contract_claim,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs,
    io::{BufRead, Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
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
const MAX_DESCRIPTOR_FILES: usize = 256;
const MAX_PROTECTED_FILES: usize = 128;
const MAX_LAUNCH_CONFIGS: usize = 256;
const MAX_STATUS_QUEUE: usize = 1_024;
const MODULE_SUPERVISOR_CLIENT_ID: &str = "eliot-module-supervisor-v1";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_ID: &str = "opencode-v2-native-options";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256: &str =
    "d597be6bae80dc82535b658b5daaf3037a09976e5704d799a6715a673a62f662";

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
}

impl SupervisorBootstrap {
    pub fn validate(&self) -> Result<()> {
        self.config.validate()
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
        bootstrap.validate()?;
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
        bootstrap.validate()?;
        Ok(bootstrap)
    }

    pub fn to_frame(&self) -> Result<Vec<u8>> {
        self.validate()?;
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
}

impl LaunchConfigProvider for StaticLaunchConfigProvider {
    fn for_binding(
        &self,
        descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig> {
        let module_id = descriptor.module_id.as_str();
        let Some(entry) = self.entries.get(module_id) else {
            return map_route_config(self.mapper, descriptor, route_native_options);
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
            Some(schema)
                if schema.schema_id == OPENCODE_NATIVE_OPTIONS_SCHEMA_ID
                    && schema.version == "1"
                    && schema.sha256.as_ref().is_some_and(|digest| {
                        digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256
                    }) =>
            {
                open_code_route_config(descriptor, route_native_options)
            }
            Some(_) => Err(Error::new(
                "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                "the retained descriptor names a launch config schema with no supervisor mapper",
            )),
        },
        StandaloneRouteConfigMapper::OpenCodeSevenField => {
            open_code_route_config(descriptor, route_native_options)
        }
    }
}

fn open_code_route_config(
    descriptor: &ModuleDescriptor,
    options: &Value,
) -> Result<BindingLaunchConfig> {
    if !descriptor.config_schema.as_ref().is_some_and(|schema| {
        schema.schema_id == OPENCODE_NATIVE_OPTIONS_SCHEMA_ID
            && schema.version == "1"
            && schema
                .sha256
                .as_ref()
                .is_some_and(|digest| digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256)
    }) {
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
    let allowed = [
        "service_id",
        "connection_file",
        "expected_version",
        "directory",
        "model",
    ];
    if object.keys().any(|key| !allowed.contains(&key.as_str())) || object.len() != allowed.len() {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode route options must contain only the seven declared adapter values",
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
    let service_id = required_config_string(&object["service_id"], "service_id", 128)?;
    let connection_file =
        required_config_string(&object["connection_file"], "connection_file", 4096)?;
    let expected_version =
        required_config_string(&object["expected_version"], "expected_version", 256)?;
    let directory = required_config_string(&object["directory"], "directory", 4096)?;
    if !Path::new(&connection_file).is_absolute() || !Path::new(&directory).is_absolute() {
        return Err(Error::new(
            "MODULE_CONFIG_INVALID",
            "OpenCode connection_file and directory values must be absolute paths",
        ));
    }
    let model_id = required_config_string(&model["id"], "model.id", 256)?;
    let provider_id = required_config_string(&model["providerID"], "model.providerID", 256)?;
    let variant = required_config_string(&model["variant"], "model.variant", 256)?;
    let config = BindingLaunchConfig {
        values: BTreeMap::from([
            (
                "OPENCODE_SERVICE_ID".to_owned(),
                LaunchValue::Literal(service_id),
            ),
            (
                "OPENCODE_CONNECTION_FILE".to_owned(),
                LaunchValue::Literal(connection_file),
            ),
            (
                "OPENCODE_EXPECTED_VERSION".to_owned(),
                LaunchValue::Literal(expected_version),
            ),
            (
                "OPENCODE_DIRECTORY".to_owned(),
                LaunchValue::Literal(directory),
            ),
            (
                "OPENCODE_MODEL_ID".to_owned(),
                LaunchValue::Literal(model_id),
            ),
            (
                "OPENCODE_PROVIDER_ID".to_owned(),
                LaunchValue::Literal(provider_id),
            ),
            ("OPENCODE_VARIANT".to_owned(), LaunchValue::Literal(variant)),
        ]),
    };
    config.validate().map_err(module_error)?;
    Ok(config)
}

fn required_config_string(value: &Value, field: &str, max: usize) -> Result<String> {
    value
        .as_str()
        .filter(|text| !text.is_empty() && text.len() <= max && !text.chars().any(char::is_control))
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            Error::new(
                "MODULE_CONFIG_INVALID",
                format!("OpenCode {field} is missing or invalid"),
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
}

impl StandaloneSupervisor {
    pub async fn start(config: StandaloneSupervisorConfig) -> Result<Self> {
        let provider = Arc::new(StaticLaunchConfigProvider {
            entries: config.launch_configs.clone(),
            mapper: config.route_config_mapper,
        });
        Self::start_with_provider(config, provider).await
    }

    pub async fn start_from_bootstrap(bootstrap: SupervisorBootstrap) -> Result<Self> {
        bootstrap.validate()?;
        Self::start(bootstrap.config).await
    }

    pub async fn start_with_provider(
        config: StandaloneSupervisorConfig,
        launch_config: Arc<dyn LaunchConfigProvider>,
    ) -> Result<Self> {
        config.validate()?;
        create_private_directory(&config.state_root)?;
        create_private_directory(&config.resolver_root)?;
        let mut descriptors = Vec::with_capacity(config.descriptor_files.len());
        for path in &config.descriptor_files {
            descriptors.push(
                load_installed_descriptor(path, &config.install_root).map_err(|error| {
                    Error::new(
                        "MODULE_DESCRIPTOR_UNAVAILABLE",
                        format!("descriptor load failed: {}", error.code),
                    )
                })?,
            );
        }
        let catalog = DescriptorCatalog::from_descriptors(descriptors)
            .map_err(|error| Error::new("MODULE_CATALOG_INVALID", error.to_string()))?;
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
            actor_instance_id: Uuid::new_v4().to_string(),
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
        let mut pending = VecDeque::<ModuleSupervisorObservation>::new();
        let mut sequence = 0_u64;
        let mut last_health = None::<HealthSnapshot>;
        let mut consecutive_failures = 0_u32;
        loop {
            if *stopping.borrow() {
                return Ok(());
            }
            let mut cycle_error = None::<String>;
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
                        match self.reconcile_demands(&mut held).await {
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
            if let Err(error) = self
                .collect_status_events(&mut last_status, &mut pending, &mut sequence)
                .await
            {
                self.registry
                    .close_durable_admission(KernelFault::StoreUnavailable);
                cycle_error.get_or_insert_with(|| error.code.clone());
                eprintln!("module supervisor observation: {}", error.code);
            }
            if let Some(ref error_code) = cycle_error {
                consecutive_failures = consecutive_failures.saturating_add(1).min(32);
                self.publish_health(
                    &mut last_health,
                    "retry_wait",
                    consecutive_failures,
                    Some(&error_code),
                    Some(RECONCILE_INTERVAL),
                )
                .await;
            } else if recovered {
                consecutive_failures = 0;
                self.publish_health(&mut last_health, "running", 0, None, None)
                    .await;
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
    ) {
        let error_code = error_code.map(safe_health_code);
        let retry_in_ms = retry.map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
        let next = HealthSnapshot {
            state: state.to_owned(),
            consecutive_failures,
            error_code,
            retry_in_ms,
        };
        if last.as_ref() == Some(&next) {
            return;
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
            Err(error) => eprintln!(
                "module supervisor health readback unavailable: {}",
                error.code
            ),
        }
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

    async fn reconcile_demands(&self, held: &mut HashMap<DemandKey, DemandLease>) -> Result<bool> {
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
                    } else if let Some(boot_id) = readback.module_hello_boot_id.as_deref() {
                        if let Err(error) = self
                            .confirm_module_hello(&key.module_id, &scope, boot_id)
                            .await
                        {
                            eprintln!("module hello readback: {}", error.code);
                        }
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
                    if let Some(boot_id) = stored.module_hello_boot_id.as_deref() {
                        if let Err(error) = self
                            .confirm_module_hello(&demand.module_id, &scope, boot_id)
                            .await
                        {
                            eprintln!("module new-scope hello readback: {}", error.code);
                        }
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
            if let Some(boot_id) = readback.module_hello_boot_id.as_deref() {
                if let Err(error) = self
                    .confirm_module_hello(&key.module_id, &scope, boot_id)
                    .await
                {
                    eprintln!("module stale-scope hello readback: {}", error.code);
                }
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
        self.registry.statuses().await.into_iter().any(|status| {
            status.owner.is_some()
                || status.worker.is_some()
                || status.readback_required
                || status.unknown_operation_count != 0
                || !status.unknown_operation_ids.is_empty()
                || matches!(
                    status.effect_certainty,
                    crate::ModuleEffectCertainty::Unknown
                )
        })
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
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        sequence: &mut u64,
    ) -> Result<()> {
        for status in self.registry.statuses().await {
            let key = (status.module_id.clone(), status.scope.clone());
            let fingerprint = serde_json::to_string(&status)?;
            if last_status.get(&key) == Some(&fingerprint) {
                continue;
            }
            let next = sequence.checked_add(1).ok_or_else(|| {
                Error::new(
                    "MODULE_OBSERVATION_SEQUENCE_EXHAUSTED",
                    "status sequence exhausted",
                )
            })?;
            if pending.len() >= MAX_STATUS_QUEUE {
                eprintln!("module observation queue is full; retaining latest status");
                continue;
            }
            let event =
                ModuleSupervisorObservation::from_status(&status, &self.actor_instance_id, next)?;
            *sequence = next;
            pending.push_back(event);
            last_status.insert(key, fingerprint);
        }
        while let Some(event) = pending.front() {
            match self.control.record_observation(event).await {
                Ok(()) => {
                    pending.pop_front();
                }
                Err(error) if is_control_failure(&error) => return Err(error),
                Err(error) => {
                    eprintln!("module observation retained: {}", error.code);
                    break;
                }
            }
        }
        Ok(())
    }
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
    swarm_process::private_permissions(path, true).map_err(Into::into)
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
