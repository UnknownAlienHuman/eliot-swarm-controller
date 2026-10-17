use crate::{
    ActivationPolicy, ArtifactSelector, BindingLaunchConfig, CapabilityId, DescriptorCatalog,
    LaunchValue, LifecycleOwnership, ModuleDescriptor, ModuleEffectCertainty, ModuleFailureStage,
    ModuleOwnerExecutable, ProtectedResolver, ProtectedResolverContext, ProtocolRange,
    ProtocolVersion, ServiceScope, SupervisorControlClient,
    descriptor::{
        extend_descriptor_catalog, validate_environment_name, validate_identifier,
        validate_launch_value,
    },
    module_link::module_contract_claim,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    future::Future,
    hash::{Hash, Hasher},
    io::{Read, Write},
    path::{Path, PathBuf},
    pin::Pin,
    process::ExitStatus,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use swarm_client::{HostConnectionConfig, IpcConfig};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};
use swarm_process::process_group::{ProcessFamilyObservation, observe_process_family};
use swarm_process::{
    module_child_belongs_to_owner, process_birth_identity, process_image_identity,
    read_private_durable, replace_private_durable,
};
use tokio::{
    process::{Child, Command},
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use uuid::Uuid;

const OWNER_RECORD_LIMIT: u64 = 65_536;
const LAUNCH_RECORD_LIMIT: u64 = 65_536;
const MODULE_PLAN_LIMIT: u64 = 65_536;
const RESTART_HISTORY_LIMIT: u64 = 16_384;
const OWNER_DEPARTURE_FILE: &str = "owner-departure.json";
const OWNER_DEPARTURE_OBSERVER_RETAINED: &str = "retained_owner_family_query";
const OWNER_DEPARTURE_OBSERVER_FRESH: &str = "fresh_open";
const MODULE_CONFIG_ENV_PREFIX: &str = "ELIOT_SWARM_CONFIG_";
const OWNER_DRAIN_POLL: Duration = Duration::from_millis(500);
const HELPER_START_POLL: Duration = Duration::from_millis(100);

#[cfg(test)]
type ModuleMembershipProbe = fn(&Value, &Value) -> Result<bool>;
const HELPER_RECEIPT_TIMEOUT: Duration = Duration::from_secs(45);
const OWNER_FINAL_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(20);
const PRIOR_OWNER_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);
const OWNER_RECONCILE_RETRY: Duration = Duration::from_secs(1);
const MAX_UNKNOWN_OPERATIONS: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DemandCause {
    /// A durable Operation already admitted by the host/kernel.
    Operation { operation_id: String },
    /// A configured due action already selected by the host/kernel.
    DueAction {
        action_id: String,
        occurrence_id: String,
    },
    /// An explicit live subscription retained by the host/kernel.
    LiveSubscription { subscription_id: String },
}

impl DemandCause {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Operation { operation_id } => validate_identifier(operation_id, "operation_id"),
            Self::DueAction {
                action_id,
                occurrence_id,
            } => {
                validate_identifier(action_id, "action_id")?;
                validate_identifier(occurrence_id, "occurrence_id")
            }
            Self::LiveSubscription { subscription_id } => {
                validate_identifier(subscription_id, "subscription_id")
            }
        }
    }

    fn allowed_by(&self, policy: ActivationPolicy) -> bool {
        match policy {
            ActivationPolicy::OnDemand => matches!(self, Self::Operation { .. }),
            ActivationPolicy::Continuous => true,
        }
    }

    fn key(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KernelFault {
    StoreUnavailable,
    DurableJournalUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionState {
    Open,
    Closed { fault: KernelFault },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state", deny_unknown_fields)]
pub enum LifecycleState {
    WaitingForDemand,
    WaitingForKernel,
    Starting {
        boot_id: String,
    },
    ProcessRunning {
        boot_id: String,
    },
    RestartBackoff {
        delay_ms: u64,
    },
    ProcessExited {
        exit_code: Option<i32>,
        /// True only after the exact owner family is proved empty; direct
        /// helper exit alone never sets this.
        #[serde(default)]
        exit_proven: bool,
    },
    OwnerGroupRetained {
        owner_pid: u32,
        reason: OwnerRetainedReason,
    },
    OwnerIdentityUnknown,
    ProcessIdentityUnknown {
        pid: Option<u32>,
    },
    Completed {
        exit_code: Option<i32>,
    },
    Isolated {
        reason: String,
    },
}

/// Diagnostic causes only; none proves departure or grants dispatch authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerRetainedReason {
    WorkerExited,
    WorkerIdentityUnknown,
    HelloUnconfirmed,
    OwnerFamilyNotEmpty,
    OwnerFamilyObservationUnknown,
}

impl OwnerRetainedReason {
    pub(crate) fn code(self) -> &'static str {
        match self {
            Self::WorkerExited => "MODULE_WORKER_EXITED",
            Self::WorkerIdentityUnknown => "MODULE_WORKER_IDENTITY_UNKNOWN",
            Self::HelloUnconfirmed => "MODULE_HELLO_UNCONFIRMED",
            Self::OwnerFamilyNotEmpty => "MODULE_OWNER_FAMILY_NOT_EMPTY",
            Self::OwnerFamilyObservationUnknown => "MODULE_OWNER_FAMILY_OBSERVATION_UNKNOWN",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: u32,
    /// Platform birth tuple from swarm_process::process_birth_identity.
    pub birth: Value,
    /// Kernel-reported executable path and image digest from swarm_process.
    pub image: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureSummary {
    pub code: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleHelloFailure {
    pub boot_id: String,
    pub error_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartStatus {
    pub starts_in_window: usize,
    pub last_delay_ms: Option<u64>,
    pub exhausted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestartHistory {
    version: u64,
    module_id: String,
    artifact_id: String,
    artifact_version: String,
    descriptor_fingerprint: String,
    scope: ServiceScope,
    starts_unix_ms: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorStatus {
    pub module_id: String,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub scope: ServiceScope,
    pub admission: AdmissionState,
    pub lifecycle: LifecycleState,
    /// Exact dedicated helper process which owns this one module scope.
    pub owner: Option<ProcessIdentity>,
    /// Exact adapter child identity. The external native service is never
    /// started or terminated by this supervisor.
    pub worker: Option<ProcessIdentity>,
    pub worker_boot_id: Option<String>,
    /// Exact Store hello boot and closed reason when confirmation failed.
    /// This diagnostic context must not overwrite the observed worker boot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hello_failure: Option<ModuleHelloFailure>,
    /// Closed adapter error_code received only from this exact worker scope.
    /// Diagnostic data does not replace the generic lifecycle failure code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_diagnostic_code: Option<String>,
    /// Native-effect certainty for the latest helper attempt. This is never
    /// inferred from IPC disconnect or process exit alone.
    pub effect_certainty: ModuleEffectCertainty,
    pub failure_stage: Option<ModuleFailureStage>,
    /// Reconnect/readback reconciliation is due after a worker exit. An
    /// accepted exact-boot module.hello may clear this reconnect gate, while
    /// unresolved Operation IDs remain separately visible below.
    pub readback_required: bool,
    /// Pending/uncertain Operation IDs, never their inputs or result bodies.
    pub unknown_operation_ids: Vec<String>,
    /// Unresolved count from the last complete scoped readback. A bounded
    /// adapter restart may proceed after exact process-family departure, but
    /// descriptor replacement remains blocked while unresolved IDs are held.
    pub unknown_operation_count: usize,
    pub unknown_operation_ids_truncated: bool,
    pub restart: RestartStatus,
    pub last_failure: Option<FailureSummary>,
}

impl SupervisorStatus {
    fn has_proven_terminal(&self) -> bool {
        matches!(
            &self.lifecycle,
            LifecycleState::Completed { .. }
                | LifecycleState::ProcessExited {
                    exit_proven: true,
                    ..
                }
        )
    }

    fn set_prelaunch_lifecycle(&mut self, lifecycle: LifecycleState) {
        // Demand, admission and restart accounting describe whether another
        // launch may begin. They do not retract the exact prior departure.
        // allocate_worker_boot replaces that attempt only after launch fences.
        if !self.has_proven_terminal() {
            self.lifecycle = lifecycle;
        }
    }

    fn matches_current_hello(&self, boot_id: &str) -> bool {
        self.worker_boot_id.as_deref() == Some(boot_id)
            && matches!(
                &self.lifecycle,
                LifecycleState::Starting { boot_id: current }
                    | LifecycleState::ProcessRunning { boot_id: current }
                    if current == boot_id
            )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationSnapshot {
    pub operation_id: String,
    pub binding_id: String,
    pub generation: u64,
    pub state: String,
}

impl OperationSnapshot {
    /// Parse only fields returned by the existing operation.get response.
    pub fn from_operation_get(value: &Value) -> Result<Self> {
        let operation_id = value
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("operation.get omitted operation_id"))?;
        let binding_id = value
            .get("binding_id")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("operation.get omitted binding_id"))?;
        let generation = value
            .get("binding_generation")
            .and_then(Value::as_u64)
            .ok_or_else(|| Error::invalid("operation.get omitted binding_generation"))?;
        let state = value
            .get("state")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::invalid("operation.get omitted state"))?;
        let item = Self {
            operation_id: operation_id.to_owned(),
            binding_id: binding_id.to_owned(),
            generation,
            state: state.to_owned(),
        };
        validate_identifier(&item.operation_id, "operation_id")?;
        validate_identifier(&item.binding_id, "binding_id")?;
        if generation == 0 {
            return Err(Error::invalid("operation.get returned invalid generation"));
        }
        validate_operation_state(&item.state)?;
        Ok(item)
    }
}

/// Root-created proof that existing Manager/Operator operation.list/get paging
/// completed for exactly one service scope. This is data, not a new RPC or role.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationReadback {
    pub scope: ServiceScope,
    pub complete: bool,
    pub operations: Vec<OperationSnapshot>,
}

impl OperationReadback {
    pub fn validate_for(&self, expected: &ServiceScope) -> Result<()> {
        if !self.complete || &self.scope != expected {
            return Err(Error::new(
                "MODULE_READBACK_INCOMPLETE",
                "trusted operation.list/get readback is incomplete or belongs to another scope",
            ));
        }
        let mut operation_ids = BTreeSet::new();
        for operation in &self.operations {
            validate_identifier(&operation.operation_id, "operation_id")?;
            validate_identifier(&operation.binding_id, "binding_id")?;
            validate_operation_state(&operation.state)?;
            if operation.generation == 0 {
                return Err(Error::invalid("operation readback has invalid generation"));
            }
            if !operation_ids.insert(operation.operation_id.as_str()) {
                return Err(Error::new(
                    "MODULE_READBACK_DUPLICATE_OPERATION",
                    "trusted Operation readback contains a duplicate operation ID",
                ));
            }
            if operation.binding_id != expected.binding_id
                || operation.generation != expected.generation
            {
                return Err(Error::new(
                    "MODULE_READBACK_SCOPE_MISMATCH",
                    "operation.get returned an Operation from another binding generation",
                ));
            }
        }
        Ok(())
    }
}

/// All fixed construction inputs for one shared module registry. Grouping
/// them makes the host lifecycle boundary explicit and keeps the public
/// constructor stable as optional runtime dependencies evolve.
pub struct SupervisorRegistryConfig {
    pub catalog: DescriptorCatalog,
    pub state_root: PathBuf,
    pub ipc_root: PathBuf,
    pub supervisor_credential: Credential,
    pub ipc: IpcConfig,
    pub owner_executable: ModuleOwnerExecutable,
    pub resolver: Arc<dyn ProtectedResolver>,
    pub admission: watch::Sender<AdmissionState>,
}

/// One already-authorized per-binding demand. The request contains no Manager
/// credential or authority beyond the supplied retained scope and cause.
pub struct ModuleDemandRequest {
    pub selector: ArtifactSelector,
    pub host_protocol: ProtocolRange,
    pub required_capabilities: BTreeSet<CapabilityId>,
    pub scope: ServiceScope,
    pub cause: DemandCause,
    pub launch_config: BindingLaunchConfig,
    pub module_client_id: String,
    pub readback: Option<OperationReadback>,
}

#[derive(Clone)]
struct RegistryKey {
    module_id: String,
    scope: ServiceScope,
}

impl PartialEq for RegistryKey {
    fn eq(&self, other: &Self) -> bool {
        self.module_id == other.module_id && self.scope == other.scope
    }
}

impl Eq for RegistryKey {}

impl Hash for RegistryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.module_id.hash(state);
        self.scope.hash(state);
    }
}

/// One registry per loaded descriptor selection. Replacing a valid descriptor
/// constructs a new fingerprinted slot; existing slots keep their old artifact.
pub struct SupervisorRegistry {
    catalog: RwLock<Arc<DescriptorCatalog>>,
    catalog_update: AsyncMutex<()>,
    state_root: PathBuf,
    ipc_root: PathBuf,
    supervisor_credential: Credential,
    ipc: IpcConfig,
    owner_executable: ModuleOwnerExecutable,
    resolver: Arc<dyn ProtectedResolver>,
    admission: watch::Sender<AdmissionState>,
    services: AsyncMutex<HashMap<RegistryKey, Arc<Service>>>,
}

impl SupervisorRegistry {
    pub fn new(
        catalog: DescriptorCatalog,
        state_root: PathBuf,
        ipc_root: PathBuf,
        supervisor_credential: Credential,
        ipc: IpcConfig,
        owner_executable: ModuleOwnerExecutable,
        resolver: Arc<dyn ProtectedResolver>,
    ) -> Result<Self> {
        let (admission, _) = watch::channel(AdmissionState::Open);
        Self::new_with_admission(SupervisorRegistryConfig {
            catalog,
            state_root,
            ipc_root,
            supervisor_credential,
            ipc,
            owner_executable,
            resolver,
            admission,
        })
    }

    /// Build a registry over the host actor's retained admission gate. The
    /// gate survives optional actor recreation so a local panic or IPC retry
    /// cannot reopen module starts after a Store or journal failure.
    pub fn new_with_admission(config: SupervisorRegistryConfig) -> Result<Self> {
        let SupervisorRegistryConfig {
            catalog,
            state_root,
            ipc_root,
            supervisor_credential,
            ipc,
            owner_executable,
            resolver,
            admission,
        } = config;
        if !state_root.is_absolute() {
            return Err(Error::invalid("module state root must be an absolute path"));
        }
        let state_root = fs::canonicalize(state_root)?;
        if !fs::metadata(&state_root)?.is_dir() {
            return Err(Error::invalid("module state root must be a directory"));
        }
        if !ipc_root.is_absolute() {
            return Err(Error::invalid(
                "module supervisor IPC root must be an absolute path",
            ));
        }
        let ipc_root = fs::canonicalize(ipc_root)?;
        if !fs::metadata(&ipc_root)?.is_dir() {
            return Err(Error::invalid(
                "module supervisor IPC root must be a directory",
            ));
        }
        owner_executable.validate()?;
        let owner_path = fs::canonicalize(&owner_executable.path).map_err(|error| {
            Error::new(
                "MODULE_OWNER_HELPER_UNAVAILABLE",
                format!("configured module-owner helper cannot be resolved: {error}"),
            )
        })?;
        if !fs::metadata(&owner_path)?.is_file() {
            return Err(Error::new(
                "MODULE_OWNER_HELPER_UNAVAILABLE",
                "configured module-owner helper is not a file",
            ));
        }
        if !hash_file_sha256(&owner_path)?.eq_ignore_ascii_case(owner_executable.sha256.as_str()) {
            return Err(Error::new(
                "MODULE_OWNER_HELPER_MISMATCH",
                "configured module-owner helper bytes do not match its SHA-256 pin",
            ));
        }
        let owner_executable = ModuleOwnerExecutable {
            path: owner_path,
            sha256: owner_executable.sha256,
        };
        Ok(Self {
            catalog: RwLock::new(Arc::new(catalog)),
            catalog_update: AsyncMutex::new(()),
            state_root,
            ipc_root,
            supervisor_credential,
            ipc,
            owner_executable,
            resolver,
            admission,
            services: AsyncMutex::new(HashMap::new()),
        })
    }

    /// The catalogue and this lookup are metadata-only. This method starts no worker.
    pub fn descriptors(&self) -> Vec<ModuleDescriptor> {
        read_lock(&self.catalog).descriptors().to_vec()
    }

    /// Select a newly validated descriptor set for future admissions. Existing
    /// workers keep their selected artifact; a scope cannot switch until the old
    /// worker and its native owner obligations are gone.
    pub fn replace_catalog(&self, catalog: DescriptorCatalog) {
        *write_lock(&self.catalog) = Arc::new(catalog);
    }

    /// Register one descriptor already present in the published catalog.
    pub async fn register_descriptor(&self, descriptor: &ModuleDescriptor) -> Result<Value> {
        let _update = self.catalog_update.lock().await;
        let catalog = read_lock(&self.catalog).clone();
        self.register_descriptor_against(descriptor, &catalog).await
    }

    /// Serialize a live install, stage the current catalog plus the immutable
    /// new artifact, durably register it, then expose it to future admissions.
    pub async fn register_installed_descriptor(
        &self,
        descriptor: &ModuleDescriptor,
    ) -> Result<Value> {
        let _update = self.catalog_update.lock().await;
        let current = read_lock(&self.catalog).clone();
        let candidate = extend_descriptor_catalog(&current, descriptor.clone())?;
        let response = self
            .register_descriptor_against(descriptor, &candidate)
            .await?;
        // The Store receipt is validated before the candidate becomes visible.
        self.replace_catalog(candidate);
        Ok(response)
    }

    async fn register_descriptor_against(
        &self,
        descriptor: &ModuleDescriptor,
        catalog: &DescriptorCatalog,
    ) -> Result<Value> {
        descriptor
            .validate()
            .map_err(|error| Error::new("MODULE_DESCRIPTOR_INVALID", error.to_string()))?;
        let expected_hash = descriptor
            .launch
            .executable_sha256
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "MODULE_ARTIFACT_DIGEST_REQUIRED",
                    "descriptor registration requires the installed executable SHA-256",
                )
            })?;
        let path = fs::canonicalize(&descriptor.launch.executable).map_err(|_| {
            Error::new(
                "MODULE_ARTIFACT_UNAVAILABLE",
                "installed executable is unavailable at its validated path",
            )
        })?;
        let actual_hash = hash_file_sha256(&path).map_err(|_| {
            Error::new(
                "MODULE_ARTIFACT_UNAVAILABLE",
                "installed executable could not be hashed",
            )
        })?;
        if !actual_hash.eq_ignore_ascii_case(expected_hash.as_str()) {
            return Err(Error::new(
                "MODULE_ARTIFACT_MISMATCH",
                "installed executable bytes do not match the descriptor SHA-256",
            ));
        }
        let selector = ArtifactSelector::new(
            descriptor.module_id.clone(),
            descriptor.artifact.artifact_id.clone(),
            descriptor.artifact.version.clone(),
            Some(expected_hash.clone()),
        );
        let selected = catalog
            .select_exact(&selector, descriptor.protocol, &descriptor.capabilities)
            .map_err(|error| Error::new("MODULE_SELECTION", error.to_string()))?;
        if selected != descriptor {
            return Err(Error::new(
                "MODULE_DESCRIPTOR_UNTRUSTED",
                "descriptor does not exactly match the staged catalog entry",
            ));
        }

        let control = SupervisorControlClient::new(
            self.ipc_root.clone(),
            self.supervisor_credential.clone(),
            self.ipc.clone(),
        )?;
        let response = control.register_descriptor(descriptor).await?;
        validate_registration_response(&response, descriptor)?;
        Ok(response)
    }

    pub async fn demand(&self, request: ModuleDemandRequest) -> Result<DemandLease> {
        let ModuleDemandRequest {
            selector,
            host_protocol,
            required_capabilities,
            scope,
            cause,
            launch_config,
            module_client_id,
            readback,
        } = request;
        scope.validate()?;
        cause.validate()?;
        launch_config.validate()?;
        validate_identifier(&module_client_id, "module_client_id")?;
        let catalog = read_lock(&self.catalog).clone();
        let descriptor = catalog
            .select_exact(&selector, host_protocol, &required_capabilities)
            .map_err(|error| Error::new("MODULE_SELECTION", error.to_string()))?
            .clone();
        let claim = module_contract_claim(&descriptor, host_protocol)?;
        let contract_json = serde_json::to_string(&claim)?;
        if !cause.allowed_by(descriptor.activation) {
            return Err(Error::new(
                "MODULE_DEMAND_NOT_ALLOWED",
                "descriptor does not allow this demand source",
            ));
        }
        if descriptor.lifecycle == LifecycleOwnership::OneShot {
            return Err(Error::new(
                "MODULE_ONE_SHOT_UNSUPPORTED",
                "one_shot requires an immutable per-Operation input snapshot and invocation identity",
            ));
        }
        if matches!(
            self.admission.borrow().clone(),
            AdmissionState::Closed { .. }
        ) {
            return Err(Error::new(
                "KERNEL_ADMISSION_CLOSED",
                "kernel/journal failure blocks new durable module admission",
            ));
        }

        let key = RegistryKey {
            module_id: descriptor.module_id.to_string(),
            scope: scope.clone(),
        };
        let wanted = descriptor_fingerprint(
            &descriptor,
            &launch_config,
            &module_client_id,
            claim.protocol,
        )?;

        // Descriptor replacement may wait for a per-scope lifecycle runner and
        // private owner receipts. Never hold the registry-wide map mutex over
        // that wait: a retained or faulted scope must not head-of-line block
        // unrelated module scopes. The shared per-scope gate keeps every
        // descriptor generation's replacement and demand linearizable while
        // different scopes proceed.
        loop {
            let existing = {
                let services = self.services.lock().await;
                services.get(&key).cloned()
            };
            let Some(existing) = existing else {
                let candidate = Arc::new(Service::new(ServiceInitialization {
                    descriptor: Arc::new(descriptor.clone()),
                    descriptor_fingerprint: wanted.clone(),
                    scope: scope.clone(),
                    state_dir: service_state_dir(&self.state_root, &descriptor, &scope)?,
                    state_root: self.state_root.clone(),
                    host_data_dir: self.ipc_root.clone(),
                    ipc: self.ipc.clone(),
                    launch_config: launch_config.clone(),
                    module_client_id: module_client_id.clone(),
                    protocol: claim.protocol,
                    module_contract_json: contract_json.clone(),
                    owner_executable: self.owner_executable.clone(),
                    resolver: self.resolver.clone(),
                    admission: self.admission.clone(),
                    demand_gate: Arc::new(AsyncMutex::new(())),
                }));
                let mut services = self.services.lock().await;
                services.entry(key.clone()).or_insert(candidate);
                continue;
            };

            let _gate = existing.demand_gate.lock().await;
            let current = {
                let services = self.services.lock().await;
                services.get(&key).cloned()
            };
            if current
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, &existing))
            {
                continue;
            }
            if existing.descriptor_fingerprint != wanted {
                if !existing.can_replace_descriptor().await {
                    return Err(Error::new(
                        "MODULE_UPDATE_DEFERRED",
                        "the previous module artifact or its native owner is still active",
                    ));
                }
                existing.clear_restart_history()?;
                let replacement = Arc::new(Service::new(ServiceInitialization {
                    descriptor: Arc::new(descriptor.clone()),
                    descriptor_fingerprint: wanted.clone(),
                    scope: scope.clone(),
                    state_dir: service_state_dir(&self.state_root, &descriptor, &scope)?,
                    state_root: self.state_root.clone(),
                    host_data_dir: self.ipc_root.clone(),
                    ipc: self.ipc.clone(),
                    launch_config: launch_config.clone(),
                    module_client_id: module_client_id.clone(),
                    protocol: claim.protocol,
                    module_contract_json: contract_json.clone(),
                    owner_executable: self.owner_executable.clone(),
                    resolver: self.resolver.clone(),
                    admission: self.admission.clone(),
                    demand_gate: existing.demand_gate.clone(),
                }));
                let mut services = self.services.lock().await;
                if services
                    .get(&key)
                    .is_none_or(|current| !Arc::ptr_eq(current, &existing))
                {
                    continue;
                }
                services.insert(key.clone(), replacement.clone());
                drop(services);
                return replacement.demand(cause.clone(), readback.clone()).await;
            }
            return existing.demand(cause.clone(), readback.clone()).await;
        }
    }

    /// Root calls this on a Store or durable-journal failure. Existing workers
    /// remain owned and running; no new worker start occurs until explicit recovery.
    pub fn close_durable_admission(&self, fault: KernelFault) {
        self.admission
            .send_replace(AdmissionState::Closed { fault });
        if let Ok(services) = self.services.try_lock() {
            for service in services.values() {
                service.update_status(|status| status.admission = AdmissionState::Closed { fault });
                service.signal();
            }
        }
    }

    /// Call only after the existing Store/journal recovery path has verified recovery.
    pub fn reopen_durable_admission_after_recovery(&self) {
        self.admission.send_replace(AdmissionState::Open);
        if let Ok(services) = self.services.try_lock() {
            for service in services.values() {
                service.update_status(|status| status.admission = AdmissionState::Open);
                service.signal();
            }
        }
    }

    /// Read the actor-level admission gate before host-side credential
    /// provisioning or other module setup writes occur.
    pub fn admission_state(&self) -> AdmissionState {
        self.admission.borrow().clone()
    }

    /// Feed complete Manager/Operator readback obtained through the existing
    /// operation.list/get RPCs. The module credential cannot call operation.get.
    pub async fn apply_operation_readback(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        readback: OperationReadback,
    ) -> Result<()> {
        let services = self.services.lock().await;
        let matching: Vec<_> = services
            .iter()
            .filter(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.clone())
            .collect();
        if matching.is_empty() {
            return Err(Error::new(
                "MODULE_NOT_ACTIVE",
                "module service scope is not active",
            ));
        }
        drop(services);
        for service in matching {
            service.apply_readback(readback.clone())?;
            let custody_pending = service.helper_child_custody.lock().await.is_some();
            let owner_pending = matches!(
                &service.status.borrow().lifecycle,
                LifecycleState::OwnerGroupRetained { .. }
                    | LifecycleState::OwnerIdentityUnknown
                    | LifecycleState::ProcessIdentityUnknown { .. }
            );
            if custody_pending || owner_pending {
                service.ensure_runner().await?;
            }
        }
        Ok(())
    }

    /// Called by the host only after Store accepted the existing
    /// module.hello for this exact verified worker boot. This clears reconnect
    /// gating but leaves every unresolved Operation identity/state untouched.
    pub async fn confirm_module_hello(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
    ) -> Result<()> {
        let services = self.services.lock().await;
        let service = services
            .iter()
            .find(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.clone())
            .ok_or_else(|| Error::new("MODULE_NOT_ACTIVE", "module service scope is not active"))?;
        service.confirm_module_hello(boot_id)
    }

    /// Reconcile a retained Store hello only against the current eligible boot.
    /// Historical hellos and scopes without a current service remain readback
    /// context; they do not replace a new attempt's failure or worker identity.
    pub(crate) async fn reconcile_retained_module_hello(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
    ) -> Result<()> {
        let services = self.services.lock().await;
        let Some(service) = services
            .iter()
            .find(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.clone())
        else {
            return Ok(());
        };
        service.reconcile_retained_module_hello(boot_id)
    }

    /// Retain a bounded reason for the current eligible hello only. A stale
    /// diagnostic cannot change a newer attempt or an owner/custody hold.
    pub async fn record_module_hello_failure(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
        error_code: &str,
    ) -> Result<()> {
        let services = self.services.lock().await;
        let service = services
            .iter()
            .find(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.clone())
            .ok_or_else(|| Error::new("MODULE_NOT_ACTIVE", "module service scope is not active"))?;
        service
            .record_module_hello_failure(boot_id, error_code)
            .map(|_| ())
    }

    /// Stop only the exact permanently rejected scope while preserving the
    /// first retained failure reason. Other module scopes continue normally.
    pub async fn quarantine_scope(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        error_code: &str,
    ) -> Result<()> {
        let services = self.services.lock().await;
        let service = services
            .iter()
            .find(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.clone())
            .ok_or_else(|| Error::new("MODULE_NOT_ACTIVE", "module service scope is not active"))?;
        service.quarantine(error_code)
    }

    pub async fn status(&self, module_id: &str, scope: &ServiceScope) -> Option<SupervisorStatus> {
        let services = self.services.lock().await;
        services
            .iter()
            .find(|(key, _)| key.module_id == module_id && &key.scope == scope)
            .map(|(_, service)| service.current_status())
    }

    pub async fn statuses(&self) -> Vec<SupervisorStatus> {
        let services = self.services.lock().await;
        let mut values: Vec<_> = services
            .values()
            .map(|service| service.current_status())
            .collect();
        values.sort_by(|left, right| {
            (
                &left.module_id,
                &left.scope.binding_id,
                left.scope.generation,
            )
                .cmp(&(
                    &right.module_id,
                    &right.scope.binding_id,
                    right.scope.generation,
                ))
        });
        values
    }

    /// Include an accepted Ready before its later sampled status. The retained
    /// snapshot is acknowledged only after the existing delivery queue owns it.
    pub(crate) async fn observation_statuses(&self) -> Vec<SupervisorStatus> {
        let services = self.services.lock().await;
        let mut values: Vec<_> = services
            .values()
            .flat_map(|service| service.observation_statuses())
            .collect();
        values.sort_by(|left, right| {
            (
                &left.module_id,
                &left.scope.binding_id,
                left.scope.generation,
            )
                .cmp(&(
                    &right.module_id,
                    &right.scope.binding_id,
                    right.scope.generation,
                ))
        });
        values
    }

    pub(crate) async fn acknowledge_ready_observation(&self, status: &SupervisorStatus) {
        let services = self.services.lock().await;
        if let Some(service) = services.values().find(|service| {
            service.descriptor.module_id.as_str() == status.module_id
                && service.scope == status.scope
        }) {
            service.acknowledge_ready_observation(status);
        }
    }
}

pub type ModuleSupervisor = SupervisorRegistry;

pub struct DemandLease {
    service: Arc<Service>,
    demand_key: String,
    released: bool,
}

impl DemandLease {
    pub fn status(&self) -> SupervisorStatus {
        self.service.current_status()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<SupervisorStatus> {
        self.service.status.subscribe()
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut demands = lock(&self.service.demands);
        if let Some(count) = demands.get_mut(&self.demand_key) {
            if *count <= 1 {
                demands.remove(&self.demand_key);
            } else {
                *count -= 1;
            }
        }
        drop(demands);
        self.service.signal();
    }
}

impl Drop for DemandLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

struct Service {
    descriptor: Arc<ModuleDescriptor>,
    descriptor_fingerprint: String,
    scope: ServiceScope,
    state_dir: PathBuf,
    state_root: PathBuf,
    host_data_dir: PathBuf,
    ipc: IpcConfig,
    launch_config: BindingLaunchConfig,
    module_client_id: String,
    protocol: ProtocolVersion,
    module_contract_json: String,
    owner_executable: ModuleOwnerExecutable,
    resolver: Arc<dyn ProtectedResolver>,
    admission: watch::Sender<AdmissionState>,
    status: watch::Sender<SupervisorStatus>,
    confirmed_ready: Mutex<Option<ConfirmedReady>>,
    demands: Mutex<HashMap<String, usize>>,
    /// Serializes replacement and demand for this exact module/scope across
    /// descriptor generations. It is an in-memory gate, not durable state.
    demand_gate: Arc<AsyncMutex<()>>,
    demand_epoch: watch::Sender<u64>,
    recovery_epoch: watch::Sender<u64>,
    readback_required: AtomicBool,
    scope_quarantined: AtomicBool,
    unknown_operation_ids: Mutex<BTreeSet<String>>,
    runner: AsyncMutex<Option<JoinHandle<()>>>,
    /// Exact helper Child retained when process, receipt, or family
    /// observation is uncertain. This slot is bounded to one per scope.
    helper_child_custody: AsyncMutex<Option<RetainedHelperChild>>,
    /// Query-only handle retained while the exact helper Child is unresolved.
    /// On Windows this keeps Job accounting available after the name or owner
    /// process can no longer be queried.
    #[cfg(windows)]
    owner_family_query: Mutex<Option<swarm_process::Group>>,
    #[cfg(test)]
    module_membership_probe: Mutex<Option<ModuleMembershipProbe>>,
    #[cfg(test)]
    prior_owner_timeout: Mutex<Option<Duration>>,
    #[cfg(test)]
    final_owner_timeout: Mutex<Option<Duration>>,
}

#[derive(Clone, Copy)]
enum LifecycleRunExit {
    Stopped,
    WaitingForDemand,
}

struct RetainedHelperChild {
    child: Child,
    pid: Option<u32>,
    identity: Option<ProcessIdentity>,
    boot_id: Option<String>,
    reason_code: String,
}

struct ConfirmedReady {
    status: SupervisorStatus,
    queued: bool,
}

struct ServiceInitialization {
    descriptor: Arc<ModuleDescriptor>,
    descriptor_fingerprint: String,
    scope: ServiceScope,
    state_dir: PathBuf,
    state_root: PathBuf,
    host_data_dir: PathBuf,
    ipc: IpcConfig,
    launch_config: BindingLaunchConfig,
    module_client_id: String,
    protocol: ProtocolVersion,
    module_contract_json: String,
    owner_executable: ModuleOwnerExecutable,
    resolver: Arc<dyn ProtectedResolver>,
    admission: watch::Sender<AdmissionState>,
    demand_gate: Arc<AsyncMutex<()>>,
}

struct OwnerHelperExit {
    exit: ExitStatus,
    worker: Option<ProcessIdentity>,
    worker_diagnostic_code: Option<String>,
    safe_to_replace: bool,
    family_departure_proven: bool,
    failure_code: Option<String>,
    failure_stage: Option<String>,
    effect_certainty: ModuleEffectCertainty,
    healthy_duration: Option<Duration>,
}

struct WorkerDiagnosticScope<'a> {
    boot_id: &'a str,
    module_id: &'a str,
    binding_id: &'a str,
    generation: u64,
    artifact_id: &'a str,
    artifact_version: &'a str,
    build_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
struct VerifiedDepartedOwner {
    receipt: Value,
    token: String,
}

impl VerifiedDepartedOwner {
    fn matches(&self, receipt: &Value, token: &str) -> bool {
        self.receipt == *receipt && self.token.as_str() == token
    }
}

/// Durable post-worker exact-family departure proof, supervisor-owned.
/// Binds descriptor fingerprint, module/artifact/version, scope binding/gen,
/// launch boot, owner receipt (pid/birth/job/purpose/token) plus helper
/// image, and verified zero ActiveProcesses. Published via the shared
/// private-file authority (`replace_private_durable`: synced temp +
/// write-through replace on Windows, parent sync on Unix) BEFORE the retained
/// query is released; consumed only via `read_private_durable` with a
/// successful read barrier plus exact full-tuple match. Visible bytes after a
/// failed write or read barrier are durability-UNKNOWN and never authority;
/// a failed write never deletes the final path (only its temp), so a later
/// durable read may still promote the genuine proof. `observer` is diagnostic
/// only (retained query vs fresh open at write time), not a security gate;
/// both values are accepted. MissingJob UNKNOWN is preserved: a missing named
/// Job never counts as empty; only this genuine previous receipt bridges a
/// restart that lost the in-memory query handle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerDepartureReceipt {
    version: u64,
    module_id: String,
    artifact_id: String,
    artifact_version: String,
    build_id: Option<String>,
    descriptor_fingerprint: String,
    scope: ServiceScope,
    boot_id: String,
    owner: Value,
    owner_helper: ProcessIdentity,
    active_processes: u64,
    family_empty: bool,
    observer: String,
}

impl Service {
    fn new(initialization: ServiceInitialization) -> Self {
        let prior_state = has_prior_module_state(&initialization.state_dir);
        let (status, _) = watch::channel(SupervisorStatus {
            module_id: initialization.descriptor.module_id.to_string(),
            artifact_id: initialization.descriptor.artifact.artifact_id.to_string(),
            artifact_version: initialization.descriptor.artifact.version.to_string(),
            build_id: initialization.descriptor.artifact.build_id.clone(),
            scope: initialization.scope.clone(),
            admission: initialization.admission.borrow().clone(),
            lifecycle: if prior_state {
                LifecycleState::WaitingForKernel
            } else {
                LifecycleState::WaitingForDemand
            },
            worker: None,
            owner: None,
            worker_boot_id: None,
            hello_failure: None,
            worker_diagnostic_code: None,
            effect_certainty: ModuleEffectCertainty::Unknown,
            failure_stage: None,
            readback_required: prior_state,
            unknown_operation_ids: Vec::new(),
            unknown_operation_count: 0,
            unknown_operation_ids_truncated: false,
            restart: RestartStatus {
                starts_in_window: 0,
                last_delay_ms: None,
                exhausted: false,
            },
            last_failure: None,
        });
        let (demand_epoch, _) = watch::channel(0);
        let (recovery_epoch, _) = watch::channel(0);
        Self {
            descriptor: initialization.descriptor,
            descriptor_fingerprint: initialization.descriptor_fingerprint,
            scope: initialization.scope,
            state_dir: initialization.state_dir,
            state_root: initialization.state_root,
            host_data_dir: initialization.host_data_dir,
            ipc: initialization.ipc,
            launch_config: initialization.launch_config,
            module_client_id: initialization.module_client_id,
            protocol: initialization.protocol,
            module_contract_json: initialization.module_contract_json,
            owner_executable: initialization.owner_executable,
            resolver: initialization.resolver,
            admission: initialization.admission,
            status,
            confirmed_ready: Mutex::new(None),
            demands: Mutex::new(HashMap::new()),
            demand_gate: initialization.demand_gate,
            demand_epoch,
            recovery_epoch,
            readback_required: AtomicBool::new(prior_state),
            scope_quarantined: AtomicBool::new(false),
            unknown_operation_ids: Mutex::new(BTreeSet::new()),
            runner: AsyncMutex::new(None),
            helper_child_custody: AsyncMutex::new(None),
            #[cfg(windows)]
            owner_family_query: Mutex::new(None),
            #[cfg(test)]
            module_membership_probe: Mutex::new(None),
            #[cfg(test)]
            prior_owner_timeout: Mutex::new(None),
            #[cfg(test)]
            final_owner_timeout: Mutex::new(None),
        }
    }

    fn remember_owner_family_query(&self, identity: &Value, token: &str) {
        #[cfg(windows)]
        {
            let mut query = lock(&self.owner_family_query);
            if query
                .as_ref()
                .is_some_and(|query| query.identity != *identity)
            {
                *query = None;
            }
            if query.is_none()
                && let Ok(opened) = swarm_process::Group::open_module_query(identity, token)
            {
                *query = Some(opened);
            }
        }
        #[cfg(not(windows))]
        let _ = (identity, token);
    }

    fn observe_named_owner_family(
        &self,
        identity: &Value,
        token: &str,
    ) -> ProcessFamilyObservation {
        observe_process_family(identity, token)
    }

    fn observe_owner_family(&self, identity: &Value, token: &str) -> ProcessFamilyObservation {
        #[cfg(windows)]
        {
            let query = lock(&self.owner_family_query);
            if let Some(query) = query.as_ref().filter(|query| query.identity == *identity) {
                return match query.family_empty() {
                    Ok(true) => ProcessFamilyObservation::ConfirmedEmpty,
                    Ok(false) => ProcessFamilyObservation::Retained,
                    Err(error) => ProcessFamilyObservation::Unknown(error),
                };
            }
        }
        self.observe_named_owner_family(identity, token)
    }

    fn release_owner_family_query(&self) {
        #[cfg(windows)]
        {
            *lock(&self.owner_family_query) = None;
        }
    }

    fn prior_owner_observation_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(timeout) = *lock(&self.prior_owner_timeout) {
            return timeout;
        }
        PRIOR_OWNER_OBSERVATION_TIMEOUT
    }

    fn final_owner_observation_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(timeout) = *lock(&self.final_owner_timeout) {
            return timeout;
        }
        OWNER_FINAL_OBSERVATION_TIMEOUT
    }

    async fn demand(
        self: &Arc<Self>,
        cause: DemandCause,
        readback: Option<OperationReadback>,
    ) -> Result<DemandLease> {
        if let Some(readback) = readback {
            self.apply_readback(readback)?;
        }
        let key = cause.key()?;
        {
            let mut demands = lock(&self.demands);
            *demands.entry(key.clone()).or_default() += 1;
        }
        self.signal();
        if let Err(error) = self.ensure_runner().await {
            let mut demands = lock(&self.demands);
            if let Some(count) = demands.get_mut(&key) {
                if *count <= 1 {
                    demands.remove(&key);
                } else {
                    *count -= 1;
                }
            }
            return Err(error);
        }
        Ok(DemandLease {
            service: self.clone(),
            demand_key: key,
            released: false,
        })
    }

    async fn ensure_runner(self: &Arc<Self>) -> Result<()> {
        let mut runner = self.runner.lock().await;
        if runner.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }
        let current = self.status.borrow().lifecycle.clone();
        let has_custody = self.helper_child_custody.lock().await.is_some();
        let ownership_pending = matches!(
            current,
            LifecycleState::OwnerGroupRetained { .. }
                | LifecycleState::OwnerIdentityUnknown
                | LifecycleState::ProcessIdentityUnknown { .. }
        );
        if (self.scope_quarantined.load(Ordering::Acquire)
            || matches!(
                &current,
                LifecycleState::Isolated { .. }
                    | LifecycleState::OwnerIdentityUnknown
                    | LifecycleState::ProcessIdentityUnknown { .. }
            ))
            && !has_custody
            && !ownership_pending
        {
            return Err(Error::new(
                "MODULE_ISOLATED",
                "module is isolated pending a changed descriptor or exact recovery evidence",
            ));
        }
        if runner.as_ref().is_some_and(JoinHandle::is_finished)
            && !matches!(
                &current,
                LifecycleState::WaitingForDemand
                    | LifecycleState::WaitingForKernel
                    | LifecycleState::Completed { .. }
                    | LifecycleState::ProcessExited { .. }
            )
            && !has_custody
            && !ownership_pending
        {
            let pid = self
                .status
                .borrow()
                .worker
                .as_ref()
                .map(|worker| worker.pid);
            self.readback_required.store(true, Ordering::Release);
            self.update_status(|status| {
                status.lifecycle = LifecycleState::ProcessIdentityUnknown { pid };
                status.readback_required = true;
            });
            return Err(Error::new(
                "MODULE_SUPERVISOR_EXITED",
                "lifecycle task exited with unresolved process state; no replacement started",
            ));
        }
        let service = self.clone();
        *runner = Some(tokio::spawn(async move {
            let cleanup = service.clone();
            let exit = service.run_lifecycle().await;
            cleanup.after_runner_exit(exit).await;
        }));
        Ok(())
    }

    async fn retain_helper_child_custody(
        &self,
        child: Child,
        identity: Option<ProcessIdentity>,
        boot_id: Option<String>,
        reason_code: &str,
    ) {
        let retained = RetainedHelperChild {
            pid: child
                .id()
                .or_else(|| identity.as_ref().map(|identity| identity.pid)),
            child,
            identity,
            boot_id,
            reason_code: reason_code.to_owned(),
        };
        loop {
            let mut custody = self.helper_child_custody.lock().await;
            if custody.is_none() {
                *custody = Some(retained);
                break;
            }
            drop(custody);
            // The per-scope runner normally makes this path unreachable. If a
            // concurrent recovery reaches it, keep this actual Child alive in
            // this future until the existing bounded custody slot is released.
            time::sleep(OWNER_RECONCILE_RETRY).await;
        }
        self.readback_required.store(true, Ordering::Release);
        self.update_status(|status| status.readback_required = true);
    }

    async fn retain_helper_after_monitor_error(
        &self,
        helper: Child,
        helper_identity: ProcessIdentity,
        boot_id: &str,
        error: Error,
    ) {
        let reason_code = error.code;
        self.record_failure(
            &reason_code,
            "helper/worker identity or owner-family proof is unresolved",
        );
        self.readback_required.store(true, Ordering::Release);
        self.update_status(|status| {
            status.lifecycle = match reason_code.as_str() {
                "MODULE_OWNER_FAMILY_NOT_EMPTY" | "MODULE_FINAL_FAMILY_OBSERVATION_TIMEOUT" => {
                    LifecycleState::OwnerGroupRetained {
                        owner_pid: helper_identity.pid,
                        reason: OwnerRetainedReason::OwnerFamilyNotEmpty,
                    }
                }
                "MODULE_OWNER_FAMILY_OBSERVATION_UNKNOWN"
                | "MODULE_FINAL_FAMILY_OBSERVATION_UNKNOWN"
                | "MODULE_MEMBERSHIP_UNKNOWN" => LifecycleState::OwnerGroupRetained {
                    owner_pid: helper_identity.pid,
                    reason: OwnerRetainedReason::OwnerFamilyObservationUnknown,
                },
                _ => LifecycleState::OwnerIdentityUnknown,
            };
            status.owner = Some(helper_identity.clone());
            status.readback_required = true;
        });
        self.retain_helper_child_custody(
            helper,
            Some(helper_identity),
            Some(boot_id.to_owned()),
            &reason_code,
        )
        .await;
    }

    async fn restore_helper_child_custody(&self, retained: RetainedHelperChild) {
        let mut retained = Some(retained);
        loop {
            let mut custody = self.helper_child_custody.lock().await;
            if custody.is_none() {
                *custody = retained.take();
                return;
            }
            drop(custody);
            time::sleep(OWNER_RECONCILE_RETRY).await;
        }
    }

    async fn reconcile_retained_helper_child(&self) -> Result<()> {
        let Some(mut retained) = self.helper_child_custody.lock().await.take() else {
            return Ok(());
        };

        let departure = self.wait_for_prior_owner_to_depart_with_custody(true).await;
        let departed = match departure {
            Ok(Some(departed)) => departed,
            Ok(None) => {
                self.record_failure(
                    &retained.reason_code,
                    "retained helper has no exact owner-family departure proof",
                );
                self.restore_helper_child_custody(retained).await;
                return Err(Error::new(
                    "MODULE_CHILD_CUSTODY_UNRESOLVED",
                    "retained module helper has no exact owner-family departure proof",
                ));
            }
            Err(error) => {
                self.record_failure(
                    &retained.reason_code,
                    "retained helper owner-family reconciliation remains unresolved",
                );
                self.restore_helper_child_custody(retained).await;
                return Err(error);
            }
        };

        let departed_pid = departed.receipt["process"]["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok());
        let direct_child_matches_departed_owner = departed_pid.is_some_and(|owner_pid| {
            retained.pid.is_none_or(|pid| pid == owner_pid)
                && retained
                    .identity
                    .as_ref()
                    .is_none_or(|identity| identity.pid == owner_pid)
        });
        if !direct_child_matches_departed_owner {
            self.record_failure(
                "MODULE_CHILD_CUSTODY_IDENTITY_MISMATCH",
                "retained direct Child does not match the departed owner receipt",
            );
            self.restore_helper_child_custody(retained).await;
            return Err(Error::new(
                "MODULE_CHILD_CUSTODY_IDENTITY_MISMATCH",
                "retained direct Child does not match the departed owner receipt",
            ));
        }

        let exit = match retained.child.try_wait() {
            Ok(Some(exit)) => exit,
            Ok(None) => {
                self.record_failure(
                    "MODULE_CHILD_CUSTODY_UNRESOLVED",
                    "owner-family departure was observed while the retained direct Child remains live",
                );
                self.restore_helper_child_custody(retained).await;
                return Err(Error::new(
                    "MODULE_CHILD_CUSTODY_UNRESOLVED",
                    "retained direct module helper has not reported its exit",
                ));
            }
            Err(_) => {
                self.record_failure(
                    "MODULE_CHILD_WAIT_UNKNOWN",
                    "retained direct Child exit could not be observed after family reconciliation",
                );
                self.restore_helper_child_custody(retained).await;
                return Err(Error::new(
                    "MODULE_CHILD_WAIT_UNKNOWN",
                    "retained direct module helper exit remains unknown",
                ));
            }
        };

        let owner = retained.identity.clone().or_else(|| {
            serde_json::from_value::<ProcessIdentity>(departed.receipt["process"].clone()).ok()
        });
        self.update_status(|status| {
            status.lifecycle = LifecycleState::ProcessExited {
                exit_code: exit.code(),
                exit_proven: true,
            };
            if let Some(owner) = owner {
                status.owner = Some(owner);
            }
            if status.worker_boot_id.is_none() {
                status.worker_boot_id = retained.boot_id.clone();
            }
        });
        self.release_owner_family_query();
        Ok(())
    }

    // This cleanup may spawn another lifecycle task. A boxed Send boundary
    // avoids a recursive opaque-future type while preserving the runner mutex.
    fn after_runner_exit(
        self: &Arc<Self>,
        exit: LifecycleRunExit,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut runner = self.runner.lock().await;
            *runner = None;
            let custody_pending = self.helper_child_custody.lock().await.is_some();
            let owner_pending = matches!(
                &self.status.borrow().lifecycle,
                LifecycleState::OwnerGroupRetained { .. }
                    | LifecycleState::OwnerIdentityUnknown
                    | LifecycleState::ProcessIdentityUnknown { .. }
            );
            if (!self.has_demand() && !custody_pending && !owner_pending)
                || (matches!(
                    self.admission.borrow().clone(),
                    AdmissionState::Closed { .. }
                ) && !custody_pending)
                || (self.scope_quarantined.load(Ordering::Acquire)
                    && !custody_pending
                    && !owner_pending)
                || (!custody_pending
                    && !owner_pending
                    && !matches!(exit, LifecycleRunExit::WaitingForDemand)
                    && !matches!(
                        &self.status.borrow().lifecycle,
                        LifecycleState::WaitingForDemand | LifecycleState::WaitingForKernel
                    ))
            {
                return;
            }

            if custody_pending || owner_pending {
                time::sleep(OWNER_RECONCILE_RETRY).await;
            }

            // Demand can race the runner's final no-demand check. Recheck and
            // replace the finished handle under the same mutex used by demand().
            let service = self.clone();
            *runner = Some(tokio::spawn(async move {
                let cleanup = service.clone();
                let exit = service.run_lifecycle().await;
                cleanup.after_runner_exit(exit).await;
            }));
        })
    }

    async fn can_replace_descriptor(&self) -> bool {
        if self.has_demand()
            || self.readback_required.load(Ordering::Acquire)
            || !lock(&self.unknown_operation_ids).is_empty()
            || lock(&self.confirmed_ready)
                .as_ref()
                .is_some_and(|ready| !ready.queued)
        {
            return false;
        }
        let status = self.status.borrow().clone();
        let settled_lifecycle = matches!(
            &status.lifecycle,
            LifecycleState::WaitingForDemand
                | LifecycleState::Completed { .. }
                | LifecycleState::Isolated { .. }
        );
        // exit_proven is sourced only from exact departed_empty evidence.
        // Require that the same boot and both process identities remain with
        // that transition; an exit code or helper PID alone is insufficient.
        let exact_departed_exit = matches!(
            &status.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            }
        ) && status.owner.is_some()
            && status.worker.is_some()
            && status
                .worker_boot_id
                .as_deref()
                .is_some_and(|boot_id| Uuid::parse_str(boot_id).is_ok());
        if !settled_lifecycle && !exact_departed_exit {
            return false;
        }
        self.runner
            .lock()
            .await
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
    }

    fn apply_readback(&self, readback: OperationReadback) -> Result<()> {
        readback.validate_for(&self.scope)?;
        // `OperationReadback::validate_for` requires a complete exact-scope
        // snapshot. Rebuild the retained set from that snapshot so Operations
        // that were removed or moved out of this binding cannot survive forever
        // merely because they are absent from the latest result.
        let mut unknown = BTreeSet::new();
        let mut overflow_count = 0usize;
        for operation in &readback.operations {
            if is_unresolved(&operation.state) {
                if unknown.len() < MAX_UNKNOWN_OPERATIONS {
                    unknown.insert(operation.operation_id.clone());
                } else {
                    overflow_count = overflow_count.saturating_add(1);
                }
            }
        }
        let ids: Vec<_> = unknown.iter().cloned().collect();
        let unknown_count = unknown.len().saturating_add(overflow_count);
        *lock(&self.unknown_operation_ids) = unknown;
        if overflow_count > 0 {
            self.update_status(|status| {
                status.readback_required = true;
                status.unknown_operation_ids = ids;
                status.unknown_operation_count = unknown_count;
                status.unknown_operation_ids_truncated = true;
                status.last_failure = Some(FailureSummary {
                    code: "MODULE_UNKNOWN_OPERATION_LIMIT".to_owned(),
                    detail: "complete scoped readback exceeds the bounded unresolved Operation ID set; descriptor replacement remains blocked".to_owned(),
                });
            });
            return Err(Error::new(
                "MODULE_UNKNOWN_OPERATION_LIMIT",
                "too many unresolved Operations to retain individually; no descriptor replacement is allowed",
            ));
        }
        self.readback_required.store(false, Ordering::Release);
        self.update_status(|status| {
            status.readback_required = false;
            status.unknown_operation_ids = ids;
            status.unknown_operation_count = unknown_count;
            status.unknown_operation_ids_truncated = false;
        });
        self.bump(&self.recovery_epoch);
        Ok(())
    }

    fn confirm_module_hello(&self, boot_id: &str) -> Result<()> {
        if self.scope_quarantined.load(Ordering::Acquire) {
            return Err(Error::new(
                "MODULE_SUPERVISOR_SCOPE_QUARANTINED",
                "module supervisor scope is permanently quarantined after rejection",
            ));
        }
        let status = self.status.borrow().clone();
        if status.worker_boot_id.as_deref() != Some(boot_id) {
            return Err(Error::new(
                "MODULE_WORKER_BOOT_MISMATCH",
                "module.hello belongs to a different worker boot",
            ));
        }
        let worker = status.worker.clone().ok_or_else(|| {
            Error::new(
                "MODULE_WORKER_IDENTITY_UNKNOWN",
                "module.hello cannot reconcile an unknown worker identity",
            )
        })?;
        if !process_identity_is_live(&worker)? {
            return Err(Error::new(
                "MODULE_WORKER_EXITED",
                "module.hello worker process is no longer the recorded live incarnation",
            ));
        }
        let mut transition_error = None;
        let transitioned = self.status.send_if_modified(|current| {
            if current.worker_boot_id.as_deref() != Some(boot_id) {
                transition_error = Some((
                    "MODULE_WORKER_BOOT_MISMATCH",
                    "module.hello belongs to a different worker boot",
                ));
                return false;
            }
            let Some(current_worker) = current.worker.as_ref() else {
                transition_error = Some((
                    "MODULE_WORKER_IDENTITY_UNKNOWN",
                    "module.hello cannot reconcile an unknown worker identity",
                ));
                return false;
            };
            if !same_process_identity(current_worker, &worker) {
                transition_error = Some((
                    "MODULE_WORKER_IDENTITY_MISMATCH",
                    "module.hello worker changed before status confirmation",
                ));
                return false;
            }
            if !matches!(
                &current.lifecycle,
                LifecycleState::Starting {
                    boot_id: current_boot
                } | LifecycleState::ProcessRunning {
                    boot_id: current_boot
                } if current_boot == boot_id
            ) {
                transition_error = Some((
                    "MODULE_WORKER_BOOT_MISMATCH",
                    "module.hello cannot confirm a worker outside its starting lifecycle",
                ));
                return false;
            }
            let mut ready = lock(&self.confirmed_ready);
            if let Some(previous) = ready.as_ref() {
                if previous.status.worker_boot_id.as_deref() == Some(boot_id) {
                    if !previous.status.worker.as_ref().is_some_and(|confirmed| {
                        same_process_identity(confirmed, current_worker)
                    }) {
                        transition_error = Some((
                            "MODULE_WORKER_IDENTITY_MISMATCH",
                            "the confirmed worker identity changed within the same boot",
                        ));
                        return false;
                    }
                } else if !previous.queued {
                    transition_error = Some((
                        "MODULE_READY_OBSERVATION_PENDING",
                        "the previous Ready must enter the observation queue before another boot is confirmed",
                    ));
                    return false;
                }
            }
            current.lifecycle = LifecycleState::ProcessRunning {
                boot_id: boot_id.to_owned(),
            };
            current.readback_required = false;
            if let Some(failure) = current.hello_failure.take()
                && current
                    .last_failure
                    .as_ref()
                    .is_some_and(|last| last.code == failure.error_code)
            {
                current.last_failure = None;
            }
            if ready.as_ref().is_none_or(|previous| {
                previous.status.worker_boot_id.as_deref() != Some(boot_id)
            }) {
                *ready = Some(ConfirmedReady { status: current.clone(), queued: false });
            }
            true
        });
        if !transitioned {
            let (code, detail) = transition_error.unwrap_or((
                "MODULE_WORKER_BOOT_MISMATCH",
                "module.hello status changed before confirmation",
            ));
            return Err(Error::new(code, detail));
        }
        self.readback_required.store(false, Ordering::Release);
        self.bump(&self.recovery_epoch);
        Ok(())
    }

    fn reconcile_retained_module_hello(&self, boot_id: &str) -> Result<()> {
        if !self.status.borrow().matches_current_hello(boot_id) {
            return Ok(());
        }
        match self.confirm_module_hello(boot_id) {
            Ok(()) => Ok(()),
            Err(error) => {
                // Recheck under the status write lock: the attempt may have
                // changed during the strict worker identity probe above.
                if self.record_module_hello_failure(boot_id, &error.code)? {
                    return Err(error);
                }
                Ok(())
            }
        }
    }

    fn record_module_hello_failure(&self, boot_id: &str, error_code: &str) -> Result<bool> {
        if boot_id.is_empty()
            || boot_id.len() > 128
            || boot_id.chars().any(char::is_control)
            || error_code.is_empty()
            || error_code.len() > 128
            || !error_code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::invalid(
                "module hello diagnostic is outside its bound",
            ));
        }
        let failure = ModuleHelloFailure {
            boot_id: boot_id.to_owned(),
            error_code: error_code.to_owned(),
        };
        let recorded = self.status.send_if_modified(|status| {
            if !status.matches_current_hello(boot_id) {
                return false;
            }
            if let LifecycleState::ProcessRunning { boot_id: current } = &status.lifecycle {
                status.lifecycle = LifecycleState::Starting {
                    boot_id: current.clone(),
                };
            }
            status.hello_failure = Some(failure.clone());
            status.last_failure = Some(FailureSummary {
                code: error_code.to_owned(),
                detail: "the exact Store module.hello boot could not be confirmed".to_owned(),
            });
            status.readback_required = true;
            self.readback_required.store(true, Ordering::Release);
            true
        });
        Ok(recorded)
    }

    fn quarantine(&self, error_code: &str) -> Result<()> {
        if error_code.is_empty()
            || error_code.len() > 128
            || !error_code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::invalid(
                "module observation rejection code is invalid",
            ));
        }
        self.scope_quarantined.store(true, Ordering::Release);
        self.readback_required.store(true, Ordering::Release);
        self.update_status(|status| {
            let code = status
                .last_failure
                .as_ref()
                .map(|failure| failure.code.clone())
                .unwrap_or_else(|| error_code.to_owned());
            status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                reason: "module supervisor observation was permanently rejected".to_owned(),
            });
            status.readback_required = true;
            status.last_failure = Some(FailureSummary {
                code,
                detail: "this exact binding generation is quarantined pending explicit recovery"
                    .to_owned(),
            });
        });
        Ok(())
    }

    async fn run_lifecycle(self: Arc<Self>) -> LifecycleRunExit {
        let policy = self.descriptor.restart;
        // A fresh binding has no scope directory yet. Verify/create the private
        // path before either reading its history or persisting the first budget.
        let mut starts = match self
            .prepare_state_dir()
            .and_then(|()| self.read_restart_history())
        {
            Ok(starts) => starts,
            Err(error) => {
                self.record_failure(
                    &error.code,
                    "persisted restart budget is invalid or unavailable",
                );
                self.update_status(|status| {
                    status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                        reason: "restart history cannot be verified".to_owned(),
                    });
                    status.restart.exhausted = true;
                });
                return LifecycleRunExit::Stopped;
            }
        };
        if let Err(error) = self.reconcile_retained_helper_child().await {
            self.record_failure(
                &error.code,
                "retained helper custody is waiting for exact owner-family departure",
            );
            self.readback_required.store(true, Ordering::Release);
            self.update_status(|status| {
                status.readback_required = true;
                if !matches!(
                    status.lifecycle,
                    LifecycleState::OwnerGroupRetained { .. }
                        | LifecycleState::OwnerIdentityUnknown
                        | LifecycleState::ProcessIdentityUnknown { .. }
                ) {
                    status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                }
            });
            return LifecycleRunExit::Stopped;
        }
        let mut demand_epoch = self.demand_epoch.subscribe();
        let mut admission = self.admission.subscribe();
        let mut recovery = self.recovery_epoch.subscribe();

        loop {
            if self.scope_quarantined.load(Ordering::Acquire) {
                self.update_status(|status| {
                    status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                        reason: "module supervisor observation was permanently rejected".to_owned(),
                    });
                    status.readback_required = true;
                });
                return LifecycleRunExit::Stopped;
            }
            if !self.has_demand() {
                self.update_status(|status| {
                    if !status.has_proven_terminal()
                        && !matches!(
                            status.lifecycle,
                            LifecycleState::OwnerGroupRetained { .. }
                                | LifecycleState::OwnerIdentityUnknown
                                | LifecycleState::ProcessIdentityUnknown { .. }
                        )
                    {
                        status.lifecycle = LifecycleState::WaitingForDemand;
                        status.worker = None;
                        status.owner = None;
                        status.worker_boot_id = None;
                    }
                });
                return LifecycleRunExit::WaitingForDemand;
            }
            if matches!(admission.borrow().clone(), AdmissionState::Closed { .. }) {
                self.update_status(|status| {
                    status.admission = admission.borrow().clone();
                    status.set_prelaunch_lifecycle(LifecycleState::WaitingForKernel);
                });
                tokio::select! {
                    _ = demand_epoch.changed() => {},
                    changed = admission.changed() => {
                        if changed.is_err() { return LifecycleRunExit::Stopped; }
                    }
                }
                continue;
            }
            self.update_status(|status| status.admission = AdmissionState::Open);

            let now = match unix_time_ms() {
                Ok(now) => now,
                Err(error) => {
                    self.record_failure(&error.code, "system clock cannot verify restart policy");
                    self.update_status(|status| {
                        status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                            reason: "restart policy time cannot be verified".to_owned(),
                        });
                        status.restart.exhausted = true;
                    });
                    return LifecycleRunExit::Stopped;
                }
            };
            prune_starts(&mut starts, policy.window_ms, now);
            if starts.len() >= usize::from(policy.max_starts) {
                self.update_status(|status| {
                    status.restart.starts_in_window = starts.len();
                    status.restart.exhausted = true;
                    status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                        reason: "restart budget exhausted; a changed valid descriptor is required"
                            .to_owned(),
                    });
                });
                return LifecycleRunExit::Stopped;
            }
            self.update_status(|status| {
                status.restart.starts_in_window = starts.len();
                status.restart.exhausted = false;
            });
            if !starts.is_empty() {
                let delay = jittered_delay(&policy, starts.len().saturating_sub(1));
                let deadline = Instant::now() + delay;
                self.update_status(|status| {
                    status.restart.starts_in_window = starts.len();
                    status.restart.last_delay_ms = Some(delay.as_millis() as u64);
                    status.set_prelaunch_lifecycle(LifecycleState::RestartBackoff {
                        delay_ms: delay.as_millis() as u64,
                    });
                });
                loop {
                    if !self.has_demand()
                        || matches!(admission.borrow().clone(), AdmissionState::Closed { .. })
                    {
                        break;
                    }
                    tokio::select! {
                        _ = time::sleep_until(deadline) => break,
                        _ = demand_epoch.changed() => {},
                        _ = recovery.changed() => {},
                        changed = admission.changed() => {
                            if changed.is_err() { return LifecycleRunExit::Stopped; }
                        }
                    }
                }
                if !self.has_demand() {
                    self.update_status(|status| {
                        status.set_prelaunch_lifecycle(LifecycleState::WaitingForDemand)
                    });
                    return LifecycleRunExit::WaitingForDemand;
                }
                if matches!(admission.borrow().clone(), AdmissionState::Closed { .. }) {
                    continue;
                }
            }

            if let Err(error) = self.prepare_state_dir() {
                self.record_failure(
                    "MODULE_STATE_DIRECTORY",
                    "module state directory is unavailable",
                );
                self.update_status(|status| {
                    status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                        reason: "module state directory could not be prepared".to_owned(),
                    });
                    status.restart.exhausted = true;
                    status.last_failure = Some(FailureSummary {
                        code: error.code,
                        detail: "module state directory could not be prepared".to_owned(),
                    });
                });
                return LifecycleRunExit::Stopped;
            }
            let departed_prior_owner = match self.wait_for_prior_owner_to_depart().await {
                Ok(owner) => owner,
                Err(error) => {
                    self.record_failure(
                        &error.code,
                        "prior module owner identity is missing or unresolved",
                    );
                    self.update_status(|status| {
                        if !matches!(status.lifecycle, LifecycleState::OwnerGroupRetained { .. }) {
                            status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                        }
                        status.readback_required = true;
                    });
                    self.readback_required.store(true, Ordering::Release);
                    return LifecycleRunExit::Stopped;
                }
            };
            if !self.has_demand()
                || matches!(admission.borrow().clone(), AdmissionState::Closed { .. })
            {
                continue;
            }
            if let Err(error) = self.validate_launch_config_final_names() {
                self.record_failure(
                    &error.code,
                    "launch config final environment name failed before restart accounting",
                );
                self.update_status(|status| {
                    status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                        reason: "launch config produces an invalid final environment name"
                            .to_owned(),
                    });
                    status.restart.exhausted = true;
                });
                return LifecycleRunExit::Stopped;
            }
            if let Err(error) = self.clear_prior_helper_result() {
                self.record_failure(
                    &error.code,
                    "prior helper receipts could not be cleared after owner departure",
                );
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                    status.readback_required = true;
                });
                self.readback_required.store(true, Ordering::Release);
                return LifecycleRunExit::Stopped;
            }

            // The helper requires the same ownership marker before any other
            // scope files exist. Initialize it only after exact prior departure,
            // then let the helper acquire its own lifetime lock at process start.
            match swarm_process::module_owner::acquire_module_state_marker(&self.state_dir) {
                Ok(marker) => drop(marker),
                Err(error) => {
                    self.record_failure(&error.code, "module state marker could not be verified");
                    self.update_status(|status| {
                        status.set_prelaunch_lifecycle(LifecycleState::Isolated {
                            reason: "module state marker initialization failed".to_owned(),
                        });
                        status.restart.exhausted = true;
                    });
                    return LifecycleRunExit::Stopped;
                }
            }

            let boot_id = self.allocate_worker_boot();
            starts.push_back(now);
            if let Err(error) = self.write_restart_history(&starts) {
                self.record_failure(
                    &error.code,
                    "restart attempt could not be persisted before helper spawn",
                );
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "restart attempt persistence failed before process start"
                            .to_owned(),
                    };
                    status.restart.exhausted = true;
                });
                return LifecycleRunExit::Stopped;
            }
            if let Err(error) = self.write_launch_attempt(&boot_id, None) {
                self.record_failure(&error.code, "launch intent could not be persisted safely");
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "launch intent persistence failed before helper start".to_owned(),
                    };
                    status.restart.exhausted = true;
                });
                return LifecycleRunExit::Stopped;
            }

            let (mut helper, _plan_path) = match self.spawn_owner_helper(&boot_id) {
                Ok(spawned) => spawned,
                Err(error) => {
                    if let Err(cleanup_error) = self.remove_plan(&boot_id) {
                        self.record_failure(
                            &cleanup_error.code,
                            "failed helper start left its private launch plan unresolved",
                        );
                        self.update_status(|status| {
                            status.lifecycle = LifecycleState::Isolated {
                                reason: "private launch plan could not be removed after a proven helper spawn failure".to_owned(),
                            };
                            status.restart.exhausted = true;
                        });
                        return LifecycleRunExit::Stopped;
                    }
                    if let Err(cleanup_error) = self.remove_launch_attempt() {
                        self.record_failure(
                            &cleanup_error.code,
                            "failed helper start left unresolved launch intent",
                        );
                        self.update_status(|status| {
                            status.lifecycle = LifecycleState::Isolated {
                                reason: "helper spawn failure left unresolved launch intent"
                                    .to_owned(),
                            };
                            status.restart.exhausted = true;
                        });
                        return LifecycleRunExit::Stopped;
                    }
                    self.record_failure(&error.code, "module-owner helper could not be started");
                    self.update_status(|status| {
                        status.restart.starts_in_window = starts.len();
                        status.lifecycle = LifecycleState::ProcessExited {
                            exit_code: None,
                            // No helper process was created, so no process-family
                            // departure can be proven on this path.
                            exit_proven: false,
                        };
                        status.owner = None;
                        status.worker = None;
                    });
                    continue;
                }
            };
            let receipt_deadline = Instant::now() + HELPER_RECEIPT_TIMEOUT;
            let Some(pid) = helper.id() else {
                self.record_failure(
                    "MODULE_OWNER_IDENTITY",
                    "module-owner helper PID was unavailable",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::ProcessIdentityUnknown { pid: None };
                    status.readback_required = true;
                });
                self.retain_helper_child_custody(
                    helper,
                    None,
                    Some(boot_id.clone()),
                    "MODULE_OWNER_IDENTITY",
                )
                .await;
                return LifecycleRunExit::Stopped;
            };
            let helper_identity = match capture_identity(pid) {
                Ok(identity) => identity,
                Err(error) => {
                    self.record_failure(
                        &error.code,
                        "module-owner helper image identity could not be captured",
                    );
                    self.readback_required.store(true, Ordering::Release);
                    self.update_status(|status| {
                        status.lifecycle =
                            LifecycleState::ProcessIdentityUnknown { pid: Some(pid) };
                        status.readback_required = true;
                    });
                    self.retain_helper_child_custody(
                        helper,
                        None,
                        Some(boot_id.clone()),
                        &error.code,
                    )
                    .await;
                    return LifecycleRunExit::Stopped;
                }
            };
            if let Err(error) = self.write_launch_attempt(&boot_id, Some(&helper_identity)) {
                self.record_failure(
                    &error.code,
                    "spawned helper identity could not be persisted",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::ProcessIdentityUnknown { pid: Some(pid) };
                    status.owner = Some(helper_identity.clone());
                    status.readback_required = true;
                });
                self.retain_helper_child_custody(
                    helper,
                    Some(helper_identity),
                    Some(boot_id.clone()),
                    &error.code,
                )
                .await;
                return LifecycleRunExit::Stopped;
            }
            if !self.image_matches_helper(&helper_identity) {
                self.record_failure(
                    "MODULE_OWNER_HELPER_MISMATCH",
                    "running helper image differs from the configured exact helper",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                    status.owner = Some(helper_identity.clone());
                    status.readback_required = true;
                });
                self.retain_helper_child_custody(
                    helper,
                    Some(helper_identity),
                    Some(boot_id.clone()),
                    "MODULE_OWNER_HELPER_MISMATCH",
                )
                .await;
                return LifecycleRunExit::Stopped;
            }

            self.update_status(|status| {
                status.owner = Some(helper_identity.clone());
                status.worker = None;
                status.lifecycle = LifecycleState::Starting {
                    boot_id: boot_id.clone(),
                };
            });
            let attempt = match self
                .monitor_owner_helper(
                    &mut helper,
                    &boot_id,
                    &helper_identity,
                    departed_prior_owner.as_ref(),
                    receipt_deadline,
                )
                .await
            {
                Ok(attempt) => attempt,
                Err(error) => {
                    self.retain_helper_after_monitor_error(
                        helper,
                        helper_identity,
                        &boot_id,
                        error,
                    )
                    .await;
                    return LifecycleRunExit::Stopped;
                }
            };
            if !attempt.safe_to_replace {
                self.record_failure(
                    attempt
                        .failure_code
                        .as_deref()
                        .unwrap_or("MODULE_WORKER_IDENTITY"),
                    "helper exited without complete exact worker or certified pre-spawn evidence",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                    status.owner = Some(helper_identity.clone());
                    status.worker = attempt.worker.clone();
                    status.worker_boot_id = Some(boot_id.clone());
                    status.readback_required = true;
                });
                self.retain_helper_child_custody(
                    helper,
                    Some(helper_identity),
                    Some(boot_id.clone()),
                    attempt
                        .failure_code
                        .as_deref()
                        .unwrap_or("MODULE_LAUNCH_RESULT_MISSING"),
                )
                .await;
                return LifecycleRunExit::Stopped;
            }
            if let Err(error) = self.remove_plan(&boot_id) {
                self.record_failure(
                    &error.code,
                    "private launch plan could not be removed after helper exit",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "private launch plan cleanup failed".to_owned(),
                    };
                    status.owner = Some(helper_identity.clone());
                    status.worker = attempt.worker.clone();
                    status.readback_required = true;
                });
                return LifecycleRunExit::Stopped;
            }
            if let Err(error) = self.remove_launch_attempt() {
                self.record_failure(
                    &error.code,
                    "verified owner exit left an unreadable launch intent",
                );
                self.readback_required.store(true, Ordering::Release);
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                    status.owner = Some(helper_identity.clone());
                    status.worker = attempt.worker.clone();
                    status.readback_required = true;
                });
                return LifecycleRunExit::Stopped;
            }
            if attempt.healthy_duration.is_some_and(|duration| {
                duration >= Duration::from_millis(policy.reset_after_healthy_ms)
            }) {
                // Only a live, exact worker receipt contributes a healthy
                // interval. Pre-spawn failures and unknown identities never
                // reset the bounded start budget.
                starts.clear();
                if let Err(error) = self.write_restart_history(&starts) {
                    self.record_failure(
                        &error.code,
                        "healthy restart budget reset could not be persisted",
                    );
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::Isolated {
                            reason: "healthy restart budget reset could not be persisted"
                                .to_owned(),
                        };
                        status.restart.exhausted = true;
                    });
                    return LifecycleRunExit::Stopped;
                }
            }

            let restart_needed = !self.scope_quarantined.load(Ordering::Acquire)
                && self.has_demand()
                && matches!(
                    self.descriptor.lifecycle,
                    LifecycleOwnership::OwnedService | LifecycleOwnership::ExternalAttach
                );
            self.record_owner_helper_exit(
                &attempt,
                &helper_identity,
                &boot_id,
                starts.len(),
                restart_needed,
            );
            if restart_needed {
                // A replacement adapter always begins at module.hello. The
                // Store retains uncertain Operations as Unknown. This lifecycle
                // never replays launch/input/native writes or treats an IPC
                // disconnect as proof that the process or owner has exited.
                continue;
            }
            // An owned service stopped only because demand was absent at the
            // sampled restart decision. A newly admitted lease may already be
            // waiting for runner cleanup; keep this scheduling fact separate
            // from the retained terminal evidence. Finite jobs stay stopped.
            if !self.scope_quarantined.load(Ordering::Acquire)
                && matches!(
                    self.descriptor.lifecycle,
                    LifecycleOwnership::OwnedService | LifecycleOwnership::ExternalAttach
                )
            {
                return LifecycleRunExit::WaitingForDemand;
            }
            return LifecycleRunExit::Stopped;
        }
    }
    fn record_owner_helper_exit(
        &self,
        attempt: &OwnerHelperExit,
        helper_identity: &ProcessIdentity,
        boot_id: &str,
        starts_in_window: usize,
        restart_needed: bool,
    ) {
        let needs_operation_readback = !attempt.exit.success() || restart_needed;
        self.readback_required
            .store(needs_operation_readback, Ordering::Release);
        self.update_status(|status| {
            status.lifecycle = if self.scope_quarantined.load(Ordering::Acquire) {
                LifecycleState::Isolated {
                    reason: "module supervisor observation was permanently rejected".to_owned(),
                }
            } else if attempt.exit.success()
                && !restart_needed
                && attempt.family_departure_proven
            {
                LifecycleState::Completed {
                    exit_code: attempt.exit.code(),
                }
            } else {
                LifecycleState::ProcessExited {
                    exit_code: attempt.exit.code(),
                    exit_proven: attempt.family_departure_proven,
                }
            };
            status.owner = Some(helper_identity.clone());
            status.worker = attempt.worker.clone();
            status.worker_boot_id = Some(boot_id.to_owned());
            status.worker_diagnostic_code = attempt.worker_diagnostic_code.clone();
            status.readback_required = needs_operation_readback;
            status.effect_certainty = attempt.effect_certainty;
            status.failure_stage = attempt
                .failure_stage
                .as_deref()
                .and_then(ModuleFailureStage::from_helper);
            status.restart.starts_in_window = starts_in_window;
            status.restart.exhausted = false;
            if needs_operation_readback {
                status.last_failure = Some(FailureSummary {
                    code: attempt
                        .failure_code
                        .clone()
                        .unwrap_or_else(|| "MODULE_EXITED".to_owned()),
                    detail: if let Some(stage) = attempt.failure_stage.as_deref() {
                        format!("helper certified no adapter start at {stage}; exact owner-family departure is proven and replacement begins in reconciliation-only mode")
                    } else if restart_needed {
                        "adapter exited while demand remained; exact owner-family departure is proven and replacement starts in reconciliation-only mode".to_owned()
                    } else {
                        "optional adapter exited; its outcome remains unknown until scoped Store readback or module.hello recovery".to_owned()
                    },
                });
            }
        });
    }

    fn spawn_owner_helper(&self, boot_id: &str) -> Result<(Child, PathBuf)> {
        let expected_hash = self
            .descriptor
            .launch
            .executable_sha256
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "MODULE_ARTIFACT_DIGEST_REQUIRED",
                    "module launch requires an exact executable SHA-256 pin",
                )
            })?;
        if self.descriptor.launch.argv.len() > 128 {
            return Err(Error::invalid(
                "module argv exceeds the bounded helper plan",
            ));
        }
        let executable = fs::canonicalize(&self.descriptor.launch.executable).map_err(|error| {
            Error::new(
                "MODULE_EXECUTABLE_UNAVAILABLE",
                format!("selected adapter executable cannot be resolved: {error}"),
            )
        })?;
        if !fs::metadata(&executable)?.is_file() {
            return Err(Error::new(
                "MODULE_EXECUTABLE_UNAVAILABLE",
                "selected adapter executable is not a file",
            ));
        }
        if self.descriptor.launch.working_directory.is_some() {
            return Err(Error::new(
                "MODULE_WORKING_DIRECTORY_UNSUPPORTED",
                "current module-owner helper plan has no reviewed working-directory field",
            ));
        }
        if !self
            .descriptor
            .launch
            .inherited_environment_allowlist
            .is_empty()
        {
            return Err(Error::new(
                "MODULE_INHERITED_ENV_UNSUPPORTED",
                "ambient inherited values are not copied into a module launch plan; use explicit literals or protected references",
            ));
        }

        let context = ProtectedResolverContext {
            module_id: self.descriptor.module_id.clone(),
            artifact: self.descriptor.artifact.clone(),
            scope: self.scope.clone(),
            protocol: self.protocol,
        };
        let resolver_map = self.resolver.resolver_map_path(&context)?;
        if !resolver_map.is_absolute() {
            return Err(Error::invalid(
                "protected resolver map path must be absolute",
            ));
        }
        let resolver_metadata = fs::symlink_metadata(&resolver_map)?;
        if resolver_metadata.file_type().is_symlink()
            || !resolver_metadata.is_file()
            || resolver_metadata.len() > MODULE_PLAN_LIMIT
        {
            return Err(Error::new(
                "MODULE_RESOLVER_MAP_INVALID",
                "trusted resolver map must be a bounded regular file without symlink traversal",
            ));
        }

        let mut environment = BTreeMap::<String, String>::new();
        let mut protected_refs = Vec::<Value>::new();
        let mut names = BTreeSet::<String>::new();
        for variable in &self.descriptor.launch.environment {
            self.add_launch_value(
                &variable.name,
                &variable.value,
                &mut environment,
                &mut protected_refs,
                &mut names,
            )?;
        }
        for (key, value) in &self.launch_config.values {
            let name = Self::module_config_environment_name(key)?;
            self.add_launch_value(
                &name,
                value,
                &mut environment,
                &mut protected_refs,
                &mut names,
            )?;
        }
        if let Some(reference) = &self.descriptor.launch.credential_ref {
            let name = "ELIOT_SWARM_MODULE_CREDENTIAL_FILE".to_owned();
            if !names.insert(name.clone()) {
                return Err(Error::new(
                    "MODULE_LAUNCH_ENV_DUPLICATE",
                    "module credential file environment name is already configured",
                ));
            }
            protected_refs.push(serde_json::json!({
                "name": name,
                "reference": reference.as_str(),
            }));
        }

        let host_config_path = self
            .descriptor
            .launch
            .argv
            .iter()
            .find_map(|value| match value {
                LaunchValue::ModuleHostConfigPath { schema_version } => Some(*schema_version),
                _ => None,
            })
            .map(|schema_version| self.materialize_module_host_config(schema_version))
            .transpose()?;

        let argv = self
            .descriptor
            .launch
            .argv
            .iter()
            .map(|value| match value {
                LaunchValue::Literal(value)
                    if !value.contains('\0') && value.len() <= 4_096 =>
                {
                    Ok(value.clone())
                }
                LaunchValue::Literal(_) => Err(Error::invalid(
                    "module argv entry is outside the bounded helper plan",
                )),
                LaunchValue::ModuleHostConfigPath { schema_version: 1 } => host_config_path
                    .as_ref()
                    .and_then(|path| path.to_str())
                    .filter(|path| path.len() <= 4_096)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::new(
                            "MODULE_HOST_CONFIG_INVALID",
                            "module host config path was not materialized as an absolute bounded Unicode path",
                        )
                    }),
                LaunchValue::ModuleHostConfigPath { .. } => Err(Error::new(
                    "MODULE_HOST_CONFIG_INVALID",
                    "module host config path marker uses an unsupported schema",
                )),
                LaunchValue::Protected(_) => Err(Error::new(
                    "MODULE_PROTECTED_ARGV_UNSUPPORTED",
                    "protected argv references require an indexed path resolver; no raw secret is passed",
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        let protocol = format!("{}.{}", self.protocol.major, self.protocol.minor);
        let plan_path = self.state_dir.join(format!("launch-{boot_id}.json"));
        let plan = serde_json::json!({
            "version": 1,
            "state_dir": self.state_dir,
            "executable": executable,
            "argv": argv,
            "protected_refs": protected_refs,
            "environment": environment.iter().map(|(name, value)| serde_json::json!({"name":name,"value":value})).collect::<Vec<_>>(),
            "module": self.descriptor.module_id.as_str(),
            "binding": self.scope.binding_id,
            "generation": self.scope.generation.to_string(),
            "module_client_id": self.module_client_id,
            "artifact_id": self.descriptor.artifact.artifact_id.as_str(),
            "artifact_version": self.descriptor.artifact.version.as_str(),
            "build_id": self.descriptor.artifact.build_id,
            "protocol": protocol,
            "boot_id": boot_id,
            "module_contract": self.module_contract_json,
        });
        let data = serde_json::to_vec(&plan)?;
        if data.len() as u64 > MODULE_PLAN_LIMIT {
            return Err(Error::invalid(
                "module-owner launch plan exceeds size limit",
            ));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&plan_path)?;
        swarm_process::private_permissions(&plan_path, false)?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);
        sync_directory(&self.state_dir)?;

        if !hash_file_sha256(&executable)?.eq_ignore_ascii_case(expected_hash.as_str()) {
            return Err(Error::new(
                "MODULE_ARTIFACT_MISMATCH",
                "selected adapter executable changed before its helper launch",
            ));
        }
        let mut command = Command::new(&self.owner_executable.path);
        command
            .arg(&plan_path)
            .arg(&resolver_map)
            .kill_on_drop(false)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        swarm_process::module_owner::apply_native_launch_environment(command.as_std_mut());
        let child = command.spawn().map_err(|error| {
            Error::new(
                "MODULE_OWNER_SPAWN_FAILED",
                format!("module-owner helper spawn failed: {error}"),
            )
        })?;
        Ok((child, plan_path))
    }

    fn materialize_module_host_config(&self, schema_version: u16) -> Result<PathBuf> {
        if schema_version != 1 {
            return Err(Error::new(
                "MODULE_HOST_CONFIG_INVALID",
                "module host config path marker uses an unsupported schema",
            ));
        }
        let path = self.state_dir.join("module-host-connection.json");
        if !path.is_absolute() || path.to_str().is_none_or(|value| value.len() > 4_096) {
            return Err(Error::new(
                "MODULE_HOST_CONFIG_INVALID",
                "module host config must resolve to an absolute bounded Unicode path",
            ));
        }
        let config = HostConnectionConfig {
            schema_version: u32::from(schema_version),
            host_data_dir: self.host_data_dir.clone(),
            ipc: self.ipc.clone(),
        };
        config.validate()?;
        let data = serde_json::to_vec(&config)?;
        if data.len() as u64 > MODULE_PLAN_LIMIT {
            return Err(Error::invalid(
                "module host connection config exceeds its size bound",
            ));
        }

        // This path is overwritten only when the prior exact owner has been
        // reconciled as gone. Exclusive creation after leaf validation avoids
        // following a module-planted link or launching against partial bytes.
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(Error::new(
                    "MODULE_HOST_CONFIG_PATH_UNSAFE",
                    "module host config path is not a regular file",
                ));
            }
            Ok(_) => fs::remove_file(&path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        swarm_process::private_permissions(&path, false)?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);
        sync_directory(&self.state_dir)?;
        Ok(path)
    }

    /// Validate every composed environment name before restart accounting or
    /// launch intent. BindingLaunchConfig validates raw keys, while this shared
    /// predicate checks the exact ELIOT_SWARM_CONFIG_<rawKey> name emitted to the
    /// native module.
    fn validate_launch_config_final_names(&self) -> Result<()> {
        for key in self.launch_config.values.keys() {
            Self::module_config_environment_name(key)?;
        }
        Ok(())
    }

    fn module_config_environment_name(key: &str) -> Result<String> {
        validate_environment_name(key)?;
        let name = format!("{MODULE_CONFIG_ENV_PREFIX}{key}");
        Self::prefixed_config_name_valid(&name)?;
        Ok(name)
    }

    /// The single source of truth for a valid final module-config environment
    /// name: exactly the ELIOT_SWARM_CONFIG_ prefix, at most 128 bytes, and ASCII
    /// uppercase letters, digits, or underscores. Preflight and emission both
    /// use this predicate so the raw-key and final-name contracts cannot diverge.
    fn prefixed_config_name_valid(name: &str) -> Result<()> {
        if !name.starts_with(MODULE_CONFIG_ENV_PREFIX)
            || name.len() > 128
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(Error::invalid("module config environment name is invalid"));
        }
        Ok(())
    }

    fn add_launch_value(
        &self,
        name: &str,
        value: &LaunchValue,
        environment: &mut BTreeMap<String, String>,
        protected_refs: &mut Vec<Value>,
        names: &mut BTreeSet<String>,
    ) -> Result<()> {
        if name.starts_with("ELIOT_SWARM_MODULE_") {
            return Err(Error::invalid(
                "descriptor launch values cannot override module owner identity",
            ));
        }
        if name.starts_with(MODULE_CONFIG_ENV_PREFIX) {
            Self::prefixed_config_name_valid(name)?;
        } else {
            validate_environment_name(name)?;
        }
        if !names.insert(name.to_owned()) {
            return Err(Error::new(
                "MODULE_LAUNCH_ENV_DUPLICATE",
                "module launch environment contains a duplicate name",
            ));
        }
        validate_launch_value(value)?;
        match value {
            LaunchValue::Literal(value) if value.len() <= 4_096 => {
                environment.insert(name.to_owned(), value.clone());
            }
            LaunchValue::Literal(_) => {
                return Err(Error::invalid(
                    "module launch environment value exceeds the bounded helper plan",
                ));
            }
            LaunchValue::Protected(reference) => {
                protected_refs.push(serde_json::json!({
                    "name": name,
                    "reference": reference.as_str(),
                }));
            }
            LaunchValue::ModuleHostConfigPath { .. } => {
                return Err(Error::invalid(
                    "module host config path markers are valid only in descriptor argv",
                ));
            }
        }
        Ok(())
    }

    fn observe_owned_worker(
        &self,
        boot_id: &str,
        helper_identity: &ProcessIdentity,
        worker_identity: Option<&ProcessIdentity>,
    ) -> Result<bool> {
        let worker_live = match worker_identity {
            Some(worker) => process_identity_is_live(worker)?,
            None => false,
        };
        let scope_quarantined = self.scope_quarantined.load(Ordering::Acquire);
        self.update_status(|status| {
            // Liveness advances only to Starting. Preserve Ready only when
            // Store already confirmed this exact boot and worker identity.
            let hello_confirmed = worker_live
                && worker_identity
                    .is_some_and(|worker| worker_is_hello_confirmed(status, boot_id, worker));
            if !scope_quarantined && !worker_live {
                status.lifecycle = LifecycleState::OwnerGroupRetained {
                    owner_pid: helper_identity.pid,
                    reason: if worker_identity.is_some() {
                        OwnerRetainedReason::WorkerExited
                    } else {
                        OwnerRetainedReason::WorkerIdentityUnknown
                    },
                };
            } else if !scope_quarantined && !hello_confirmed {
                status.lifecycle = LifecycleState::Starting {
                    boot_id: boot_id.to_owned(),
                };
            }
            status.owner = Some(helper_identity.clone());
            status.worker = worker_identity.cloned();
            status.worker_boot_id = Some(boot_id.to_owned());
        });
        Ok(worker_live)
    }

    async fn monitor_owner_helper(
        &self,
        helper: &mut Child,
        boot_id: &str,
        helper_identity: &ProcessIdentity,
        departed_prior_owner: Option<&VerifiedDepartedOwner>,
        receipt_deadline: Instant,
    ) -> Result<OwnerHelperExit> {
        let owner_path = self.state_dir.join("owner.json");
        let worker_path = self.state_dir.join("worker.json");
        let launch_result_path = self.state_dir.join("launch-result.json");
        let mut owner_record = None::<Value>;
        let mut owner_token = None::<String>;
        let mut worker_record = None::<Value>;
        let mut worker_identity = None::<ProcessIdentity>;
        let mut worker_validated = false;
        let mut launch_result = None::<Value>;
        let mut worker_started_at = None::<Instant>;
        let mut worker_last_live_at = None::<Instant>;

        loop {
            if owner_record.is_none()
                && let Some((owner, token)) = read_current_owner_receipt_optional(
                    &owner_path,
                    helper_identity,
                    departed_prior_owner,
                )?
            {
                owner_record = Some(owner);
                owner_token = Some(token);
            }
            if let (Some(owner), Some(token)) = (owner_record.as_ref(), owner_token.as_deref()) {
                self.remember_owner_family_query(&owner["process"], token);
            }
            if worker_record.is_none() {
                worker_record = read_json_receipt_optional(&worker_path, OWNER_RECORD_LIMIT)?;
            }
            if !worker_validated
                && let (Some(owner), Some(record)) = (owner_record.as_ref(), worker_record.as_ref())
            {
                worker_identity = self.validate_worker_receipt(record, boot_id, owner)?;
                worker_validated = true;
            }
            if launch_result.is_none() {
                launch_result =
                    read_json_receipt_optional(&launch_result_path, OWNER_RECORD_LIMIT)?;
                if launch_result.as_ref().is_some_and(|value| {
                    value.get("boot_id").and_then(Value::as_str) != Some(boot_id)
                }) {
                    // A prior boot's receipt is not evidence about this launch.
                    launch_result = None;
                }
            }
            if let (Some(owner), Some(receipt)) = (owner_record.as_ref(), launch_result.as_ref()) {
                self.validate_launch_result(receipt, boot_id, owner)?;
            }

            if let Some(exit) = helper.try_wait()? {
                // Close the receipt race after observing the exact helper exit.
                if owner_record.is_none()
                    && let Some((owner, token)) = read_current_owner_receipt_optional(
                        &owner_path,
                        helper_identity,
                        departed_prior_owner,
                    )?
                {
                    owner_record = Some(owner);
                    owner_token = Some(token);
                }
                if let (Some(owner), Some(token)) = (owner_record.as_ref(), owner_token.as_deref())
                {
                    self.remember_owner_family_query(&owner["process"], token);
                }
                if worker_record.is_none() {
                    worker_record = read_json_receipt_optional(&worker_path, OWNER_RECORD_LIMIT)?;
                }
                if !worker_validated
                    && let (Some(owner), Some(record)) =
                        (owner_record.as_ref(), worker_record.as_ref())
                {
                    worker_identity = self.validate_worker_receipt(record, boot_id, owner)?;
                }
                if launch_result.is_none() {
                    launch_result =
                        read_json_receipt_optional(&launch_result_path, OWNER_RECORD_LIMIT)?;
                    if launch_result.as_ref().is_some_and(|value| {
                        value.get("boot_id").and_then(Value::as_str) != Some(boot_id)
                    }) {
                        launch_result = None;
                    }
                }
                if let (Some(owner), Some(receipt)) =
                    (owner_record.as_ref(), launch_result.as_ref())
                {
                    self.validate_launch_result(receipt, boot_id, owner)?;
                }

                let Some(owner) = owner_record.as_ref() else {
                    // The reviewed helper publishes owner.json before resolving
                    // references or spawning an adapter. Its exact process has
                    // exited and no owner receipt exists, so no adapter started.
                    if worker_record.is_some() || launch_result.is_some() {
                        return Err(Error::new(
                            "MODULE_LAUNCH_RESULT_INVALID",
                            "worker or launch-result evidence exists without its required owner receipt",
                        ));
                    }
                    return Ok(OwnerHelperExit {
                        exit,
                        worker: None,
                        worker_diagnostic_code: None,
                        safe_to_replace: false,
                        family_departure_proven: false,
                        failure_code: Some("MODULE_LAUNCH_RESULT_MISSING".to_owned()),
                        failure_stage: None,
                        effect_certainty: ModuleEffectCertainty::Unknown,
                        healthy_duration: None,
                    });
                };
                let token = owner_token.as_deref().ok_or_else(|| {
                    Error::new("MODULE_OWNER_IDENTITY_INVALID", "owner token is missing")
                })?;

                if let Some(result) = launch_result.as_ref() {
                    if worker_record.is_some() || worker_identity.is_some() {
                        return Err(Error::new(
                            "MODULE_LAUNCH_RESULT_INVALID",
                            "certified pre-spawn result conflicts with worker process evidence",
                        ));
                    }
                    let failure_code = result
                        .get("error_code")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::invalid("launch-result omitted error_code"))?
                        .to_owned();
                    let failure_stage = result
                        .get("stage")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::invalid("launch-result omitted stage"))?
                        .to_owned();
                    self.release_owner_family_query();
                    return Ok(OwnerHelperExit {
                        exit,
                        worker: None,
                        worker_diagnostic_code: None,
                        safe_to_replace: true,
                        // The exact validated receipt proves the owner observed
                        // an empty family before any worker spawn.
                        family_departure_proven: false,
                        failure_code: Some(failure_code),
                        failure_stage: Some(failure_stage),
                        effect_certainty: ModuleEffectCertainty::NotStarted,
                        healthy_duration: None,
                    });
                }

                let final_observation_deadline =
                    Instant::now() + self.final_owner_observation_timeout();
                loop {
                    let family_observation_unknown =
                        match self.observe_owner_family(&owner["process"], token) {
                            ProcessFamilyObservation::ConfirmedEmpty => break,
                            ProcessFamilyObservation::Retained => false,
                            ProcessFamilyObservation::Unknown(_) => true,
                        };
                    if self.observe_owned_worker(
                        boot_id,
                        helper_identity,
                        worker_identity.as_ref(),
                    )? {
                        let now = Instant::now();
                        worker_started_at.get_or_insert(now);
                        worker_last_live_at = Some(now);
                    }
                    if Instant::now() >= final_observation_deadline {
                        let (code, message) = if family_observation_unknown {
                            (
                                "MODULE_FINAL_FAMILY_OBSERVATION_UNKNOWN",
                                "exact owner-family state remained unavailable after direct helper exit",
                            )
                        } else {
                            (
                                "MODULE_FINAL_FAMILY_OBSERVATION_TIMEOUT",
                                "exact owner family remained after direct helper exit",
                            )
                        };
                        return Err(Error::new(code, message));
                    }
                    time::sleep(OWNER_DRAIN_POLL).await;
                }

                let healthy_duration = worker_started_at
                    .zip(worker_last_live_at)
                    .map(|(started, last_live)| last_live.saturating_duration_since(started));
                // worker.json is emitted only after Command::spawn succeeds
                // and the exact child image is captured. Its boot, artifact,
                // membership, and whole-family departure proofs are enough
                // to replace this failed adapter without replaying work.
                let worker = worker_identity.ok_or_else(|| {
                    Error::new(
                        "MODULE_LAUNCH_RESULT_MISSING",
                        "helper exited without an exact worker receipt or certified pre-spawn result",
                    )
                })?;
                let worker_diagnostic_code = if exit.success() {
                    None
                } else {
                    self.worker_diagnostic_code(boot_id)
                };
                // Durable post-worker handoff: fsync exact departure proof BEFORE
                // releasing the retained query. No retroactive proof: this write
                // happens only after genuine ConfirmedEmpty above for this exact
                // boot/owner/helper. A restart that lost the query consumes the
                // fsynced receipt instead of reusing a missing Job as empty.
                self.write_owner_departure_receipt(boot_id, owner, token, helper_identity)?;
                self.release_owner_family_query();
                return Ok(OwnerHelperExit {
                    exit,
                    worker: Some(worker),
                    worker_diagnostic_code,
                    safe_to_replace: true,
                    // This is the only path with an exact worker receipt;
                    // zero-member Job accounting has already succeeded above.
                    family_departure_proven: true,
                    failure_code: Some("MODULE_EXITED".to_owned()),
                    failure_stage: None,
                    effect_certainty: ModuleEffectCertainty::Unknown,
                    healthy_duration,
                });
            }

            if let Some(worker) = worker_identity.as_ref()
                && self.observe_owned_worker(boot_id, helper_identity, Some(worker))?
            {
                let now = Instant::now();
                worker_started_at.get_or_insert(now);
                worker_last_live_at = Some(now);
            }
            let startup_receipt_observed = owner_record.is_some()
                && ((worker_record.is_some() && worker_validated) || launch_result.is_some());
            if !startup_receipt_observed && Instant::now() >= receipt_deadline {
                return Err(Error::new(
                    "MODULE_START_RECEIPT_TIMEOUT",
                    "exact helper startup owner and worker or pre-spawn receipts were not observed before the bounded deadline",
                ));
            }
            time::sleep(HELPER_START_POLL).await;
        }
    }

    fn validate_worker_receipt(
        &self,
        receipt: &Value,
        boot_id: &str,
        owner: &Value,
    ) -> Result<Option<ProcessIdentity>> {
        let protocol = format!("{}.{}", self.protocol.major, self.protocol.minor);
        let generation = self.scope.generation.to_string();
        if receipt.get("version").and_then(Value::as_u64) != Some(1)
            || receipt.get("boot_id").and_then(Value::as_str) != Some(boot_id)
            || receipt.get("module").and_then(Value::as_str)
                != Some(self.descriptor.module_id.as_str())
            || receipt.get("binding").and_then(Value::as_str)
                != Some(self.scope.binding_id.as_str())
            || receipt.get("generation").and_then(Value::as_str) != Some(generation.as_str())
            || receipt.get("artifact_id").and_then(Value::as_str)
                != Some(self.descriptor.artifact.artifact_id.as_str())
            || receipt.get("artifact_version").and_then(Value::as_str)
                != Some(self.descriptor.artifact.version.as_str())
            || receipt.get("build_id").and_then(Value::as_str)
                != self.descriptor.artifact.build_id.as_deref()
            || receipt.get("protocol").and_then(Value::as_str) != Some(protocol.as_str())
            || receipt.get("module_contract").and_then(Value::as_str)
                != Some(self.module_contract_json.as_str())
        {
            return Err(Error::new(
                "MODULE_WORKER_IDENTITY_MISMATCH",
                "worker receipt does not match selected descriptor, scope, generation, protocol, and boot",
            ));
        }
        let worker_image = receipt
            .get("process")
            .filter(|value| value.is_object())
            .ok_or_else(|| Error::invalid("worker receipt process image is missing"))?;
        let expected_hash = self
            .descriptor
            .launch
            .executable_sha256
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "MODULE_ARTIFACT_DIGEST_REQUIRED",
                    "selected adapter executable has no SHA-256 pin",
                )
            })?
            .as_str();
        if !image_matches_digest(worker_image, expected_hash)
            || !image_path_matches(
                worker_image,
                &fs::canonicalize(&self.descriptor.launch.executable)?,
            )
        {
            return Err(Error::new(
                "MODULE_ARTIFACT_MISMATCH",
                "adapter process image differs from the exact selected artifact",
            ));
        }
        let worker = identity_from_process_image(worker_image)?;
        match process_birth_identity(worker.pid)? {
            Some(birth) if birth == worker.birth => {
                let current = match capture_identity(worker.pid) {
                    Ok(current) => current,
                    // A live birth read can precede exit during image capture.
                    // Keep the trusted spawn receipt; family departure still
                    // requires the independent owner-group observation.
                    Err(error) if error.code == "PROCESS_GONE" => return Ok(Some(worker)),
                    Err(error) => return Err(error),
                };
                if current.image != *worker_image {
                    return Err(Error::new(
                        "MODULE_WORKER_IDENTITY_MISMATCH",
                        "worker image receipt differs from the current process incarnation",
                    ));
                }
                let membership = {
                    #[cfg(test)]
                    {
                        let probe = *lock(&self.module_membership_probe);
                        probe.map_or_else(
                            || module_child_belongs_to_owner(owner, worker_image),
                            |probe| probe(owner, worker_image),
                        )
                    }
                    #[cfg(not(test))]
                    {
                        module_child_belongs_to_owner(owner, worker_image)
                    }
                };
                if !membership? {
                    return Err(Error::new(
                        "MODULE_WORKER_NOT_OWNED",
                        "adapter process is not a distinct member of the exact owner group",
                    ));
                }
                Ok(Some(current))
            }
            Some(_) | None => {
                // The trusted helper receipt captured the exact child image at
                // spawn. The child may already have exited; its helper still
                // owns the process family until empty-family proof succeeds.
                Ok(Some(worker))
            }
        }
    }

    fn worker_diagnostic_code(&self, boot_id: &str) -> Option<String> {
        let path = self
            .state_dir
            .join(swarm_process::module_owner::WORKER_DIAGNOSTIC_FILE);
        let receipt = read_canonical_json_receipt_optional(
            &path,
            swarm_process::module_owner::WORKER_DIAGNOSTIC_MAX_BYTES,
        )?;
        validate_worker_diagnostic_receipt(
            &receipt,
            WorkerDiagnosticScope {
                boot_id,
                module_id: self.descriptor.module_id.as_str(),
                binding_id: self.scope.binding_id.as_str(),
                generation: self.scope.generation,
                artifact_id: self.descriptor.artifact.artifact_id.as_str(),
                artifact_version: self.descriptor.artifact.version.as_str(),
                build_id: self.descriptor.artifact.build_id.as_deref(),
            },
        )
    }

    fn validate_launch_result(&self, receipt: &Value, boot_id: &str, owner: &Value) -> Result<()> {
        let object = receipt
            .as_object()
            .ok_or_else(|| Error::invalid("launch-result must be an object"))?;
        if object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "version"
                    | "boot_id"
                    | "module"
                    | "module_client_id"
                    | "binding"
                    | "generation"
                    | "artifact_id"
                    | "artifact_version"
                    | "build_id"
                    | "protocol"
                    | "owner"
                    | "disposition"
                    | "worker_started"
                    | "family_empty"
                    | "stage"
                    | "error_code"
            )
        }) {
            return Err(Error::new(
                "MODULE_LAUNCH_RESULT_INVALID",
                "launch-result contains unsupported fields",
            ));
        }
        let generation = self.scope.generation.to_string();
        let protocol = format!("{}.{}", self.protocol.major, self.protocol.minor);
        let build_id_matches = match (
            self.descriptor.artifact.build_id.as_deref(),
            receipt.get("build_id"),
        ) {
            (None, Some(Value::Null)) => true,
            (Some(expected), Some(Value::String(actual))) => expected == actual,
            _ => false,
        };
        let stage = receipt.get("stage").and_then(Value::as_str);
        let error_code = receipt.get("error_code").and_then(Value::as_str);
        let valid_error_code = error_code.is_some_and(|code| {
            !code.is_empty()
                && code.len() <= 128
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        });
        if object.len() != 16
            || receipt.get("version").and_then(Value::as_u64) != Some(1)
            || receipt.get("boot_id").and_then(Value::as_str) != Some(boot_id)
            || receipt.get("module").and_then(Value::as_str)
                != Some(self.descriptor.module_id.as_str())
            || receipt.get("module_client_id").and_then(Value::as_str)
                != Some(self.module_client_id.as_str())
            || receipt.get("binding").and_then(Value::as_str)
                != Some(self.scope.binding_id.as_str())
            || receipt.get("generation").and_then(Value::as_str) != Some(generation.as_str())
            || receipt.get("artifact_id").and_then(Value::as_str)
                != Some(self.descriptor.artifact.artifact_id.as_str())
            || receipt.get("artifact_version").and_then(Value::as_str)
                != Some(self.descriptor.artifact.version.as_str())
            || !build_id_matches
            || receipt.get("protocol").and_then(Value::as_str) != Some(protocol.as_str())
            || receipt.get("owner") != Some(owner)
            || receipt.get("disposition").and_then(Value::as_str) != Some("not_started")
            || receipt.get("worker_started").and_then(Value::as_bool) != Some(false)
            || receipt.get("family_empty").and_then(Value::as_bool) != Some(true)
            // "spawn" is the proven Command::spawn failure stage: the helper
            // published the exact pre-spawn not_started receipt after proving an
            // empty family, so recovery can consume it like the resolver and
            // launch-validation stages instead of reporting launch evidence
            // missing for a worker that was never started.
            || !matches!(stage, Some("resolve_refs" | "validate_launch" | "spawn"))
            || !valid_error_code
        {
            return Err(Error::new(
                "MODULE_LAUNCH_RESULT_INVALID",
                "launch-result does not match the exact pre-spawn attempt, owner, and scope",
            ));
        }
        Ok(())
    }

    async fn wait_for_prior_owner_to_depart(&self) -> Result<Option<VerifiedDepartedOwner>> {
        self.wait_for_prior_owner_to_depart_with_custody(false)
            .await
    }

    async fn wait_for_prior_owner_to_depart_with_custody(
        &self,
        keep_owner_query: bool,
    ) -> Result<Option<VerifiedDepartedOwner>> {
        let observation_deadline = Instant::now() + self.prior_owner_observation_timeout();
        let attempt_path = self.state_dir.join("launch-attempt.json");
        let attempt = read_json_receipt_optional(&attempt_path, LAUNCH_RECORD_LIMIT)?;
        let mut attempt_boot_id = None::<String>;
        let attempted_helper = if let Some(attempt) = attempt.as_ref() {
            let object = attempt.as_object().ok_or_else(|| {
                Error::new(
                    "MODULE_LAUNCH_INTENT_INVALID",
                    "launch intent must be an object",
                )
            })?;
            if object.len() != 8
                || object.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "version"
                            | "module_id"
                            | "artifact_id"
                            | "artifact_version"
                            | "descriptor_fingerprint"
                            | "scope"
                            | "boot_id"
                            | "owner_helper"
                    )
                })
                || attempt.get("version").and_then(Value::as_u64) != Some(1)
                || attempt.get("module_id").and_then(Value::as_str)
                    != Some(self.descriptor.module_id.as_str())
                || attempt.get("artifact_id").and_then(Value::as_str)
                    != Some(self.descriptor.artifact.artifact_id.as_str())
                || attempt.get("artifact_version").and_then(Value::as_str)
                    != Some(self.descriptor.artifact.version.as_str())
                || attempt
                    .get("descriptor_fingerprint")
                    .and_then(Value::as_str)
                    != Some(self.descriptor_fingerprint.as_str())
                || attempt
                    .get("scope")
                    .and_then(|scope| scope.get("binding_id"))
                    .and_then(Value::as_str)
                    != Some(self.scope.binding_id.as_str())
                || attempt
                    .get("scope")
                    .and_then(|scope| scope.get("generation"))
                    .and_then(Value::as_u64)
                    != Some(self.scope.generation)
                || attempt
                    .get("boot_id")
                    .and_then(Value::as_str)
                    .is_none_or(|boot| Uuid::parse_str(boot).is_err())
            {
                return Err(Error::new(
                    "MODULE_LAUNCH_INTENT_INVALID",
                    "persisted module launch intent is malformed or belongs to another scope",
                ));
            }
            attempt_boot_id = attempt
                .get("boot_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            match attempt.get("owner_helper") {
                Some(Value::Null) | None => {
                    return Err(Error::new(
                        "MODULE_LAUNCH_INTENT_UNKNOWN",
                        "launch intent has no exact helper process identity; prior start is unresolved",
                    ));
                }
                Some(value) => Some(
                    serde_json::from_value::<ProcessIdentity>(value.clone()).map_err(|_| {
                        Error::new(
                            "MODULE_LAUNCH_INTENT_INVALID",
                            "persisted helper process identity is malformed",
                        )
                    })?,
                ),
            }
        } else {
            None
        };
        if attempted_helper
            .as_ref()
            .is_some_and(|helper| !self.image_matches_helper(helper))
        {
            return Err(Error::new(
                "MODULE_OWNER_HELPER_MISMATCH",
                "persisted launch intent does not identify the exact pinned helper image",
            ));
        }

        let owner_path = self.state_dir.join("owner.json");
        let mut prior_owner = read_owner_receipt_optional(&owner_path)?;
        if let Some((owner, token)) = prior_owner.as_ref() {
            self.remember_owner_family_query(&owner["process"], token);
        }
        if attempted_helper.is_none() && prior_owner.is_none() {
            let mut marker_present = false;
            for entry in fs::read_dir(&self.state_dir)? {
                let name = entry?.file_name();
                if name == "module.lock" {
                    marker_present = true;
                } else if name == "restart-history.json" {
                    // Accounting is written before any launch intent. It is not
                    // process custody; accept only its exact validated shape/scope.
                    self.read_restart_history()?;
                } else {
                    return Err(Error::new(
                        "MODULE_OWNER_IDENTITY_MISSING",
                        "module state exists without its prior owner receipt; no process was started",
                    ));
                }
            }
            if marker_present {
                // A busy, malformed or linked marker is not evidence of an
                // ownerless scope, even when no owner envelope is available.
                let _marker =
                    swarm_process::module_owner::acquire_module_state_marker(&self.state_dir)?;
            }
            return Ok(None);
        }
        if let (Some(helper), Some((owner, _))) = (attempted_helper.as_ref(), prior_owner.as_ref())
            && !owner_matches_worker(&owner["process"], helper)
        {
            return Err(Error::new(
                "MODULE_OWNER_IDENTITY_MISMATCH",
                "persisted owner.json does not match the exact helper launch intent",
            ));
        }

        let mut prior_start_proved = attempt.is_none();
        let mut final_owner_receipt_proves_empty = false;
        loop {
            if Instant::now() >= observation_deadline {
                return Err(Error::new(
                    "MODULE_PRIOR_OWNER_WAIT_TIMEOUT",
                    "prior exact helper or owner family did not prove departure before the bounded wait expired",
                ));
            }
            if let Some(helper) = attempted_helper.as_ref()
                && process_identity_is_live(helper)?
            {
                let attached_worker = match prior_owner.as_ref() {
                    Some((owner, _)) => match read_json_receipt_optional(
                        &self.state_dir.join("worker.json"),
                        OWNER_RECORD_LIMIT,
                    )? {
                        Some(receipt) => self.validate_worker_receipt(
                            &receipt,
                            attempt_boot_id.as_deref().ok_or_else(|| {
                                Error::new(
                                    "MODULE_LAUNCH_INTENT_INVALID",
                                    "live helper launch intent omitted its boot ID",
                                )
                            })?,
                            owner,
                        )?,
                        None => None,
                    },
                    None => None,
                };
                let boot_id = attempt_boot_id.clone();
                let worker_live = match attached_worker.as_ref() {
                    Some(worker) => process_identity_is_live(worker)?,
                    None => false,
                };
                self.update_status(|status| {
                    let hello_confirmed = worker_live
                        && boot_id
                            .as_deref()
                            .zip(attached_worker.as_ref())
                            .is_some_and(|(boot, worker)| {
                                worker_is_hello_confirmed(status, boot, worker)
                            });
                    if !hello_confirmed {
                        status.lifecycle = LifecycleState::OwnerGroupRetained {
                            owner_pid: helper.pid,
                            reason: if worker_live {
                                OwnerRetainedReason::HelloUnconfirmed
                            } else if attached_worker.is_some() {
                                OwnerRetainedReason::WorkerExited
                            } else {
                                OwnerRetainedReason::WorkerIdentityUnknown
                            },
                        };
                    }
                    status.owner = Some(helper.clone());
                    status.worker = attached_worker.clone();
                    status.worker_boot_id = boot_id.clone();
                });
                time::sleep(
                    OWNER_DRAIN_POLL
                        .min(observation_deadline.saturating_duration_since(Instant::now())),
                )
                .await;
                continue;
            }
            if prior_owner.is_none() {
                prior_owner = read_owner_receipt_optional(&owner_path)?;
                if let (Some(helper), Some((owner, _))) =
                    (attempted_helper.as_ref(), prior_owner.as_ref())
                    && !owner_matches_worker(&owner["process"], helper)
                {
                    return Err(Error::new(
                        "MODULE_OWNER_IDENTITY_MISMATCH",
                        "persisted owner.json does not match the exact helper launch intent",
                    ));
                }
                if let Some((owner, token)) = prior_owner.as_ref() {
                    self.remember_owner_family_query(&owner["process"], token);
                }
            }
            let Some((owner, token)) = prior_owner.as_ref() else {
                // A helper that started but never published its exact owner or
                // certified pre-spawn receipt is an unresolved start. Process
                // exit alone is not a negative launch receipt.
                return Err(Error::new(
                    "MODULE_LAUNCH_RESULT_MISSING",
                    "prior helper exited without exact owner and launch evidence",
                ));
            };
            if !prior_start_proved {
                let boot_id = attempt_boot_id.as_deref().ok_or_else(|| {
                    Error::new(
                        "MODULE_LAUNCH_INTENT_INVALID",
                        "persisted launch intent omitted its exact boot ID",
                    )
                })?;
                let worker = read_json_receipt_optional(
                    &self.state_dir.join("worker.json"),
                    OWNER_RECORD_LIMIT,
                )?;
                let launch_result = read_json_receipt_optional(
                    &self.state_dir.join("launch-result.json"),
                    OWNER_RECORD_LIMIT,
                )?
                .filter(|receipt| receipt.get("boot_id").and_then(Value::as_str) == Some(boot_id));
                if worker.is_some() && launch_result.is_some() {
                    return Err(Error::new(
                        "MODULE_LAUNCH_RESULT_INVALID",
                        "one boot has both worker-started and not-started receipts",
                    ));
                }
                if let Some(receipt) = worker.as_ref() {
                    let _ = self.validate_worker_receipt(receipt, boot_id, owner)?;
                } else if let Some(receipt) = launch_result.as_ref() {
                    self.validate_launch_result(receipt, boot_id, owner)?;
                    final_owner_receipt_proves_empty = true;
                } else {
                    return Err(Error::new(
                        "MODULE_LAUNCH_RESULT_MISSING",
                        "prior helper has no exact worker or certified pre-spawn receipt",
                    ));
                }
                prior_start_proved = true;
            }
            if final_owner_receipt_proves_empty {
                if !keep_owner_query {
                    self.release_owner_family_query();
                }
                return Ok(Some(VerifiedDepartedOwner {
                    receipt: owner.clone(),
                    token: token.clone(),
                }));
            }
            let retained_reason = match self.observe_owner_family(&owner["process"], token) {
                ProcessFamilyObservation::ConfirmedEmpty => {
                    // Durable post-worker handoff: fsync exact proof BEFORE releasing query.
                    // Pre-spawn certified path already returned above via its durable
                    // launch-result; reaching here means a validated worker receipt was
                    // required (prior_start_proved) when an attempt is present. Never
                    // manufacture retroactive proof for old launches.
                    if let Some(boot) = attempt_boot_id.as_deref() {
                        let helper = attempted_helper.as_ref().ok_or_else(|| {
                            Error::new(
                                "MODULE_LAUNCH_RESULT_MISSING",
                                "prior helper identity is missing for departure proof",
                            )
                        })?;
                        self.write_owner_departure_receipt(boot, owner, token, helper)?;
                    } else {
                        // Attempt-less owner: preserve the legacy contract. A genuine
                        // fresh ConfirmedEmpty from the OS is itself authority; no
                        // prior durable receipt is required. A stale mismatched
                        // receipt, if present, is ignored only here under this fresh
                        // real empty proof; it never authorizes on its own (see the
                        // Unknown arm, where MissingJob without genuine proof stays
                        // UNKNOWN and stale mismatch stays UNKNOWN).
                    }
                    if !keep_owner_query {
                        self.release_owner_family_query();
                    }
                    return Ok(Some(VerifiedDepartedOwner {
                        receipt: owner.clone(),
                        token: token.clone(),
                    }));
                }
                ProcessFamilyObservation::Retained => OwnerRetainedReason::OwnerFamilyNotEmpty,
                ProcessFamilyObservation::Unknown(_) => {
                    // Restart handoff: fresh missing-Job stays UNKNOWN without genuine
                    // proof, but an exact durable receipt from the previous retained-query
                    // proof bridges it. Retained never consumes; live helper never
                    // reaches here; timeout still fences. Visible bytes after a failed
                    // write or read barrier (PRIVATE_FILE_DURABILITY_UNKNOWN) stay
                    // UNKNOWN here and never authorize; a later successful durable
                    // read may still promote the genuine proof. Corrupt shape blocks;
                    // stale boot/owner mismatch keeps UNKNOWN so genuine fresh
                    // ConfirmedEmpty can fsync its own exact receipt. Neither path
                    // starts replacement on wrong proof.
                    match self.read_owner_departure_receipt_for(
                        attempt_boot_id.as_deref(),
                        owner,
                        token,
                        attempted_helper.as_ref(),
                    ) {
                        Ok(Some(_)) => {
                            if !keep_owner_query {
                                self.release_owner_family_query();
                            }
                            return Ok(Some(VerifiedDepartedOwner {
                                receipt: owner.clone(),
                                token: token.clone(),
                            }));
                        }
                        Ok(None) => OwnerRetainedReason::OwnerFamilyObservationUnknown,
                        Err(error)
                            if error.code == "MODULE_OWNER_DEPARTURE_INVALID"
                                || error.code == "MODULE_OWNER_DEPARTURE_MISSING" =>
                        {
                            return Err(error);
                        }
                        Err(_) => OwnerRetainedReason::OwnerFamilyObservationUnknown,
                    }
                }
            };
            let owner_pid = prior_owner
                .as_ref()
                .and_then(|(owner, _)| owner["process"]["pid"].as_u64())
                .and_then(|value| u32::try_from(value).ok())
                .or_else(|| attempted_helper.as_ref().map(|helper| helper.pid))
                .ok_or_else(|| Error::invalid("module owner process PID is invalid"))?;
            let owner_identity =
                serde_json::from_value::<ProcessIdentity>(owner["process"].clone()).ok();
            self.update_status(|status| {
                status.lifecycle = LifecycleState::OwnerGroupRetained {
                    owner_pid,
                    reason: retained_reason,
                };
                if let Some(owner_identity) = owner_identity {
                    status.owner = Some(owner_identity);
                }
                if let Some(boot_id) = attempt_boot_id.as_ref() {
                    status.worker_boot_id = Some(boot_id.clone());
                }
                status.readback_required = true;
            });
            self.readback_required.store(true, Ordering::Release);
            // Never adopt or signal a surviving helper/group. It must prove
            // family departure before this scope can receive a new helper.
            time::sleep(
                OWNER_DRAIN_POLL
                    .min(observation_deadline.saturating_duration_since(Instant::now())),
            )
            .await;
        }
    }

    fn clear_prior_helper_result(&self) -> Result<()> {
        for name in ["launch-result.json", "worker.json"] {
            let path = self.state_dir.join(name);
            match fs::symlink_metadata(&path) {
                Ok(metadata) => {
                    if metadata.file_type().is_symlink() || !metadata.is_file() {
                        return Err(Error::new(
                            "MODULE_OWNER_STATE_UNSAFE",
                            "prior helper receipt is not a regular file",
                        ));
                    }
                    fs::remove_file(path)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        self.remove_launch_attempt()?;
        sync_directory(&self.state_dir)
    }

    fn remove_plan(&self, boot_id: &str) -> Result<()> {
        let path = self.state_dir.join(format!("launch-{boot_id}.json"));
        match fs::symlink_metadata(&path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(Error::new(
                        "MODULE_OWNER_STATE_UNSAFE",
                        "module launch plan is not a regular file",
                    ));
                }
                fs::remove_file(path)?;
                sync_directory(&self.state_dir)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn image_matches_helper(&self, identity: &ProcessIdentity) -> bool {
        image_matches_digest(&identity.image, self.owner_executable.sha256.as_str())
            && image_path_matches(&identity.image, &self.owner_executable.path)
    }
    fn write_launch_attempt(
        &self,
        boot_id: &str,
        owner_helper: Option<&ProcessIdentity>,
    ) -> Result<()> {
        let receipt = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "module_id": self.descriptor.module_id.as_str(),
            "artifact_id": self.descriptor.artifact.artifact_id.as_str(),
            "artifact_version": self.descriptor.artifact.version.as_str(),
            "descriptor_fingerprint": self.descriptor_fingerprint.as_str(),
            "scope": &self.scope,
            "boot_id": boot_id,
            "owner_helper": owner_helper,
        }))?;
        if receipt.len() as u64 > LAUNCH_RECORD_LIMIT {
            return Err(Error::invalid("module launch receipt exceeds size limit"));
        }
        let path = self.state_dir.join("launch-attempt.json");
        let temp = self
            .state_dir
            .join(format!(".launch-{}.tmp", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            swarm_process::private_permissions(&temp, false)?;
            file.write_all(&receipt)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &path)?;
            sync_directory(&self.state_dir)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    fn remove_launch_attempt(&self) -> Result<()> {
        let path = self.state_dir.join("launch-attempt.json");
        match fs::remove_file(path) {
            Ok(()) => sync_directory(&self.state_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn departure_receipt_path(&self) -> PathBuf {
        self.state_dir.join(OWNER_DEPARTURE_FILE)
    }

    fn write_owner_departure_receipt(
        &self,
        boot_id: &str,
        owner: &Value,
        token: &str,
        helper: &ProcessIdentity,
    ) -> Result<()> {
        Uuid::parse_str(boot_id).map_err(|_| Error::invalid("departure boot is not a UUID"))?;
        Uuid::parse_str(token).map_err(|_| Error::invalid("departure token is invalid"))?;
        if owner.get("version").and_then(Value::as_u64) != Some(1)
            || owner.get("token").and_then(Value::as_str) != Some(token)
            || owner
                .get("process")
                .and_then(|process| process.get("purpose"))
                .and_then(Value::as_str)
                != Some("module")
        {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_INVALID",
                "prior owner receipt is not a valid v1 module envelope",
            ));
        }
        if helper.pid == 0 {
            return Err(Error::invalid("departure helper PID is invalid"));
        }
        if owner["process"]["pid"].as_u64() != Some(u64::from(helper.pid)) {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_MISMATCH",
                "departure helper does not match the prior owner process",
            ));
        }
        #[cfg(windows)]
        let observer = if lock(&self.owner_family_query).is_some() {
            OWNER_DEPARTURE_OBSERVER_RETAINED
        } else {
            OWNER_DEPARTURE_OBSERVER_FRESH
        };
        #[cfg(not(windows))]
        let observer = OWNER_DEPARTURE_OBSERVER_FRESH;
        let receipt = OwnerDepartureReceipt {
            version: 1,
            module_id: self.descriptor.module_id.to_string(),
            artifact_id: self.descriptor.artifact.artifact_id.to_string(),
            artifact_version: self.descriptor.artifact.version.to_string(),
            build_id: self.descriptor.artifact.build_id.clone(),
            descriptor_fingerprint: self.descriptor_fingerprint.clone(),
            scope: self.scope.clone(),
            boot_id: boot_id.to_owned(),
            owner: owner.clone(),
            owner_helper: helper.clone(),
            active_processes: 0,
            family_empty: true,
            observer: observer.to_owned(),
        };
        let data = serde_json::to_vec(&receipt)?;
        if data.len() as u64 > OWNER_RECORD_LIMIT {
            return Err(Error::invalid("departure receipt exceeds size limit"));
        }
        // Durable publication via the shared private-file authority: synced
        // temp plus write-through replace (Windows) and parent sync (Unix).
        // On barrier failure the target may already be visible; that visible
        // but durability-UNKNOWN data is never authority (see read barrier).
        // Only the temp is cleaned by the helper; the final path is never
        // deleted here because it may still be durable.
        replace_private_durable(&self.departure_receipt_path(), &data)
    }

    fn read_owner_departure_receipt_for(
        &self,
        boot_id: Option<&str>,
        owner: &Value,
        token: &str,
        helper: Option<&ProcessIdentity>,
    ) -> Result<Option<OwnerDepartureReceipt>> {
        let path = self.departure_receipt_path();
        // Fast missing check preserves Ok(None) for absent proof without
        // touching the durability barrier. All present data goes through the
        // shared read barrier below: file sync plus parent barrier plus
        // opened-identity recheck. Visible bytes after a failed write or read
        // barrier surface as PRIVATE_FILE_DURABILITY_UNKNOWN and must stay
        // UNKNOWN upstream, never Verified.
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(Error::new(
                    "MODULE_OWNER_DEPARTURE_INVALID",
                    "departure receipt is not a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) => return Err(error.into()),
        }
        let bytes = read_private_durable(&path, OWNER_RECORD_LIMIT).map_err(|error| {
            if error.code == "PRIVATE_FILE_INVALID" {
                Error::new(
                    "MODULE_OWNER_DEPARTURE_INVALID",
                    "departure receipt is not a regular file",
                )
            } else {
                error
            }
        })?;
        let receipt: OwnerDepartureReceipt = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "MODULE_OWNER_DEPARTURE_INVALID",
                "departure receipt has an invalid shape",
            )
        })?;
        if receipt.version != 1
            || receipt.module_id != self.descriptor.module_id.as_str()
            || receipt.artifact_id != self.descriptor.artifact.artifact_id.as_str()
            || receipt.artifact_version != self.descriptor.artifact.version.as_str()
            || receipt.build_id != self.descriptor.artifact.build_id
            || receipt.descriptor_fingerprint != self.descriptor_fingerprint
            || receipt.scope != self.scope
            || receipt.active_processes != 0
            || !receipt.family_empty
            // `observer` is diagnostic only (which query state was present at
            // write time), not a security gate; both labels are accepted.
            || (receipt.observer != OWNER_DEPARTURE_OBSERVER_RETAINED
                && receipt.observer != OWNER_DEPARTURE_OBSERVER_FRESH)
        {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_MISMATCH",
                "departure receipt does not match this exact descriptor, scope, and empty-family proof",
            ));
        }
        Uuid::parse_str(&receipt.boot_id).map_err(|_| {
            Error::new(
                "MODULE_OWNER_DEPARTURE_INVALID",
                "departure receipt boot is not a UUID",
            )
        })?;
        if let Some(expected) = boot_id {
            Uuid::parse_str(expected).map_err(|_| {
                Error::new(
                    "MODULE_OWNER_DEPARTURE_INVALID",
                    "launch boot is not a UUID",
                )
            })?;
            if receipt.boot_id != expected {
                return Err(Error::new(
                    "MODULE_OWNER_DEPARTURE_MISMATCH",
                    "departure receipt belongs to another boot; no reuse across boots",
                ));
            }
        }
        if receipt.owner != *owner {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_MISMATCH",
                "departure receipt does not match the prior owner receipt",
            ));
        }
        if receipt.owner.get("version").and_then(Value::as_u64) != Some(1)
            || receipt.owner.get("token").and_then(Value::as_str) != Some(token)
            || owner.get("token").and_then(Value::as_str) != Some(token)
            || receipt
                .owner
                .get("process")
                .and_then(|process| process.get("purpose"))
                .and_then(Value::as_str)
                != Some("module")
        {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_INVALID",
                "departure owner envelope is invalid",
            ));
        }
        if let Some(expected_helper) = helper
            && !same_process_identity(&receipt.owner_helper, expected_helper)
        {
            return Err(Error::new(
                "MODULE_OWNER_DEPARTURE_MISMATCH",
                "departure helper image does not match the prior helper",
            ));
        }
        Ok(Some(receipt))
    }

    fn read_restart_history(&self) -> Result<VecDeque<u64>> {
        let path = self.state_dir.join("restart-history.json");
        let Some(value) = read_json_receipt_optional(&path, RESTART_HISTORY_LIMIT)? else {
            return Ok(VecDeque::new());
        };
        let history: RestartHistory = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "MODULE_RESTART_HISTORY_INVALID",
                "persisted restart history has an invalid shape",
            )
        })?;
        if history.version != 1
            || history.module_id != self.descriptor.module_id.as_str()
            || history.scope != self.scope
            || history.starts_unix_ms.len() > 64
            || history
                .starts_unix_ms
                .windows(2)
                .any(|pair| pair[0] > pair[1])
        {
            return Err(Error::new(
                "MODULE_RESTART_HISTORY_INVALID",
                "persisted restart history is malformed or belongs to another scope",
            ));
        }
        if history.artifact_id != self.descriptor.artifact.artifact_id.as_str()
            || history.artifact_version != self.descriptor.artifact.version.as_str()
            || history.descriptor_fingerprint != self.descriptor_fingerprint
        {
            // A valid changed descriptor is an explicit budget reset. The
            // prior exact owner still has to depart before the new process starts.
            return Ok(VecDeque::new());
        }
        Ok(history.starts_unix_ms.into())
    }

    fn write_restart_history(&self, starts: &VecDeque<u64>) -> Result<()> {
        let history = RestartHistory {
            version: 1,
            module_id: self.descriptor.module_id.to_string(),
            artifact_id: self.descriptor.artifact.artifact_id.to_string(),
            artifact_version: self.descriptor.artifact.version.to_string(),
            descriptor_fingerprint: self.descriptor_fingerprint.clone(),
            scope: self.scope.clone(),
            starts_unix_ms: starts.iter().copied().collect(),
        };
        let data = serde_json::to_vec(&history)?;
        if data.len() as u64 > RESTART_HISTORY_LIMIT {
            return Err(Error::invalid(
                "persisted restart history exceeds its bound",
            ));
        }
        let path = self.state_dir.join("restart-history.json");
        let temp = self
            .state_dir
            .join(format!(".restart-{}.tmp", Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            swarm_process::private_permissions(&temp, false)?;
            file.write_all(&data)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp, &path)?;
            sync_directory(&self.state_dir)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    fn clear_restart_history(&self) -> Result<()> {
        let path = self.state_dir.join("restart-history.json");
        match fs::remove_file(path) {
            Ok(()) => sync_directory(&self.state_dir),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn prepare_state_dir(&self) -> Result<()> {
        prepare_private_state_dir(&self.state_root, &self.state_dir)?;
        swarm_process::private_permissions(&self.state_dir, true)
    }

    fn allocate_worker_boot(&self) -> String {
        let boot_id = Uuid::new_v4().to_string();
        self.update_status(|status| {
            status.lifecycle = LifecycleState::Starting {
                boot_id: boot_id.clone(),
            };
            status.owner = None;
            status.worker = None;
            status.worker_boot_id = Some(boot_id.clone());
            // This diagnostic belongs to the prior attempt, whose retained
            // Store observations and owner receipts remain unchanged.
            status.hello_failure = None;
            status.worker_diagnostic_code = None;
            status.effect_certainty = ModuleEffectCertainty::Unknown;
            status.failure_stage = None;
            status.last_failure = None;
        });
        boot_id
    }

    fn record_failure(&self, code: &str, detail: &str) {
        self.update_status(|status| {
            status.last_failure = Some(FailureSummary {
                code: code.to_owned(),
                detail: detail.to_owned(),
            });
            // Keep the manager-facing callback useful even when a failure
            // happened before the helper could publish a typed launch result.
            // Only the closed code vocabulary is mapped; detail remains
            // private and is never copied into the observation DTO.
            if let Some(stage) = ModuleFailureStage::from_failure_code(code) {
                status.failure_stage = Some(stage);
            }
        });
    }

    fn has_demand(&self) -> bool {
        lock(&self.demands).values().any(|count| *count > 0)
    }

    fn signal(&self) {
        self.bump(&self.demand_epoch);
    }

    fn bump(&self, sender: &watch::Sender<u64>) {
        sender.send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    fn update_status(&self, update: impl FnOnce(&mut SupervisorStatus)) {
        // Mutate the current value under the watch sender's lock so concurrent
        // lifecycle and hello updates cannot overwrite one another.
        self.status.send_if_modified(|status| {
            update(status);
            true
        });
    }

    fn current_status(&self) -> SupervisorStatus {
        let mut value = self.status.borrow().clone();
        value.admission = self.admission.borrow().clone();
        value
    }

    fn observation_statuses(&self) -> Vec<SupervisorStatus> {
        // Use the same lock order as confirmation: watch status, then Ready.
        let current = self.status.borrow();
        let ready = lock(&self.confirmed_ready);
        let mut values = Vec::with_capacity(2);
        if let Some(ready) = ready.as_ref().filter(|ready| !ready.queued) {
            values.push(ready.status.clone());
        }
        let mut current = current.clone();
        current.admission = self.admission.borrow().clone();
        values.push(current);
        values
    }

    fn acknowledge_ready_observation(&self, status: &SupervisorStatus) {
        if !matches!(status.lifecycle, LifecycleState::ProcessRunning { .. }) {
            return;
        }
        let mut ready = lock(&self.confirmed_ready);
        if let Some(ready) = ready.as_mut()
            && ready.status.worker_boot_id == status.worker_boot_id
            && ready
                .status
                .worker
                .as_ref()
                .zip(status.worker.as_ref())
                .is_some_and(|(expected, actual)| same_process_identity(expected, actual))
        {
            ready.queued = true;
        }
    }
}

fn descriptor_fingerprint(
    descriptor: &ModuleDescriptor,
    launch_config: &BindingLaunchConfig,
    module_client_id: &str,
    protocol: ProtocolVersion,
) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(descriptor, launch_config, module_client_id, protocol))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn service_state_dir(
    root: &Path,
    descriptor: &ModuleDescriptor,
    scope: &ServiceScope,
) -> Result<PathBuf> {
    let artifact_identity = serde_json::to_vec(&descriptor.artifact)?;
    Ok(root
        .join(path_component_hash(descriptor.module_id.as_str()))
        .join(path_component_hash_bytes(&artifact_identity))
        .join(path_component_hash(&scope.binding_id))
        .join(scope.generation.to_string()))
}

/// Create only the hashed scope path beneath the already-canonical private root.
/// Every existing component is checked without following it first, then its
/// canonical target is compared with the path we intended. This refuses symlink
/// and junction adoption instead of treating another scope's directory as ours.
fn prepare_private_state_dir(root: &Path, target: &Path) -> Result<()> {
    let relative = target
        .strip_prefix(root)
        .map_err(|_| Error::invalid("module state directory escapes its configured root"))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::invalid(
                "module state directory has an unsafe component",
            ));
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(Error::new(
                        "MODULE_STATE_PATH_UNSAFE",
                        "module state path contains a symlink or non-directory component",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
        if fs::canonicalize(&current)? != current {
            return Err(Error::new(
                "MODULE_STATE_PATH_UNSAFE",
                "module state path resolves outside its intended hashed scope",
            ));
        }
        swarm_process::private_permissions(&current, true)?;
    }
    Ok(())
}

fn path_component_hash(value: &str) -> String {
    path_component_hash_bytes(value.as_bytes())
}

fn path_component_hash_bytes(value: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn capture_identity(pid: u32) -> Result<ProcessIdentity> {
    let birth = process_birth_identity(pid)?.ok_or_else(|| {
        Error::new(
            "PROCESS_GONE",
            "spawned module exited before identity capture",
        )
    })?;
    let image = process_image_identity(pid)?;
    let after = process_birth_identity(pid)?.ok_or_else(|| {
        Error::new(
            "PROCESS_GONE",
            "spawned module exited during identity capture",
        )
    })?;
    if birth != after {
        return Err(Error::new(
            "PROCESS_IDENTITY",
            "module process birth identity changed during image capture",
        ));
    }
    Ok(ProcessIdentity { pid, birth, image })
}

fn process_identity_is_live(identity: &ProcessIdentity) -> Result<bool> {
    match process_birth_identity(identity.pid)? {
        None => Ok(false),
        Some(birth) if birth != identity.birth => Ok(false),
        Some(_) => match capture_identity(identity.pid) {
            Ok(current) => Ok(current.birth == identity.birth && current.image == identity.image),
            // Direct exit may race the bounded executable-image read. This is
            // a liveness observation, never an owner-family departure proof.
            Err(error) if error.code == "PROCESS_GONE" => Ok(false),
            Err(error) => Err(error),
        },
    }
}

fn identity_from_process_image(image: &Value) -> Result<ProcessIdentity> {
    let pid = image
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(|| Error::invalid("worker image receipt has an invalid PID"))?;
    #[cfg(windows)]
    let birth = serde_json::json!({
        "platform": "windows",
        "pid": pid,
        "creation_filetime": image.get("creation_filetime")
            .ok_or_else(|| Error::invalid("worker image receipt has no creation time"))?,
    });
    #[cfg(target_os = "linux")]
    let birth = serde_json::json!({
        "platform": "linux",
        "pid": pid,
        "start_ticks": image.get("start_ticks")
            .ok_or_else(|| Error::invalid("worker image receipt has no start time"))?,
        "boot_id": image.get("boot_id")
            .ok_or_else(|| Error::invalid("worker image receipt has no boot id"))?,
    });
    #[cfg(not(any(windows, target_os = "linux")))]
    return Err(Error::new(
        "PROCESS_IDENTITY_UNSUPPORTED",
        "exact module worker identity is supported only on Windows and Linux",
    ));
    #[cfg(any(windows, target_os = "linux"))]
    Ok(ProcessIdentity {
        pid,
        birth,
        image: image.clone(),
    })
}

fn image_matches_digest(image: &Value, expected: &str) -> bool {
    image
        .get("image_sha256")
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("sha256:"))
        .is_some_and(|actual| actual.eq_ignore_ascii_case(expected))
}

fn image_path_matches(image: &Value, expected: &Path) -> bool {
    let Some(actual) = image.get("image_path").and_then(Value::as_str) else {
        return false;
    };
    #[cfg(windows)]
    {
        actual.eq_ignore_ascii_case(&expected.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        Path::new(actual) == expected
    }
}

fn hash_file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_registration_response(response: &Value, descriptor: &ModuleDescriptor) -> Result<()> {
    let registered = response.get("registered").and_then(Value::as_bool);
    let unchanged = response.get("unchanged").and_then(Value::as_bool);
    let revision = response.get("catalog_revision").and_then(Value::as_u64);
    let registered_revision = response.get("registered_revision").and_then(Value::as_u64);
    let expected_artifact = serde_json::to_value(&descriptor.artifact)?;
    if registered.is_none()
        || unchanged.is_none()
        || registered == unchanged
        || revision.is_none_or(|value| value == 0)
        || registered_revision.is_none_or(|value| value == 0 || value > revision.unwrap_or(0))
        || response.get("module_id").and_then(Value::as_str) != Some(descriptor.module_id.as_str())
        || response.get("artifact") != Some(&expected_artifact)
    {
        return Err(Error::new(
            "MODULE_DESCRIPTOR_REGISTER_RESPONSE",
            "Store registration response does not confirm this exact descriptor identity",
        ));
    }
    Ok(())
}

fn read_json_receipt_optional(path: &Path, limit: u64) -> Result<Option<Value>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(Error::new(
            "MODULE_OWNER_RECEIPT_INVALID",
            "module helper receipt is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(Error::invalid("module helper receipt exceeds size limit"));
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn read_canonical_json_receipt_optional(path: &Path, limit: u64) -> Option<Value> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return None;
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)
        .ok()?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > limit {
        return None;
    }
    let receipt: Value = serde_json::from_slice(&bytes).ok()?;
    if serde_json::to_vec(&receipt).ok()?.as_slice() != bytes.as_slice() {
        return None;
    }
    Some(receipt)
}

fn validate_worker_diagnostic_receipt(
    receipt: &Value,
    expected: WorkerDiagnosticScope<'_>,
) -> Option<String> {
    let object = receipt.as_object()?;
    if object.len() != 9
        || object.keys().any(|key| {
            !matches!(
                key.as_str(),
                "version"
                    | "boot_id"
                    | "module"
                    | "binding"
                    | "generation"
                    | "artifact_id"
                    | "artifact_version"
                    | "build_id"
                    | "error_code"
            )
        })
    {
        return None;
    }
    let generation = expected.generation.to_string();
    let build_id_matches = match (expected.build_id, receipt.get("build_id")) {
        (None, Some(Value::Null)) => true,
        (Some(expected), Some(Value::String(actual))) => expected == actual,
        _ => false,
    };
    let error_code = receipt.get("error_code")?.as_str()?;
    if receipt.get("version").and_then(Value::as_u64) != Some(1)
        || receipt.get("boot_id").and_then(Value::as_str) != Some(expected.boot_id)
        || receipt.get("module").and_then(Value::as_str) != Some(expected.module_id)
        || receipt.get("binding").and_then(Value::as_str) != Some(expected.binding_id)
        || receipt.get("generation").and_then(Value::as_str) != Some(generation.as_str())
        || receipt.get("artifact_id").and_then(Value::as_str) != Some(expected.artifact_id)
        || receipt.get("artifact_version").and_then(Value::as_str)
            != Some(expected.artifact_version)
        || !build_id_matches
        || error_code.is_empty()
        || error_code.len() > 64
        || !error_code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return None;
    }
    Some(error_code.to_owned())
}

fn read_owner_receipt_optional(path: &Path) -> Result<Option<(Value, String)>> {
    match fs::symlink_metadata(path) {
        Ok(_) => read_owner_receipt(path).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_current_owner_receipt_optional(
    path: &Path,
    helper_identity: &ProcessIdentity,
    departed_prior_owner: Option<&VerifiedDepartedOwner>,
) -> Result<Option<(Value, String)>> {
    let Some((receipt, token)) = read_owner_receipt_optional(path)? else {
        return Ok(None);
    };
    if owner_matches_worker(&receipt["process"], helper_identity) {
        return Ok(Some((receipt, token)));
    }
    // Ignore only the exact prior receipt whose family departure was already
    // proved. Any other owner identity remains a hard error.
    if departed_prior_owner.is_some_and(|prior| prior.matches(&receipt, &token)) {
        return Ok(None);
    }
    Err(Error::new(
        "MODULE_OWNER_IDENTITY_MISMATCH",
        "owner.json does not identify the exact spawned module-owner helper",
    ))
}

fn owner_matches_worker(owner_process: &Value, worker: &ProcessIdentity) -> bool {
    if owner_process.get("purpose").and_then(Value::as_str) != Some("module")
        || owner_process.get("pid").and_then(Value::as_u64) != Some(u64::from(worker.pid))
        || worker.birth.get("pid").and_then(Value::as_u64) != Some(u64::from(worker.pid))
    {
        return false;
    }
    let mut compared = false;
    for key in ["creation_filetime", "boot_id", "start_ticks"] {
        let Some(expected) = worker.birth.get(key) else {
            continue;
        };
        let Some(actual) = owner_process.get(key) else {
            continue;
        };
        if scalar_text(expected) != scalar_text(actual) {
            return false;
        }
        compared = true;
    }
    compared
}

fn same_process_identity(left: &ProcessIdentity, right: &ProcessIdentity) -> bool {
    left.pid == right.pid && left.birth == right.birth && left.image == right.image
}

fn worker_is_hello_confirmed(
    status: &SupervisorStatus,
    boot_id: &str,
    worker: &ProcessIdentity,
) -> bool {
    matches!(
        &status.lifecycle,
        LifecycleState::ProcessRunning { boot_id: confirmed_boot } if confirmed_boot == boot_id
    ) && status.worker_boot_id.as_deref() == Some(boot_id)
        && status
            .worker
            .as_ref()
            .is_some_and(|confirmed_worker| same_process_identity(confirmed_worker, worker))
}

fn scalar_text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|number| number.to_string()))
}

fn read_owner_receipt(path: &Path) -> Result<(Value, String)> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "MODULE_OWNER_IDENTITY_INVALID",
            "module owner receipt is not a regular file",
        ));
    }
    if metadata.len() > OWNER_RECORD_LIMIT {
        return Err(Error::invalid("module owner record exceeds size limit"));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(OWNER_RECORD_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > OWNER_RECORD_LIMIT {
        return Err(Error::invalid("module owner record exceeds size limit"));
    }
    let owner: Value = serde_json::from_slice(&bytes)?;
    let token = owner
        .get("token")
        .and_then(Value::as_str)
        .filter(|token| Uuid::parse_str(token).is_ok())
        .ok_or_else(|| Error::invalid("module owner token is missing or invalid"))?
        .to_owned();
    if owner.get("version").and_then(Value::as_u64) != Some(1)
        || owner
            .get("process")
            .and_then(Value::as_object)
            .and_then(|process| process.get("purpose"))
            .and_then(Value::as_str)
            != Some("module")
    {
        return Err(Error::invalid("module owner receipt is invalid"));
    }
    Ok((owner, token))
}

fn has_prior_module_state(path: &Path) -> bool {
    match fs::read_dir(path) {
        Ok(mut entries) => match entries.next() {
            Some(Ok(_)) => true,
            Some(Err(_)) => true,
            None => false,
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn is_unresolved(state: &str) -> bool {
    matches!(state, "sending" | "native_accepted" | "outcome_unknown")
}

fn validate_operation_state(state: &str) -> Result<()> {
    if matches!(
        state,
        "queued"
            | "sending"
            | "native_accepted"
            | "outcome_unknown"
            | "settled"
            | "rejected"
            | "cancelled"
    ) {
        Ok(())
    } else {
        Err(Error::new(
            "MODULE_READBACK_STATE_UNKNOWN",
            "Operation readback contains an unrecognized state; restart is unsafe",
        ))
    }
}

fn unix_time_ms() -> Result<u64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("CLOCK_ERROR", "system clock is earlier than the Unix epoch"))?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        Error::new(
            "CLOCK_ERROR",
            "system clock exceeds the restart timestamp range",
        )
    })
}

fn prune_starts(starts: &mut VecDeque<u64>, window_ms: u64, now_ms: u64) {
    starts.retain(|started| *started > now_ms || now_ms.saturating_sub(*started) < window_ms);
}

fn jittered_delay(policy: &crate::RestartPolicy, failure_index: usize) -> Duration {
    let shift = failure_index.min(31) as u32;
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    let base_ms = policy
        .initial_backoff_ms
        .saturating_mul(multiplier)
        .min(policy.max_backoff_ms);
    if !policy.jitter {
        return Duration::from_millis(base_ms);
    }
    let random = Uuid::new_v4().as_u128() as u64;
    // Full-range bounded jitter: choose uniformly from [50%, 100%] of the
    // exponential delay, then cap again at the descriptor's configured max.
    let adjusted = (base_ms / 2).saturating_add(random % (base_ms / 2 + 1));
    let adjusted = adjusted.clamp(1, policy.max_backoff_ms);
    Duration::from_millis(adjusted)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(any(windows, target_os = "linux"))]
    use std::{
        io::Read,
        process::{Child as StdChild, Command as StdCommand, Stdio},
        thread,
    };

    #[test]
    #[cfg(any(windows, target_os = "linux"))]
    fn worker_receipt_identity_matches_live_os_birth_and_rejects_mismatches() {
        let pid = std::process::id();
        let image = process_image_identity(pid).unwrap();
        let identity = identity_from_process_image(&image).unwrap();
        assert_eq!(identity.pid, pid);
        assert_eq!(identity.image, image);
        assert_eq!(
            identity.birth,
            process_birth_identity(pid).unwrap().unwrap()
        );
        assert!(process_identity_is_live(&identity).unwrap());

        let mut wrong_birth = identity_from_process_image(&image).unwrap();
        #[cfg(windows)]
        let birth_field = "creation_filetime";
        #[cfg(target_os = "linux")]
        let birth_field = "start_ticks";
        wrong_birth.birth[birth_field] = serde_json::json!("different-process-birth");
        assert!(!process_identity_is_live(&wrong_birth).unwrap());

        let mut wrong_image = identity_from_process_image(&image).unwrap();
        wrong_image.image["image_sha256"] =
            serde_json::json!(format!("sha256:{}", "00".repeat(32)));
        assert!(!process_identity_is_live(&wrong_image).unwrap());
    }

    #[test]
    fn worker_diagnostic_receipt_requires_exact_boot_scope_and_artifact() {
        let receipt = serde_json::json!({
            "version": 1,
            "boot_id": "boot-1",
            "module": "fixture",
            "binding": "binding-1",
            "generation": "7",
            "artifact_id": "fixture.adapter",
            "artifact_version": "3",
            "build_id": null,
            "error_code": "HOST_RETRY_EXHAUSTED",
        });
        let expected = |receipt: &Value| {
            validate_worker_diagnostic_receipt(
                receipt,
                WorkerDiagnosticScope {
                    boot_id: "boot-1",
                    module_id: "fixture",
                    binding_id: "binding-1",
                    generation: 7,
                    artifact_id: "fixture.adapter",
                    artifact_version: "3",
                    build_id: None,
                },
            )
        };
        assert_eq!(expected(&receipt), Some("HOST_RETRY_EXHAUSTED".to_owned()));

        for field in [
            "boot_id",
            "module",
            "binding",
            "generation",
            "artifact_id",
            "artifact_version",
            "build_id",
        ] {
            let mut mismatch = receipt.clone();
            mismatch[field] = serde_json::json!("different");
            assert!(expected(&mismatch).is_none(), "accepted mismatched {field}");
        }

        let mut extra = receipt.clone();
        extra["detail"] = serde_json::json!("raw output is not admitted");
        assert!(expected(&extra).is_none());

        let mut invalid_code = receipt;
        invalid_code["error_code"] = serde_json::json!("lowercase");
        assert!(expected(&invalid_code).is_none());
    }

    struct NoResolver;

    impl ProtectedResolver for NoResolver {
        fn resolver_map_path(&self, _: &ProtectedResolverContext) -> Result<PathBuf> {
            panic!("startup fixtures must stop before resolver or process creation")
        }
    }

    struct StartupFixture {
        root: PathBuf,
        service: Arc<Service>,
    }

    impl StartupFixture {
        fn prepare_accounting(&self) {
            self.service.prepare_state_dir().unwrap();
            drop(
                swarm_process::module_owner::acquire_module_state_marker(&self.service.state_dir)
                    .unwrap(),
            );
        }

        fn new() -> Self {
            Self::build(false)
        }

        #[cfg(any(windows, target_os = "linux"))]
        fn new_with_current_executable() -> Self {
            Self::build(true)
        }

        fn build(pin_current_executable: bool) -> Self {
            let root = std::env::temp_dir().join(format!("swarm-startup-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            let root = fs::canonicalize(root).unwrap();
            swarm_process::private_permissions(&root, true).unwrap();
            let current_executable = pin_current_executable
                .then(|| fs::canonicalize(std::env::current_exe().unwrap()).unwrap());
            let executable = current_executable
                .clone()
                .unwrap_or_else(|| root.join("missing-adapter"));
            let executable_sha256 = current_executable
                .as_ref()
                .map(|path| hash_file_sha256(path).unwrap())
                .unwrap_or_else(|| "11".repeat(32));
            let owner_executable = current_executable
                .clone()
                .unwrap_or_else(|| root.join("missing-owner"));
            let owner_executable_sha256 = current_executable
                .as_ref()
                .map(|path| hash_file_sha256(path).unwrap())
                .unwrap_or_else(|| "33".repeat(32));
            let descriptor: ModuleDescriptor = serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "module_id": "fixture",
                "artifact": {"artifact_id": "fixture.adapter", "version": "1"},
                "launch": {
                    "executable": executable,
                    "executable_sha256": executable_sha256
                },
                "protocol": {
                    "minimum": {"major": 1, "minor": 0},
                    "maximum": {"major": 1, "minor": 0}
                },
                "lifecycle": "owned_service",
                "activation": "on_demand",
                "enabled": true
            }))
            .unwrap();
            let mut descriptor = descriptor;
            descriptor.restart.max_starts = 1;
            descriptor.validate().unwrap();
            let scope = ServiceScope {
                binding_id: Uuid::new_v4().to_string(),
                generation: 1,
            };
            let (admission, _) = watch::channel(AdmissionState::Open);
            let service = Arc::new(Service::new(ServiceInitialization {
                state_dir: service_state_dir(&root, &descriptor, &scope).unwrap(),
                state_root: root.clone(),
                host_data_dir: root.clone(),
                descriptor: Arc::new(descriptor),
                descriptor_fingerprint: "22".repeat(32),
                scope,
                ipc: IpcConfig::default(),
                launch_config: BindingLaunchConfig::default(),
                module_client_id: "fixture.client".to_owned(),
                protocol: ProtocolVersion { major: 1, minor: 0 },
                module_contract_json: "{}".to_owned(),
                owner_executable: ModuleOwnerExecutable {
                    path: owner_executable,
                    sha256: crate::Sha256Digest::new(owner_executable_sha256).unwrap(),
                },
                resolver: Arc::new(NoResolver),
                admission,
                demand_gate: Arc::new(AsyncMutex::new(())),
            }));
            Self { root, service }
        }
    }

    impl Drop for StartupFixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    async fn startup_with_config_key(raw_key: String) -> (StartupFixture, Vec<u8>, Vec<u8>) {
        let mut fixture = StartupFixture::new();
        Arc::get_mut(&mut fixture.service)
            .expect("startup fixture owns its service uniquely")
            .launch_config
            .values
            .insert(raw_key, LaunchValue::Literal("fixture-value".to_owned()));
        fixture.service.launch_config.validate().unwrap();
        fixture.prepare_accounting();
        fixture
            .service
            .write_restart_history(&VecDeque::new())
            .unwrap();
        let history_before =
            fs::read(fixture.service.state_dir.join("restart-history.json")).unwrap();
        let marker_before = fs::read(fixture.service.state_dir.join("module.lock")).unwrap();
        lock(&fixture.service.demands).insert("fixture-demand".to_owned(), 1);
        time::timeout(
            Duration::from_secs(5),
            fixture.service.clone().run_lifecycle(),
        )
        .await
        .unwrap();
        (fixture, history_before, marker_before)
    }

    async fn assert_invalid_module_config_key_rejected_before_accounting(raw_key: String) {
        let (fixture, history_before, marker_before) =
            startup_with_config_key(raw_key.clone()).await;
        let service = &fixture.service;
        let status = service.current_status();
        assert!(matches!(
            &status.lifecycle,
            LifecycleState::Isolated { reason }
                if reason == "launch config produces an invalid final environment name"
        ));
        assert!(status.last_failure.is_some());
        assert_eq!(status.restart.starts_in_window, 0);
        assert!(status.restart.exhausted);
        assert!(status.owner.is_none());
        assert!(status.worker.is_none());
        assert!(status.worker_boot_id.is_none());
        assert!(matches!(
            &status.effect_certainty,
            ModuleEffectCertainty::Unknown
        ));
        assert!(!status.readback_required);
        assert!(service.read_restart_history().unwrap().is_empty());
        assert_eq!(
            fs::read(service.state_dir.join("restart-history.json")).unwrap(),
            history_before,
            "invalid raw key {raw_key:?} must not persist a restart attempt"
        );
        assert_eq!(
            fs::read(service.state_dir.join("module.lock")).unwrap(),
            marker_before,
            "invalid raw key {raw_key:?} must preserve the existing custody marker"
        );
        assert!(!service.state_dir.join("launch-attempt.json").exists());
        assert!(!service.state_dir.join("owner.json").exists());
        assert!(!service.state_dir.join("worker.json").exists());
        assert!(!service.state_dir.join("launch-result.json").exists());
        assert!(service.helper_child_custody.lock().await.is_none());
    }

    #[test]
    fn module_config_final_name_uses_exact_prefix_and_128_byte_predicate() {
        let raw_limit = 128 - MODULE_CONFIG_ENV_PREFIX.len();
        let raw_key = "A".repeat(raw_limit);
        let final_name = Service::module_config_environment_name(&raw_key).unwrap();
        assert_eq!(final_name, format!("ELIOT_SWARM_CONFIG_{raw_key}"));
        assert_eq!(final_name.len(), 128);

        let over_limit_key = "A".repeat(raw_limit + 1);
        assert!(Service::module_config_environment_name(&over_limit_key).is_err());
        assert!(Service::module_config_environment_name("opencode_service_id").is_err());
    }

    #[tokio::test]
    async fn startup_invalid_module_config_name_rejects_before_restart_budget_or_owner_launch() {
        let raw_limit = 128 - MODULE_CONFIG_ENV_PREFIX.len();
        let (at_limit, history_before, _) = startup_with_config_key("A".repeat(raw_limit)).await;
        assert_eq!(
            at_limit
                .service
                .current_status()
                .last_failure
                .as_ref()
                .map(|failure| failure.code.as_str()),
            Some("MODULE_EXECUTABLE_UNAVAILABLE"),
            "a 128-byte final name must pass preflight"
        );
        assert_eq!(at_limit.service.read_restart_history().unwrap().len(), 1);
        assert_ne!(
            fs::read(at_limit.service.state_dir.join("restart-history.json")).unwrap(),
            history_before,
            "the admitted boundary name must reach restart accounting"
        );
        assert!(at_limit.service.helper_child_custody.lock().await.is_none());

        assert_invalid_module_config_key_rejected_before_accounting("A".repeat(raw_limit + 1))
            .await;
        assert_invalid_module_config_key_rejected_before_accounting(
            "opencode_service_id".to_owned(),
        )
        .await;
    }

    #[cfg(any(windows, target_os = "linux"))]
    struct WorkerProcessFixture {
        child: StdChild,
        _stdin: std::process::ChildStdin,
        identity: ProcessIdentity,
    }

    #[cfg(any(windows, target_os = "linux"))]
    impl WorkerProcessFixture {
        fn new() -> Self {
            Self::spawn(None, None)
        }

        fn new_with_release_file(path: &Path) -> Self {
            Self::spawn(Some(path), None)
        }

        fn new_with_grandchild(
            worker_release_path: &Path,
            grandchild_release_path: &Path,
            grandchild_ready_path: &Path,
        ) -> Self {
            Self::spawn(
                Some(worker_release_path),
                Some((grandchild_release_path, grandchild_ready_path)),
            )
        }

        fn spawn(release_file: Option<&Path>, grandchild: Option<(&Path, &Path)>) -> Self {
            let mut command = StdCommand::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "supervisor::tests::worker_process_fixture_waits_for_release",
                    "--ignored",
                    "--nocapture",
                ])
                .env("ELIOT_SWARM_SUPERVISOR_WORKER_FIXTURE", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Some(path) = release_file {
                command.env("ELIOT_SWARM_SUPERVISOR_WORKER_RELEASE_PATH", path);
            }
            if let Some((release_path, ready_path)) = grandchild {
                command
                    .env(
                        "ELIOT_SWARM_SUPERVISOR_GRANDCHILD_RELEASE_PATH",
                        release_path,
                    )
                    .env("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_READY_PATH", ready_path);
            }
            let mut child = command.spawn().expect("start OS-backed worker fixture");
            if let Some((_, ready_path)) = grandchild {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !ready_path.is_file() {
                    if let Some(exit) = child.try_wait().expect("poll worker fixture") {
                        panic!("worker exited before its grandchild started: {exit}");
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "worker fixture did not start its grandchild before deadline"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
            }
            let pid = child.id();
            let stdin = child.stdin.take().expect("pipe worker fixture stdin");
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let identity = loop {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("worker fixture identity was not readable before deadline");
                }
                match capture_identity(pid) {
                    Ok(identity) => break identity,
                    Err(error) if error.code == "PROCESS_GONE" => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        panic!("worker fixture identity failed: {}", error.code);
                    }
                }
            };
            Self {
                child,
                _stdin: stdin,
                identity,
            }
        }

        fn stop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    impl Drop for WorkerProcessFixture {
        fn drop(&mut self) {
            self.stop();
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    struct ConnectedOwnerFixture {
        startup: StartupFixture,
        helper: Option<Child>,
        helper_identity: ProcessIdentity,
        worker_identity: ProcessIdentity,
        boot_id: String,
        owner_receipt: Value,
        release_path: PathBuf,
        worker_release_path: PathBuf,
        owner_exit_path: PathBuf,
        grandchild_release_path: PathBuf,
        grandchild_identity: Option<ProcessIdentity>,
    }

    #[cfg(any(windows, target_os = "linux"))]
    impl ConnectedOwnerFixture {
        async fn new() -> Self {
            Self::new_inner(false, false).await
        }

        async fn new_with_grandchild() -> Self {
            Self::new_inner(true, false).await
        }

        async fn new_successful() -> Self {
            Self::new_inner(false, true).await
        }

        async fn new_inner(with_grandchild: bool, successful_exit: bool) -> Self {
            let startup = StartupFixture::new_with_current_executable();
            startup.prepare_accounting();
            let boot_id = Uuid::new_v4().to_string();
            let state_dir = &startup.service.state_dir;
            let release_path = state_dir.join("projection-fixture-release");
            let worker_release_path = state_dir.join("projection-worker-release");
            let owner_exit_path = state_dir.join("projection-fixture-owner-exit");
            let grandchild_release_path = std::env::temp_dir()
                .join(format!("eliot-swarm-grandchild-{}.release", Uuid::new_v4()));
            let grandchild_ready_path = state_dir.join("projection-grandchild-ready");
            let mut owner_command = Command::new(std::env::current_exe().unwrap());
            owner_command
                .args([
                    "--exact",
                    "supervisor::tests::owner_group_fixture_waits_for_release",
                    "--ignored",
                    "--nocapture",
                ])
                .env("ELIOT_SWARM_SUPERVISOR_OWNER_FIXTURE", "1")
                .env(
                    "ELIOT_SWARM_SUPERVISOR_OWNER_FIXTURE_SUCCESS",
                    if successful_exit { "1" } else { "0" },
                )
                .env("ELIOT_SWARM_SUPERVISOR_OWNER_STATE_DIR", state_dir)
                .env("ELIOT_SWARM_SUPERVISOR_OWNER_BOOT_ID", &boot_id)
                .env(
                    "ELIOT_SWARM_SUPERVISOR_OWNER_BINDING_ID",
                    &startup.service.scope.binding_id,
                )
                .env(
                    "ELIOT_SWARM_SUPERVISOR_OWNER_GENERATION",
                    startup.service.scope.generation.to_string(),
                );
            if with_grandchild {
                owner_command
                    .env(
                        "ELIOT_SWARM_SUPERVISOR_OWNER_GRANDCHILD_RELEASE_PATH",
                        &grandchild_release_path,
                    )
                    .env(
                        "ELIOT_SWARM_SUPERVISOR_OWNER_GRANDCHILD_READY_PATH",
                        &grandchild_ready_path,
                    );
            }
            let mut helper = owner_command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start OS-backed owner-family fixture");
            let helper_pid = helper.id().expect("owner-family fixture has a PID");
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let helper_identity = loop {
                if std::time::Instant::now() >= deadline {
                    let _ = helper.start_kill();
                    let _ = helper.wait().await;
                    panic!("owner-family fixture identity was not readable before deadline");
                }
                match capture_identity(helper_pid) {
                    Ok(identity) => break identity,
                    Err(error) if error.code == "PROCESS_GONE" => {
                        if let Some(exit) = helper.try_wait().unwrap() {
                            panic!("owner-family fixture exited before identity capture: {exit}");
                        }
                        time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => {
                        let _ = helper.start_kill();
                        let _ = helper.wait().await;
                        panic!("owner-family fixture identity failed: {}", error.code);
                    }
                }
            };
            startup
                .service
                .write_launch_attempt(&boot_id, Some(&helper_identity))
                .unwrap();

            let ready_path = state_dir.join("projection-fixture-ready");
            while !ready_path.is_file() {
                if let Some(exit) = helper.try_wait().unwrap() {
                    panic!("owner-family fixture exited before publishing receipts: {exit}");
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "owner-family fixture did not publish receipts before deadline"
                );
                time::sleep(Duration::from_millis(10)).await;
            }

            let owner_receipt: Value =
                serde_json::from_slice(&fs::read(state_dir.join("owner.json")).unwrap()).unwrap();
            let worker_receipt: Value =
                serde_json::from_slice(&fs::read(state_dir.join("worker.json")).unwrap()).unwrap();
            let worker_identity = startup
                .service
                .validate_worker_receipt(&worker_receipt, &boot_id, &owner_receipt)
                .unwrap()
                .expect("connected worker receipt identifies the live worker");
            let grandchild_identity = if with_grandchild {
                let pid = fs::read_to_string(&grandchild_ready_path)
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();
                Some(capture_identity(pid).expect("capture actual grandchild identity"))
            } else {
                None
            };
            assert!(
                startup
                    .service
                    .observe_owned_worker(&boot_id, &helper_identity, Some(&worker_identity))
                    .unwrap()
            );

            Self {
                startup,
                helper: Some(helper),
                helper_identity,
                worker_identity,
                boot_id,
                owner_receipt,
                release_path,
                worker_release_path,
                owner_exit_path,
                grandchild_release_path,
                grandchild_identity,
            }
        }

        fn service(&self) -> &Service {
            &self.startup.service
        }

        fn release(&self) {
            fs::write(&self.release_path, b"release").unwrap();
        }

        async fn exit_owner_only(&self) {
            fs::write(&self.owner_exit_path, b"exit").unwrap();
            let mut custody = self.service().helper_child_custody.lock().await;
            let retained = custody
                .as_mut()
                .expect("the direct owner Child remains in custody");
            let exit = retained.child.wait().await.unwrap();
            assert_eq!(exit.code(), Some(23));
        }

        fn release_worker(&self) {
            fs::write(&self.worker_release_path, b"release").unwrap();
        }

        fn release_grandchild(&self) {
            fs::write(&self.grandchild_release_path, b"release").unwrap();
        }

        async fn prove_departure(&mut self) {
            let helper = self.helper.take().expect("owner helper is retained");
            self.service()
                .retain_helper_child_custody(
                    helper,
                    Some(self.helper_identity.clone()),
                    Some(self.boot_id.clone()),
                    "MODULE_WORKER_EXITED",
                )
                .await;
            self.service().remember_owner_family_query(
                &self.owner_receipt["process"],
                self.owner_receipt["token"].as_str().unwrap(),
            );
            self.release();
            {
                let mut custody = self.service().helper_child_custody.lock().await;
                let retained = custody
                    .as_mut()
                    .expect("exact owner Child remains retained");
                assert_eq!(retained.child.wait().await.unwrap().code(), Some(23));
            }
            self.service()
                .reconcile_retained_helper_child()
                .await
                .unwrap();
            assert!(matches!(
                self.service().current_status().lifecycle,
                LifecycleState::ProcessExited {
                    exit_proven: true,
                    ..
                }
            ));
        }

        async fn finish_without_proving_departure(&mut self) {
            self.release();
            if let Some(mut helper) = self.helper.take() {
                let _ = helper.wait().await.unwrap();
            }
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    impl Drop for ConnectedOwnerFixture {
        fn drop(&mut self) {
            let _ = fs::write(&self.release_path, b"release");
            let _ = fs::write(&self.worker_release_path, b"release");
            let _ = fs::write(&self.owner_exit_path, b"exit");
            let _ = fs::write(&self.grandchild_release_path, b"release");
            if let Some(helper) = self.helper.as_mut() {
                let deadline = std::time::Instant::now() + Duration::from_secs(3);
                loop {
                    match helper.try_wait() {
                        Ok(Some(_)) => break,
                        _ if std::time::Instant::now() >= deadline => {
                            let _ = helper.start_kill();
                            break;
                        }
                        _ => thread::sleep(Duration::from_millis(10)),
                    }
                }
            }
            let mut grandchild_exited = self.grandchild_identity.is_none();
            if let Some(grandchild) = self.grandchild_identity.as_ref() {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    match process_identity_is_live(grandchild) {
                        Ok(false) => {
                            grandchild_exited = true;
                            break;
                        }
                        Err(error) if error.code == "PROCESS_GONE" => {
                            grandchild_exited = true;
                            break;
                        }
                        _ if std::time::Instant::now() >= deadline => break,
                        _ => thread::sleep(Duration::from_millis(10)),
                    }
                }
            }
            if grandchild_exited {
                let _ = fs::remove_file(&self.grandchild_release_path);
            }
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    fn empty_operation_readback(scope: &ServiceScope) -> OperationReadback {
        OperationReadback {
            scope: scope.clone(),
            complete: true,
            operations: Vec::new(),
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    fn aggregate_has_obligations(service: &Service) -> bool {
        crate::standalone::StandaloneSupervisor::registry_statuses_have_obligations_for_test([
            service.current_status(),
        ])
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    #[ignore = "spawned by the supervisor connected registry projection regressions"]
    fn owner_group_fixture_waits_for_release() {
        if std::env::var_os("ELIOT_SWARM_SUPERVISOR_OWNER_FIXTURE").is_none() {
            return;
        }
        let state_dir = PathBuf::from(
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_OWNER_STATE_DIR")
                .expect("owner fixture state directory"),
        );
        let boot_id =
            std::env::var("ELIOT_SWARM_SUPERVISOR_OWNER_BOOT_ID").expect("owner fixture boot ID");
        let binding_id = std::env::var("ELIOT_SWARM_SUPERVISOR_OWNER_BINDING_ID")
            .expect("owner fixture binding ID");
        let generation = std::env::var("ELIOT_SWARM_SUPERVISOR_OWNER_GENERATION")
            .expect("owner fixture generation");
        let token = Uuid::new_v4().to_string();
        let group = swarm_process::Group::enter_module(&token).unwrap();
        let owner_receipt = serde_json::json!({
            "version": 1,
            "token": token,
            "process": group.identity.clone(),
        });
        fs::write(
            state_dir.join("owner.json"),
            serde_json::to_vec(&owner_receipt).unwrap(),
        )
        .unwrap();

        let worker_release_path = state_dir.join("projection-worker-release");
        let mut worker = match (
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_OWNER_GRANDCHILD_RELEASE_PATH"),
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_OWNER_GRANDCHILD_READY_PATH"),
        ) {
            (Some(release_path), Some(ready_path)) => WorkerProcessFixture::new_with_grandchild(
                &worker_release_path,
                Path::new(&release_path),
                Path::new(&ready_path),
            ),
            _ => WorkerProcessFixture::new_with_release_file(&worker_release_path),
        };
        let worker_receipt = serde_json::json!({
            "version": 1,
            "boot_id": boot_id,
            "module": "fixture",
            "binding": binding_id,
            "generation": generation,
            "artifact_id": "fixture.adapter",
            "artifact_version": "1",
            "build_id": null,
            "protocol": "1.0",
            "module_contract": "{}",
            "process": worker.identity.image.clone(),
        });
        fs::write(
            state_dir.join("worker.json"),
            serde_json::to_vec(&worker_receipt).unwrap(),
        )
        .unwrap();
        fs::write(state_dir.join("projection-fixture-ready"), b"ready").unwrap();

        let release_path = state_dir.join("projection-fixture-release");
        let owner_exit_path = state_dir.join("projection-fixture-owner-exit");
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !release_path.exists() && !owner_exit_path.exists() {
            if std::time::Instant::now() >= deadline {
                worker.stop();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        if owner_exit_path.exists() {
            std::process::exit(23);
        }
        worker.stop();
        std::process::exit(
            if std::env::var("ELIOT_SWARM_SUPERVISOR_OWNER_FIXTURE_SUCCESS").as_deref() == Ok("1") {
                0
            } else {
                23
            },
        );
    }

    #[cfg(windows)]
    fn module_job_open_access_denied(_: &Value, _: &Value) -> Result<bool> {
        Err(Error::new(
            "MODULE_MEMBERSHIP_UNKNOWN",
            "OpenJobObjectW failed: ERROR_ACCESS_DENIED (5); exact module Job membership is unknown",
        ))
    }

    #[cfg(windows)]
    #[repr(C)]
    struct TestAcl {
        revision: u8,
        sbz1: u8,
        size: u16,
        ace_count: u16,
        sbz2: u16,
    }

    #[cfg(windows)]
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenJobObjectW(access: u32, inherit: i32, name: *const u16) -> *mut std::ffi::c_void;
        fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
    }

    #[cfg(windows)]
    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn InitializeAcl(acl: *mut TestAcl, length: u32, revision: u32) -> i32;
        fn SetSecurityInfo(
            handle: *mut std::ffi::c_void,
            object_type: i32,
            security_info: u32,
            owner: *mut std::ffi::c_void,
            group: *mut std::ffi::c_void,
            dacl: *const TestAcl,
            sacl: *const TestAcl,
        ) -> u32;
    }

    #[cfg(windows)]
    fn deny_named_job_query(identity: &Value, token: &str) -> Result<()> {
        const WRITE_DAC: u32 = 0x0004_0000;
        const SE_KERNEL_OBJECT: i32 = 6;
        const DACL_SECURITY_INFORMATION: u32 = 4;
        const ACL_REVISION: u32 = 2;

        let expected = format!(r"Global\EliotSwarmModule-{token}");
        if identity["job_name"].as_str() != Some(expected.as_str()) {
            return Err(Error::new(
                "PROCESS_FAMILY_IDENTITY_INVALID",
                "test ACL target does not match the exact module Job receipt",
            ));
        }
        let name: Vec<u16> = expected.encode_utf16().chain(Some(0)).collect();
        // SAFETY: the name is nul-terminated and the opened handle requests only WRITE_DAC.
        let handle = unsafe { OpenJobObjectW(WRITE_DAC, 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error().into());
        }
        let result = (|| {
            let mut acl = TestAcl {
                revision: 0,
                sbz1: 0,
                size: 0,
                ace_count: 0,
                sbz2: 0,
            };
            // SAFETY: acl points to writable storage of the exact Windows ACL header size.
            if unsafe {
                InitializeAcl(
                    &mut acl,
                    std::mem::size_of::<TestAcl>() as u32,
                    ACL_REVISION,
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: handle is open and acl is a valid initialized empty DACL.
            let status = unsafe {
                SetSecurityInfo(
                    handle,
                    SE_KERNEL_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &acl,
                    std::ptr::null(),
                )
            };
            if status != 0 {
                return Err(std::io::Error::from_raw_os_error(status as i32).into());
            }
            Ok(())
        })();
        // SAFETY: handle was returned by OpenJobObjectW and is closed exactly once.
        unsafe { CloseHandle(handle) };
        result
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn owner_worker_grandchild_requires_authoritative_job_departure() {
        let mut fixture = ConnectedOwnerFixture::new_with_grandchild().await;
        let service = fixture.startup.service.clone();
        let token = fixture.owner_receipt["token"].as_str().unwrap();
        let grandchild = fixture
            .grandchild_identity
            .as_ref()
            .expect("the fixture has a real grandchild process");
        assert!(process_identity_is_live(grandchild).unwrap());

        // The production monitor captures this query handle when owner.json
        // first arrives, while the name is still openable.
        service.remember_owner_family_query(&fixture.owner_receipt["process"], token);
        assert!(lock(&service.owner_family_query).is_some());
        deny_named_job_query(&fixture.owner_receipt["process"], token).unwrap();

        let mut helper = fixture.helper.take().expect("owner Child is retained");
        fs::write(&fixture.owner_exit_path, b"exit").unwrap();
        assert_eq!(helper.wait().await.unwrap().code(), Some(23));
        fixture.release_worker();
        time::timeout(Duration::from_secs(5), async {
            while process_identity_is_live(&fixture.worker_identity).unwrap() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker exits while the grandchild remains alive");
        assert!(!process_identity_is_live(&fixture.worker_identity).unwrap());
        assert!(process_identity_is_live(grandchild).unwrap());
        assert!(matches!(
            observe_process_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::Unknown(_)
        ));

        let missing_token = Uuid::new_v4().to_string();
        let missing_identity = serde_json::json!({
            "pid": u64::from(u32::MAX),
            "creation_filetime": 1,
            "scope": "windows_job",
            "purpose": "module",
            "job_name": format!(r"Global\EliotSwarmModule-{missing_token}"),
        });
        let missing = swarm_process::departed_empty(&missing_identity, &missing_token)
            .expect_err("a missing named Job cannot prove family departure");
        assert_eq!(missing.code, "PROCESS_FAMILY_UNKNOWN");

        let helper_identity = fixture.helper_identity.clone();
        let boot_id = fixture.boot_id.clone();
        *lock(&service.final_owner_timeout) = Some(Duration::from_millis(100));
        let monitor = service.monitor_owner_helper(
            &mut helper,
            &boot_id,
            &helper_identity,
            None,
            Instant::now() + Duration::from_secs(5),
        );
        let query_error = match monitor.await {
            Err(error) => error,
            Ok(_) => {
                panic!("monitor must retain custody while a real grandchild remains in the Job")
            }
        };
        assert_eq!(query_error.code, "MODULE_FINAL_FAMILY_OBSERVATION_TIMEOUT");
        assert!(matches!(
            service.observe_owner_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::Retained
        ));
        service
            .retain_helper_after_monitor_error(helper, helper_identity, &boot_id, query_error)
            .await;

        *lock(&service.prior_owner_timeout) = Some(Duration::from_millis(100));
        let launch_attempt = fs::read(service.state_dir.join("launch-attempt.json")).unwrap();
        time::timeout(Duration::from_secs(2), service.clone().run_lifecycle())
            .await
            .expect("bounded retained-family reconciliation");
        assert!(service.helper_child_custody.lock().await.is_some());
        assert!(process_identity_is_live(grandchild).unwrap());
        assert_eq!(
            service
                .current_status()
                .last_failure
                .as_ref()
                .map(|failure| failure.code.as_str()),
            Some("MODULE_PRIOR_OWNER_WAIT_TIMEOUT")
        );
        assert_eq!(
            fs::read(service.state_dir.join("launch-attempt.json")).unwrap(),
            launch_attempt,
            "no replacement launch may be recorded while the grandchild lives"
        );

        fixture.release_grandchild();
        time::timeout(Duration::from_secs(5), async {
            while process_identity_is_live(grandchild).unwrap() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild exits only after explicit release");
        assert!(matches!(
            observe_process_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::Unknown(_)
        ));
        assert!(matches!(
            service.observe_owner_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::ConfirmedEmpty
        ));
        service
            .reconcile_retained_helper_child()
            .await
            .expect("retained Job accounting proves exact zero-member departure");
        assert!(service.helper_child_custody.lock().await.is_none());
        assert!(lock(&service.owner_family_query).is_none());
        app369_supervisor_consumes_actual_spawn_receipt_fixture().await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn module_membership_job_open_uncertainty_retains_child_until_exact_family_departure() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        let service = fixture.startup.service.clone();
        let launch_attempt_path = service.state_dir.join("launch-attempt.json");
        let launch_attempt_before = fs::read(&launch_attempt_path).unwrap();
        let token = fixture.owner_receipt["token"].as_str().unwrap();
        assert!(matches!(
            observe_process_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::Retained
        ));

        *lock(&service.module_membership_probe) = Some(module_job_open_access_denied);
        let mut helper = fixture.helper.take().expect("owner Child is retained");
        let error = match service
            .monitor_owner_helper(
                &mut helper,
                &fixture.boot_id,
                &fixture.helper_identity,
                None,
                Instant::now() + Duration::from_secs(5),
            )
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("uncertain Job membership must reject the worker receipt"),
        };
        assert_eq!(error.code, "MODULE_MEMBERSHIP_UNKNOWN");
        assert!(error.message.contains("OpenJobObjectW"));
        assert!(error.message.contains("ERROR_ACCESS_DENIED"));

        service
            .retain_helper_after_monitor_error(
                helper,
                fixture.helper_identity.clone(),
                &fixture.boot_id,
                error,
            )
            .await;
        assert!(matches!(
            service.current_status().lifecycle,
            LifecycleState::OwnerGroupRetained {
                reason: OwnerRetainedReason::OwnerFamilyObservationUnknown,
                ..
            }
        ));
        assert!(service.current_status().readback_required);
        assert!(service.helper_child_custody.lock().await.is_some());
        assert_eq!(
            fs::read(&launch_attempt_path).unwrap(),
            launch_attempt_before
        );

        let error = service
            .reconcile_retained_helper_child()
            .await
            .expect_err("uncertain Job membership cannot release retained Child custody");
        assert_eq!(error.code, "MODULE_MEMBERSHIP_UNKNOWN");
        assert!(service.helper_child_custody.lock().await.is_some());
        assert_eq!(
            fs::read(&launch_attempt_path).unwrap(),
            launch_attempt_before
        );

        *lock(&service.module_membership_probe) = None;
        fixture.exit_owner_only().await;
        assert!(!process_identity_is_live(&fixture.helper_identity).unwrap());
        assert!(process_identity_is_live(&fixture.worker_identity).unwrap());

        lock(&service.demands).insert("queued-second-launch".to_owned(), 1);
        tokio::time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        assert!(service.helper_child_custody.lock().await.is_some());
        assert_eq!(
            service
                .current_status()
                .last_failure
                .as_ref()
                .map(|failure| failure.code.as_str()),
            Some("MODULE_MEMBERSHIP_UNKNOWN")
        );
        assert_eq!(
            fs::read(&launch_attempt_path).unwrap(),
            launch_attempt_before
        );

        lock(&service.demands).clear();
        service.signal();
        fixture.release_worker();
        tokio::time::timeout(Duration::from_secs(5), async {
            while process_identity_is_live(&fixture.worker_identity).unwrap() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            service.reconcile_retained_helper_child(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(service.helper_child_custody.lock().await.is_none());
        assert!(matches!(
            service.current_status().lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            }
        ));
        assert_eq!(
            fs::read(&launch_attempt_path).unwrap(),
            launch_attempt_before
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn registry_obligation_projection_releases_departed_worker_after_complete_empty_readback()
    {
        let mut fixture = ConnectedOwnerFixture::new().await;
        let scope = fixture.service().scope.clone();
        fixture
            .service()
            .apply_readback(OperationReadback {
                scope: scope.clone(),
                complete: true,
                operations: vec![OperationSnapshot {
                    operation_id: "departed-worker-operation".to_owned(),
                    binding_id: scope.binding_id,
                    generation: scope.generation,
                    state: "outcome_unknown".to_owned(),
                }],
            })
            .unwrap();
        assert_eq!(
            fixture.service().current_status().unknown_operation_count,
            1
        );
        fixture.prove_departure().await;

        let before_readback = fixture.service().current_status();
        assert!(before_readback.readback_required);
        assert_eq!(
            before_readback.effect_certainty,
            ModuleEffectCertainty::Unknown
        );
        assert_eq!(before_readback.unknown_operation_count, 1);
        assert_eq!(
            before_readback.unknown_operation_ids,
            vec!["departed-worker-operation".to_owned()]
        );
        assert!(aggregate_has_obligations(fixture.service()));

        fixture
            .service()
            .apply_readback(empty_operation_readback(&before_readback.scope))
            .unwrap();
        let after_readback = fixture.service().current_status();
        assert!(matches!(
            after_readback.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            }
        ));
        assert!(!after_readback.readback_required);
        assert!(after_readback.unknown_operation_ids.is_empty());
        assert_eq!(after_readback.unknown_operation_count, 0);
        assert_eq!(
            after_readback.effect_certainty,
            ModuleEffectCertainty::Unknown
        );
        assert!(!aggregate_has_obligations(fixture.service()));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn registry_obligation_projection_keeps_incomplete_readback_blocked() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        fixture.prove_departure().await;
        let status = fixture.service().current_status();
        let mut readback = empty_operation_readback(&status.scope);
        readback.complete = false;

        assert_eq!(
            fixture.service().apply_readback(readback).unwrap_err().code,
            "MODULE_READBACK_INCOMPLETE"
        );
        assert!(fixture.service().current_status().readback_required);
        assert!(aggregate_has_obligations(fixture.service()));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn registry_obligation_projection_keeps_mismatched_readback_blocked() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        fixture.prove_departure().await;
        let status = fixture.service().current_status();
        let mut readback = empty_operation_readback(&status.scope);
        readback.scope.binding_id = "other-binding".to_owned();

        assert_eq!(
            fixture.service().apply_readback(readback).unwrap_err().code,
            "MODULE_READBACK_INCOMPLETE"
        );
        assert!(fixture.service().current_status().readback_required);
        assert!(aggregate_has_obligations(fixture.service()));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn registry_obligation_projection_keeps_unresolved_operations_blocked() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        fixture.prove_departure().await;
        let status = fixture.service().current_status();
        fixture
            .service()
            .apply_readback(OperationReadback {
                scope: status.scope.clone(),
                complete: true,
                operations: vec![OperationSnapshot {
                    operation_id: "fixture-operation".to_owned(),
                    binding_id: status.scope.binding_id,
                    generation: status.scope.generation,
                    state: "outcome_unknown".to_owned(),
                }],
            })
            .unwrap();

        let status = fixture.service().current_status();
        assert!(!status.readback_required);
        assert_eq!(status.unknown_operation_count, 1);
        assert_eq!(
            status.unknown_operation_ids,
            vec!["fixture-operation".to_owned()]
        );
        assert_eq!(status.effect_certainty, ModuleEffectCertainty::Unknown);
        assert!(aggregate_has_obligations(fixture.service()));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn registry_obligation_projection_keeps_unproved_departure_blocked() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        let token = fixture.owner_receipt["token"].as_str().unwrap();
        assert!(matches!(
            observe_process_family(&fixture.owner_receipt["process"], token),
            ProcessFamilyObservation::Retained
        ));

        let scope = fixture.service().scope.clone();
        fixture
            .service()
            .apply_readback(empty_operation_readback(&scope))
            .unwrap();
        assert!(matches!(
            fixture.service().current_status().lifecycle,
            LifecycleState::Starting { .. }
        ));
        assert!(aggregate_has_obligations(fixture.service()));
        fixture.finish_without_proving_departure().await;
    }

    #[cfg(any(windows, target_os = "linux"))]
    fn set_worker_status(
        service: &Service,
        owner: &ProcessIdentity,
        worker: &ProcessIdentity,
        boot_id: &str,
        lifecycle: LifecycleState,
    ) {
        service.update_status(|status| {
            status.lifecycle = lifecycle;
            status.owner = Some(owner.clone());
            status.worker = Some(worker.clone());
            status.worker_boot_id = Some(boot_id.to_owned());
        });
    }

    fn retain_hello_fixture_operations(service: &Service) {
        service
            .apply_readback(OperationReadback {
                scope: service.scope.clone(),
                complete: true,
                operations: [
                    ("queued-input", "queued"),
                    ("pending-result", "sending"),
                    ("pending-refresh", "outcome_unknown"),
                ]
                .into_iter()
                .map(|(operation_id, state)| OperationSnapshot {
                    operation_id: operation_id.to_owned(),
                    binding_id: service.scope.binding_id.clone(),
                    generation: service.scope.generation,
                    state: state.to_owned(),
                })
                .collect(),
            })
            .unwrap();
    }

    #[cfg(any(windows, target_os = "linux"))]
    fn terminal_retention_lease(service: &Arc<Service>) -> DemandLease {
        let demand_key = "terminal-retention-demand".to_owned();
        lock(&service.demands).insert(demand_key.clone(), 1);
        DemandLease {
            service: service.clone(),
            demand_key,
            released: false,
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    fn assert_terminal_retention(before: &SupervisorStatus, after: &SupervisorStatus) {
        // Exact predicate used by the connected Core retirement wait in
        // module_prelaunch_observation_tests.rs. Do not substitute helper exit,
        // demand absence, a NotStarted result, or a synthetic empty-family flag.
        assert!(matches!(
            &after.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            } | LifecycleState::Completed { .. }
        ));
        assert_eq!(
            serde_json::to_value(&after.lifecycle).unwrap(),
            serde_json::to_value(&before.lifecycle).unwrap(),
        );
        assert_eq!(after.worker_boot_id, before.worker_boot_id);
        assert!(after.worker_boot_id.is_some());
        assert!(after.owner.is_some() && after.worker.is_some());
        assert_eq!(
            serde_json::to_value(&after.owner).unwrap(),
            serde_json::to_value(&before.owner).unwrap(),
        );
        assert_eq!(
            serde_json::to_value(&after.worker).unwrap(),
            serde_json::to_value(&before.worker).unwrap(),
        );
        assert_eq!(after.effect_certainty, before.effect_certainty);
        assert_eq!(after.readback_required, before.readback_required);
        assert_eq!(after.unknown_operation_ids, before.unknown_operation_ids);
        assert_eq!(
            after.unknown_operation_count,
            before.unknown_operation_count
        );
        assert_eq!(
            after.unknown_operation_ids_truncated,
            before.unknown_operation_ids_truncated
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    async fn terminal_retention_monitor_exit(
        fixture: &mut ConnectedOwnerFixture,
        restart_needed: bool,
    ) {
        let service = fixture.startup.service.clone();
        service
            .write_restart_history(&VecDeque::from([unix_time_ms().unwrap()]))
            .unwrap();
        // Production captures this exact query while the helper is live. Keep
        // it before releasing the fixture, including on Windows.
        service.remember_owner_family_query(
            &fixture.owner_receipt["process"],
            fixture.owner_receipt["token"].as_str().unwrap(),
        );
        let mut helper = fixture.helper.take().expect("exact helper Child");
        fixture.release();
        let attempt = time::timeout(
            Duration::from_secs(5),
            service.monitor_owner_helper(
                &mut helper,
                &fixture.boot_id,
                &fixture.helper_identity,
                None,
                Instant::now() + Duration::from_secs(5),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(attempt.exit.success());
        assert!(attempt.safe_to_replace && attempt.family_departure_proven);
        service.record_owner_helper_exit(
            &attempt,
            &fixture.helper_identity,
            &fixture.boot_id,
            1,
            restart_needed,
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_completed_survives_no_demand_lease_drop() {
        let mut fixture = ConnectedOwnerFixture::new_successful().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        let lease = terminal_retention_lease(&service);
        drop(lease);
        terminal_retention_monitor_exit(&mut fixture, false).await;
        let before = service.current_status();
        assert!(matches!(before.lifecycle, LifecycleState::Completed { .. }));
        let launch_before = fs::read(service.state_dir.join("launch-attempt.json")).unwrap();
        let history_before = fs::read(service.state_dir.join("restart-history.json")).unwrap();
        time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        assert_terminal_retention(&before, &service.current_status());
        assert!(!service.has_demand());
        assert!(aggregate_has_obligations(&service));
        assert!(!service.can_replace_descriptor().await);
        assert_eq!(
            fs::read(service.state_dir.join("launch-attempt.json")).unwrap(),
            launch_before
        );
        assert_eq!(
            fs::read(service.state_dir.join("restart-history.json")).unwrap(),
            history_before
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_monitored_exit_survives_sampled_restart_then_lease_drop() {
        let mut fixture = ConnectedOwnerFixture::new_successful().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        let lease = terminal_retention_lease(&service);
        let restart_needed = service.has_demand();
        assert!(restart_needed);
        terminal_retention_monitor_exit(&mut fixture, restart_needed).await;
        let before = service.current_status();
        assert!(matches!(
            before.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            }
        ));
        drop(lease);
        time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        assert_terminal_retention(&before, &service.current_status());
        assert!(!service.has_demand());
        assert!(service.readback_required.load(Ordering::Acquire));
        assert!(aggregate_has_obligations(&service));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_recovered_owner_is_not_erased_after_custody_reconciliation() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        let lease = terminal_retention_lease(&service);
        service
            .write_restart_history(&VecDeque::from([unix_time_ms().unwrap()]))
            .unwrap();
        let helper = fixture.helper.take().expect("exact helper Child");
        service
            .retain_helper_child_custody(
                helper,
                Some(fixture.helper_identity.clone()),
                Some(fixture.boot_id.clone()),
                "MODULE_WORKER_EXITED",
            )
            .await;
        service.remember_owner_family_query(
            &fixture.owner_receipt["process"],
            fixture.owner_receipt["token"].as_str().unwrap(),
        );
        fixture.release();
        {
            let mut custody = service.helper_child_custody.lock().await;
            let retained = custody.as_mut().unwrap();
            assert_eq!(retained.child.wait().await.unwrap().code(), Some(23));
        }
        drop(lease);
        let unknown_before = service.current_status().unknown_operation_ids;
        time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        let after = service.current_status();
        // run_lifecycle itself must consume genuine family departure and publish
        // exit_proven before its no-demand branch, not a test-created terminal.
        assert!(matches!(
            after.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            }
        ));
        assert_eq!(
            after.worker_boot_id.as_deref(),
            Some(fixture.boot_id.as_str())
        );
        assert!(same_process_identity(
            after.owner.as_ref().unwrap(),
            &fixture.helper_identity
        ));
        assert!(same_process_identity(
            after.worker.as_ref().unwrap(),
            &fixture.worker_identity
        ));
        assert_eq!(after.unknown_operation_ids, unknown_before);
        assert_eq!(after.unknown_operation_count, 2);
        assert!(after.readback_required && service.readback_required.load(Ordering::Acquire));
        assert_eq!(after.effect_certainty, ModuleEffectCertainty::Unknown);
        assert!(service.helper_child_custody.lock().await.is_none());
        assert!(aggregate_has_obligations(&service));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_max_starts_one_preserves_exit_and_current_budget_flags() {
        let mut fixture = ConnectedOwnerFixture::new_successful().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        let lease = terminal_retention_lease(&service);
        terminal_retention_monitor_exit(&mut fixture, true).await;
        let before = service.current_status();
        assert_eq!(service.descriptor.restart.max_starts, 1);
        let launch_before = fs::read(service.state_dir.join("launch-attempt.json")).unwrap();
        time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        let after = service.current_status();
        assert_terminal_retention(&before, &after);
        assert!(service.has_demand());
        assert_eq!(after.restart.starts_in_window, 1);
        assert!(after.restart.exhausted);
        assert_eq!(
            fs::read(service.state_dir.join("launch-attempt.json")).unwrap(),
            launch_before
        );
        assert_eq!(service.read_restart_history().unwrap().len(), 1);
        assert!(!service.can_replace_descriptor().await);
        service.quarantine("MODULE_OBSERVATION_REJECTED").unwrap();
        assert_terminal_retention(&before, &service.current_status());
        assert!(service.scope_quarantined.load(Ordering::Acquire));
        assert_eq!(
            service.ensure_runner().await.unwrap_err().code,
            "MODULE_ISOLATED"
        );
        drop(lease);
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_new_demand_during_runner_cleanup_rechecks_budget() {
        let mut fixture = ConnectedOwnerFixture::new_successful().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        let prior_lease = terminal_retention_lease(&service);
        terminal_retention_monitor_exit(&mut fixture, true).await;
        drop(prior_lease);
        let before = service.current_status();
        let exit = time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        assert!(matches!(exit, LifecycleRunExit::WaitingForDemand));
        assert!(!service.current_status().restart.exhausted);
        // The new lease is admitted while the prior runner is finishing its
        // no-demand path. Cleanup must schedule the ordinary gated runner even
        // though the lifecycle truth stays terminal rather than WaitingForDemand.
        let lease = terminal_retention_lease(&service);
        let mut statuses = service.status.subscribe();
        service.after_runner_exit(exit).await;
        time::timeout(Duration::from_secs(5), async {
            loop {
                if statuses.borrow().restart.exhausted {
                    break;
                }
                statuses.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let runner = service.runner.lock().await.take();
        if let Some(runner) = runner {
            runner.await.unwrap();
        }
        assert_terminal_retention(&before, &service.current_status());
        assert_eq!(service.current_status().restart.starts_in_window, 1);
        assert_eq!(service.read_restart_history().unwrap().len(), 1);
        assert!(service.has_demand());
        drop(lease);
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn terminal_retention_actual_boot_allocation_resets_only_attempt_identity() {
        let mut fixture = ConnectedOwnerFixture::new_successful().await;
        let service = fixture.startup.service.clone();
        retain_hello_fixture_operations(&service);
        terminal_retention_monitor_exit(&mut fixture, true).await;
        let before = service.current_status();
        // Exercise the actual allocator at the unchanged production launch
        // boundary. This unit call is not permission to bypass launch fences.
        let boot = service.allocate_worker_boot();
        let starting = service.current_status();
        assert_ne!(starting.worker_boot_id, before.worker_boot_id);
        assert_eq!(starting.worker_boot_id.as_deref(), Some(boot.as_str()));
        assert!(
            matches!(&starting.lifecycle, LifecycleState::Starting { boot_id } if boot_id == &boot)
        );
        assert!(starting.owner.is_none() && starting.worker.is_none());
        assert_eq!(starting.unknown_operation_ids, before.unknown_operation_ids);
        assert_eq!(
            starting.unknown_operation_count,
            before.unknown_operation_count
        );
        assert_eq!(starting.readback_required, before.readback_required);
        assert!(service.readback_required.load(Ordering::Acquire));
        assert!(aggregate_has_obligations(&service));
    }

    #[tokio::test]
    async fn terminal_retention_never_promotes_unproved_exit_or_owner_uncertainty() {
        let fixture = StartupFixture::new();
        retain_hello_fixture_operations(&fixture.service);
        // The complete readback above legitimately clears its readback fence.
        // Model a subsequent unproved exit, as the production failure path does,
        // before verifying that administrative polling preserves uncertainty.
        fixture
            .service
            .readback_required
            .store(true, Ordering::Release);
        fixture.service.update_status(|status| {
            status.readback_required = true;
            status.lifecycle = LifecycleState::ProcessExited {
                exit_code: Some(0),
                exit_proven: false,
            };
        });
        fixture.service.clone().run_lifecycle().await;
        let status = fixture.service.current_status();
        assert!(!matches!(
            status.lifecycle,
            LifecycleState::ProcessExited {
                exit_proven: true,
                ..
            } | LifecycleState::Completed { .. }
        ));
        assert!(status.readback_required);
        assert_eq!(status.unknown_operation_count, 2);
        fixture
            .service
            .update_status(|status| status.lifecycle = LifecycleState::OwnerIdentityUnknown);
        fixture.service.clone().run_lifecycle().await;
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::OwnerIdentityUnknown
        ));
        assert!(fixture.service.readback_required.load(Ordering::Acquire));
    }

    #[test]
    fn historical_hello_new_boot_failure_projects_new_attempt_before_confirmation() {
        let fixture = StartupFixture::new();
        retain_hello_fixture_operations(&fixture.service);
        let boot_a = fixture.service.allocate_worker_boot();
        assert!(
            fixture
                .service
                .record_module_hello_failure(&boot_a, "MODULE_WORKER_IDENTITY_UNKNOWN")
                .unwrap()
        );
        let event_a = crate::ModuleSupervisorObservation::from_status(
            &fixture.service.current_status(),
            "fixture-actor",
            1,
        )
        .unwrap();
        assert_eq!(event_a.boot_id.as_deref(), Some(boot_a.as_str()));
        assert_eq!(
            event_a.error_code.as_deref(),
            Some("MODULE_WORKER_IDENTITY_UNKNOWN")
        );

        let boot_b = fixture.service.allocate_worker_boot();
        assert_ne!(boot_a, boot_b);
        let starting = fixture.service.current_status();
        assert!(starting.hello_failure.is_none());
        assert!(starting.last_failure.is_none());
        assert!(starting.failure_stage.is_none());
        fixture.service.record_failure(
            "MODULE_OWNER_HELPER_MISMATCH",
            "current helper failed before worker confirmation",
        );
        fixture.service.update_status(|status| {
            status.lifecycle = LifecycleState::OwnerIdentityUnknown;
        });
        fixture
            .service
            .reconcile_retained_module_hello(&boot_a)
            .unwrap();

        let failed = fixture.service.current_status();
        assert_eq!(failed.worker_boot_id.as_deref(), Some(boot_b.as_str()));
        assert!(failed.hello_failure.is_none());
        assert!(failed.owner.is_none());
        assert!(failed.worker.is_none());
        assert!(failed.readback_required);
        assert!(fixture.service.readback_required.load(Ordering::Acquire));
        assert!(lock(&fixture.service.confirmed_ready).is_none());
        let event_b =
            crate::ModuleSupervisorObservation::from_status(&failed, "fixture-actor", 2).unwrap();
        assert_eq!(event_b.boot_id.as_deref(), Some(boot_b.as_str()));
        assert_eq!(event_b.event_id, format!("{boot_b}:fixture-actor:2"));
        assert_eq!(
            event_b.error_code.as_deref(),
            Some("MODULE_OWNER_HELPER_MISMATCH")
        );
        assert_eq!(event_b.stage, Some(ModuleFailureStage::Owner));
        assert_eq!(event_b.effect_certainty, ModuleEffectCertainty::Unknown);
        assert_eq!(event_b.unknown_operation_count, 2);
        assert_eq!(
            event_b.unknown_operation_ids,
            ["pending-refresh", "pending-result"]
        );
    }

    #[test]
    fn historical_hello_without_new_receipts_preserves_attempt_and_primary_failure() {
        let fixture = StartupFixture::new();
        retain_hello_fixture_operations(&fixture.service);
        for worker_boot in [Some("new-boot"), Some("old-boot"), None] {
            fixture.service.update_status(|status| {
                status.lifecycle = LifecycleState::Starting {
                    boot_id: "new-boot".to_owned(),
                };
                status.worker_boot_id = worker_boot.map(str::to_owned);
                status.failure_stage = Some(ModuleFailureStage::Owner);
                status.last_failure = Some(FailureSummary {
                    code: "MODULE_START_RECEIPT_TIMEOUT".to_owned(),
                    detail: "current helper has no startup receipt".to_owned(),
                });
                status.readback_required = true;
            });
            fixture
                .service
                .readback_required
                .store(true, Ordering::Release);
            let before = serde_json::to_value(fixture.service.current_status()).unwrap();

            fixture
                .service
                .reconcile_retained_module_hello("old-boot")
                .unwrap();
            assert!(
                !fixture
                    .service
                    .record_module_hello_failure("old-boot", "MODULE_WORKER_BOOT_MISMATCH")
                    .unwrap()
            );
            assert_eq!(
                serde_json::to_value(fixture.service.current_status()).unwrap(),
                before
            );
            assert!(fixture.service.readback_required.load(Ordering::Acquire));
            assert!(lock(&fixture.service.confirmed_ready).is_none());
        }
    }

    #[test]
    fn historical_hello_matching_boot_preserves_ineligible_owner_and_lifecycle_holds() {
        let fixture = StartupFixture::new();
        retain_hello_fixture_operations(&fixture.service);
        for lifecycle in [
            LifecycleState::OwnerIdentityUnknown,
            LifecycleState::OwnerGroupRetained {
                owner_pid: std::process::id(),
                reason: OwnerRetainedReason::OwnerFamilyObservationUnknown,
            },
            LifecycleState::ProcessIdentityUnknown { pid: None },
            LifecycleState::RestartBackoff { delay_ms: 1 },
            LifecycleState::WaitingForKernel,
            LifecycleState::Completed { exit_code: None },
        ] {
            fixture.service.update_status(|status| {
                status.lifecycle = lifecycle;
                status.worker_boot_id = Some("current-boot".to_owned());
                status.failure_stage = Some(ModuleFailureStage::Owner);
                status.last_failure = Some(FailureSummary {
                    code: "MODULE_OWNER_FAMILY_OBSERVATION_UNKNOWN".to_owned(),
                    detail: "current owner proof is unresolved".to_owned(),
                });
            });
            let before = serde_json::to_value(fixture.service.current_status()).unwrap();
            fixture
                .service
                .reconcile_retained_module_hello("current-boot")
                .unwrap();
            assert!(
                !fixture
                    .service
                    .record_module_hello_failure("current-boot", "MODULE_WORKER_IDENTITY_UNKNOWN")
                    .unwrap()
            );
            assert_eq!(
                serde_json::to_value(fixture.service.current_status()).unwrap(),
                before
            );
            assert!(!fixture.service.readback_required.load(Ordering::Acquire));
            assert!(lock(&fixture.service.confirmed_ready).is_none());
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn historical_hello_matching_current_worker_confirms_without_releasing_unknown_ops() {
        let fixture = StartupFixture::new();
        let worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        retain_hello_fixture_operations(&fixture.service);
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            "current-boot",
            LifecycleState::Starting {
                boot_id: "current-boot".to_owned(),
            },
        );

        fixture
            .service
            .reconcile_retained_module_hello("current-boot")
            .unwrap();
        let ready = fixture.service.current_status();
        assert!(matches!(
            ready.lifecycle,
            LifecycleState::ProcessRunning { ref boot_id } if boot_id == "current-boot"
        ));
        assert_eq!(ready.unknown_operation_count, 2);
        assert_eq!(
            ready.unknown_operation_ids,
            ["pending-refresh", "pending-result"]
        );
        assert!(!ready.readback_required);
        assert!(ready.hello_failure.is_none());
        assert!(!fixture.service.can_replace_descriptor().await);
        let before = serde_json::to_value(&ready).unwrap();
        fixture
            .service
            .reconcile_retained_module_hello("old-boot")
            .unwrap();
        assert_eq!(
            fixture
                .service
                .confirm_module_hello("old-boot")
                .unwrap_err()
                .code,
            "MODULE_WORKER_BOOT_MISMATCH"
        );
        fixture
            .service
            .reconcile_retained_module_hello("current-boot")
            .unwrap();
        assert_eq!(
            serde_json::to_value(fixture.service.current_status()).unwrap(),
            before
        );
    }

    #[test]
    fn historical_hello_current_failure_retains_exact_code_and_unknown_ops() {
        let fixture = StartupFixture::new();
        retain_hello_fixture_operations(&fixture.service);
        fixture.service.update_status(|status| {
            status.lifecycle = LifecycleState::Starting {
                boot_id: "current-boot".to_owned(),
            };
            status.worker_boot_id = Some("current-boot".to_owned());
        });
        assert_eq!(
            fixture
                .service
                .reconcile_retained_module_hello("current-boot")
                .unwrap_err()
                .code,
            "MODULE_WORKER_IDENTITY_UNKNOWN"
        );
        let status = fixture.service.current_status();
        assert!(matches!(
            status.lifecycle,
            LifecycleState::Starting { ref boot_id } if boot_id == "current-boot"
        ));
        assert!(status.readback_required);
        assert!(fixture.service.readback_required.load(Ordering::Acquire));
        assert!(lock(&fixture.service.confirmed_ready).is_none());
        let event =
            crate::ModuleSupervisorObservation::from_status(&status, "fixture-actor", 1).unwrap();
        assert_eq!(event.boot_id.as_deref(), Some("current-boot"));
        assert_eq!(
            event.error_code.as_deref(),
            Some("MODULE_WORKER_IDENTITY_UNKNOWN")
        );
        assert_eq!(event.effect_certainty, ModuleEffectCertainty::Unknown);
        assert_eq!(event.unknown_operation_count, 2);
        assert_eq!(
            event.unknown_operation_ids,
            ["pending-refresh", "pending-result"]
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn confirmed_ready_is_retained_before_worker_departure_between_status_polls() {
        let fixture = StartupFixture::new();
        let mut worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        let boot_id = "lost-ready-fixture";
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            boot_id,
            LifecycleState::Starting {
                boot_id: boot_id.to_owned(),
            },
        );
        fixture.service.confirm_module_hello(boot_id).unwrap();
        fixture.service.confirm_module_hello(boot_id).unwrap();
        worker.stop();
        assert!(
            !fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&worker.identity))
                .unwrap()
        );

        let statuses = fixture.service.observation_statuses();
        assert_eq!(statuses.len(), 2);
        let ready =
            crate::ModuleSupervisorObservation::from_status(&statuses[0], "fixture-actor", 1)
                .unwrap();
        let retained =
            crate::ModuleSupervisorObservation::from_status(&statuses[1], "fixture-actor", 2)
                .unwrap();
        assert_eq!(ready.phase, crate::ModuleSupervisorPhase::Ready);
        assert_eq!(retained.phase, crate::ModuleSupervisorPhase::OwnerRetained);
        assert_eq!(retained.error_code.as_deref(), Some("MODULE_WORKER_EXITED"));
        assert_eq!(ready.boot_id, retained.boot_id);
        assert_eq!(ready.scope, retained.scope);
        // Failed delivery leaves the immutable Ready available on the next poll.
        assert_eq!(fixture.service.observation_statuses().len(), 2);
        fixture.service.acknowledge_ready_observation(&statuses[0]);
        assert_eq!(fixture.service.observation_statuses().len(), 1);
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::OwnerGroupRetained {
                reason: OwnerRetainedReason::WorkerExited,
                ..
            }
        ));
        assert_eq!(
            fixture
                .service
                .confirm_module_hello(boot_id)
                .unwrap_err()
                .code,
            "MODULE_WORKER_EXITED"
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn unqueued_ready_cannot_be_overwritten_by_another_boot() {
        let fixture = StartupFixture::new();
        let worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            "first-boot",
            LifecycleState::Starting {
                boot_id: "first-boot".to_owned(),
            },
        );
        fixture.service.confirm_module_hello("first-boot").unwrap();
        let ready = fixture.service.observation_statuses().remove(0);
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            "second-boot",
            LifecycleState::Starting {
                boot_id: "second-boot".to_owned(),
            },
        );
        assert_eq!(
            fixture
                .service
                .confirm_module_hello("second-boot")
                .unwrap_err()
                .code,
            "MODULE_READY_OBSERVATION_PENDING"
        );
        assert_eq!(
            fixture.service.observation_statuses()[0]
                .worker_boot_id
                .as_deref(),
            Some("first-boot")
        );
        fixture.service.acknowledge_ready_observation(&ready);
        fixture.service.confirm_module_hello("second-boot").unwrap();
        assert_eq!(
            fixture.service.observation_statuses()[0]
                .worker_boot_id
                .as_deref(),
            Some("second-boot")
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn rejected_hello_never_retains_ready() {
        let fixture = StartupFixture::new();
        let worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            "exact-boot",
            LifecycleState::Starting {
                boot_id: "exact-boot".to_owned(),
            },
        );
        assert_eq!(
            fixture
                .service
                .confirm_module_hello("wrong-boot")
                .unwrap_err()
                .code,
            "MODULE_WORKER_BOOT_MISMATCH"
        );
        let mut changed_worker = worker.identity.clone();
        changed_worker.image["sha256"] = serde_json::json!("wrong-image");
        set_worker_status(
            &fixture.service,
            &owner,
            &changed_worker,
            "exact-boot",
            LifecycleState::Starting {
                boot_id: "exact-boot".to_owned(),
            },
        );
        assert!(fixture.service.confirm_module_hello("exact-boot").is_err());
        assert!(lock(&fixture.service.confirmed_ready).is_none());
        assert_eq!(fixture.service.observation_statuses().len(), 1);
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn live_owner_retained_worker_stays_starting_until_hello_and_ready_survives_next_poll() {
        let fixture = StartupFixture::new();
        let mut worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        let boot_id = "worker-live-fixture";
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            boot_id,
            LifecycleState::OwnerGroupRetained {
                owner_pid: owner.pid,
                reason: OwnerRetainedReason::HelloUnconfirmed,
            },
        );

        assert!(
            fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&worker.identity))
                .unwrap()
        );
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::Starting { boot_id: current } if current == boot_id
        ));

        fixture.service.confirm_module_hello(boot_id).unwrap();
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::ProcessRunning { boot_id: current } if current == boot_id
        ));
        assert!(
            fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&worker.identity))
                .unwrap()
        );
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::ProcessRunning { boot_id: current } if current == boot_id
        ));
        worker.stop();
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn mismatched_live_worker_stays_nonready() {
        let fixture = StartupFixture::new();
        let previous_worker = WorkerProcessFixture::new();
        let observed_worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        let boot_id = "worker-mismatch-fixture";
        set_worker_status(
            &fixture.service,
            &owner,
            &previous_worker.identity,
            boot_id,
            LifecycleState::ProcessRunning {
                boot_id: boot_id.to_owned(),
            },
        );

        assert!(
            fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&observed_worker.identity))
                .unwrap()
        );
        let status = fixture.service.current_status();
        assert!(matches!(
            status.lifecycle,
            LifecycleState::Starting { boot_id: current } if current == boot_id
        ));
        assert!(
            status
                .worker
                .as_ref()
                .is_some_and(|worker| { same_process_identity(worker, &observed_worker.identity) })
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn same_pid_and_birth_with_changed_image_requires_new_hello() {
        let fixture = StartupFixture::new();
        let worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        let boot_id = "worker-image-mismatch-fixture";
        let mut previous = worker.identity.clone();
        previous.image["sha256"] = serde_json::json!("different-previous-image");
        assert_eq!(previous.pid, worker.identity.pid);
        assert_eq!(previous.birth, worker.identity.birth);
        assert_ne!(previous.image, worker.identity.image);
        set_worker_status(
            &fixture.service,
            &owner,
            &previous,
            boot_id,
            LifecycleState::ProcessRunning {
                boot_id: boot_id.to_owned(),
            },
        );
        assert!(!worker_is_hello_confirmed(
            &fixture.service.current_status(),
            boot_id,
            &worker.identity,
        ));
        assert!(
            fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&worker.identity))
                .unwrap()
        );
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::Starting { boot_id: current } if current == boot_id
        ));
        fixture.service.confirm_module_hello(boot_id).unwrap();
        assert!(worker_is_hello_confirmed(
            &fixture.service.current_status(),
            boot_id,
            &worker.identity,
        ));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn dead_worker_stays_owner_group_retained() {
        let fixture = StartupFixture::new();
        let mut worker = WorkerProcessFixture::new();
        let owner = capture_identity(std::process::id()).unwrap();
        let boot_id = "worker-dead-fixture";
        set_worker_status(
            &fixture.service,
            &owner,
            &worker.identity,
            boot_id,
            LifecycleState::ProcessRunning {
                boot_id: boot_id.to_owned(),
            },
        );
        worker.stop();

        assert!(
            !fixture
                .service
                .observe_owned_worker(boot_id, &owner, Some(&worker.identity))
                .unwrap()
        );
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::OwnerGroupRetained { owner_pid, reason: OwnerRetainedReason::WorkerExited } if owner_pid == owner.pid
        ));
        assert_eq!(
            fixture
                .service
                .confirm_module_hello(boot_id)
                .unwrap_err()
                .code,
            "MODULE_WORKER_EXITED"
        );

        assert!(
            !fixture
                .service
                .observe_owned_worker(boot_id, &owner, None)
                .unwrap()
        );
        assert!(matches!(
            fixture.service.current_status().lifecycle,
            LifecycleState::OwnerGroupRetained { owner_pid, reason: OwnerRetainedReason::WorkerIdentityUnknown } if owner_pid == owner.pid
        ));
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    #[ignore = "spawned by the supervisor retained-worker lifecycle regressions"]
    fn worker_process_fixture_waits_for_release() {
        if std::env::var_os("ELIOT_SWARM_SUPERVISOR_WORKER_FIXTURE").is_none() {
            return;
        }
        let _grandchild = match (
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_RELEASE_PATH"),
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_READY_PATH"),
        ) {
            (Some(release_path), Some(ready_path)) => {
                let child = StdCommand::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "supervisor::tests::grandchild_process_fixture_waits_for_release",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_FIXTURE", "1")
                    .env(
                        "ELIOT_SWARM_SUPERVISOR_GRANDCHILD_RELEASE_PATH",
                        &release_path,
                    )
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .expect("start actual grandchild process fixture");
                fs::write(ready_path, child.id().to_string()).unwrap();
                Some(child)
            }
            _ => None,
        };
        if let Some(path) = std::env::var_os("ELIOT_SWARM_SUPERVISOR_WORKER_RELEASE_PATH") {
            let path = PathBuf::from(path);
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while !path.exists() {
                if std::time::Instant::now() >= deadline {
                    return;
                }
                thread::sleep(Duration::from_millis(10));
            }
            return;
        }
        let mut byte = [0_u8; 1];
        let _ = std::io::stdin().read_exact(&mut byte);
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    #[ignore = "spawned by the supervisor owner-worker-grandchild custody regression"]
    fn grandchild_process_fixture_waits_for_release() {
        if std::env::var_os("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_FIXTURE").is_none() {
            return;
        }
        let path = PathBuf::from(
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_GRANDCHILD_RELEASE_PATH")
                .expect("grandchild release path"),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            if std::time::Instant::now() >= deadline {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[tokio::test]
    async fn startup_fresh_scope_reaches_executable_gate_and_retains_budget() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        assert!(!service.state_dir.exists());
        lock(&service.demands).insert("fixture-demand".to_owned(), 1);
        time::timeout(Duration::from_secs(5), service.clone().run_lifecycle())
            .await
            .unwrap();
        let status = service.current_status();
        assert_eq!(
            status.last_failure.unwrap().code,
            "MODULE_EXECUTABLE_UNAVAILABLE"
        );
        assert!(status.restart.exhausted);
        assert_eq!(service.read_restart_history().unwrap().len(), 1);
        assert!(service.state_dir.join("module.lock").is_file());
        assert!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap()
                .is_none()
        );
        assert!(!service.state_dir.join("launch-attempt.json").exists());
        assert!(!service.state_dir.join("owner.json").exists());
    }

    #[tokio::test]
    async fn startup_valid_budget_alone_does_not_invent_prior_owner() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        fixture.prepare_accounting();
        service
            .write_restart_history(&VecDeque::from([unix_time_ms().unwrap()]))
            .unwrap();
        assert!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn startup_orphan_receipts_and_unknown_entries_remain_closed() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        fixture.prepare_accounting();
        service.write_restart_history(&VecDeque::new()).unwrap();
        for name in [
            "worker.json",
            "launch-result.json",
            "launch-orphan.json",
            "module-host-connection.json",
            "unknown",
        ] {
            let path = service.state_dir.join(name);
            fs::write(&path, b"{}").unwrap();
            assert_eq!(
                service
                    .wait_for_prior_owner_to_depart()
                    .await
                    .unwrap_err()
                    .code,
                "MODULE_OWNER_IDENTITY_MISSING"
            );
            fs::remove_file(path).unwrap();
        }
    }

    #[tokio::test]
    async fn startup_budget_from_another_scope_remains_closed() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        fixture.prepare_accounting();
        service.write_restart_history(&VecDeque::new()).unwrap();
        let path = service.state_dir.join("restart-history.json");
        let mut history: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        history["scope"]["generation"] = serde_json::json!(2);
        fs::write(path, serde_json::to_vec(&history).unwrap()).unwrap();
        assert_eq!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap_err()
                .code,
            "MODULE_RESTART_HISTORY_INVALID"
        );
    }

    #[tokio::test]
    async fn startup_launch_intent_without_helper_identity_remains_unknown() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        fixture.prepare_accounting();
        service.write_restart_history(&VecDeque::new()).unwrap();
        service
            .write_launch_attempt(&Uuid::new_v4().to_string(), None)
            .unwrap();
        assert_eq!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap_err()
                .code,
            "MODULE_LAUNCH_INTENT_UNKNOWN"
        );
    }

    #[tokio::test]
    async fn startup_non_directory_scope_component_is_refused_before_accounting() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        let first = service
            .state_dir
            .strip_prefix(&fixture.root)
            .unwrap()
            .components()
            .next()
            .unwrap();
        fs::write(fixture.root.join(first), b"foreign").unwrap();
        lock(&service.demands).insert("fixture-demand".to_owned(), 1);
        service.clone().run_lifecycle().await;
        assert_eq!(
            service.current_status().last_failure.unwrap().code,
            "MODULE_STATE_PATH_UNSAFE"
        );
        assert!(!service.state_dir.exists());
    }

    #[tokio::test]
    async fn startup_unmarked_bookkeeping_is_not_adopted() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        service.prepare_state_dir().unwrap();
        service.write_restart_history(&VecDeque::new()).unwrap();
        lock(&service.demands).insert("fixture-demand".to_owned(), 1);
        service.clone().run_lifecycle().await;
        assert_eq!(
            service.current_status().last_failure.unwrap().code,
            "FOREIGN_STATE_DIRECTORY"
        );
        assert!(!service.state_dir.join("module.lock").exists());
        assert!(!service.state_dir.join("launch-attempt.json").exists());
        assert!(service.read_restart_history().unwrap().is_empty());
    }

    #[tokio::test]
    async fn startup_incomplete_marker_with_budget_is_not_repaired() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        service.prepare_state_dir().unwrap();
        let marker = service.state_dir.join("module.lock");
        fs::write(&marker, b"ELIOT_SWARM_MODULE_").unwrap();
        service.write_restart_history(&VecDeque::new()).unwrap();
        assert_eq!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap_err()
                .code,
            "FOREIGN_STATE_DIRECTORY"
        );
        assert_eq!(fs::read(marker).unwrap(), b"ELIOT_SWARM_MODULE_");
    }

    #[tokio::test]
    async fn startup_busy_marker_does_not_grant_ownerless_start() {
        let fixture = StartupFixture::new();
        let service = &fixture.service;
        fixture.prepare_accounting();
        service.write_restart_history(&VecDeque::new()).unwrap();
        let _marker =
            swarm_process::module_owner::acquire_module_state_marker(&service.state_dir).unwrap();
        assert_eq!(
            service
                .wait_for_prior_owner_to_depart()
                .await
                .unwrap_err()
                .code,
            "MODULE_OWNER_ACTIVE"
        );
        assert!(!service.state_dir.join("launch-attempt.json").exists());
    }

    #[cfg(any(windows, target_os = "linux"))]
    struct App369EmptyRefResolver;

    #[cfg(any(windows, target_os = "linux"))]
    impl swarm_process::module_owner::ProtectedRefResolver for App369EmptyRefResolver {
        fn resolve(
            &self,
            plan: &swarm_process::module_owner::ModuleOwnerPlan,
            _: &std::path::Path,
            _: &Value,
        ) -> swarm_contracts::error::Result<swarm_process::module_owner::ResolvedLaunch> {
            Ok(swarm_process::module_owner::ResolvedLaunch {
                executable: plan.executable.clone(),
                argv: plan.argv.clone(),
                environment: Vec::new(),
            })
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    async fn app369_supervisor_consumes_actual_spawn_receipt_fixture() {
        let fixture = StartupFixture::new_with_current_executable();
        fixture.prepare_accounting();
        let service = &fixture.service;
        let prior_start_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        service
            .write_restart_history(&VecDeque::from([prior_start_ms]))
            .unwrap();
        let state_dir = service.state_dir.clone();
        let boot_id = Uuid::new_v4().to_string();
        let worker_executable = fixture.root.join("app369-invalid-worker.exe");
        fs::write(&worker_executable, b"not a native executable").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(&worker_executable).unwrap().permissions();
            permissions.set_mode(0o600);
            fs::set_permissions(&worker_executable, permissions).unwrap();
        }

        let mut helper = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "supervisor::tests::app369_spawn_failure_owner_helper_process",
                "--ignored",
                "--nocapture",
            ])
            .env("ELIOT_SWARM_SUPERVISOR_APP369_HELPER", "1")
            .env("ELIOT_SWARM_SUPERVISOR_APP369_STATE_DIR", &state_dir)
            .env("ELIOT_SWARM_SUPERVISOR_APP369_WORKER", &worker_executable)
            .env("ELIOT_SWARM_SUPERVISOR_APP369_BOOT_ID", &boot_id)
            .env(
                "ELIOT_SWARM_SUPERVISOR_APP369_BINDING_ID",
                &service.scope.binding_id,
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start APP-369 owner helper fixture");
        let helper_pid = helper.id().expect("owner helper fixture has a PID");
        let identity_deadline = Instant::now() + Duration::from_secs(10);
        let helper_identity = loop {
            if Instant::now() >= identity_deadline {
                let _ = helper.start_kill();
                let _ = helper.wait().await;
                panic!("APP-369 helper identity was not readable before deadline");
            }
            match capture_identity(helper_pid) {
                Ok(identity) => break identity,
                Err(error) if error.code == "PROCESS_GONE" => {
                    if let Some(exit) = helper.try_wait().unwrap() {
                        panic!("APP-369 helper exited before launch admission: {exit}");
                    }
                    time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("APP-369 helper identity failed: {}", error.code),
            }
        };

        service
            .write_launch_attempt(&boot_id, Some(&helper_identity))
            .unwrap();
        let attempt_path = state_dir.join("launch-attempt.json");
        let attempt_before = fs::read(&attempt_path).unwrap();
        let history_path = state_dir.join("restart-history.json");
        let history_before = fs::read(&history_path).ok();
        let restart_starts_before = service.current_status().restart.starts_in_window;
        let restart_history_before = service.read_restart_history().unwrap();
        assert_eq!(restart_history_before.len(), 1);

        let result = service
            .monitor_owner_helper(
                &mut helper,
                &boot_id,
                &helper_identity,
                None,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .expect("consume the helper's produced launch-result receipt");
        assert!(result.exit.success());
        assert_eq!(
            result.failure_code.as_deref(),
            Some("MODULE_OWNER_SPAWN_FAILED")
        );
        assert_eq!(result.failure_stage.as_deref(), Some("spawn"));
        assert_eq!(result.effect_certainty, ModuleEffectCertainty::NotStarted);
        assert!(result.safe_to_replace);
        assert!(!result.family_departure_proven);
        assert!(result.worker.is_none());

        let receipt: Value =
            serde_json::from_slice(&fs::read(state_dir.join("launch-result.json")).unwrap())
                .unwrap();
        assert_eq!(receipt.as_object().unwrap().len(), 16);
        assert_eq!(receipt["version"].as_u64(), Some(1));
        assert_eq!(receipt["boot_id"].as_str(), Some(boot_id.as_str()));
        assert_eq!(receipt["module"].as_str(), Some("fixture"));
        assert_eq!(
            receipt["binding"].as_str(),
            Some(service.scope.binding_id.as_str())
        );
        assert_eq!(receipt["generation"].as_str(), Some("1"));
        assert_eq!(receipt["artifact_id"].as_str(), Some("fixture.adapter"));
        assert_eq!(receipt["artifact_version"].as_str(), Some("1"));
        assert_eq!(receipt["disposition"].as_str(), Some("not_started"));
        assert_eq!(receipt["worker_started"].as_bool(), Some(false));
        assert_eq!(receipt["family_empty"].as_bool(), Some(true));
        assert_eq!(receipt["stage"].as_str(), Some("spawn"));
        assert_eq!(
            receipt["error_code"].as_str(),
            Some("MODULE_OWNER_SPAWN_FAILED")
        );

        let departed = service
            .wait_for_prior_owner_to_depart()
            .await
            .expect("validate the same receipt during prior-owner recovery")
            .expect("the exact owner family is now departed");
        assert_eq!(
            departed.receipt["process"]["pid"].as_u64(),
            Some(u64::from(helper_pid))
        );
        assert_eq!(fs::read(attempt_path).unwrap(), attempt_before);
        assert_eq!(fs::read(history_path).ok(), history_before);
        assert_eq!(
            service.read_restart_history().unwrap(),
            restart_history_before
        );
        assert_eq!(
            service.current_status().restart.starts_in_window,
            restart_starts_before
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn app369_supervisor_consumes_actual_spawn_receipt_without_retry_or_budget_mutation() {
        app369_supervisor_consumes_actual_spawn_receipt_fixture().await;
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    #[ignore = "spawned by app369_supervisor_consumes_actual_spawn_receipt_without_retry_or_budget_mutation"]
    fn app369_spawn_failure_owner_helper_process() {
        if std::env::var_os("ELIOT_SWARM_SUPERVISOR_APP369_HELPER").is_none() {
            return;
        }
        let state_dir = PathBuf::from(
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_APP369_STATE_DIR")
                .expect("APP-369 state directory"),
        );
        let worker_executable = PathBuf::from(
            std::env::var_os("ELIOT_SWARM_SUPERVISOR_APP369_WORKER")
                .expect("APP-369 invalid worker executable"),
        );
        let boot_id =
            std::env::var("ELIOT_SWARM_SUPERVISOR_APP369_BOOT_ID").expect("APP-369 boot ID");
        let binding =
            std::env::var("ELIOT_SWARM_SUPERVISOR_APP369_BINDING_ID").expect("APP-369 binding ID");
        let attempt_path = state_dir.join("launch-attempt.json");
        let deadline = Instant::now() + Duration::from_secs(10);
        while !attempt_path.is_file() {
            assert!(
                Instant::now() < deadline,
                "APP-369 launch attempt was not admitted"
            );
            thread::sleep(Duration::from_millis(10));
        }

        let plan = swarm_process::module_owner::ModuleOwnerPlan {
            state_dir,
            executable: worker_executable,
            argv: Vec::new(),
            protected_refs: Vec::new(),
            environment: Vec::new(),
            module: "fixture".to_owned(),
            binding,
            generation: "1".to_owned(),
            artifact_id: "fixture.adapter".to_owned(),
            artifact_version: "1".to_owned(),
            build_id: None,
            protocol: "1.0".to_owned(),
            module_contract: "{}".to_owned(),
            module_client_id: "fixture.client".to_owned(),
            boot_id,
        };
        let error =
            swarm_process::module_owner::run_module_with_resolver(plan, &App369EmptyRefResolver)
                .expect_err("invalid native image must fail at the actual worker Command::spawn");
        assert_eq!(error.code, "MODULE_OWNER_SPAWN_FAILED");
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn owner_departure_restart_handoff_with_missing_corrupt_mismatched_guards() {
        // Real-Job restart regression: prove exact family departure with the
        // retained query, lose the query (restart), then bridge via the fsynced
        // durable receipt. Missing/corrupt/mismatched proofs must never start a
        // replacement. Uses the existing ConnectedOwnerFixture (real Job/group).
        let mut fixture = ConnectedOwnerFixture::new().await;
        let service = fixture.startup.service.clone();
        fixture.prove_departure().await;
        let boot = fixture.boot_id.clone();
        let owner = fixture.owner_receipt.clone();
        let token = owner["token"].as_str().unwrap().to_owned();
        let helper = fixture.helper_identity.clone();
        let path = service.departure_receipt_path();
        assert!(
            path.is_file(),
            "durable departure receipt must exist after proven exit"
        );
        let proved = service
            .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
            .unwrap()
            .expect("valid departure must be consumable");
        assert_eq!(proved.version, 1);
        assert_eq!(proved.module_id, service.descriptor.module_id.as_str());
        assert_eq!(
            proved.artifact_id,
            service.descriptor.artifact.artifact_id.as_str()
        );
        assert_eq!(
            proved.artifact_version,
            service.descriptor.artifact.version.as_str()
        );
        assert_eq!(proved.build_id, service.descriptor.artifact.build_id);
        assert_eq!(
            proved.descriptor_fingerprint,
            service.descriptor_fingerprint
        );
        assert_eq!(proved.scope, service.scope);
        assert_eq!(proved.boot_id, boot);
        assert_eq!(proved.owner, owner);
        assert!(same_process_identity(&proved.owner_helper, &helper));
        assert_eq!(proved.active_processes, 0);
        assert!(proved.family_empty);
        assert!(
            proved.observer == OWNER_DEPARTURE_OBSERVER_RETAINED
                || proved.observer == OWNER_DEPARTURE_OBSERVER_FRESH
        );
        // Restart handoff: query already released; fresh missing-Job stays
        // UNKNOWN on Windows, but the exact durable receipt bridges it.
        let restarted = service
            .wait_for_prior_owner_to_depart()
            .await
            .unwrap()
            .expect("restart must consume exact durable departure");
        assert_eq!(restarted.receipt, owner);
        assert_eq!(restarted.token, token);
        // Missing guard: no file is never a proof.
        fs::remove_file(&path).unwrap();
        assert!(
            service
                .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
                .unwrap()
                .is_none()
        );
        // Restore valid for corrupt/mismatch guards.
        service
            .write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .unwrap();
        // Corrupt guard: invalid shape must block.
        fs::write(&path, b"{not-json").unwrap();
        let corrupt =
            service.read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper));
        assert!(corrupt.is_err(), "corrupt receipt must not be consumable");
        assert_eq!(corrupt.unwrap_err().code, "MODULE_OWNER_DEPARTURE_INVALID");
        // Wrong-boot guard: no reuse across boots.
        service
            .write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .unwrap();
        let mut wrong_boot: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        wrong_boot["boot_id"] = serde_json::json!(Uuid::new_v4().to_string());
        fs::write(&path, serde_json::to_vec(&wrong_boot).unwrap()).unwrap();
        assert!(
            service
                .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
                .is_err(),
            "wrong-boot receipt must not be consumable"
        );
        // Wrong-owner guard.
        service
            .write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .unwrap();
        let mut wrong_owner: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        wrong_owner["owner"]["token"] = serde_json::json!(Uuid::new_v4().to_string());
        fs::write(&path, serde_json::to_vec(&wrong_owner).unwrap()).unwrap();
        assert!(
            service
                .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
                .is_err(),
            "wrong-owner receipt must not be consumable"
        );
        // Stale-scope guard.
        service
            .write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .unwrap();
        let mut stale_scope: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        stale_scope["scope"]["generation"] = serde_json::json!(999999u64);
        fs::write(&path, serde_json::to_vec(&stale_scope).unwrap()).unwrap();
        assert!(
            service
                .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
                .is_err(),
            "stale-scope receipt must not be consumable"
        );
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn owner_departure_visible_but_undurable_never_authorizes() {
        use std::{cell::Cell, rc::Rc};
        let mut fixture = ConnectedOwnerFixture::new().await;
        let service = fixture.startup.service.clone();
        fixture.prove_departure().await;
        let boot = fixture.boot_id.clone();
        let owner = fixture.owner_receipt.clone();
        let token = owner["token"].as_str().unwrap().to_owned();
        let helper = fixture.helper_identity.clone();
        let path = service.departure_receipt_path();
        assert!(
            path.is_file(),
            "durable receipt must exist after proven exit"
        );
        let failing = Rc::new(Cell::new(true));
        let failing_clone = Rc::clone(&failing);
        let target_path = path.clone();
        let _barrier = swarm_process::test_support::scoped_durability_barrier(move |target| {
            if *target == target_path && failing_clone.get() {
                Err(swarm_contracts::error::Error::new(
                    "PRIVATE_FILE_DURABILITY_UNKNOWN",
                    "fixture failed at the departure durability boundary",
                ))
            } else {
                swarm_process::sync_parent_directory(target)
            }
        });
        let write_err = service
            .write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .expect_err("failed barrier must not report durable publication");
        assert_eq!(write_err.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        assert!(
            path.is_file(),
            "failed barrier must not delete possibly-durable proof"
        );
        let read_err = service
            .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
            .expect_err("visible but undurable bytes must not be consumable");
        assert_eq!(read_err.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        failing.set(false);
        let promoted = service
            .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
            .unwrap()
            .expect("genuine proof promotes after durable barrier succeeds");
        assert_eq!(promoted.boot_id, boot);
        assert_eq!(promoted.owner, owner);
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[tokio::test]
    async fn owner_departure_fresh_service_restart_bridges_exact_proof() {
        let mut fixture = ConnectedOwnerFixture::new().await;
        let old = fixture.startup.service.clone();
        fixture.prove_departure().await;
        let boot = fixture.boot_id.clone();
        let owner = fixture.owner_receipt.clone();
        let token = owner["token"].as_str().unwrap().to_owned();
        let helper = fixture.helper_identity.clone();
        let path = old.departure_receipt_path();
        assert!(path.is_file());
        fn fresh_service_from(old: &Service) -> Arc<Service> {
            let (admission, _) = watch::channel(AdmissionState::Open);
            Arc::new(Service::new(ServiceInitialization {
                descriptor: old.descriptor.clone(),
                descriptor_fingerprint: old.descriptor_fingerprint.clone(),
                scope: old.scope.clone(),
                state_dir: old.state_dir.clone(),
                state_root: old.state_root.clone(),
                host_data_dir: old.host_data_dir.clone(),
                ipc: old.ipc.clone(),
                launch_config: old.launch_config.clone(),
                module_client_id: old.module_client_id.clone(),
                protocol: old.protocol,
                module_contract_json: old.module_contract_json.clone(),
                owner_executable: old.owner_executable.clone(),
                resolver: old.resolver.clone(),
                admission,
                demand_gate: Arc::new(AsyncMutex::new(())),
            }))
        }
        let fresh = fresh_service_from(&old);
        #[cfg(windows)]
        assert!(
            lock(&fresh.owner_family_query).is_none(),
            "fresh restart has no retained query"
        );
        let bridged = fresh
            .wait_for_prior_owner_to_depart()
            .await
            .unwrap()
            .expect("fresh restart must consume exact durable departure");
        assert_eq!(bridged.receipt, owner);
        assert_eq!(bridged.token, token);
        fs::remove_file(&path).unwrap();
        assert!(
            fresh
                .read_owner_departure_receipt_for(Some(&boot), &owner, &token, Some(&helper))
                .unwrap()
                .is_none(),
            "missing file is never proof"
        );
        old.write_owner_departure_receipt(&boot, &owner, &token, &helper)
            .unwrap();
    }
}
