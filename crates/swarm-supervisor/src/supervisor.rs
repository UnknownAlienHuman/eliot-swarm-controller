use crate::{
    ActivationPolicy, ArtifactSelector, BindingLaunchConfig, CapabilityId, DescriptorCatalog,
    LaunchValue, LifecycleOwnership, ModuleDescriptor, ModuleEffectCertainty, ModuleFailureStage,
    ModuleOwnerExecutable, ProtectedResolver, ProtectedResolverContext, ProtocolRange,
    ProtocolVersion, ServiceScope, SupervisorControlClient,
    descriptor::{validate_environment_name, validate_identifier, validate_launch_value},
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
use swarm_process::{
    departed_empty, module_child_belongs_to_owner, process_birth_identity, process_image_identity,
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
const OWNER_DRAIN_POLL: Duration = Duration::from_millis(500);
const HELPER_START_POLL: Duration = Duration::from_millis(100);
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

    /// Register one already-installed descriptor through the dedicated
    /// ModuleSupervisor credential. Descriptor registration is a Store
    /// metadata mutation and starts no process. The local descriptor must be
    /// present in this trusted catalog and its executable bytes must match
    /// the installer-provided digest before the request is sent.
    pub async fn register_descriptor(&self, descriptor: &ModuleDescriptor) -> Result<Value> {
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
        let path = fs::canonicalize(&descriptor.launch.executable)?;
        let actual_hash = hash_file_sha256(&path)?;
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
        let catalog = read_lock(&self.catalog).clone();
        let selected = catalog
            .select_exact(&selector, descriptor.protocol, &descriptor.capabilities)
            .map_err(|error| Error::new("MODULE_SELECTION", error.to_string()))?;
        if selected != descriptor {
            return Err(Error::new(
                "MODULE_DESCRIPTOR_UNTRUSTED",
                "descriptor does not exactly match the host's validated catalog entry",
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
        for service in matching {
            service.apply_readback(readback.clone())?;
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
    demands: Mutex<HashMap<String, usize>>,
    /// Serializes replacement and demand for this exact module/scope across
    /// descriptor generations. It is an in-memory gate, not durable state.
    demand_gate: Arc<AsyncMutex<()>>,
    demand_epoch: watch::Sender<u64>,
    recovery_epoch: watch::Sender<u64>,
    readback_required: AtomicBool,
    unknown_operation_ids: Mutex<BTreeSet<String>>,
    runner: AsyncMutex<Option<JoinHandle<()>>>,
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
    safe_to_replace: bool,
    family_departure_proven: bool,
    failure_code: Option<String>,
    failure_stage: Option<String>,
    effect_certainty: ModuleEffectCertainty,
    healthy_duration: Option<Duration>,
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
            demands: Mutex::new(HashMap::new()),
            demand_gate: initialization.demand_gate,
            demand_epoch,
            recovery_epoch,
            readback_required: AtomicBool::new(prior_state),
            unknown_operation_ids: Mutex::new(BTreeSet::new()),
            runner: AsyncMutex::new(None),
        }
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
        if matches!(
            &current,
            LifecycleState::Isolated { .. }
                | LifecycleState::OwnerIdentityUnknown
                | LifecycleState::ProcessIdentityUnknown { .. }
        ) {
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
            service.run_lifecycle().await;
            cleanup.after_runner_exit().await;
        }));
        Ok(())
    }

    // This cleanup may spawn another lifecycle task. A boxed Send boundary
    // avoids a recursive opaque-future type while preserving the runner mutex.
    fn after_runner_exit(self: &Arc<Self>) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let mut runner = self.runner.lock().await;
            *runner = None;
            if !self.has_demand()
                || matches!(
                    self.admission.borrow().clone(),
                    AdmissionState::Closed { .. }
                )
                || !matches!(
                    &self.status.borrow().lifecycle,
                    LifecycleState::WaitingForDemand | LifecycleState::WaitingForKernel
                )
            {
                return;
            }

            // Demand can race the runner's final no-demand check. Recheck and
            // replace the finished handle under the same mutex used by demand().
            let service = self.clone();
            *runner = Some(tokio::spawn(async move {
                let cleanup = service.clone();
                service.run_lifecycle().await;
                cleanup.after_runner_exit().await;
            }));
        })
    }

    async fn can_replace_descriptor(&self) -> bool {
        if self.has_demand()
            || self.readback_required.load(Ordering::Acquire)
            || !lock(&self.unknown_operation_ids).is_empty()
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
            current.lifecycle = LifecycleState::ProcessRunning {
                boot_id: boot_id.to_owned(),
            };
            current.readback_required = false;
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

    async fn run_lifecycle(self: Arc<Self>) {
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
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "restart history cannot be verified".to_owned(),
                    };
                    status.restart.exhausted = true;
                });
                return;
            }
        };
        let mut demand_epoch = self.demand_epoch.subscribe();
        let mut admission = self.admission.subscribe();
        let mut recovery = self.recovery_epoch.subscribe();

        loop {
            if !self.has_demand() {
                self.update_status(|status| {
                    if !matches!(
                        status.lifecycle,
                        LifecycleState::OwnerGroupRetained { .. }
                            | LifecycleState::OwnerIdentityUnknown
                            | LifecycleState::ProcessIdentityUnknown { .. }
                    ) {
                        status.lifecycle = LifecycleState::WaitingForDemand;
                        status.worker = None;
                        status.owner = None;
                        status.worker_boot_id = None;
                    }
                });
                return;
            }
            if matches!(admission.borrow().clone(), AdmissionState::Closed { .. }) {
                self.update_status(|status| {
                    status.admission = admission.borrow().clone();
                    status.lifecycle = LifecycleState::WaitingForKernel;
                });
                tokio::select! {
                    _ = demand_epoch.changed() => {},
                    changed = admission.changed() => {
                        if changed.is_err() { return; }
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
                        status.lifecycle = LifecycleState::Isolated {
                            reason: "restart policy time cannot be verified".to_owned(),
                        };
                        status.restart.exhausted = true;
                    });
                    return;
                }
            };
            prune_starts(&mut starts, policy.window_ms, now);
            if starts.len() >= usize::from(policy.max_starts) {
                self.update_status(|status| {
                    status.restart.starts_in_window = starts.len();
                    status.restart.exhausted = true;
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "restart budget exhausted; a changed valid descriptor is required"
                            .to_owned(),
                    };
                });
                return;
            }
            if !starts.is_empty() {
                let delay = jittered_delay(&policy, starts.len().saturating_sub(1));
                let deadline = Instant::now() + delay;
                self.update_status(|status| {
                    status.restart.starts_in_window = starts.len();
                    status.restart.last_delay_ms = Some(delay.as_millis() as u64);
                    status.lifecycle = LifecycleState::RestartBackoff {
                        delay_ms: delay.as_millis() as u64,
                    };
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
                            if changed.is_err() { return; }
                        }
                    }
                }
                if !self.has_demand() {
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::WaitingForDemand
                    });
                    return;
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
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "module state directory could not be prepared".to_owned(),
                    };
                    status.restart.exhausted = true;
                    status.last_failure = Some(FailureSummary {
                        code: error.code,
                        detail: "module state directory could not be prepared".to_owned(),
                    });
                });
                return;
            }
            let departed_prior_owner = match self.wait_for_prior_owner_to_depart().await {
                Ok(owner) => owner,
                Err(error) => {
                    self.record_failure(
                        &error.code,
                        "prior module owner identity is missing or unresolved",
                    );
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                        status.readback_required = true;
                    });
                    self.readback_required.store(true, Ordering::Release);
                    return;
                }
            };
            if !self.has_demand()
                || matches!(admission.borrow().clone(), AdmissionState::Closed { .. })
            {
                continue;
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
                return;
            }

            // The helper requires the same ownership marker before any other
            // scope files exist. Initialize it only after exact prior departure,
            // then let the helper acquire its own lifetime lock at process start.
            match swarm_process::module_owner::acquire_module_state_marker(&self.state_dir) {
                Ok(marker) => drop(marker),
                Err(error) => {
                    self.record_failure(&error.code, "module state marker could not be verified");
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::Isolated {
                            reason: "module state marker initialization failed".to_owned(),
                        };
                        status.restart.exhausted = true;
                    });
                    return;
                }
            }

            let boot_id = Uuid::new_v4().to_string();
            self.update_status(|status| {
                status.lifecycle = LifecycleState::Starting {
                    boot_id: boot_id.clone(),
                };
                status.owner = None;
                status.worker = None;
                status.worker_boot_id = Some(boot_id.clone());
                status.effect_certainty = ModuleEffectCertainty::Unknown;
                status.failure_stage = None;
                status.last_failure = None;
            });
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
                return;
            }
            if let Err(error) = self.write_launch_attempt(&boot_id, None) {
                self.record_failure(&error.code, "launch intent could not be persisted safely");
                self.update_status(|status| {
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "launch intent persistence failed before helper start".to_owned(),
                    };
                    status.restart.exhausted = true;
                });
                return;
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
                        return;
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
                        return;
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
                return;
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
                    return;
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
                return;
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
                return;
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
                )
                .await
            {
                Ok(attempt) => attempt,
                Err(error) => {
                    self.record_failure(
                        &error.code,
                        "helper/worker identity or owner-family proof is unresolved",
                    );
                    self.readback_required.store(true, Ordering::Release);
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::OwnerIdentityUnknown;
                        status.owner = Some(helper_identity.clone());
                        status.readback_required = true;
                    });
                    return;
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
                    status.lifecycle = LifecycleState::Isolated {
                        reason: "launch outcome is unknown; persisted evidence is retained and no replacement started".to_owned(),
                    };
                    status.owner = Some(helper_identity.clone());
                    status.worker = attempt.worker.clone();
                    status.worker_boot_id = Some(boot_id.clone());
                    status.readback_required = true;
                });
                return;
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
                return;
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
                return;
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
                    return;
                }
            }

            let restart_needed = self.has_demand()
                && matches!(
                    self.descriptor.lifecycle,
                    LifecycleOwnership::OwnedService | LifecycleOwnership::ExternalAttach
                );
            let needs_operation_readback = !attempt.exit.success() || restart_needed;
            self.readback_required
                .store(needs_operation_readback, Ordering::Release);
            self.update_status(|status| {
                status.lifecycle = if attempt.exit.success()
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
                status.worker_boot_id = Some(boot_id.clone());
                status.readback_required = needs_operation_readback;
                status.effect_certainty = attempt.effect_certainty;
                status.failure_stage = attempt
                    .failure_stage
                    .as_deref()
                    .and_then(ModuleFailureStage::from_helper);
                status.restart.starts_in_window = starts.len();
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
            if restart_needed {
                // A replacement adapter always begins at module.hello. The
                // Store retains uncertain Operations as Unknown. This lifecycle
                // never replays launch/input/native writes or treats an IPC
                // disconnect as proof that the process or owner has exited.
                continue;
            }
            return;
        }
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
            validate_environment_name(key)?;
            let name = format!("ELIOT_SWARM_CONFIG_{key}");
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
        swarm_process::module_owner::apply_launch_environment(command.as_std_mut());
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
        if name.starts_with("ELIOT_SWARM_CONFIG_") {
            if name.len() > 128
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(Error::invalid("module config environment name is invalid"));
            }
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
    async fn monitor_owner_helper(
        &self,
        helper: &mut Child,
        boot_id: &str,
        helper_identity: &ProcessIdentity,
        departed_prior_owner: Option<&VerifiedDepartedOwner>,
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
                loop {
                    if departed_empty(&owner["process"], token)? {
                        break;
                    }
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::OwnerGroupRetained {
                            owner_pid: helper_identity.pid,
                        };
                        status.owner = Some(helper_identity.clone());
                        status.worker = worker_identity.clone();
                        status.worker_boot_id = Some(boot_id.to_owned());
                    });
                    time::sleep(OWNER_DRAIN_POLL).await;
                }

                let healthy_duration = worker_started_at
                    .zip(worker_last_live_at)
                    .map(|(started, last_live)| last_live.saturating_duration_since(started));
                let Some(result) = launch_result.as_ref() else {
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
                    return Ok(OwnerHelperExit {
                        exit,
                        worker: Some(worker),
                        safe_to_replace: true,
                        // This is the only path with an exact worker receipt;
                        // departed_empty has already succeeded above.
                        family_departure_proven: true,
                        failure_code: Some("MODULE_EXITED".to_owned()),
                        failure_stage: None,
                        effect_certainty: ModuleEffectCertainty::Unknown,
                        healthy_duration,
                    });
                };
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
                return Ok(OwnerHelperExit {
                    exit,
                    worker: None,
                    safe_to_replace: true,
                    // Certified pre-spawn means no adapter family existed.
                    family_departure_proven: false,
                    failure_code: Some(failure_code),
                    failure_stage: Some(failure_stage),
                    effect_certainty: ModuleEffectCertainty::NotStarted,
                    healthy_duration: None,
                });
            }

            if let Some(worker) = worker_identity.as_ref() {
                if process_identity_is_live(worker)? {
                    let now = Instant::now();
                    worker_started_at.get_or_insert(now);
                    worker_last_live_at = Some(now);
                    self.update_status(|status| {
                        let hello_confirmed = matches!(
                            &status.lifecycle,
                            LifecycleState::ProcessRunning {
                                boot_id: confirmed_boot
                            } if confirmed_boot == boot_id
                        ) && status.worker_boot_id.as_deref()
                            == Some(boot_id)
                            && status.worker.as_ref().is_some_and(|confirmed_worker| {
                                same_process_identity(confirmed_worker, worker)
                            });
                        if !hello_confirmed {
                            // A live receipt alone is not Ready. Preserve a
                            // Store-confirmed ProcessRunning state for this
                            // exact boot and worker instead of regressing it.
                            status.lifecycle = LifecycleState::Starting {
                                boot_id: boot_id.to_owned(),
                            };
                        }
                        status.owner = Some(helper_identity.clone());
                        status.worker = Some(worker.clone());
                        status.worker_boot_id = Some(boot_id.to_owned());
                    });
                } else {
                    self.update_status(|status| {
                        status.lifecycle = LifecycleState::OwnerGroupRetained {
                            owner_pid: helper_identity.pid,
                        };
                        status.owner = Some(helper_identity.clone());
                        status.worker = Some(worker.clone());
                        status.worker_boot_id = Some(boot_id.to_owned());
                    });
                }
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
                let current = capture_identity(worker.pid)?;
                if current.image != *worker_image {
                    return Err(Error::new(
                        "MODULE_WORKER_IDENTITY_MISMATCH",
                        "worker image receipt differs from the current process incarnation",
                    ));
                }
                if !module_child_belongs_to_owner(owner, worker_image)? {
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
            || !matches!(stage, Some("resolve_refs" | "validate_launch"))
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
        loop {
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
                let prior_status = self.current_status();
                let confirmed_boot = match (
                    &prior_status.lifecycle,
                    prior_status.worker.as_ref(),
                    attached_worker.as_ref(),
                    boot_id.as_deref(),
                ) {
                    (
                        LifecycleState::ProcessRunning {
                            boot_id: current_boot,
                        },
                        Some(previous),
                        Some(current_worker),
                        Some(attempt_boot),
                    ) if current_boot == attempt_boot
                        && previous.pid == current_worker.pid
                        && previous.birth == current_worker.birth =>
                    {
                        Some(attempt_boot.to_owned())
                    }
                    _ => None,
                };
                self.update_status(|status| {
                    status.lifecycle = if let Some(confirmed_boot) = confirmed_boot.as_ref() {
                        LifecycleState::ProcessRunning {
                            boot_id: confirmed_boot.clone(),
                        }
                    } else {
                        LifecycleState::OwnerGroupRetained {
                            owner_pid: helper.pid,
                        }
                    };
                    status.owner = Some(helper.clone());
                    status.worker = attached_worker.clone();
                    status.worker_boot_id = boot_id.clone();
                });
                time::sleep(OWNER_DRAIN_POLL).await;
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
                } else {
                    return Err(Error::new(
                        "MODULE_LAUNCH_RESULT_MISSING",
                        "prior helper has no exact worker or certified pre-spawn receipt",
                    ));
                }
                prior_start_proved = true;
            }
            if departed_empty(&owner["process"], token)? {
                return Ok(Some(VerifiedDepartedOwner {
                    receipt: owner.clone(),
                    token: token.clone(),
                }));
            }
            let owner_pid = prior_owner
                .as_ref()
                .and_then(|(owner, _)| owner["process"]["pid"].as_u64())
                .and_then(|value| u32::try_from(value).ok())
                .or_else(|| attempted_helper.as_ref().map(|helper| helper.pid))
                .ok_or_else(|| Error::invalid("module owner process PID is invalid"))?;
            self.update_status(|status| {
                status.lifecycle = LifecycleState::OwnerGroupRetained { owner_pid };
                status.owner = None;
                status.worker = None;
                status.worker_boot_id = None;
            });
            // Never adopt or signal a surviving helper/group. It must prove
            // family departure before this scope can receive a new helper.
            time::sleep(OWNER_DRAIN_POLL).await;
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
        Some(_) => Ok(process_image_identity(identity.pid)? == identity.image),
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
        "pid": pid,
        "creation_filetime": image.get("creation_filetime")
            .ok_or_else(|| Error::invalid("worker image receipt has no creation time"))?,
    });
    #[cfg(target_os = "linux")]
    let birth = serde_json::json!({
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

#[cfg(test)]
mod tests {
    use super::*;

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
            let root = std::env::temp_dir().join(format!("swarm-startup-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            let root = fs::canonicalize(root).unwrap();
            swarm_process::private_permissions(&root, true).unwrap();
            let descriptor: ModuleDescriptor = serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "module_id": "fixture",
                "artifact": {"artifact_id": "fixture.adapter", "version": "1"},
                "launch": {
                    "executable": root.join("missing-adapter"),
                    "executable_sha256": "11".repeat(32)
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
                    path: root.join("missing-owner"),
                    sha256: crate::Sha256Digest::new("33".repeat(32)).unwrap(),
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
