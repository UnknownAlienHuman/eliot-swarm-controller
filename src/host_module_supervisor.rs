//! Optional host actor joining durable Store demand to the standalone module
//! supervisor. The actor is deliberately outside `host::run_until`'s required
//! supervisor JoinSet: a module failure cannot close the IPC listener or stop
//! unrelated workers.

use crate::{
    error::{Error, Result},
    model,
    store::{
        Store,
        module_demand::{ModuleDemand, ModuleDemandBlock, ModuleDemandCursor, ModuleScopeReadback},
    },
};
use futures_util::FutureExt;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use swarm_contracts::{
    Credential,
    module_catalog::{
        ArtifactSelector, CapabilityId, ProtectedRef, ProtocolRange, ProtocolVersion,
    },
};
use swarm_supervisor::{
    AdmissionState, BindingLaunchConfig, DemandCause, KernelFault, LaunchValue, ModuleDescriptor,
    ModuleEffectCertainty, ModuleFailureStage, ModuleOwnerExecutable, ModuleSupervisorObservation,
    ModuleSupervisorPhase, OperationReadback, OperationSnapshot, ProtectedResolverContext,
    ResolverMapDirectory, ServiceScope, SupervisorRegistry, load_installed_descriptor,
    module_contract_claim,
};
use tokio::{sync::watch, task::JoinHandle, time};

const HOST_MODULE_PROTOCOL: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
const MODULE_SCAN_FALLBACK: Duration = Duration::from_secs(2);
const MODULE_ACTOR_RETRY_MAX: Duration = Duration::from_secs(30);
const MODULE_HELLO_IDENTITY_RETRY: Duration = Duration::from_millis(50);
const MODULE_HELLO_IDENTITY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ADDITIONAL_PROTECTED_FILES: usize = 128;
const OPENCODE_NATIVE_OPTIONS_SCHEMA_ID: &str = "opencode-v2-native-options";
const OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256: &str =
    "d597be6bae80dc82535b658b5daaf3037a09976e5704d799a6715a673a62f662";

#[derive(Clone)]
pub(crate) struct ModuleSupervisorHostConfig {
    /// Explicit install root and descriptor files. No catalogue directory scan
    /// can start workers; only durable Operation demand reaches `demand()`.
    pub install_root: PathBuf,
    pub descriptor_files: Vec<PathBuf>,
    pub state_root: PathBuf,
    pub resolver_root: PathBuf,
    pub owner_helper: ModuleOwnerExecutable,
    /// Opaque reference -> existing protected file mappings. File contents
    /// never enter this actor, launch plans, or Store metadata.
    pub protected_files: BTreeMap<ProtectedRef, PathBuf>,
    pub launch_config: Arc<dyn BindingLaunchConfigProvider>,
}

impl ModuleSupervisorHostConfig {
    pub(crate) fn from_runtime_config(
        config: &crate::config::ModuleSupervisorConfig,
        root: &Path,
    ) -> Result<Option<Self>> {
        config.validate()?;
        if !config.enabled {
            return Ok(None);
        }
        let install_root = config.install_root.clone().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "enabled module supervisor needs install_root",
            )
        })?;
        let owner_path = config.owner_helper.clone().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "enabled module supervisor needs owner_helper",
            )
        })?;
        let owner_digest = config.owner_helper_sha256.clone().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "enabled module supervisor needs owner_helper_sha256",
            )
        })?;
        let owner_helper = ModuleOwnerExecutable {
            path: owner_path,
            sha256: swarm_supervisor::Sha256Digest::new(owner_digest).map_err(|error| {
                Error::new("MODULE_SUPERVISOR_CONFIG_INVALID", error.to_string())
            })?,
        };
        let mut protected_files = BTreeMap::new();
        for (reference, path) in &config.protected_files {
            let reference = ProtectedRef::new(reference.clone()).map_err(|error| {
                Error::new("MODULE_SUPERVISOR_CONFIG_INVALID", error.to_string())
            })?;
            if protected_files.insert(reference, path.clone()).is_some() {
                return Err(Error::new(
                    "MODULE_SUPERVISOR_CONFIG_INVALID",
                    "duplicate protected reference",
                ));
            }
        }
        let launch_config: Arc<dyn BindingLaunchConfigProvider> = match config.route_config_mapper {
            crate::config::ModuleRouteConfigMapper::DescriptorSchema => {
                Arc::new(DescriptorSchemaRouteConfig)
            }
            crate::config::ModuleRouteConfigMapper::EmptyOnly => Arc::new(EmptyRouteOptions),
            crate::config::ModuleRouteConfigMapper::OpenCodeSevenField => {
                Arc::new(OpenCodeRouteConfig)
            }
        };
        let value = Self {
            install_root,
            descriptor_files: config.descriptor_files.clone(),
            state_root: root.join("module-supervisor/state"),
            resolver_root: root.join("module-supervisor/resolver"),
            owner_helper,
            protected_files,
            launch_config,
        };
        validate_host_config(&value)?;
        Ok(Some(value))
    }
}

pub(crate) trait BindingLaunchConfigProvider: Send + Sync + 'static {
    fn for_binding(
        &self,
        descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig>;
}

/// An explicit compatibility mode for hosts that admit only empty native
/// options. New configurations use descriptor-schema dispatch below.
pub(crate) struct EmptyRouteOptions;

impl BindingLaunchConfigProvider for EmptyRouteOptions {
    fn for_binding(
        &self,
        _descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig> {
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
}

/// Route config is selected by the immutable descriptor schema, never by an
/// artifact/module-name allowlist. Adapters with no launch config schema get
/// an empty process config: their native options remain in the admitted
/// RuntimeCommand and are validated/consumed by that adapter.
pub(crate) struct DescriptorSchemaRouteConfig;

impl BindingLaunchConfigProvider for DescriptorSchemaRouteConfig {
    fn for_binding(
        &self,
        descriptor: &ModuleDescriptor,
        route_native_options: &Value,
    ) -> Result<BindingLaunchConfig> {
        match descriptor.config_schema.as_ref() {
            None => Ok(BindingLaunchConfig::default()),
            Some(schema)
                if schema.schema_id == OPENCODE_NATIVE_OPTIONS_SCHEMA_ID
                    && schema.version == "1"
                    && schema.sha256.as_ref().is_some_and(|digest| {
                        digest.as_str() == OPENCODE_NATIVE_OPTIONS_SCHEMA_SHA256
                    }) =>
            {
                OpenCodeRouteConfig.for_binding(descriptor, route_native_options)
            }
            Some(_) => Err(Error::new(
                "MODULE_CONFIG_SCHEMA_UNSUPPORTED",
                "the retained descriptor names a launch config schema with no host mapper",
            )),
        }
    }
}

/// Exact config bridge for the currently frozen OpenCode adapter contract.
/// Values remain per-binding literal strings; the connection file is passed
/// by path and is never read or copied by this adapter host.
pub(crate) struct OpenCodeRouteConfig;

impl BindingLaunchConfigProvider for OpenCodeRouteConfig {
    fn for_binding(
        &self,
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
        if object.keys().any(|key| !allowed.contains(&key.as_str()))
            || object.len() != allowed.len()
        {
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
        let values = BTreeMap::from([
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
        ]);
        let config = BindingLaunchConfig { values };
        config.validate().map_err(module_error)?;
        Ok(config)
    }
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

#[derive(Clone, Hash, PartialEq, Eq)]
struct DemandKey {
    module_id: String,
    scope: ServiceScope,
    operation_id: String,
}

struct HeldDemand {
    _lease: swarm_supervisor::DemandLease,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct ModuleScopeKey {
    module_id: String,
    scope: ServiceScope,
}

struct RecoveryBlock {
    descriptor: ModuleDescriptor,
    error_code: String,
    stage: ModuleFailureStage,
    release_after_readback: bool,
}

pub(crate) struct ModuleSupervisorHost {
    store: Store,
    registry: Arc<SupervisorRegistry>,
    descriptors: Vec<ModuleDescriptor>,
    config: ModuleSupervisorHostConfig,
    resolver: Arc<ResolverMapDirectory>,
    actor_instance_id: String,
}

#[derive(Clone)]
pub(crate) struct ModuleSupervisorHandle {
    current: Arc<RwLock<Option<Arc<ModuleSupervisorHost>>>>,
    admission: watch::Sender<AdmissionState>,
}

impl ModuleSupervisorHandle {
    pub(crate) async fn confirm_module_hello(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
    ) -> Result<()> {
        let current = self
            .current
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                Error::new(
                    "MODULE_SUPERVISOR_UNAVAILABLE",
                    "optional actor is not active",
                )
            })?;
        let deadline = time::Instant::now() + MODULE_HELLO_IDENTITY_TIMEOUT;
        loop {
            match current
                .confirm_module_hello(module_id, scope, boot_id)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error)
                    if error.code == "MODULE_WORKER_IDENTITY_UNKNOWN"
                        && time::Instant::now() < deadline =>
                {
                    // Store has already committed this exact module.hello.
                    // The owner helper may publish worker.json just after the
                    // adapter starts and sends hello, so wait briefly for the
                    // exact image receipt; never infer identity from the claim.
                    time::sleep(MODULE_HELLO_IDENTITY_RETRY).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn install(&self, host: Arc<ModuleSupervisorHost>) {
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(host);
    }

    fn clear(&self) {
        *self
            .current
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl Default for ModuleSupervisorHandle {
    fn default() -> Self {
        // The optional actor verifies the durable intake and exact Store
        // readbacks before admitting any new helper start.
        let (admission, _) = watch::channel(AdmissionState::Closed {
            fault: KernelFault::StoreUnavailable,
        });
        Self {
            current: Arc::new(RwLock::new(None)),
            admission,
        }
    }
}

pub(crate) struct OptionalModuleSupervisor {
    pub(crate) handle: ModuleSupervisorHandle,
    task: JoinHandle<()>,
}

impl OptionalModuleSupervisor {
    pub(crate) async fn join(self) {
        let _ = self.task.await;
    }
}

impl ModuleSupervisorHost {
    pub(crate) async fn start(
        store: Store,
        supervisor_credential: Credential,
        admission: watch::Sender<AdmissionState>,
        root: &Path,
        ipc: swarm_client::IpcConfig,
        config: ModuleSupervisorHostConfig,
    ) -> Result<Self> {
        validate_host_config(&config)?;
        let mut descriptors = Vec::new();
        for path in &config.descriptor_files {
            match load_installed_descriptor(path, &config.install_root) {
                Ok(descriptor) => descriptors.push(descriptor),
                Err(error) => {
                    eprintln!("module descriptor unavailable: {}", error.code);
                }
            }
        }
        let catalog = swarm_supervisor::DescriptorCatalog::from_descriptors(descriptors.clone())
            .map_err(|error| Error::new("MODULE_CATALOG_INVALID", error.to_string()))?;
        create_private_directory(&config.state_root)?;
        create_private_directory(&config.resolver_root)?;
        let resolver = Arc::new(
            ResolverMapDirectory::new(config.resolver_root.clone()).map_err(module_error)?,
        );
        let registry = Arc::new(
            SupervisorRegistry::new_with_admission(
                catalog,
                config.state_root.clone(),
                root.to_path_buf(),
                supervisor_credential,
                ipc,
                config.owner_helper.clone(),
                resolver.clone(),
                admission,
            )
            .map_err(module_error)?,
        );
        for descriptor in &descriptors {
            if let Err(error) = registry
                .register_descriptor(descriptor)
                .await
                .map_err(module_error)
            {
                // An exact descriptor registration is idempotent. Store/IPC
                // loss is retried by rebuilding this optional actor; a single
                // invalid package never takes down the native host.
                if retryable_supervisor_start(&error) {
                    return Err(error);
                }
                eprintln!("module descriptor registration blocked: {}", error.code);
            }
        }
        Ok(Self {
            store,
            registry,
            descriptors,
            config,
            resolver,
            actor_instance_id: model::new_id(),
        })
    }

    /// Run the optional actor. Store/journal errors close only new module
    /// starts; existing leases and native-owner obligations are retained.
    pub(crate) async fn run(&self, mut stopping: watch::Receiver<bool>) -> Result<()> {
        let mut changed = self.store.subscribe_module_demand_changes();
        let mut scan = time::interval(MODULE_SCAN_FALLBACK);
        scan.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut held = HashMap::<DemandKey, HeldDemand>::new();
        let mut event_sequence = 0_u64;
        let mut pending_events = VecDeque::<ModuleSupervisorObservation>::new();
        // Callback events rejected for a missing/cross-scope Operation stay
        // available as bounded private evidence; only a sanitized, same-scope
        // status is sent back to Store.
        let mut quarantined_events = VecDeque::<ModuleSupervisorObservation>::new();
        let mut host_diagnostics = VecDeque::<String>::new();
        let mut last_status = HashMap::<(String, ServiceScope), String>::new();
        let mut recovery_blocks = HashMap::<ModuleScopeKey, RecoveryBlock>::new();

        loop {
            if *stopping.borrow() {
                return Ok(());
            }
            if matches!(
                self.registry.admission_state(),
                AdmissionState::Closed { .. }
            ) {
                match self
                    .verify_recovery(
                        &mut recovery_blocks,
                        &mut event_sequence,
                        &mut pending_events,
                        &mut host_diagnostics,
                    )
                    .await
                {
                    Ok(()) => self.registry.reopen_durable_admission_after_recovery(),
                    Err(error) => {
                        if is_durable_journal_failure(&error) {
                            self.registry
                                .close_durable_admission(KernelFault::DurableJournalUnavailable);
                        } else if is_store_unavailable(&error) {
                            self.registry
                                .close_durable_admission(KernelFault::StoreUnavailable);
                        }
                        eprintln!("module recovery verification: {}", error.code);
                    }
                }
            } else if !recovery_blocks.is_empty() {
                if let Err(error) = self
                    .refresh_recovery_blocks(
                        &mut recovery_blocks,
                        &mut event_sequence,
                        &mut pending_events,
                    )
                    .await
                {
                    if is_store_unavailable(&error) {
                        self.registry
                            .close_durable_admission(KernelFault::StoreUnavailable);
                    }
                    eprintln!("module scoped recovery readback: {}", error.code);
                }
            }

            let scan_result = if matches!(
                self.registry.admission_state(),
                AdmissionState::Closed { .. }
            ) {
                Ok(())
            } else {
                self.reconcile_demands(
                    &mut held,
                    &mut recovery_blocks,
                    &mut event_sequence,
                    &mut pending_events,
                    &mut host_diagnostics,
                )
                .await
            };
            if let Err(error) = scan_result {
                if is_durable_journal_failure(&error) {
                    self.registry
                        .close_durable_admission(KernelFault::DurableJournalUnavailable);
                } else if is_store_unavailable(&error) {
                    self.registry
                        .close_durable_admission(KernelFault::StoreUnavailable);
                }
                eprintln!("module supervisor Store readback: {}", error.code);
            }
            self.collect_status_events(
                &mut event_sequence,
                &mut last_status,
                &mut pending_events,
                &mut recovery_blocks,
                &mut quarantined_events,
            )
            .await;

            tokio::select! {
                changed_result = changed.changed() => {
                    if changed_result.is_err() {
                        self.registry.close_durable_admission(KernelFault::StoreUnavailable);
                        time::sleep(MODULE_SCAN_FALLBACK).await;
                    }
                }
                _ = scan.tick() => {}
                stop_result = stopping.changed() => {
                    if stop_result.is_err() || *stopping.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub(crate) async fn confirm_module_hello(
        &self,
        module_id: &str,
        scope: &ServiceScope,
        boot_id: &str,
    ) -> Result<()> {
        self.registry
            .confirm_module_hello(module_id, scope, boot_id)
            .await
            .map_err(module_error)
    }

    /// Reconcile the existing durable intake journal and read every currently
    /// demanded binding scope before reopening helper starts after a Store
    /// failure or actor restart. Database-wide failures keep the shared gate
    /// closed; descriptor/Operation/identity uncertainty is retained by exact
    /// scope so verified neighboring bindings can continue.
    async fn verify_recovery(
        &self,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        host_diagnostics: &mut VecDeque<String>,
    ) -> Result<()> {
        // This bounded Store transaction verifies the existing durable source
        // journal and advances at most one page. Remaining backlog is normal;
        // it is not a global admission failure for unrelated module scopes.
        self.store.reconcile_module_recovery_journal_page().await?;

        let mut cursor = None::<ModuleDemandCursor>;
        let mut blocked_scopes = HashSet::<ModuleScopeKey>::new();
        loop {
            let snapshot = self.store.module_demand_snapshot(cursor.clone()).await?;
            for blocked in &snapshot.blocked {
                self.note_blocked_demand(
                    blocked,
                    recovery_blocks,
                    &mut blocked_scopes,
                    sequence,
                    pending,
                    host_diagnostics,
                )
                .await?;
            }
            if snapshot.truncated && snapshot.next_cursor.is_none() {
                return Err(Error::new(
                    "MODULE_DEMAND_CURSOR_MISSING",
                    "recovery demand page omitted its continuation cursor",
                ));
            }
            for demand in &snapshot.demands {
                let scope = ServiceScope {
                    binding_id: demand.binding_id.clone(),
                    generation: demand.generation,
                };
                let key = ModuleScopeKey {
                    module_id: demand.module_id.clone(),
                    scope: scope.clone(),
                };
                let readback = self
                    .store
                    .module_scope_readback(
                        demand.module_id.clone(),
                        scope.binding_id.clone(),
                        i64::try_from(scope.generation)
                            .map_err(|_| Error::invalid("binding generation overflow"))?,
                    )
                    .await;
                match readback {
                    Ok(stored) => {
                        let operations = operation_readback_for_scope(&scope, &stored);
                        let active = self
                            .registry
                            .status(&demand.module_id, &scope)
                            .await
                            .is_some();
                        let applied = match operations {
                            Ok(operations) => {
                                if active {
                                    self.registry
                                        .apply_operation_readback(
                                            &demand.module_id,
                                            &scope,
                                            operations,
                                        )
                                        .await
                                        .map_err(module_error)
                                } else {
                                    Ok(())
                                }
                            }
                            Err(error) => Err(error),
                        };
                        match applied {
                            Ok(()) => {
                                self.clear_recovery_block(&key, recovery_blocks, sequence, pending)?
                            }
                            Err(error) if is_store_unavailable(&error) => return Err(error),
                            Err(error) => self.set_recovery_block(
                                &key,
                                &demand.descriptor,
                                &error.code,
                                recovery_blocks,
                                sequence,
                                pending,
                            )?,
                        }
                    }
                    Err(error) if is_store_unavailable(&error) => return Err(error),
                    Err(error) => self.set_recovery_block(
                        &key,
                        &demand.descriptor,
                        &error.code,
                        recovery_blocks,
                        sequence,
                        pending,
                    )?,
                }
            }
            cursor = snapshot.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(())
    }

    /// Retry only scopes that failed their previous exact Store readback. This
    /// does not close admission for healthy neighbors or start a process.
    async fn refresh_recovery_blocks(
        &self,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        let keys = recovery_blocks.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some((descriptor, release_after_readback)) = recovery_blocks
                .get(&key)
                .map(|block| (block.descriptor.clone(), block.release_after_readback))
            else {
                continue;
            };
            let generation = i64::try_from(key.scope.generation)
                .map_err(|_| Error::invalid("binding generation overflow"))?;
            let readback = self
                .store
                .module_scope_readback(
                    key.module_id.clone(),
                    key.scope.binding_id.clone(),
                    generation,
                )
                .await;
            match readback {
                Ok(stored) => {
                    let operations = operation_readback_for_scope(&key.scope, &stored);
                    let scope_still_needed = stored.native_identity_retained
                        || stored
                            .operations
                            .iter()
                            .any(|operation| is_pending_operation(&operation.state));
                    let active = self
                        .registry
                        .status(&key.module_id, &key.scope)
                        .await
                        .is_some();
                    let applied = match operations {
                        Ok(operations) if active => self
                            .registry
                            .apply_operation_readback(&key.module_id, &key.scope, operations)
                            .await
                            .map_err(module_error),
                        Ok(_) => Ok(()),
                        Err(error) => Err(error),
                    };
                    match applied {
                        Ok(()) if release_after_readback || !scope_still_needed => {
                            self.clear_recovery_block(&key, recovery_blocks, sequence, pending)?
                        }
                        Ok(()) => {}
                        Err(error) if is_store_unavailable(&error) => return Err(error),
                        Err(error) => self.set_recovery_block(
                            &key,
                            &descriptor,
                            &error.code,
                            recovery_blocks,
                            sequence,
                            pending,
                        )?,
                    }
                }
                Err(error) if is_store_unavailable(&error) => return Err(error),
                Err(error) => self.set_recovery_block(
                    &key,
                    &descriptor,
                    &error.code,
                    recovery_blocks,
                    sequence,
                    pending,
                )?,
            }
        }
        Ok(())
    }

    fn set_recovery_block(
        &self,
        key: &ModuleScopeKey,
        descriptor: &ModuleDescriptor,
        error_code: &str,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        self.set_recovery_block_at_stage(
            key,
            descriptor,
            error_code,
            ModuleFailureStage::Store,
            recovery_blocks,
            sequence,
            pending,
        )
    }

    fn set_recovery_block_at_stage(
        &self,
        key: &ModuleScopeKey,
        descriptor: &ModuleDescriptor,
        error_code: &str,
        stage: ModuleFailureStage,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        self.set_recovery_block_policy(
            key,
            descriptor,
            error_code,
            stage,
            true,
            recovery_blocks,
            sequence,
            pending,
        )
    }

    fn set_persistent_recovery_block(
        &self,
        key: &ModuleScopeKey,
        descriptor: &ModuleDescriptor,
        error_code: &str,
        stage: ModuleFailureStage,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        self.set_recovery_block_policy(
            key,
            descriptor,
            error_code,
            stage,
            false,
            recovery_blocks,
            sequence,
            pending,
        )
    }

    fn set_recovery_block_policy(
        &self,
        key: &ModuleScopeKey,
        descriptor: &ModuleDescriptor,
        error_code: &str,
        stage: ModuleFailureStage,
        release_after_readback: bool,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        let safe_code = safe_observation_error_code(error_code);
        let changed = recovery_blocks.get(key).is_none_or(|block| {
            block.error_code != safe_code
                || block.stage != stage
                || block.release_after_readback != release_after_readback
        });
        recovery_blocks.insert(
            key.clone(),
            RecoveryBlock {
                descriptor: descriptor.clone(),
                error_code: safe_code.to_owned(),
                stage,
                release_after_readback,
            },
        );
        if changed {
            self.queue_recovery_observation(
                key,
                descriptor,
                ModuleSupervisorPhase::Isolated,
                Some(stage),
                Some(safe_code),
                sequence,
                pending,
            )?;
        }
        Ok(())
    }

    async fn note_blocked_demand(
        &self,
        blocked: &ModuleDemandBlock,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        reported: &mut HashSet<ModuleScopeKey>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        host_diagnostics: &mut VecDeque<String>,
    ) -> Result<()> {
        let Some(descriptor) = blocked.descriptor.as_ref() else {
            // No trusted descriptor means there is no honest module identity
            // to put in an observation. The candidate remains blocked in the
            // Store projection and this bounded source page is retried later.
            remember_host_diagnostic(
                host_diagnostics,
                format!(
                    "demand_without_descriptor:{}:{}:{}",
                    blocked.binding_id, blocked.generation, blocked.error_code
                ),
            );
            return Ok(());
        };
        let key = ModuleScopeKey {
            module_id: descriptor.module_id.to_string(),
            scope: ServiceScope {
                binding_id: blocked.binding_id.clone(),
                generation: blocked.generation,
            },
        };
        if !reported.insert(key.clone()) {
            return Ok(());
        }
        let generation = i64::try_from(key.scope.generation)
            .map_err(|_| Error::invalid("binding generation overflow"))?;
        let readback = match self
            .store
            .module_scope_readback(
                key.module_id.clone(),
                key.scope.binding_id.clone(),
                generation,
            )
            .await
        {
            Ok(readback) => readback,
            Err(error) if is_store_unavailable(&error) => {
                self.set_recovery_block_at_stage(
                    &key,
                    descriptor,
                    &error.code,
                    failure_stage_for(&error.code),
                    recovery_blocks,
                    sequence,
                    pending,
                )?;
                return Err(error);
            }
            Err(error) => {
                self.set_recovery_block(
                    &key,
                    descriptor,
                    &error.code,
                    recovery_blocks,
                    sequence,
                    pending,
                )?;
                return Ok(());
            }
        };
        let operations = match operation_readback_for_scope(&key.scope, &readback) {
            Ok(operations) => operations,
            Err(error) => {
                self.set_recovery_block(
                    &key,
                    descriptor,
                    &error.code,
                    recovery_blocks,
                    sequence,
                    pending,
                )?;
                return Ok(());
            }
        };
        if self
            .registry
            .status(&key.module_id, &key.scope)
            .await
            .is_some()
        {
            if let Err(error) = self
                .registry
                .apply_operation_readback(&key.module_id, &key.scope, operations)
                .await
                .map_err(module_error)
            {
                if is_store_unavailable(&error) {
                    self.set_recovery_block_at_stage(
                        &key,
                        descriptor,
                        &error.code,
                        failure_stage_for(&error.code),
                        recovery_blocks,
                        sequence,
                        pending,
                    )?;
                    return Err(error);
                }
                self.set_recovery_block(
                    &key,
                    descriptor,
                    &error.code,
                    recovery_blocks,
                    sequence,
                    pending,
                )?;
                return Ok(());
            }
        }
        if !readback.native_identity_retained
            && !readback
                .operations
                .iter()
                .any(|operation| is_pending_operation(&operation.state))
        {
            // The demand disappeared between the bounded projection and the
            // exact readback; do not create a failure status or lease.
            return Ok(());
        }
        self.set_persistent_recovery_block(
            &key,
            descriptor,
            &blocked.error_code,
            failure_stage_for(&blocked.error_code),
            recovery_blocks,
            sequence,
            pending,
        )
    }

    fn clear_recovery_block(
        &self,
        key: &ModuleScopeKey,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        host_diagnostics: &mut VecDeque<String>,
    ) -> Result<()> {
        if let Some(block) = recovery_blocks.remove(key) {
            self.queue_recovery_observation(
                key,
                &block.descriptor,
                ModuleSupervisorPhase::WaitingForDemand,
                None,
                None,
                sequence,
                pending,
            )?;
        }
        Ok(())
    }

    fn queue_recovery_observation(
        &self,
        key: &ModuleScopeKey,
        descriptor: &ModuleDescriptor,
        phase: ModuleSupervisorPhase,
        stage: Option<ModuleFailureStage>,
        error_code: Option<&str>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
    ) -> Result<()> {
        if pending.len() >= 1024 {
            return Err(Error::new(
                "MODULE_OBSERVATION_QUEUE_FULL",
                "module recovery status queue is full",
            ));
        }
        let next = sequence.checked_add(1).ok_or_else(|| {
            Error::new(
                "MODULE_OBSERVATION_SEQUENCE_EXHAUSTED",
                "module status sequence exhausted",
            )
        })?;
        let event_id = format!("{}:{next}", self.actor_instance_id);
        let event = ModuleSupervisorObservation {
            schema_version: 1,
            actor_instance_id: self.actor_instance_id.clone(),
            event_id,
            sequence: next,
            module_id: descriptor.module_id.to_string(),
            artifact_id: descriptor.artifact.artifact_id.to_string(),
            artifact_version: descriptor.artifact.version.to_string(),
            build_id: descriptor.artifact.build_id.clone(),
            scope: key.scope.clone(),
            boot_id: None,
            phase,
            effect_certainty: ModuleEffectCertainty::Unknown,
            stage,
            error_code: error_code.map(str::to_owned),
            unknown_operation_ids: Vec::new(),
            unknown_operation_count: 0,
            unknown_operation_ids_truncated: false,
        };
        *sequence = next;
        pending.push_back(event);
        Ok(())
    }

    async fn reconcile_demands(
        &self,
        held: &mut HashMap<DemandKey, HeldDemand>,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        sequence: &mut u64,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        host_diagnostics: &mut VecDeque<String>,
    ) -> Result<()> {
        let mut cursor = None::<ModuleDemandCursor>;
        let mut seen = HashSet::<DemandKey>::new();
        let mut blocked_scopes = HashSet::<ModuleScopeKey>::new();
        loop {
            let snapshot = self.store.module_demand_snapshot(cursor.clone()).await?;
            for blocked in snapshot.blocked {
                self.note_blocked_demand(
                    &blocked,
                    recovery_blocks,
                    &mut blocked_scopes,
                    sequence,
                    pending,
                    host_diagnostics,
                )
                .await?;
            }
            if snapshot.truncated && snapshot.next_cursor.is_none() {
                return Err(Error::new(
                    "MODULE_DEMAND_CURSOR_MISSING",
                    "bounded module demand page omitted its continuation cursor",
                ));
            }
            for demand in snapshot.demands {
                let key = DemandKey {
                    module_id: demand.module_id.clone(),
                    scope: ServiceScope {
                        binding_id: demand.binding_id.clone(),
                        generation: demand.generation,
                    },
                    operation_id: demand.operation_id.clone(),
                };
                seen.insert(key.clone());
                let scope_key = ModuleScopeKey {
                    module_id: demand.module_id.clone(),
                    scope: key.scope.clone(),
                };
                if recovery_blocks.contains_key(&scope_key) {
                    // Keep an existing lease alive, but do not start or
                    // refresh this scope while its per-scope failure is held.
                    continue;
                }
                if held.contains_key(&key) {
                    if let Err(error) = self.refresh_readback(&demand).await {
                        self.set_recovery_block_at_stage(
                            &scope_key,
                            &demand.descriptor,
                            &error.code,
                            failure_stage_for(&error.code),
                            recovery_blocks,
                            sequence,
                            pending,
                        )?;
                        if is_store_unavailable(&error) {
                            return Err(error);
                        }
                    }
                    continue;
                }
                match self.start_demand(&demand).await {
                    Ok(lease) => {
                        held.insert(key, HeldDemand { _lease: lease });
                    }
                    Err(error) => {
                        if is_store_unavailable(&error) {
                            self.set_recovery_block_at_stage(
                                &scope_key,
                                &demand.descriptor,
                                &error.code,
                                failure_stage_for(&error.code),
                                recovery_blocks,
                                sequence,
                                pending,
                            )?;
                        } else {
                            self.set_persistent_recovery_block(
                                &scope_key,
                                &demand.descriptor,
                                &error.code,
                                failure_stage_for(&error.code),
                                recovery_blocks,
                                sequence,
                                pending,
                            )?;
                        }
                        if is_store_unavailable(&error) {
                            return Err(error);
                        }
                        eprintln!("module worker isolated for this binding: {}", error.code);
                    }
                }
            }
            cursor = snapshot.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        // An Operation can settle between pages or between this scan and the
        // Store failure callback. Refresh each scope whose held leases would
        // otherwise disappear before dropping its final lease. This readback
        // also retains a scope with an unresolved operation or native identity.
        let mut stale_scopes = BTreeSet::<(String, String, u64)>::new();
        for key in held.keys().filter(|key| !seen.contains(*key)) {
            stale_scopes.insert((
                key.module_id.clone(),
                key.scope.binding_id.clone(),
                key.scope.generation,
            ));
        }
        let mut retained_scopes = HashSet::<(String, ServiceScope)>::new();
        for (module_id, binding_id, generation) in stale_scopes {
            let scope = ServiceScope {
                binding_id: binding_id.clone(),
                generation,
            };
            let readback = match self
                .store
                .module_scope_readback(
                    module_id.clone(),
                    binding_id.clone(),
                    i64::try_from(generation)
                        .map_err(|_| Error::invalid("binding generation overflow"))?,
                )
                .await
            {
                Ok(readback) => readback,
                Err(error) if is_store_unavailable(&error) => return Err(error),
                Err(error) => {
                    eprintln!(
                        "module stale scope retained for binding {}: {}",
                        binding_id, error.code
                    );
                    retained_scopes.insert((module_id, scope));
                    continue;
                }
            };
            let operations = match operation_readback_for_scope(&scope, &readback) {
                Ok(operations) => operations,
                Err(error) => {
                    eprintln!(
                        "module stale scope readback retained for binding {}: {}",
                        binding_id, error.code
                    );
                    retained_scopes.insert((module_id, scope));
                    continue;
                }
            };
            if let Err(error) = self
                .registry
                .apply_operation_readback(&module_id, &scope, operations)
                .await
                .map_err(module_error)
            {
                if is_store_unavailable(&error) {
                    return Err(error);
                }
                eprintln!(
                    "module stale scope held after supervisor readback failure for binding {}: {}",
                    binding_id, error.code
                );
                retained_scopes.insert((module_id, scope));
                continue;
            }
            if readback.native_identity_retained
                || readback
                    .operations
                    .iter()
                    .any(|operation| is_pending_operation(&operation.state))
            {
                retained_scopes.insert((module_id, scope));
            }
        }
        held.retain(|key, _| {
            seen.contains(key)
                || retained_scopes.contains(&(key.module_id.clone(), key.scope.clone()))
        });
        Ok(())
    }

    async fn start_demand(&self, demand: &ModuleDemand) -> Result<swarm_supervisor::DemandLease> {
        if matches!(
            self.registry.admission_state(),
            AdmissionState::Closed { .. }
        ) {
            return Err(Error::new(
                "KERNEL_ADMISSION_CLOSED",
                "kernel/journal failure blocks module setup until verified recovery",
            ));
        }
        let launch_config = self
            .config
            .launch_config
            .for_binding(&demand.descriptor, &demand.route_native_options)?;
        let provisioned = self
            .store
            .ensure_module_binding_credential(
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
                "Store provisioned a different protected credential reference than the retained descriptor",
            ));
        }
        if self.config.protected_files.len() > MAX_ADDITIONAL_PROTECTED_FILES {
            return Err(Error::new(
                "MODULE_RESOLVER_LIMIT",
                "too many configured protected file references",
            ));
        }
        let claim = module_contract_claim(
            &demand.descriptor,
            ProtocolRange::exact(HOST_MODULE_PROTOCOL),
        )
        .map_err(module_error)?;
        let context = ProtectedResolverContext {
            module_id: demand.descriptor.module_id.clone(),
            artifact: demand.descriptor.artifact.clone(),
            scope: ServiceScope {
                binding_id: demand.binding_id.clone(),
                generation: demand.generation,
            },
            protocol: HOST_MODULE_PROTOCOL,
        };
        self.resolver
            .publish_binding_map(
                &context,
                &demand.descriptor,
                &claim,
                credential_ref,
                &provisioned.credential_file,
                &provisioned.credential_file_sha256,
                &self.config.protected_files,
            )
            .map_err(module_error)?;
        let ready = self
            .store
            .check_module_binding_credential_ready(
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
        let readback = operation_readback(demand)?;
        self.registry
            .demand(
                &selector,
                ProtocolRange::exact(HOST_MODULE_PROTOCOL),
                &required,
                context.scope,
                DemandCause::Operation {
                    operation_id: demand.operation_id.clone(),
                },
                launch_config,
                ready.module_client_id,
                Some(readback),
            )
            .await
            .map_err(module_error)
    }

    async fn refresh_readback(&self, demand: &ModuleDemand) -> Result<()> {
        let readback = operation_readback(demand)?;
        self.registry
            .apply_operation_readback(
                &demand.module_id,
                &ServiceScope {
                    binding_id: demand.binding_id.clone(),
                    generation: demand.generation,
                },
                readback,
            )
            .await
            .map_err(module_error)
    }

    async fn collect_status_events(
        &self,
        sequence: &mut u64,
        last_status: &mut HashMap<(String, ServiceScope), String>,
        pending: &mut VecDeque<ModuleSupervisorObservation>,
        recovery_blocks: &mut HashMap<ModuleScopeKey, RecoveryBlock>,
        quarantined: &mut VecDeque<ModuleSupervisorObservation>,
    ) {
        for status in self.registry.statuses().await {
            let key = (status.module_id.clone(), status.scope.clone());
            let fingerprint = match serde_json::to_string(&status) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if last_status.get(&key) != Some(&fingerprint) {
                let Some(next) = sequence.checked_add(1) else {
                    eprintln!("module status sequence exhausted");
                    continue;
                };
                if pending.len() >= 1024 {
                    eprintln!(
                        "module status callback queue is full; retaining latest Store status"
                    );
                    continue;
                }
                if let Ok(value) =
                    ModuleSupervisorObservation::from_status(&status, &self.actor_instance_id, next)
                {
                    *sequence = next;
                    pending.push_back(value);
                    last_status.insert(key, fingerprint);
                } else {
                    eprintln!("module status observation rejected");
                }
            }
        }
        while let Some(event) = pending.front().cloned() {
            match self
                .store
                .record_module_supervisor_observation(event.clone())
                .await
            {
                Ok(()) => {
                    pending.pop_front();
                }
                Err(error) => {
                    if is_store_unavailable(&error) {
                        self.registry
                            .close_durable_admission(KernelFault::StoreUnavailable);
                    }
                    if error.code == "MODULE_OBSERVATION_OPERATION_TERMINAL" {
                        // The callback confirmed an exact-scope Operation is
                        // terminal after our prior readback. Refresh the exact
                        // binding generation before replacing the rejected
                        // status event. If this readback fails, keep the
                        // original event byte-for-byte so an ambiguous Store
                        // result remains idempotently retryable.
                        let module_id = event.module_id.clone();
                        let scope = event.scope.clone();
                        let generation = match i64::try_from(scope.generation) {
                            Ok(generation) => generation,
                            Err(_) => {
                                eprintln!("module status readback generation overflow");
                                break;
                            }
                        };
                        let refreshed = self
                            .store
                            .module_scope_readback(
                                module_id.clone(),
                                scope.binding_id.clone(),
                                generation,
                            )
                            .await;
                        match refreshed.and_then(|readback| {
                            operation_readback_for_scope(&scope, &readback)
                                .map(|operations| (readback, operations))
                        }) {
                            Ok((_readback, operations)) => {
                                match self
                                    .registry
                                    .apply_operation_readback(&module_id, &scope, operations)
                                    .await
                                    .map_err(module_error)
                                {
                                    Ok(()) => {
                                        pending.pop_front();
                                        last_status.remove(&(module_id, scope));
                                    }
                                    Err(readback_error) => {
                                        eprintln!(
                                            "module status scoped readback rejected: {}",
                                            readback_error.code
                                        );
                                    }
                                }
                            }
                            Err(readback_error) => {
                                if is_store_unavailable(&readback_error) {
                                    self.registry
                                        .close_durable_admission(KernelFault::StoreUnavailable);
                                }
                                eprintln!(
                                    "module status scoped readback unavailable: {}",
                                    readback_error.code
                                );
                            }
                        }
                        // A refreshed status will receive a new actor sequence
                        // on the next poll. Do not send newer events ahead of it.
                        break;
                    }
                    if error.code == "MODULE_OBSERVATION_OPERATION_SCOPE" {
                        // The Store authenticated the exact binding and
                        // descriptor, then rejected one or more listed IDs
                        // because they are absent or belong to another scope.
                        // Keep the original bytes only in a bounded local
                        // quarantine. Do not attach those IDs to any scope.
                        if quarantined.len() >= 256 {
                            quarantined.pop_front();
                        }
                        quarantined.push_back(event.clone());
                        pending.pop_front();

                        let scope_key = ModuleScopeKey {
                            module_id: event.module_id.clone(),
                            scope: event.scope.clone(),
                        };
                        if let Some(descriptor) = self.descriptor_for_observation(&event) {
                            recovery_blocks.insert(
                                scope_key.clone(),
                                RecoveryBlock {
                                    descriptor: descriptor.clone(),
                                    error_code: error.code.clone(),
                                    stage: ModuleFailureStage::Store,
                                    release_after_readback: true,
                                },
                            );
                        }

                        let Some(next) = sequence.checked_add(1) else {
                            eprintln!("module status sequence exhausted after scope rejection");
                            break;
                        };
                        let mut isolated = event;
                        isolated.sequence = next;
                        isolated.event_id = match isolated.boot_id.as_deref() {
                            Some(boot_id) => {
                                format!("{boot_id}:{}:{next}", isolated.actor_instance_id,)
                            }
                            None => format!("{}:{next}", isolated.actor_instance_id),
                        };
                        isolated.phase = ModuleSupervisorPhase::Isolated;
                        isolated.effect_certainty = ModuleEffectCertainty::Unknown;
                        isolated.stage = Some(ModuleFailureStage::Store);
                        isolated.error_code = Some(error.code.clone());
                        isolated.unknown_operation_ids.clear();
                        isolated.unknown_operation_count = 0;
                        isolated.unknown_operation_ids_truncated = false;
                        *sequence = next;
                        pending.push_back(isolated);
                        eprintln!(
                            "module status Operation scope rejected; quarantined event {} and held binding {}",
                            quarantined
                                .back()
                                .map(|value| value.event_id.as_str())
                                .unwrap_or("unknown"),
                            scope_key.scope.binding_id
                        );
                        // Tail events retain their existing order and IDs;
                        // the replacement has the next fresh sequence at the
                        // queue tail, so callbacks remain monotonic.
                        continue;
                    }
                    if callback_rejection_is_permanent(&error.code) {
                        // The Store did not accept this event as a valid
                        // observation. Keep a bounded local copy for operator
                        // diagnosis and remove it from the delivery head; do
                        // not present it as Manager-visible evidence or let a
                        // revoked/mismatched scope block neighboring scopes.
                        if quarantined.len() >= 256 {
                            quarantined.pop_front();
                        }
                        quarantined.push_back(event.clone());
                        pending.pop_front();
                        eprintln!(
                            "module status not delivered by Store ({}): {}",
                            error.code, event.event_id
                        );
                        continue;
                    }
                    eprintln!("module status callback retained: {}", error.code);
                    break;
                }
            }
        }
    }

    fn descriptor_for_observation(
        &self,
        observation: &ModuleSupervisorObservation,
    ) -> Option<&ModuleDescriptor> {
        self.descriptors.iter().find(|descriptor| {
            descriptor.module_id.as_str() == observation.module_id
                && descriptor.artifact.artifact_id.as_str() == observation.artifact_id
                && descriptor.artifact.version.as_str() == observation.artifact_version
                && descriptor.artifact.build_id == observation.build_id
        })
    }
}

/// Start an optional actor that retries its own configuration/Store failures.
/// The parent host owns this handle separately and must never feed its error
/// or panic into the required-supervisor JoinSet.
pub(crate) fn spawn_isolated_module_supervisor(
    store: Store,
    supervisor_credential: Credential,
    root: PathBuf,
    ipc: swarm_client::IpcConfig,
    config: crate::config::ModuleSupervisorConfig,
    stopping: watch::Receiver<bool>,
) -> OptionalModuleSupervisor {
    let handle = ModuleSupervisorHandle::default();
    let actor_handle = handle.clone();
    let task = tokio::spawn(async move {
        let mut retry = Duration::from_millis(250);
        loop {
            if *stopping.borrow() {
                return;
            }
            let host_config = match ModuleSupervisorHostConfig::from_runtime_config(&config, &root)
            {
                Ok(Some(value)) => value,
                Ok(None) => return,
                Err(error) => {
                    eprintln!(
                        "optional module supervisor configuration unavailable: {}",
                        error.code
                    );
                    // The host's Config is immutable for this run. Repeating
                    // the same malformed paths cannot repair it; a changed
                    // configuration is picked up on the next host start.
                    return;
                }
            };
            let host = ModuleSupervisorHost::start(
                store.clone(),
                supervisor_credential.clone(),
                actor_handle.admission.clone(),
                &root,
                ipc.clone(),
                host_config,
            )
            .await;
            match host {
                Ok(host) => {
                    let host = Arc::new(host);
                    actor_handle.install(host.clone());
                    let run = AssertUnwindSafe(host.run(stopping.clone()))
                        .catch_unwind()
                        .await;
                    actor_handle.clear();
                    match run {
                        Ok(Ok(())) if *stopping.borrow() => return,
                        Ok(Ok(())) => eprintln!("optional module supervisor returned unexpectedly"),
                        Ok(Err(error)) => {
                            eprintln!("optional module supervisor isolated: {}", error.code)
                        }
                        Err(_) => eprintln!(
                            "optional module supervisor panicked; retaining host and native workers"
                        ),
                    }
                }
                Err(error) => {
                    eprintln!("optional module supervisor unavailable: {}", error.code);
                    if !retryable_supervisor_start(&error) {
                        // Incompatible protocol, rejected credential, or a
                        // malformed trusted descriptor needs changed local
                        // evidence. Do not repeat the identical failure.
                        return;
                    }
                }
            }
            tokio::select! {
                _ = time::sleep(retry) => {},
                result = wait_for_stop(stopping.clone()) => if result { return; },
            }
            retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
        }
    });
    OptionalModuleSupervisor { handle, task }
}

async fn wait_for_stop(mut stopping: watch::Receiver<bool>) -> bool {
    loop {
        if *stopping.borrow() {
            return true;
        }
        if stopping.changed().await.is_err() {
            return true;
        }
    }
}

fn operation_readback(demand: &ModuleDemand) -> Result<OperationReadback> {
    let operations = demand
        .operation_readback
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
        scope: ServiceScope {
            binding_id: demand.binding_id.clone(),
            generation: demand.generation,
        },
        complete: true,
        operations,
    };
    readback
        .validate_for(&readback.scope)
        .map_err(module_error)?;
    Ok(readback)
}

fn operation_readback_for_scope(
    scope: &ServiceScope,
    stored: &ModuleScopeReadback,
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

fn is_pending_operation(state: &str) -> bool {
    matches!(
        state,
        "queued" | "sending" | "native_accepted" | "outcome_unknown"
    )
}

fn validate_provisioned(
    credential: &crate::store::ProvisionedModuleCredential,
    demand: &ModuleDemand,
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
            "Store credential readiness does not match the exact retained binding and descriptor",
        ));
    }
    Ok(())
}

fn validate_host_config(config: &ModuleSupervisorHostConfig) -> Result<()> {
    if !config.install_root.is_absolute()
        || !config.state_root.is_absolute()
        || !config.resolver_root.is_absolute()
        || config.descriptor_files.len() > 256
        || config.protected_files.len() > MAX_ADDITIONAL_PROTECTED_FILES
    {
        return Err(Error::new(
            "MODULE_SUPERVISOR_CONFIG_INVALID",
            "module supervisor roots must be absolute and package/reference counts bounded",
        ));
    }
    config.owner_helper.validate().map_err(module_error)?;
    Ok(())
}

fn module_error(error: swarm_supervisor::Error) -> Error {
    Error::new(error.code, error.message)
}

fn create_private_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::invalid(
            "module supervisor state root must be absolute",
        ));
    }
    let mut chain = path.ancestors().collect::<Vec<_>>();
    chain.reverse();
    for component in chain
        .into_iter()
        .filter(|item| !item.as_os_str().is_empty())
    {
        match std::fs::symlink_metadata(component) {
            Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_dir() => {
                return Err(Error::new(
                    "MODULE_SUPERVISOR_PATH_INVALID",
                    "module supervisor state roots must not traverse links or non-directories",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(component)?;
                let metadata = std::fs::symlink_metadata(component)?;
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    return Err(Error::new(
                        "MODULE_SUPERVISOR_PATH_INVALID",
                        "module supervisor directory creation resolved to a link or non-directory",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    swarm_process::private_permissions(path, true).map_err(Into::into)
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn is_store_unavailable(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "STORE_CLOSED" | "STORE_ERROR" | "STORE_PANIC"
    )
}

fn is_durable_journal_failure(error: &Error) -> bool {
    error.code.starts_with("AUTOMATION_INTAKE_") || error.code.starts_with("AUTOMATION_HOOK_INDEX_")
}

fn safe_observation_error_code(code: &str) -> &str {
    if !code.is_empty()
        && code.len() <= 128
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code
    } else {
        "MODULE_RECOVERY_READBACK_FAILED"
    }
}

fn remember_host_diagnostic(diagnostics: &mut VecDeque<String>, mut value: String) {
    if value.len() > 512 {
        let mut end = 512;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    if diagnostics.contains(&value) {
        return;
    }
    if diagnostics.len() >= 256 {
        diagnostics.pop_front();
    }
    eprintln!("module supervisor host-only diagnostic: {value}");
    diagnostics.push_back(value);
}

fn failure_stage_for(code: &str) -> ModuleFailureStage {
    if code.starts_with("STORE_")
        || code.starts_with("AUTOMATION_INTAKE_")
        || code.starts_with("AUTOMATION_HOOK_INDEX_")
        || code.starts_with("MODULE_READBACK_")
        || code == "BINDING_CLOSED"
        || code == "MODULE_CREDENTIAL_OPERATION_NOT_PENDING"
        || code == "KERNEL_ADMISSION_CLOSED"
    {
        ModuleFailureStage::Store
    } else if code.starts_with("MODULE_CREDENTIAL_") || code.starts_with("MODULE_RESOLVER_") {
        ModuleFailureStage::ResolveRefs
    } else {
        ModuleFailureStage::ValidateLaunch
    }
}

fn callback_rejection_is_permanent(code: &str) -> bool {
    matches!(
        code,
        "MODULE_OBSERVATION_CONFLICT"
            | "MODULE_OBSERVATION_STALE"
            | "MODULE_OBSERVATION_SCOPE_MISSING"
            | "MODULE_OBSERVATION_IDENTITY_MISMATCH"
            | "MODULE_OBSERVATION_CERTAINTY_INVALID"
            | "MODULE_OBSERVATION_INVALID"
            | "MODULE_DESCRIPTOR_MISSING"
            | "MODULE_ROUTE_CORRUPT"
            | "BINDING_CLOSED"
    )
}

fn retryable_supervisor_start(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "HOST_UNAVAILABLE" | "OUTCOME_UNKNOWN" | "STORE_CLOSED" | "STORE_ERROR" | "IO_ERROR"
    )
}
