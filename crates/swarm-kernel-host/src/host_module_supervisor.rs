//! Optional host handoff joining durable Store demand to the independently
//! installed module-supervisor sibling. The handoff is deliberately outside
//! `host::run_until`'s required supervisor JoinSet: a module failure cannot
//! close the IPC listener or stop unrelated workers.

use crate::{
    error::{Error, Result},
    store::Store,
};
use futures_util::FutureExt;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use swarm_contracts::{Credential, module_catalog::ProtectedRef};
use swarm_process::{
    child_error::{ChildError, project_child_error_line},
    process_birth_identity, process_image_identity,
};
use swarm_supervisor::control::{
    SupervisorChildExit, SupervisorChildExitCategory, SupervisorChildHealth,
    SupervisorChildProcessIdentity, SupervisorChildStopState,
};
use swarm_supervisor::{
    ModuleOwnerExecutable, StandaloneRouteConfigMapper, StandaloneSupervisorConfig,
    SupervisorBootstrap, SupervisorControlClient,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStderr, ChildStdin, Command},
    runtime::Handle,
    sync::watch,
    task::JoinHandle,
    time,
};

const MODULE_ACTOR_RETRY_MAX: Duration = Duration::from_secs(30);
const MODULE_ACTOR_ISOLATED_RETRY: Duration = Duration::from_secs(60);
const MAX_ADDITIONAL_PROTECTED_FILES: usize = 128;
const SUPERVISOR_CHILD_DIAGNOSTIC_LIMIT: usize = 1024;

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
    /// never enter this bootstrap, launch plans, or Store metadata.
    pub protected_files: BTreeMap<ProtectedRef, PathBuf>,
    pub route_config_mapper: crate::config::ModuleRouteConfigMapper,
}

impl ModuleSupervisorHostConfig {
    pub(crate) fn from_runtime_config(
        config: &crate::config::Config,
        root: &Path,
    ) -> Result<Option<Self>> {
        config.module_supervisor.validate()?;
        if !config.module_supervisor.enabled {
            return Ok(None);
        }
        let module_config = &config.module_supervisor;
        let install_root = module_config.install_root.clone().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "enabled module supervisor needs install_root",
            )
        })?;
        let owner_path = module_config.owner_helper.clone().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "enabled module supervisor needs owner_helper",
            )
        })?;
        let owner_digest = module_config.owner_helper_sha256.clone().ok_or_else(|| {
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
        for (reference, path) in &module_config.protected_files {
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
        // Owned OpenCode auth sources are private host paths, not credential
        // bytes. Carry the configured opaque reference through the supervisor
        // bootstrap so the route mapper can retain the exact source path.
        for (reference, source) in &config.opencode_provider_auth_sources {
            let reference = ProtectedRef::new(reference.clone()).map_err(|error| {
                Error::new("MODULE_SUPERVISOR_CONFIG_INVALID", error.to_string())
            })?;
            if let Some(previous) = protected_files.insert(reference, source.auth_file.clone())
                && previous != source.auth_file
            {
                return Err(Error::new(
                    "MODULE_SUPERVISOR_CONFIG_INVALID",
                    "protected reference maps to conflicting host files",
                ));
            }
        }
        let value = Self {
            install_root,
            descriptor_files: module_config.descriptor_files.clone(),
            state_root: root.join("module-supervisor/state"),
            resolver_root: root.join("module-supervisor/resolver"),
            owner_helper,
            protected_files,
            route_config_mapper: module_config.route_config_mapper,
        };
        validate_host_config(&value)?;
        Ok(Some(value))
    }

    fn standalone_bootstrap(
        &self,
        root: &Path,
        ipc: swarm_client::IpcConfig,
        supervisor_credential: Credential,
    ) -> Result<SupervisorBootstrap> {
        let route_config_mapper = match self.route_config_mapper {
            crate::config::ModuleRouteConfigMapper::DescriptorSchema => {
                StandaloneRouteConfigMapper::DescriptorSchema
            }
            crate::config::ModuleRouteConfigMapper::EmptyOnly => {
                StandaloneRouteConfigMapper::EmptyOnly
            }
            crate::config::ModuleRouteConfigMapper::OpenCodeSevenField => {
                StandaloneRouteConfigMapper::OpenCodeSevenField
            }
        };
        let bootstrap = SupervisorBootstrap {
            config: StandaloneSupervisorConfig {
                schema_version: 1,
                root: root.to_path_buf(),
                ipc,
                supervisor_credential,
                install_root: self.install_root.clone(),
                descriptor_files: self.descriptor_files.clone(),
                state_root: self.state_root.clone(),
                resolver_root: self.resolver_root.clone(),
                owner_helper: self.owner_helper.path.clone(),
                owner_helper_sha256: self.owner_helper.sha256.clone(),
                protected_files: self.protected_files.clone(),
                launch_configs: BTreeMap::new(),
                route_config_mapper,
            },
        };
        bootstrap.validate().map_err(module_error)?;
        Ok(bootstrap)
    }
}

pub(crate) struct OptionalModuleSupervisor {
    task: JoinHandle<Result<()>>,
    health_control: Option<SupervisorControlClient>,
}

impl OptionalModuleSupervisor {
    pub(crate) async fn join(self) -> Result<()> {
        let OptionalModuleSupervisor {
            task,
            health_control,
        } = self;
        match task.await {
            Ok(result) => result,
            Err(_) => {
                let join_error = Error::new(
                    "MODULE_SUPERVISOR_JOIN_FAILED",
                    "module supervisor task failed",
                );
                if let Some(control) = health_control
                    && let Err(health_error) = record_module_actor_status(
                        &control,
                        "isolated",
                        1,
                        Some("MODULE_SUPERVISOR_JOIN_FAILED"),
                        Some(MODULE_ACTOR_ISOLATED_RETRY),
                    )
                    .await
                {
                    return Err(join_error.with_secondary_error(health_error));
                }
                Err(join_error)
            }
        }
    }
}

/// Start the optional supervisor as an independently installed sibling process.
///
/// The host owns only this child handle and its private bootstrap pipe. Durable
/// admission, descriptor identity, owner receipts, operation readback, and
/// native process ownership remain behind the supervisor's authenticated IPC
/// client; this coordinator never imports Store data into the child.
pub(crate) fn spawn_independent_module_supervisor(
    store: Store,
    supervisor_credential: Credential,
    root: PathBuf,
    ipc: swarm_client::IpcConfig,
    config: Arc<crate::config::Config>,
    stopping: watch::Receiver<bool>,
) -> OptionalModuleSupervisor {
    let control =
        SupervisorControlClient::new(root.clone(), supervisor_credential.clone(), ipc.clone());
    let health_control = control.as_ref().ok().cloned();
    let panic_control = health_control.clone();
    let task = tokio::spawn(async move {
        let run = AssertUnwindSafe(async move {
            let control = match control {
                Ok(control) => control,
                Err(error) => {
                    eprintln!("optional module supervisor IPC unavailable: {}", error.code);
                    return Err(module_error(error));
                }
            };
            let host_config =
                match ModuleSupervisorHostConfig::from_runtime_config(config.as_ref(), &root) {
                    Ok(Some(value)) => value,
                    Ok(None) => return Ok(()),
                    Err(error) => {
                        let error = match record_module_actor_status(
                            &control,
                            "isolated",
                            1,
                            Some(&error.code),
                            Some(MODULE_ACTOR_ISOLATED_RETRY),
                        )
                        .await
                        {
                            Ok(()) => error,
                            Err(health_error) => {
                                return Err(error.with_secondary_error(health_error));
                            }
                        };
                        eprintln!(
                            "optional module supervisor configuration unavailable: {}",
                            error.code
                        );
                        return Err(error);
                    }
                };
            let bootstrap =
                match host_config.standalone_bootstrap(&root, ipc, supervisor_credential) {
                    Ok(bootstrap) => bootstrap,
                    Err(error) => {
                        let error = match record_module_actor_status(
                            &control,
                            "isolated",
                            1,
                            Some(&error.code),
                            Some(MODULE_ACTOR_ISOLATED_RETRY),
                        )
                        .await
                        {
                            Ok(()) => error,
                            Err(health_error) => {
                                return Err(error.with_secondary_error(health_error));
                            }
                        };
                        eprintln!(
                            "optional module supervisor bootstrap unavailable: {}",
                            error.code
                        );
                        return Err(error);
                    }
                };
            run_supervisor_process_loop(store, control, bootstrap, stopping).await
        })
        .catch_unwind()
        .await;
        match run {
            Ok(result) => result,
            Err(_) => {
                let panic_error =
                    Error::new(SUPERVISOR_CHILD_PANICKED, "module supervisor task panicked");
                if let Some(control) = panic_control
                    && let Err(health_error) = record_module_actor_status(
                        &control,
                        "isolated",
                        1,
                        Some(SUPERVISOR_CHILD_PANICKED),
                        Some(MODULE_ACTOR_ISOLATED_RETRY),
                    )
                    .await
                {
                    return Err(panic_error.with_secondary_error(health_error));
                }
                Err(panic_error)
            }
        }
    });
    OptionalModuleSupervisor {
        task,
        health_control,
    }
}

/// The host supplies the existing Store only as an activation oracle. No Store
/// object or database capability is serialized into the private bootstrap.
pub(crate) fn spawn_isolated_module_supervisor(
    store: Store,
    supervisor_credential: Credential,
    root: PathBuf,
    ipc: swarm_client::IpcConfig,
    config: Arc<crate::config::Config>,
    stopping: watch::Receiver<bool>,
) -> OptionalModuleSupervisor {
    spawn_independent_module_supervisor(store, supervisor_credential, root, ipc, config, stopping)
}

const SUPERVISOR_PROCESS_BASE_RETRY: Duration = Duration::from_millis(250);
const SUPERVISOR_PROCESS_MAX_FAILURES: u32 = 5;
const SUPERVISOR_PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISOR_PROCESS_STABLE: Duration = Duration::from_secs(5);
const SUPERVISOR_DEMAND_PAGE_LIMIT: usize = 256;
const SUPERVISOR_CHILD_RETAINED: &str = "MODULE_SUPERVISOR_CHILD_RETAINED";
const SUPERVISOR_CHILD_PANICKED: &str = "MODULE_SUPERVISOR_PANICKED";
const SUPERVISOR_SPAWN_PENDING: &str = "MODULE_SUPERVISOR_SPAWN_PENDING";

async fn run_supervisor_process_loop(
    store: Store,
    control: SupervisorControlClient,
    bootstrap: SupervisorBootstrap,
    stopping: watch::Receiver<bool>,
) -> Result<()> {
    let mut stopping = stopping;
    let mut demand_changes = store.subscribe_module_demand_changes();
    let mut failures = 0_u32;
    let mut retry = SUPERVISOR_PROCESS_BASE_RETRY;
    // Configured descriptors need their catalogue owner before the first agent launch.
    let catalog_needed = !bootstrap.config.descriptor_files.is_empty();
    let frame = match bootstrap.to_frame() {
        Ok(frame) => frame,
        Err(error) => {
            let error = module_error(error);
            let error = match record_module_actor_status(
                &control,
                "isolated",
                1,
                Some(&error.code),
                Some(MODULE_ACTOR_ISOLATED_RETRY),
            )
            .await
            {
                Ok(()) => error,
                Err(health_error) => return Err(error.with_secondary_error(health_error)),
            };
            return Err(error);
        }
    };
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        let demanded = if catalog_needed {
            Ok(true)
        } else {
            wait_for_module_demand(&store, &mut demand_changes, &mut stopping).await
        };
        let demanded = match demanded {
            Ok(demanded) => demanded,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                if let Err(health_error) = record_module_actor_status(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                )
                .await
                {
                    return Err(error.with_secondary_error(health_error));
                }
                if !wait_for_supervisor_backoff(delay, &mut stopping).await {
                    return Ok(());
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };
        if !demanded {
            return Ok(());
        }

        let executable = match supervisor_sibling_executable() {
            Ok(path) => path,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                if let Err(health_error) = record_module_actor_status(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                )
                .await
                {
                    return Err(error.with_secondary_error(health_error));
                }
                if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
                    return Ok(());
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };

        let prior_departure = match reconcile_prior_child(&control).await {
            Ok(PriorChildState::Clear { retained }) => retained,
            Ok(PriorChildState::Alive(child)) => {
                failures = failures.saturating_add(1).min(32);
                record_module_actor_health(
                    &control,
                    "isolated",
                    failures,
                    Some("MODULE_SUPERVISOR_PRIOR_CHILD_LIVE"),
                    Some(MODULE_ACTOR_ISOLATED_RETRY),
                    Some(&child),
                )
                .await?;
                if !wait_for_supervisor_retry(
                    MODULE_ACTOR_ISOLATED_RETRY,
                    &mut demand_changes,
                    &mut stopping,
                )
                .await
                {
                    return Ok(());
                }
                continue;
            }
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                if let Err(health_error) = record_module_actor_status(
                    &control,
                    "isolated",
                    failures,
                    Some(&error.code),
                    Some(MODULE_ACTOR_ISOLATED_RETRY),
                )
                .await
                {
                    return Err(error.with_secondary_error(health_error));
                }
                if !wait_for_supervisor_retry(
                    MODULE_ACTOR_ISOLATED_RETRY,
                    &mut demand_changes,
                    &mut stopping,
                )
                .await
                {
                    return Ok(());
                }
                continue;
            }
        };

        // Persist the launch intent, then read it back through the same
        // authenticated Store path before the first exec. A pending intent
        // with no departed receipt remains a restart fence after this host
        // disappears; a failed write therefore never claims durable health or
        // authorizes a replacement.
        record_module_actor_status(
            &control,
            "isolated",
            failures,
            Some(SUPERVISOR_SPAWN_PENDING),
            Some(MODULE_ACTOR_ISOLATED_RETRY),
        )
        .await?;
        let intent = control.read_health().await.map_err(module_error)?;
        if !spawn_intent_is_confirmed(&intent, prior_departure.as_ref()) {
            return Err(Error::new(
                "MODULE_SUPERVISOR_SPAWN_INTENT_UNCONFIRMED",
                "supervisor launch intent did not read back as a pending, replaceable receipt",
            ));
        }

        let started_at = time::Instant::now();
        let (child, bootstrap_pipe, child_identity) =
            match spawn_supervisor_child(&control, &executable, &frame).await {
                Ok(child) => child,
                Err(error) => {
                    failures = failures.saturating_add(1).min(32);
                    let (state, delay) = process_failure_state(failures, retry);
                    let status_code = if error.secondary_codes.is_empty() {
                        error.code.as_str()
                    } else {
                        // The reaper still owns an exact child, but its
                        // initial receipt was rejected. Preserve the durable
                        // pre-launch uncertainty across a host restart rather
                        // than replacing the pending fence with a retryable
                        // primary code.
                        SUPERVISOR_SPAWN_PENDING
                    };
                    if let Err(health_error) = record_module_actor_status(
                        &control,
                        state,
                        failures,
                        Some(status_code),
                        Some(delay),
                    )
                    .await
                    {
                        return Err(error.with_secondary_error(health_error));
                    }
                    if !is_retryable_child_spawn_error(&error) {
                        return Err(error);
                    }
                    if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
                        return Ok(());
                    }
                    retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                    continue;
                }
            };
        let mut child =
            SupervisorChildLease::new(child, bootstrap_pipe, child_identity, control.clone());
        // Own stderr before the running-health write. A failed or panicking
        // health path must not leave the child blocked on a full pipe while
        // the lease transfers the exact handle to its detached reaper.
        if let Some(stderr) = child.child_mut().stderr.take() {
            child.set_diagnostic(spawn_child_diagnostic(stderr));
        }
        let running_health =
            supervisor_child_health(&child.identity, None, SupervisorChildStopState::Running);
        let running_health_result = record_module_actor_health(
            &control,
            "running",
            failures,
            None,
            None,
            Some(&running_health),
        )
        .await;
        child.set_initial_health_result(running_health_result.clone());
        if let Err(error) = running_health_result {
            child.retain_with_code("MODULE_SUPERVISOR_HEALTH_WRITE_FAILED");
            return Err(error);
        }

        let mut bootstrap_pipe = child.take_stdin();
        let exit = tokio::select! {
            status = child.child_mut().wait() => Some(status),
            _changed = stopping.changed() => {
                // Dropping the only stdin handle is the child protocol's
                // graceful stop. The supervisor then releases only its own
                // leases; it never broad-kills native owners or adapters.
                drop(bootstrap_pipe.take());
                let stopped = time::timeout(
                    SUPERVISOR_PROCESS_STOP_TIMEOUT,
                    child.child_mut().wait(),
                )
                .await;
                let child_diagnostic =
                    join_child_diagnostic(child.take_diagnostic()).await;
                if matches!(&stopped, Ok(Ok(_))) {
                    let _ = child.take_child();
                } else {
                    child.retain_with_code("SUPERVISOR_STOP_UNKNOWN");
                }
                let (state, error_code, exit, stop) = match stopped {
                    Ok(Ok(status)) => (
                        "dormant",
                        None,
                        Some(child_exit_from_status(&status, child_diagnostic.as_ref())),
                        SupervisorChildStopState::Confirmed,
                    ),
                    Ok(Err(error)) => {
                        let exit = Some(Err(error));
                        (
                            "isolated",
                            Some("SUPERVISOR_STOP_UNKNOWN"),
                            Some(child_exit_from_result(&exit, child_diagnostic.as_ref())),
                            SupervisorChildStopState::Uncertain,
                        )
                    }
                    Err(_) => (
                        "isolated",
                        Some("SUPERVISOR_STOP_TIMEOUT"),
                        Some(child_exit_from_result(&None, child_diagnostic.as_ref())),
                        SupervisorChildStopState::Uncertain,
                    ),
                };
                let child_health = supervisor_child_health(&child.identity, exit, stop);
                let retry = error_code.map(|_| MODULE_ACTOR_ISOLATED_RETRY);
                record_module_actor_health(
                    &control,
                    state,
                    1,
                    error_code,
                    retry,
                    Some(&child_health),
                )
                .await
                .inspect_err(|_| {
                    child.retain_with_code("MODULE_SUPERVISOR_HEALTH_WRITE_FAILED");
                })?;
                return Ok(());
            }
        };
        drop(bootstrap_pipe);
        let child_diagnostic = join_child_diagnostic(child.take_diagnostic()).await;
        let child_exit = child_exit_from_result(&exit, child_diagnostic.as_ref());
        let child_stop = match &exit {
            Some(Ok(_)) => SupervisorChildStopState::NotRequested,
            Some(Err(_)) | None => SupervisorChildStopState::Uncertain,
        };
        let child_health = supervisor_child_health(&child.identity, Some(child_exit), child_stop);
        if matches!(&exit, Some(Ok(_))) {
            let _ = child.take_child();
        }
        let demanded = match module_supervisor_has_demand(&store).await {
            Ok(demanded) => catalog_needed || demanded,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                if let Err(health_error) = record_module_actor_health(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                    Some(&child_health),
                )
                .await
                {
                    return Err(error.with_secondary_error(health_error));
                }
                if !wait_for_supervisor_backoff(delay, &mut stopping).await {
                    return Ok(());
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };
        if !demanded {
            record_module_actor_health(&control, "dormant", 0, None, None, Some(&child_health))
                .await?;
            failures = 0;
            retry = SUPERVISOR_PROCESS_BASE_RETRY;
            continue;
        }
        if started_at.elapsed() >= SUPERVISOR_PROCESS_STABLE {
            failures = 0;
            retry = SUPERVISOR_PROCESS_BASE_RETRY;
        }
        failures = failures.saturating_add(1).min(32);
        let (state, delay) = process_failure_state(failures, retry);
        let status_code = match &exit {
            Some(Ok(status)) if status.success() => "SUPERVISOR_STOPPED",
            Some(Ok(_)) => "SUPERVISOR_EXITED",
            Some(Err(_)) | None => "SUPERVISOR_STATUS_UNKNOWN",
        };
        record_module_actor_health(
            &control,
            state,
            failures,
            Some(status_code),
            Some(delay),
            Some(&child_health),
        )
        .await?;
        if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
            return Ok(());
        }
        retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
    }
}

async fn wait_for_module_demand(
    store: &Store,
    demand_changes: &mut watch::Receiver<u64>,
    stopping: &mut watch::Receiver<bool>,
) -> Result<bool> {
    loop {
        if *stopping.borrow() {
            return Ok(false);
        }
        if module_supervisor_has_demand(store).await? {
            return Ok(true);
        }
        tokio::select! {
            changed = demand_changes.changed() => {
                if changed.is_err() {
                    return Err(Error::new("STORE_CLOSED", "module demand stream ended"));
                }
            }
            changed = stopping.changed() => {
                if changed.is_err() || *stopping.borrow() {
                    return Ok(false);
                }
            }
        }
    }
}

async fn wait_for_supervisor_backoff(
    delay: Duration,
    stopping: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = time::sleep(delay) => true,
        changed = stopping.changed() => changed.is_ok() && !*stopping.borrow(),
    }
}

async fn wait_for_supervisor_retry(
    delay: Duration,
    demand_changes: &mut watch::Receiver<u64>,
    stopping: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = time::sleep(delay) => true,
        changed = demand_changes.changed() => changed.is_ok(),
        changed = stopping.changed() => changed.is_ok() && !*stopping.borrow(),
    }
}

async fn module_supervisor_has_demand(store: &Store) -> Result<bool> {
    let mut cursor = None;
    for _ in 0..SUPERVISOR_DEMAND_PAGE_LIMIT {
        let page = store.module_demand_snapshot(cursor).await?;
        if !page.demands.is_empty() || !page.blocked.is_empty() {
            return Ok(true);
        }
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
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(false);
        }
    }
    Err(Error::new(
        "MODULE_DEMAND_PAGE_LIMIT",
        "module demand readback exceeded its bounded page limit",
    ))
}

enum PriorChildState {
    Clear {
        retained: Option<SupervisorChildHealth>,
    },
    Alive(SupervisorChildHealth),
}

/// Reconcile the existing typed child receipt before a fresh process launch.
/// A matching birth and image proves that the prior child is still the exact
/// owner; a missing or changed birth proves only that this incarnation
/// departed. Any identity-read uncertainty blocks replacement.
async fn reconcile_prior_child(control: &SupervisorControlClient) -> Result<PriorChildState> {
    let readback = control.read_health().await.map_err(module_error)?;
    let Some(child) = readback.child else {
        if readback.error_code.as_deref() == Some(SUPERVISOR_SPAWN_PENDING) {
            return Err(Error::new(
                SUPERVISOR_SPAWN_PENDING,
                "prior supervisor launch intent has no child receipt proving departure",
            ));
        }
        return Ok(PriorChildState::Clear { retained: None });
    };
    if matches!(
        child.stop,
        SupervisorChildStopState::Confirmed | SupervisorChildStopState::NotRequested
    ) {
        return Ok(PriorChildState::Clear {
            retained: Some(child),
        });
    }
    let birth = process_birth_identity(child.process.pid).map_err(|_| {
        Error::new(
            "MODULE_SUPERVISOR_PRIOR_CHILD_UNKNOWN",
            "prior supervisor child birth identity could not be read",
        )
    })?;
    let Some(birth) = birth else {
        return Ok(PriorChildState::Clear {
            retained: Some(child),
        });
    };
    if birth != child.process.birth {
        return Ok(PriorChildState::Clear {
            retained: Some(child),
        });
    }
    let image = process_image_identity(child.process.pid).map_err(|_| {
        Error::new(
            "MODULE_SUPERVISOR_PRIOR_CHILD_UNKNOWN",
            "prior supervisor child image identity could not be read",
        )
    })?;
    if image != child.process.image {
        return Err(Error::new(
            "MODULE_SUPERVISOR_PRIOR_CHILD_UNKNOWN",
            "prior supervisor child image identity is not an exact match",
        ));
    }
    Ok(PriorChildState::Alive(child))
}

fn spawn_intent_is_confirmed(
    readback: &swarm_supervisor::control::SupervisorChildHealthReadback,
    prior_departure: Option<&SupervisorChildHealth>,
) -> bool {
    readback.error_code.as_deref() == Some(SUPERVISOR_SPAWN_PENDING)
        && readback.child.as_ref().is_none_or(|child| {
            matches!(
                child.stop,
                SupervisorChildStopState::Confirmed | SupervisorChildStopState::NotRequested
            ) || prior_departure.is_some_and(|previous| {
                previous.process.pid == child.process.pid
                    && previous.process.birth == child.process.birth
                    && previous.process.image == child.process.image
            })
        })
}

fn process_failure_state(failures: u32, retry: Duration) -> (&'static str, Duration) {
    if failures >= SUPERVISOR_PROCESS_MAX_FAILURES {
        ("isolated", MODULE_ACTOR_ISOLATED_RETRY)
    } else {
        ("retry_wait", retry)
    }
}

fn is_retryable_child_spawn_error(error: &Error) -> bool {
    // A health-write failure is carried as a bounded secondary fact. The
    // exact child is still owned by the reaper, so retrying here could launch
    // a duplicate before that child has a durable departure receipt.
    if !error.secondary_codes.is_empty() {
        return false;
    }
    !matches!(
        error.code.as_str(),
        "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN"
            | "MODULE_SUPERVISOR_CHILD_BIRTH_CHANGED"
            | "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH"
    )
}

fn supervisor_sibling_executable() -> Result<PathBuf> {
    let host = std::env::current_exe().map_err(|_| {
        Error::new(
            "MODULE_SUPERVISOR_BINARY_PATH_UNAVAILABLE",
            "host executable path is unavailable",
        )
    })?;
    let parent = host.parent().ok_or_else(|| {
        Error::new(
            "MODULE_SUPERVISOR_BINARY_PATH_UNAVAILABLE",
            "host executable has no package directory",
        )
    })?;
    let name = if cfg!(windows) {
        "swarm-supervisor.exe"
    } else {
        "swarm-supervisor"
    };
    let executable = parent.join(name);
    if !executable.is_absolute() || !executable.is_file() {
        return Err(Error::new(
            "MODULE_SUPERVISOR_BINARY_UNAVAILABLE",
            "installed swarm-supervisor sibling executable is unavailable",
        ));
    }
    Ok(executable)
}

#[derive(Debug, Clone)]
struct SupervisorChildIdentity {
    pid: u32,
    birth: Value,
    image: Value,
}

/// Owns every live child handle created by the coordinator. If an async path
/// returns or unwinds before it explicitly reaps the child, Drop transfers the
/// same handle to one bounded health-aware reaper instead of dropping a
/// `kill_on_drop(false)` child into an untracked process.
struct SupervisorChildLease {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    diagnostic: Option<ChildDiagnosticTask>,
    identity: SupervisorChildIdentity,
    control: SupervisorControlClient,
    retain_code: &'static str,
    initial_health_result: Option<Result<()>>,
}

impl SupervisorChildLease {
    fn new(
        child: Child,
        stdin: ChildStdin,
        identity: SupervisorChildIdentity,
        control: SupervisorControlClient,
    ) -> Self {
        Self {
            child: Some(child),
            stdin: Some(stdin),
            diagnostic: None,
            identity,
            control,
            retain_code: SUPERVISOR_CHILD_RETAINED,
            initial_health_result: None,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child
            .as_mut()
            .expect("supervisor child lease is still owned")
    }

    fn set_diagnostic(&mut self, diagnostic: ChildDiagnosticTask) {
        self.diagnostic = Some(diagnostic);
    }

    fn set_initial_health_result(&mut self, result: Result<()>) {
        self.initial_health_result = Some(result);
    }

    fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.take()
    }

    fn take_diagnostic(&mut self) -> Option<ChildDiagnosticTask> {
        self.diagnostic.take()
    }

    fn take_child(&mut self) -> Option<Child> {
        self.child.take()
    }

    fn retain_with_code(&mut self, code: &'static str) {
        self.retain_code = code;
    }
}

impl Drop for SupervisorChildLease {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        let stdin = self.stdin.take();
        let diagnostic = self.diagnostic.take();
        let identity = self.identity.clone();
        let control = self.control.clone();
        let retain_code = self.retain_code;
        let initial_health_result = self.initial_health_result.take();
        if let Ok(handle) = Handle::try_current() {
            drop(handle.spawn(reap_owned_supervisor_child(
                child,
                stdin,
                diagnostic,
                identity,
                control,
                retain_code,
                initial_health_result,
            )));
        } else {
            // The lease was created inside the Tokio coordinator. If the
            // runtime is already gone, no detached async reaper can own the
            // wait. Retain the OS child rather than issue a signal or falsely
            // claim that it departed; the durable pending/uncertain receipt
            // remains the Manager's only truthful recovery boundary.
            std::mem::forget(child);
            drop(stdin);
            if let Some(task) = diagnostic {
                task.abort();
            }
        }
    }
}

impl SupervisorChildIdentity {
    fn validate(&self, executable: &Path) -> Result<()> {
        if self.pid == 0
            || self.birth.get("pid").and_then(Value::as_u64) != Some(u64::from(self.pid))
            || self.image.get("pid").and_then(Value::as_u64) != Some(u64::from(self.pid))
        {
            return Err(Error::new(
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
                "supervisor child PID or birth identity is not verifiable",
            ));
        }
        let image_path = self
            .image
            .get("image_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::new(
                    "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH",
                    "supervisor child image path is unavailable",
                )
            })?;
        let image_digest = self
            .image
            .get("image_sha256")
            .and_then(Value::as_str)
            .and_then(|value| value.strip_prefix("sha256:"))
            .filter(|value| value.len() == 64)
            .filter(|value| {
                value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            });
        if image_digest.is_none() {
            return Err(Error::new(
                "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH",
                "supervisor child image digest is unavailable",
            ));
        }
        let expected_path = executable.canonicalize().map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH",
                "configured supervisor executable could not be canonicalized",
            )
        })?;
        let observed_path = Path::new(image_path).canonicalize().map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH",
                "running supervisor image path could not be canonicalized",
            )
        })?;
        let same_path = if cfg!(windows) {
            expected_path
                .to_string_lossy()
                .eq_ignore_ascii_case(&observed_path.to_string_lossy())
        } else {
            expected_path == observed_path
        };
        if !same_path {
            return Err(Error::new(
                "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH",
                "running supervisor image does not match the installed sibling",
            ));
        }
        Ok(())
    }
}

fn capture_supervisor_child_identity(
    child: &Child,
    executable: &Path,
) -> Result<SupervisorChildIdentity> {
    let pid = child.id().ok_or_else(|| {
        Error::new(
            "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
            "supervisor child PID is unavailable",
        )
    })?;
    let birth = process_birth_identity(pid)
        .map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
                "supervisor child birth identity could not be read",
            )
        })?
        .ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
                "supervisor child exited before birth identity capture",
            )
        })?;
    let image = process_image_identity(pid).map_err(|_| {
        Error::new(
            "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
            "supervisor child image identity could not be read",
        )
    })?;
    let after = process_birth_identity(pid)
        .map_err(|_| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
                "supervisor child birth identity could not be rechecked",
            )
        })?
        .ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
                "supervisor child exited during identity capture",
            )
        })?;
    if birth != after {
        return Err(Error::new(
            "MODULE_SUPERVISOR_CHILD_BIRTH_CHANGED",
            "supervisor child birth identity changed during image capture",
        ));
    }
    let identity = SupervisorChildIdentity { pid, birth, image };
    identity.validate(executable)?;
    Ok(identity)
}

struct ChildDiagnosticTask {
    task: JoinHandle<Option<ChildError>>,
    latest: Arc<Mutex<Option<ChildError>>>,
}

impl ChildDiagnosticTask {
    fn abort(self) {
        self.task.abort();
    }
}

fn spawn_child_diagnostic(stderr: ChildStderr) -> ChildDiagnosticTask {
    let latest = Arc::new(Mutex::new(None));
    let task = tokio::spawn(read_bounded_child_diagnostic(stderr, Arc::clone(&latest)));
    ChildDiagnosticTask { task, latest }
}

async fn read_bounded_child_diagnostic(
    mut stderr: ChildStderr,
    shared_latest: Arc<Mutex<Option<ChildError>>>,
) -> Option<ChildError> {
    let mut latest = None;
    let mut line = Vec::with_capacity(SUPERVISOR_CHILD_DIAGNOSTIC_LIMIT);
    let mut line_overflowed = false;
    let mut chunk = [0_u8; 256];
    loop {
        let read = stderr.read(&mut chunk).await.ok()?;
        if read == 0 {
            if !line.is_empty() && !line_overflowed {
                latest = merge_child_diagnostic(latest, project_child_diagnostic_line(&line));
                publish_child_diagnostic(&shared_latest, &latest);
            }
            break;
        }
        for byte in &chunk[..read] {
            if *byte == b'\n' {
                if !line_overflowed {
                    latest = merge_child_diagnostic(latest, project_child_diagnostic_line(&line));
                    publish_child_diagnostic(&shared_latest, &latest);
                }
                line.clear();
                line_overflowed = false;
            } else if !line_overflowed {
                if line.len() < SUPERVISOR_CHILD_DIAGNOSTIC_LIMIT {
                    line.push(*byte);
                } else {
                    line.clear();
                    line_overflowed = true;
                }
            }
        }
    }
    latest
}

fn publish_child_diagnostic(
    shared_latest: &Arc<Mutex<Option<ChildError>>>,
    latest: &Option<ChildError>,
) {
    if let Ok(mut current) = shared_latest.lock() {
        *current = latest.clone();
    }
}

fn project_child_diagnostic_line(line: &[u8]) -> Option<ChildError> {
    let projected_line = line
        .strip_prefix(b"standalone module supervisor: ")
        .unwrap_or(line);
    if let Some(error) = project_child_error_line(projected_line) {
        return normalize_child_diagnostic(error);
    }
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let line = std::str::from_utf8(line).ok()?.trim();
    let code = line.strip_prefix("standalone module supervisor: ")?.trim();
    is_safe_optional_worker_code(code).then(|| ChildError {
        code: code.to_owned(),
        phase: None,
        message: None,
        secondary_codes: Vec::new(),
    })
}

fn normalize_child_diagnostic(mut error: ChildError) -> Option<ChildError> {
    if !is_safe_optional_worker_code(&error.code) {
        return None;
    }
    let primary = error.code.clone();
    let mut secondary_codes = Vec::with_capacity(2);
    for code in std::mem::take(&mut error.secondary_codes) {
        if is_safe_optional_worker_code(&code)
            && code != primary.as_str()
            && !secondary_codes.contains(&code)
        {
            secondary_codes.push(code);
            if secondary_codes.len() == 2 {
                break;
            }
        }
    }
    error.secondary_codes = secondary_codes;
    Some(error)
}

fn merge_child_diagnostic(
    previous: Option<ChildError>,
    next: Option<ChildError>,
) -> Option<ChildError> {
    let Some(next) = next else {
        return previous;
    };
    match previous {
        Some(previous)
            if previous.code == next.code
                && !previous.secondary_codes.is_empty()
                && next.secondary_codes.is_empty() =>
        {
            Some(previous)
        }
        _ => Some(next),
    }
}

async fn join_child_diagnostic(task: Option<ChildDiagnosticTask>) -> Option<ChildError> {
    let ChildDiagnosticTask { mut task, latest } = task?;
    match time::timeout(SUPERVISOR_PROCESS_STOP_TIMEOUT, &mut task).await {
        Ok(Ok(code)) => code.or_else(|| latest.lock().ok().and_then(|value| value.clone())),
        Ok(Err(_)) | Err(_) => {
            task.abort();
            latest.lock().ok().and_then(|value| value.clone())
        }
    }
}

async fn reap_failed_supervisor_child(
    mut child: Child,
    stdin: Option<ChildStdin>,
    identity: Option<SupervisorChildIdentity>,
    control: &SupervisorControlClient,
    retain_code: &'static str,
) -> Result<()> {
    let stdin = stdin.or_else(|| child.stdin.take());
    let diagnostic = child.stderr.take().map(spawn_child_diagnostic);
    let Some(identity) = identity else {
        // Without a verified birth/image receipt the coordinator cannot safely
        // claim this process as a replaceable owner. Keep the exact handle in
        // a one-shot reaper and let the caller surface the identity failure.
        if let Ok(handle) = Handle::try_current() {
            drop(handle.spawn(reap_unidentified_supervisor_child(child, stdin, diagnostic)));
        } else {
            std::mem::forget(child);
            drop(stdin);
            if let Some(task) = diagnostic {
                task.abort();
            }
        }
        return Ok(());
    };
    let retained_health =
        supervisor_child_health(&identity, None, SupervisorChildStopState::Uncertain);
    let health_result = record_module_actor_health(
        control,
        "isolated",
        1,
        Some(retain_code),
        Some(MODULE_ACTOR_ISOLATED_RETRY),
        Some(&retained_health),
    )
    .await;
    let reaper_health_result = health_result.clone();
    if let Ok(handle) = Handle::try_current() {
        drop(handle.spawn(reap_owned_supervisor_child(
            child,
            stdin,
            diagnostic,
            identity,
            control.clone(),
            retain_code,
            Some(reaper_health_result),
        )));
    } else {
        // With no runtime there is no bounded reaper task to await; retaining
        // the exact child leaves durable Store uncertainty for Manager rather
        // than inventing a departure receipt.
        std::mem::forget(child);
        drop(stdin);
        if let Some(task) = diagnostic {
            task.abort();
        }
    }
    health_result
}

async fn reap_unidentified_supervisor_child(
    mut child: Child,
    stdin: Option<ChildStdin>,
    diagnostic: Option<ChildDiagnosticTask>,
) {
    let diagnostic = diagnostic.or_else(|| child.stderr.take().map(spawn_child_diagnostic));
    drop(stdin);
    let _ = child.wait().await;
    let _ = join_child_diagnostic(diagnostic).await;
}

/// Reap the exact child handle transferred by `SupervisorChildLease::drop`.
/// This is a one-shot wait, not a new health poller. When no health write was
/// attempted, the reaper may make the still-live identity durable; an attempted
/// but rejected write is carried in `initial_health_result` and never replaced
/// by a synthetic receipt. The final receipt records the same child
/// incarnation after the wait completes.
async fn reap_owned_supervisor_child(
    mut child: Child,
    stdin: Option<ChildStdin>,
    diagnostic: Option<ChildDiagnosticTask>,
    identity: SupervisorChildIdentity,
    control: SupervisorControlClient,
    retain_code: &'static str,
    initial_health_result: Option<Result<()>>,
) {
    // `None` means no initial health write was attempted. `Some(Ok(()))` is
    // already durable, while `Some(Err(_))` deliberately leaves the pending
    // launch intent/uncertainty untouched; writing a synthetic receipt here
    // would claim health that Store rejected.
    let diagnostic = diagnostic.or_else(|| child.stderr.take().map(spawn_child_diagnostic));
    if initial_health_result.is_none() {
        let retained_health =
            supervisor_child_health(&identity, None, SupervisorChildStopState::Uncertain);
        let _ = record_module_actor_health(
            &control,
            "isolated",
            1,
            Some(retain_code),
            Some(MODULE_ACTOR_ISOLATED_RETRY),
            Some(&retained_health),
        )
        .await;
    }
    // EOF is the existing supervisor protocol's graceful release request. No
    // OS signal or broad kill is issued; the exact Child remains owned below.
    drop(stdin);
    let exit = Some(child.wait().await);
    let diagnostic = join_child_diagnostic(diagnostic).await;
    let child_exit = child_exit_from_result(&exit, diagnostic.as_ref());
    let stop = match &exit {
        Some(Ok(_)) => SupervisorChildStopState::Confirmed,
        Some(Err(_)) | None => SupervisorChildStopState::Uncertain,
    };
    let child_health = supervisor_child_health(&identity, Some(child_exit), stop);
    let _ = record_module_actor_health(
        &control,
        "isolated",
        1,
        Some(retain_code),
        Some(MODULE_ACTOR_ISOLATED_RETRY),
        Some(&child_health),
    )
    .await;
}

fn supervisor_child_health(
    identity: &SupervisorChildIdentity,
    exit: Option<SupervisorChildExit>,
    stop: SupervisorChildStopState,
) -> SupervisorChildHealth {
    SupervisorChildHealth {
        process: SupervisorChildProcessIdentity {
            pid: identity.pid,
            birth: identity.birth.clone(),
            image: identity.image.clone(),
        },
        stop,
        exit,
    }
}

fn child_exit_from_status(
    status: &ExitStatus,
    diagnostic: Option<&ChildError>,
) -> SupervisorChildExit {
    SupervisorChildExit {
        category: if status.code().is_some() {
            SupervisorChildExitCategory::Exited
        } else {
            SupervisorChildExitCategory::Signaled
        },
        code: status.code(),
        error_code: diagnostic.map(|error| error.code.clone()),
        secondary_codes: diagnostic
            .map(|error| error.secondary_codes.clone())
            .unwrap_or_default(),
    }
}

fn child_exit_from_result(
    exit: &Option<std::result::Result<ExitStatus, std::io::Error>>,
    diagnostic: Option<&ChildError>,
) -> SupervisorChildExit {
    match exit {
        Some(Ok(status)) => child_exit_from_status(status, diagnostic),
        Some(Err(_)) | None => SupervisorChildExit {
            category: SupervisorChildExitCategory::WaitError,
            code: None,
            error_code: diagnostic.map(|error| error.code.clone()),
            secondary_codes: diagnostic
                .map(|error| error.secondary_codes.clone())
                .unwrap_or_default(),
        },
    }
}

async fn spawn_supervisor_child(
    control: &SupervisorControlClient,
    executable: &Path,
    frame: &[u8],
) -> Result<(Child, ChildStdin, SupervisorChildIdentity)> {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    #[cfg(windows)]
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "MODULE_SUPERVISOR_PROCESS_START_FAILED",
            "installed supervisor process could not be started",
        )
    })?;
    let identity = match capture_supervisor_child_identity(&child, executable) {
        Ok(identity) => identity,
        Err(error) => {
            let _ = reap_failed_supervisor_child(
                child,
                None,
                None,
                control,
                "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
            )
            .await;
            return Err(error);
        }
    };
    let Some(mut stdin) = child.stdin.take() else {
        let error = Error::new(
            "MODULE_SUPERVISOR_BOOTSTRAP_PIPE_UNAVAILABLE",
            "supervisor bootstrap pipe is unavailable",
        );
        return match reap_failed_supervisor_child(
            child,
            None,
            Some(identity),
            control,
            "MODULE_SUPERVISOR_BOOTSTRAP_PIPE_UNAVAILABLE",
        )
        .await
        {
            Ok(()) => Err(error),
            Err(health_error) => Err(error.with_secondary_error(health_error)),
        };
    };
    if stdin.write_all(frame).await.is_err() || stdin.flush().await.is_err() {
        drop(stdin);
        let error = Error::new(
            "MODULE_SUPERVISOR_BOOTSTRAP_WRITE_FAILED",
            "supervisor bootstrap frame could not be delivered",
        );
        return match reap_failed_supervisor_child(
            child,
            None,
            Some(identity),
            control,
            "MODULE_SUPERVISOR_BOOTSTRAP_WRITE_FAILED",
        )
        .await
        {
            Ok(()) => Err(error),
            Err(health_error) => Err(error.with_secondary_error(health_error)),
        };
    }
    Ok((child, stdin, identity))
}
async fn record_module_actor_status(
    control: &SupervisorControlClient,
    state: &'static str,
    failures: u32,
    error_code: Option<&str>,
    retry: Option<Duration>,
) -> Result<()> {
    record_module_actor_health(control, state, failures, error_code, retry, None).await
}

async fn record_module_actor_health(
    control: &SupervisorControlClient,
    state: &'static str,
    failures: u32,
    error_code: Option<&str>,
    retry: Option<Duration>,
    child: Option<&SupervisorChildHealth>,
) -> Result<()> {
    let error_code = error_code.map(safe_optional_worker_code);
    let retry_in_ms = retry.map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    control
        .record_health_with_child(state, failures, error_code.as_deref(), retry_in_ms, child)
        .await
        .map_err(module_error)
}

fn safe_optional_worker_code(code: &str) -> String {
    if is_safe_optional_worker_code(code) {
        code.to_owned()
    } else {
        "SUPERVISOR_ERROR".to_owned()
    }
}

fn is_safe_optional_worker_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
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

#[cfg(all(test, any(windows, target_os = "linux")))]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        ipc,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::{Path, PathBuf},
        process::{Command as StdCommand, Stdio},
        sync::Arc,
        thread,
        time::Duration as StdDuration,
    };
    use tokio::{
        process::Command,
        sync::watch,
        task::JoinHandle,
        time::{self, Duration},
    };

    const ROLE_ENV: &str = "ELIOT_SWARM_UNKNOWN362_ROLE";
    const ROOT_ENV: &str = "ELIOT_SWARM_UNKNOWN362_ROOT";
    const RELEASE_ENV: &str = "ELIOT_SWARM_UNKNOWN362_RELEASE";
    const LAUNCHES_ENV: &str = "ELIOT_SWARM_UNKNOWN362_LAUNCHES";
    const CHILD_RELEASE_ENV: &str = "ELIOT_SWARM_UNKNOWN362_CHILD_RELEASE";
    const MAIN_TEST: &str = "host_module_supervisor::tests::unknown362_identity_failure_and_health_write_failure_survive_restart";
    const CHILD_TEST: &str = "host_module_supervisor::tests::child_waits_for_release";

    #[tokio::test]
    async fn unknown362_identity_failure_and_health_write_failure_survive_restart() {
        match std::env::var(ROLE_ENV).as_deref() {
            Ok("host-a") => run_host_a().await,
            Ok("host-b") => run_host_b().await,
            _ => run_outer_harness().await,
        }
    }

    #[test]
    fn child_waits_for_release() {
        let Ok(release) = std::env::var(CHILD_RELEASE_ENV) else {
            return;
        };
        let release = PathBuf::from(release);
        while !release.exists() {
            thread::sleep(StdDuration::from_millis(20));
        }
    }

    struct TestIpcRuntime {
        owner: StoreOwner,
        control: SupervisorControlClient,
        stop: watch::Sender<bool>,
        accept: JoinHandle<()>,
    }

    impl TestIpcRuntime {
        async fn start(root: &Path) -> Self {
            let data_root = DataRoot::acquire(root).expect("acquire temporary Store root");
            let config = test_config(&data_root.path);
            let credential = bootstrap_credential(&data_root.path)
                .expect("load temporary Store operator credential");
            let owner = StoreOwner::start(data_root, config.clone(), credential)
                .await
                .expect("start temporary Store owner");
            let (stop, mut stopping) = watch::channel(false);
            let mut listener = ipc::Listener::bind(root).expect("bind real Store IPC");
            let store = owner.store.clone();
            let ipc_config = Arc::new(config.ipc.clone());
            let accept = tokio::spawn(async move {
                loop {
                    tokio::select! {
                        changed = stopping.changed() => {
                            if changed.is_err() || *stopping.borrow() {
                                break;
                            }
                        }
                        accepted = listener.accept() => match accepted {
                            Ok(stream) => {
                                let store = store.clone();
                                let ipc_config = ipc_config.clone();
                                let stopping = stopping.clone();
                                tokio::spawn(async move {
                                    let _ = ipc::serve(stream, store, ipc_config, stopping).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            });
            let control = SupervisorControlClient::new(
                root.to_path_buf(),
                owner.module_supervisor_credential(),
                config.ipc.clone(),
            )
            .expect("create authenticated supervisor IPC client");
            Self {
                owner,
                control,
                stop,
                accept,
            }
        }

        async fn close(self) {
            let _ = self.stop.send(true);
            let mut accept = self.accept;
            if time::timeout(Duration::from_secs(3), &mut accept)
                .await
                .is_err()
            {
                accept.abort();
            }
            self.owner
                .close()
                .await
                .expect("close temporary Store owner");
        }
    }

    fn test_config(root: &Path) -> Arc<Config> {
        let mut config = Config::default();
        config.storage.data_dir = root.to_path_buf();
        Arc::new(config)
    }

    async fn initialize_store(root: &Path) {
        fs::create_dir_all(root).expect("create temporary Store directory");
        let root = fs::canonicalize(root).expect("canonicalize temporary Store directory");
        let data_root = DataRoot::acquire(&root).expect("acquire temporary Store root");
        let credential = bootstrap_credential(&data_root.path)
            .expect("create temporary Store operator credential");
        StoreOwner::start(data_root, test_config(&root), credential)
            .await
            .expect("initialize temporary Store")
            .close()
            .await
            .expect("close temporary Store initializer");
    }

    fn install_identity_health_rejection(root: &Path) {
        let db = Connection::open(root.join("swarm.db")).expect("open temporary Store database");
        db.execute_batch(
            r#"CREATE TRIGGER reject_unknown362_health
               BEFORE UPDATE OF value_json ON meta
               WHEN OLD.key = 'host:optional-workers:v1'
                AND instr(NEW.value_json,
                    '"last_error_code":"MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH"') > 0
               BEGIN
                   SELECT RAISE(ABORT, 'fixture rejected child identity health');
               END;"#,
        )
        .expect("install exact SQLite health-write rejection");
    }

    async fn run_outer_harness() {
        let root =
            std::env::temp_dir().join(format!("eliot-unknown362-{}", crate::model::new_id()));
        fs::create_dir_all(&root).expect("create exact test directory");
        let root = fs::canonicalize(root).expect("canonicalize exact test directory");
        let release = root.join("release-child");
        let launches = root.join("launches.jsonl");
        let mut release_guard = ReleaseFileGuard::new(release.clone());
        initialize_store(&root).await;
        install_identity_health_rejection(&root);

        let mut host_a = spawn_host_process("host-a", &root, &release, &launches);
        let host_a_status = host_a.wait().expect("wait for first host generation");
        assert!(host_a_status.success(), "first host generation failed");
        let first_launch = read_launches(&launches)
            .into_iter()
            .next()
            .expect("first generation recorded its exact child");
        let (pid, birth) = launch_identity(&first_launch);
        assert_same_live_process(pid, &birth, "after first host teardown");

        let mut host_b = spawn_host_process("host-b", &root, &release, &launches);
        let host_b_status = host_b.wait().expect("wait for restarted host generation");
        let launch_records = read_launches(&launches);
        assert_same_live_process(pid, &birth, "after restart reconciliation");

        release_guard.release();
        for record in &launch_records {
            let (child_pid, child_birth) = launch_identity(record);
            wait_for_process_departure(child_pid, &child_birth).await;
        }

        assert!(host_b_status.success(), "restarted host generation failed");
        assert_eq!(
            launch_records.len(),
            1,
            "retained pending launch must block a duplicate child"
        );
        fs::remove_dir_all(&root).expect("remove exact temporary Store directory");
    }

    fn spawn_host_process(
        role: &str,
        root: &Path,
        release: &Path,
        launches: &Path,
    ) -> std::process::Child {
        StdCommand::new(std::env::current_exe().expect("resolve current test executable"))
            .args(["--exact", MAIN_TEST, "--nocapture"])
            .env(ROLE_ENV, role)
            .env(ROOT_ENV, root)
            .env(RELEASE_ENV, release)
            .env(LAUNCHES_ENV, launches)
            .spawn()
            .expect("start isolated host-generation test process")
    }

    async fn run_host_a() {
        let root = env_path(ROOT_ENV);
        let release = env_path(RELEASE_ENV);
        let launches = env_path(LAUNCHES_ENV);
        let runtime = TestIpcRuntime::start(&root).await;
        record_module_actor_status(
            &runtime.control,
            "isolated",
            0,
            Some(SUPERVISOR_SPAWN_PENDING),
            Some(MODULE_ACTOR_ISOLATED_RETRY),
        )
        .await
        .expect("persist launch-pending fence before child start");

        let child = spawn_fixture_child(&release, true);
        let pid = child.id().expect("test child has a PID");
        append_launch_record(&launches, pid).await;
        let capture_error = capture_supervisor_child_identity(&child, &root)
            .expect_err("real process capture must reject the wrong expected image");
        assert_eq!(capture_error.code, "MODULE_SUPERVISOR_CHILD_IMAGE_MISMATCH");
        reap_failed_supervisor_child(
            child,
            None,
            None,
            &runtime.control,
            "MODULE_SUPERVISOR_CHILD_IDENTITY_UNKNOWN",
        )
        .await
        .expect("transfer the exact unidentified Child to its reaper");

        let rejected = record_module_actor_status(
            &runtime.control,
            "isolated",
            1,
            Some(&capture_error.code),
            Some(MODULE_ACTOR_ISOLATED_RETRY),
        )
        .await
        .expect_err("SQLite trigger must reject the child-failure health update");
        assert_eq!(rejected.code, "STORE_ERROR");
        let retained = runtime
            .control
            .read_health()
            .await
            .expect("read retained health through authenticated IPC");
        assert_eq!(
            retained.error_code.as_deref(),
            Some(SUPERVISOR_SPAWN_PENDING)
        );
        assert!(
            retained.child.is_none(),
            "no unverified child receipt was written"
        );
        runtime.close().await;
    }

    async fn run_host_b() {
        let root = env_path(ROOT_ENV);
        let release = env_path(RELEASE_ENV);
        let launches = env_path(LAUNCHES_ENV);
        let runtime = TestIpcRuntime::start(&root).await;
        match reconcile_prior_child(&runtime.control).await {
            Err(error) if error.code == SUPERVISOR_SPAWN_PENDING => {}
            Ok(PriorChildState::Clear { .. }) => {
                let child = spawn_fixture_child(&release, false);
                append_launch_record(&launches, child.id().expect("replacement child has PID"))
                    .await;
                drop(child);
            }
            Ok(PriorChildState::Alive(_)) => {
                panic!("no child receipt exists to adopt during this restart")
            }
            Err(error) => panic!("restart returned an unexpected gate error: {}", error.code),
        }
        runtime.close().await;
    }

    fn env_path(name: &str) -> PathBuf {
        PathBuf::from(std::env::var_os(name).expect("required subprocess fixture path"))
    }

    fn spawn_fixture_child(release: &Path, pipe_stdin: bool) -> Child {
        let mut command = Command::new(std::env::current_exe().expect("resolve test executable"));
        command
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_RELEASE_ENV, release)
            .stdin(if pipe_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        command.spawn().expect("start real fixture child process")
    }

    async fn append_launch_record(path: &Path, pid: u32) {
        let birth = wait_for_birth(pid).await;
        let record = json!({"pid":pid,"birth":birth});
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open child launch ledger");
        writeln!(file, "{record}").expect("append child launch identity");
    }

    async fn wait_for_birth(pid: u32) -> Value {
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(identity) =
                    process_birth_identity(pid).expect("read test child birth identity")
                {
                    return identity;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("test child birth identity became readable")
    }

    fn read_launches(path: &Path) -> Vec<Value> {
        fs::read_to_string(path)
            .expect("read child launch ledger")
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse child launch identity"))
            .collect()
    }

    fn launch_identity(record: &Value) -> (u32, Value) {
        (
            record["pid"].as_u64().expect("launch PID is numeric") as u32,
            record["birth"].clone(),
        )
    }

    fn assert_same_live_process(pid: u32, expected_birth: &Value, phase: &str) {
        let actual = process_birth_identity(pid)
            .expect("read process identity during restart test")
            .unwrap_or_else(|| panic!("original child departed {phase}"));
        assert_eq!(&actual, expected_birth, "PID was reused {phase}");
    }

    async fn wait_for_process_departure(pid: u32, expected_birth: &Value) {
        time::timeout(Duration::from_secs(10), async {
            loop {
                let current = process_birth_identity(pid)
                    .expect("verify exact child departure after release");
                if current.as_ref() != Some(expected_birth) {
                    return;
                }
                time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("released fixture child departed");
    }

    struct ReleaseFileGuard {
        path: PathBuf,
        armed: bool,
    }

    impl ReleaseFileGuard {
        fn new(path: PathBuf) -> Self {
            Self { path, armed: true }
        }

        fn release(&mut self) {
            fs::write(&self.path, b"release").expect("release exact fixture child");
            self.armed = false;
        }
    }

    impl Drop for ReleaseFileGuard {
        fn drop(&mut self) {
            if self.armed {
                let _ = fs::write(&self.path, b"release");
            }
        }
    }
}
