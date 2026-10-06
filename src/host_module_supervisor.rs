//! Optional host handoff joining durable Store demand to the independently
//! installed module-supervisor sibling. The handoff is deliberately outside
//! `host::run_until`'s required supervisor JoinSet: a module failure cannot
//! close the IPC listener or stop unrelated workers.

use crate::{
    error::{Error, Result},
    store::Store,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use swarm_contracts::{Credential, module_catalog::ProtectedRef};
use swarm_supervisor::{
    ModuleOwnerExecutable, StandaloneRouteConfigMapper, StandaloneSupervisorConfig,
    SupervisorBootstrap, SupervisorControlClient,
};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, ChildStdin, Command},
    sync::watch,
    task::JoinHandle,
    time,
};

const MODULE_ACTOR_RETRY_MAX: Duration = Duration::from_secs(30);
const MODULE_ACTOR_ISOLATED_RETRY: Duration = Duration::from_secs(60);
const MAX_ADDITIONAL_PROTECTED_FILES: usize = 128;

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
        let value = Self {
            install_root,
            descriptor_files: config.descriptor_files.clone(),
            state_root: root.join("module-supervisor/state"),
            resolver_root: root.join("module-supervisor/resolver"),
            owner_helper,
            protected_files,
            route_config_mapper: config.route_config_mapper,
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
    task: JoinHandle<()>,
}

impl OptionalModuleSupervisor {
    pub(crate) async fn join(self) {
        let _ = self.task.await;
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
    config: crate::config::ModuleSupervisorConfig,
    stopping: watch::Receiver<bool>,
) -> OptionalModuleSupervisor {
    let task = tokio::spawn(async move {
        let control = match SupervisorControlClient::new(
            root.clone(),
            supervisor_credential.clone(),
            ipc.clone(),
        ) {
            Ok(control) => control,
            Err(error) => {
                eprintln!("optional module supervisor IPC unavailable: {}", error.code);
                return;
            }
        };
        let host_config = match ModuleSupervisorHostConfig::from_runtime_config(&config, &root) {
            Ok(Some(value)) => value,
            Ok(None) => return,
            Err(error) => {
                record_module_actor_status(
                    &control,
                    "isolated",
                    1,
                    Some(&error.code),
                    Some(MODULE_ACTOR_ISOLATED_RETRY),
                )
                .await;
                eprintln!(
                    "optional module supervisor configuration unavailable: {}",
                    error.code
                );
                return;
            }
        };
        let bootstrap = match host_config.standalone_bootstrap(&root, ipc, supervisor_credential) {
            Ok(bootstrap) => bootstrap,
            Err(error) => {
                record_module_actor_status(
                    &control,
                    "isolated",
                    1,
                    Some(&error.code),
                    Some(MODULE_ACTOR_ISOLATED_RETRY),
                )
                .await;
                eprintln!(
                    "optional module supervisor bootstrap unavailable: {}",
                    error.code
                );
                return;
            }
        };
        run_supervisor_process_loop(store, control, bootstrap, stopping).await;
    });
    OptionalModuleSupervisor { task }
}

/// The host supplies the existing Store only as an activation oracle. No Store
/// object or database capability is serialized into the private bootstrap.
pub(crate) fn spawn_isolated_module_supervisor(
    store: Store,
    supervisor_credential: Credential,
    root: PathBuf,
    ipc: swarm_client::IpcConfig,
    config: crate::config::ModuleSupervisorConfig,
    stopping: watch::Receiver<bool>,
) -> OptionalModuleSupervisor {
    spawn_independent_module_supervisor(store, supervisor_credential, root, ipc, config, stopping)
}

const SUPERVISOR_PROCESS_BASE_RETRY: Duration = Duration::from_millis(250);
const SUPERVISOR_PROCESS_MAX_FAILURES: u32 = 5;
const SUPERVISOR_PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(5);
const SUPERVISOR_PROCESS_STABLE: Duration = Duration::from_secs(5);
const SUPERVISOR_DEMAND_PAGE_LIMIT: usize = 256;

async fn run_supervisor_process_loop(
    store: Store,
    control: SupervisorControlClient,
    bootstrap: SupervisorBootstrap,
    stopping: watch::Receiver<bool>,
) {
    let mut stopping = stopping;
    let mut demand_changes = store.subscribe_module_demand_changes();
    let mut failures = 0_u32;
    let mut retry = SUPERVISOR_PROCESS_BASE_RETRY;
    let frame = match bootstrap.to_frame() {
        Ok(frame) => frame,
        Err(error) => {
            record_module_actor_status(
                &control,
                "isolated",
                1,
                Some(&error.code),
                Some(MODULE_ACTOR_ISOLATED_RETRY),
            )
            .await;
            return;
        }
    };
    loop {
        let demanded =
            match wait_for_module_demand(&store, &mut demand_changes, &mut stopping).await {
                Ok(demanded) => demanded,
                Err(error) => {
                    failures = failures.saturating_add(1).min(32);
                    let (state, delay) = process_failure_state(failures, retry);
                    record_module_actor_status(
                        &control,
                        state,
                        failures,
                        Some(&error.code),
                        Some(delay),
                    )
                    .await;
                    if !wait_for_supervisor_backoff(delay, &mut stopping).await {
                        return;
                    }
                    retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                    continue;
                }
            };
        if !demanded {
            return;
        }

        let executable = match supervisor_sibling_executable() {
            Ok(path) => path,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                record_module_actor_status(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                )
                .await;
                if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
                    return;
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };

        let started_at = time::Instant::now();
        let (mut child, bootstrap_pipe) = match spawn_supervisor_child(&executable, &frame).await {
            Ok(child) => child,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                record_module_actor_status(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                )
                .await;
                if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
                    return;
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };
        record_module_actor_status(&control, "running", failures, None, None).await;

        let mut bootstrap_pipe = Some(bootstrap_pipe);
        let exit = tokio::select! {
            status = child.wait() => Some(status),
            _changed = stopping.changed() => {
                // Dropping the only stdin handle is the child protocol's
                // graceful stop. The supervisor then releases only its own
                // leases; it never broad-kills native owners or adapters.
                drop(bootstrap_pipe.take());
                let stopped = time::timeout(SUPERVISOR_PROCESS_STOP_TIMEOUT, child.wait()).await;
                match stopped {
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) => {
                        record_module_actor_status(
                            &control,
                            "isolated",
                            1,
                            Some("SUPERVISOR_STOP_UNKNOWN"),
                            Some(MODULE_ACTOR_ISOLATED_RETRY),
                        )
                        .await;
                    }
                    Err(_) => {
                        record_module_actor_status(
                            &control,
                            "isolated",
                            1,
                            Some("SUPERVISOR_STOP_TIMEOUT"),
                            Some(MODULE_ACTOR_ISOLATED_RETRY),
                        )
                        .await;
                        // kill_on_drop(false) keeps this exact child alive if
                        // EOF did not complete in time. Dropping its handle
                        // preserves truthful stop uncertainty without a
                        // destructive signal or unbounded host drain.
                    }
                }
                return;
            }
        };
        drop(bootstrap_pipe);
        let status_code = match exit {
            Some(Ok(status)) if status.success() => "SUPERVISOR_STOPPED",
            Some(Ok(_)) => "SUPERVISOR_EXITED",
            Some(Err(_)) | None => "SUPERVISOR_STATUS_UNKNOWN",
        };
        let demanded = match module_supervisor_has_demand(&store).await {
            Ok(demanded) => demanded,
            Err(error) => {
                failures = failures.saturating_add(1).min(32);
                let (state, delay) = process_failure_state(failures, retry);
                record_module_actor_status(
                    &control,
                    state,
                    failures,
                    Some(&error.code),
                    Some(delay),
                )
                .await;
                if !wait_for_supervisor_backoff(delay, &mut stopping).await {
                    return;
                }
                retry = (retry * 2).min(MODULE_ACTOR_RETRY_MAX);
                continue;
            }
        };
        if !demanded {
            record_module_actor_status(&control, "dormant", 0, None, None).await;
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
        record_module_actor_status(&control, state, failures, Some(status_code), Some(delay)).await;
        if !wait_for_supervisor_retry(delay, &mut demand_changes, &mut stopping).await {
            return;
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

fn process_failure_state(failures: u32, retry: Duration) -> (&'static str, Duration) {
    if failures >= SUPERVISOR_PROCESS_MAX_FAILURES {
        ("isolated", MODULE_ACTOR_ISOLATED_RETRY)
    } else {
        ("retry_wait", retry)
    }
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

async fn spawn_supervisor_child(executable: &Path, frame: &[u8]) -> Result<(Child, ChildStdin)> {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.wait().await;
        return Err(Error::new(
            "MODULE_SUPERVISOR_BOOTSTRAP_PIPE_UNAVAILABLE",
            "supervisor bootstrap pipe is unavailable",
        ));
    };
    if stdin.write_all(frame).await.is_err() || stdin.flush().await.is_err() {
        drop(stdin);
        let _ = child.wait().await;
        return Err(Error::new(
            "MODULE_SUPERVISOR_BOOTSTRAP_WRITE_FAILED",
            "supervisor bootstrap frame could not be delivered",
        ));
    }
    Ok((child, stdin))
}
async fn record_module_actor_status(
    control: &SupervisorControlClient,
    state: &'static str,
    failures: u32,
    error_code: Option<&str>,
    retry: Option<Duration>,
) {
    let error_code = error_code.map(safe_optional_worker_code);
    let retry_in_ms = retry.map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    if let Err(error) = control
        .record_health(state, failures, error_code.as_deref(), retry_in_ms)
        .await
    {
        eprintln!(
            "module supervisor health readback unavailable: {}",
            error.code
        );
    }
}

fn safe_optional_worker_code(code: &str) -> String {
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
