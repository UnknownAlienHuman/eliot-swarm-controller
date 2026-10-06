//! Lazy, isolated lifecycle for the Store's legacy host reconcilers.
//!
//! One coordinator owns the wake/timer for all eleven workers. A worker exists
//! only while its exact config or retained-work predicate is true. Failures
//! are visible through Store `host.status`, paced with bounded backoff, and
//! never terminate the IPC listener unless the Store/kernel itself failed.
use crate::{
    error::{Error, Result},
    store::{LegacyWorkerDemand, Store},
};
use std::{collections::BTreeMap, process::Stdio, time::Duration};
use tokio::process::Command;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

const COORDINATOR_TICK: Duration = Duration::from_secs(2);
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
const MAX_FAILURES_PER_WINDOW: u32 = 5;
const BASE_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(30);
const ISOLATED_RETRY: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Worker {
    Checks,
    Scripts,
    OpenCode,
    Zed,
    Scheduler,
    AutomationScheduler,
    Automation,
    Launcher,
    NativeMcp,
    NativeMcpTools,
    Forge,
}

impl Worker {
    const ALL: [Self; 11] = [
        Self::Checks,
        Self::Scripts,
        Self::OpenCode,
        Self::Zed,
        Self::Scheduler,
        Self::AutomationScheduler,
        Self::Automation,
        Self::Launcher,
        Self::NativeMcp,
        Self::NativeMcpTools,
        Self::Forge,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Checks => "checks",
            Self::Scripts => "scripts",
            Self::OpenCode => "opencode",
            Self::Zed => "zed",
            Self::Scheduler => "scheduler",
            Self::AutomationScheduler => "automation-scheduler",
            Self::Automation => "automation",
            Self::Launcher => "launcher",
            Self::NativeMcp => "native-mcp",
            Self::NativeMcpTools => "native-mcp-tools",
            Self::Forge => "forge",
        }
    }

    const fn demanded(self, demand: LegacyWorkerDemand) -> bool {
        match self {
            Self::Checks => demand.checks,
            Self::Scripts => demand.scripts,
            Self::OpenCode => demand.opencode,
            Self::Zed => demand.zed,
            Self::Scheduler => demand.scheduler,
            Self::AutomationScheduler => demand.automation_scheduler,
            Self::Automation => demand.automation,
            Self::Launcher => demand.launcher,
            Self::NativeMcp => demand.native_mcp,
            Self::NativeMcpTools => demand.native_mcp_tools,
            Self::Forge => demand.forge,
        }
    }
}

struct Slot {
    task: Option<JoinHandle<Result<()>>>,
    stop: Option<watch::Sender<bool>>,
    failure_window_started: Instant,
    failures: u32,
    retry_at: Instant,
}

impl Slot {
    fn new(now: Instant) -> Self {
        Self {
            task: None,
            stop: None,
            failure_window_started: now,
            failures: 0,
            retry_at: now,
        }
    }
}

pub(super) async fn run(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut changed = store.subscribe_legacy_worker_demand_changes();
    let mut tick = tokio::time::interval(COORDINATOR_TICK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut slots = Worker::ALL
        .into_iter()
        .map(|worker| (worker, Slot::new(Instant::now())))
        .collect::<BTreeMap<_, _>>();

    let result = 'run: loop {
        if *stopping.borrow() {
            break 'run Ok(());
        }

        if let Err(error) = reap_finished(&store, &mut slots).await {
            break Err(error);
        }

        // This read is the only activation oracle for retained work. A broken
        // Store snapshot cannot safely mean "no demand", so it remains fatal.
        let demand = match store.legacy_worker_demand_snapshot().await {
            Ok(demand) => demand,
            Err(error) => {
                if error.code.starts_with("STORE_") {
                    break 'run Err(error);
                } else {
                    break 'run Err(Error::new(
                        "STORE_DEMAND_READBACK_FAILED",
                        "legacy worker demand readback failed",
                    ));
                }
            }
        };
        for worker in Worker::ALL {
            let slot = slots
                .get_mut(&worker)
                .expect("every legacy worker has one bounded slot");
            if worker.demanded(demand) {
                if slot.task.is_none()
                    && Instant::now() >= slot.retry_at
                    && let Err(error) = start_worker(&store, worker, slot).await
                {
                    break 'run Err(error);
                }
            } else if slot.task.is_some()
                && let Err(error) = stop_worker(&store, worker, slot).await
            {
                break 'run Err(error);
            }
        }

        // Keep the bounded readback/retry cadence while any worker is selected
        // or any slot is still active (including a just-finished task that
        // must be reaped). A stored retry time is runnable only while its
        // demand remains selected; a later demand change wakes this watch and
        // preserves the original backoff deadline. With no demand or active
        // slot, the guarded tick is not polled and the coordinator is quiescent.
        let periodic_reconciliation = Worker::ALL
            .into_iter()
            .any(|worker| worker.demanded(demand))
            || slots.values().any(|slot| slot.task.is_some());
        let wake = tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() {
                    break 'run Ok(());
                }
                false
            }
            result = changed.changed() => {
                if result.is_err() {
                    break 'run Err(Error::new("STORE_CLOSED", "legacy worker demand stream ended"));
                }
                true
            }
            _ = tick.tick(), if periodic_reconciliation => true
        };
        if !wake {
            break 'run Ok(());
        }
    };
    let persist_cleanup_status = !result.as_ref().is_err_and(is_store_or_kernel_failure);
    let cleanup = stop_all(&store, &mut slots, persist_cleanup_status).await;
    match (result, cleanup) {
        (Err(error), Err(cleanup_error)) => {
            eprintln!("legacy worker cleanup: {}", cleanup_error.code);
            Err(error)
        }
        (Err(error), _) => Err(error),
        (Ok(()), cleanup) => cleanup,
    }
}

async fn start_worker(store: &Store, worker: Worker, slot: &mut Slot) -> Result<()> {
    store
        .record_legacy_worker_status(worker.name(), "running", slot.failures, None, None)
        .await?;
    let (worker_stop, worker_stopping) = watch::channel(false);
    let worker_store = store.clone();
    let task = tokio::spawn(async move { run_worker(worker, worker_store, worker_stopping).await });
    slot.stop = Some(worker_stop);
    slot.task = Some(task);
    Ok(())
}

async fn run_worker(worker: Worker, store: Store, stopping: watch::Receiver<bool>) -> Result<()> {
    match worker {
        Worker::Checks => {
            store.supervise_checks(stopping).await;
            Ok(())
        }
        Worker::Scripts => store.supervise_scripts(stopping).await,
        Worker::OpenCode => store.supervise_opencode(stopping).await,
        Worker::Zed => {
            store.supervise_zed(stopping).await;
            Ok(())
        }
        Worker::Scheduler => crate::scheduler::run(store, stopping).await,
        Worker::AutomationScheduler => run_automation_scheduler_worker(store, stopping).await,
        Worker::Automation => super::supervise_automation(store, stopping).await,
        Worker::Launcher => super::supervise_launcher(store, stopping).await,
        Worker::NativeMcp => super::supervise_native_mcp(store, stopping).await,
        Worker::NativeMcpTools => super::supervise_native_mcp_tools(store, stopping).await,
        Worker::Forge => super::supervise_forge(store, stopping).await,
    }
}

async fn run_automation_scheduler_worker(
    store: Store,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let executable_name = if cfg!(windows) {
        "swarm-automation-worker.exe"
    } else {
        "swarm-automation-worker"
    };
    let executable = std::env::current_exe()
        .map_err(|_| {
            Error::new(
                "AUTOMATION_WORKER_PATH_UNAVAILABLE",
                "host path is unavailable",
            )
        })?
        .with_file_name(executable_name);
    if !executable.is_file() {
        return Err(Error::new(
            "AUTOMATION_WORKER_NOT_INSTALLED",
            "standalone scheduler executable is unavailable",
        ));
    }
    let config_path = store.automation_scheduler_worker_config_path();
    let (scope, launch_id) = store.begin_automation_scheduler_worker().await?;
    let mut command = Command::new(executable);
    command.env_clear();
    #[cfg(windows)]
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    let spawn = command
        .arg("run")
        .arg("--config")
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(false)
        .spawn();
    let mut child = match spawn {
        Ok(child) => child,
        Err(_) => {
            store
                .abandon_automation_scheduler_worker(scope, launch_id)
                .await?;
            return Err(Error::new(
                "AUTOMATION_WORKER_START_FAILED",
                "standalone scheduler process could not be started",
            ));
        }
    };
    let Some(pid) = child.id() else {
        let _ = child.kill().await;
        if child.wait().await.is_err() {
            return Err(Error::new(
                "AUTOMATION_WORKER_DEPARTURE_UNKNOWN",
                "scheduler process exit could not be confirmed",
            ));
        }
        return Err(Error::new(
            "AUTOMATION_WORKER_IDENTITY_UNKNOWN",
            "standalone scheduler PID is unavailable; owner receipt remains held",
        ));
    };
    let launched = swarm_process::spawned_identity(pid).map_err(|_| {
        Error::new(
            "AUTOMATION_WORKER_IDENTITY_UNKNOWN",
            "standalone scheduler birth proof could not be read",
        )
    });
    let identity = match launched.and_then(|identity| {
        swarm_automation::service_owner_group_identity(&identity, store.scheduler_owner_token()?)
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_WORKER_IDENTITY_UNKNOWN",
                    "standalone scheduler group proof is invalid",
                )
            })
    }) {
        Ok(identity) => identity,
        Err(error) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error);
        }
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        if child.wait().await.is_err() {
            return Err(Error::new(
                "AUTOMATION_WORKER_DEPARTURE_UNKNOWN",
                "scheduler process exit could not be confirmed",
            ));
        }
        store
            .abandon_exited_automation_scheduler_worker(
                scope.clone(),
                launch_id.clone(),
                identity.clone(),
            )
            .await?;
        return Err(Error::new(
            "AUTOMATION_WORKER_READY_UNKNOWN",
            "scheduler readiness pipe is unavailable",
        ));
    };
    if let Err(error) = wait_for_scheduler_ready(stdout).await {
        let _ = child.kill().await;
        if child.wait().await.is_err() {
            return Err(Error::new(
                "AUTOMATION_WORKER_DEPARTURE_UNKNOWN",
                "scheduler process exit could not be confirmed",
            ));
        }
        store
            .abandon_exited_automation_scheduler_worker(
                scope.clone(),
                launch_id.clone(),
                identity.clone(),
            )
            .await?;
        return Err(error);
    }
    if let Err(error) = store
        .activate_automation_scheduler_worker(scope.clone(), launch_id.clone(), identity.clone())
        .await
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
        let _ = store
            .finish_automation_scheduler_worker(scope.clone(), launch_id.clone(), identity.clone())
            .await;
        let _ = store
            .abandon_exited_automation_scheduler_worker(scope, launch_id, identity.clone())
            .await;
        return Err(error);
    }
    tokio::select! {
        status = child.wait() => {
            let status = status.map_err(|_| Error::new(
                "AUTOMATION_WORKER_STATUS_UNKNOWN",
                "standalone scheduler process status could not be read",
            ))?;
            store
                .finish_automation_scheduler_worker(scope, launch_id, identity)
                .await?;
            if status.success() {
                Err(Error::new(
                    "AUTOMATION_WORKER_STOPPED",
                    "standalone scheduler stopped while Store demand remained active",
                ))
            } else {
                Err(Error::new(
                    "AUTOMATION_WORKER_EXITED",
                    "standalone scheduler process exited unsuccessfully",
                ))
            }
        }
        changed = stopping.changed() => {
            if changed.is_err() || *stopping.borrow() {
                // Only this exact Store-owned Rust worker is stopped here. It
                // starts no native/provider child; external SDK processes are
                // outside this supervisor and are never terminated here.
                child.kill().await.map_err(|_| Error::new(
                    "AUTOMATION_WORKER_STOP_UNKNOWN",
                    "standalone scheduler did not confirm shutdown",
                ))?;
                child.wait().await.map_err(|_| Error::new(
                    "AUTOMATION_WORKER_STOP_UNKNOWN",
                    "standalone scheduler exit could not be confirmed",
                ))?;
                store
                    .finish_automation_scheduler_worker(scope, launch_id, identity)
                    .await?;
                Ok(())
            } else {
                Err(Error::new(
                    "AUTOMATION_WORKER_STOP_UNKNOWN",
                    "standalone scheduler stop channel changed unexpectedly",
                ))
            }
        }
    }
}

async fn wait_for_scheduler_ready(stdout: tokio::process::ChildStdout) -> Result<()> {
    const MAX_READY_FRAME_BYTES: usize = 64;
    let bounded = stdout.take((MAX_READY_FRAME_BYTES + 1) as u64);
    let mut reader = BufReader::new(bounded);
    let mut frame = Vec::with_capacity(MAX_READY_FRAME_BYTES + 1);
    let read = tokio::time::timeout(
        Duration::from_secs(10),
        reader.read_until(b'\n', &mut frame),
    )
    .await
    .map_err(|_| {
        Error::new(
            "AUTOMATION_WORKER_READY_TIMEOUT",
            "scheduler did not confirm service-group readiness",
        )
    })?
    .map_err(|_| {
        Error::new(
            "AUTOMATION_WORKER_READY_READ_FAILED",
            "scheduler readiness frame could not be read",
        )
    })?;
    if read > MAX_READY_FRAME_BYTES || frame.as_slice() != swarm_automation::READY_FRAME {
        return Err(Error::new(
            "AUTOMATION_WORKER_READY_INVALID",
            "scheduler readiness frame did not match its fixed protocol",
        ));
    }
    Ok(())
}

async fn reap_finished(store: &Store, slots: &mut BTreeMap<Worker, Slot>) -> Result<()> {
    let finished = Worker::ALL
        .into_iter()
        .filter(|worker| {
            slots
                .get(worker)
                .and_then(|slot| slot.task.as_ref())
                .is_some_and(|task| task.is_finished())
        })
        .collect::<Vec<_>>();
    for worker in finished {
        let slot = slots
            .get_mut(&worker)
            .expect("finished worker retains its slot");
        let Some(task) = slot.task.take() else {
            continue;
        };
        slot.stop.take();
        let error = match task.await {
            Ok(Ok(())) => Error::new(
                "SUPERVISOR_STOPPED",
                "optional worker stopped before its demand was released",
            ),
            Ok(Err(error)) => error,
            Err(_) => Error::new("SUPERVISOR_PANIC", "optional worker task panicked"),
        };
        record_failure(store, worker, slot, error).await?;
    }
    Ok(())
}

async fn stop_worker(store: &Store, worker: Worker, slot: &mut Slot) -> Result<()> {
    if let Some(stop) = slot.stop.take() {
        let _ = stop.send(true);
    }
    if let Some(task) = slot.task.take() {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return record_failure(store, worker, slot, error).await,
            Err(_) => {
                return record_failure(
                    store,
                    worker,
                    slot,
                    Error::new(
                        "SUPERVISOR_PANIC",
                        "optional worker panicked while draining its admitted pass",
                    ),
                )
                .await;
            }
        }
    }
    store
        .record_legacy_worker_status(worker.name(), "dormant", slot.failures, None, None)
        .await
}

/// Persist worker-local errors before restarting. A Store error cannot be
/// truthfully reported through that same Store, while a kernel/journal failure
/// remains fatal. Panics from a legacy worker are handled like any other
/// optional failure rather than escaping to host-level supervisor failure.
async fn record_failure(
    store: &Store,
    worker: Worker,
    slot: &mut Slot,
    error: Error,
) -> Result<()> {
    if error.code.starts_with("STORE_") {
        return Err(error);
    }
    let now = Instant::now();
    if now.duration_since(slot.failure_window_started) >= FAILURE_WINDOW {
        slot.failure_window_started = now;
        slot.failures = 0;
    }
    slot.failures = slot.failures.saturating_add(1).min(32);
    let fatal = error.code.starts_with("KERNEL_");
    let (state, delay) = if fatal || slot.failures >= MAX_FAILURES_PER_WINDOW {
        ("isolated", ISOLATED_RETRY)
    } else {
        let exponent = slot.failures.saturating_sub(1).min(16);
        let delay = BASE_RETRY.saturating_mul(1_u32 << exponent).min(MAX_RETRY);
        ("retry_wait", delay)
    };
    slot.retry_at = now + delay;
    store
        .record_legacy_worker_status(
            worker.name(),
            state,
            slot.failures,
            Some(safe_code(&error.code)),
            Some(delay.as_millis() as u64),
        )
        .await?;
    eprintln!(
        "{} optional worker failure: {}",
        worker.name(),
        safe_code(&error.code)
    );
    if fatal { Err(error) } else { Ok(()) }
}

async fn stop_all(
    store: &Store,
    slots: &mut BTreeMap<Worker, Slot>,
    persist_status: bool,
) -> Result<()> {
    let mut fatal = None;
    let mut persistence_unavailable = false;
    let mut updates = Vec::new();
    for slot in slots.values_mut() {
        if let Some(stop) = slot.stop.take() {
            let _ = stop.send(true);
        }
    }
    for worker in Worker::ALL {
        let slot = slots
            .get_mut(&worker)
            .expect("every legacy worker retains its slot");
        if let Some(task) = slot.task.take() {
            match task.await {
                Ok(Ok(())) => {
                    updates.push((worker, "dormant", slot.failures, None, None));
                }
                Ok(Err(error)) if is_store_or_kernel_failure(&error) => {
                    persistence_unavailable = true;
                    if !error.code.starts_with("STORE_") {
                        updates.push((
                            worker,
                            "isolated",
                            slot.failures,
                            Some(safe_code(&error.code)),
                            Some(ISOLATED_RETRY.as_millis() as u64),
                        ));
                    }
                    fatal.get_or_insert(error);
                }
                Ok(Err(error)) => {
                    updates.push((
                        worker,
                        "isolated",
                        slot.failures,
                        Some(safe_code(&error.code)),
                        Some(ISOLATED_RETRY.as_millis() as u64),
                    ));
                }
                Err(_) => {
                    updates.push((
                        worker,
                        "isolated",
                        slot.failures,
                        Some("SUPERVISOR_PANIC".to_owned()),
                        Some(ISOLATED_RETRY.as_millis() as u64),
                    ));
                }
            }
        }
    }
    // A Store/journal fault is irreducible here. Do not issue cleanup status
    // writes that can obscure the first core failure with a secondary error.
    if persist_status && !persistence_unavailable {
        for (worker, state, failures, error_code, retry_in_ms) in updates {
            store
                .record_legacy_worker_status(
                    worker.name(),
                    state,
                    failures,
                    error_code,
                    retry_in_ms,
                )
                .await?;
        }
    }
    match fatal {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn is_store_or_kernel_failure(error: &Error) -> bool {
    error.code.starts_with("STORE_") || error.code.starts_with("KERNEL_")
}

fn safe_code(code: &str) -> String {
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
