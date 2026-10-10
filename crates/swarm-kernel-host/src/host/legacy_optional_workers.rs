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
const WORKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const WORKER_SHUTDOWN_STATUS_BUDGET: Duration = Duration::from_millis(250);
const WORKER_SHUTDOWN_UNCONFIRMED: &str = "WORKER_SHUTDOWN_UNCONFIRMED";

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
    stop_deadline: Option<Instant>,
    shutdown_unconfirmed: bool,
    failure_window_started: Instant,
    failures: u32,
    retry_at: Instant,
}

impl Slot {
    fn new(now: Instant) -> Self {
        Self {
            task: None,
            stop: None,
            stop_deadline: None,
            shutdown_unconfirmed: false,
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
    let persist_cleanup_status = !result.as_ref().is_err_and(|error| {
        is_store_or_kernel_failure(error) || error.code == "LEGACY_WORKER_SHUTDOWN_UNKNOWN"
    });
    let cleanup = stop_all(&store, &mut slots, persist_cleanup_status).await;
    match (result, cleanup) {
        (Err(error), Err(cleanup_error)) => {
            if is_store_or_kernel_failure(&cleanup_error) && !is_store_or_kernel_failure(&error) {
                eprintln!("legacy worker shutdown outcome: {}", error.code);
                return Err(cleanup_error);
            }
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
    slot.stop_deadline = None;
    slot.shutdown_unconfirmed = false;
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
        let identity_error = Error::new(
            "AUTOMATION_WORKER_IDENTITY_UNKNOWN",
            "standalone scheduler PID is unavailable",
        );
        return Err(retain_unidentified_scheduler_owner(&mut child, identity_error).await);
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
            return Err(retain_unidentified_scheduler_owner(&mut child, error).await);
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

/// A spawned child with no exact service-family identity cannot settle the
/// durable owner receipt. Try to stop and reap the direct child, then report
/// cleanup attention so the optional-worker supervisor persists it in
/// `host.status`; the `launching` receipt remains held for exact recovery.
async fn retain_unidentified_scheduler_owner(
    child: &mut tokio::process::Child,
    identity_error: Error,
) -> Error {
    let _ = child.kill().await;
    match child.wait().await {
        Ok(_) => Error::new(
            "AUTOMATION_WORKER_CLEANUP_UNKNOWN",
            "scheduler child exited, but its exact service-family departure is unproved; owner receipt remains held",
        )
        .with_secondary_error(identity_error),
        Err(_) => Error::new(
            "AUTOMATION_WORKER_DEPARTURE_UNKNOWN",
            "scheduler child departure is unconfirmed; owner receipt remains held for cleanup",
        )
        .with_secondary_error(identity_error),
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
        let stop_requested = slot.stop_deadline.is_some();
        let _ = slot.stop.take();
        slot.stop_deadline = None;
        slot.shutdown_unconfirmed = false;
        let error = match task.await {
            Ok(Ok(())) if stop_requested => {
                record_bounded_shutdown_status(
                    store,
                    worker,
                    "dormant",
                    slot.failures,
                    None,
                    None,
                    Instant::now() + WORKER_SHUTDOWN_STATUS_BUDGET,
                )
                .await?;
                continue;
            }
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
    if slot.task.is_none() || slot.task.as_ref().is_some_and(|task| task.is_finished()) {
        return Ok(());
    }
    if slot.stop_deadline.is_none() {
        if let Some(stop) = slot.stop.as_ref() {
            let _ = stop.send(true);
        }
        slot.stop_deadline = Some(Instant::now() + WORKER_SHUTDOWN_GRACE);
    }
    let deadline = slot
        .stop_deadline
        .expect("a requested worker stop has one bounded deadline");
    if !slot.shutdown_unconfirmed && Instant::now() >= deadline {
        slot.shutdown_unconfirmed = true;
        record_bounded_shutdown_status(
            store,
            worker,
            "isolated",
            slot.failures,
            Some(WORKER_SHUTDOWN_UNCONFIRMED.to_owned()),
            Some(ISOLATED_RETRY.as_millis() as u64),
            Instant::now() + WORKER_SHUTDOWN_STATUS_BUDGET,
        )
        .await?;
    }
    Ok(())
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

struct UnfinishedWorker {
    worker: Worker,
    join: JoinHandle<Result<()>>,
    consecutive_failures: u32,
}

async fn record_bounded_shutdown_status(
    store: &Store,
    worker: Worker,
    state: &'static str,
    failures: u32,
    error_code: Option<String>,
    retry_in_ms: Option<u64>,
    deadline: Instant,
) -> Result<()> {
    match tokio::time::timeout_at(
        deadline,
        store.record_legacy_worker_status(worker.name(), state, failures, error_code, retry_in_ms),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(Error::new(
            "LEGACY_WORKER_SHUTDOWN_UNKNOWN",
            "worker shutdown status did not finish within its budget; an already queued Store write may still complete",
        )),
    }
}

async fn stop_all(
    store: &Store,
    slots: &mut BTreeMap<Worker, Slot>,
    persist_status: bool,
) -> Result<()> {
    let deadline = Instant::now() + WORKER_SHUTDOWN_GRACE;
    let mut fatal = None;
    let mut persistence_unavailable = false;
    let mut updates = Vec::new();
    let mut unfinished_workers = Vec::new();
    for slot in slots.values_mut() {
        if let Some(stop) = slot.stop.as_ref() {
            let _ = stop.send(true);
        }
        if slot.task.is_some() {
            slot.stop_deadline.get_or_insert(deadline);
        }
    }
    for worker in Worker::ALL {
        let slot = slots
            .get_mut(&worker)
            .expect("every legacy worker retains its slot");
        let joined = {
            let Some(task) = slot.task.as_mut() else {
                continue;
            };
            if task.is_finished() {
                Some(task.await)
            } else {
                tokio::time::timeout_at(deadline, task).await.ok()
            }
        };
        let Some(joined) = joined else {
            let join = slot
                .task
                .take()
                .expect("an unfinished worker retains its join handle");
            let _ = slot.stop.take();
            slot.stop_deadline = None;
            slot.shutdown_unconfirmed = true;
            unfinished_workers.push(UnfinishedWorker {
                worker,
                join,
                consecutive_failures: slot.failures,
            });
            updates.push((
                worker,
                "isolated",
                slot.failures,
                Some(WORKER_SHUTDOWN_UNCONFIRMED.to_owned()),
                Some(ISOLATED_RETRY.as_millis() as u64),
            ));
            continue;
        };
        let _ = slot.task.take();
        let _ = slot.stop.take();
        slot.stop_deadline = None;
        slot.shutdown_unconfirmed = false;
        match joined {
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
    // Establish in-process join custody before any Store status await. The
    // reaper waits for the gate so a late initial status write cannot overwrite
    // a completion status written out of order.
    let has_unfinished = !unfinished_workers.is_empty();
    let completion_gate = retain_unfinished_worker_custody(store.clone(), unfinished_workers);

    // A Store/journal fault is irreducible here. Do not issue cleanup status
    // writes that can obscure the first core failure with a secondary error.
    let mut status_error = None;
    let mut status_timed_out = false;
    if persist_status && !persistence_unavailable {
        let status_deadline = Instant::now() + WORKER_SHUTDOWN_STATUS_BUDGET;
        for (worker, state, failures, error_code, retry_in_ms) in updates {
            match record_bounded_shutdown_status(
                store,
                worker,
                state,
                failures,
                error_code,
                retry_in_ms,
                status_deadline,
            )
            .await
            {
                Ok(()) => {}
                Err(error) if error.code == "LEGACY_WORKER_SHUTDOWN_UNKNOWN" => {
                    status_timed_out = true;
                    break;
                }
                Err(error) => {
                    status_error = Some(error);
                    break;
                }
            }
        }
    }
    if let Some(gate) = completion_gate {
        let _ = gate.send(
            persist_status
                && !persistence_unavailable
                && status_error.is_none()
                && !status_timed_out,
        );
    }
    if let Some(error) = fatal {
        if let Some(status_error) = status_error {
            eprintln!("legacy worker shutdown status: {}", status_error.code);
        }
        if status_timed_out {
            eprintln!("legacy worker shutdown status: LEGACY_WORKER_SHUTDOWN_UNKNOWN");
        }
        return Err(error);
    }
    if let Some(error) = status_error {
        return Err(error);
    }
    if status_timed_out {
        return Err(Error::new(
            "LEGACY_WORKER_SHUTDOWN_UNKNOWN",
            "worker shutdown status is unknown; an already queued Store write may still complete",
        ));
    }
    if has_unfinished {
        return Err(Error::new(
            "LEGACY_WORKER_SHUTDOWN_UNKNOWN",
            "one or more optional workers remain under shutdown custody",
        ));
    }
    Ok(())
}

/// Keep unfinished worker joins under an in-process reaper after the
/// coordinator's common deadline. Join custody lasts only while the Tokio
/// runtime remains alive; it is not durable across runtime shutdown. This
/// never aborts a worker or native descendant, and independent durable native
/// owner receipts remain untouched. Completion writes wait until the initial
/// shutdown status writes are known to have finished.
fn retain_unfinished_worker_custody(
    store: Store,
    unfinished: Vec<UnfinishedWorker>,
) -> Option<tokio::sync::oneshot::Sender<bool>> {
    if unfinished.is_empty() {
        return None;
    }
    let (release_completion_status, completion_status_gate) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let mut persist_completion = completion_status_gate.await.unwrap_or(false);
        for worker in unfinished {
            let UnfinishedWorker {
                worker: name,
                join,
                consecutive_failures,
            } = worker;
            let update = match join.await {
                Ok(Ok(())) => Some(("dormant", consecutive_failures, None, None)),
                Ok(Err(error)) if is_store_or_kernel_failure(&error) => {
                    eprintln!("legacy worker custody {}: {}", name.name(), error.code);
                    persist_completion = false;
                    None
                }
                Ok(Err(error)) => Some((
                    "isolated",
                    consecutive_failures.saturating_add(1).min(32),
                    Some(safe_code(&error.code)),
                    Some(ISOLATED_RETRY.as_millis() as u64),
                )),
                Err(_) => Some((
                    "isolated",
                    consecutive_failures.saturating_add(1).min(32),
                    Some("SUPERVISOR_PANIC".to_owned()),
                    Some(ISOLATED_RETRY.as_millis() as u64),
                )),
            };
            if persist_completion
                && let Some((state, failures, error_code, retry_in_ms)) = update
                && let Err(error) = record_bounded_shutdown_status(
                    &store,
                    name,
                    state,
                    failures,
                    error_code,
                    retry_in_ms,
                    Instant::now() + WORKER_SHUTDOWN_STATUS_BUDGET,
                )
                .await
            {
                eprintln!(
                    "legacy worker custody status {}: {}",
                    name.name(),
                    error.code
                );
                persist_completion = false;
            }
        }
    });
    Some(release_completion_status)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        model::{self, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use rusqlite::Connection;
    use serde_json::{Value, json};
    use std::{
        fs,
        io::Read,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        process::Command,
        sync::{oneshot, watch},
    };

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("legacy-worker-shutdown-{}", model::new_id()));
            fs::create_dir(&path).expect("create unique worker-shutdown test directory");
            Self(path)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct Fixture {
        directory: ScratchDir,
        owner: StoreOwner,
        operator: Principal,
    }

    impl Fixture {
        async fn new() -> Self {
            let directory = ScratchDir::new();
            let root = DataRoot::acquire(&directory.0).expect("acquire fixture data root");
            let credential = bootstrap_credential(&root.path).expect("bootstrap fixture operator");
            let owner = StoreOwner::start(root, Arc::new(Config::default()), credential.clone())
                .await
                .expect("start real Store fixture");
            let operator = owner
                .store
                .authenticate(credential)
                .await
                .expect("authenticate fixture operator");
            Self {
                directory,
                owner,
                operator,
            }
        }

        async fn status(&self) -> Value {
            self.owner
                .store
                .call(self.operator.clone(), "host.status".to_owned(), json!({}))
                .await
                .expect("read host status through the real Store")
        }

        fn optional_workers(status: &Value) -> &Value {
            &status["host_lifecycle"]["optional_workers"]
        }

        async fn close(self) {
            let Self {
                directory,
                owner,
                operator: _,
            } = self;
            owner.close().await.expect("close fixture StoreOwner");
            drop(directory);
        }

        async fn wait_for_states(&self, expected: &[(Worker, &str)]) -> Value {
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    let status = self.status().await;
                    if expected.iter().all(|(worker, state)| {
                        Self::optional_workers(&status)[worker.name()]["state"].as_str()
                            == Some(*state)
                    }) {
                        return status;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("optional-worker status reached the expected state")
        }
    }

    struct CompletionProbe {
        completed: Arc<AtomicBool>,
        dropped_early: Arc<AtomicBool>,
    }

    impl Drop for CompletionProbe {
        fn drop(&mut self) {
            if !self.completed.load(Ordering::SeqCst) {
                self.dropped_early.store(true, Ordering::SeqCst);
            }
        }
    }

    struct ControlledWorker {
        release: oneshot::Sender<()>,
        stop_seen: oneshot::Receiver<()>,
        completed: oneshot::Receiver<()>,
        dropped_early: Arc<AtomicBool>,
    }

    fn empty_slots() -> BTreeMap<Worker, Slot> {
        Worker::ALL
            .into_iter()
            .map(|worker| (worker, Slot::new(Instant::now())))
            .collect()
    }

    fn install_controlled_worker(
        slots: &mut BTreeMap<Worker, Slot>,
        worker: Worker,
    ) -> ControlledWorker {
        let (stop, mut stopping) = watch::channel(false);
        let (release, release_rx) = oneshot::channel();
        let (stop_seen_tx, stop_seen) = oneshot::channel();
        let (completed_tx, completed) = oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let dropped_early = Arc::new(AtomicBool::new(false));
        let probe = CompletionProbe {
            completed: finished.clone(),
            dropped_early: dropped_early.clone(),
        };
        let task = tokio::spawn(async move {
            let _probe = probe;
            stopping.changed().await.map_err(|_| {
                Error::new(
                    "TEST_WORKER_STOP_CHANNEL_CLOSED",
                    "stop signal sender closed",
                )
            })?;
            if !*stopping.borrow() {
                return Err(Error::new(
                    "TEST_WORKER_STOP_SIGNAL_MISSING",
                    "worker received a change without the stop signal",
                ));
            }
            let _ = stop_seen_tx.send(());
            release_rx.await.map_err(|_| {
                Error::new("TEST_WORKER_RELEASE_MISSING", "test did not release worker")
            })?;
            finished.store(true, Ordering::SeqCst);
            let _ = completed_tx.send(());
            Ok(())
        });
        let slot = slots.get_mut(&worker).expect("test worker has a slot");
        slot.stop = Some(stop);
        slot.task = Some(task);
        ControlledWorker {
            release,
            stop_seen,
            completed,
            dropped_early,
        }
    }

    fn install_stop_responsive_worker(slots: &mut BTreeMap<Worker, Slot>, worker: Worker) {
        let (stop, mut stopping) = watch::channel(false);
        let task = tokio::spawn(async move {
            stopping.changed().await.map_err(|_| {
                Error::new(
                    "TEST_WORKER_STOP_CHANNEL_CLOSED",
                    "stop signal sender closed",
                )
            })?;
            Ok(())
        });
        let slot = slots.get_mut(&worker).expect("test worker has a slot");
        slot.stop = Some(stop);
        slot.task = Some(task);
    }

    struct NativeWorker {
        check_alive: oneshot::Sender<()>,
        alive_after_deadline: oneshot::Receiver<bool>,
        release: oneshot::Sender<()>,
        stop_seen: oneshot::Receiver<()>,
        completed: oneshot::Receiver<bool>,
        dropped_early: Arc<AtomicBool>,
    }

    fn install_native_worker(
        slots: &mut BTreeMap<Worker, Slot>,
        worker: Worker,
    ) -> (oneshot::Receiver<()>, NativeWorker) {
        let (stop, mut stopping) = watch::channel(false);
        let (ready_tx, ready) = oneshot::channel();
        let (check_alive, check_alive_rx) = oneshot::channel();
        let (alive_tx, alive_after_deadline) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let (stop_seen_tx, stop_seen) = oneshot::channel();
        let (completed_tx, completed) = oneshot::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let dropped_early = Arc::new(AtomicBool::new(false));
        let probe = CompletionProbe {
            completed: finished.clone(),
            dropped_early: dropped_early.clone(),
        };
        let executable = std::env::current_exe().expect("locate current test executable");
        let task = tokio::spawn(async move {
            let _probe = probe;
            let mut command = Command::new(executable);
            command
                .args([
                    "--ignored",
                    "--exact",
                    "host::legacy_optional_workers::tests::native_child_waits_for_stdin_eof",
                ])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(false);
            let mut child = command.spawn().map_err(|_| {
                Error::new(
                    "TEST_NATIVE_CHILD_START_FAILED",
                    "native test child did not start",
                )
            })?;
            let _ = ready_tx.send(());
            stopping.changed().await.map_err(|_| {
                Error::new(
                    "TEST_WORKER_STOP_CHANNEL_CLOSED",
                    "stop signal sender closed",
                )
            })?;
            if !*stopping.borrow() {
                return Err(Error::new(
                    "TEST_WORKER_STOP_SIGNAL_MISSING",
                    "worker received a change without the stop signal",
                ));
            }
            let _ = stop_seen_tx.send(());
            check_alive_rx.await.map_err(|_| {
                Error::new(
                    "TEST_NATIVE_CHILD_CHECK_MISSING",
                    "test did not check child liveness",
                )
            })?;
            let alive = child
                .try_wait()
                .map_err(|_| {
                    Error::new(
                        "TEST_NATIVE_CHILD_QUERY_FAILED",
                        "native child status unavailable",
                    )
                })?
                .is_none();
            let _ = alive_tx.send(alive);
            release_rx.await.map_err(|_| {
                Error::new(
                    "TEST_WORKER_RELEASE_MISSING",
                    "test did not release native worker",
                )
            })?;
            drop(child.stdin.take());
            let status = child.wait().await.map_err(|_| {
                Error::new(
                    "TEST_NATIVE_CHILD_WAIT_FAILED",
                    "native child exit unavailable",
                )
            })?;
            finished.store(true, Ordering::SeqCst);
            let _ = completed_tx.send(status.success());
            if !status.success() {
                return Err(Error::new(
                    "TEST_NATIVE_CHILD_FAILED",
                    "native test child did not exit successfully",
                ));
            }
            Ok(())
        });
        let slot = slots.get_mut(&worker).expect("test worker has a slot");
        slot.stop = Some(stop);
        slot.task = Some(task);
        (
            ready,
            NativeWorker {
                check_alive,
                alive_after_deadline,
                release,
                stop_seen,
                completed,
                dropped_early,
            },
        )
    }

    async fn set_worker_status(
        fixture: &Fixture,
        worker: Worker,
        state: &'static str,
        error_code: Option<String>,
        retry_in_ms: Option<u64>,
    ) {
        fixture
            .owner
            .store
            .record_legacy_worker_status(worker.name(), state, 0, error_code, retry_in_ms)
            .await
            .expect("write initial worker status through Store");
    }

    #[tokio::test]
    async fn stop_all_shares_deadline_and_reaper_keeps_worker_and_native_child_until_late_completion()
     {
        let fixture = Fixture::new().await;
        let mut slots = empty_slots();
        for worker in [Worker::Checks, Worker::Scripts] {
            set_worker_status(&fixture, worker, "running", None, None).await;
        }
        let (native_ready, native) = install_native_worker(&mut slots, Worker::Checks);
        let _ = native_ready
            .await
            .expect("native child worker started before shutdown");
        let script = install_controlled_worker(&mut slots, Worker::Scripts);

        let started = Instant::now();
        let error = stop_all(&fixture.owner.store, &mut slots, true)
            .await
            .expect_err("unfinished workers remain under shutdown custody");
        let elapsed = started.elapsed();
        assert_eq!(error.code, "LEGACY_WORKER_SHUTDOWN_UNKNOWN");
        assert!(
            elapsed >= WORKER_SHUTDOWN_GRACE - Duration::from_secs(1),
            "shutdown returned before its shared join deadline: {elapsed:?}"
        );
        assert!(
            elapsed < WORKER_SHUTDOWN_GRACE + Duration::from_secs(3),
            "unfinished workers consumed separate grace periods: {elapsed:?}"
        );
        native
            .stop_seen
            .await
            .expect("native worker observed stop signal");
        script
            .stop_seen
            .await
            .expect("script worker observed stop signal");
        let _ = native.check_alive.send(());
        assert!(
            native
                .alive_after_deadline
                .await
                .expect("worker checked native child after timeout"),
            "shutdown terminated the native child"
        );
        assert!(!native.dropped_early.load(Ordering::SeqCst));
        assert!(!script.dropped_early.load(Ordering::SeqCst));

        let timed_out_status = fixture.status().await;
        let optional_workers = Fixture::optional_workers(&timed_out_status);
        for worker in [Worker::Checks, Worker::Scripts] {
            assert_eq!(
                optional_workers[worker.name()]["state"].as_str(),
                Some("isolated")
            );
            assert_eq!(
                optional_workers[worker.name()]["last_error_code"].as_str(),
                Some(WORKER_SHUTDOWN_UNCONFIRMED)
            );
        }

        let _ = native.release.send(());
        let _ = script.release.send(());
        assert!(
            native
                .completed
                .await
                .expect("native worker completed after release"),
            "native test child did not exit after its stdin was closed"
        );
        script
            .completed
            .await
            .expect("script worker completed after release");
        let late_status = fixture
            .wait_for_states(&[(Worker::Checks, "dormant"), (Worker::Scripts, "dormant")])
            .await;
        let optional_workers = Fixture::optional_workers(&late_status);
        for worker in [Worker::Checks, Worker::Scripts] {
            assert_eq!(
                optional_workers[worker.name()]["last_failure"]["code"].as_str(),
                Some(WORKER_SHUTDOWN_UNCONFIRMED),
                "late completion must retain the shutdown-unknown evidence"
            );
        }
        assert!(!native.dropped_early.load(Ordering::SeqCst));
        assert!(!script.dropped_early.load(Ordering::SeqCst));
        fixture.close().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_timeout_and_rejected_status_never_clear_shutdown_unknown() {
        let fixture = Fixture::new().await;
        set_worker_status(&fixture, Worker::Checks, "running", None, None).await;
        let database_path = fixture.directory.0.join("swarm.db");
        let blocker = Connection::open(&database_path).expect("open independent SQLite writer");
        blocker
            .execute_batch(
                "CREATE TABLE shutdown_status_audit(value_json TEXT NOT NULL);
                 CREATE TRIGGER audit_optional_worker_status
                 AFTER UPDATE OF value_json ON meta
                 WHEN NEW.key='host:optional-workers:v1'
                 BEGIN
                     INSERT INTO shutdown_status_audit(value_json) VALUES(NEW.value_json);
                 END;
                 BEGIN IMMEDIATE;",
            )
            .expect("hold the SQLite writer lock after installing audit trigger");

        let mut slots = empty_slots();
        let worker = install_controlled_worker(&mut slots, Worker::Checks);
        let error = stop_all(&fixture.owner.store, &mut slots, true)
            .await
            .expect_err("status persistence exceeds its budget behind the writer lock");
        assert_eq!(error.code, "LEGACY_WORKER_SHUTDOWN_UNKNOWN");
        let while_write_is_queued = fixture.status().await;
        assert_eq!(
            Fixture::optional_workers(&while_write_is_queued)["checks"]["state"].as_str(),
            Some("running"),
            "an uncommitted status write must not be reported as dormant"
        );

        let _ = worker.release.send(());
        worker
            .completed
            .await
            .expect("worker completes while its status write remains queued");
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        blocker
            .execute_batch("COMMIT;")
            .expect("release SQLite writer lock after the caller timed out");
        fixture
            .owner
            .store
            .legacy_worker_demand_snapshot()
            .await
            .expect("drain the Store job queued behind the timed-out status caller");
        let late_status = fixture
            .wait_for_states(&[(Worker::Checks, "isolated")])
            .await;
        assert_eq!(
            Fixture::optional_workers(&late_status)["checks"]["last_error_code"].as_str(),
            Some(WORKER_SHUTDOWN_UNCONFIRMED)
        );
        let audit_states = {
            let mut statement = blocker
                .prepare("SELECT value_json FROM shutdown_status_audit ORDER BY rowid")
                .expect("read status write audit");
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("query status write audit")
                .map(|raw| {
                    let raw = raw.expect("read status write audit row");
                    serde_json::from_str::<Value>(&raw)
                        .expect("audit stores valid worker status JSON")["workers"]["checks"]["state"]
                        .as_str()
                        .expect("audit worker status has a state")
                        .to_owned()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(audit_states, vec!["isolated".to_owned()]);

        blocker
            .execute_batch(
                "CREATE TRIGGER reject_optional_worker_status_insert
                 BEFORE INSERT ON meta
                 WHEN NEW.key='host:optional-workers:v1'
                 BEGIN SELECT RAISE(ABORT, 'injected worker status rejection'); END;
                 CREATE TRIGGER reject_optional_worker_status_update
                 BEFORE UPDATE OF value_json ON meta
                 WHEN NEW.key='host:optional-workers:v1'
                 BEGIN SELECT RAISE(ABORT, 'injected worker status rejection'); END;",
            )
            .expect("install Store status rejection trigger");
        let mut rejected_slots = empty_slots();
        install_stop_responsive_worker(&mut rejected_slots, Worker::Checks);
        let rejected = stop_all(&fixture.owner.store, &mut rejected_slots, true)
            .await
            .expect_err("a rejected Store status write stays a hard failure");
        assert!(
            is_store_or_kernel_failure(&rejected),
            "Store rejection was softened to {}",
            rejected.code
        );
        let retained = fixture.status().await;
        assert_eq!(
            Fixture::optional_workers(&retained)["checks"]["state"].as_str(),
            Some("isolated")
        );
        assert_eq!(
            Fixture::optional_workers(&retained)["checks"]["last_error_code"].as_str(),
            Some(WORKER_SHUTDOWN_UNCONFIRMED)
        );
        let _ = blocker.execute_batch("COMMIT;");
        drop(blocker);
        fixture.close().await;
    }

    #[test]
    #[ignore = "spawned by the legacy-worker shutdown custody test as a native child"]
    fn native_child_waits_for_stdin_eof() {
        let mut stdin = std::io::stdin().lock();
        let mut input = Vec::new();
        stdin
            .read_to_end(&mut input)
            .expect("read until parent closes the child pipe");
    }
}
