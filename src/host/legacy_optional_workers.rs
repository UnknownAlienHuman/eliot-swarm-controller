//! Lazy, isolated lifecycle for the Store's legacy host reconcilers.
//!
//! One coordinator owns the wake/timer for all ten workers. A worker exists
//! only while its exact config or retained-work predicate is true. Failures
//! are visible through Store `host.status`, paced with bounded backoff, and
//! never terminate the IPC listener unless the Store/kernel itself failed.
use crate::{
    error::{Error, Result},
    store::{LegacyWorkerDemand, Store},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
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
    Automation,
    Launcher,
    NativeMcp,
    NativeMcpTools,
    Forge,
}

impl Worker {
    const ALL: [Self; 10] = [
        Self::Checks,
        Self::Scripts,
        Self::OpenCode,
        Self::Zed,
        Self::Scheduler,
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
                if slot.task.is_none() && Instant::now() >= slot.retry_at {
                    if let Err(error) = start_worker(&store, worker, slot).await {
                        break 'run Err(error);
                    }
                }
            } else if slot.task.is_some() {
                if let Err(error) = stop_worker(&store, worker, slot).await {
                    break 'run Err(error);
                }
            }
        }

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
            _ = tick.tick() => true
        };
        if !wake {
            break 'run Ok(());
        }
    };
    let persist_cleanup_status = !result
        .as_ref()
        .is_err_and(|error| error.code.starts_with("STORE_"));
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
        Worker::Automation => super::supervise_automation(store, stopping).await,
        Worker::Launcher => super::supervise_launcher(store, stopping).await,
        Worker::NativeMcp => super::supervise_native_mcp(store, stopping).await,
        Worker::NativeMcpTools => super::supervise_native_mcp_tools(store, stopping).await,
        Worker::Forge => super::supervise_forge(store, stopping).await,
    }
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

/// Persist optional errors before restarting. A Store error cannot be truthfully
/// reported through that same Store, while a kernel error remains fatal after
/// its bounded health receipt is written. Panics from a legacy worker are
/// handled like any other optional failure rather than escaping to host-level
/// supervisor failure.
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
    let mut store_unavailable = false;
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
                    store_unavailable |= error.code.starts_with("STORE_");
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
    if persist_status && !store_unavailable {
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
