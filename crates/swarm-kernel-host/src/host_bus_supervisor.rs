//! One coordinator for all explicitly managed durable bus consumers.
//!
//! Dormant registrations create no task or timer. Active scopes share one
//! Store-change watch and one fallback scan. Each dispatcher owns a distinct
//! non-killing OS process group; the coordinator adopts only an exact verified
//! receipt and never kills a process family.

use crate::{
    config::BusSupervisorConfig,
    error::{Error, Result},
    store::{
        Store,
        bus_kernel::{DemandState, ManagedBusOwnerState, ManagedBusServiceDemand},
    },
};
use futures_util::FutureExt;
use serde::Deserialize;
use sha2::Digest;
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File},
    io::Read,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use swarm_bus::{
    ManagedWorkerConfigExpectation, managed_service_state_path, managed_worker_config_path,
    verify_managed_worker_config,
};
use swarm_contracts::DeclaredServiceScope;
use swarm_process::{
    departed_empty, process_image_identity, service_owner_is_live, write_private_new,
};
use tokio::{
    process::{Child, Command},
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

const SCAN_FALLBACK: Duration = Duration::from_secs(2);
const FAILURE_WINDOW: Duration = Duration::from_secs(60);
const MAX_STARTS_PER_WINDOW: usize = 5;
const BASE_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(30);
const ISOLATED_RETRY: Duration = Duration::from_secs(60);
const OWNER_RECEIPT_LIMIT: u64 = 64 * 1024;
const DISPATCHER_IMAGE_LIMIT: u64 = 512 * 1024 * 1024;
const MAX_SCOPES: usize = 128;
const ACTOR_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub(crate) struct DispatcherPin {
    pub executable: PathBuf,
    pub sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerReceipt {
    schema_version: u16,
    scope: DeclaredServiceScope,
    owner: OwnerIdentity,
    worker_image: serde_json::Value,
    #[serde(default)]
    worker_config_sha256: Option<String>,
    #[serde(default)]
    credential_token_sha256: Option<String>,
    #[serde(default)]
    scope_digest: Option<String>,
}

struct OwnerReceiptFile {
    receipt: OwnerReceipt,
    sha256: String,
}

impl std::ops::Deref for OwnerReceiptFile {
    type Target = OwnerReceipt;

    fn deref(&self) -> &Self::Target {
        &self.receipt
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerIdentity {
    version: u64,
    token: String,
    process: serde_json::Value,
}

struct Slot {
    scope: DeclaredServiceScope,
    data_root: PathBuf,
    owner_dir: PathBuf,
    config_path: PathBuf,
    child: Option<Child>,
    spawned_image: Option<serde_json::Value>,
    spawn_verified: bool,
    child_exited: bool,
    start_attempt: Instant,
    starts: VecDeque<Instant>,
    failures: VecDeque<Instant>,
    retry_at: Instant,
    consecutive_failures: u32,
    demanded: bool,
    stopping: bool,
    last_error: Option<&'static str>,
    last_health: Option<PersistedHealth>,
    last_demand_state: Option<DemandState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PersistedHealth {
    state: String,
    consecutive_failures: u32,
    error: Option<String>,
    retry_in_ms: Option<u64>,
}

impl Slot {
    fn new(scope: DeclaredServiceScope, root: &Path, now: Instant) -> Result<Self> {
        Ok(Self {
            data_root: root.to_path_buf(),
            config_path: managed_worker_config_path(root, &scope.service_id)?,
            owner_dir: managed_service_state_path(root, &scope)?,
            scope,
            child: None,
            spawned_image: None,
            spawn_verified: false,
            child_exited: false,
            start_attempt: now,
            starts: VecDeque::new(),
            failures: VecDeque::new(),
            retry_at: now,
            consecutive_failures: 0,
            demanded: false,
            stopping: false,
            last_error: None,
            last_health: None,
            last_demand_state: None,
        })
    }

    fn note_failure(&mut self, now: Instant, code: &'static str) -> Option<Duration> {
        while self
            .failures
            .front()
            .is_some_and(|when| now.saturating_duration_since(*when) > FAILURE_WINDOW)
        {
            self.failures.pop_front();
        }
        self.failures.push_back(now);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1).min(32);
        self.last_error = Some(code);
        if self.starts.len() >= MAX_STARTS_PER_WINDOW {
            self.retry_at = now + ISOLATED_RETRY;
            return Some(ISOLATED_RETRY);
        }
        let exponent = self.failures.len().saturating_sub(1).min(16) as u32;
        let multiplier = 1_u32 << exponent;
        let delay = BASE_RETRY.saturating_mul(multiplier).min(MAX_RETRY);
        self.retry_at = now + delay;
        Some(delay)
    }

    fn note_start(&mut self, now: Instant) {
        while self
            .starts
            .front()
            .is_some_and(|when| now.saturating_duration_since(*when) > FAILURE_WINDOW)
        {
            self.starts.pop_front();
        }
        self.starts.push_back(now);
    }

    fn reset_after_healthy_run(&mut self, now: Instant) {
        if now.saturating_duration_since(self.start_attempt) >= FAILURE_WINDOW {
            self.starts.clear();
            self.failures.clear();
            self.consecutive_failures = 0;
            self.last_error = None;
        }
    }
}

pub(crate) async fn run(
    store: Store,
    data_root: PathBuf,
    pin: DispatcherPin,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let mut changed = store.subscribe_managed_bus_service_changes();
    let mut tick = tokio::time::interval(SCAN_FALLBACK);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut slots: BTreeMap<DeclaredServiceScope, Slot> = BTreeMap::new();
    let mut store_changes_open = true;

    loop {
        if *stopping.borrow() {
            return drain_on_shutdown(&store, &pin, &mut slots).await;
        }

        let snapshot = match store.managed_bus_service_snapshot().await {
            Ok(snapshot) if snapshot.len() <= MAX_SCOPES => snapshot,
            Ok(_) => {
                eprintln!("managed bus coordinator isolated: BUS_SERVICE_CAPACITY");
                tokio::select! {
                    _ = tick.tick() => {},
                    _ = stopping.changed() => return drain_on_shutdown(&store, &pin, &mut slots).await,
                }
                continue;
            }
            Err(error) => {
                if !is_store_unavailable(&error) {
                    eprintln!(
                        "managed bus demand snapshot isolated: {}",
                        safe_reconcile_error(&error.code)
                    );
                }
                // Keep the current child handles and ownership view while the
                // Store is unavailable. Returning here would drop the handles
                // and could race a second dispatcher against a live receipt.
                tokio::select! {
                    _ = tick.tick() => {},
                    _ = stopping.changed() => return drain_on_shutdown(&store, &pin, &mut slots).await,
                }
                continue;
            }
        };
        let snapshot_count = snapshot.len();
        let demands: BTreeMap<_, _> = snapshot
            .into_iter()
            .map(|demand| (demand.scope.clone(), demand))
            .collect();
        if demands.len() != snapshot_count {
            eprintln!("managed bus coordinator isolated: BUS_SERVICE_SCOPE_DUPLICATE");
            tokio::select! {
                _ = tick.tick() => {},
                _ = stopping.changed() => return drain_on_shutdown(&store, &pin, &mut slots).await,
            }
            continue;
        }

        for demand in demands.values() {
            slots.entry(demand.scope.clone()).or_insert(Slot::new(
                demand.scope.clone(),
                &data_root,
                Instant::now(),
            )?);
        }
        let known: Vec<_> = slots.keys().cloned().collect();
        let mut store_failed = false;
        for scope in known {
            let demand = demands.get(&scope);
            let Some(slot) = slots.get_mut(&scope) else {
                continue;
            };
            slot.demanded = demand.is_some_and(|demand| matches!(demand.state, DemandState::Ready));
            let demand_state = demand.map(|demand| demand.state);
            if slot.last_demand_state != demand_state {
                if matches!(demand_state, Some(DemandState::Ready)) {
                    slot.retry_at = Instant::now();
                }
                slot.last_demand_state = demand_state;
            }
            if let Err(error) = reconcile_slot(&store, &pin, demand, slot).await {
                if is_store_unavailable(&error) {
                    store_failed = true;
                    break;
                }
                if error.code == "BUS_SERVICE_SCOPE_STALE" {
                    eprintln!("managed bus scope readback: {}", error.code);
                    continue;
                }
                let code = safe_reconcile_error(&error.code);
                slot.last_error = Some(code);
                if let Err(status_error) =
                    persist_status(&store, slot, "unknown", Some(code), None).await
                {
                    if is_store_unavailable(&status_error) {
                        store_failed = true;
                        break;
                    }
                }
            }
            let mut remove_slot = false;
            if demand.is_none() && slot.child.is_none() {
                match owner_receipt_exists(slot) {
                    Ok(false) => remove_slot = true,
                    Ok(true) => {}
                    Err(error) => {
                        let code = safe_reconcile_error(&error.code);
                        slot.last_error = Some(code);
                        if let Err(status_error) =
                            persist_status(&store, slot, "unknown", Some(code), None).await
                        {
                            if is_store_unavailable(&status_error) {
                                store_failed = true;
                                break;
                            }
                        }
                    }
                }
            }
            if remove_slot {
                slots.remove(&scope);
            }
        }

        if store_failed {
            tokio::select! {
                _ = tick.tick() => {},
                _ = stopping.changed() => return drain_on_shutdown(&store, &pin, &mut slots).await,
            }
            continue;
        }

        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() {
                    return drain_on_shutdown(&store, &pin, &mut slots).await;
                }
            }
            result = changed.changed(), if store_changes_open => {
                if result.is_err() {
                    store_changes_open = false;
                }
            }
            _ = tick.tick() => {}
        }
    }
}

pub(crate) struct OptionalManagedBusSupervisor {
    task: JoinHandle<Result<()>>,
}

impl OptionalManagedBusSupervisor {
    pub(crate) async fn join(self) -> Result<()> {
        self.task.await.map_err(|_| {
            Error::new(
                "BUS_SUPERVISOR_JOIN_FAILED",
                "managed bus supervisor task failed to complete",
            )
        })?
    }
}

/// Launch the one optional coordinator only when locally enabled. The
/// coordinator itself starts no process for registrations that are absent,
/// disabled, or idle.
pub(crate) fn spawn_isolated_managed_bus_supervisor(
    store: Store,
    data_root: PathBuf,
    config: BusSupervisorConfig,
    stopping: watch::Receiver<bool>,
) -> Option<OptionalManagedBusSupervisor> {
    if !config.enabled {
        return None;
    }
    let pin = DispatcherPin {
        executable: config.dispatcher_executable.unwrap_or_default(),
        sha256: config.dispatcher_sha256.unwrap_or_default(),
    };
    let task = tokio::spawn(async move {
        let mut retry = BASE_RETRY;
        let mut failures = 0_u32;
        loop {
            if *stopping.borrow() {
                return Ok(());
            }
            record_managed_bus_actor_status(&store, "running", failures, None, None).await?;
            let run = AssertUnwindSafe(run(
                store.clone(),
                data_root.clone(),
                pin.clone(),
                stopping.clone(),
            ))
            .catch_unwind()
            .await;
            match run {
                Ok(Ok(())) if *stopping.borrow() => return Ok(()),
                Ok(Ok(())) => {
                    failures = failures.saturating_add(1).min(32);
                    if let Err(status_error) = record_managed_bus_actor_status(
                        &store,
                        "retry_wait",
                        failures,
                        Some("BUS_SUPERVISOR_ERROR"),
                        Some(retry),
                    )
                    .await
                    {
                        return Err(Error::new(
                            "BUS_SUPERVISOR_ERROR",
                            "managed bus coordinator returned unexpectedly",
                        )
                        .with_secondary_error(status_error));
                    }
                    eprintln!("managed bus coordinator returned unexpectedly");
                }
                Ok(Err(error)) => {
                    failures = failures.saturating_add(1).min(32);
                    if let Err(status_error) = record_managed_bus_actor_status(
                        &store,
                        "retry_wait",
                        failures,
                        Some(&error.code),
                        Some(retry),
                    )
                    .await
                    {
                        return Err(error.with_secondary_error(status_error));
                    }
                    eprintln!(
                        "managed bus coordinator isolated: {}",
                        safe_reconcile_error(&error.code)
                    );
                }
                Err(_) => {
                    failures = failures.saturating_add(1).min(32);
                    let panic_error =
                        Error::new("BUS_SUPERVISOR_ERROR", "managed bus coordinator panicked");
                    if let Err(status_error) = record_managed_bus_actor_status(
                        &store,
                        "retry_wait",
                        failures,
                        Some("BUS_SUPERVISOR_ERROR"),
                        Some(retry),
                    )
                    .await
                    {
                        return Err(panic_error.with_secondary_error(status_error));
                    }
                    eprintln!("managed bus coordinator panicked");
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(retry) => {},
                result = wait_for_stop(stopping.clone()) => if result { return Ok(()); },
            }
            retry = (retry * 2).min(ACTOR_RETRY_MAX);
        }
    });
    Some(OptionalManagedBusSupervisor { task })
}

async fn record_managed_bus_actor_status(
    store: &Store,
    state: &'static str,
    failures: u32,
    error_code: Option<&str>,
    retry: Option<Duration>,
) -> Result<()> {
    let error_code = error_code.map(|code| safe_reconcile_error(code).to_owned());
    let retry_in_ms = retry.map(|delay| u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    store
        .record_legacy_worker_status(
            "managed-bus-supervisor",
            state,
            failures,
            error_code,
            retry_in_ms,
        )
        .await
}

async fn wait_for_stop(mut stopping: watch::Receiver<bool>) -> bool {
    loop {
        if *stopping.borrow() || stopping.changed().await.is_err() {
            return true;
        }
    }
}

fn is_store_unavailable(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "STORE_CLOSED" | "STORE_ERROR" | "STORE_PANIC"
    )
}

fn safe_reconcile_error(code: &str) -> &'static str {
    const ALLOWED: &[&str] = &[
        "BUS_CONSUMER_REGISTRATION_CORRUPT",
        "BUS_SERVICE_CAPACITY",
        "BUS_SERVICE_SCOPE_DUPLICATE",
        "BUS_SERVICE_SCOPE_STALE",
        "BUS_SERVICE_SCOPE_INACTIVE",
        "BUS_SERVICE_START_LIMIT",
        "BUS_SERVICE_GENERATION_CORRUPT",
        "BUS_SERVICE_GENERATION_EXHAUSTED",
        "BUS_SERVICE_HEALTH_CORRUPT",
        "BUS_SERVICE_HEALTH_INVALID",
        "BUS_SERVICE_OWNER_UNKNOWN",
        "BUS_SERVICE_OWNER_READ_FAILED",
        "BUS_SERVICE_OWNER_READBACK_INVALID",
        "BUS_SERVICE_OWNER_READBACK_STALE",
        "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
        "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
        "BUS_SERVICE_STOP_REQUEST_INVALID",
        "BUS_SERVICE_STOP_REQUEST_UNKNOWN",
        "BUS_WORKER_CONFIG_MISMATCH",
        "BUS_WORKER_CONFIG_MISSING",
        "BUS_WORKER_CONFIG_INVALID",
        "BUS_WORKER_CONFIG_DIGEST_MISMATCH",
        "BUS_DISPATCHER_PIN_INVALID",
        "BUS_DISPATCHER_START_FAILED",
        "BUS_DISPATCHER_IMAGE_UNKNOWN",
        "BUS_DISPATCHER_EXIT",
        "BUS_WORKER_WAIT_UNKNOWN",
        "BUS_SUPERVISOR_ERROR",
    ];
    ALLOWED
        .iter()
        .copied()
        .find(|allowed| *allowed == code)
        .unwrap_or("BUS_SUPERVISOR_ERROR")
}
async fn reconcile_slot(
    store: &Store,
    pin: &DispatcherPin,
    demand: Option<&ManagedBusServiceDemand>,
    slot: &mut Slot,
) -> Result<()> {
    if let Some(child) = slot.child.as_mut() {
        match child.try_wait() {
            Ok(Some(_status)) => slot.child_exited = true,
            Ok(None) => {
                if !slot.spawn_verified
                    && let Some(pid) = child.id()
                    && let Ok(image) = process_image_identity(pid)
                {
                    if image_matches_pin(&image, pin) {
                        slot.spawn_verified = true;
                        slot.spawned_image = Some(image);
                    } else {
                        slot.last_error = Some("BUS_DISPATCHER_IMAGE_UNKNOWN");
                    }
                }
            }
            Err(_) => {
                slot.last_error = Some("BUS_WORKER_WAIT_UNKNOWN");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_WORKER_WAIT_UNKNOWN"),
                    None,
                )
                .await?;
                return Ok(());
            }
        }
    }

    let receipt = match read_owner_receipt(slot) {
        Ok(receipt) => receipt,
        Err(OwnerReadError::Missing) => None,
        Err(OwnerReadError::Invalid) => {
            slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_UNKNOWN"),
                None,
            )
            .await?;
            return Ok(());
        }
        Err(OwnerReadError::Io) => {
            slot.last_error = Some("BUS_SERVICE_OWNER_READ_FAILED");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_READ_FAILED"),
                None,
            )
            .await?;
            return Ok(());
        }
    };

    if let Some(receipt) = receipt {
        if let Err(_) = verify_owner_receipt(slot, &receipt, pin) {
            slot.last_error = Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH"),
                None,
            )
            .await?;
            return Ok(());
        }
        if receipt.schema_version != 2 {
            // Legacy receipts have no retained config/token/scope binding. Keep
            // them unknown even after the old process family has disappeared;
            // clearing one would authorize a replacement without v2 proof.
            slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_UNKNOWN"),
                None,
            )
            .await?;
            return Ok(());
        }
        if demand.is_some_and(|demand| {
            demand
                .owner_receipt_sha256
                .as_deref()
                .is_some_and(|digest| digest != receipt.sha256)
        }) {
            slot.last_error = Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH"),
                None,
            )
            .await?;
            return Ok(());
        }
        if slot.child.is_some() && !slot.spawn_verified {
            slot.spawn_verified = true;
            slot.spawned_image = Some(receipt.worker_image.clone());
        }
        if let Some(spawned) = &slot.spawned_image
            && !same_process_image(spawned, &receipt.worker_image)
        {
            slot.last_error = Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_SCOPE_MISMATCH"),
                None,
            )
            .await?;
            return Ok(());
        }
        let owner_live = match service_owner_is_live(&receipt.owner.process, &receipt.owner.token) {
            Ok(live) => live,
            Err(_) => {
                slot.last_error = Some("BUS_SERVICE_OWNER_READ_FAILED");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_SERVICE_OWNER_READ_FAILED"),
                    None,
                )
                .await?;
                return Ok(());
            }
        };
        if owner_live {
            let Some(demand) = demand else {
                slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_SERVICE_OWNER_UNKNOWN"),
                    None,
                )
                .await?;
                return Ok(());
            };
            if verify_adoptable_owner_receipt(
                slot,
                demand,
                &receipt,
                pin,
                matches!(demand.state, DemandState::Ready),
            )
            .is_err()
            {
                // V1 receipts and any mismatch in Store scope, token/config
                // hashes, executable bytes, or OS incarnation remain held.
                // A live but unproven owner is never stopped or replaced.
                slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_SERVICE_OWNER_UNKNOWN"),
                    None,
                )
                .await?;
                return Ok(());
            }
            if !matches!(demand.state, DemandState::Ready) {
                // A stop marker is scoped to this exact live receipt. Host
                // shutdown and Store outages never create one.
                request_stop(slot)?;
                persist_status(store, slot, "stopping", None, None).await?;
                return Ok(());
            }
            if stop_request_exists(slot)? {
                slot.last_error = Some("BUS_SERVICE_STOP_REQUEST_UNKNOWN");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_SERVICE_STOP_REQUEST_UNKNOWN"),
                    None,
                )
                .await?;
                return Ok(());
            }
            store
                .record_managed_bus_owner_readback(
                    slot.scope.clone(),
                    ManagedBusOwnerState::Live,
                    Some(receipt.sha256.clone()),
                )
                .await?;
            slot.last_error = None;
            slot.reset_after_healthy_run(Instant::now());
            persist_status(store, slot, "running", None, None).await?;
            return Ok(());
        }
        let departed = match departed_empty(&receipt.owner.process, &receipt.owner.token) {
            Ok(departed) => departed,
            Err(_) => {
                slot.last_error = Some("BUS_SERVICE_OWNER_READ_FAILED");
                persist_status(
                    store,
                    slot,
                    "unknown",
                    Some("BUS_SERVICE_OWNER_READ_FAILED"),
                    None,
                )
                .await?;
                return Ok(());
            }
        };
        if !departed {
            slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_SERVICE_OWNER_UNKNOWN"),
                None,
            )
            .await?;
            return Ok(());
        }
        if demand.is_some() {
            store
                .record_managed_bus_owner_readback(
                    slot.scope.clone(),
                    ManagedBusOwnerState::Departed,
                    None,
                )
                .await?;
        }
        remove_owner_receipt(slot)?;
        slot.child = None;
        slot.spawned_image = None;
        slot.spawn_verified = false;
        slot.child_exited = false;
        clear_stop_request(slot)?;
        if slot.stopping || !slot.demanded {
            persist_status(store, slot, "dormant", None, None).await?;
            return Ok(());
        }
        let code = if slot.last_error.is_some() {
            slot.last_error.take().unwrap_or("BUS_DISPATCHER_EXIT")
        } else {
            "BUS_DISPATCHER_EXIT"
        };
        let delay = slot
            .note_failure(Instant::now(), code)
            .unwrap_or(ISOLATED_RETRY);
        let state = if slot.starts.len() >= MAX_STARTS_PER_WINDOW {
            "isolated"
        } else {
            "retry_wait"
        };
        persist_status(store, slot, state, Some(code), Some(delay)).await?;
        return Ok(());
    }

    if slot.child.is_some() && slot.child_exited {
        // The dispatcher publishes its owner receipt before it can call Store
        // or create any descendants. A waited child with no receipt therefore
        // has no hidden process family to adopt.
        if !slot.spawn_verified {
            slot.last_error = Some("BUS_DISPATCHER_IMAGE_UNKNOWN");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_DISPATCHER_IMAGE_UNKNOWN"),
                None,
            )
            .await?;
            return Ok(());
        }
        if demand.is_some() {
            store
                .record_managed_bus_owner_readback(
                    slot.scope.clone(),
                    ManagedBusOwnerState::Departed,
                    None,
                )
                .await?;
        }
        slot.child = None;
        slot.spawned_image = None;
        slot.spawn_verified = false;
        slot.child_exited = false;
        if slot.demanded {
            let code = slot.last_error.take().unwrap_or("BUS_DISPATCHER_EXIT");
            let delay = slot
                .note_failure(Instant::now(), code)
                .unwrap_or(ISOLATED_RETRY);
            let state = if slot.starts.len() >= MAX_STARTS_PER_WINDOW {
                "isolated"
            } else {
                "retry_wait"
            };
            persist_status(store, slot, state, Some(code), Some(delay)).await?;
        } else {
            persist_status(store, slot, "dormant", None, None).await?;
        }
        return Ok(());
    }

    if slot.child.is_some() {
        // The exact child has started but has not published its service owner
        // receipt. It may still be resolving startup; keep the scope single-flight.
        let elapsed = Instant::now().saturating_duration_since(slot.start_attempt);
        if !slot.spawn_verified {
            slot.last_error = Some("BUS_DISPATCHER_IMAGE_UNKNOWN");
            persist_status(
                store,
                slot,
                "unknown",
                Some("BUS_DISPATCHER_IMAGE_UNKNOWN"),
                None,
            )
            .await?;
            return Ok(());
        }
        let error = if elapsed > Duration::from_secs(10) {
            Some("BUS_SERVICE_OWNER_RECEIPT_MISSING")
        } else {
            None
        };
        if let Some(code) = error {
            slot.last_error = Some(code);
            persist_status(store, slot, "unknown", Some(code), None).await?;
        } else {
            persist_status(store, slot, "starting", None, None).await?;
        }
        return Ok(());
    }

    let Some(demand) = demand else {
        persist_status(store, slot, "dormant", None, None).await?;
        return Ok(());
    };
    if !matches!(
        demand.owner_state,
        ManagedBusOwnerState::NeverStarted | ManagedBusOwnerState::Departed
    ) {
        slot.last_error = Some("BUS_SERVICE_OWNER_UNKNOWN");
        persist_status(
            store,
            slot,
            "unknown",
            Some("BUS_SERVICE_OWNER_UNKNOWN"),
            None,
        )
        .await?;
        return Ok(());
    }
    if matches!(demand.state, DemandState::Disabled) {
        persist_status(store, slot, "dormant", None, None).await?;
        return Ok(());
    }
    if !matches!(demand.state, DemandState::Ready) {
        let code = match demand.state {
            DemandState::Disabled => unreachable!(),
            DemandState::OwnerInactive => "BUS_SERVICE_OWNER_INACTIVE",
            DemandState::ScriptRunInactive => "BUS_SERVICE_SCRIPT_RUN_INACTIVE",
            DemandState::ScopeChanged => "BUS_SERVICE_SCOPE_CHANGED",
            DemandState::Ready => unreachable!(),
        };
        slot.last_error = Some(code);
        slot.retry_at = Instant::now() + ISOLATED_RETRY;
        persist_status(store, slot, "isolated", Some(code), Some(ISOLATED_RETRY)).await?;
        return Ok(());
    }
    let now = Instant::now();
    while slot
        .starts
        .front()
        .is_some_and(|when| now.saturating_duration_since(*when) > FAILURE_WINDOW)
    {
        slot.starts.pop_front();
    }
    if slot.starts.len() >= MAX_STARTS_PER_WINDOW {
        slot.retry_at = now + ISOLATED_RETRY;
        slot.last_error = Some("BUS_SERVICE_START_LIMIT");
        persist_status(
            store,
            slot,
            "isolated",
            Some("BUS_SERVICE_START_LIMIT"),
            Some(ISOLATED_RETRY),
        )
        .await?;
        return Ok(());
    }
    if Instant::now() < slot.retry_at {
        return Ok(());
    }

    let data_root = slot.data_root.clone();
    match start_worker(store, pin, demand, &data_root, slot).await {
        Ok(()) => {
            slot.start_attempt = Instant::now();
            slot.stopping = false;
            slot.last_error = None;
            persist_status(store, slot, "starting", None, None).await?;
        }
        Err(StartFailure::Store(error)) => return Err(error),
        Err(StartFailure::Code("BUS_SERVICE_SCOPE_STALE")) => {
            return Err(Error::new(
                "BUS_SERVICE_SCOPE_STALE",
                "managed bus registration changed before process start",
            ));
        }
        Err(StartFailure::Code(code)) => {
            let now = Instant::now();
            let delay = slot.note_failure(now, code).unwrap_or(ISOLATED_RETRY);
            let persistent_limit = code == "BUS_SERVICE_START_LIMIT";
            let delay = if persistent_limit {
                ISOLATED_RETRY
            } else {
                delay
            };
            if persistent_limit {
                slot.retry_at = now + delay;
            }
            let state = if persistent_limit || slot.starts.len() >= MAX_STARTS_PER_WINDOW {
                "isolated"
            } else {
                "retry_wait"
            };
            persist_status(store, slot, state, Some(code), Some(delay)).await?;
        }
    }
    Ok(())
}

async fn start_worker(
    store: &Store,
    pin: &DispatcherPin,
    demand: &ManagedBusServiceDemand,
    data_root: &Path,
    slot: &mut Slot,
) -> std::result::Result<(), StartFailure> {
    if verify_dispatcher(pin).is_err() {
        return Err(StartFailure::Code("BUS_DISPATCHER_PIN_INVALID"));
    }
    let expected = ManagedWorkerConfigExpectation {
        path: slot.config_path.clone(),
        store_root: data_root.to_path_buf(),
        manager_id: demand.owner_manager_id.clone(),
        project_id: demand.project_id.clone(),
        automation_id: demand.automation_id.clone(),
        consumer_client_id: demand.consumer_client_id.clone(),
        credential_token_sha256: demand.credential_token_sha256.clone(),
        worker_config_sha256: demand.worker_config_sha256.clone(),
    };
    if verify_managed_worker_config(&expected).is_err() {
        return Err(StartFailure::Code("BUS_WORKER_CONFIG_MISMATCH"));
    }
    ensure_private_dir(data_root, &slot.owner_dir)
        .map_err(|_| StartFailure::Code("BUS_SERVICE_OWNER_DIRECTORY_INVALID"))?;
    if owner_receipt_exists(slot).map_err(|_| StartFailure::Code("BUS_SERVICE_OWNER_UNKNOWN"))? {
        return Err(StartFailure::Code("BUS_SERVICE_OWNER_UNKNOWN"));
    }
    if stop_request_exists(slot)
        .map_err(|_| StartFailure::Code("BUS_SERVICE_STOP_REQUEST_INVALID"))?
    {
        return Err(StartFailure::Code("BUS_SERVICE_STOP_REQUEST_UNKNOWN"));
    }
    let executable =
        verify_dispatcher(pin).map_err(|_| StartFailure::Code("BUS_DISPATCHER_PIN_INVALID"))?;
    if verify_managed_worker_config(&expected).is_err() {
        return Err(StartFailure::Code("BUS_WORKER_CONFIG_MISMATCH"));
    }
    if owner_receipt_exists(slot).map_err(|_| StartFailure::Code("BUS_SERVICE_OWNER_UNKNOWN"))? {
        return Err(StartFailure::Code("BUS_SERVICE_OWNER_UNKNOWN"));
    }
    if stop_request_exists(slot)
        .map_err(|_| StartFailure::Code("BUS_SERVICE_STOP_REQUEST_INVALID"))?
    {
        return Err(StartFailure::Code("BUS_SERVICE_STOP_REQUEST_UNKNOWN"));
    }
    store
        .record_managed_bus_service_start(slot.scope.clone())
        .await
        .map_err(|error| {
            if is_store_unavailable(&error) {
                StartFailure::Store(error)
            } else {
                StartFailure::Code(match error.code.as_str() {
                    "BUS_SERVICE_START_LIMIT" => "BUS_SERVICE_START_LIMIT",
                    "BUS_SERVICE_OWNER_UNKNOWN" => "BUS_SERVICE_OWNER_UNKNOWN",
                    "BUS_SERVICE_SCOPE_INACTIVE" => "BUS_SERVICE_SCOPE_INACTIVE",
                    "BUS_SERVICE_SCOPE_STALE" => "BUS_SERVICE_SCOPE_STALE",
                    _ => "BUS_SERVICE_START_GATE_FAILED",
                })
            }
        })?;
    let attempt_now = Instant::now();
    slot.note_start(attempt_now);
    let mut command = Command::new(executable);
    let generation = demand.scope.generation.to_string();
    command
        .env_clear()
        .args([
            "run",
            "--worker-config",
            slot.config_path
                .to_str()
                .ok_or(StartFailure::Code("BUS_WORKER_CONFIG_PATH_INVALID"))?,
            "--managed-owner-dir",
            slot.owner_dir
                .to_str()
                .ok_or(StartFailure::Code("BUS_SERVICE_OWNER_PATH_INVALID"))?,
            "--service-generation",
            &generation,
            "--worker-config-sha256",
            &demand.worker_config_sha256,
            "--credential-token-sha256",
            &demand.credential_token_sha256,
            "--scope-digest",
            &demand.scope_digest,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false);
    #[cfg(windows)]
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    let child = command
        .spawn()
        .map_err(|_| StartFailure::Code("BUS_DISPATCHER_START_FAILED"))?;
    let spawned_image = child.id().and_then(|pid| process_image_identity(pid).ok());
    slot.spawn_verified = spawned_image
        .as_ref()
        .is_some_and(|image| image_matches_pin(image, pin));
    slot.spawned_image = spawned_image;
    slot.child_exited = false;
    slot.child = Some(child);
    if !slot.spawn_verified {
        slot.last_error = Some("BUS_DISPATCHER_IMAGE_UNKNOWN");
    }
    Ok(())
}

fn verify_dispatcher(pin: &DispatcherPin) -> Result<PathBuf> {
    if !pin.executable.is_absolute()
        || pin.sha256.len() != 64
        || !pin.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "BUS_DISPATCHER_PIN_INVALID",
            "dispatcher pin is invalid",
        ));
    }
    reject_reparse_components(&pin.executable)?;
    let executable = fs::canonicalize(&pin.executable)?;
    let metadata = fs::symlink_metadata(&executable)?;
    if !metadata.is_file()
        || is_link_or_reparse(&metadata)
        || metadata.len() == 0
        || metadata.len() > DISPATCHER_IMAGE_LIMIT
    {
        return Err(Error::new(
            "BUS_DISPATCHER_PIN_INVALID",
            "dispatcher path is not a regular executable",
        ));
    }
    let mut file = File::open(&executable)?;
    let before = file.metadata()?;
    let modified_before = before.modified()?;
    let mut hasher = sha2::Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| Error::new("BUS_DISPATCHER_PIN_INVALID", "dispatcher size overflow"))?;
        if total > DISPATCHER_IMAGE_LIMIT {
            return Err(Error::new(
                "BUS_DISPATCHER_PIN_INVALID",
                "dispatcher exceeds the configured hashing bound",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    if total != before.len() || after.len() != before.len() || after.modified()? != modified_before
    {
        return Err(Error::new(
            "BUS_DISPATCHER_PIN_INVALID",
            "dispatcher changed while its configured digest was checked",
        ));
    }
    let digest = format!("{:x}", hasher.finalize());
    if digest != pin.sha256.to_ascii_lowercase() {
        return Err(Error::new(
            "BUS_DISPATCHER_PIN_INVALID",
            "dispatcher bytes do not match the configured SHA-256",
        ));
    }
    Ok(executable)
}

fn image_matches_pin(image: &serde_json::Value, pin: &DispatcherPin) -> bool {
    let configured_image = pin.executable.canonicalize().ok();
    let observed_image = image["image_path"]
        .as_str()
        .and_then(|path| Path::new(path).canonicalize().ok());
    image["image_sha256"]
        .as_str()
        .and_then(|digest| digest.strip_prefix("sha256:"))
        .is_some_and(|digest| digest.eq_ignore_ascii_case(&pin.sha256))
        && configured_image.is_some()
        && observed_image == configured_image
}

fn read_owner_receipt(
    slot: &Slot,
) -> std::result::Result<Option<OwnerReceiptFile>, OwnerReadError> {
    match validate_private_dir_beneath(&slot.data_root, &slot.owner_dir) {
        Ok(false) => return Err(OwnerReadError::Missing),
        Ok(true) => {}
        Err(_) => return Err(OwnerReadError::Invalid),
    }
    let path = slot.owner_dir.join("owner.json");
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(OwnerReadError::Missing);
        }
        Err(_) => return Err(OwnerReadError::Io),
    };
    if !metadata.is_file() || is_link_or_reparse(&metadata) || metadata.len() > OWNER_RECEIPT_LIMIT
    {
        return Err(OwnerReadError::Invalid);
    }
    #[cfg(windows)]
    swarm_process::private_permissions(&path, false).map_err(|_| OwnerReadError::Invalid)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(OwnerReadError::Invalid);
        }
    }
    let bytes = fs::read(path).map_err(|_| OwnerReadError::Io)?;
    let sha256 = format!("{:x}", sha2::Sha256::digest(&bytes));
    let receipt = serde_json::from_slice(&bytes).map_err(|_| OwnerReadError::Invalid)?;
    Ok(Some(OwnerReceiptFile { receipt, sha256 }))
}

enum OwnerReadError {
    Missing,
    Invalid,
    Io,
}

enum StartFailure {
    Code(&'static str),
    Store(Error),
}

fn verify_owner_receipt(slot: &Slot, receipt: &OwnerReceipt, pin: &DispatcherPin) -> Result<()> {
    receipt.scope.validate()?;
    let valid_version = matches!(receipt.schema_version, 1 | 2);
    let valid_v2_hashes = receipt.schema_version == 1
        || [
            receipt.worker_config_sha256.as_deref(),
            receipt.credential_token_sha256.as_deref(),
            receipt.scope_digest.as_deref(),
        ]
        .into_iter()
        .all(|value| value.is_some_and(is_sha256));
    if !valid_version
        || !valid_v2_hashes
        || receipt.scope != slot.scope
        || receipt.owner.version != 1
        || uuid::Uuid::parse_str(&receipt.owner.token).is_err()
        || receipt.owner.process["purpose"] != "bus_consumer"
        || !same_owner_image(&receipt.owner.process, &receipt.worker_image)
        || !image_matches_pin(&receipt.worker_image, pin)
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
            "owner receipt scope or image is invalid",
        ));
    }
    if let Some(spawned) = &slot.spawned_image
        && !same_process_image(spawned, &receipt.worker_image)
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
            "owner receipt belongs to another process",
        ));
    }
    if let Some(child_pid) = slot.child.as_ref().and_then(Child::id)
        && receipt.worker_image["pid"].as_u64() != Some(u64::from(child_pid))
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
            "owner receipt does not match the exact launched dispatcher",
        ));
    }
    Ok(())
}

fn verify_adoptable_owner_receipt(
    slot: &Slot,
    demand: &ManagedBusServiceDemand,
    receipt: &OwnerReceiptFile,
    pin: &DispatcherPin,
    require_ready: bool,
) -> Result<()> {
    verify_owner_receipt(slot, receipt, pin)?;
    if receipt.schema_version != 2
        || demand.scope != receipt.scope
        || (require_ready && !matches!(demand.state, DemandState::Ready))
        || !matches!(
            demand.owner_state,
            ManagedBusOwnerState::LaunchUncertain
                | ManagedBusOwnerState::Live
                | ManagedBusOwnerState::Unknown
        )
        || (slot.child.is_none()
            && demand.owner_receipt_sha256.as_deref() != Some(receipt.sha256.as_str()))
        || receipt.worker_config_sha256.as_deref() != Some(demand.worker_config_sha256.as_str())
        || receipt.credential_token_sha256.as_deref()
            != Some(demand.credential_token_sha256.as_str())
        || receipt.scope_digest.as_deref() != Some(demand.scope_digest.as_str())
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_SCOPE_MISMATCH",
            "owner receipt does not match the retained Store registration",
        ));
    }
    let expected = ManagedWorkerConfigExpectation {
        path: slot.config_path.clone(),
        store_root: slot.data_root.clone(),
        manager_id: demand.owner_manager_id.clone(),
        project_id: demand.project_id.clone(),
        automation_id: demand.automation_id.clone(),
        consumer_client_id: demand.consumer_client_id.clone(),
        credential_token_sha256: demand.credential_token_sha256.clone(),
        worker_config_sha256: demand.worker_config_sha256.clone(),
    };
    verify_managed_worker_config(&expected).map_err(|_| {
        Error::new(
            "BUS_WORKER_CONFIG_MISMATCH",
            "retained worker configuration no longer matches Store registration",
        )
    })?;
    let pid = receipt.worker_image["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or_else(|| Error::new("BUS_SERVICE_OWNER_SCOPE_MISMATCH", "owner PID is invalid"))?;
    let observed = process_image_identity(pid)?;
    if !same_process_image(&observed, &receipt.worker_image)
        || !service_owner_is_live(&receipt.owner.process, &receipt.owner.token)?
    {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_UNKNOWN",
            "prior owner process incarnation or family membership is not live",
        ));
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn same_process_image(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    left["pid"] == right["pid"]
        && left["image_path"] == right["image_path"]
        && left["image_sha256"] == right["image_sha256"]
        && same_process_birth(left, right)
}

fn same_owner_image(owner: &serde_json::Value, image: &serde_json::Value) -> bool {
    owner["pid"] == image["pid"] && same_process_birth(owner, image)
}

fn same_process_birth(left: &serde_json::Value, right: &serde_json::Value) -> bool {
    match (
        left["creation_filetime"].as_u64(),
        right["creation_filetime"].as_u64(),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => {
            matches!(
                (
                    left["boot_id"].as_str(),
                    left["start_ticks"].as_str(),
                    right["boot_id"].as_str(),
                    right["start_ticks"].as_str(),
                ),
                (Some(left_boot), Some(left_start), Some(right_boot), Some(right_start))
                    if left_boot == right_boot && left_start == right_start
            )
        }
    }
}

fn owner_receipt_exists(slot: &Slot) -> Result<bool> {
    match read_owner_receipt(slot) {
        Ok(Some(_)) => Ok(true),
        Ok(None) | Err(OwnerReadError::Missing) => Ok(false),
        Err(OwnerReadError::Invalid) => Err(Error::new(
            "BUS_SERVICE_OWNER_UNKNOWN",
            "managed owner receipt is invalid",
        )),
        Err(OwnerReadError::Io) => Err(Error::new(
            "BUS_SERVICE_OWNER_READ_FAILED",
            "managed owner receipt could not be read",
        )),
    }
}

fn remove_owner_receipt(slot: &Slot) -> Result<()> {
    if !validate_private_dir_beneath(&slot.data_root, &slot.owner_dir)? {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_UNKNOWN",
            "managed owner directory disappeared before receipt cleanup",
        ));
    }
    let path = slot.owner_dir.join("owner.json");
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if !metadata.is_file() || is_link_or_reparse(&metadata) {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_UNKNOWN",
                "owner receipt changed before cleanup",
            ));
        }
        fs::remove_file(path)?;
    }
    Ok(())
}

fn request_stop(slot: &mut Slot) -> Result<()> {
    if !slot.stopping {
        ensure_private_dir(&slot.data_root, &slot.owner_dir)?;
        let path = slot.owner_dir.join("stop.request");
        match write_private_new(&path, &[]) {
            Ok(()) => {}
            Err(error) if error.code == "FILE_EXISTS" => {
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() || is_link_or_reparse(&metadata) || metadata.len() != 0 {
                    return Err(Error::new(
                        "BUS_SERVICE_STOP_REQUEST_INVALID",
                        "stop marker is invalid",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
        slot.stopping = true;
    }
    Ok(())
}

fn clear_stop_request(slot: &Slot) -> Result<()> {
    if !validate_private_dir_beneath(&slot.data_root, &slot.owner_dir)? {
        return Ok(());
    }
    let path = slot.owner_dir.join("stop.request");
    match fs::symlink_metadata(&path) {
        Ok(metadata)
            if metadata.is_file() && !is_link_or_reparse(&metadata) && metadata.len() == 0 =>
        {
            #[cfg(windows)]
            swarm_process::private_permissions(&path, false)?;
            fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => Err(Error::new(
            "BUS_SERVICE_STOP_REQUEST_INVALID",
            "stop marker changed before cleanup",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn ensure_private_dir(root: &Path, path: &Path) -> Result<()> {
    let relative = owner_path_relative(root, path)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
                "service owner path contains an unsafe component",
            ));
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => validate_private_directory(&current, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&current, fs::Permissions::from_mode(0o700))?;
                }
                #[cfg(windows)]
                swarm_process::private_permissions(&current, true)?;
                let metadata = fs::symlink_metadata(&current)?;
                validate_private_directory(&current, &metadata)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn owner_path_relative<'a>(root: &Path, path: &'a Path) -> Result<&'a Path> {
    if !root.is_absolute() || !path.is_absolute() {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "service owner paths must be absolute",
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "service owner path is outside the Store root",
        )
    })?;
    if relative.as_os_str().is_empty() {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "service owner path must be below the Store root",
        ));
    }
    fs::canonicalize(root).map_err(|_| {
        Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "Store root cannot be resolved",
        )
    })?;
    Ok(relative)
}

fn validate_private_dir_beneath(root: &Path, path: &Path) -> Result<bool> {
    let relative = owner_path_relative(root, path)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
                "service owner path contains an unsafe component",
            ));
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => validate_private_directory(&current, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(true)
}

fn validate_private_directory(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_dir() || is_link_or_reparse(metadata) {
        return Err(Error::new(
            "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
            "service owner path traverses a link or non-directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
                "service owner directory is not private",
            ));
        }
    }
    #[cfg(windows)]
    swarm_process::private_permissions(path, true)?;
    Ok(())
}

fn reject_reparse_components(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(Error::new(
            "BUS_DISPATCHER_PIN_INVALID",
            "dispatcher path must be absolute",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if is_link_or_reparse(&metadata) => {
                return Err(Error::new(
                    "BUS_DISPATCHER_PIN_INVALID",
                    "dispatcher path traverses a link or reparse point",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn stop_request_exists(slot: &Slot) -> Result<bool> {
    if !validate_private_dir_beneath(&slot.data_root, &slot.owner_dir)? {
        return Ok(false);
    }
    let path = slot.owner_dir.join("stop.request");
    match fs::symlink_metadata(&path) {
        Ok(metadata)
            if metadata.is_file() && !is_link_or_reparse(&metadata) && metadata.len() == 0 =>
        {
            #[cfg(windows)]
            swarm_process::private_permissions(&path, false)?;
            Ok(true)
        }
        Ok(_) => Err(Error::new(
            "BUS_SERVICE_STOP_REQUEST_INVALID",
            "managed service stop marker is invalid",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

async fn persist_status(
    store: &Store,
    slot: &mut Slot,
    state: &str,
    error: Option<&str>,
    retry: Option<Duration>,
) -> Result<()> {
    let next = PersistedHealth {
        state: state.to_owned(),
        consecutive_failures: slot.consecutive_failures,
        error: error.map(str::to_owned),
        retry_in_ms: retry.map(|value| value.as_millis().min(u64::MAX as u128) as u64),
    };
    if slot.last_health.as_ref() == Some(&next) {
        return Ok(());
    }
    store
        .record_managed_bus_service_health(
            slot.scope.clone(),
            next.state.clone(),
            next.consecutive_failures,
            next.error.clone(),
            next.retry_in_ms,
        )
        .await?;
    slot.last_health = Some(next);
    Ok(())
}

async fn drain_on_shutdown(
    _store: &Store,
    _pin: &DispatcherPin,
    _slots: &mut BTreeMap<DeclaredServiceScope, Slot>,
) -> Result<()> {
    // Process ownership is independent of this host actor. Child handles use
    // kill_on_drop(false), and owner.json is the next host's read-only
    // adoption proof. Host shutdown alone is not authorization to stop a
    // durable bus consumer or mutate its receipt.
    Ok(())
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
