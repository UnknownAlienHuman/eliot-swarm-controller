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
    departed_empty, process_image_identity, service_owner_is_live, spawned_identity,
    write_private_new,
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
const OWNER_RECEIPT_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const PRE_READY_STOP_GRACE: Duration = Duration::from_secs(2);
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
    spawned_process_identity: Option<serde_json::Value>,
    launch_contract: Option<DispatcherLaunchContract>,
    spawn_verified: bool,
    child_exited: bool,
    missing_owner_cleanup_started: Option<Instant>,
    missing_owner_kill_attempted: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DispatcherLaunchContract {
    scope: DeclaredServiceScope,
    store_root: PathBuf,
    config_path: PathBuf,
    owner_dir: PathBuf,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    consumer_client_id: String,
    scope_digest: String,
    credential_token_sha256: String,
    worker_config_sha256: String,
    dispatcher_path: PathBuf,
    dispatcher_sha256: String,
}

impl DispatcherLaunchContract {
    fn from_launch(demand: &ManagedBusServiceDemand, pin: &DispatcherPin, slot: &Slot) -> Self {
        Self {
            scope: demand.scope.clone(),
            store_root: slot.data_root.clone(),
            config_path: slot.config_path.clone(),
            owner_dir: slot.owner_dir.clone(),
            owner_manager_id: demand.owner_manager_id.clone(),
            project_id: demand.project_id.clone(),
            automation_id: demand.automation_id.clone(),
            consumer_client_id: demand.consumer_client_id.clone(),
            scope_digest: demand.scope_digest.clone(),
            credential_token_sha256: demand.credential_token_sha256.clone(),
            worker_config_sha256: demand.worker_config_sha256.clone(),
            dispatcher_path: pin.executable.clone(),
            dispatcher_sha256: pin.sha256.to_ascii_lowercase(),
        }
    }

    fn still_matches(
        &self,
        demand: Option<&ManagedBusServiceDemand>,
        pin: &DispatcherPin,
        slot: &Slot,
    ) -> bool {
        self.scope == slot.scope
            && self.store_root == slot.data_root
            && self.config_path == slot.config_path
            && self.owner_dir == slot.owner_dir
            && self.dispatcher_path == pin.executable
            && self.dispatcher_sha256.eq_ignore_ascii_case(&pin.sha256)
            && demand.is_none_or(|current| {
                self.scope == current.scope
                    && self.owner_manager_id == current.owner_manager_id
                    && self.project_id == current.project_id
                    && self.automation_id == current.automation_id
                    && self.consumer_client_id == current.consumer_client_id
                    && self.scope_digest == current.scope_digest
                    && self.credential_token_sha256 == current.credential_token_sha256
                    && self.worker_config_sha256 == current.worker_config_sha256
            })
    }
}

impl Slot {
    fn new(scope: DeclaredServiceScope, root: &Path, now: Instant) -> Result<Self> {
        #[cfg(all(test, any(windows, target_os = "linux")))]
        if tests::slot_construction_failure(&scope, root) {
            return Err(Error::new(
                "BUS_SERVICE_OWNER_DIRECTORY_INVALID",
                "test-only scoped slot construction failure",
            ));
        }
        Ok(Self {
            data_root: root.to_path_buf(),
            config_path: managed_worker_config_path(root, &scope.service_id)?,
            owner_dir: managed_service_state_path(root, &scope)?,
            scope,
            child: None,
            spawned_image: None,
            spawned_process_identity: None,
            launch_contract: None,
            spawn_verified: false,
            child_exited: false,
            missing_owner_cleanup_started: None,
            missing_owner_kill_attempted: false,
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
    let mut constructor_health: BTreeMap<DeclaredServiceScope, PersistedHealth> = BTreeMap::new();
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

        let mut store_failed = false;
        for demand in demands.values() {
            if slots.contains_key(&demand.scope) {
                continue;
            }
            match Slot::new(demand.scope.clone(), &data_root, Instant::now()) {
                Ok(slot) => {
                    slots.insert(demand.scope.clone(), slot);
                    constructor_health.remove(&demand.scope);
                }
                Err(error) => {
                    let health = PersistedHealth {
                        state: "unknown".to_owned(),
                        consecutive_failures: 0,
                        error: Some(safe_reconcile_error(&error.code).to_owned()),
                        retry_in_ms: None,
                    };
                    let disposition_changed =
                        constructor_health.get(&demand.scope) != Some(&health);
                    if disposition_changed {
                        match store
                            .record_managed_bus_service_health(
                                demand.scope.clone(),
                                health.state.clone(),
                                health.consecutive_failures,
                                health.error.clone(),
                                health.retry_in_ms,
                            )
                            .await
                        {
                            Ok(()) => {
                                constructor_health.insert(demand.scope.clone(), health.clone());
                            }
                            Err(error) if is_store_unavailable(&error) => {
                                #[cfg(all(test, any(windows, target_os = "linux")))]
                                tests::notify_constructor_store_failure();
                                // Do not continue other Store reconciliation on
                                // an unknown database outcome. Existing child
                                // handles remain owned in `slots`.
                                store_failed = true;
                                break;
                            }
                            Err(error) if error.code == "BUS_SERVICE_SCOPE_STALE" => {
                                constructor_health.remove(&demand.scope);
                            }
                            Err(error) => return Err(error),
                        }
                        eprintln!(
                            "managed bus scope isolated during slot construction: {}",
                            health.error.as_deref().unwrap_or("BUS_SUPERVISOR_ERROR")
                        );
                    }
                }
            }
        }
        constructor_health
            .retain(|scope, _| demands.contains_key(scope) && !slots.contains_key(scope));
        if store_failed {
            tokio::select! {
                _ = tick.tick() => {},
                _ = stopping.changed() => return drain_on_shutdown(&store, &pin, &mut slots).await,
            }
            continue;
        }

        let known: Vec<_> = slots.keys().cloned().collect();
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
                    && is_store_unavailable(&status_error)
                {
                    store_failed = true;
                    break;
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
                            && is_store_unavailable(&status_error)
                        {
                            store_failed = true;
                            break;
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

async fn reconcile_unreceipted_child(
    store: &Store,
    pin: &DispatcherPin,
    demand: Option<&ManagedBusServiceDemand>,
    slot: &mut Slot,
) -> Result<()> {
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
    if elapsed < OWNER_RECEIPT_STARTUP_TIMEOUT {
        persist_status(store, slot, "starting", None, None).await?;
        return Ok(());
    }

    let launch_matches = slot
        .launch_contract
        .as_ref()
        .is_some_and(|contract| contract.still_matches(demand, pin, slot));
    if !launch_matches || !exact_spawned_dispatcher_is_live(slot, pin) {
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

    // The dispatcher writes owner.json before its first stop-request check and
    // before poll_once, its first Store/network effect. Publish the stop marker
    // and then reread owner.json: an already-ready dispatcher must leave its
    // persistent receipt visible; one that publishes later will observe the
    // marker before it can begin effects.
    if let Err(error) = request_pre_ready_stop(slot) {
        let code = safe_reconcile_error(&error.code);
        slot.last_error = Some(code);
        persist_status(store, slot, "unknown", Some(code), None).await?;
        return Ok(());
    }
    match owner_receipt_exists(slot) {
        Ok(true) => {
            slot.missing_owner_cleanup_started = None;
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
        Ok(false) => {}
        Err(error) => {
            let code = safe_reconcile_error(&error.code);
            slot.last_error = Some(code);
            persist_status(store, slot, "unknown", Some(code), None).await?;
            return Ok(());
        }
    }

    let cleanup_started = *slot
        .missing_owner_cleanup_started
        .get_or_insert_with(Instant::now);
    if Instant::now().saturating_duration_since(cleanup_started) < PRE_READY_STOP_GRACE {
        slot.last_error = Some("BUS_SERVICE_OWNER_RECEIPT_MISSING");
        persist_status(
            store,
            slot,
            "unknown",
            Some("BUS_SERVICE_OWNER_RECEIPT_MISSING"),
            None,
        )
        .await?;
        return Ok(());
    }

    if slot.missing_owner_kill_attempted {
        let code = slot
            .last_error
            .unwrap_or("BUS_SERVICE_OWNER_RECEIPT_MISSING");
        persist_status(store, slot, "unknown", Some(code), None).await?;
        return Ok(());
    }
    if !slot
        .launch_contract
        .as_ref()
        .is_some_and(|contract| contract.still_matches(demand, pin, slot))
        || !exact_spawned_dispatcher_is_live(slot, pin)
    {
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
    match owner_receipt_exists(slot) {
        Ok(false) => {}
        Ok(true) => {
            slot.missing_owner_cleanup_started = None;
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
        Err(error) => {
            let code = safe_reconcile_error(&error.code);
            slot.last_error = Some(code);
            persist_status(store, slot, "unknown", Some(code), None).await?;
            return Ok(());
        }
    }

    // Only the exact verified Child handle is signaled. If termination cannot
    // be initiated or later departure cannot be read back, keep the handle and
    // scope fence and expose unknown; never infer family absence from age.
    slot.missing_owner_kill_attempted = true;
    let code = match slot.child.as_mut() {
        Some(child) => match child.start_kill() {
            Ok(()) => "BUS_SERVICE_OWNER_RECEIPT_MISSING",
            Err(_) => "BUS_WORKER_WAIT_UNKNOWN",
        },
        None => "BUS_SERVICE_OWNER_UNKNOWN",
    };
    slot.last_error = Some(code);
    persist_status(store, slot, "unknown", Some(code), None).await
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
                if slot.spawned_process_identity.is_none()
                    && let Some(pid) = child.id()
                    && let Ok(identity) = spawned_identity(pid)
                {
                    slot.spawned_process_identity = Some(identity);
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
        if verify_owner_receipt(slot, &receipt, pin).is_err() {
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
        slot.spawned_process_identity = None;
        slot.launch_contract = None;
        slot.spawn_verified = false;
        slot.child_exited = false;
        slot.missing_owner_cleanup_started = None;
        slot.missing_owner_kill_attempted = false;
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
        // run_managed_worker publishes owner.json before its first stop check
        // and before poll_once, its first Store/network effect. When the exact
        // verified Child handle is reaped with no receipt, no worker effect or
        // descendant could have started.
        if !slot.spawn_verified
            || slot
                .launch_contract
                .as_ref()
                .is_none_or(|contract| contract.scope != slot.scope)
        {
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
        clear_stop_request(slot)?;
        slot.child = None;
        slot.spawned_image = None;
        slot.spawned_process_identity = None;
        slot.launch_contract = None;
        slot.spawn_verified = false;
        slot.child_exited = false;
        slot.missing_owner_cleanup_started = None;
        slot.missing_owner_kill_attempted = false;
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
        return reconcile_unreceipted_child(store, pin, demand, slot).await;
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
    let launch_contract = DispatcherLaunchContract::from_launch(demand, pin, slot);
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
    let spawned_process_identity = child.id().and_then(|pid| spawned_identity(pid).ok());
    let spawned_image = child.id().and_then(|pid| process_image_identity(pid).ok());
    slot.spawn_verified = spawned_image
        .as_ref()
        .is_some_and(|image| image_matches_pin(image, pin));
    slot.spawned_image = spawned_image;
    slot.spawned_process_identity = spawned_process_identity;
    slot.launch_contract = Some(launch_contract);
    slot.missing_owner_cleanup_started = None;
    slot.missing_owner_kill_attempted = false;
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

/// Confirm that the still-owned Child handle, birth identity, and pinned image
/// all identify the exact dispatcher launched for this slot. Signaling remains
/// through Child::start_kill, never through the observed PID.
fn exact_spawned_dispatcher_is_live(slot: &Slot, pin: &DispatcherPin) -> bool {
    let Some(child) = slot.child.as_ref() else {
        return false;
    };
    let Some(pid) = child.id() else {
        return false;
    };
    let Some(image) = slot.spawned_image.as_ref() else {
        return false;
    };
    let Some(identity) = slot.spawned_process_identity.as_ref() else {
        return false;
    };
    if identity["scope"] != "launcher_spawned_process"
        || identity["purpose"] != "check"
        || identity["pid"].as_u64() != Some(u64::from(pid))
        || image["pid"].as_u64() != Some(u64::from(pid))
        || !image_matches_pin(image, pin)
    {
        return false;
    }
    let Ok(current_image) = process_image_identity(pid) else {
        return false;
    };
    let Ok(current_identity) = spawned_identity(pid) else {
        return false;
    };
    same_process_image(image, &current_image) && &current_identity == identity
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
        left.get("creation_filetime"),
        right.get("creation_filetime"),
    ) {
        (Some(left), Some(right)) => matches!(
            (process_birth_filetime(left), process_birth_filetime(right)),
            (Some(left), Some(right)) if left == right
        ),
        (None, None) => matches!(
            (
                left["boot_id"].as_str(),
                left["start_ticks"].as_str(),
                right["boot_id"].as_str(),
                right["start_ticks"].as_str(),
            ),
            (Some(left_boot), Some(left_start), Some(right_boot), Some(right_start))
                if left_boot == right_boot && left_start == right_start
        ),
        _ => false,
    }
}

/// Normalize existing Windows encodings without accepting lossy, signed,
/// zero, or noncanonical process birth values.
fn process_birth_filetime(value: &serde_json::Value) -> Option<u64> {
    if let Some(birth) = value.as_u64() {
        return (birth > 0).then_some(birth);
    }
    let text = value.as_str()?;
    let birth = text.parse::<u64>().ok()?;
    (birth > 0 && birth.to_string() == text).then_some(birth)
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
        ensure_stop_request(slot)?;
        slot.stopping = true;
    }
    Ok(())
}

fn request_pre_ready_stop(slot: &Slot) -> Result<()> {
    ensure_stop_request(slot)
}

fn ensure_stop_request(slot: &Slot) -> Result<()> {
    ensure_private_dir(&slot.data_root, &slot.owner_dir)?;
    let path = slot.owner_dir.join("stop.request");
    match write_private_new(&path, &[]) {
        Ok(()) => Ok(()),
        Err(error) if error.code == "FILE_EXISTS" => {
            if stop_request_exists(slot)? {
                Ok(())
            } else {
                Err(Error::new(
                    "BUS_SERVICE_STOP_REQUEST_INVALID",
                    "stop marker is invalid",
                ))
            }
        }
        Err(error) => Err(error.into()),
    }
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

fn validate_private_directory(_path: &Path, metadata: &fs::Metadata) -> Result<()> {
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
    swarm_process::private_permissions(_path, true)?;
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
        if matches!(component, std::path::Component::Prefix(_)) {
            continue;
        }
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

#[cfg(all(test, any(windows, target_os = "linux")))]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        platform::{DataRoot, bootstrap_credential, process_group::process_birth_identity},
        store::StoreOwner,
    };
    use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
    use serde_json::{Value, json};
    use sha2::Digest;
    use std::{
        collections::BTreeSet,
        fs,
        path::{Path, PathBuf},
        process::Stdio,
        sync::{Arc, Mutex, OnceLock},
        thread,
        time::Duration as StdDuration,
    };
    use tokio::{
        process::Command,
        sync::{oneshot, watch},
        time::{self, Duration, Instant},
    };

    const DAMAGE_SCOPE: &str = "bus-script-damaged";
    const HEALTHY_SCOPE: &str = "bus-script-healthy";
    const MISSING_RECEIPT_SCOPE: &str = "bus-script-no-receipt";
    const CHILD_RELEASE_ENV: &str = "ELIOT_SWARM_BUS_SUPERVISOR_CHILD_RELEASE";
    const CHILD_TEST: &str = "host_bus_supervisor::tests::child_waits_for_release";

    #[cfg(windows)]
    #[test]
    fn bootstrap_path_canonical_dispatcher_pin_is_readable() {
        let root = test_root("canonical-dispatcher-path");
        let file = root.join("dispatcher.exe");
        fs::write(&file, b"fixture").unwrap();
        reject_reparse_components(&file).unwrap();
        reject_reparse_components(&fs::canonicalize(&file).unwrap()).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    type FailedSlotConstruction = (DeclaredServiceScope, PathBuf);

    static FAILED_SLOT_CONSTRUCTIONS: OnceLock<Mutex<BTreeSet<FailedSlotConstruction>>> =
        OnceLock::new();
    static CONSTRUCTOR_STORE_FAILURE: OnceLock<Mutex<Option<oneshot::Sender<()>>>> =
        OnceLock::new();

    pub(super) fn slot_construction_failure(scope: &DeclaredServiceScope, root: &Path) -> bool {
        FAILED_SLOT_CONSTRUCTIONS.get().is_some_and(|scopes| {
            scopes
                .lock()
                .is_ok_and(|scopes| scopes.contains(&(scope.clone(), root.to_path_buf())))
        })
    }

    pub(super) fn notify_constructor_store_failure() {
        if let Some(signal) = CONSTRUCTOR_STORE_FAILURE.get()
            && let Ok(mut signal) = signal.lock()
            && let Some(sender) = signal.take()
        {
            let _ = sender.send(());
        }
    }

    struct SlotConstructionFailureGuard(FailedSlotConstruction);

    impl Drop for SlotConstructionFailureGuard {
        fn drop(&mut self) {
            if let Some(scopes) = FAILED_SLOT_CONSTRUCTIONS.get()
                && let Ok(mut scopes) = scopes.lock()
            {
                scopes.remove(&self.0);
            }
        }
    }

    fn fail_slot_construction_for(
        scope: DeclaredServiceScope,
        root: &Path,
    ) -> SlotConstructionFailureGuard {
        let key = (scope, root.to_path_buf());
        FAILED_SLOT_CONSTRUCTIONS
            .get_or_init(|| Mutex::new(BTreeSet::new()))
            .lock()
            .expect("lock test-only slot-construction injection")
            .insert(key.clone());
        SlotConstructionFailureGuard(key)
    }

    fn arm_constructor_store_failure_signal() -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        let mut signal = CONSTRUCTOR_STORE_FAILURE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .expect("lock test-only Store-failure signal");
        assert!(
            signal.replace(sender).is_none(),
            "only one Store failure is armed"
        );
        receiver
    }

    #[test]
    fn process_birth_comparison_normalizes_exact_windows_filetimes_only() {
        assert!(same_process_birth(
            &json!({"creation_filetime": 123}),
            &json!({"creation_filetime": "123"})
        ));
        assert!(same_process_birth(
            &json!({"creation_filetime": "123"}),
            &json!({"creation_filetime": "123"})
        ));
        for invalid in [
            json!("124"),
            json!("00123"),
            json!("+123"),
            json!("-123"),
            json!("123 "),
            json!("18446744073709551616"),
            json!(0),
            json!(-123),
            json!(123.5),
            json!(true),
            json!(null),
        ] {
            assert!(!same_process_birth(
                &json!({"creation_filetime": 123}),
                &json!({"creation_filetime": invalid})
            ));
        }
        assert!(!same_process_birth(
            &json!({"creation_filetime": "corrupt", "boot_id": "boot", "start_ticks": "7"}),
            &json!({"creation_filetime": "corrupt", "boot_id": "boot", "start_ticks": "7"})
        ));
        assert!(!same_process_birth(
            &json!({"creation_filetime": 0}),
            &json!({"creation_filetime": 0})
        ));
        assert!(!same_process_birth(
            &json!({"creation_filetime": 123}),
            &json!({})
        ));
        assert!(!same_process_birth(&json!({}), &json!({})));
        assert!(same_process_birth(
            &json!({"boot_id": "boot", "start_ticks": "7"}),
            &json!({"boot_id": "boot", "start_ticks": "7"})
        ));
    }

    #[test]
    fn child_waits_for_release() {
        let Ok(release) = std::env::var(CHILD_RELEASE_ENV) else {
            return;
        };
        let release = PathBuf::from(release);
        fs::write(release.with_extension("ready"), b"ready")
            .expect("publish fixture child readiness");
        while !release.exists() {
            thread::sleep(StdDuration::from_millis(20));
        }
    }

    #[tokio::test]
    async fn constructor_failure_is_persisted_without_blocking_healthy_scope() {
        let root = test_root("bus-scope-isolation");
        let owner =
            store_with_registrations(&root, &[(DAMAGE_SCOPE, false), (HEALTHY_SCOPE, false)]).await;
        let damaged_scope = test_scope(DAMAGE_SCOPE);
        let _constructor_failure = fail_slot_construction_for(damaged_scope, &root);
        let pin = inert_pin();
        let (stop, stopping) = watch::channel(false);
        let task = tokio::spawn(run(owner.store.clone(), root.clone(), pin, stopping));

        wait_for_health(
            &root,
            DAMAGE_SCOPE,
            "unknown",
            Some("BUS_SERVICE_OWNER_DIRECTORY_INVALID"),
        )
        .await;
        wait_for_health(&root, HEALTHY_SCOPE, "dormant", None).await;
        let _ = stop.send(true);
        time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stop scoped bus supervisor")
            .expect("join scoped bus supervisor")
            .expect("scope-local constructor error is not a host error");

        let demands = owner
            .store
            .managed_bus_service_snapshot()
            .await
            .expect("read both retained managed scopes");
        assert_eq!(demands.len(), 2, "damaged scope did not hide its neighbor");
        assert!(
            demands
                .iter()
                .any(|demand| demand.scope.service_id == HEALTHY_SCOPE)
        );
        owner.close().await.expect("close temporary Store owner");
        fs::remove_dir_all(root).expect("remove exact test Store directory");
    }

    #[tokio::test]
    async fn constructor_health_sqlite_failure_holds_later_scope_reconciliation() {
        let root = test_root("bus-scope-store-failure");
        let owner =
            store_with_registrations(&root, &[(DAMAGE_SCOPE, false), (HEALTHY_SCOPE, false)]).await;
        let healthy_before = read_registration(&root, HEALTHY_SCOPE)
            .expect("read healthy registration before reconciliation")
            .expect("healthy registration exists before reconciliation");
        install_registration_update_rejection(&root, DAMAGE_SCOPE);
        let _constructor_failure = fail_slot_construction_for(test_scope(DAMAGE_SCOPE), &root);
        let store_failure = arm_constructor_store_failure_signal();
        let (stop, stopping) = watch::channel(false);
        let task = tokio::spawn(run(
            owner.store.clone(),
            root.clone(),
            inert_pin(),
            stopping,
        ));

        time::timeout(Duration::from_secs(5), store_failure)
            .await
            .expect("constructor health write reached the Store failure branch")
            .expect("Store failure signal was delivered");
        assert!(
            !task.is_finished(),
            "unknown SQLite outcome remains held and observable"
        );
        assert_eq!(
            read_registration(&root, HEALTHY_SCOPE)
                .expect("read healthy registration after held reconciliation"),
            Some(healthy_before),
            "later scope was not reconciled after unknown Store outcome"
        );
        let _ = stop.send(true);
        time::timeout(Duration::from_secs(5), task)
            .await
            .expect("stop Store-held bus supervisor")
            .expect("join Store-held bus supervisor")
            .expect("Store outage remains a held supervisor state");
        owner.close().await.expect("close temporary Store owner");
        fs::remove_dir_all(root).expect("remove exact test Store directory");
    }

    #[tokio::test]
    async fn missing_owner_receipt_keeps_scope_fenced_until_exact_child_departure() {
        let root = test_root("bus-missing-owner-receipt");
        let owner = store_with_registrations(&root, &[(MISSING_RECEIPT_SCOPE, true)]).await;
        let demand = test_demand(test_scope(MISSING_RECEIPT_SCOPE));
        let pin = test_dispatcher_pin();
        let release = root.join("release-child");
        let mut release_guard = ReleaseFileGuard::new(release.clone());
        let mut slot = Slot::new(demand.scope.clone(), &root, Instant::now())
            .expect("construct exact managed service slot");
        slot.demanded = true;
        slot.start_attempt =
            Instant::now() - OWNER_RECEIPT_STARTUP_TIMEOUT - Duration::from_secs(1);
        slot.missing_owner_cleanup_started =
            Some(Instant::now() - PRE_READY_STOP_GRACE - Duration::from_millis(1));

        let child = spawn_fixture_child(&release);
        let ready = release.with_extension("ready");
        time::timeout(Duration::from_secs(5), async {
            while !ready.is_file() {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture dispatcher entered its release wait before identity checks");
        let pid = child.id().expect("fixture dispatcher has a PID");
        let birth = process_birth_identity(pid)
            .expect("read fixture dispatcher birth identity")
            .expect("fixture dispatcher remains alive");
        let image = process_image_identity(pid).expect("read fixture dispatcher image identity");
        let process_identity = spawned_identity(pid).expect("read fixture dispatcher custody");
        slot.child = Some(child);
        assert!(
            image_matches_pin(&image, &pin),
            "child image matches exact test pin"
        );
        slot.spawned_image = Some(image);
        slot.spawned_process_identity = Some(process_identity);
        slot.spawn_verified = true;
        slot.launch_contract = Some(DispatcherLaunchContract::from_launch(&demand, &pin, &slot));
        assert!(exact_spawned_dispatcher_is_live(&slot, &pin));
        assert!(!owner_receipt_exists(&slot).expect("read missing owner receipt"));

        reconcile_unreceipted_child(&owner.store, &pin, Some(&demand), &mut slot)
            .await
            .expect("bounded missing-receipt cleanup requests exact Child stop");
        assert!(slot.missing_owner_kill_attempted);
        assert!(
            slot.child.is_some(),
            "exact Child remains owned until wait proves exit"
        );
        assert!(
            slot.launch_contract.is_some(),
            "launch contract remains fenced"
        );
        assert!(
            slot.starts.is_empty(),
            "missing receipt did not reserve a replacement"
        );
        let before_departure = owner
            .store
            .managed_bus_service_snapshot()
            .await
            .expect("read retained owner state");
        assert_eq!(
            before_departure[0].owner_state,
            ManagedBusOwnerState::LaunchUncertain,
            "no owner receipt means no durable departure proof"
        );
        assert_eq!(
            read_registration(&root, MISSING_RECEIPT_SCOPE)
                .expect("read service registration")
                .expect("service registration remains retained")
                ["managed_bus_service_health"]
                ["start_attempts_ms"]
                .as_array()
                .map(Vec::len),
            Some(1),
            "no replacement start was persisted before child departure"
        );
        let retained_health = read_registration(&root, MISSING_RECEIPT_SCOPE)
            .expect("read missing-receipt health disposition")
            .expect("missing-receipt service registration remains retained")
            ["managed_bus_service_health"]
            .clone();
        assert_eq!(retained_health["state"].as_str(), Some("unknown"));
        assert_eq!(
            retained_health["error_code"].as_str(),
            Some("BUS_SERVICE_OWNER_RECEIPT_MISSING")
        );

        time::timeout(Duration::from_secs(8), async {
            loop {
                reconcile_slot(&owner.store, &pin, Some(&demand), &mut slot)
                    .await
                    .expect("reconcile exact child after stop request");
                if slot.child.is_none() {
                    break;
                }
                time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("exact direct child departed within the bounded cleanup window");
        release_guard.release();
        let current_birth = process_birth_identity(pid)
            .expect("verify exact child departure")
            .unwrap_or(Value::Null);
        assert_ne!(
            current_birth, birth,
            "departed PID did not retain the same birth identity"
        );
        assert!(
            slot.starts.is_empty(),
            "reconciliation records departure without replacement"
        );
        let after_departure = owner
            .store
            .managed_bus_service_snapshot()
            .await
            .expect("read exact departed owner state");
        assert_eq!(
            after_departure[0].owner_state,
            ManagedBusOwnerState::Departed
        );
        owner.close().await.expect("close temporary Store owner");
        fs::remove_dir_all(root).expect("remove exact test Store directory");
    }

    fn test_root(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("eliot-{label}-{}", crate::model::new_id()));
        fs::create_dir_all(&path).expect("create exact test Store directory");
        fs::canonicalize(path).expect("canonicalize exact test Store directory")
    }

    fn test_scope(service_id: &str) -> DeclaredServiceScope {
        DeclaredServiceScope::new(
            swarm_contracts::DeclaredServicePurpose::BusConsumer,
            service_id,
            1,
        )
        .expect("construct valid test service scope")
    }

    fn registration(service_id: &str, launch_uncertain: bool) -> Value {
        let mut value = json!({
            "role":"module",
            "token_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "disabled":true,
            "bus_consumer":{
                "schema_version":1,
                "owner_manager_id":"fixture-manager",
                "project_id":"fixture-project",
                "automation_id":"fixture-automation",
                "consumer_client_id":service_id,
                "scope_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "method_scope":["bus.events.page","bus.consumer.admit"],
                "created_at_ms":1,
                "managed_service":true,
                "service_generation":1,
                "worker_config_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            }
        });
        if launch_uncertain {
            value["managed_bus_service_health"] = json!({
                "schema_version":1,
                "state":"starting",
                "consecutive_failures":0,
                "error_code":null,
                "retry_after_ms":null,
                "updated_at_ms":1,
                "start_attempts_ms":[1],
                "owner_state":"launch_uncertain",
                "owner_receipt_sha256":null
            });
        }
        value
    }

    async fn store_with_registrations(root: &Path, registrations: &[(&str, bool)]) -> StoreOwner {
        fs::create_dir_all(root).expect("create test Store root");
        let root = fs::canonicalize(root).expect("canonicalize test Store root");
        let initial = open_store_owner(&root).await;
        initial
            .close()
            .await
            .expect("close Store before fixture seeding");
        let db = Connection::open(root.join("swarm.db")).expect("open quiescent fixture database");
        for (service_id, launch_uncertain) in registrations {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    format!("client:{service_id}"),
                    serde_json::to_string(&registration(service_id, *launch_uncertain))
                        .expect("serialize fixture Module registration")
                ],
            )
            .expect("seed exact disabled managed service registration");
        }
        drop(db);
        open_store_owner(&root).await
    }

    async fn open_store_owner(root: &Path) -> StoreOwner {
        let data_root = DataRoot::acquire(root).expect("acquire test Store root");
        let config = test_config(&data_root.path);
        let credential =
            bootstrap_credential(&data_root.path).expect("load test Store operator credential");
        StoreOwner::start(data_root, config, credential)
            .await
            .expect("start test Store owner")
    }

    fn test_config(root: &Path) -> Arc<Config> {
        let mut config = Config::default();
        config.storage.data_dir = root.to_path_buf();
        Arc::new(config)
    }

    fn read_registration(root: &Path, service_id: &str) -> Result<Option<Value>> {
        let db = Connection::open_with_flags(
            root.join("swarm.db"),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let encoded = db
            .query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [format!("client:{service_id}")],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded
            .map(|raw| serde_json::from_str(&raw).map_err(Into::into))
            .transpose()
    }

    async fn wait_for_health(root: &Path, service_id: &str, state: &str, error_code: Option<&str>) {
        time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(registration) =
                    read_registration(root, service_id).expect("read retained service registration")
                {
                    let health = &registration["managed_bus_service_health"];
                    if health["state"].as_str() == Some(state)
                        && health["error_code"].as_str() == error_code
                    {
                        return;
                    }
                }
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "scope health did not reach {state}/{error_code:?}: {error}; retained={:?}",
                read_registration(root, service_id)
            )
        });
    }

    fn install_registration_update_rejection(root: &Path, service_id: &str) {
        let db = Connection::open(root.join("swarm.db")).expect("open quiescent Store database");
        let sql = format!(
            "CREATE TRIGGER reject_test_scope_health BEFORE UPDATE OF value_json ON meta \
             WHEN OLD.key='client:{service_id}' BEGIN \
             SELECT RAISE(ABORT,'fixture SQLite failure'); END;"
        );
        db.execute_batch(&sql)
            .expect("install scoped Store failure trigger");
    }

    fn test_demand(scope: DeclaredServiceScope) -> ManagedBusServiceDemand {
        ManagedBusServiceDemand {
            consumer_client_id: scope.service_id.clone(),
            scope,
            owner_manager_id: "fixture-manager".to_owned(),
            project_id: "fixture-project".to_owned(),
            automation_id: "fixture-automation".to_owned(),
            scope_digest: "b".repeat(64),
            credential_token_sha256: "a".repeat(64),
            worker_config_sha256: "c".repeat(64),
            owner_state: ManagedBusOwnerState::LaunchUncertain,
            owner_receipt_sha256: None,
            state: DemandState::Ready,
        }
    }

    fn inert_pin() -> DispatcherPin {
        DispatcherPin {
            executable: std::env::current_exe().expect("resolve test executable"),
            sha256: "0".repeat(64),
        }
    }

    fn test_dispatcher_pin() -> DispatcherPin {
        let executable = std::env::current_exe()
            .expect("resolve test dispatcher image")
            .canonicalize()
            .expect("canonicalize test dispatcher image");
        let bytes = fs::read(&executable).expect("read test dispatcher bytes");
        DispatcherPin {
            executable,
            sha256: format!("{:x}", sha2::Sha256::digest(bytes)),
        }
    }

    fn spawn_fixture_child(release: &Path) -> Child {
        let mut command = Command::new(std::env::current_exe().expect("resolve test executable"));
        command
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_RELEASE_ENV, release)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        command.spawn().expect("start real no-effect child process")
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
            fs::write(&self.path, b"release").expect("release fixture child");
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
