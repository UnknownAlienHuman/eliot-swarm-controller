//! Automatic, advisory Agent attention reports. Native snapshots are
//! generation-bound, read-only, and always run after releasing the Store lock.

use super::{Principal, Router};
use crate::common::now_ms;
use eliot_core::api::{
    EmitDefinitionArgs, MailMode, MailSendRequest, ObservationView, SourceState,
};
use eliot_core::{Agent, AgentState, CommandState, Error, ProcessState, Result};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(15);
const PAGE_SIZE: usize = 128;
const MAX_CHECKS_IN_FLIGHT: usize = 32;
const MAX_SWEEP_CHECKS: usize = 8;
const MAX_RETAINED_INCIDENTS: usize = 512;
const MAX_PENDING_CHECKS: usize = 512;
const REPORT_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SOURCE_FACTS: usize = 32;
const MAX_PENDING_KINDS: usize = 16;
const MAX_PROCESS_FACTS: usize = 16;
const MAX_STARTUP_LOSSES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum IncidentKind {
    SourceUnhealthy,
    NoProgress,
    ChildrenUnattended,
}

impl IncidentKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceUnhealthy => "source_unhealthy",
            Self::NoProgress => "no_progress",
            Self::ChildrenUnattended => "children_unattended",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReportPhase {
    Incident,
    Recovery,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct IncidentKey {
    agent: String,
    generation: i64,
    kind: IncidentKind,
    turn_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AgentKey {
    agent: String,
    generation: i64,
}

#[derive(Default)]
struct AttentionState {
    active: HashMap<IncidentKey, ActiveIncident>,
    in_flight: HashSet<AgentKey>,
    pending_checks: HashMap<AgentKey, Trigger>,
    reconcile_all: bool,
    active_reconcile_cursor: usize,
    reserved_incidents: usize,
}

#[derive(Clone)]
struct ActiveIncident {
    incident_id: String,
    baseline_progress_at: Option<i64>,
    baseline_model_progress_at: Option<i64>,
    incident_delivery: Option<FrozenReport>,
    recovery_delivery: Option<FrozenReport>,
}

#[derive(Clone)]
struct FrozenReport {
    report: AttentionReport,
    emit_command_id: String,
    affected_mail_id: String,
    supervisor_mail_id: String,
    owner_notify_id: String,
}

struct ReportContext {
    phase: ReportPhase,
    incident_id: String,
    activity: String,
    observed_at: i64,
    suspicion_window: Option<Duration>,
    trigger_event_seq: Option<i64>,
    native_status: NativeStatus,
    process_observation: ProcessObservation,
}

#[derive(Clone)]
struct CapturedAgent {
    agent: Agent,
    source_states: Vec<SourceState>,
    source_healthy: Option<bool>,
    unhealthy_source_seq: Option<i64>,
    last_model_progress_at: Option<i64>,
    progress_basis_at: Option<i64>,
    progress_age_ms: Option<i64>,
    turn_command_id: Option<String>,
    tool_coverage: eliot_store::ActiveToolCoverage,
    pending_kinds: Vec<String>,
    pending_kinds_truncated: bool,
    children: eliot_store::NativeChildrenAttention,
    children_revision: Option<i64>,
    manager_progress_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct AttentionReport {
    schema: u32,
    incident_id: String,
    phase: ReportPhase,
    reason: String,
    agent_id: String,
    generation: i64,
    agent_state: String,
    turn_id: Option<String>,
    parent_agent: Option<String>,
    observed_at: i64,
    suspicion_window_ms: Option<i64>,
    last_model_progress_at: Option<i64>,
    progress_basis_at: Option<i64>,
    progress_age_ms: Option<i64>,
    source_health: Option<bool>,
    activity: String,
    children: eliot_store::NativeChildrenAttention,
    pending_kinds: Vec<String>,
    pending_kinds_truncated: bool,
    tool_coverage: String,
    active_tool_count: Option<usize>,
    sources: Vec<SourceFact>,
    sources_truncated: bool,
    process_observation: ProcessObservation,
    native_status: NativeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    trigger_event_seq: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct ProcessObservation {
    basis: String,
    available: bool,
    sampled_at: Option<i64>,
    sample_age_ms: Option<i64>,
    complete: Option<bool>,
    coverage: String,
    clock_regressed: Option<bool>,
    record_count: usize,
    records_truncated: bool,
    records: Vec<ProcessFact>,
    error_code: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct ProcessFact {
    role: String,
    pid: u32,
    start_time: u64,
    state: String,
    exit_code: Option<i32>,
    updated_at: i64,
    age_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct SourceFact {
    source: String,
    families: Vec<String>,
    state: String,
    health: String,
    last_store_seq: Option<i64>,
    gap_id: Option<String>,
    gap_seq: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
struct NativeStatus {
    probe_requested: bool,
    freshness: String,
    basis: String,
    state: Option<String>,
    active_turn: Option<String>,
    source_unknown: Option<bool>,
    batch_unknown: Option<bool>,
    process_exited: Option<bool>,
    binding_matches: bool,
    error_code: Option<String>,
}

#[derive(Clone, Copy)]
enum Trigger {
    Sweep,
    SourceEvent { event_seq: i64 },
    AgentLost { event_seq: i64 },
}

pub(crate) async fn run_attention(
    router: Router,
    suspicion_window: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut events = router.inner.bus.subscribe();
    let mut ticks = interval(SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let state = Arc::new(Mutex::new(AttentionState::default()));
    let sweep_running = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks: JoinSet<()> = JoinSet::new();

    // Sweep immediately at startup so a source already unhealthy after restart
    // is handled without waiting for its next transition event.
    ticks.tick().await;
    launch_sweep(
        &router,
        Arc::clone(&state),
        Arc::clone(&sweep_running),
        suspicion_window,
        &mut tasks,
    );
    // Reconcile bounded retained loss evidence through the existing
    // `AgentLost` handling. Recovery publishes losses before attention
    // subscribes, and a crash between the recovery commit and this consumer
    // leaves no live event at all; the retained `lost` rows plus their
    // latest `agent.lost` sequence are the bounded evidence. Drained in
    // bounded pages and deduplicated per incident by the existing reservation
    // map plus the durable incident record: not every historical lost row
    // becomes a new incident engine.
    queue_startup_losses(&router, &state).await;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            received = events.recv() => match received {
                Ok(event) => match (
                    event.kind.as_str(),
                    event.agent.as_deref(),
                    event.generation,
                ) {
                    ("agent.lost", Some(agent), Some(generation)) => launch_check(
                        &router,
                        Arc::clone(&state),
                        &mut tasks,
                        agent,
                        generation,
                        suspicion_window,
                        Trigger::AgentLost {
                            event_seq: event.seq,
                        },
                    ),
                    (kind, Some(agent), Some(generation))
                        if attention_transition_event(kind) =>
                    {
                        launch_check(
                            &router,
                            Arc::clone(&state),
                            &mut tasks,
                            agent,
                            generation,
                            suspicion_window,
                            Trigger::SourceEvent {
                                event_seq: event.seq,
                            },
                        )
                    }
                    _ => {}
                },
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    request_full_reconcile(&state);
                    launch_sweep(
                        &router,
                        Arc::clone(&state),
                        Arc::clone(&sweep_running),
                        suspicion_window,
                        &mut tasks,
                    );
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = ticks.tick() => launch_sweep(
                &router,
                Arc::clone(&state),
                Arc::clone(&sweep_running),
                suspicion_window,
                &mut tasks,
            ),
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(_)) = joined {
                    crate::logging::record(
                        &eliot_core::SecretPolicy::default(),
                        "daemon.attention_check_failed",
                        &[],
                        None,
                    );
                }
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

fn attention_transition_event(kind: &str) -> bool {
    matches!(kind, "source.health" | "source.gap" | "source.closed")
        || matches!(
            kind,
            "turn.started"
                | "turn.completed"
                | "request.opened"
                | "request.resolved"
                | "request.closed"
                | "tool.started"
                | "tool.completed"
                | "session.ended"
        )
}

fn launch_sweep(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    suspicion_window: Duration,
    tasks: &mut JoinSet<()>,
) {
    use std::sync::atomic::Ordering;

    if tasks.len() >= MAX_CHECKS_IN_FLIGHT {
        request_full_reconcile(&state);
        return;
    }
    if running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let router = router.clone();
    tasks.spawn(async move {
        let _reset = SweepReset(running);
        if let Err(error) =
            scan_attention_agents(&router, Arc::clone(&state), suspicion_window).await
        {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.attention_scan_failed",
                &[],
                Some(&error),
            );
        }
        if let Err(error) = reconcile_attention_state(&router, state, suspicion_window).await {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.attention_reconcile_failed",
                &[],
                Some(&error),
            );
        }
    });
}

struct SweepReset(Arc<std::sync::atomic::AtomicBool>);

impl Drop for SweepReset {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

fn launch_check(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    tasks: &mut JoinSet<()>,
    agent: &str,
    generation: i64,
    suspicion_window: Duration,
    trigger: Trigger,
) {
    let key = AgentKey {
        agent: agent.to_owned(),
        generation,
    };
    if tasks.len() >= MAX_CHECKS_IN_FLIGHT || !reserve_check(&state, &key) {
        queue_check(&state, key, trigger);
        return;
    }
    let router = router.clone();
    tasks.spawn(run_check(router, state, key, suspicion_window, trigger));
}

fn reserve_check(state: &Arc<Mutex<AttentionState>>, key: &AgentKey) -> bool {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if state.in_flight.len() >= MAX_CHECKS_IN_FLIGHT || !state.in_flight.insert(key.clone()) {
        return false;
    }
    true
}

fn queue_check(state: &Arc<Mutex<AttentionState>>, key: AgentKey, trigger: Trigger) {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(pending) = state.pending_checks.get_mut(&key) {
        *pending = merge_trigger(*pending, trigger);
    } else if state.pending_checks.len() < MAX_PENDING_CHECKS {
        state.pending_checks.insert(key, trigger);
    } else {
        state.reconcile_all = true;
    }
}

fn merge_trigger(current: Trigger, incoming: Trigger) -> Trigger {
    match (current, incoming) {
        (_, lost @ Trigger::AgentLost { .. }) => lost,
        (lost @ Trigger::AgentLost { .. }, _) => lost,
        (Trigger::SourceEvent { event_seq: current }, Trigger::SourceEvent { event_seq }) => {
            Trigger::SourceEvent {
                event_seq: current.max(event_seq),
            }
        }
        (Trigger::SourceEvent { .. }, Trigger::Sweep) => current,
        (Trigger::Sweep, trigger) => trigger,
    }
}

fn request_full_reconcile(state: &Arc<Mutex<AttentionState>>) {
    state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .reconcile_all = true;
}

/// Drain retained startup losses as `AgentLost` triggers in bounded,
/// resumable pages. Each entry flows through `inspect_lost_agent` (or the
/// child cohort path when the lost manager still has unfinished children),
/// so reporting stays deduplicated per incident by the reservation map and no
/// second incident engine is introduced. Rows whose exact `agent.lost`
/// evidence was pruned carry no sequence and are skipped here, never
/// synthesized. A lost Agent whose evidence went stale before commit is
/// classified and dropped there, never mailed.
async fn queue_startup_losses(router: &Router, state: &Arc<Mutex<AttentionState>>) {
    let mut after: Option<(i64, i64)> = None;
    loop {
        let page = {
            let store = router.inner.store.lock().await;
            store.startup_lost_agents(MAX_STARTUP_LOSSES, after)
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.attention_startup_losses_failed",
                    &[],
                    Some(&error),
                );
                return;
            }
        };
        if page.is_empty() {
            return;
        }
        after = page.last().map(|loss| (loss.updated_at, loss.agent_num));
        for loss in &page {
            let Some(event_seq) = loss.event_seq else {
                continue;
            };
            queue_check(
                state,
                AgentKey {
                    agent: loss.agent.clone(),
                    generation: loss.generation,
                },
                Trigger::AgentLost { event_seq },
            );
        }
        if page.len() < MAX_STARTUP_LOSSES {
            return;
        }
    }
}

fn reserve_pending_checks(
    state: &Arc<Mutex<AttentionState>>,
    limit: usize,
) -> Vec<(AgentKey, Trigger)> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let available = MAX_CHECKS_IN_FLIGHT.saturating_sub(state.in_flight.len());
    let keys = state
        .pending_checks
        .keys()
        .filter(|key| !state.in_flight.contains(*key))
        .take(limit.min(available))
        .cloned()
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| {
            let trigger = state.pending_checks.remove(&key)?;
            state.in_flight.insert(key.clone());
            Some((key, trigger))
        })
        .collect()
}

fn active_reconcile_batch(state: &Arc<Mutex<AttentionState>>, limit: usize) -> Vec<AgentKey> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let mut keys = state
        .active
        .keys()
        .map(|key| AgentKey {
            agent: key.agent.clone(),
            generation: key.generation,
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    keys.sort_by(|left, right| {
        left.agent
            .cmp(&right.agent)
            .then_with(|| left.generation.cmp(&right.generation))
    });
    if keys.is_empty() {
        state.active_reconcile_cursor = 0;
        return keys;
    }
    let count = limit.min(keys.len());
    let start = state.active_reconcile_cursor % keys.len();
    let batch = (0..count)
        .map(|offset| keys[(start + offset) % keys.len()].clone())
        .collect();
    state.active_reconcile_cursor = (start + count) % keys.len();
    batch
}

async fn run_check(
    router: Router,
    state: Arc<Mutex<AttentionState>>,
    key: AgentKey,
    suspicion_window: Duration,
    trigger: Trigger,
) {
    let _guard = CheckGuard {
        state: Arc::clone(&state),
        key: key.clone(),
    };
    if let Err(error) = inspect_agent(
        &router,
        state,
        &key.agent,
        key.generation,
        suspicion_window,
        trigger,
    )
    .await
    {
        crate::logging::record(
            &eliot_core::SecretPolicy::default(),
            "daemon.attention_check_failed",
            &[],
            Some(&error),
        );
    }
}

struct CheckGuard {
    state: Arc<Mutex<AttentionState>>,
    key: AgentKey,
}

impl Drop for CheckGuard {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_flight
            .remove(&self.key);
    }
}

struct IncidentReservation {
    state: Arc<Mutex<AttentionState>>,
    committed: bool,
}

impl IncidentReservation {
    fn commit(&mut self, key: IncidentKey, incident: ActiveIncident) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.reserved_incidents = state.reserved_incidents.saturating_sub(1);
        state.active.insert(key, incident);
        self.committed = true;
    }
}

impl Drop for IncidentReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.reserved_incidents = state.reserved_incidents.saturating_sub(1);
        }
    }
}

async fn scan_attention_agents(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    let mut after: Option<String> = None;
    let mut checks = JoinSet::new();
    loop {
        let page = {
            let store = router.inner.store.lock().await;
            store.list_attention_agents(after.as_deref(), PAGE_SIZE)?
        };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|agent| agent.id.clone());
        for agent in page {
            while checks.len() >= MAX_SWEEP_CHECKS {
                let _ = checks.join_next().await;
            }
            let key = AgentKey {
                agent: agent.id.clone(),
                generation: agent.generation,
            };
            if !reserve_check(&state, &key) {
                queue_check(&state, key, Trigger::Sweep);
                continue;
            }
            let router = router.clone();
            checks.spawn(run_check(
                router,
                Arc::clone(&state),
                key,
                suspicion_window,
                Trigger::Sweep,
            ));
        }
        if after.is_none() || checks.len() >= MAX_SWEEP_CHECKS {
            tokio::task::yield_now().await;
        }
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

async fn reconcile_attention_state(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    retire_superseded_incidents(router, &state).await?;
    retry_pending_deliveries(router, Arc::clone(&state)).await;
    let reconcile_all = {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        std::mem::take(&mut state.reconcile_all)
    };
    if reconcile_all {
        scan_all_agents(router, Arc::clone(&state), suspicion_window).await?;
    }

    let pending = reserve_pending_checks(&state, MAX_SWEEP_CHECKS);
    let active = active_reconcile_batch(&state, MAX_SWEEP_CHECKS);
    if pending.is_empty() && active.is_empty() {
        return Ok(());
    }
    let mut checks = JoinSet::new();
    for (key, trigger) in pending {
        checks.spawn(run_check(
            router.clone(),
            Arc::clone(&state),
            key,
            suspicion_window,
            trigger,
        ));
    }
    for key in active {
        if reserve_check(&state, &key) {
            checks.spawn(run_check(
                router.clone(),
                Arc::clone(&state),
                key,
                suspicion_window,
                Trigger::Sweep,
            ));
        } else {
            queue_check(&state, key, Trigger::Sweep);
        }
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

async fn retire_superseded_incidents(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
) -> Result<()> {
    let agents = {
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        let mut agents = state
            .active
            .keys()
            .map(|key| key.agent.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        agents.sort();
        agents
    };
    for agent_id in agents {
        let (current_generation, current_state) = {
            let store = router.inner.store.lock().await;
            store
                .get_agent(&agent_id)?
                .map(|agent| (Some(agent.generation), Some(agent.state)))
                .unwrap_or((None, None))
        };
        let obsolete = {
            let state = state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .active
                .iter()
                .filter(|(key, _)| {
                    key.agent == agent_id
                        && (Some(key.generation) != current_generation
                            || (key.kind == IncidentKind::SourceUnhealthy
                                && current_state == Some(AgentState::Stopped)))
                })
                .map(|(key, incident)| {
                    let stopped_source = Some(key.generation) == current_generation
                        && key.kind == IncidentKind::SourceUnhealthy
                        && current_state == Some(AgentState::Stopped);
                    (key.clone(), incident.clone(), stopped_source)
                })
                .collect::<Vec<_>>()
        };
        for (key, incident, stopped_source) in obsolete {
            retire_obsolete_incident(router, state, key, incident, stopped_source).await;
        }
    }
    Ok(())
}

async fn retire_stopped_source_incidents(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
) {
    if captured.agent.state != AgentState::Stopped {
        return;
    }
    let obsolete = {
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .active
            .iter()
            .filter(|(key, _)| {
                key.agent == captured.agent.id
                    && key.generation == captured.agent.generation
                    && key.kind == IncidentKind::SourceUnhealthy
            })
            .map(|(key, incident)| (key.clone(), incident.clone()))
            .collect::<Vec<_>>()
    };
    for (key, incident) in obsolete {
        retire_obsolete_incident(router, state, key, incident, true).await;
    }
}

async fn retire_obsolete_incident(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    key: IncidentKey,
    incident: ActiveIncident,
    suppress_recovery: bool,
) {
    let mut incident_settled = incident.incident_delivery.is_none();
    if let Some(delivery) = &incident.incident_delivery {
        match persist_and_notify(router, delivery).await {
            Ok(()) => incident_settled = true,
            Err(error) => record_obsolete_delivery(&key, "incident", &error),
        }
    }
    if incident_settled && !suppress_recovery {
        if let Some(delivery) = &incident.recovery_delivery
            && let Err(error) = persist_and_notify(router, delivery).await
        {
            record_obsolete_delivery(&key, "recovery", &error);
        }
    } else if incident.recovery_delivery.is_some() {
        let message = if suppress_recovery {
            "Stopped source-unhealthy state retired without dispatching recovery"
        } else {
            "obsolete incident delivery was not confirmed; recovery was not dispatched"
        };
        record_obsolete_delivery(&key, "recovery", &Error::new("unknown", message));
    }
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if state
        .active
        .get(&key)
        .is_some_and(|current| current.incident_id == incident.incident_id)
    {
        state.active.remove(&key);
    }
    state.pending_checks.remove(&AgentKey {
        agent: key.agent.clone(),
        generation: key.generation,
    });
}

fn record_obsolete_delivery(key: &IncidentKey, phase: &str, error: &Error) {
    let generation = key.generation.to_string();
    crate::logging::record(
        &eliot_core::SecretPolicy::default(),
        "daemon.attention_obsolete_delivery_unconfirmed",
        &[
            ("agent", key.agent.as_str()),
            ("generation", generation.as_str()),
            ("phase", phase),
        ],
        Some(error),
    );
}

fn reserve_pending_deliveries(
    state: &Arc<Mutex<AttentionState>>,
    limit: usize,
) -> Vec<(IncidentKey, String, FrozenReport, bool)> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let available = MAX_CHECKS_IN_FLIGHT.saturating_sub(state.in_flight.len());
    let mut selected_agents = HashSet::new();
    let selected = state
        .active
        .iter()
        .filter_map(|(key, incident)| {
            let agent_key = AgentKey {
                agent: key.agent.clone(),
                generation: key.generation,
            };
            if state.in_flight.contains(&agent_key) || selected_agents.contains(&agent_key) {
                return None;
            }
            let (delivery, recovery) = incident
                .incident_delivery
                .as_ref()
                .map(|delivery| (delivery, false))
                .or(incident
                    .recovery_delivery
                    .as_ref()
                    .map(|delivery| (delivery, true)))?;
            selected_agents.insert(agent_key.clone());
            Some((
                key.clone(),
                incident.incident_id.clone(),
                delivery.clone(),
                recovery,
                agent_key,
            ))
        })
        .take(limit.min(available))
        .collect::<Vec<_>>();
    selected
        .into_iter()
        .map(|(key, incident_id, delivery, recovery, agent_key)| {
            state.in_flight.insert(agent_key);
            (key, incident_id, delivery, recovery)
        })
        .collect()
}

async fn retry_pending_deliveries(router: &Router, state: Arc<Mutex<AttentionState>>) {
    let pending = reserve_pending_deliveries(&state, MAX_SWEEP_CHECKS);
    if pending.is_empty() {
        return;
    }
    let mut tasks = JoinSet::new();
    for (key, incident_id, delivery, recovery) in pending {
        let router = router.clone();
        let state = Arc::clone(&state);
        tasks.spawn(async move {
            let _guard = CheckGuard {
                state: Arc::clone(&state),
                key: AgentKey {
                    agent: key.agent.clone(),
                    generation: key.generation,
                },
            };
            if let Err(error) = persist_and_notify(&router, &delivery).await {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.attention_delivery_retry_failed",
                    &[],
                    Some(&error),
                );
                return;
            }
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            if !state
                .active
                .get(&key)
                .is_some_and(|incident| incident.incident_id == incident_id)
            {
                return;
            }
            if recovery {
                state.active.remove(&key);
            } else if let Some(incident) = state.active.get_mut(&key)
                && incident
                    .incident_delivery
                    .as_ref()
                    .is_some_and(|current| current.emit_command_id == delivery.emit_command_id)
            {
                incident.incident_delivery = None;
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

async fn scan_all_agents(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    let mut after: Option<String> = None;
    let mut checks = JoinSet::new();
    loop {
        let page = {
            let store = router.inner.store.lock().await;
            store.list_agents(after.as_deref(), PAGE_SIZE)?
        };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|agent| agent.id.clone());
        for agent in page {
            while checks.len() >= MAX_SWEEP_CHECKS {
                let _ = checks.join_next().await;
            }
            let key = AgentKey {
                agent: agent.id,
                generation: agent.generation,
            };
            if reserve_check(&state, &key) {
                checks.spawn(run_check(
                    router.clone(),
                    Arc::clone(&state),
                    key,
                    suspicion_window,
                    Trigger::Sweep,
                ));
            } else {
                queue_check(&state, key, Trigger::Sweep);
            }
        }
        tokio::task::yield_now().await;
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

async fn inspect_agent(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    agent_id: &str,
    generation: i64,
    suspicion_window: Duration,
    trigger: Trigger,
) -> Result<()> {
    let trigger_event_seq = match trigger {
        Trigger::SourceEvent { event_seq } => Some(event_seq),
        Trigger::Sweep | Trigger::AgentLost { .. } => None,
    };

    let Some(captured) = capture_agent(router, agent_id, generation, suspicion_window).await?
    else {
        return Ok(());
    };
    if captured.agent.observe_only {
        return Ok(());
    }
    if let Trigger::AgentLost { event_seq } = trigger
        && child_attention_activity(&captured, suspicion_window).is_none()
    {
        return inspect_lost_agent(
            router,
            state,
            agent_id,
            generation,
            event_seq,
            suspicion_window,
        )
        .await;
    }
    let desired = desired_incident(&captured, suspicion_window);
    report_cleared_incidents(router, &state, &captured, suspicion_window).await?;

    let Some((key, _activity)) = desired else {
        return Ok(());
    };
    if deliver_active_incident(router, &state, &key).await? {
        return Ok(());
    }
    let Some(mut reservation) = reserve_incident(&state, &key) else {
        return Ok(());
    };
    let process_observation = process_observation(router, &captured.agent).await;
    let native_status = if key.kind == IncidentKind::ChildrenUnattended {
        NativeStatus::not_requested()
    } else {
        probe_native_status(router, &captured).await
    };
    let Some(after_probe) = revalidate_probe(router, &captured, key.kind, suspicion_window).await?
    else {
        return Ok(());
    };
    let Some((still_key, still_activity)) = desired_incident(&after_probe, suspicion_window) else {
        return Ok(());
    };
    if still_key != key {
        return Ok(());
    }
    let report = build_report(
        &after_probe,
        key.kind,
        ReportContext {
            phase: ReportPhase::Incident,
            incident_id: incident_id(&after_probe, key.kind, still_activity.as_str()),
            activity: still_activity,
            observed_at: now_ms(),
            suspicion_window: matches!(
                key.kind,
                IncidentKind::NoProgress | IncidentKind::ChildrenUnattended
            )
            .then_some(suspicion_window),
            trigger_event_seq,
            native_status,
            process_observation,
        },
    );
    let frozen = freeze_report(report);
    let incident_id = frozen.report.incident_id.clone();
    let baseline_progress_at = frozen.report.progress_basis_at;
    let baseline_model_progress_at = frozen.report.last_model_progress_at;
    let child_guard = if key.kind == IncidentKind::ChildrenUnattended {
        Some(router.inner.store.lock().await)
    } else {
        None
    };
    if let Some(store) = child_guard.as_deref()
        && !child_capture_is_current(store, &after_probe)?
    {
        return Ok(());
    }
    reservation.commit(
        key.clone(),
        ActiveIncident {
            incident_id,
            baseline_progress_at,
            baseline_model_progress_at,
            incident_delivery: Some(frozen),
            recovery_delivery: None,
        },
    );
    drop(child_guard);
    let _ = deliver_active_incident(router, &state, &key).await?;
    Ok(())
}

async fn inspect_lost_agent(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    agent_id: &str,
    generation: i64,
    event_seq: i64,
    suspicion_window: Duration,
) -> Result<()> {
    let Some(captured) = capture_agent(router, agent_id, generation, suspicion_window).await?
    else {
        return Ok(());
    };
    if captured.agent.observe_only || captured.agent.state == AgentState::Stopped {
        return Ok(());
    }
    // One incident per exact loss event: the id is anchored only on the
    // retained `agent.lost` sequence, so a post-restart re-report maps to the
    // same incident and its deterministic mutation ids make it a no-op.
    let incident_id = loss_incident_id(agent_id, generation, event_seq);
    // The deterministic mutation ids below derive from this timestamp. When
    // the evidence was pruned between the trigger and this read, there is
    // nothing to anchor on: skip rather than synthesize an event identity.
    let loss_ts = {
        let store = router.inner.store.lock().await;
        store.daemon_event_timestamp(event_seq)?
    };
    let Some(loss_ts) = loss_ts else {
        return Ok(());
    };
    if loss_report_already_complete(router, &incident_id, &captured.agent).await? {
        return Ok(());
    }
    let key = IncidentKey {
        agent: agent_id.to_owned(),
        generation,
        kind: IncidentKind::SourceUnhealthy,
        turn_id: None,
    };
    if deliver_active_incident(router, &state, &key).await? {
        return Ok(());
    }
    let Some(mut reservation) = reserve_incident(&state, &key) else {
        return Ok(());
    };
    let process_observation = process_observation(router, &captured.agent).await;
    let native_status = probe_native_status(router, &captured).await;
    let Some(after_probe) = revalidate_probe(
        router,
        &captured,
        IncidentKind::SourceUnhealthy,
        suspicion_window,
    )
    .await?
    else {
        return Ok(());
    };
    let report = build_report(
        &after_probe,
        IncidentKind::SourceUnhealthy,
        ReportContext {
            phase: ReportPhase::Incident,
            incident_id: incident_id.clone(),
            activity: activity_context(&after_probe),
            observed_at: now_ms(),
            suspicion_window: None,
            trigger_event_seq: Some(event_seq),
            native_status,
            process_observation,
        },
    );
    let frozen = freeze_loss_report(report, loss_ts);
    let incident_id = frozen.report.incident_id.clone();
    let baseline_progress_at = frozen.report.progress_basis_at;
    let baseline_model_progress_at = frozen.report.last_model_progress_at;
    reservation.commit(
        key.clone(),
        ActiveIncident {
            incident_id,
            baseline_progress_at,
            baseline_model_progress_at,
            incident_delivery: Some(frozen),
            recovery_delivery: None,
        },
    );
    persist_loss_report(router, &state, &key).await
}

/// Whether the retained loss incident needs no further work: its incident
/// event is already committed and every expected notice is already
/// delivered. Expected notices are the advisory subject copy, plus the
/// supervisor escalation for a parented subject or the owner notice
/// otherwise. A post-restart re-report that finds this state is a no-op.
async fn loss_report_already_complete(
    router: &Router,
    incident_id: &str,
    agent: &Agent,
) -> Result<bool> {
    let store = router.inner.store.lock().await;
    if !store.attention_incident_exists(incident_id)? {
        return Ok(false);
    }
    let escalation_thread = Some(format!(
        "attention-{}",
        &incident_id[..20.min(incident_id.len())]
    ));
    let advisory_thread = escalation_thread
        .as_deref()
        .map(|thread| format!("{thread}-advisory"));
    let Some(advisory_thread) = advisory_thread else {
        return Ok(false);
    };
    if !store.attention_thread_mailed_to(&advisory_thread, &agent.id)? {
        return Ok(false);
    }
    if let Some(parent) = agent.parent.as_deref().filter(|parent| *parent != agent.id) {
        let Some(escalation_thread) = escalation_thread.as_deref() else {
            return Ok(false);
        };
        if !store.attention_thread_mailed_to(escalation_thread, parent)? {
            return Ok(false);
        }
    } else if !store.attention_owner_notice_exists(incident_id)? {
        return Ok(false);
    }
    Ok(true)
}

/// Deliver the frozen loss incident, then drop it from the active set.
/// Retried reconciliation resends only missing notices: the deterministic
/// mutation ids replay the retained emit, and per-thread presence skips mail
/// that is already delivered, so a crash between the emit commit and notice
/// delivery cannot duplicate the incident.
async fn persist_loss_report(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    key: &IncidentKey,
) -> Result<()> {
    let Some((incident_id, delivery)) = ({
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state.active.get(key).map(|incident| {
            (
                incident.incident_id.clone(),
                incident.incident_delivery.clone(),
            )
        })
    }) else {
        return Ok(());
    };
    if let Some(delivery) = delivery {
        if let Err(error) = persist_loss_delivery(router, &delivery).await {
            if matches!(error.code.as_str(), "stale_generation" | "not_found") {
                let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
                if state
                    .active
                    .get(key)
                    .is_some_and(|incident| incident.incident_id == incident_id)
                {
                    state.active.remove(key);
                }
                state.pending_checks.remove(&AgentKey {
                    agent: key.agent.clone(),
                    generation: key.generation,
                });
                return Ok(());
            }
            return Err(error);
        }
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(incident) = state.active.get_mut(key)
            && incident.incident_id == incident_id
            && incident
                .incident_delivery
                .as_ref()
                .is_some_and(|current| current.emit_command_id == delivery.emit_command_id)
        {
            incident.incident_delivery = None;
        }
    }
    Ok(())
}

async fn report_cleared_incidents(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
    suspicion_window: Duration,
) -> Result<()> {
    retire_stopped_source_incidents(router, state, captured).await;
    // Mail wakes an idle manager and starts/completes a new parent turn. That
    // must not rearm this incident. Only a fully finished child cohort clears it.
    retire_finished_child_cohort(router, state, captured).await?;
    let mut candidates = recovery_candidates(state, captured);
    if candidates.is_empty() {
        return Ok(());
    }

    for (key, incident) in &candidates {
        if incident.incident_delivery.is_some() {
            let _ = deliver_active_incident(router, state, key).await?;
        }
    }

    candidates = recovery_candidates(state, captured);
    if candidates.is_empty() {
        return Ok(());
    }
    let process_observation = if candidates
        .iter()
        .any(|(_, incident)| incident.recovery_delivery.is_none())
    {
        Some(process_observation(router, &captured.agent).await)
    } else {
        None
    };
    for (key, incident) in candidates {
        let delivery = match incident.recovery_delivery {
            Some(delivery) => delivery,
            None => {
                let process_observation = process_observation.as_ref().ok_or_else(|| {
                    Error::new("internal", "recovery process observation is unavailable")
                })?;
                let report = build_report(
                    captured,
                    key.kind,
                    ReportContext {
                        phase: ReportPhase::Recovery,
                        incident_id: incident.incident_id.clone(),
                        activity: "condition_cleared".to_owned(),
                        observed_at: now_ms(),
                        suspicion_window: (key.kind == IncidentKind::NoProgress)
                            .then_some(suspicion_window),
                        trigger_event_seq: None,
                        native_status: NativeStatus::not_requested(),
                        process_observation: process_observation.clone(),
                    },
                );
                let delivery = freeze_report(report);
                let mut state_guard = state.lock().unwrap_or_else(|error| error.into_inner());
                let Some(current) = state_guard.active.get_mut(&key) else {
                    continue;
                };
                if current.incident_id != incident.incident_id {
                    continue;
                }
                current.recovery_delivery.get_or_insert(delivery).clone()
            }
        };
        if let Err(error) = persist_and_notify(router, &delivery).await {
            if matches!(error.code.as_str(), "stale_generation" | "not_found") {
                let mut state_guard = state.lock().unwrap_or_else(|error| error.into_inner());
                if state_guard
                    .active
                    .get(&key)
                    .is_some_and(|current| current.incident_id == incident.incident_id)
                {
                    state_guard.active.remove(&key);
                }
                continue;
            }
            return Err(error);
        }
        let mut state_guard = state.lock().unwrap_or_else(|error| error.into_inner());
        if state_guard.active.get(&key).is_some_and(|current| {
            current.incident_id == incident.incident_id
                && current
                    .recovery_delivery
                    .as_ref()
                    .is_some_and(|pending| pending.emit_command_id == delivery.emit_command_id)
        }) {
            state_guard.active.remove(&key);
        }
    }
    Ok(())
}

fn child_capture_is_current(store: &eliot_store::Store, captured: &CapturedAgent) -> Result<bool> {
    let Some(revision) = captured.children_revision else {
        return Ok(false);
    };
    if store.native_children_revision(&captured.agent.id, captured.agent.generation)?
        != Some(revision)
    {
        return Ok(false);
    }
    Ok(store.get_agent(&captured.agent.id)?.is_some_and(|agent| {
        agent.generation == captured.agent.generation
            && agent.active_turn == captured.agent.active_turn
    }))
}

async fn retire_finished_child_cohort(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
) -> Result<()> {
    if captured.children.unfinished != 0 {
        return Ok(());
    }
    let key = IncidentKey {
        agent: captured.agent.id.clone(),
        generation: captured.agent.generation,
        kind: IncidentKind::ChildrenUnattended,
        turn_id: None,
    };
    if !state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .active
        .contains_key(&key)
    {
        return Ok(());
    }
    // Settle only the already-frozen delivery, using its retained IDs.
    deliver_active_incident(router, state, &key).await?;
    let store = router.inner.store.lock().await;
    retire_child_incident_if_current(&store, state, &key, captured)
}

fn retire_child_incident_if_current(
    store: &eliot_store::Store,
    state: &Arc<Mutex<AttentionState>>,
    key: &IncidentKey,
    captured: &CapturedAgent,
) -> Result<()> {
    if captured.children.unfinished == 0 && child_capture_is_current(store, captured)? {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        if state
            .active
            .get(key)
            .is_some_and(|incident| incident.incident_delivery.is_none())
        {
            state.active.remove(key);
        }
    }
    Ok(())
}

fn recovery_candidates(
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
) -> Vec<(IncidentKey, ActiveIncident)> {
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    state
        .active
        .iter()
        .filter(|(key, incident)| {
            key.agent == captured.agent.id
                && key.generation == captured.agent.generation
                && (incident.recovery_delivery.is_some()
                    || incident_condition_cleared(key, incident, captured))
        })
        .map(|(key, incident)| (key.clone(), incident.clone()))
        .collect()
}

fn incident_condition_cleared(
    key: &IncidentKey,
    incident: &ActiveIncident,
    captured: &CapturedAgent,
) -> bool {
    match key.kind {
        IncidentKind::ChildrenUnattended => false,
        IncidentKind::SourceUnhealthy => {
            captured.source_healthy == Some(true)
                && !source_is_unhealthy(captured.agent.state, &captured.source_states)
        }
        IncidentKind::NoProgress => {
            if captured.agent.state == AgentState::Stopped {
                return true;
            }
            let progressed =
                timestamp_advanced(captured.progress_basis_at, incident.baseline_progress_at)
                    || timestamp_advanced(
                        captured.last_model_progress_at,
                        incident.baseline_model_progress_at,
                    );
            if progressed {
                return true;
            }
            if captured.source_healthy != Some(true) {
                return false;
            }
            let waiting =
                captured.agent.state == AgentState::Waiting || !captured.agent.pending.is_empty();
            let idle =
                captured.agent.state == AgentState::Idle && captured.agent.active_turn.is_none();
            let turn_changed = captured
                .agent
                .active_turn
                .as_deref()
                .is_some_and(|turn_id| key.turn_id.as_deref() != Some(turn_id));
            waiting || idle || turn_changed
        }
    }
}

fn timestamp_advanced(current: Option<i64>, baseline: Option<i64>) -> bool {
    match (current, baseline) {
        (Some(current), Some(baseline)) => current > baseline,
        (Some(_), None) => true,
        _ => false,
    }
}

fn reserve_incident(
    state: &Arc<Mutex<AttentionState>>,
    key: &IncidentKey,
) -> Option<IncidentReservation> {
    let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
    if guard.active.contains_key(key)
        || guard.active.len() + guard.reserved_incidents >= MAX_RETAINED_INCIDENTS
    {
        return None;
    }
    guard.reserved_incidents += 1;
    drop(guard);
    Some(IncidentReservation {
        state: Arc::clone(state),
        committed: false,
    })
}

async fn probe_native_status(router: &Router, captured: &CapturedAgent) -> NativeStatus {
    let turn = captured
        .turn_command_id
        .as_deref()
        .zip(captured.agent.active_turn.as_deref());
    let result = if captured.agent.active_turn.is_some() && turn.is_none() {
        Err(Error::new(
            "unknown",
            "exact turn command binding is unavailable",
        ))
    } else {
        router
            .inner
            .plugin_host
            .probe_snapshot(&captured.agent, turn)
            .await
    };
    NativeStatus::from_result(
        &captured.agent.id,
        captured.agent.generation,
        &captured.agent.active_turn,
        result,
    )
}

async fn revalidate_probe(
    router: &Router,
    captured: &CapturedAgent,
    kind: IncidentKind,
    suspicion_window: Duration,
) -> Result<Option<CapturedAgent>> {
    let Some(current) = capture_agent(
        router,
        &captured.agent.id,
        captured.agent.generation,
        suspicion_window,
    )
    .await?
    else {
        return Ok(None);
    };
    if current.agent.generation != captured.agent.generation
        || current.agent.active_turn != captured.agent.active_turn
        || (kind == IncidentKind::ChildrenUnattended
            && current.children_revision != captured.children_revision)
        || (kind == IncidentKind::NoProgress
            && current.progress_basis_at != captured.progress_basis_at)
    {
        return Ok(None);
    }
    Ok(Some(current))
}

fn freeze_report(report: AttentionReport) -> FrozenReport {
    FrozenReport {
        report,
        emit_command_id: ulid::Ulid::new().to_string(),
        affected_mail_id: ulid::Ulid::new().to_string(),
        supervisor_mail_id: ulid::Ulid::new().to_string(),
        owner_notify_id: ulid::Ulid::new().to_string(),
    }
}

/// Deterministic mutation ids for a loss-evidence report, derived from the
/// incident id, phase and the retained loss event timestamp. A post-restart
/// re-report of the same loss reuses these ids, so the existing
/// command/delivery idempotency makes it a no-op instead of a duplicate
/// incident. No second incident ledger is introduced.
fn freeze_loss_report(report: AttentionReport, loss_event_ts_ms: i64) -> FrozenReport {
    let phase = match report.phase {
        ReportPhase::Incident => "incident",
        ReportPhase::Recovery => "recovery",
    };
    FrozenReport {
        emit_command_id: attention_ulid(&report.incident_id, phase, "emit", loss_event_ts_ms),
        affected_mail_id: attention_ulid(
            &report.incident_id,
            phase,
            "affected-mail",
            loss_event_ts_ms,
        ),
        supervisor_mail_id: attention_ulid(
            &report.incident_id,
            phase,
            "supervisor-mail",
            loss_event_ts_ms,
        ),
        owner_notify_id: attention_ulid(
            &report.incident_id,
            phase,
            "owner-notify",
            loss_event_ts_ms,
        ),
        report,
    }
}

fn attention_ulid(incident_id: &str, phase: &str, purpose: &str, ts_ms: i64) -> String {
    let mut digest = Sha256::new();
    for part in ["attention-mutation-id", incident_id, phase, purpose] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    let finalized = digest.finalize();
    let mut random = [0u8; 16];
    random.copy_from_slice(&finalized[..16]);
    let random = u128::from_be_bytes(random) & ((1u128 << 80) - 1);
    ulid::Ulid::from_parts(u64::try_from(ts_ms).unwrap_or(0), random).to_string()
}

async fn deliver_active_incident(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    key: &IncidentKey,
) -> Result<bool> {
    let Some((incident_id, delivery)) = ({
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state.active.get(key).map(|incident| {
            (
                incident.incident_id.clone(),
                incident.incident_delivery.clone(),
            )
        })
    }) else {
        return Ok(false);
    };
    if let Some(delivery) = delivery {
        // Stale evidence (Agent gone or generation moved between capture
        // and commit) is classified, not retried: drop the frozen delivery
        // so an unchanged superseded incident cannot produce further wakes.
        if let Err(error) = persist_and_notify(router, &delivery).await {
            if matches!(error.code.as_str(), "stale_generation" | "not_found") {
                let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
                if state
                    .active
                    .get(key)
                    .is_some_and(|incident| incident.incident_id == incident_id)
                {
                    state.active.remove(key);
                }
                state.pending_checks.remove(&AgentKey {
                    agent: key.agent.clone(),
                    generation: key.generation,
                });
                return Ok(true);
            }
            return Err(error);
        }
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(incident) = state.active.get_mut(key)
            && incident.incident_id == incident_id
            && incident
                .incident_delivery
                .as_ref()
                .is_some_and(|current| current.emit_command_id == delivery.emit_command_id)
        {
            incident.incident_delivery = None;
        }
    }
    Ok(true)
}

async fn capture_agent(
    router: &Router,
    agent_id: &str,
    generation: i64,
    suspicion_window: Duration,
) -> Result<Option<CapturedAgent>> {
    let binding = {
        let store = router.inner.store.lock().await;
        let Some(agent) = store.get_agent(agent_id)? else {
            return Ok(None);
        };
        if agent.generation != generation {
            return Ok(None);
        }
        agent
    };
    let Some((children, children_revision)) =
        capture_child_activity(&router.inner.store, &binding, suspicion_window).await?
    else {
        return Ok(None);
    };
    let (
        agent,
        source_states,
        source_healthy,
        unhealthy_source_seq,
        last_model_progress_at,
        turn,
        turn_command_id,
        tool_coverage,
        manager_progress_at,
    ) = {
        let store = router.inner.store.lock().await;
        let Some(agent) = store.get_agent(agent_id)? else {
            return Ok(None);
        };
        if agent.generation != generation
            || agent.active_turn != binding.active_turn
            || (children_revision.is_some()
                && store.native_children_revision(agent_id, generation)? != children_revision)
        {
            return Ok(None);
        }
        let source_states = store
            .list_source_states(agent_id, generation)?
            .into_iter()
            .map(serde_json::from_value::<SourceState>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| Error::new("internal", "Agent source state projection is invalid"))?;
        let last_model_progress_at = store.last_model_progress(agent_id, generation)?;
        let turn = agent
            .active_turn
            .as_deref()
            .map(|turn_id| store.get_turn(agent_id, generation, turn_id))
            .transpose()?
            .flatten();
        let manager_progress_at = store
            .native_model_progress(agent_id, generation)?
            .into_iter()
            .chain(turn.as_ref().and_then(|turn| turn.started_at))
            .max();
        let turn_command_id = match agent.active_turn.as_deref() {
            Some(turn_id) => store.turn_command_id(agent_id, generation, turn_id)?,
            None => None,
        };
        let source_healthy = native_sources_healthy(&source_states);
        let unhealthy_source_seq = if source_is_unhealthy(agent.state, &source_states) {
            source_states
                .iter()
                .filter(|source| source_is_unhealthy(agent.state, std::slice::from_ref(source)))
                .filter_map(|source| {
                    store
                        .latest_unhealthy_source_event_seq(agent_id, generation, &source.source)
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max()
        } else {
            None
        };
        let tool_coverage = match (agent.active_turn.as_deref(), source_healthy) {
            (Some(turn_id), Some(true)) => {
                store.active_tool_coverage(agent_id, generation, turn_id)?
            }
            (Some(_), _) => eliot_store::ActiveToolCoverage::Unknown,
            (None, _) => eliot_store::ActiveToolCoverage::Unknown,
        };
        (
            agent,
            source_states,
            source_healthy,
            unhealthy_source_seq,
            last_model_progress_at,
            turn,
            turn_command_id,
            tool_coverage,
            manager_progress_at,
        )
    };
    let turn_started_at = turn.and_then(|turn| turn.started_at);
    let progress_basis_at = match (last_model_progress_at, turn_started_at) {
        (Some(progress), Some(started)) => Some(progress.max(started)),
        (Some(progress), None) => Some(progress),
        (None, Some(started)) => Some(started),
        (None, None) => None,
    };
    let now = now_ms();
    let progress_age_ms = progress_basis_at
        .and_then(|at| now.checked_sub(at))
        .filter(|age| *age >= 0);
    let mut pending_kinds = agent
        .pending
        .iter()
        .map(|request| request.kind.clone())
        .collect::<Vec<_>>();
    pending_kinds.sort();
    pending_kinds.dedup();
    let pending_kinds_truncated = pending_kinds.len() > MAX_PENDING_KINDS;
    pending_kinds.truncate(MAX_PENDING_KINDS);
    Ok(Some(CapturedAgent {
        agent,
        source_states,
        source_healthy,
        unhealthy_source_seq,
        last_model_progress_at,
        progress_basis_at,
        progress_age_ms,
        turn_command_id,
        tool_coverage,
        pending_kinds,
        pending_kinds_truncated,
        children,
        children_revision,
        manager_progress_at,
    }))
}

async fn capture_child_activity(
    store: &super::StoreHandle,
    manager: &Agent,
    suspicion_window: Duration,
) -> Result<Option<(eliot_store::NativeChildrenAttention, Option<i64>)>> {
    let mut children = eliot_store::NativeChildrenAttention::default();
    if manager.observe_only {
        return Ok(Some((children, None)));
    }
    let (through, revision) = {
        let store = store.lock().await;
        let Some(current) = store.get_agent(&manager.id)? else {
            return Ok(None);
        };
        if current.generation != manager.generation || current.active_turn != manager.active_turn {
            return Ok(None);
        }
        let Some(revision) = store.native_children_revision(&manager.id, manager.generation)?
        else {
            return Ok(None);
        };
        (
            store.native_children_attention_end(&manager.id, manager.generation)?,
            revision,
        )
    };
    let Some(through) = through else {
        return Ok(Some((children, Some(revision))));
    };
    let observed_at = now_ms();
    let suspicion_ms = i64::try_from(suspicion_window.as_millis()).unwrap_or(i64::MAX);
    let mut after = None;
    loop {
        let page = {
            let store = store.lock().await;
            let page = store.native_children_attention_page(
                &manager.id,
                manager.generation,
                after.as_deref(),
                &through,
                observed_at,
                suspicion_ms,
            )?;
            // Check every page against the original cohort revision as well as
            // the manager generation/turn; no mixed-time counts may escape.
            let Some(current) = store.get_agent(&manager.id)? else {
                return Ok(None);
            };
            if current.generation != manager.generation
                || current.active_turn != manager.active_turn
                || store.native_children_revision(&manager.id, manager.generation)?
                    != Some(revision)
            {
                return Ok(None);
            }
            page
        };
        children.append(page.summary);
        let Some(next) = page.next else {
            break;
        };
        after = Some(next);
        // Give admission and native ingestion an opportunity between bounded
        // pages, even when the next mutex acquisition could otherwise be ready.
        tokio::task::yield_now().await;
    }
    Ok(Some((children, Some(revision))))
}

fn desired_incident(
    captured: &CapturedAgent,
    suspicion_window: Duration,
) -> Option<(IncidentKey, String)> {
    if let Some(activity) = child_attention_activity(captured, suspicion_window) {
        return Some((
            IncidentKey {
                agent: captured.agent.id.clone(),
                generation: captured.agent.generation,
                kind: IncidentKind::ChildrenUnattended,
                turn_id: None,
            },
            activity.to_owned(),
        ));
    }
    if captured.agent.state != AgentState::Stopped
        && source_is_unhealthy(captured.agent.state, &captured.source_states)
    {
        return Some((
            IncidentKey {
                agent: captured.agent.id.clone(),
                generation: captured.agent.generation,
                kind: IncidentKind::SourceUnhealthy,
                turn_id: None,
            },
            activity_context(captured),
        ));
    }
    let threshold_ms = i64::try_from(suspicion_window.as_millis()).ok()?;
    if captured.agent.state != AgentState::Busy
        || captured.agent.active_turn.is_none()
        || captured.source_healthy != Some(true)
        || !captured.agent.pending.is_empty()
        || !captured
            .progress_age_ms
            .is_some_and(|age| age >= threshold_ms)
    {
        return None;
    }
    let turn_id = captured.agent.active_turn.clone()?;
    Some((
        IncidentKey {
            agent: captured.agent.id.clone(),
            generation: captured.agent.generation,
            kind: IncidentKind::NoProgress,
            turn_id: Some(turn_id),
        },
        activity_classification(captured, suspicion_window),
    ))
}

fn child_attention_activity(
    captured: &CapturedAgent,
    suspicion_window: Duration,
) -> Option<&'static str> {
    if captured.children.unfinished == 0 {
        return None;
    }
    let threshold = i64::try_from(suspicion_window.as_millis()).unwrap_or(i64::MAX);
    let manager_silent = captured
        .manager_progress_at
        .and_then(|at| now_ms().checked_sub(at))
        .is_some_and(|age| age >= threshold);
    if matches!(captured.agent.state, AgentState::Idle | AgentState::Stopped) || manager_silent {
        Some("manager_inactive_with_unfinished_children")
    } else if captured.agent.state == AgentState::Busy
        && (captured.children.lost > 0 || captured.children.stalled > 0)
    {
        Some("child_progress_needs_manager")
    } else {
        None
    }
}

fn activity_classification(captured: &CapturedAgent, suspicion_window: Duration) -> String {
    let context = activity_context(captured);
    if matches!(
        context.as_str(),
        "waiting_approval"
            | "waiting_question"
            | "waiting_request"
            | "active_tool"
            | "tool_coverage_unknown"
    ) {
        return context;
    }
    let threshold_ms = i64::try_from(suspicion_window.as_millis()).unwrap_or(i64::MAX);
    if captured.agent.state == AgentState::Busy
        && captured.source_healthy == Some(true)
        && captured
            .progress_age_ms
            .is_some_and(|age| age >= threshold_ms)
    {
        return "no_progress_suspected".to_owned();
    }
    context
}

fn activity_context(captured: &CapturedAgent) -> String {
    let pending = captured
        .agent
        .pending
        .iter()
        .map(|request| request.kind.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if pending.iter().any(|kind| kind.contains("approval")) {
        return "waiting_approval".to_owned();
    }
    if pending
        .iter()
        .any(|kind| kind.contains("question") || kind.contains("input"))
    {
        return "waiting_question".to_owned();
    }
    if !pending.is_empty() || captured.agent.state == AgentState::Waiting {
        return "waiting_request".to_owned();
    }
    match captured.tool_coverage {
        eliot_store::ActiveToolCoverage::Active { .. } => "active_tool".to_owned(),
        eliot_store::ActiveToolCoverage::Unknown => "tool_coverage_unknown".to_owned(),
        eliot_store::ActiveToolCoverage::Clear => {
            format!("state_{}", state_name(captured.agent.state))
        }
    }
}

fn native_sources_healthy(states: &[SourceState]) -> Option<bool> {
    let mut selected = false;
    let mut unknown = false;
    let mut unhealthy = false;
    for source in states {
        if matches!(
            source.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) || !source.families.iter().any(|family| {
            matches!(
                family.as_str(),
                "session" | "turn" | "message" | "tool" | "request" | "child" | "goal" | "usage"
            )
        }) {
            continue;
        }
        selected = true;
        match (source.state.as_str(), source.health.as_str()) {
            ("open", "ok") => {}
            ("closed", _) | ("open", "slow" | "stale" | "error") => unhealthy = true,
            _ => unknown = true,
        }
    }
    if unhealthy {
        Some(false)
    } else if selected && !unknown {
        Some(true)
    } else {
        None
    }
}

fn source_is_unhealthy(agent_state: AgentState, states: &[SourceState]) -> bool {
    states.iter().any(|source| {
        !matches!(
            source.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) && source.families.iter().any(|family| {
            matches!(
                family.as_str(),
                "session" | "turn" | "message" | "tool" | "request" | "child" | "goal" | "usage"
            )
        }) && if source.state == "closed" {
            agent_state != AgentState::Stopped || source.health == "error"
        } else {
            source.state == "open" && matches!(source.health.as_str(), "slow" | "stale" | "error")
        }
    })
}

fn incident_id(captured: &CapturedAgent, kind: IncidentKind, activity: &str) -> String {
    let identity = incident_identity(captured, kind, activity);
    digest_id(&[&identity, &ulid::Ulid::new().to_string()])
}

/// Deterministic incident id for loss evidence, anchored only on the exact
/// retained `agent.lost` event (Agent, generation, sequence). The same loss
/// re-reported after a restart maps to the same incident, anchoring the
/// deterministic mutation ids that make the re-report a Store-level no-op.
/// No fresh randomness or volatile capture detail is mixed in here.
fn loss_incident_id(agent_id: &str, generation: i64, event_seq: i64) -> String {
    digest_id(&[
        "session_lost",
        agent_id,
        &generation.to_string(),
        &event_seq.to_string(),
    ])
}

fn incident_identity(captured: &CapturedAgent, kind: IncidentKind, activity: &str) -> String {
    let mut parts = vec![
        kind.as_str().to_owned(),
        captured.agent.id.clone(),
        captured.agent.generation.to_string(),
        captured.agent.active_turn.clone().unwrap_or_default(),
        captured.progress_basis_at.unwrap_or_default().to_string(),
        activity.to_owned(),
    ];
    if kind == IncidentKind::SourceUnhealthy {
        parts.push(
            captured
                .unhealthy_source_seq
                .unwrap_or_default()
                .to_string(),
        );
        let mut sources = captured
            .source_states
            .iter()
            .filter(|source| {
                source_is_unhealthy(captured.agent.state, std::slice::from_ref(source))
            })
            .map(|source| {
                format!(
                    "{}:{}:{}:{}:{}",
                    source.source,
                    source.state,
                    source.health,
                    source.gap_id.as_deref().unwrap_or_default(),
                    source.gap_seq.unwrap_or_default()
                )
            })
            .collect::<Vec<_>>();
        sources.sort();
        parts.extend(sources);
    }
    digest_id(&parts.iter().map(String::as_str).collect::<Vec<_>>())
}

fn digest_id(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    let digest = digest.finalize();
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("attention-{hex}")
}

fn build_report(
    captured: &CapturedAgent,
    kind: IncidentKind,
    context: ReportContext,
) -> AttentionReport {
    let ReportContext {
        phase,
        incident_id,
        activity,
        observed_at,
        suspicion_window,
        trigger_event_seq,
        native_status,
        process_observation,
    } = context;
    let mut sources = captured
        .source_states
        .iter()
        .filter(|source| {
            !matches!(
                source.source.as_str(),
                "daemon" | "os" | "git" | "workspace_process"
            ) && source.families.iter().any(|family| {
                matches!(
                    family.as_str(),
                    "session"
                        | "turn"
                        | "message"
                        | "tool"
                        | "request"
                        | "child"
                        | "goal"
                        | "usage"
                )
            })
        })
        .map(|source| SourceFact {
            source: source.source.clone(),
            families: source.families.clone(),
            state: source.state.clone(),
            health: source.health.clone(),
            last_store_seq: source.last_store_seq,
            gap_id: source.gap_id.clone(),
            gap_seq: source.gap_seq,
        })
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| left.source.cmp(&right.source));
    let sources_truncated = sources.len() > MAX_SOURCE_FACTS;
    sources.truncate(MAX_SOURCE_FACTS);
    let (tool_coverage, active_tool_count) = match captured.tool_coverage {
        eliot_store::ActiveToolCoverage::Unknown => ("unknown".to_owned(), None),
        eliot_store::ActiveToolCoverage::Clear => ("clear".to_owned(), Some(0)),
        eliot_store::ActiveToolCoverage::Active { count } => ("active".to_owned(), Some(count)),
    };
    let (last_model_progress_at, progress_basis_at, progress_age_ms) =
        if kind == IncidentKind::ChildrenUnattended {
            let at = captured.manager_progress_at;
            (
                at,
                at,
                at.and_then(|at| observed_at.checked_sub(at))
                    .filter(|age| *age >= 0),
            )
        } else {
            (
                captured.last_model_progress_at,
                captured.progress_basis_at,
                captured.progress_age_ms,
            )
        };
    AttentionReport {
        schema: 1,
        incident_id,
        phase,
        reason: kind.as_str().to_owned(),
        agent_id: captured.agent.id.clone(),
        generation: captured.agent.generation,
        agent_state: state_name(captured.agent.state).to_owned(),
        turn_id: captured.agent.active_turn.clone(),
        parent_agent: captured.agent.parent.clone(),
        observed_at,
        suspicion_window_ms: suspicion_window
            .and_then(|window| i64::try_from(window.as_millis()).ok()),
        last_model_progress_at,
        progress_basis_at,
        progress_age_ms,
        source_health: captured.source_healthy,
        activity,
        children: captured.children.clone(),
        pending_kinds: captured.pending_kinds.clone(),
        pending_kinds_truncated: captured.pending_kinds_truncated,
        tool_coverage,
        active_tool_count,
        sources,
        sources_truncated,
        process_observation,
        native_status,
        trigger_event_seq,
    }
}

async fn process_observation(router: &Router, agent: &Agent) -> ProcessObservation {
    let view = match router.inner.monitoring.view(agent).await {
        Ok(view) => view,
        Err(error) => return ProcessObservation::unavailable(&error.code),
    };
    match view {
        ObservationView::Unavailable { error_code, .. } => {
            ProcessObservation::unavailable(&error_code)
        }
        ObservationView::Data(view) => {
            let record_count = view.processes.len();
            let observed_at = now_ms();
            let records = view
                .processes
                .iter()
                .take(MAX_PROCESS_FACTS)
                .map(|record| ProcessFact {
                    role: record.role.clone(),
                    pid: record.identity.pid,
                    start_time: record.identity.start_time,
                    state: process_state_name(record.state).to_owned(),
                    exit_code: record.exit_code,
                    updated_at: record.updated_at,
                    age_ms: observed_at
                        .checked_sub(record.updated_at)
                        .filter(|age| *age >= 0),
                })
                .collect::<Vec<_>>();
            ProcessObservation {
                basis: "monitor_view_cache".to_owned(),
                available: view.stats.available,
                sampled_at: view.stats.sampled_at,
                sample_age_ms: view.stats.age_ms,
                complete: view.stats.complete,
                coverage: stats_coverage_name(view.stats.coverage).to_owned(),
                clock_regressed: Some(view.stats.clock_regressed),
                record_count,
                records_truncated: view.processes_truncated || record_count > MAX_PROCESS_FACTS,
                records,
                error_code: view.stats.error.map(|code| code.chars().take(64).collect()),
            }
        }
    }
}

impl ProcessObservation {
    fn unavailable(error_code: &str) -> Self {
        Self {
            basis: "monitor_view_cache".to_owned(),
            available: false,
            sampled_at: None,
            sample_age_ms: None,
            complete: None,
            coverage: "unknown".to_owned(),
            clock_regressed: None,
            record_count: 0,
            records_truncated: false,
            records: Vec::new(),
            error_code: Some(error_code.chars().take(64).collect()),
        }
    }
}

fn stats_coverage_name(coverage: eliot_core::api::StatsCoverage) -> &'static str {
    match coverage {
        eliot_core::api::StatsCoverage::Unknown => "unknown",
        eliot_core::api::StatsCoverage::Stale => "stale",
        eliot_core::api::StatsCoverage::Complete => "complete",
        eliot_core::api::StatsCoverage::Partial => "partial",
    }
}

fn process_state_name(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Running => "running",
        ProcessState::Exited => "exited",
        ProcessState::Unknown => "unknown",
    }
}

impl NativeStatus {
    fn not_requested() -> Self {
        Self {
            probe_requested: false,
            freshness: "freshness_unspecified".to_owned(),
            basis: "plugin_snapshot".to_owned(),
            state: None,
            active_turn: None,
            source_unknown: None,
            batch_unknown: None,
            process_exited: None,
            binding_matches: true,
            error_code: None,
        }
    }

    fn unavailable(code: &str) -> Self {
        Self {
            probe_requested: true,
            freshness: "freshness_unspecified".to_owned(),
            basis: "plugin_snapshot".to_owned(),
            state: None,
            active_turn: None,
            source_unknown: None,
            batch_unknown: None,
            process_exited: None,
            binding_matches: false,
            error_code: Some(code.chars().take(64).collect()),
        }
    }

    fn from_result(
        expected_agent: &str,
        expected_generation: i64,
        expected_turn: &Option<String>,
        result: Result<Value>,
    ) -> Self {
        let value = match result {
            Ok(value) => value,
            Err(error) => return Self::unavailable(&error.code),
        };
        let generation_matches = value.get("generation").and_then(Value::as_i64)
            == Some(expected_generation)
            && value.get("agent_id").and_then(Value::as_str) == Some(expected_agent);
        let returned_turn = value
            .get("active_turn")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let returned_last_turn = value
            .get("last_turn")
            .and_then(|turn| turn.get("turn_id"))
            .and_then(Value::as_str);
        let binding_matches = generation_matches
            && match (expected_turn.as_deref(), returned_turn.as_deref()) {
                (Some(expected), Some(actual)) => actual == expected,
                (Some(expected), None) => returned_last_turn == Some(expected),
                (None, _) => true,
            };
        let state = value
            .get("state")
            .and_then(Value::as_str)
            .filter(|state| {
                matches!(
                    *state,
                    "busy" | "waiting" | "idle" | "stopped" | "lost" | "unknown"
                )
            })
            .map(ToOwned::to_owned);
        let (basis, freshness) = if binding_matches {
            (
                observation_label(value.get("observation_basis"), "plugin_snapshot"),
                observation_label(value.get("observation_freshness"), "freshness_unspecified"),
            )
        } else {
            (
                "plugin_snapshot".to_owned(),
                "freshness_unspecified".to_owned(),
            )
        };
        Self {
            probe_requested: true,
            freshness,
            basis,
            state: binding_matches.then_some(state).flatten(),
            active_turn: binding_matches.then_some(returned_turn).flatten(),
            source_unknown: binding_matches
                .then(|| value.get("source_unknown").and_then(Value::as_bool))
                .flatten(),
            batch_unknown: binding_matches
                .then(|| value.get("batch_unknown").and_then(Value::as_bool))
                .flatten(),
            process_exited: binding_matches
                .then(|| value.get("process_exited").and_then(Value::as_bool))
                .flatten(),
            binding_matches,
            error_code: value
                .get("error_code")
                .and_then(Value::as_str)
                .map(|code| code.chars().take(64).collect()),
        }
    }
}

fn observation_label(value: Option<&Value>, fallback: &str) -> String {
    const MAX_LABEL_BYTES: usize = 64;
    value
        .and_then(Value::as_str)
        .filter(|label| {
            !label.is_empty()
                && label.len() <= MAX_LABEL_BYTES
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
        .unwrap_or(fallback)
        .to_owned()
}

async fn persist_and_notify(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let data = serde_json::to_value(report)
        .map_err(|_| Error::new("internal", "attention report could not be encoded"))?;
    let request = EmitDefinitionArgs {
        topic: "agent.attention".to_owned(),
        data,
    };
    let args = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "attention event args could not be encoded"))?;
    let already_admitted = {
        let store = router.inner.store.lock().await;
        store.get_command(&delivery.emit_command_id)?.is_some()
    };
    if !already_admitted {
        // Only the internal attention verb reaches the bound commit; a public
        // emit naming the reserved topic is rejected at admission.
        router
            .submit_attention_event(
                &json!({"id": delivery.emit_command_id.clone(), "verb": "attention.emit", "args": args}),
            )
            .await?;
    }
    wait_for_applied(router, &delivery.emit_command_id).await?;
    send_notice(router, delivery).await?;
    Ok(())
}

/// Persist one frozen loss-evidence delivery. The deterministic mutation ids
/// replay the retained emit when this loss was already reported (a retained
/// command binding skips submission entirely), and per-thread presence skips
/// mail that is already delivered, so a crash between the emit commit and
/// notice delivery — or a restart re-report — cannot duplicate the incident.
async fn persist_loss_delivery(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let data = serde_json::to_value(report)
        .map_err(|_| Error::new("internal", "attention report could not be encoded"))?;
    let request = EmitDefinitionArgs {
        topic: "agent.attention".to_owned(),
        data,
    };
    let args = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "attention event args could not be encoded"))?;
    let already_admitted = {
        let store = router.inner.store.lock().await;
        store.get_command(&delivery.emit_command_id)?.is_some()
    };
    if !already_admitted
        && let Err(error) = router
            .submit_attention_event(
                &json!({"id": delivery.emit_command_id.clone(), "verb": "attention.emit", "args": args}),
            )
            .await
        // A retained binding without a command row is this incident's own
        // emit (deterministic ids collide only with themselves): continue
        // to settlement instead of duplicating it.
        && (error.code != "id_conflict" || !is_own_loss_emit(router, delivery).await?)
    {
        return Err(error);
    }
    wait_for_applied(router, &delivery.emit_command_id).await?;
    send_loss_notice(router, delivery).await?;
    Ok(())
}

/// Whether the retained command under this loss delivery's deterministic emit
/// id is this incident's own attention emit.
async fn is_own_loss_emit(router: &Router, delivery: &FrozenReport) -> Result<bool> {
    let store = router.inner.store.lock().await;
    let retained = store.get_command(&delivery.emit_command_id)?;
    let Some(command) = retained else {
        return Ok(false);
    };
    if command.verb != "attention.emit" {
        return Ok(false);
    }
    let ours = serde_json::from_value::<EmitDefinitionArgs>(command.args.clone())
        .map(|definition| {
            definition.topic == "agent.attention"
                && definition.data.get("incident_id").and_then(Value::as_str)
                    == Some(delivery.report.incident_id.as_str())
        })
        .unwrap_or(false);
    Ok(ours)
}

async fn wait_for_applied(router: &Router, command_id: &str) -> Result<()> {
    let mut progress = router.progress_receiver();
    timeout(REPORT_SETTLEMENT_TIMEOUT, async {
        loop {
            let command = {
                let store = router.inner.store.lock().await;
                store.get_command(command_id)?
            };
            match command.as_ref().map(|command| command.state) {
                Some(CommandState::Applied) => return Ok(()),
                Some(
                    CommandState::Rejected
                    | CommandState::Failed
                    | CommandState::Cancelled
                    | CommandState::Unknown,
                ) => {
                    // Preserve a classified commit failure (notably
                    // `stale_generation` from the trusted attention commit)
                    // so callers can drop superseded evidence instead of
                    // retrying it.
                    let code = command
                        .as_ref()
                        .and_then(|command| command.outcome.as_ref())
                        .and_then(|outcome| outcome.get("code"))
                        .and_then(Value::as_str)
                        .filter(|code| {
                            matches!(
                                *code,
                                "stale_generation" | "not_found" | "invalid" | "bad_state"
                            )
                        })
                        .unwrap_or("unknown");
                    if command
                        .as_ref()
                        .is_some_and(|command| command.state == CommandState::Unknown)
                    {
                        return Err(Error::new(
                            code,
                            "attention event commit outcome is unknown",
                        ));
                    }
                    return Err(Error::new(code, "attention event did not commit"));
                }
                _ => {}
            }
            progress
                .changed()
                .await
                .map_err(|_| Error::new("unavailable", "router progress channel closed"))?;
        }
    })
    .await
    .map_err(|_| Error::new("unknown", "attention event settlement deadline elapsed"))?
}

/// Shared notice text and threads for one frozen report: the inbox text, the
/// escalation thread (which wakes an eligible idle recipient at most once
/// per thread), and the subject copy thread (advisory and never waking,
/// except for a `children_unattended` report addressed to the manager
/// itself, whose subject copy is the actionable escalation).
fn attention_notice_parts(report: &AttentionReport) -> (String, Option<String>, Option<String>) {
    let phase_label = match report.phase {
        ReportPhase::Incident => "report",
        ReportPhase::Recovery => "recovery",
    };
    let diagnosis = diagnosis_summary(report);
    let text = format!(
        "Eliot attention {phase_label} for Agent {} generation {}. Incident {}. {diagnosis} See the structured x.user.agent.attention event for details.",
        report.agent_id, report.generation, report.incident_id,
    );
    let escalation_thread = Some(format!(
        "attention-{}",
        &report.incident_id[..20.min(report.incident_id.len())]
    ));
    let affected_thread = if report.reason == "children_unattended" {
        escalation_thread.clone()
    } else {
        escalation_thread
            .as_deref()
            .map(|thread| format!("{thread}-advisory"))
    };
    (text, escalation_thread, affected_thread)
}

async fn send_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    // The subject's own copy is advisory: durable in its inbox for voluntary
    // reads and acknowledgement, but never wakes it (the Store excludes the
    // `-advisory` thread from mail wake claims and reoffer pages). A
    // `children_unattended` report is addressed to the manager itself, so its
    // subject copy is the actionable escalation and keeps the waking thread.
    // At most one incident and one recovery notice are ever sent per
    // incident, and the Store spends each escalation thread's wake only once,
    // so an unchanged unread set cannot produce further wakes.
    let (text, escalation_thread, affected_thread) = attention_notice_parts(report);
    let affected = MailSendRequest {
        id: delivery.affected_mail_id.clone(),
        to: vec![report.agent_id.clone()],
        mode: MailMode::Queue,
        text: text.clone(),
        thread: affected_thread,
        reply_to_delivery: None,
    };
    let mut delivery_errors = Vec::with_capacity(2);
    match serde_json::to_value(affected) {
        Ok(affected) => {
            if let Err(error) = router.mail_send(&affected, &Principal::Owner).await {
                delivery_errors.push(format!("affected_mail:{}", error.code));
            }
        }
        Err(_) => delivery_errors.push("affected_mail:encode".to_owned()),
    }

    if let Some(parent) = report
        .parent_agent
        .as_ref()
        .filter(|parent| *parent != &report.agent_id && report.reason != "children_unattended")
    {
        let supervisor = MailSendRequest {
            id: delivery.supervisor_mail_id.clone(),
            to: vec![parent.clone()],
            mode: MailMode::Queue,
            text,
            thread: escalation_thread,
            reply_to_delivery: None,
        };
        match serde_json::to_value(supervisor) {
            Ok(supervisor) => {
                if let Err(error) = router.mail_send(&supervisor, &Principal::Owner).await {
                    delivery_errors.push(format!("supervisor_mail:{}", error.code));
                }
            }
            Err(_) => delivery_errors.push("supervisor_mail:encode".to_owned()),
        }
    } else {
        let title = match report.phase {
            ReportPhase::Incident => "Agent attention",
            ReportPhase::Recovery => "Agent attention recovered",
        };
        let diagnosis = diagnosis_summary(report);
        let message = format!(
            "Agent {} generation {}: {} (incident {}). {diagnosis} See the structured x.user.agent.attention event for details.",
            report.agent_id, report.generation, report.reason, report.incident_id
        );
        if let Err(error) = router
            .submit(
                &json!({
                    "id": delivery.owner_notify_id.clone(),
                    "verb": "notify",
                    "args": {"title": title, "message": message}
                }),
                &Principal::Owner,
            )
            .await
        {
            delivery_errors.push(format!("owner_notify:{}", error.code));
        }
        if report.reason == "children_unattended"
            && let Err(error) = wait_for_applied(router, &delivery.owner_notify_id).await
        {
            delivery_errors.push(format!("owner_notify_settlement:{}", error.code));
        }
    }
    if delivery_errors.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            "unknown",
            format!(
                "attention notice delivery incomplete: {}",
                delivery_errors.join(", ")
            ),
        ))
    }
}

/// Send the notices for one frozen loss-evidence delivery, resending only
/// what is missing. Mail is skipped when its thread already reached the
/// recipient (a retained `id_conflict` binding means the notice was already
/// delivered, possibly acked and pruned since), and the owner notice is
/// skipped when its command or a notice naming this incident already exists.
/// A loss incident is never about a `children_unattended` cohort, so the
/// subject copy is always the advisory thread here.
async fn send_loss_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let (text, escalation_thread, affected_thread) = attention_notice_parts(report);
    let mut delivery_errors = Vec::with_capacity(2);
    send_loss_mail(
        router,
        &delivery.affected_mail_id,
        &report.agent_id,
        affected_thread,
        text.clone(),
        &mut delivery_errors,
        "affected_mail",
    )
    .await;
    if let Some(parent) = report
        .parent_agent
        .as_ref()
        .filter(|parent| *parent != &report.agent_id)
    {
        send_loss_mail(
            router,
            &delivery.supervisor_mail_id,
            parent,
            escalation_thread,
            text,
            &mut delivery_errors,
            "supervisor_mail",
        )
        .await;
    } else {
        let owner_done = {
            let store = router.inner.store.lock().await;
            store.get_command(&delivery.owner_notify_id)?.is_some()
                || store.attention_owner_notice_exists(&report.incident_id)?
        };
        if !owner_done {
            let title = match report.phase {
                ReportPhase::Incident => "Agent attention",
                ReportPhase::Recovery => "Agent attention recovered",
            };
            let diagnosis = diagnosis_summary(report);
            let message = format!(
                "Agent {} generation {}: {} (incident {}). {diagnosis} See the structured x.user.agent.attention event for details.",
                report.agent_id, report.generation, report.reason, report.incident_id
            );
            let submit_owner = |id: &str| {
                let id = id.to_owned();
                let title = title.to_owned();
                let message = message.clone();
                async move {
                    router
                        .submit(
                            &json!({
                                "id": id,
                                "verb": "notify",
                                "args": {"title": title, "message": message}
                            }),
                            &Principal::Owner,
                        )
                        .await
                }
            };
            if let Err(error) = submit_owner(&delivery.owner_notify_id).await {
                // The retained evidence can predate the retry window: without
                // a binding this id cannot be admitted, so fall back to a
                // fresh owner-notice id once. The incident id stays
                // deterministic, so the next restart still reconciles by
                // incident record instead of duplicating.
                if error.code == "id_expired" && {
                    let store = router.inner.store.lock().await;
                    store.get_command(&delivery.owner_notify_id)?.is_none()
                } {
                    if let Err(error) = submit_owner(&ulid::Ulid::new().to_string()).await {
                        delivery_errors.push(format!("owner_notify:{}", error.code));
                    }
                } else {
                    delivery_errors.push(format!("owner_notify:{}", error.code));
                }
            }
        }
    }
    if delivery_errors.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            "unknown",
            format!(
                "attention notice delivery incomplete: {}",
                delivery_errors.join(", ")
            ),
        ))
    }
}

/// Send one loss notice unless its thread already reached the recipient. A
/// retained `id_conflict` binding means the notice was already delivered
/// (possibly acked and pruned since): re-check presence and skip instead of
/// duplicating it.
#[allow(clippy::too_many_arguments)]
async fn send_loss_mail(
    router: &Router,
    mail_id: &str,
    recipient: &str,
    thread: Option<String>,
    text: String,
    delivery_errors: &mut Vec<String>,
    label: &str,
) {
    let Some(thread) = thread else {
        return;
    };
    let already_mailed = {
        let store = router.inner.store.lock().await;
        store.attention_thread_mailed_to(&thread, recipient)
    };
    let already_mailed = match already_mailed {
        Ok(mailed) => mailed,
        Err(error) => {
            delivery_errors.push(format!("{label}:{}", error.code));
            return;
        }
    };
    if already_mailed {
        return;
    }
    let request = MailSendRequest {
        id: mail_id.to_owned(),
        to: vec![recipient.to_owned()],
        mode: MailMode::Queue,
        text,
        thread: Some(thread),
        reply_to_delivery: None,
    };
    let encoded = match serde_json::to_value(request) {
        Ok(encoded) => encoded,
        Err(_) => {
            delivery_errors.push(format!("{label}:encode"));
            return;
        }
    };
    if let Err(error) = router.mail_send(&encoded, &Principal::Owner).await {
        if error.code == "id_conflict" {
            let still_missing = {
                let store = router.inner.store.lock().await;
                store.attention_thread_mailed_to(
                    encoded
                        .get("thread")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                    recipient,
                )
            };
            if still_missing.is_ok_and(|missing| !missing) {
                return;
            }
        }
        delivery_errors.push(format!("{label}:{}", error.code));
    }
}

fn diagnosis_summary(report: &AttentionReport) -> String {
    let source_health = match report.source_health {
        Some(true) => "healthy",
        Some(false) => "unhealthy",
        None => "unknown",
    };
    let progress_age = report
        .progress_age_ms
        .map(|age| age.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let native_state = report.native_status.state.as_deref().unwrap_or("unknown");
    let native_error = report.native_status.error_code.as_deref().unwrap_or("none");
    let process_age = report
        .process_observation
        .sample_age_ms
        .map(|age| age.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let process_complete = report
        .process_observation
        .complete
        .map(|complete| complete.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let process_exited = report
        .native_status
        .process_exited
        .map(|exited| exited.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "Diagnosis: reason={}, activity={}, children_unfinished={}, children_lost={}, children_stalled={}, source_health={}, progress_age_ms={}, native_probe_requested={}, native_state={}, native_error={}, native_basis={}, native_freshness={}, native_binding_matches={}, process_coverage={}, process_sample_age_ms={}, process_complete={}, process_exited={}",
        report.reason,
        report.activity,
        report.children.unfinished,
        report.children.lost,
        report.children.stalled,
        source_health,
        progress_age,
        report.native_status.probe_requested,
        native_state,
        native_error,
        report.native_status.basis,
        report.native_status.freshness,
        report.native_status.binding_matches,
        report.process_observation.coverage,
        process_age,
        process_complete,
        process_exited,
    )
}

fn state_name(state: AgentState) -> &'static str {
    match state {
        AgentState::Created => "created",
        AgentState::Starting => "starting",
        AgentState::Idle => "idle",
        AgentState::Busy => "busy",
        AgentState::Waiting => "waiting",
        AgentState::Stopped => "stopped",
        AgentState::Failed => "failed",
        AgentState::Lost => "lost",
        AgentState::Unknown => "unknown",
    }
}

#[cfg(test)]
mod child_attention_tests {
    use super::*;

    fn store_with_children(child_count: usize) -> (eliot_store::Store, Agent, Vec<String>) {
        use eliot_core::{ChildRegistration, NativeEvent, SourceBatch, SubmitCommand};
        let now = now_ms();
        let mut store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"harness":"codex", "profile":"page-test", "profile_snapshot":{},
            "project":"page-test", "workspace":"page-test"});
        let command = launch.id.clone();
        let manager_id = store
            .submit("owner", launch, now)
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&command, now).unwrap();
        let plan = ["session", "turn", "message"]
            .into_iter()
            .map(|family| (family.to_owned(), "stream".to_owned()))
            .collect();
        store
            .install_plugin_sources(&manager_id, 1, &command, &plan, now)
            .unwrap();
        store.put_batch(SourceBatch {
            batch_id: "ready".to_owned(), agent_id: manager_id.clone(), generation: 1,
            source: "stream".to_owned(), cursor: None,
            events: vec![NativeEvent { kind: "session.ready".to_owned(), native_id: "ready".to_owned(),
                ts: Some(now), data: json!({"command_id":command,"native_scope":"page-test","native_ref":"manager"}) }],
        }, now).unwrap();
        store
            .settle(&command, CommandState::Applied, None, now)
            .unwrap();
        let mut children = Vec::new();
        for index in 0..child_count {
            children.push(
                store
                    .register_native_child(
                        &manager_id,
                        1,
                        ChildRegistration {
                            registration_id: format!("child-{index}"),
                            parent_id: manager_id.clone(),
                            parent_generation: 1,
                            parent_native_ref: "manager".to_owned(),
                            native_scope: "page-test".to_owned(),
                            native_ref: format!("child-{index}"),
                        },
                        now,
                    )
                    .unwrap()
                    .value
                    .agent
                    .id,
            );
        }
        let manager = store.get_agent(&manager_id).unwrap().unwrap();
        (store, manager, children)
    }

    #[tokio::test]
    async fn pages_release_store_and_reject_a_replaced_manager_generation() {
        use eliot_core::SubmitCommand;
        let now = now_ms();
        let (store, manager, _) =
            store_with_children(eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE + 1);
        let manager_id = manager.id.clone();
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let capture = capture_child_activity(&store, &manager, Duration::from_secs(900));
        tokio::pin!(capture);
        // The first pending point is the explicit handoff after one complete page.
        assert!(futures_util::poll!(&mut capture).is_pending());
        {
            let mut writer = store
                .try_lock()
                .expect("capture retained Store across a page handoff");
            writer.mark_lost(&manager_id, 1, now + 1).unwrap();
            // Store-only fixture: no process or native reader was launched.
            writer
                .close_native_sources(&manager_id, 1, now + 1)
                .unwrap();
            writer
                .release_restart_closed_generation(
                    &manager_id,
                    1,
                    "store_only_fixture",
                    false,
                    now + 1,
                )
                .unwrap();
            let mut resume = SubmitCommand::new(ulid::Ulid::new().to_string(), "resume");
            resume.target = Some(manager_id.clone());
            resume.expected_generation = Some(1);
            let resume_id = resume.id.clone();
            writer.submit("owner", resume, now + 2).unwrap();
            writer.dispatch(&resume_id, now + 2).unwrap();
            assert_eq!(
                writer.get_agent(&manager_id).unwrap().unwrap().generation,
                2
            );
        }
        assert!(capture.await.unwrap().is_none());
    }

    fn child_turn_fact(
        store: &mut eliot_store::Store,
        manager: &Agent,
        child: &str,
        turn: &str,
        kind: &str,
    ) {
        let id = ulid::Ulid::new().to_string();
        let data = if kind == "turn.completed" {
            json!({"turn_id":turn, "status":"completed"})
        } else {
            json!({"turn_id":turn})
        };
        store
            .put_native_child_batch(
                &manager.id,
                manager.generation,
                eliot_core::SourceBatch {
                    batch_id: id.clone(),
                    agent_id: child.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    cursor: Some(id.clone()),
                    events: vec![eliot_core::NativeEvent {
                        kind: kind.to_owned(),
                        native_id: id,
                        ts: Some(now_ms()),
                        data,
                    }],
                },
                now_ms(),
            )
            .unwrap();
    }

    async fn assert_overlap_invalidates_capture(existing_child: bool) {
        let (mut store, manager, children) =
            store_with_children(eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE + 1);
        for child in &children[..eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE] {
            child_turn_fact(&mut store, &manager, child, "finished", "turn.completed");
        }
        let a = children.last().unwrap();
        child_turn_fact(&mut store, &manager, a, "A", "turn.started");
        let through = store
            .native_children_attention_end(&manager.id, manager.generation)
            .unwrap();
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let capture = capture_child_activity(&store, &manager, Duration::from_secs(900));
        tokio::pin!(capture);
        assert!(futures_util::poll!(&mut capture).is_pending());
        {
            let mut writer = store.try_lock().unwrap();
            let b = if existing_child {
                children[0].clone()
            } else {
                writer
                    .register_native_child(
                        &manager.id,
                        manager.generation,
                        eliot_core::ChildRegistration {
                            registration_id: "overlapping-B".to_owned(),
                            parent_id: manager.id.clone(),
                            parent_generation: manager.generation,
                            parent_native_ref: "manager".to_owned(),
                            native_scope: "page-test".to_owned(),
                            native_ref: "overlapping-B".to_owned(),
                        },
                        now_ms(),
                    )
                    .unwrap()
                    .value
                    .agent
                    .id
            };
            child_turn_fact(&mut writer, &manager, &b, "B", "turn.started");
            child_turn_fact(&mut writer, &manager, a, "A", "turn.completed");
            if existing_child {
                assert_eq!(
                    writer
                        .native_children_attention_end(&manager.id, manager.generation)
                        .unwrap(),
                    through
                );
            }
            let current = writer.get_agent(&manager.id).unwrap().unwrap();
            assert_eq!(
                (current.generation, current.active_turn),
                (manager.generation, manager.active_turn.clone())
            );
        }
        // The old mixed-time read would return zero and retire the incident.
        assert!(capture.await.unwrap().is_none());
        let (stable, _) = capture_child_activity(&store, &manager, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stable.unfinished, 1);
    }

    #[tokio::test]
    async fn overlapping_new_child_invalidates_the_whole_capture() {
        assert_overlap_invalidates_capture(false).await;
    }

    #[tokio::test]
    async fn existing_child_new_turn_invalidates_without_registration_change() {
        assert_overlap_invalidates_capture(true).await;
    }

    #[tokio::test]
    async fn stale_empty_capture_cannot_retire_or_rearm_an_existing_incident() {
        let (store, owner, children) = store_with_children(1);
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let (summary, revision) = capture_child_activity(&store, &owner, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        let mut captured = manager();
        captured.agent = owner.clone();
        captured.children = summary;
        captured.children_revision = revision;
        let key = IncidentKey {
            agent: owner.id.clone(),
            generation: owner.generation,
            kind: IncidentKind::ChildrenUnattended,
            turn_id: None,
        };
        let state = Arc::new(Mutex::new(AttentionState::default()));
        reserve_incident(&state, &key).unwrap().commit(
            key.clone(),
            ActiveIncident {
                incident_id: "already-delivered".to_owned(),
                baseline_progress_at: None,
                baseline_model_progress_at: None,
                incident_delivery: None,
                recovery_delivery: None,
            },
        );
        {
            let mut writer = store.lock().await;
            child_turn_fact(&mut writer, &owner, &children[0], "late", "turn.started");
            assert!(!child_capture_is_current(&writer, &captured).unwrap());
            retire_child_incident_if_current(&writer, &state, &key, &captured).unwrap();
            assert!(state.lock().unwrap().active.contains_key(&key));
            assert!(reserve_incident(&state, &key).is_none());
            child_turn_fact(&mut writer, &owner, &children[0], "late", "turn.completed");
        }
        let (summary, revision) = capture_child_activity(&store, &owner, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        captured.children = summary;
        captured.children_revision = revision;
        let writer = store.lock().await;
        retire_child_incident_if_current(&writer, &state, &key, &captured).unwrap();
        assert!(!state.lock().unwrap().active.contains_key(&key));
    }

    fn manager() -> CapturedAgent {
        let now = now_ms();
        let mut agent = Agent::new("a1", "project", "codex", "profile", now);
        agent.generation = 1;
        agent.state = AgentState::Busy;
        agent.active_turn = Some("parent-turn".to_owned());
        CapturedAgent {
            agent,
            source_states: Vec::new(),
            source_healthy: Some(true),
            unhealthy_source_seq: None,
            last_model_progress_at: Some(now),
            progress_basis_at: Some(now),
            progress_age_ms: Some(0),
            turn_command_id: None,
            tool_coverage: eliot_store::ActiveToolCoverage::Clear,
            pending_kinds: Vec::new(),
            pending_kinds_truncated: false,
            children: eliot_store::NativeChildrenAttention {
                unfinished: 1,
                ..Default::default()
            },
            children_revision: None,
            manager_progress_at: Some(now),
        }
    }

    #[test]
    fn decision_requires_unfinished_children_and_manager_or_child_suspicion() {
        let window = Duration::from_secs(900);
        let mut captured = manager();
        assert!(child_attention_activity(&captured, window).is_none());
        for state in [AgentState::Idle, AgentState::Stopped] {
            captured.agent.state = state;
            assert_eq!(
                child_attention_activity(&captured, window),
                Some("manager_inactive_with_unfinished_children")
            );
        }
        captured.agent.state = AgentState::Busy;
        captured.children.stalled = 1;
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("child_progress_needs_manager")
        );
        captured.children.stalled = 0;
        captured.children.lost = 1;
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("child_progress_needs_manager")
        );
        captured.agent.state = AgentState::Waiting;
        assert!(child_attention_activity(&captured, window).is_none());
        captured.agent.state = AgentState::Busy;
        captured.children.lost = 0;
        captured.manager_progress_at = Some(now_ms() - 900_001);
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("manager_inactive_with_unfinished_children")
        );
        captured.children.unfinished = 0;
        captured.agent.state = AgentState::Idle;
        assert!(child_attention_activity(&captured, window).is_none());
    }

    #[test]
    fn mail_wake_and_parent_turn_changes_do_not_rearm_child_cohort() {
        let window = Duration::from_secs(900);
        let mut captured = manager();
        captured.agent.state = AgentState::Idle;
        let (key, _) = desired_incident(&captured, window).unwrap();
        let state = Arc::new(Mutex::new(AttentionState::default()));
        let incident = ActiveIncident {
            incident_id: "frozen-child-incident".to_owned(),
            baseline_progress_at: captured.progress_basis_at,
            baseline_model_progress_at: captured.last_model_progress_at,
            incident_delivery: None,
            recovery_delivery: None,
        };
        reserve_incident(&state, &key)
            .unwrap()
            .commit(key.clone(), incident.clone());
        captured.agent.active_turn = Some("mail-wake-turn".to_owned());
        captured.manager_progress_at = Some(now_ms());
        captured.children.unfinished = 100;
        let (same_key, _) = desired_incident(&captured, window).unwrap();
        assert_eq!(same_key, key);
        assert!(reserve_incident(&state, &same_key).is_none());
        assert!(!incident_condition_cleared(&same_key, &incident, &captured));
    }
}

#[cfg(test)]
mod loss_identity_tests {
    use super::*;

    #[test]
    fn loss_incident_id_is_deterministic_per_loss_event() {
        let first = loss_incident_id("a1", 2, 99);
        assert_eq!(first, loss_incident_id("a1", 2, 99));
        assert_ne!(first, loss_incident_id("a1", 2, 100));
        assert_ne!(first, loss_incident_id("a1", 3, 99));
        assert_ne!(first, loss_incident_id("a2", 2, 99));
    }

    #[test]
    fn loss_mutation_ids_are_deterministic_per_incident_phase_and_purpose() {
        let loss_ts = 1_700_000_000_000_i64;
        let emit = attention_ulid("attention-abc", "incident", "emit", loss_ts);
        assert_eq!(
            emit,
            attention_ulid("attention-abc", "incident", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "incident", "affected-mail", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "recovery", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abd", "incident", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "incident", "emit", loss_ts + 1)
        );
        // Deterministic ids stay valid ULIDs carrying the loss timestamp, so
        // admission can check their shape without a retry-window rebuke.
        let parsed = ulid::Ulid::from_string(&emit).expect("deterministic ULID");
        assert_eq!(parsed.timestamp_ms(), loss_ts as u64);
    }
}
