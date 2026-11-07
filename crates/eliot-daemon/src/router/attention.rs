//! Automatic, advisory Agent attention reports. Native snapshots are
//! generation-bound, read-only, and always run after releasing the Store lock.

use super::{Principal, Router};
use crate::common::now_ms;
use eliot_core::api::{
    EmitDefinitionArgs, MailMode, MailSendRequest, ObservationView, SourceState,
};
use eliot_core::{Agent, AgentState, CommandState, Error, Event, ProcessState, Result};
use eliot_plugin::sync::LockExt;
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
const REPORT_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_SOURCE_FACTS: usize = 32;
const MAX_PENDING_KINDS: usize = 16;
const MAX_PROCESS_FACTS: usize = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum IncidentKind {
    SourceUnhealthy,
    NoProgress,
}

impl IncidentKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceUnhealthy => "source_unhealthy",
            Self::NoProgress => "no_progress",
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
    reserved_incidents: usize,
}

#[derive(Clone)]
struct ActiveIncident {
    incident_id: String,
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
    SessionLost { event_seq: i64 },
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

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            received = events.recv() => match received {
                Ok(event) => {
                    if event.kind == "session.lost" {
                        if let (Some(agent), Some(generation)) =
                            (event.agent.as_deref(), event.generation)
                        {
                            launch_check(
                                &router,
                                Arc::clone(&state),
                                &mut tasks,
                                agent,
                                generation,
                                suspicion_window,
                                Trigger::SessionLost {
                                    event_seq: event.seq,
                                },
                            );
                        }
                    } else if attention_transition_event(&event.kind) {
                        if let (Some(agent), Some(generation)) =
                            (event.agent.as_deref(), event.generation)
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
                            );
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
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
    kind == "source.health"
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

    if running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let router = router.clone();
    tasks.spawn(async move {
        let _reset = SweepReset(running);
        if let Err(error) = scan_attention_agents(&router, state, suspicion_window).await {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.attention_scan_failed",
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
    if !reserve_check(&state, &key) {
        return;
    }
    let agent_id = agent.to_owned();
    let router = router.clone();
    tasks.spawn(async move {
        let guard = CheckGuard { state, key };
        if let Err(error) = inspect_agent(
            &router,
            Arc::clone(&guard.state),
            &agent_id,
            generation,
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
    });
}

fn reserve_check(state: &Arc<Mutex<AttentionState>>, key: &AgentKey) -> bool {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if state.in_flight.len() >= MAX_CHECKS_IN_FLIGHT || !state.in_flight.insert(key.clone()) {
        return false;
    }
    true
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
                continue;
            }
            let router = router.clone();
            let state = Arc::clone(&state);
            checks.spawn(async move {
                let _guard = CheckGuard {
                    state: Arc::clone(&state),
                    key,
                };
                if let Err(error) = inspect_agent(
                    &router,
                    state,
                    &agent.id,
                    agent.generation,
                    suspicion_window,
                    Trigger::Sweep,
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
            });
        }
        if after.is_none() || checks.len() >= MAX_SWEEP_CHECKS {
            tokio::task::yield_now().await;
        }
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
    if let Trigger::SessionLost { event_seq } = trigger {
        return inspect_lost_agent(router, state, agent_id, generation, event_seq).await;
    }

    let trigger_event_seq = match trigger {
        Trigger::SourceEvent { event_seq } => Some(event_seq),
        Trigger::Sweep | Trigger::SessionLost { .. } => None,
    };

    let Some(captured) = capture_agent(router, agent_id, generation).await? else {
        return Ok(());
    };
    if captured.agent.observe_only {
        return Ok(());
    }
    let desired = desired_incident(&captured, suspicion_window);
    report_cleared_incidents(
        router,
        &state,
        &captured,
        desired.as_ref(),
        suspicion_window,
    )
    .await?;

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
    let native_status = probe_native_status(router, &captured).await;
    let Some(after_probe) = revalidate_probe(router, &captured, key.kind).await? else {
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
        ReportPhase::Incident,
        incident_id(&after_probe, key.kind, still_activity.as_str()),
        still_activity.as_str(),
        now_ms(),
        (key.kind == IncidentKind::NoProgress).then_some(suspicion_window),
        trigger_event_seq,
        native_status,
        process_observation,
    );
    let frozen = freeze_report(report);
    let incident_id = frozen.report.incident_id.clone();
    reservation.commit(
        key.clone(),
        ActiveIncident {
            incident_id,
            incident_delivery: Some(frozen),
            recovery_delivery: None,
        },
    );
    let _ = deliver_active_incident(router, &state, &key).await?;
    Ok(())
}

async fn inspect_lost_agent(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    agent_id: &str,
    generation: i64,
    event_seq: i64,
) -> Result<()> {
    let Some(captured) = capture_agent(router, agent_id, generation).await? else {
        return Ok(());
    };
    if captured.agent.observe_only {
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
    let Some(after_probe) =
        revalidate_probe(router, &captured, IncidentKind::SourceUnhealthy).await?
    else {
        return Ok(());
    };
    let incident_id = if after_probe.unhealthy_source_seq.is_some() {
        incident_id(
            &after_probe,
            IncidentKind::SourceUnhealthy,
            "source_unhealthy",
        )
    } else {
        digest_id(&[
            "session_lost",
            agent_id,
            &generation.to_string(),
            &event_seq.to_string(),
        ])
    };
    let report = build_report(
        &after_probe,
        IncidentKind::SourceUnhealthy,
        ReportPhase::Incident,
        incident_id.clone(),
        activity_context(&after_probe).as_str(),
        now_ms(),
        None,
        Some(event_seq),
        native_status,
        process_observation,
    );
    let frozen = freeze_report(report);
    let incident_id = frozen.report.incident_id.clone();
    reservation.commit(
        key.clone(),
        ActiveIncident {
            incident_id,
            incident_delivery: Some(frozen),
            recovery_delivery: None,
        },
    );
    let _ = deliver_active_incident(router, &state, &key).await?;
    Ok(())
}

async fn report_cleared_incidents(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
    desired: Option<&(IncidentKey, String)>,
    suspicion_window: Duration,
) -> Result<()> {
    let mut candidates = recovery_candidates(state, captured, desired);
    if candidates.is_empty() {
        return Ok(());
    }

    for (key, incident) in &candidates {
        if incident.incident_delivery.is_some() {
            let _ = deliver_active_incident(router, state, key).await?;
        }
    }

    candidates = recovery_candidates(state, captured, desired);
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
                    ReportPhase::Recovery,
                    incident.incident_id.clone(),
                    "condition_cleared",
                    now_ms(),
                    (key.kind == IncidentKind::NoProgress).then_some(suspicion_window),
                    None,
                    NativeStatus::not_requested(),
                    process_observation.clone(),
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
        persist_and_notify(router, &delivery).await?;
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

fn recovery_candidates(
    state: &Arc<Mutex<AttentionState>>,
    captured: &CapturedAgent,
    desired: Option<&(IncidentKey, String)>,
) -> Vec<(IncidentKey, ActiveIncident)> {
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    state
        .active
        .iter()
        .filter(|(key, incident)| {
            key.agent == captured.agent.id
                && key.generation == captured.agent.generation
                && (incident.recovery_delivery.is_some()
                    || desired.is_none_or(|(desired_key, _)| desired_key != *key))
        })
        .map(|(key, incident)| (key.clone(), incident.clone()))
        .collect()
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
        .agent
        .active_turn
        .as_deref()
        .zip(captured.turn_command_id.as_deref());
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
) -> Result<Option<CapturedAgent>> {
    let Some(current) =
        capture_agent(router, &captured.agent.id, captured.agent.generation).await?
    else {
        return Ok(None);
    };
    if current.agent.generation != captured.agent.generation
        || current.agent.active_turn != captured.agent.active_turn
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
        persist_and_notify(router, &delivery).await?;
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
) -> Result<Option<CapturedAgent>> {
    let (
        agent,
        source_states,
        source_healthy,
        unhealthy_source_seq,
        last_model_progress_at,
        turn,
        turn_command_id,
        tool_coverage,
    ) = {
        let store = router.inner.store.lock().await;
        let Some(agent) = store.get_agent(agent_id)? else {
            return Ok(None);
        };
        if agent.generation != generation {
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
        let turn_command_id = match agent.active_turn.as_deref() {
            Some(turn_id) => store.turn_command_id(agent_id, generation, turn_id)?,
            None => None,
        };
        let source_healthy = native_sources_healthy(&source_states);
        let unhealthy_source_seq = if source_is_unhealthy(&source_states) {
            source_states
                .iter()
                .filter(|source| source_is_unhealthy(std::slice::from_ref(source)))
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
    }))
}

fn desired_incident(
    captured: &CapturedAgent,
    suspicion_window: Duration,
) -> Option<(IncidentKey, String)> {
    if source_is_unhealthy(&captured.source_states) {
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
        || matches!(
            captured.tool_coverage,
            eliot_store::ActiveToolCoverage::Active { .. }
        )
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

fn source_is_unhealthy(states: &[SourceState]) -> bool {
    states.iter().any(|source| {
        !matches!(
            source.source.as_str(),
            "daemon" | "os" | "git" | "workspace_process"
        ) && source.families.iter().any(|family| {
            matches!(
                family.as_str(),
                "session" | "turn" | "message" | "tool" | "request" | "child" | "goal" | "usage"
            )
        }) && (source.state == "closed"
            || (source.state == "open"
                && matches!(source.health.as_str(), "slow" | "stale" | "error")))
    })
}

fn incident_id(captured: &CapturedAgent, kind: IncidentKind, activity: &str) -> String {
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
            .filter(|source| source_is_unhealthy(std::slice::from_ref(source)))
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
    let identity = digest_id(&parts.iter().map(String::as_str).collect::<Vec<_>>());
    digest_id(&[&identity, &ulid::Ulid::new().to_string()])
}

fn digest_id(parts: &[&str]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    format!("attention-{:x}", digest.finalize())
}

fn build_report(
    captured: &CapturedAgent,
    kind: IncidentKind,
    phase: ReportPhase,
    incident_id: String,
    activity: &str,
    observed_at: i64,
    suspicion_window: Option<Duration>,
    trigger_event_seq: Option<i64>,
    native_status: NativeStatus,
    process_observation: ProcessObservation,
) -> AttentionReport {
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
        last_model_progress_at: captured.last_model_progress_at,
        progress_basis_at: captured.progress_basis_at,
        progress_age_ms: captured.progress_age_ms,
        source_health: captured.source_healthy,
        activity: activity.to_owned(),
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
        router
            .submit(
                &json!({"id": delivery.emit_command_id.clone(), "verb": "emit", "args": args}),
                &Principal::Owner,
            )
            .await?;
    }
    wait_for_applied(router, &delivery.emit_command_id).await?;
    send_notice(router, delivery).await?;
    Ok(())
}

async fn wait_for_applied(router: &Router, command_id: &str) -> Result<()> {
    let mut progress = router.progress_receiver();
    timeout(REPORT_SETTLEMENT_TIMEOUT, async {
        loop {
            let command = {
                let store = router.inner.store.lock().await;
                store.get_command(command_id)?
            };
            match command.map(|command| command.state) {
                Some(CommandState::Applied) => return Ok(()),
                Some(CommandState::Rejected | CommandState::Failed | CommandState::Cancelled) => {
                    return Err(Error::new("unknown", "attention event did not commit"));
                }
                Some(CommandState::Unknown) => {
                    return Err(Error::new(
                        "unknown",
                        "attention event commit outcome is unknown",
                    ));
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

async fn send_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let phase_label = match report.phase {
        ReportPhase::Incident => "report",
        ReportPhase::Recovery => "recovery",
    };
    let diagnosis = diagnosis_summary(report);
    let text = format!(
        "Eliot attention {phase_label} for Agent {} generation {}. Incident {}. {diagnosis} See the structured x.user.agent.attention event for details.",
        report.agent_id, report.generation, report.incident_id,
    );
    let thread = Some(format!(
        "attention-{}",
        &report.incident_id[..20.min(report.incident_id.len())]
    ));
    let affected = MailSendRequest {
        id: delivery.affected_mail_id.clone(),
        to: vec![report.agent_id.clone()],
        mode: MailMode::Queue,
        text: text.clone(),
        thread: thread.clone(),
        reply_to_delivery: None,
    };
    let affected = serde_json::to_value(affected).map_err(|_| {
        Error::new(
            "internal",
            "affected Agent attention mail could not be encoded",
        )
    })?;
    router
        .mail_send_attention_queue(&affected, &Principal::Owner)
        .await?;

    if let Some(parent) = report
        .parent_agent
        .as_ref()
        .filter(|parent| *parent != &report.agent_id)
    {
        let supervisor = MailSendRequest {
            id: delivery.supervisor_mail_id.clone(),
            to: vec![parent.clone()],
            mode: MailMode::Queue,
            text,
            thread,
            reply_to_delivery: None,
        };
        let supervisor = serde_json::to_value(supervisor).map_err(|_| {
            Error::new(
                "internal",
                "supervising Agent attention mail could not be encoded",
            )
        })?;
        router.mail_send(&supervisor, &Principal::Owner).await?;
    } else {
        let title = match report.phase {
            ReportPhase::Incident => "Agent attention",
            ReportPhase::Recovery => "Agent attention recovered",
        };
        let message = format!(
            "Agent {} generation {}: {} (incident {}). {diagnosis} See the structured x.user.agent.attention event for details.",
            report.agent_id, report.generation, report.reason, report.incident_id
        );
        router
            .submit(
                &json!({
                    "id": delivery.owner_notify_id.clone(),
                    "verb": "notify",
                    "args": {"title": title, "message": message}
                }),
                &Principal::Owner,
            )
            .await?;
    }
    Ok(())
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
        "Diagnosis: reason={}, activity={}, source_health={}, progress_age_ms={}, native_probe_requested={}, native_state={}, native_error={}, native_basis={}, native_freshness={}, native_binding_matches={}, process_coverage={}, process_sample_age_ms={}, process_complete={}, process_exited={}",
        report.reason,
        report.activity,
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
