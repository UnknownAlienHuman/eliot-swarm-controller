//! Automatic, advisory Agent attention reports. Native snapshots are
//! generation-bound, read-only, and always run after releasing the Store lock.

mod delivery;
mod incidents;
mod observe;
mod sweep;

#[cfg(test)]
#[path = "attention/tests.rs"]
mod tests;

use super::{Router, StoreHandle};
use crate::common::now_ms;
use eliot_core::api::SourceState;
use eliot_core::{Agent, AgentState, Result};
use incidents::{
    activity_context, child_attention_activity, deliver_active_incident, desired_incident,
    freeze_loss_report, freeze_report, incident_id, loss_incident_id, loss_report_already_complete,
    persist_loss_report, report_cleared_incidents,
};
use observe::{
    build_report, capture_agent, child_capture_is_current, probe_native_status,
    process_observation, revalidate_probe,
};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use sweep::{
    launch_check, launch_sweep, queue_startup_losses, request_full_reconcile, reserve_incident,
};
use tokio::{
    sync::{broadcast, watch},
    task::JoinSet,
    time::{MissedTickBehavior, interval},
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

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
    /// Overflowed `AgentLost` triggers the bounded check map could not hold.
    /// Drained through `AgentLost` handling on the next reconcile (never as
    /// a generic sweep), so the deterministic loss identity survives a full
    /// pending map. Keys stay unique; a repeat loss refreshes the sequence.
    pending_losses: std::collections::VecDeque<(AgentKey, i64)>,
    /// A loss was dropped because both the check map and the loss queue were
    /// saturated. The next reconcile re-pages retained loss evidence from
    /// the Store instead of synthesizing an identity.
    losses_overflow: bool,
    /// Resume cursor for retained loss re-paging: the last fully-queued
    /// `(updated_at, agent_num)` of `startup_lost_agents` order, or `None`
    /// to start at the newest. An interrupted pass resumes at the first
    /// unqueued row instead of restarting at the newest, so the oldest loss
    /// cannot starve behind saturation while newer rows refill freed slots.
    loss_page_cursor: Option<(i64, i64)>,
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
    // A lost Agent always reports under its deterministic loss identity, no
    // matter which trigger found it: a sweep that minted a random incident
    // id would fork the identity, and a later restart re-report would mint
    // the loss id again. Non-loss triggers re-page the retained `agent.lost`
    // sequence; rows whose evidence was pruned are skipped, never
    // synthesized. A lost manager with unfinished children still takes the
    // generic child-cohort path below, matching the `AgentLost` trigger.
    if captured.agent.state == AgentState::Lost
        && child_attention_activity(&captured, suspicion_window).is_none()
    {
        let event_seq = match trigger {
            Trigger::AgentLost { event_seq } => Some(event_seq),
            Trigger::Sweep | Trigger::SourceEvent { .. } => {
                let store = router.inner.store.lock().await;
                store.latest_lost_event_seq(&captured.agent.id, captured.agent.generation)?
            }
        };
        let Some(event_seq) = event_seq else {
            return Ok(());
        };
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
