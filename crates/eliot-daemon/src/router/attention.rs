//! Automatic Agent diagnosis and active-turn recovery. Native snapshots are
//! generation-bound, read-only, and always run after releasing the Store lock.

mod delivery;
mod incidents;
mod observe;
mod recovery;
mod steps;
mod sweep;

use super::{Router, StoreHandle};
use crate::common::now_ms;
use eliot_core::api::{
    AttentionThreshold, AttentionThresholdPair, AttentionThresholdsResponse, SourceState,
};
use eliot_core::{Agent, AgentState, Result};
use eliot_store::CodeProgressState;
use incidents::{
    activity_context, child_attention_activity, deliver_active_incident, desired_incident,
    freeze_loss_report, freeze_report, incident_id, loss_incident_id, loss_report_already_complete,
    persist_loss_report, report_cleared_incidents,
};
use observe::{
    build_report, capture_agent, child_capture_is_current, probe_native_status,
    process_observation, revalidate_probe,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use sweep::{
    launch_check, launch_sweep, queue_startup_losses, request_full_reconcile, reserve_incident,
};
use tokio::{
    sync::{broadcast, oneshot, watch},
    task::JoinSet,
    time::{MissedTickBehavior, interval},
};

const SWEEP_INTERVAL: Duration = Duration::from_secs(15);
const ATTENTION_THRESHOLD_FLOOR_SECS: i64 = eliot_core::api::MIN_ATTENTION_THRESHOLD_SECS;
const ATTENTION_THRESHOLD_CACHE_TTL: Duration = Duration::from_secs(30);
const ATTENTION_THRESHOLD_CACHE_MAX: usize = eliot_core::api::MAX_ATTENTION_THRESHOLD_PAIRS;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum IncidentKind {
    SourceUnhealthy,
    NoProgress,
    CodeProgress,
    ChildrenUnattended,
}

impl IncidentKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::SourceUnhealthy => "source_unhealthy",
            Self::NoProgress => "no_progress",
            Self::CodeProgress => "no_code_progress",
            Self::ChildrenUnattended => "children_unattended",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    config: crate::config::AttentionConfig,
    threshold_cache: HashMap<AttentionPairKey, CachedAttentionThresholds>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AttentionPairKey {
    profile: String,
    project: String,
}

#[derive(Clone, Copy)]
struct CachedAttentionThresholds {
    thresholds: PairAttentionThresholds,
    refreshed_at: Instant,
}

#[derive(Clone, Copy)]
struct PairAttentionThresholds {
    no_progress: AttentionThreshold,
    no_code_progress: AttentionThreshold,
}

#[derive(Clone)]
struct ActiveIncident {
    incident_id: String,
    baseline_progress_at: Option<i64>,
    baseline_model_progress_at: Option<i64>,
    incident_delivery: Option<FrozenReport>,
    recovery_delivery: Option<FrozenReport>,
    /// Deterministic loss-evidence report. Delivery is terminal (no recovery
    /// follows a lost Agent), so a successful delivery drops the entry and
    /// frees its reservation slot instead of retaining it.
    is_loss: bool,
}

#[derive(Clone)]
struct FrozenReport {
    report: AttentionReport,
    emit_command_id: String,
    supervisor_mail_id: String,
    owner_notify_id: String,
}

struct ReportContext {
    phase: ReportPhase,
    incident_id: String,
    activity: String,
    observed_at: i64,
    suspicion_window: Option<Duration>,
    threshold_basis: Option<AttentionThreshold>,
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
    code_progress: Option<CodeProgressState>,
    /// The active turn's start: a turn's code progress is measured from
    /// it when the worktree has no later write or commit.
    turn_started_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    observed_quiet_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    normal_quiet_gap_p90_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold_configured_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold_baseline_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold_history_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    threshold_fallback: Option<bool>,
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
    /// `no_code_progress` only: when the turn last made code progress (the
    /// later of the last write or commit and the turn's start), and the
    /// turn's tool calls and messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    code_progress_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProcessFact {
    role: String,
    pid: u32,
    start_time: u64,
    state: String,
    exit_code: Option<i32>,
    updated_at: i64,
    age_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceFact {
    source: String,
    families: Vec<String>,
    state: String,
    health: String,
    last_store_seq: Option<i64>,
    gap_id: Option<String>,
    gap_seq: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
    SourceEvent { event_seq: i64, recovery_due: bool },
    AgentLost { event_seq: i64 },
}

fn attention_duration(threshold_secs: Option<i64>) -> Duration {
    Duration::from_secs(threshold_secs.unwrap_or(15 * 60) as u64)
}

fn attention_threshold(samples_ms: &[i64], configured_secs: Option<i64>) -> AttentionThreshold {
    let configured_secs = configured_secs
        .unwrap_or(ATTENTION_THRESHOLD_FLOOR_SECS)
        .max(1);
    let baseline_secs = configured_secs.max(ATTENTION_THRESHOLD_FLOOR_SECS);
    let mut samples = samples_ms
        .iter()
        .copied()
        .filter(|sample| *sample >= 0)
        .take(eliot_core::api::MAX_ATTENTION_HISTORY)
        .collect::<Vec<_>>();
    samples.sort_unstable();
    let history_count = samples.len();
    let fallback = history_count < eliot_core::api::MIN_ATTENTION_HISTORY;
    let normal_p90_secs = if fallback {
        None
    } else {
        let nearest_rank = (history_count * 90).div_ceil(100);
        let p90_ms = samples[nearest_rank - 1];
        Some(
            p90_ms
                .saturating_add(999)
                .checked_div(1_000)
                .unwrap_or(0)
                .min(i64::MAX / 1_000),
        )
    };
    let threshold_secs = baseline_secs
        .max(normal_p90_secs.unwrap_or_default())
        .min(i64::MAX / 1_000);
    AttentionThreshold {
        threshold_secs,
        configured_secs,
        baseline_secs,
        normal_p90_secs,
        history_count,
        fallback,
    }
}

fn pair_attention_thresholds(
    native_gaps_ms: &[i64],
    git_gaps_ms: &[i64],
    config: crate::config::AttentionConfig,
) -> PairAttentionThresholds {
    PairAttentionThresholds {
        no_progress: attention_threshold(native_gaps_ms, config.no_progress.threshold_secs),
        no_code_progress: attention_threshold(git_gaps_ms, config.no_code_progress.threshold_secs),
    }
}

async fn pair_thresholds_for_agent(
    router: &Router,
    state: &Arc<Mutex<AttentionState>>,
    agent: &Agent,
    config: crate::config::AttentionConfig,
) -> Result<PairAttentionThresholds> {
    let key = AttentionPairKey {
        profile: agent.profile.clone(),
        project: agent.project.clone(),
    };
    {
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(cached) = state.threshold_cache.get(&key)
            && cached.refreshed_at.elapsed() < ATTENTION_THRESHOLD_CACHE_TTL
        {
            return Ok(cached.thresholds);
        }
    }

    let (native_gaps_ms, git_gaps_ms) = {
        let store = router.inner.store.lock().await;
        let include_git = store.attention_pair_code_eligible(&agent.profile, &agent.project)?;
        store.attention_quiet_gaps(&agent.profile, &agent.project, include_git)?
    };
    let thresholds = pair_attention_thresholds(&native_gaps_ms, &git_gaps_ms, config);
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if !state.threshold_cache.contains_key(&key)
        && state.threshold_cache.len() >= ATTENTION_THRESHOLD_CACHE_MAX
        && let Some(oldest) = state
            .threshold_cache
            .iter()
            .min_by_key(|(_, cached)| cached.refreshed_at)
            .map(|(key, _)| key.clone())
    {
        state.threshold_cache.remove(&oldest);
    }
    state.threshold_cache.insert(
        key,
        CachedAttentionThresholds {
            thresholds,
            refreshed_at: Instant::now(),
        },
    );
    Ok(thresholds)
}

impl Router {
    /// Current profile and project thresholds for doctor.
    pub(super) async fn attention_thresholds(&self) -> Result<AttentionThresholdsResponse> {
        let config = self.inner.attention;
        let mut profile_pairs = {
            let store = self.inner.store.lock().await;
            store.attention_profile_pairs(eliot_core::api::MAX_ATTENTION_THRESHOLD_PAIRS + 1)?
        };
        let truncated = profile_pairs.len() > eliot_core::api::MAX_ATTENTION_THRESHOLD_PAIRS;
        profile_pairs.truncate(eliot_core::api::MAX_ATTENTION_THRESHOLD_PAIRS);
        let mut pairs = Vec::with_capacity(profile_pairs.len());
        for (profile, project, code_eligible) in profile_pairs {
            let (native_gaps_ms, git_gaps_ms) = {
                let store = self.inner.store.lock().await;
                store.attention_quiet_gaps(&profile, &project, code_eligible)?
            };
            let thresholds = pair_attention_thresholds(&native_gaps_ms, &git_gaps_ms, config);
            pairs.push(AttentionThresholdPair {
                profile,
                project,
                no_progress: thresholds.no_progress,
                no_code_progress: thresholds.no_code_progress,
            });
        }
        let response = AttentionThresholdsResponse { pairs, truncated };
        response.validate()?;
        Ok(response)
    }
}

pub(crate) async fn run_attention(
    router: Router,
    attention_config: crate::config::AttentionConfig,
    mut shutdown: watch::Receiver<bool>,
    reporter: crate::daemon::workers::WorkerReporter,
) -> Result<()> {
    let mut events = router.inner.bus.subscribe();
    let mut ticks = interval(SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut liveness_ticks = interval(Duration::from_secs(180));
    liveness_ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut previous_stuck = eliot_store::StuckStates::new();
    let mut liveness_result: Option<oneshot::Receiver<Result<eliot_store::StuckStates>>> = None;
    let suspicion_window = attention_duration(attention_config.no_progress.threshold_secs);
    let state = Arc::new(Mutex::new(AttentionState {
        config: attention_config,
        ..AttentionState::default()
    }));
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
    reporter.running("attention");

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
                                recovery_due: matches!(kind, "source.gap" | "source.health" | "source.closed" | "turn.started"),
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
            _ = liveness_ticks.tick(), if liveness_result.is_none() => {
                let (sender, receiver) = oneshot::channel();
                let router = router.clone();
                tasks.spawn(async move {
                    let _ = sender.send(router.sweep_liveness().await);
                });
                liveness_result = Some(receiver);
            }
            result = async { liveness_result.as_mut().expect("pending sweep").await },
                if liveness_result.is_some() =>
            {
                liveness_result = None;
                let result = result.unwrap_or_else(|_| Err(eliot_core::Error::new(
                    "internal", "owner inbox sweep ended without a sample",
                )));
                match result {
                    Ok(current) => router.publish_liveness(current, &mut previous_stuck),
                    Err(error) => {
                        previous_stuck.clear();
                    crate::logging::record(
                        &router.secret_policy_snapshot(),
                        "daemon.liveness_sweep_failed",
                        &[],
                        Some(&error),
                    );
                    }
                }
            }
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
                | "turn.unknown"
                | "request.opened"
                | "request.resolved"
                | "request.closed"
                | "tool.started"
                | "tool.completed"
                | "session.ended"
                | "session.growth"
                | "capacity.released"
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
    let config = state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .config;
    let children_window = attention_duration(config.children_unattended.threshold_secs);
    let trigger_event_seq = match trigger {
        Trigger::SourceEvent { event_seq, .. } => Some(event_seq),
        Trigger::Sweep | Trigger::AgentLost { .. } => None,
    };

    let Some(captured) = capture_agent(router, agent_id, generation, children_window).await? else {
        return Ok(());
    };
    if captured.agent.observe_only {
        return Ok(());
    }
    let thresholds = pair_thresholds_for_agent(router, &state, &captured.agent, config).await?;
    let no_progress_window = Duration::from_secs(thresholds.no_progress.threshold_secs as u64);
    let no_code_progress_window =
        Duration::from_secs(thresholds.no_code_progress.threshold_secs as u64);
    if captured.agent.state == AgentState::Idle
        && config.session_growth.enabled
        && let Err(error) = router.react_to_session_growth(&captured.agent).await
    {
        crate::logging::record(
            &router.secret_policy_snapshot(),
            "daemon.attention_session_growth_failed",
            &[("agent", agent_id)],
            Some(&error),
        );
    }
    if matches!(
        trigger,
        Trigger::Sweep
            | Trigger::SourceEvent {
                recovery_due: true,
                ..
            }
    ) && recovery::recover_active_turn(router, &captured, suspicion_window).await?
    {
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
        && child_attention_activity(&captured, children_window).is_none()
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
            children_window,
        )
        .await;
    }
    let desired = desired_incident(
        &captured,
        &config,
        no_progress_window,
        no_code_progress_window,
    );
    report_cleared_incidents(router, &state, &captured, thresholds).await?;

    let Some((key, _activity)) = desired else {
        return Ok(());
    };
    if key.kind == IncidentKind::SourceUnhealthy && owned_exit_source(&captured) {
        return Ok(());
    }
    if deliver_active_incident(router, &state, &key).await? {
        if matches!(
            key.kind,
            IncidentKind::NoProgress | IncidentKind::CodeProgress
        ) {
            steps::advance_time_ladder(router, &state, &key, &captured, config, no_progress_window)
                .await?;
        }
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
    let Some(after_probe) = revalidate_probe(router, &captured, key.kind, children_window).await?
    else {
        return Ok(());
    };
    // A source probe may race the transition to Lost. The retained loss
    // trigger owns that report, including when the source check began earlier.
    if after_probe.agent.state == AgentState::Lost && key.kind != IncidentKind::ChildrenUnattended {
        return Ok(());
    }
    let Some((still_key, still_activity)) = desired_incident(
        &after_probe,
        &config,
        no_progress_window,
        no_code_progress_window,
    ) else {
        return Ok(());
    };
    if still_key != key
        || (key.kind == IncidentKind::SourceUnhealthy && owned_exit_source(&after_probe))
    {
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
            suspicion_window: match key.kind {
                IncidentKind::NoProgress => Some(no_progress_window),
                IncidentKind::CodeProgress => Some(no_code_progress_window),
                IncidentKind::ChildrenUnattended => Some(children_window),
                IncidentKind::SourceUnhealthy => None,
            },
            threshold_basis: match key.kind {
                IncidentKind::NoProgress => Some(thresholds.no_progress),
                IncidentKind::CodeProgress => Some(thresholds.no_code_progress),
                IncidentKind::ChildrenUnattended | IncidentKind::SourceUnhealthy => None,
            },
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
            is_loss: false,
        },
    );
    drop(child_guard);
    let _ = deliver_active_incident(router, &state, &key).await?;
    if matches!(
        key.kind,
        IncidentKind::NoProgress | IncidentKind::CodeProgress
    ) {
        steps::advance_time_ladder(
            router,
            &state,
            &key,
            &after_probe,
            config,
            no_progress_window,
        )
        .await?;
    }
    Ok(())
}

fn owned_exit_source(captured: &CapturedAgent) -> bool {
    captured.source_states.iter().any(|source| {
        source
            .gap_id
            .as_deref()
            .is_some_and(|id| id.starts_with("host-root-exit:"))
    })
}

async fn inspect_lost_agent(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    agent_id: &str,
    generation: i64,
    event_seq: i64,
    children_window: Duration,
) -> Result<()> {
    let Some(captured) = capture_agent(router, agent_id, generation, children_window).await? else {
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
        router
            .resume_after_process_loss(agent_id, generation, event_seq, &incident_id)
            .await?;
        return Ok(());
    }
    let key = IncidentKey {
        agent: agent_id.to_owned(),
        generation,
        kind: IncidentKind::SourceUnhealthy,
        turn_id: None,
    };
    if let Some(existing_id) = {
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state
            .active
            .get(&key)
            .map(|incident| incident.incident_id.clone())
    } {
        if existing_id == incident_id {
            // Already tracking this exact loss: settle any remaining
            // delivery through the loss path, then return. A durably
            // complete re-report no-ops above; a restart finds the same
            // deterministic identity instead of a second one.
            persist_loss_report(router, &state, &key).await?;
            return Ok(());
        }
        // A generic incident occupies the loss key: settle its pending
        // delivery, then drop it so the deterministic loss report is still
        // persisted. The Agent is lost (terminal); no recovery follows a
        // generic source incident for it. Returning here would swallow the
        // loss identity, and a restart would then report it under a second
        // identity.
        deliver_active_incident(router, &state, &key).await?;
        {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .active
                .get(&key)
                .is_some_and(|incident| incident.incident_id == existing_id)
            {
                state.active.remove(&key);
            }
        }
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
        children_window,
    )
    .await?
    else {
        return Ok(());
    };
    let loss_turn_id = {
        let store = router.inner.store.lock().await;
        store.attention_loss_turn_id(agent_id, generation, event_seq)?
    };
    let mut report = build_report(
        &after_probe,
        IncidentKind::SourceUnhealthy,
        ReportContext {
            phase: ReportPhase::Incident,
            incident_id: incident_id.clone(),
            activity: activity_context(&after_probe),
            observed_at: now_ms(),
            suspicion_window: None,
            threshold_basis: None,
            trigger_event_seq: Some(event_seq),
            native_status,
            process_observation,
        },
    );
    if let Some(turn_id) = loss_turn_id {
        report.turn_id = Some(turn_id);
    }
    report.reason = "agent_lost".to_owned();
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
            is_loss: true,
        },
    );
    persist_loss_report(router, &state, &key).await
}
