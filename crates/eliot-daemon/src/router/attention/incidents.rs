//! Incident diagnosis, recovery, and frozen report identity.

use super::{
    ActiveIncident, AgentKey, AttentionReport, AttentionState, CapturedAgent, FrozenReport,
    IncidentKey, IncidentKind, NativeStatus, ReportContext, ReportPhase, Router,
    delivery::{persist_and_notify, persist_loss_delivery},
    observe::{build_report, child_capture_is_current, process_observation, source_is_unhealthy},
};
use crate::common::now_ms;
use eliot_core::api::agent_state_name;
use eliot_core::{Agent, AgentState, Error, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) async fn retire_superseded_incidents(
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

/// Whether the retained loss incident needs no further work: its incident
/// event is already committed and every expected notice is already
/// delivered. Expected notices are the advisory subject copy, plus the
/// supervisor escalation for a parented subject or the owner notice
/// otherwise. A post-restart re-report that finds this state is a no-op.
pub(super) async fn loss_report_already_complete(
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
pub(super) async fn persist_loss_report(
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
        // Delivery is terminal for a loss (no recovery follows a lost
        // Agent): drop the entry so the delivered report releases its
        // reservation slot and retained losses behind it can still reserve.
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        if state.active.get(key).is_some_and(|incident| {
            incident.incident_id == incident_id
                && incident
                    .incident_delivery
                    .as_ref()
                    .is_some_and(|current| current.emit_command_id == delivery.emit_command_id)
        }) {
            state.active.remove(key);
        }
    }
    Ok(())
}

pub(super) async fn report_cleared_incidents(
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

pub(super) fn freeze_report(report: AttentionReport) -> FrozenReport {
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
pub(super) fn freeze_loss_report(report: AttentionReport, loss_event_ts_ms: i64) -> FrozenReport {
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

pub(super) async fn deliver_active_incident(
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

pub(super) fn desired_incident(
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

pub(super) fn child_attention_activity(
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

pub(super) fn activity_context(captured: &CapturedAgent) -> String {
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
            format!("state_{}", agent_state_name(captured.agent.state))
        }
    }
}

pub(super) fn incident_id(captured: &CapturedAgent, kind: IncidentKind, activity: &str) -> String {
    let identity = incident_identity(captured, kind, activity);
    digest_id(&[&identity, &ulid::Ulid::new().to_string()])
}

/// Deterministic incident id for loss evidence, anchored only on the exact
/// retained `agent.lost` event (Agent, generation, sequence). The same loss
/// re-reported after a restart maps to the same incident, anchoring the
/// deterministic mutation ids that make the re-report a Store-level no-op.
/// No fresh randomness or volatile capture detail is mixed in here.
pub(super) fn loss_incident_id(agent_id: &str, generation: i64, event_seq: i64) -> String {
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
    digest_id(&parts)
}

fn digest_id(parts: &[impl AsRef<str>]) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        let part = part.as_ref();
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
