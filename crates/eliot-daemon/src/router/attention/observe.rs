//! Bound Store capture, native/process readbacks, and report shaping.

use super::{
    AttentionReport, CapturedAgent, IncidentKind, NativeStatus, ProcessFact, ProcessObservation,
    ReportContext, Router, SourceFact,
};
use crate::common::now_ms;
use eliot_core::api::{
    ObservationView, SourceState, agent_state_name, process_state_name, stats_coverage_name,
};
use eliot_core::{Agent, AgentState, Error, Result};
use serde_json::Value;
use std::time::Duration;

const MAX_SOURCE_FACTS: usize = 32;
const MAX_PENDING_KINDS: usize = 16;
const MAX_PROCESS_FACTS: usize = 16;

pub(super) fn child_capture_is_current(
    store: &eliot_store::Store,
    captured: &CapturedAgent,
) -> Result<bool> {
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

pub(super) async fn probe_native_status(router: &Router, captured: &CapturedAgent) -> NativeStatus {
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

pub(super) async fn revalidate_probe(
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

pub(super) async fn capture_agent(
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
        code_progress,
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
        let code_progress = store.code_progress_state(
            &agent.id,
            generation,
            agent.workspace.as_deref(),
            agent.active_turn.as_deref(),
        )?;
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
            code_progress,
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
        code_progress: Some(code_progress),
        turn_started_at,
    }))
}

pub(super) async fn capture_child_activity(
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

pub(super) fn source_is_unhealthy(agent_state: AgentState, states: &[SourceState]) -> bool {
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

pub(super) fn build_report(
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
        threshold_basis,
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
    // Turn counts exist only while a turn is open (a recovery after the
    // turn ended carries none, never a zero).
    let (code_progress_at, tool_calls, messages) = match (kind, &captured.code_progress) {
        (IncidentKind::CodeProgress, Some(code)) if captured.agent.active_turn.is_some() => (
            super::incidents::code_progress_basis(captured),
            Some(code.tool_calls),
            Some(code.messages),
        ),
        _ => (None, None, None),
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
    let observed_quiet_ms = match kind {
        IncidentKind::NoProgress => progress_age_ms,
        IncidentKind::CodeProgress => code_progress_at
            .and_then(|at| observed_at.checked_sub(at))
            .filter(|age| *age >= 0),
        _ => None,
    };
    AttentionReport {
        schema: 2,
        incident_id,
        phase,
        reason: kind.as_str().to_owned(),
        agent_id: captured.agent.id.clone(),
        generation: captured.agent.generation,
        agent_state: agent_state_name(captured.agent.state).to_owned(),
        turn_id: captured.agent.active_turn.clone(),
        parent_agent: captured.agent.parent.clone(),
        observed_at,
        suspicion_window_ms: suspicion_window
            .and_then(|window| i64::try_from(window.as_millis()).ok()),
        observed_quiet_ms,
        normal_quiet_gap_p90_ms: threshold_basis.and_then(|basis| {
            basis
                .normal_p90_secs
                .map(|seconds| seconds.saturating_mul(1_000))
        }),
        threshold_configured_ms: threshold_basis
            .map(|basis| basis.configured_secs.saturating_mul(1_000)),
        threshold_baseline_ms: threshold_basis
            .map(|basis| basis.baseline_secs.saturating_mul(1_000)),
        threshold_history_count: threshold_basis.map(|basis| basis.history_count),
        threshold_fallback: threshold_basis.map(|basis| basis.fallback),
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
        code_progress_at,
        tool_calls,
        messages,
    }
}

pub(super) async fn process_observation(router: &Router, agent: &Agent) -> ProcessObservation {
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

impl NativeStatus {
    pub(super) fn not_requested() -> Self {
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
