use super::super::Principal;
use super::{AttentionReport, CapturedAgent, IncidentKey, IncidentKind, Router};
use crate::common::now_ms;
use crate::config::AttentionConfig;
use eliot_core::{CommandState, Error, Result, api::CommandSubmitRequest};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::timeout;

const STEP_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) async fn advance_time_ladder(
    router: &Router,
    state: &Arc<Mutex<super::AttentionState>>,
    key: &IncidentKey,
    captured: &CapturedAgent,
    config: AttentionConfig,
) -> Result<()> {
    let Some(incident) = ({
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state.active.get(key).cloned()
    }) else {
        return Ok(());
    };
    if incident.is_loss
        || !matches!(
            key.kind,
            IncidentKind::NoProgress | IncidentKind::CodeProgress
        )
        || key.turn_id.as_deref() != captured.agent.active_turn.as_deref()
        || captured.agent.state != eliot_core::AgentState::Busy
    {
        return Ok(());
    }
    if (key.kind == IncidentKind::NoProgress && !config.no_progress.enabled)
        || (key.kind == IncidentKind::CodeProgress && !config.no_code_progress.enabled)
    {
        return Ok(());
    }

    let report_event = {
        let store = router.inner.store.lock().await;
        store.attention_incident_event(&incident.incident_id)?
    };
    let Some(report_event) = report_event else {
        return Ok(());
    };
    let report_value = report_event
        .data
        .get("data")
        .cloned()
        .unwrap_or_else(|| report_event.data.clone());
    let report = serde_json::from_value::<AttentionReport>(report_value)
        .map_err(|_| Error::new("source_integrity", "stored attention report is invalid"))?;
    if report.incident_id != incident.incident_id
        || report.agent_id != key.agent
        || report.generation != key.generation
        || report.turn_id != key.turn_id
    {
        return Ok(());
    }

    let Some(turn_id) = key.turn_id.as_deref() else {
        return Ok(());
    };
    let question = {
        let store = router.inner.store.lock().await;
        store.attention_turn_step(
            &key.agent,
            key.generation,
            turn_id,
            "question",
            Some(key.kind.as_str()),
        )?
    };
    let notice_after_question =
        key.kind == IncidentKind::CodeProgress || !config.no_progress.interrupt;
    let Some((question_seq, question_at)) = question else {
        // Choose the channel before reserving the step. Safe-point context
        // stays queued on this turn even if no native hook can consume it.
        let point = super::super::attention_inject_point(&captured.agent);
        if let Some(id) = claim_step(
            router,
            key,
            &incident.incident_id,
            Step::Question(point),
            None,
        )
        .await?
            && let Some(point) = point
        {
            let mut request = bound_request(key, turn_id, id, "inject");
            request.args = json!({"text": question_text(key.kind, &report), "point": point});
            submit_step(
                router,
                request,
                point != eliot_core::InjectPoint::TurnSafePoint,
            )
            .await?;
        }
        if notice_after_question {
            send_turn_notice(router, key, &report).await?;
        }
        return Ok(());
    };

    if notice_after_question {
        return send_turn_notice(router, key, &report).await;
    }
    let window_ms =
        i64::try_from(super::attention_duration(config.no_progress.threshold_secs).as_millis())
            .unwrap_or(i64::MAX);
    // A later stall in this turn starts its own observation window, but
    // retains the first question and any subsequent Owner intervention.
    if !no_progress_is_still_same_episode(captured, &incident)
        || now_ms().saturating_sub(question_at.max(report_event.ts)) < window_ms
    {
        return Ok(());
    }
    if let Some(id) = claim_step(
        router,
        key,
        &incident.incident_id,
        Step::Interrupt,
        Some(question_seq),
    )
    .await?
    {
        submit_step(router, bound_request(key, turn_id, id, "interrupt"), true).await?;
    }
    send_turn_notice(router, key, &report).await
}

fn no_progress_is_still_same_episode(
    captured: &CapturedAgent,
    incident: &super::ActiveIncident,
) -> bool {
    let model_progress = match (
        captured.last_model_progress_at,
        incident.baseline_model_progress_at,
    ) {
        (Some(current), Some(baseline)) => current > baseline,
        (Some(_), None) => true,
        _ => false,
    };
    let progress_basis = match (captured.progress_basis_at, incident.baseline_progress_at) {
        (Some(current), Some(baseline)) => current > baseline,
        (Some(_), None) => true,
        _ => false,
    };
    !model_progress && !progress_basis
}

fn question_text(kind: IncidentKind, report: &AttentionReport) -> String {
    match kind {
        IncidentKind::NoProgress => {
            let age_ms = report
                .progress_age_ms
                .or(report.suspicion_window_ms)
                .unwrap_or(0);
            format!(
                "No model activity for {}. If you are waiting, what are you waiting for? Otherwise please continue.",
                elapsed_text(age_ms)
            )
        }
        IncidentKind::CodeProgress => {
            let age_ms = report
                .code_progress_at
                .and_then(|at| now_ms().checked_sub(at))
                .filter(|age| *age >= 0)
                .unwrap_or_else(|| report.suspicion_window_ms.unwrap_or(0));
            let tools = report
                .tool_calls
                .map(|count| count.to_string())
                .unwrap_or_else(|| "an unknown number of".to_owned());
            let messages = report
                .messages
                .map(|count| count.to_string())
                .unwrap_or_else(|| "an unknown number of".to_owned());
            format!(
                "No code change for {}; this turn has {tools} tool calls and {messages} messages. What is blocking code progress? Please continue or explain.",
                elapsed_text(age_ms)
            )
        }
        _ => "Please continue or explain what is blocking progress.".to_owned(),
    }
}

fn elapsed_text(milliseconds: i64) -> String {
    let seconds = milliseconds.max(0) / 1000;
    if seconds < 60 {
        format!("{seconds} seconds")
    } else {
        let minutes = seconds / 60;
        if minutes == 1 {
            "1 minute".to_owned()
        } else {
            format!("{minutes} minutes")
        }
    }
}

#[derive(Clone, Copy)]
enum Step {
    Question(Option<eliot_core::InjectPoint>),
    Interrupt,
    OwnerNotice,
}

impl Step {
    fn name(self) -> &'static str {
        match self {
            Self::Question(_) => "question",
            Self::Interrupt => "interrupt",
            Self::OwnerNotice => "owner_notice",
        }
    }
}

async fn claim_step(
    router: &Router,
    key: &IncidentKey,
    incident_id: &str,
    step: Step,
    after_question: Option<i64>,
) -> Result<Option<String>> {
    let Some(turn) = key.turn_id.as_deref() else {
        return Ok(None);
    };
    // All turn-bound terminal steps share the cap across stall episodes.
    // The Owner notice is also shared across the timed incident kinds.
    let reason = (!matches!(step, Step::OwnerNotice)).then(|| key.kind.as_str());
    let mut store = router.inner.store.lock().await;
    if store
        .attention_turn_step(&key.agent, key.generation, turn, step.name(), reason)?
        .is_some()
    {
        return Ok(None);
    }
    let outcome = if let Some(seq) = after_question
        && store.attention_owner_input_after(&key.agent, key.generation, turn, seq)?
    {
        "owner_input"
    } else if matches!(step, Step::Question(None)) {
        "unsupported"
    } else {
        "unknown"
    };
    let command_id = (outcome == "unknown").then(|| ulid::Ulid::new().to_string());
    let reaction = eliot_core::AttentionStep {
        incident_id: incident_id.to_owned(),
        step: step.name().to_owned(),
        outcome: outcome.to_owned(),
        command_id: command_id.clone(),
        turn_id: Some(turn.to_owned()),
    };
    let Some(event) = store.claim_timed_attention_step(
        &key.agent,
        key.generation,
        reason,
        &reaction,
        after_question,
        now_ms(),
    )?
    else {
        return Ok(None);
    };
    router.publish_locked(&[event]);
    Ok(command_id)
}

fn bound_request(key: &IncidentKey, turn: &str, id: String, verb: &str) -> CommandSubmitRequest {
    let mut request = CommandSubmitRequest::new(id, verb);
    request.target = Some(key.agent.clone());
    request.expected_generation = Some(key.generation);
    request.expected_turn_id = Some(turn.to_owned());
    request
}

async fn send_turn_notice(
    router: &Router,
    key: &IncidentKey,
    report: &AttentionReport,
) -> Result<()> {
    let Some(id) = claim_step(router, key, &report.incident_id, Step::OwnerNotice, None).await?
    else {
        return Ok(());
    };
    super::delivery::send_notice(router, report, &id, &id).await
}

async fn submit_step(router: &Router, request: CommandSubmitRequest, wait: bool) -> Result<()> {
    let id = request.id.clone();
    let params = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "attention command could not be encoded"))?;
    if let Err(error) = router.submit(&params, &Principal::Owner).await {
        crate::logging::record(
            &router.secret_policy_snapshot(),
            "daemon.attention_action_failed",
            &[("command", &id)],
            Some(&error),
        );
        return Ok(());
    }
    if wait {
        let mut progress = router.progress_receiver();
        let _ = timeout(STEP_SETTLEMENT_TIMEOUT, async {
            loop {
                let command = router.inner.store.lock().await.get_command(&id)?;
                if command.is_none_or(|command| {
                    command.state.is_terminal() || command.state == CommandState::Unknown
                }) || progress.changed().await.is_err()
                {
                    return Ok::<_, Error>(());
                }
            }
        })
        .await;
    }
    Ok(())
}
