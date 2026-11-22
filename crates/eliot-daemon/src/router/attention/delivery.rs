//! Persist frozen reports and settle their existing mail/notice deliveries.

use super::super::Principal;
use super::{AttentionReport, FrozenReport, ReportPhase, Router};
use eliot_core::api::{EmitDefinitionArgs, MailMode, MailSendRequest};
use eliot_core::{Agent, AgentState, AttentionStep, CommandState, Error, Result};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::timeout;

const REPORT_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(15);

impl Router {
    pub(crate) async fn report_daemon_attention(&self, message: String) -> Result<()> {
        let id = ulid::Ulid::new().to_string();
        let message = self.secret_policy_snapshot().redact_text(&message);
        Box::pin(self.submit(
            &json!({"id": id, "verb": "notify", "args": {
                "title": "Daemon attention",
                "message": message,
            }}),
            &Principal::Owner,
        ))
        .await?;
        wait_for_applied(self, &id).await
    }
}

pub(super) async fn persist_report_event(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let phase = match report.phase {
        ReportPhase::Incident => "incident",
        ReportPhase::Recovery => "recovery",
    };
    if router
        .inner
        .store
        .lock()
        .await
        .attention_report_event(&report.incident_id, phase)?
        .is_some()
    {
        return Ok(());
    }
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
            .submit_attention_event(
                &json!({"id": delivery.emit_command_id.clone(), "verb": "attention.emit", "args": args}),
            )
            .await?;
    }
    match wait_for_applied(router, &delivery.emit_command_id).await {
        Ok(()) => Ok(()),
        Err(_)
            if router
                .inner
                .store
                .lock()
                .await
                .attention_report_event(&report.incident_id, phase)?
                .is_some() =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn persist_and_notify(router: &Router, delivery: &FrozenReport) -> Result<()> {
    persist_report_event(router, delivery).await?;
    if matches!(
        delivery.report.reason.as_str(),
        "no_progress" | "no_code_progress"
    ) {
        return Ok(());
    }
    send_notice(
        router,
        &delivery.report,
        &delivery.supervisor_mail_id,
        &delivery.owner_notify_id,
    )
    .await
}
/// Persist one frozen loss-evidence delivery. The deterministic mutation ids
/// replay the retained emit when this loss was already reported (a retained
/// command binding skips submission entirely), and per-thread presence skips
/// mail that is already delivered, so a crash between the emit commit and
/// notice delivery — or a restart re-report — cannot duplicate the incident.
pub(super) async fn persist_loss_delivery(
    router: &Router,
    delivery: &FrozenReport,
    resume_enabled: bool,
) -> Result<()> {
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
    if !resume_enabled {
        return Ok(());
    }
    if let Some(event_seq) = report.trigger_event_seq {
        router
            .resume_after_process_loss(
                &report.agent_id,
                report.generation,
                event_seq,
                &report.incident_id,
            )
            .await?;
        let command_id = {
            let store = router.inner.store.lock().await;
            store
                .attention_step_event(&report.incident_id, "resume_after_process_loss")?
                .and_then(|event| event.data["command_id"].as_str().map(str::to_owned))
        };
        if let Some(command_id) = command_id {
            let _ = wait_for_applied(router, &command_id).await;
        }
    }
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

pub(super) fn attention_thread(incident_id: &str) -> String {
    let prefix: String = incident_id.chars().take(20).collect();
    format!("attention-{prefix}")
}

async fn notice_recipient(router: &Router, candidate: Option<&str>) -> Result<Option<String>> {
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let store = router.inner.store.lock().await;
    Ok(store
        .get_agent(candidate)?
        .filter(|agent| {
            !agent.observe_only
                && matches!(
                    agent.state,
                    AgentState::Idle | AgentState::Busy | AgentState::Waiting
                )
        })
        .map(|agent| agent.id))
}

pub(super) async fn send_notice(
    router: &Router,
    report: &AttentionReport,
    mail_id: &str,
    owner_id: &str,
) -> Result<()> {
    let candidate = if report.reason == "children_unattended" {
        Some(report.agent_id.as_str())
    } else {
        report.parent_agent.as_deref()
    };
    let title = match report.phase {
        ReportPhase::Incident => "Agent attention",
        ReportPhase::Recovery => "Agent attention recovered",
    };
    route_notice(
        router,
        candidate,
        mail_id,
        owner_id,
        &report.incident_id,
        title,
        attention_summary(report),
    )
    .await
}

async fn route_notice(
    router: &Router,
    candidate: Option<&str>,
    mail_id: &str,
    owner_id: &str,
    incident_id: &str,
    title: &str,
    text: String,
) -> Result<()> {
    if let Some(recipient) = notice_recipient(router, candidate).await? {
        let request = MailSendRequest {
            id: mail_id.to_owned(),
            to: vec![recipient],
            mode: MailMode::Queue,
            text,
            thread: Some(attention_thread(incident_id)),
            reply_to_delivery: None,
        };
        let params = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "attention mail could not be encoded"))?;
        router.mail_send(&params, &Principal::Owner).await?;
    } else {
        router
            .submit(
                &json!({"id": owner_id, "verb": "notify", "args": {
                    "title": title, "message": format!("{text} ({incident_id})")
                }}),
                &Principal::Owner,
            )
            .await?;
        wait_for_applied(router, owner_id).await?;
    }
    Ok(())
}

impl Router {
    pub(in crate::router) async fn report_session_growth(
        &self,
        agent: &Agent,
        reaction: &AttentionStep,
    ) -> Result<()> {
        // Bind the report before routing. The manager's attention thread
        // needs this committed subject to make its one eligible wake.
        let emit_id = ulid::Ulid::new().to_string();
        let args = EmitDefinitionArgs {
            topic: "agent.attention".to_owned(),
            data: json!({
                "schema": 1,
                "incident_id": reaction.incident_id,
                "phase": "incident",
                "reason": "session_growth",
                "agent_id": agent.id,
                "generation": agent.generation,
                "turn_id": reaction.turn_id,
            }),
        };
        self.submit_attention_event(
            &json!({"id": emit_id, "verb": "attention.emit", "args": args}),
        )
        .await?;
        wait_for_applied(self, &emit_id).await?;
        let id = ulid::Ulid::new().to_string();
        let text = format!(
            "{} has session growth. Can you check it with swarm show {}?",
            agent.id, agent.id
        );
        route_notice(
            self,
            agent.parent.as_deref(),
            &id,
            &id,
            &reaction.incident_id,
            "Agent attention",
            text,
        )
        .await
    }
}

/// A retained loss has a single manager or Owner notice, even on retries.
async fn loss_notice_exists(router: &Router, delivery: &FrozenReport) -> Result<bool> {
    let report = &delivery.report;
    let store = router.inner.store.lock().await;
    Ok(store.get_command(&delivery.owner_notify_id)?.is_some()
        || store.attention_owner_notice_exists(&report.incident_id)?
        || report.parent_agent.as_deref().map_or(Ok(false), |parent| {
            store.attention_thread_mailed_to(&attention_thread(&report.incident_id), parent)
        })?)
}

async fn send_loss_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    if loss_notice_exists(router, delivery).await? {
        return Ok(());
    }
    let report = &delivery.report;
    match send_notice(
        router,
        report,
        &delivery.supervisor_mail_id,
        &delivery.owner_notify_id,
    )
    .await
    {
        Err(error)
            if error.code == "id_conflict" && loss_notice_exists(router, delivery).await? =>
        {
            Ok(())
        }
        Err(error)
            if error.code == "id_expired" && !loss_notice_exists(router, delivery).await? =>
        {
            // The old id was refused without dispatch after the retry window.
            let id = ulid::Ulid::new().to_string();
            send_notice(router, report, &id, &id).await
        }
        result => result,
    }
}

fn attention_summary(report: &AttentionReport) -> String {
    let agent = &report.agent_id;
    let fact = if report.phase == ReportPhase::Recovery {
        match (report.agent_state.as_str(), report.reason.as_str()) {
            ("stopped", _) => format!("{agent} stopped and its alert cleared."),
            (_, "no_code_progress") => format!("{agent}'s code-progress alert cleared."),
            (_, "no_progress") => format!("{agent}'s model-progress alert cleared."),
            _ => format!("{agent}'s observation source recovered."),
        }
    } else {
        let age = match report.reason.as_str() {
            "no_code_progress" => report
                .code_progress_at
                .and_then(|at| report.observed_at.checked_sub(at)),
            _ => report.progress_age_ms,
        };
        let elapsed = age
            .filter(|age| *age >= 0)
            .map(|age| format!(" for {} s", age / 1000))
            .unwrap_or_default();
        match report.reason.as_str() {
            "no_code_progress" => format!("{agent} has made no code changes{elapsed}."),
            "no_progress" => format!("{agent} has shown no model progress{elapsed}."),
            "children_unattended" => {
                format!(
                    "{agent} has {} unfinished children needing attention.",
                    report.children.unfinished
                )
            }
            _ if report.agent_state == "lost" => format!("{agent} lost its native session."),
            _ => format!("{agent} has an unhealthy observation source."),
        }
    };
    if report.reason == "children_unattended" {
        format!("{fact} Can you check those child turns?")
    } else {
        format!("{fact} Can you check it with swarm show {agent}?")
    }
}
