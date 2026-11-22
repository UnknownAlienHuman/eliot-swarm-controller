//! Persist frozen reports and settle their existing mail/notice deliveries.

use super::super::Principal;
use super::{AttentionReport, FrozenReport, ReportPhase, Router};
use eliot_core::api::{EmitDefinitionArgs, MailMode, MailSendRequest};
use eliot_core::{AgentState, CommandState, Error, Result};
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

pub(super) async fn persist_and_notify(router: &Router, delivery: &FrozenReport) -> Result<()> {
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
/// notice delivery â€” or a restart re-report â€” cannot duplicate the incident.
pub(super) async fn persist_loss_delivery(router: &Router, delivery: &FrozenReport) -> Result<()> {
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

pub(super) fn attention_thread(incident_id: &str) -> String {
    let prefix: String = incident_id.chars().take(20).collect();
    format!("attention-{prefix}")
}

async fn notice_recipient(router: &Router, report: &AttentionReport) -> Result<Option<String>> {
    let recipient = if report.reason == "children_unattended" {
        Some(report.agent_id.as_str())
    } else {
        report.parent_agent.as_deref()
    };
    let Some(recipient) = recipient else {
        return Ok(None);
    };
    let store = router.inner.store.lock().await;
    Ok(store
        .get_agent(recipient)?
        .filter(|agent| {
            !agent.observe_only
                && matches!(
                    agent.state,
                    AgentState::Idle | AgentState::Busy | AgentState::Waiting
                )
        })
        .map(|agent| agent.id))
}

async fn send_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    if let Some(recipient) = notice_recipient(router, report).await? {
        let request = MailSendRequest {
            id: delivery.supervisor_mail_id.clone(),
            to: vec![recipient],
            mode: MailMode::Queue,
            text: attention_summary(report),
            thread: Some(attention_thread(&report.incident_id)),
            reply_to_delivery: None,
        };
        let params = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "attention mail could not be encoded"))?;
        router.mail_send(&params, &Principal::Owner).await?;
    } else {
        submit_owner_notice(router, &delivery.owner_notify_id, report).await?;
        wait_for_applied(router, &delivery.owner_notify_id).await?;
    }
    Ok(())
}

async fn submit_owner_notice(router: &Router, id: &str, report: &AttentionReport) -> Result<()> {
    let title = match report.phase {
        ReportPhase::Incident => "Agent attention",
        ReportPhase::Recovery => "Agent attention recovered",
    };
    // Readback of a durable loss notice uses this incident ID even after the
    // original notify command is pruned.
    let message = format!("{} ({})", attention_summary(report), report.incident_id);
    router
        .submit(
            &json!({
                "id": id,
                "verb": "notify",
                "args": {"title": title, "message": message},
            }),
            &Principal::Owner,
        )
        .await?;
    Ok(())
}

/// Settle the one actionable notice for a loss without resending a retained
/// manager delivery or owner notification.
async fn send_loss_notice(router: &Router, delivery: &FrozenReport) -> Result<()> {
    let report = &delivery.report;
    let thread = attention_thread(&report.incident_id);
    {
        let store = router.inner.store.lock().await;
        if store.get_command(&delivery.owner_notify_id)?.is_some()
            || store.attention_owner_notice_exists(&report.incident_id)?
            || report.parent_agent.as_deref().map_or(Ok(false), |parent| {
                store.attention_thread_mailed_to(&thread, parent)
            })?
        {
            return Ok(());
        }
    }
    if let Some(recipient) = notice_recipient(router, report).await? {
        return send_loss_mail(
            router,
            &delivery.supervisor_mail_id,
            &recipient,
            thread,
            attention_summary(report),
        )
        .await;
    }
    match submit_owner_notice(router, &delivery.owner_notify_id, report).await {
        Err(error)
            if error.code == "id_expired" && {
                let store = router.inner.store.lock().await;
                store.get_command(&delivery.owner_notify_id)?.is_none()
            } =>
        {
            // Retained loss evidence can predate the mutation retry window.
            submit_owner_notice(router, &ulid::Ulid::new().to_string(), report).await
        }
        result => result,
    }
}

/// Keep the exact deterministic mail ID on retries. A delivery committed
/// concurrently is confirmed through the existing thread readback.
async fn send_loss_mail(
    router: &Router,
    mail_id: &str,
    recipient: &str,
    thread: String,
    text: String,
) -> Result<()> {
    let request = MailSendRequest {
        id: mail_id.to_owned(),
        to: vec![recipient.to_owned()],
        mode: MailMode::Queue,
        text,
        thread: Some(thread.clone()),
        reply_to_delivery: None,
    };
    let params = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "attention mail could not be encoded"))?;
    match router.mail_send(&params, &Principal::Owner).await {
        Ok(_) => Ok(()),
        Err(error)
            if error.code == "id_conflict" && {
                let store = router.inner.store.lock().await;
                store.attention_thread_mailed_to(&thread, recipient)?
            } =>
        {
            Ok(())
        }
        Err(error) => Err(error),
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
