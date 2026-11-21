//! Persist frozen reports and settle their existing mail/notice deliveries.

use super::super::Principal;
use super::{AttentionReport, FrozenReport, ReportPhase, Router};
use eliot_core::api::{EmitDefinitionArgs, MailMode, MailSendRequest};
use eliot_core::{CommandState, Error, Result};
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
/// notice delivery — or a restart re-report — cannot duplicate the incident.
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
