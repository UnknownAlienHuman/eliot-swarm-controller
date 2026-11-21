//! Owner-scoped failure diagnosis: `diagnose` assembles a bounded causal
//! chain from existing events and readbacks, with no new store of record.
//! The chain is the command lifecycle (code, message, `where`), the related
//! turn outcomes and their `preceded_by` links, process exits with codes,
//! the last stderr lines, and a previous-run crash marker when the terminal
//! failure has no classified origin. The one-line summary always starts
//! with the failure origin when a failure was observed.

use super::{Principal, Router, require_owner};
use eliot_core::{
    Command, Error, Event, Result, Turn,
    api::{
        CrashMarker, DiagnoseRequest, DiagnoseResponse, DiagnoseStep, MAX_DIAGNOSE_STEPS,
        MAX_DIAGNOSE_SUMMARY_CHARS, discovery::decode_params,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;

const MAX_TURNS_PER_DIAGNOSIS: usize = 8;
const MAX_LOSS_EVENTS_PER_DIAGNOSIS: usize = 2;

impl Router {
    pub(super) async fn diagnose(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: DiagnoseRequest = decode_params(params.clone())?;
        request.validate()?;
        let target = request.target.trim().to_owned();
        let response = self.diagnose_target(&target).await?;
        request
            .validate_response(&response)
            .map_err(|_| Error::new("internal", "diagnose response failed validation"))?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "diagnose response could not be encoded"))
    }

    async fn diagnose_target(&self, target: &str) -> Result<DiagnoseResponse> {
        let previous_crash = self
            .inner
            .previous_crash
            .lock_recover()
            .clone()
            .filter(|marker| marker.validate().is_ok());
        if let Some((agent, turn_id)) = target.split_once(':')
            && !agent.is_empty()
            && !turn_id.is_empty()
            && !turn_id.contains(':')
        {
            return self
                .diagnose_turn(agent, turn_id, target, previous_crash)
                .await;
        }
        let is_command = match self.inner.store.lock().await.get_command(target) {
            Ok(command) => command.is_some(),
            Err(error) if error.code == "invalid" => false,
            Err(error) => return Err(error),
        };
        if is_command {
            return self.diagnose_command(target, target, previous_crash).await;
        }
        let is_agent = match self.inner.store.lock().await.get_agent(target) {
            Ok(agent) => agent.is_some(),
            Err(error) if error.code == "invalid" => false,
            Err(error) => return Err(error),
        };
        if is_agent {
            return self.diagnose_agent(target, target, previous_crash).await;
        }
        if let Some(step) = crate::logging::capacity_refusal(
            &self.inner.data_root,
            target,
            &self.secret_policy_snapshot(),
        ) {
            let steps = vec![step];
            return Ok(DiagnoseResponse {
                target: target.to_owned(),
                summary: summary_for(target, &steps, "admission"),
                steps,
                preceded_by: None,
                previous_crash: None,
            });
        }
        Err(Error::new(
            "not_found",
            format!("diagnose target {target} is not a command, Agent or agent:turn"),
        ))
    }

    async fn diagnose_command(
        &self,
        command_id: &str,
        target: &str,
        previous_crash: Option<CrashMarker>,
    ) -> Result<DiagnoseResponse> {
        let store = self.inner.store.lock().await;
        let command = store.get_command(command_id)?.ok_or_else(|| {
            Error::new("not_found", format!("command {command_id} was not found"))
        })?;
        // A start command's own outcome can be `ready` while the failure it
        // produced sits on its linked input expansion (`<id>/send`), the
        // command that actually settled the turn. Diagnosing the start alone
        // would report success, so follow that exact link when it exists.
        let linked_send = store
            .get_command(&format!("{command_id}/send"))?
            .filter(|linked| {
                linked.target.as_deref() == command.target.as_deref()
                    && linked.generation == command.generation
            })
            .map(|linked| linked.id.clone());
        let mut steps = vec![command_step(&command)];
        if let Some(linked) = linked_send.as_deref()
            && let Some(child) = store.get_command(linked)?
        {
            steps.push(command_step(&child));
        }
        let mut preceded_by = None;
        if let (Some(agent), Some(generation)) = (command.target.as_deref(), command.generation)
            && let Ok(turns) = store.list_turns(agent, generation, 32)
        {
            for turn in turns {
                if steps.len() > MAX_TURNS_PER_DIAGNOSIS {
                    break;
                }
                let turn_command = store.turn_command_id(agent, generation, &turn.turn_id)?;
                // A turn belongs to this diagnosis only through a recorded
                // settlement link. Comparing two absent ids would otherwise
                // adopt an unrelated unmapped turn for a start that never had
                // a linked send.
                let settled_here = turn_command.as_deref() == Some(command_id)
                    || linked_send
                        .as_deref()
                        .is_some_and(|linked| turn_command.as_deref() == Some(linked));
                if !settled_here {
                    continue;
                }
                if preceded_by.is_none() {
                    preceded_by = turn.preceded_by.clone();
                }
                steps.push(turn_step(
                    &turn,
                    store
                        .turn_terminal_event(agent, generation, &turn.turn_id)?
                        .as_ref(),
                ));
            }
            steps.extend(loss_steps(&store, agent, generation)?);
        }
        drop(store);
        let origin = chain_origin(&steps);
        Ok(DiagnoseResponse {
            target: target.to_owned(),
            summary: summary_for(target, &steps, "command"),
            steps: bounded_steps(steps),
            preceded_by,
            previous_crash: crash_if_unexplained(&origin, previous_crash),
        })
    }

    async fn diagnose_turn(
        &self,
        agent: &str,
        turn_id: &str,
        target: &str,
        previous_crash: Option<CrashMarker>,
    ) -> Result<DiagnoseResponse> {
        let store = self.inner.store.lock().await;
        let generation = store
            .get_agent(agent)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?
            .generation;
        let turn = store
            .get_turn(agent, generation, turn_id)?
            .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
        let mut steps = Vec::new();
        if let Some(command_id) = store.turn_command_id(agent, generation, turn_id)?
            && let Some(command) = store.get_command(&command_id)?
        {
            steps.push(command_step(&command));
        }
        let preceded_by = turn.preceded_by.clone();
        steps.push(turn_step(
            &turn,
            store
                .turn_terminal_event(agent, generation, turn_id)?
                .as_ref(),
        ));
        steps.extend(loss_steps(&store, agent, generation)?);
        drop(store);
        let origin = chain_origin(&steps);
        Ok(DiagnoseResponse {
            target: target.to_owned(),
            summary: summary_for(target, &steps, "turn"),
            steps: bounded_steps(steps),
            preceded_by,
            previous_crash: crash_if_unexplained(&origin, previous_crash),
        })
    }

    async fn diagnose_agent(
        &self,
        agent: &str,
        target: &str,
        previous_crash: Option<CrashMarker>,
    ) -> Result<DiagnoseResponse> {
        let store = self.inner.store.lock().await;
        let row = store
            .get_agent(agent)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
        let generation = row.generation;
        let mut steps = vec![DiagnoseStep {
            kind: "agent".to_owned(),
            code: Some(bound_chars(
                eliot_core::api::agent_state_name(row.state),
                128,
            )),
            message: None,
            failure_where: None,
            origin: None,
            origin_evidence: None,
            exit_code: None,
            ntstatus: None,
            at: (row.updated_at >= 0).then_some(row.updated_at),
        }];
        let mut preceded_by = None;
        let mut commands = std::collections::HashSet::new();
        let mut unresolved = Vec::new();
        let (command, turn) = store.agent_unknowns(agent)?;
        if let Some(command) = command {
            unresolved.push(unknown_command_message(&command, row.observe_only));
            commands.insert(command.id.clone());
            let mut step = command_step(&command);
            step.message = unresolved.last().cloned();
            steps.push(step);
        }
        if let Some(turn) = turn {
            unresolved.push(format!("unknown turn {} (generation {}); resolve with swarm turn resolve using an exact Owner assertion",
                turn.turn_id, turn.generation));
            let mut step = turn_step(
                &turn,
                store
                    .turn_terminal_event(agent, turn.generation, &turn.turn_id)?
                    .as_ref(),
            );
            step.message = unresolved.last().cloned();
            steps.push(step);
        }
        if let Some(command) = store.generation_start_command(agent, generation)? {
            commands.insert(command.id.clone());
            if command.state != eliot_core::CommandState::Applied {
                steps.push(command_step(&command));
            }
        }
        {
            let turns = store.list_turns(agent, generation, 32)?;
            for turn in turns {
                if steps.len() > MAX_TURNS_PER_DIAGNOSIS {
                    break;
                }
                if !matches!(
                    turn.outcome,
                    Some(
                        eliot_core::TurnOutcome::Failure
                            | eliot_core::TurnOutcome::Interrupted
                            | eliot_core::TurnOutcome::Unknown
                    )
                ) {
                    continue;
                }
                if preceded_by.is_none() {
                    preceded_by = turn.preceded_by.clone();
                }
                // Prefer the command which settled this failure. The original
                // input association remains a distinct exact causal link.
                let mut causal = Vec::new();
                if let Some(id) = turn.preceded_by.as_ref() {
                    causal.push(id.clone());
                }
                if let Some(id) = store.turn_command_id(agent, generation, &turn.turn_id)? {
                    causal.push(id);
                }
                for id in causal {
                    if commands.insert(id.clone())
                        && let Some(command) = store.get_command(&id)?
                        && command.target.as_deref() == Some(agent)
                        && command.generation == Some(generation)
                    {
                        steps.push(command_step(&command));
                    }
                }
                steps.push(turn_step(
                    &turn,
                    store
                        .turn_terminal_event(agent, generation, &turn.turn_id)?
                        .as_ref(),
                ));
            }
        }
        steps.extend(loss_steps(&store, agent, generation)?);
        drop(store);
        let origin = chain_origin(&steps);
        Ok(DiagnoseResponse {
            target: target.to_owned(),
            summary: append_retained(&summary_for(target, &steps, "agent"), &unresolved),
            previous_crash: if healthy_agent(&steps) {
                None
            } else {
                crash_if_unexplained(&origin, previous_crash)
            },
            steps: bounded_steps(steps),
            preceded_by,
        })
    }
}

fn append_retained(summary: &str, unresolved: &[String]) -> String {
    if unresolved.is_empty() {
        return summary.to_owned();
    }
    // Reserve a prefix for the origin and target even when retained details
    // are long, then bound the failure message to leave space for the suffix.
    let retained = bound_chars(
        &format!("; retained {}", unresolved.join("; ")),
        MAX_DIAGNOSE_SUMMARY_CHARS * 3 / 4,
    );
    let prefix = bound_chars(
        summary,
        MAX_DIAGNOSE_SUMMARY_CHARS - retained.chars().count(),
    );
    format!("{prefix}{retained}")
}

fn unknown_command_message(command: &Command, observe_only: bool) -> String {
    let resolution = if observe_only {
        "not Owner-resolvable: observe-only commands remain retained for the record"
    } else {
        "resolve with swarm cmd resolve using an exact Owner assertion"
    };
    format!(
        "unknown command {} (generation {}); {resolution}",
        command.id,
        command
            .generation
            .map_or_else(|| "unassigned".to_owned(), |g| g.to_string())
    )
}

/// The previous-run crash marker only when the chain leaves the
/// terminal failure unexplained: a classified origin already names
/// the culprit, while an unknown origin may be the crash itself.
fn crash_if_unexplained(origin: &str, previous_crash: Option<CrashMarker>) -> Option<CrashMarker> {
    (origin == "unknown").then_some(previous_crash).flatten()
}

fn chain_origin(steps: &[DiagnoseStep]) -> String {
    steps
        .iter()
        .filter_map(|step| step.origin.as_deref())
        .find(|origin| *origin != "unknown")
        .unwrap_or("unknown")
        .to_owned()
}

fn command_step(command: &Command) -> DiagnoseStep {
    let outcome = command.outcome.as_ref();
    DiagnoseStep {
        kind: "command".to_owned(),
        code: outcome.and_then(outcome_code).or_else(|| {
            Some(bound_chars(
                &format!("{:?}", command.state).to_lowercase(),
                128,
            ))
        }),
        message: outcome.and_then(|outcome| {
            outcome
                .get("message")
                .and_then(Value::as_str)
                .map(|message| bound_chars(message, 1024))
        }),
        failure_where: outcome.and_then(|outcome| {
            outcome
                .get("where")
                .and_then(Value::as_str)
                .map(|where_| bound_chars(where_, 128))
        }),
        origin: outcome.and_then(|outcome| {
            outcome
                .get("origin")
                .and_then(Value::as_str)
                .map(|origin| bound_chars(origin, 32))
        }),
        origin_evidence: outcome.and_then(|outcome| {
            outcome
                .get("origin_evidence")
                .and_then(Value::as_str)
                .map(|evidence| bound_chars(evidence, 512))
        }),
        exit_code: outcome
            .and_then(|outcome| outcome.get("exit_code"))
            .and_then(Value::as_i64),
        ntstatus: outcome.and_then(|outcome| {
            outcome
                .get("ntstatus")
                .and_then(Value::as_str)
                .map(|ntstatus| bound_chars(ntstatus, 16))
        }),
        at: (command.updated_at >= 0).then_some(command.updated_at),
    }
}

fn outcome_code(outcome: &Value) -> Option<String> {
    for key in ["code", "error", "rejected", "failed", "state", "cause"] {
        if let Some(code) = outcome.get(key).and_then(Value::as_str)
            && !code.is_empty()
        {
            return Some(bound_chars(code, 128));
        }
    }
    None
}

fn turn_step(turn: &Turn, terminal: Option<&Event>) -> DiagnoseStep {
    let mut message = terminal
        .and_then(|event| event.data.get("native_error")?.get("message")?.as_str())
        .or(turn.native_status.as_deref())
        .or(turn.status.as_deref())
        .map(|message| bound_chars(message, 1024));
    if let Some(reset) = terminal.and_then(|event| {
        event
            .data
            .get("provider_restriction")?
            .get("retry_after")?
            .as_i64()
    }) {
        message = Some(bound_chars(
            &format!(
                "{}; reset at {reset} (Unix ms)",
                message.as_deref().unwrap_or("provider restriction")
            ),
            1024,
        ));
    }
    DiagnoseStep {
        kind: "turn".to_owned(),
        code: terminal
            .and_then(|event| event.data.get("native_error")?.get("code")?.as_str())
            .map(|code| bound_chars(code, 128))
            .or_else(|| {
                if turn.state == eliot_core::TurnState::Unknown {
                    Some("unknown".to_owned())
                } else {
                    turn.outcome.map(|outcome| outcome.as_str().to_owned())
                }
            }),
        message,
        failure_where: terminal
            .and_then(|event| event.data.get("where")?.as_str())
            .map(|text| bound_chars(text, 128)),
        origin: terminal
            .and_then(|event| event.data.get("origin")?.as_str())
            .map(|text| bound_chars(text, 32)),
        origin_evidence: terminal
            .and_then(|event| event.data.get("origin_evidence")?.as_str())
            .map(|text| bound_chars(text, 512)),
        exit_code: None,
        ntstatus: None,
        at: turn.completed_at.filter(|at| *at >= 0),
    }
}

/// Newest-first loss steps for one generation: one `process` step per
/// `agent.lost`/`agent.failed` event with the retained exit code and
/// NTSTATUS, plus one `stderr` step when the event kept a tail.
fn loss_steps(
    store: &eliot_store::Store,
    agent: &str,
    generation: i64,
) -> Result<Vec<DiagnoseStep>> {
    let mut steps = Vec::new();
    let events = store.latest_agent_terminal_events(agent, generation, 8)?;
    for event in events.into_iter().take(MAX_LOSS_EVENTS_PER_DIAGNOSIS) {
        let at = (event.ts >= 0).then_some(event.ts);
        steps.push(loss_step(&event, at));
        if let Some(tail) = event
            .data
            .get("stderr_tail")
            .and_then(Value::as_str)
            .map(|tail| bound_chars(tail.trim(), 1024))
            .filter(|tail| !tail.is_empty())
        {
            steps.push(DiagnoseStep {
                kind: "stderr".to_owned(),
                code: None,
                message: Some(tail),
                failure_where: None,
                origin: None,
                origin_evidence: None,
                exit_code: None,
                ntstatus: None,
                at,
            });
        }
    }
    Ok(steps)
}

/// One loss/failure event as a step. Its recorded `origin`/`where` come
/// first; a retained loss written before they were recorded classifies only
/// from its typed `reason` code (a daemon restart is Swarm's own), never
/// from text.
fn loss_step(event: &Event, at: Option<i64>) -> DiagnoseStep {
    let reason = event.data.get("reason").and_then(Value::as_str);
    let typed = reason.and_then(eliot_core::swarm_loss_location);
    let text = |key: &str, limit: usize| {
        event
            .data
            .get(key)
            .and_then(Value::as_str)
            .map(|value| bound_chars(value, limit))
    };
    DiagnoseStep {
        kind: "process".to_owned(),
        code: Some(bound_chars(&event.kind, 128)),
        message: reason.map(|reason| bound_chars(reason, 1024)),
        failure_where: text("where", 128)
            .or_else(|| typed.map(|(_, location)| location.to_owned())),
        origin: text("origin", 32).or_else(|| typed.map(|(origin, _)| origin.as_str().to_owned())),
        origin_evidence: text("origin_evidence", 512),
        exit_code: event.data.get("exit_code").and_then(Value::as_i64),
        ntstatus: event
            .data
            .get("ntstatus")
            .and_then(Value::as_str)
            .map(|ntstatus| bound_chars(ntstatus, 16)),
        at,
    }
}

/// One plain-language line that starts with the failure origin: the most
/// informative step (command outcome, else turn outcome, else loss reason,
/// else agent state) rendered as origin, target, what, where and message.
fn summary_for(target: &str, steps: &[DiagnoseStep], fallback_what: &str) -> String {
    if healthy_agent(steps) {
        return bound_chars(
            &format!(
                "{target} is {}; nothing has failed in recent retained history",
                steps[0].code.as_deref().unwrap_or("idle")
            ),
            MAX_DIAGNOSE_SUMMARY_CHARS,
        );
    }
    let step = eliot_core::api::diagnosis_reason(steps);
    let (origin, what, where_, message) = match step {
        Some(step) => (
            step.origin.as_deref().unwrap_or("unknown"),
            step.code.as_deref().unwrap_or(fallback_what),
            step.failure_where.as_deref(),
            step.message.as_deref(),
        ),
        None => ("unknown", fallback_what, None, None),
    };
    let mut summary = format!("{origin} origin: {target} {what}");
    if let Some(where_) = where_
        && !where_.is_empty()
    {
        summary.push_str(" at ");
        summary.push_str(where_);
    }
    if let Some(message) = message
        && !message.trim().is_empty()
    {
        summary.push_str(": ");
        summary.push_str(message.trim());
    }
    bound_chars(&summary, MAX_DIAGNOSE_SUMMARY_CHARS)
}

fn healthy_agent(steps: &[DiagnoseStep]) -> bool {
    steps.len() == 1
        && steps[0].kind == "agent"
        && matches!(
            steps[0].code.as_deref(),
            Some("idle" | "busy" | "waiting" | "stopped")
        )
}

/// Hard cap: the command step plus bounded turn, loss and agent evidence
/// never exceed the response step budget.
fn bounded_steps(mut steps: Vec<DiagnoseStep>) -> Vec<DiagnoseStep> {
    steps.truncate(MAX_DIAGNOSE_STEPS);
    steps
}

/// Char-boundary truncation for step strings the store did not already
/// bound (native reasons, agent states, assembled summaries).
fn bound_chars(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let end = text
        .char_indices()
        .take(limit)
        .last()
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    text[..end].to_owned()
}
