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
        let mut steps = vec![command_step(&command)];
        let mut preceded_by = None;
        if let (Some(agent), Some(generation)) = (command.target.as_deref(), command.generation)
            && let Ok(turns) = store.list_turns(agent, generation, 32)
        {
            for turn in turns {
                if steps.len() > MAX_TURNS_PER_DIAGNOSIS {
                    break;
                }
                if store.turn_command_id(agent, generation, &turn.turn_id)?
                    != Some(command_id.to_owned())
                {
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
            code: Some(bound_chars(eliot_core::api::agent_state_name(row.state), 128)),
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
            summary: summary_for(target, &steps, "agent"),
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
            .or_else(|| turn.outcome.map(|outcome| outcome.as_str().to_owned())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::now_ms;
    use eliot_core::api::DiagnoseResponse;
    use eliot_core::{CommandState, NativeEvent, SourceBatch, SubmitCommand};
    use serde_json::json;

    fn failed_send_fixture(store: &mut eliot_store::Store) -> (String, String, String) {
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let agent = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"why-test", "workspace":"synthetic-workspace", "harness":"codex", "profile":"codex-fixture", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch")
            .command
            .target
            .expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        let send_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text":"hi"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        store
            .settle(
                &launch_id,
                CommandState::Failed,
                Some(json!({"code":"provider_timeout", "message":"model timed out", "where":"provider.request", "origin":"provider"})),
                now,
            )
            .expect("settle launch failed");
        (agent, launch_id, send_id)
    }

    async fn test_router() -> super::super::Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).expect("open store");
        super::super::Router::new(
            std::path::PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            std::sync::Arc::new(tokio::sync::Mutex::new(store)),
            crate::bus::Bus::new(16),
        )
    }

    #[tokio::test]
    async fn failed_launch_agent_includes_exact_command_without_a_turn() {
        let router = test_router().await;
        let (agent, _, _) = {
            let mut store = router.inner.store.lock().await;
            failed_send_fixture(&mut store)
        };
        let value = router
            .diagnose(&json!({"target": agent}), &Principal::Owner)
            .await
            .unwrap();
        let response: DiagnoseResponse = serde_json::from_value(value).unwrap();
        assert!(
            response.summary.starts_with("provider origin:"),
            "{}",
            response.summary
        );
        assert!(response.steps.iter().any(|step| step.kind == "command"
            && step.failure_where.as_deref() == Some("provider.request")));
    }

    #[tokio::test]
    async fn diagnose_is_owner_only() {
        let router = test_router().await;
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        assert_eq!(
            router
                .diagnose(&json!({"target":"a1"}), &agent)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[tokio::test]
    async fn diagnose_unknown_target_is_not_found() {
        let router = test_router().await;
        assert_eq!(
            router
                .diagnose(&json!({"target":"missing"}), &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "not_found"
        );
    }

    #[tokio::test]
    async fn diagnose_failed_command_names_origin_and_where() {
        let router = test_router().await;
        let (_agent, failed_id, _send) = {
            let mut store = router.inner.store.lock().await;
            failed_send_fixture(&mut store)
        };
        let value = router
            .diagnose(&json!({"target":failed_id}), &Principal::Owner)
            .await
            .expect("diagnose failed command");
        let response: DiagnoseResponse = serde_json::from_value(value).expect("typed reply");
        assert!(
            response.summary.starts_with("provider origin:"),
            "{}",
            response.summary
        );
        assert!(
            response.summary.contains("provider.request"),
            "{}",
            response.summary
        );
        let command = response
            .steps
            .iter()
            .find(|step| step.kind == "command")
            .expect("command step");
        assert_eq!(command.failure_where.as_deref(), Some("provider.request"));
        assert_eq!(command.origin.as_deref(), Some("provider"));
        assert_eq!(response.target, failed_id);
    }

    #[tokio::test]
    async fn diagnose_failed_turn_links_its_command() {
        let router = test_router().await;
        let now = now_ms();
        let (agent, send_id, turn_id) = {
            let mut store = router.inner.store.lock().await;
            let launch_id = ulid::Ulid::new().to_string();
            let agent = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"why-turn", "workspace":"synthetic-workspace", "harness":"codex", "profile":"codex-fixture", "profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .expect("admit launch")
                .command
                .target
                .expect("launch target");
            store.dispatch(&launch_id, now).expect("dispatch launch");
            let plan = ["session", "turn", "message", "tool", "request"]
                .into_iter()
                .map(|family| (family.to_owned(), "stream".to_owned()))
                .collect();
            store
                .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
                .expect("install sources");
            store
                .put_batch(
                    SourceBatch {
                        batch_id: "ready".to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: "ready".to_owned(),
                            data: json!({"command_id":launch_id}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .expect("commit ready");
            store
                .settle(&launch_id, CommandState::Applied, None, now)
                .expect("settle start");
            let send_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: send_id.clone(),
                        target: Some(agent.clone()),
                        selector: None,
                        verb: "send".to_owned(),
                        args: json!({"text":"hi"}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .expect("admit send");
            store.dispatch(&send_id, now).expect("dispatch send");
            let turn_id = format!("turn-{send_id}");
            for (kind, suffix, with_outcome) in [
                ("turn.started", "started", false),
                ("turn.completed", "terminal", true),
            ] {
                let mut data = json!({"command_id":send_id, "turn_id":turn_id, "status":"error"});
                if with_outcome {
                    data["outcome"] = json!("failure");
                }
                store
                    .put_batch(
                        SourceBatch {
                            batch_id: format!("{turn_id}:{suffix}"),
                            agent_id: agent.clone(),
                            generation: 1,
                            source: "stream".to_owned(),
                            events: vec![NativeEvent {
                                ts: None,
                                kind: kind.to_owned(),
                                native_id: format!("{turn_id}:{suffix}"),
                                data,
                            }],
                            cursor: None,
                        },
                        now,
                    )
                    .expect("commit turn event");
            }
            (agent, send_id, turn_id)
        };
        let target = format!("{agent}:{turn_id}");
        let value = router
            .diagnose(&json!({"target":target}), &Principal::Owner)
            .await
            .expect("diagnose failed turn");
        let response: DiagnoseResponse = serde_json::from_value(value).expect("typed reply");
        assert_eq!(response.preceded_by.as_deref(), Some(send_id.as_str()));
        assert!(
            response
                .steps
                .iter()
                .any(|step| step.kind == "turn" && step.code.as_deref() == Some("failure")),
            "{}",
            serde_json::to_string(&response.steps).expect("steps encode")
        );
        assert!(
            response.summary.starts_with("unknown origin:"),
            "{}",
            response.summary
        );
        assert!(response.summary.contains("failure"), "{}", response.summary);
    }

    #[tokio::test]
    async fn a_loss_to_a_daemon_restart_is_swarm_origin_at_daemon_restart() {
        let router = test_router().await;
        let now = now_ms();
        let agent = {
            let mut store = router.inner.store.lock().await;
            let launch_id = ulid::Ulid::new().to_string();
            let agent = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"why-restart", "workspace":"synthetic-workspace", "harness":"codex", "profile":"codex-fixture", "profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .expect("admit launch")
                .command
                .target
                .expect("launch target");
            store.dispatch(&launch_id, now).expect("dispatch launch");
            let plan = ["session", "turn", "message", "tool", "request"]
                .into_iter()
                .map(|family| (family.to_owned(), "stream".to_owned()))
                .collect();
            store
                .install_plugin_sources(&agent, 1, &launch_id, &plan, now)
                .expect("install sources");
            store
                .put_batch(
                    SourceBatch {
                        batch_id: "ready".to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: "ready".to_owned(),
                            data: json!({"command_id":launch_id}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .expect("commit ready");
            store
                .settle(&launch_id, CommandState::Applied, None, now)
                .expect("settle start");
            store.recover_on_restart(now + 1).expect("restart");
            agent
        };
        let value = router
            .diagnose(&json!({"target": agent}), &Principal::Owner)
            .await
            .expect("diagnose lost agent");
        let response: DiagnoseResponse = serde_json::from_value(value).expect("typed reply");
        assert!(
            response.summary.starts_with("swarm origin:"),
            "{}",
            response.summary
        );
        assert!(
            response.summary.contains("daemon.restart"),
            "{}",
            response.summary
        );
        let loss = response
            .steps
            .iter()
            .find(|step| step.kind == "process")
            .expect("loss step");
        assert_eq!(loss.origin.as_deref(), Some("swarm"));
        assert_eq!(loss.failure_where.as_deref(), Some("daemon.restart"));
    }

    #[test]
    fn a_retained_restart_loss_without_recorded_origin_classifies_from_its_reason() {
        let event: Event = serde_json::from_value(json!({
            "seq": 7, "ts": 1, "kind": "agent.lost", "family": "agent", "agent": "a2",
            "generation": 1, "source": "daemon",
            "data": {"agent_id": "a2", "reason": "daemon_restart"}
        }))
        .expect("retained event");
        let step = loss_step(&event, Some(1));
        assert_eq!(step.origin.as_deref(), Some("swarm"));
        assert_eq!(step.failure_where.as_deref(), Some("daemon.restart"));
        let mut other = event.clone();
        other.data = json!({"agent_id": "a2", "reason": "native_exit"});
        let step = loss_step(&other, Some(1));
        assert_eq!(step.origin, None);
        assert_eq!(step.failure_where, None);
    }
}
