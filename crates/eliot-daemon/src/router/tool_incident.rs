//! React once when an exact native command failure repeats three times in one turn.
//!
//! Native tool events are committed before they reach this worker. The Store
//! joins the exact start/completion pair and bounds the evidence; the router
//! owns settings, turn binding, durable step admission and the injected action.

use super::{MODE_OPEN, Router};
use crate::daemon::workers::WorkerReporter;
use eliot_core::{
    Agent, AgentState, AttentionStep, CommandState, Error, Event, Result, SecretPolicy,
    api::EmitDefinitionArgs,
};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::{
    sync::{broadcast, watch},
    time::timeout,
};

const FAILURE_LIMIT: u8 = 3;
const REPLAY_TOOL_EVENT_LIMIT: usize = 64;
const REPLAY_SEQUENCE_WINDOW: i64 = 4_096;
const MESSAGE_COMMAND_BYTES: usize = 256;
const MESSAGE_OUTPUT_BYTES: usize = 384;
const REPORT_SETTLEMENT_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn run_tool_failure_attention(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    reporter: WorkerReporter,
) -> Result<()> {
    let mut events = router.inner.bus.subscribe();
    let (mut cursor, startup_events) = recent_event_tail(&router).await?;
    reporter.running("tool_failure_attention");
    for event in startup_events {
        process_event(&router, &event).await?;
    }

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            received = events.recv() => match received {
                Ok(event) if event.seq <= cursor => {}
                Ok(event) => {
                    if event.seq > cursor.saturating_add(1) {
                        replay_recent(&router, &mut cursor).await?;
                    }
                    if event.seq > cursor {
                        process_event(&router, &event).await?;
                        cursor = event.seq;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    replay_recent(&router, &mut cursor).await?;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("internal", "committed event bus closed"));
                }
            }
        }
    }
    Ok(())
}

async fn recent_event_tail(router: &Router) -> Result<(i64, Vec<Event>)> {
    let store = router.inner.store.lock().await;
    let bounds = store.event_bounds()?;
    let after = bounds
        .hi
        .saturating_sub(REPLAY_SEQUENCE_WINDOW)
        .max(bounds.pruned_through);
    let events = store.recent_native_tool_completions(after, bounds.hi, REPLAY_TOOL_EVENT_LIMIT)?;
    Ok((bounds.hi, events))
}

async fn replay_recent(router: &Router, cursor: &mut i64) -> Result<()> {
    let (high_water, events) = recent_event_tail(router).await?;
    for event in events {
        if event.seq > *cursor {
            process_event(router, &event).await?;
            *cursor = event.seq;
        }
    }
    *cursor = (*cursor).max(high_water);
    Ok(())
}

async fn process_event(router: &Router, event: &Event) -> Result<()> {
    if event.kind != "tool.completed" || event.native_id.is_none() {
        return Ok(());
    }
    let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
        return Ok(());
    };
    let Some(turn_id) = event
        .data
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .filter(|turn| !turn.trim().is_empty())
    else {
        return Ok(());
    };
    if event.data.get("status").and_then(serde_json::Value::as_str) != Some("error") {
        return Ok(());
    }
    if !router.inner.attention.repeated_command_failure.enabled
        || router.inner.mode.load(std::sync::atomic::Ordering::SeqCst) != MODE_OPEN
    {
        return Ok(());
    }

    let (evidence, point) = {
        let store = router.inner.store.lock().await;
        let Some(current) = store.get_agent(agent)? else {
            return Ok(());
        };
        if !active_turn_matches(&current, generation, turn_id) {
            return Ok(());
        }
        (
            store.repeated_command_failure_evidence(agent, generation, turn_id, event.seq)?,
            super::attention_inject_point(&current),
        )
    };
    let Some((command, output, failures)) = evidence else {
        return Ok(());
    };
    if failures < FAILURE_LIMIT {
        return Ok(());
    }

    let policy = router.secret_policy_snapshot();
    let command_excerpt = single_line_excerpt(&policy, &command, MESSAGE_COMMAND_BYTES);
    let output_excerpt = single_line_excerpt(&policy, &output, MESSAGE_OUTPUT_BYTES);
    if command_excerpt.is_empty() || output_excerpt.is_empty() {
        return Ok(());
    }
    let message = format!(
        "The same command failed three times: {command_excerpt}\n\
         Same failure output: {output_excerpt}. What will you check or change?"
    );
    let incident_id = repeated_failure_id(agent, generation, turn_id, &command, &output);
    let inject_id = repeated_failure_ulid(&incident_id, "inject", event.ts);
    let attention_id = repeated_failure_ulid(&incident_id, "attention", event.ts);
    let step = AttentionStep {
        incident_id: incident_id.clone(),
        step: "question".to_owned(),
        outcome: if point.is_some() {
            "unknown"
        } else {
            "unsupported"
        }
        .to_owned(),
        command_id: point.map(|_| inject_id.clone()),
        turn_id: Some(turn_id.to_owned()),
    };

    {
        let mut store = router.inner.store.lock().await;
        let Some(current) = store.get_agent(agent)? else {
            return Ok(());
        };
        if !active_turn_matches(&current, generation, turn_id) {
            return Ok(());
        }
        let Some(step_event) =
            store.claim_attention_step(agent, generation, &step, crate::common::now_ms())?
        else {
            return Ok(());
        };
        router.publish_locked(&[step_event]);
    }

    let report = serde_json::json!({
        "schema": 1,
        "incident_id": incident_id,
        "phase": "incident",
        "reason": "repeated_command_failure",
        "agent_id": agent,
        "generation": generation,
        "turn_id": turn_id,
        "observed_at": event.ts,
        "attention_step": step,
        "message": message,
    });
    let args = serde_json::to_value(EmitDefinitionArgs {
        topic: "agent.attention".to_owned(),
        data: report,
        key: None,
    })
    .map_err(|_| Error::new("internal", "attention report could not be encoded"))?;
    router
        .submit_attention_event(&serde_json::json!({
            "id": attention_id,
            "verb": "attention.emit",
            "args": args
        }))
        .await?;
    wait_for_attention_report(router, &attention_id).await?;
    if point.is_some() {
        router
            .queue_fact_note_with_id(
                event,
                "repeated_command_failure",
                &message,
                Some(&inject_id),
            )
            .await?;
    }
    Ok(())
}

fn active_turn_matches(agent: &Agent, generation: i64, turn_id: &str) -> bool {
    !agent.archived
        && !agent.observe_only
        && agent.generation == generation
        && agent.active_turn.as_deref() == Some(turn_id)
        && matches!(agent.state, AgentState::Busy | AgentState::Waiting)
}

fn single_line_excerpt(policy: &SecretPolicy, text: &str, max_bytes: usize) -> String {
    let redacted = policy.redact_bounded(text, max_bytes);
    let mut output = String::with_capacity(redacted.len());
    let mut space = false;
    for character in redacted.chars() {
        if character.is_whitespace() || character.is_control() {
            space = !output.is_empty();
            continue;
        }
        if space && output.len() < max_bytes {
            output.push(' ');
            space = false;
        }
        if output.len().saturating_add(character.len_utf8()) > max_bytes {
            break;
        }
        output.push(character);
    }
    output
}

fn repeated_failure_id(
    agent: &str,
    generation: i64,
    turn_id: &str,
    command: &str,
    output: &str,
) -> String {
    let mut digest = Sha256::new();
    for part in [
        "repeated-command-failure",
        agent,
        &generation.to_string(),
        turn_id,
        command,
        output,
    ] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    format!("repeated-command-failure-{}", lower_hex(&digest.finalize()))
}

fn repeated_failure_ulid(incident_id: &str, purpose: &str, at_ms: i64) -> String {
    let mut digest = Sha256::new();
    for part in ["repeated-command-failure-command", incident_id, purpose] {
        digest.update((part.len() as u64).to_le_bytes());
        digest.update(part.as_bytes());
    }
    let hash = digest.finalize();
    let mut random = [0; 16];
    random.copy_from_slice(&hash[..16]);
    let random = u128::from_be_bytes(random) & ((1u128 << 80) - 1);
    let timestamp = u64::try_from(at_ms).unwrap_or(1).min((1u64 << 48) - 1);
    ulid::Ulid::from_parts(timestamp, random).to_string()
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

async fn wait_for_attention_report(router: &Router, command_id: &str) -> Result<()> {
    let mut progress = router.progress_receiver();
    timeout(REPORT_SETTLEMENT_TIMEOUT, async {
        loop {
            let command = {
                let store = router.inner.store.lock().await;
                store.get_command(command_id)?
            };
            let Some(command) = command else {
                return Err(Error::new(
                    "not_found",
                    "attention report command was not retained",
                ));
            };
            match command.state {
                CommandState::Applied => return Ok(()),
                CommandState::Rejected
                | CommandState::Failed
                | CommandState::Cancelled
                | CommandState::Unknown => {
                    return Err(Error::new(
                        "unknown",
                        "attention report did not reach a committed state",
                    ));
                }
                _ => {}
            }
            progress
                .changed()
                .await
                .map_err(|_| Error::new("unknown", "attention report worker stopped"))?;
        }
    })
    .await
    .map_err(|_| Error::new("busy", "attention report settlement deadline elapsed"))?
}
