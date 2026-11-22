use super::{CapturedAgent, Router};
use crate::common::now_ms;
use eliot_core::api::SourceState;
use eliot_core::{Agent, Result, TurnOutcome};
use eliot_store::{ActiveTurnRecovery, RecoveredTurn};
use serde_json::{Value, json};
use std::time::Duration;

const RETRY_DELAYS: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

fn can_recover_active_turn(agent: &Agent, sources: &[SourceState]) -> bool {
    // Every native adapter answers an exact-turn agent.snapshot with native
    // turn evidence when it has any; the policy below is harness-neutral.
    agent.active_turn.is_some()
        && sources.iter().any(|source| {
            (source.state == "closed"
                || source.gap_seq.is_some()
                || matches!(source.health.as_str(), "stale" | "error"))
                && source.families.iter().any(|family| {
                    matches!(
                        family.as_str(),
                        "session" | "turn" | "message" | "tool" | "request"
                    )
                })
        })
}

/// A `busy` read (the Agent's one snapshot slot or its queue is occupied,
/// possibly by a read waiting on this turn's own terminal write) proves
/// nothing and spends no read: it defers to the next sweep while the turn
/// has committed progress within the attention suspicion window. Past that
/// window a busy read counts like any inconclusive one, so a source that
/// stays busy still settles; its late terminal then resolves the turn.
fn defers_busy_read(captured: &CapturedAgent, suspicion_window: Duration) -> bool {
    let window_ms = i64::try_from(suspicion_window.as_millis()).unwrap_or(i64::MAX);
    captured
        .progress_basis_at
        .is_some_and(|at| now_ms().saturating_sub(at) < window_ms)
}

pub(super) async fn recover_active_turn(
    router: &Router,
    captured: &CapturedAgent,
    suspicion_window: Duration,
) -> Result<bool> {
    let agent = &captured.agent;
    if !can_recover_active_turn(agent, &captured.source_states) {
        return Ok(false);
    }
    let mut binding = None;
    for delay in RETRY_DELAYS {
        if !delay.is_zero() {
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded wait is owned by the surrounding operation."
            )]
            tokio::time::sleep(delay).await;
        }
        let current = {
            let store = router.inner.store.lock().await;
            let Some(current) = store.get_agent(&agent.id)? else {
                return Ok(false);
            };
            if current.generation != agent.generation
                || current.active_turn != agent.active_turn
                || current.native_scope != agent.native_scope
                || current.native_ref != agent.native_ref
            {
                return Ok(false);
            }
            store.active_turn_recovery(&current)?
        };
        let Some(current) = current else {
            return Ok(false);
        };
        if binding
            .as_ref()
            .is_some_and(|previous| previous != &current)
        {
            // A newer episode gets its own budget on the queued check/sweep.
            return Ok(false);
        }
        let Some(command_id) = current
            .command_id
            .clone()
            .or_else(|| router.inner.plugin_host.session_start_command(agent))
        else {
            binding = Some(current);
            continue;
        };
        let snapshot = router
            .inner
            .plugin_host
            .probe_snapshot(agent, Some((&command_id, &current.turn_id)))
            .await;
        match snapshot {
            Ok(snapshot) => {
                match readback(agent, &command_id, &current.turn_id, &snapshot) {
                    Readback::Running => return Ok(false),
                    Readback::Terminal(mut terminal) => {
                        router.secret_policy_snapshot().redact_values(&mut terminal);
                        return settle(router, agent, &current, RecoveredTurn::Terminal(terminal))
                            .await;
                    }
                    // No terminal can follow a drained exit: settle at once.
                    Readback::Exited => {
                        return settle(router, agent, &current, RecoveredTurn::NativeExited).await;
                    }
                    Readback::Inconclusive => {}
                }
            }
            Err(error) if error.code == "busy" && defers_busy_read(captured, suspicion_window) => {
                return Ok(false);
            }
            Err(_) => {}
        }
        binding = Some(current);
    }
    if let Some(binding) = binding {
        return settle(router, agent, &binding, RecoveredTurn::Inconclusive).await;
    }
    Ok(false)
}

async fn settle(
    router: &Router,
    agent: &Agent,
    binding: &ActiveTurnRecovery,
    outcome: RecoveredTurn,
) -> Result<bool> {
    let mut store = router.inner.store.lock().await;
    // The adapter's own terminal write for this exact turn reached the host
    // and intake has not finished it: that write commits the turn, or its
    // failure clears the mark for the next sweep. This only spares a needless
    // `unknown`; a terminal that commits after a settlement resolves it.
    if router
        .inner
        .plugin_host
        .terminal_write_in_flight(agent, &binding.turn_id)
    {
        return Ok(false);
    }
    let events = store.settle_recovered_turn(agent, binding, outcome, now_ms())?;
    if events.is_empty() {
        return Ok(false);
    }
    router.publish_locked(&events);
    router.signal_progress();
    Ok(true)
}

enum Readback {
    /// The exact turn is still open: running, or its observed terminal is
    /// still being written by the adapter. Kept for the next sweep.
    Running,
    Terminal(Value),
    /// The owned native process exited and its drained stream held no
    /// terminal for the exact turn.
    Exited,
    Inconclusive,
}

fn readback(agent: &Agent, command_id: &str, turn_id: &str, snapshot: &Value) -> Readback {
    let Some(evidence) = snapshot["turn_evidence"].as_str().filter(|value| {
        !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
    }) else {
        return Readback::Inconclusive;
    };
    if snapshot["agent_id"].as_str() != Some(agent.id.as_str())
        || snapshot["generation"].as_i64() != Some(agent.generation)
        || snapshot["command_id"].as_str() != Some(command_id)
        || agent.native_ref.is_none()
        || snapshot["native_ref"].as_str() != agent.native_ref.as_deref()
        || snapshot["native_scope"].as_str() != agent.native_scope.as_deref()
    {
        return Readback::Inconclusive;
    }
    let turn = &snapshot["last_turn"];
    if turn["turn_id"].as_str() != Some(turn_id) || turn["command_id"].as_str() != Some(command_id)
    {
        return Readback::Inconclusive;
    }
    // The adapter observed this terminal but its own write of it is still
    // in flight: that write commits it, or its resolution clears the mark
    // for the next sweep.
    if turn["terminal"].as_bool() == Some(true) && turn["write_pending"].as_bool() == Some(true) {
        return Readback::Running;
    }
    let state = snapshot["state"].as_str();
    let turn_status = turn["status"].as_str();
    if matches!(state, Some("busy" | "running" | "waiting"))
        && snapshot["active_turn"].as_str() == Some(turn_id)
        && turn["terminal"].as_bool() == Some(false)
        && matches!(
            turn_status,
            Some("inProgress" | "running" | "busy" | "waiting")
        )
    {
        return Readback::Running;
    }
    if state == Some("lost")
        && snapshot["process_exited"].as_bool() == Some(true)
        && snapshot.get("active_turn") == Some(&Value::Null)
        && turn["terminal"].as_bool() == Some(false)
    {
        return Readback::Exited;
    }
    if !matches!(state, Some("idle" | "terminal"))
        || snapshot.get("active_turn") != Some(&Value::Null)
        || turn["terminal"].as_bool() != Some(true)
    {
        return Readback::Inconclusive;
    }
    let (status, outcome) = match turn_status {
        Some("completed") => ("completed", TurnOutcome::Success),
        Some("interrupted") => ("interrupted", TurnOutcome::Interrupted),
        Some("failed") => ("failed", TurnOutcome::Failure),
        _ => return Readback::Inconclusive,
    };
    let mut data = json!({
        "status": status,
        "outcome": outcome,
        "native_status": turn["native_status"].as_str().filter(|status| !status.trim().is_empty()).unwrap_or(status),
        "final_text": turn["final_text"].as_str(),
        "turn_evidence": evidence,
    });
    // The adapter knows why no reply text is presented (it exceeded the event
    // bound, or a normal completion supplied none): the recovered terminal
    // keeps that reason, as the adapter's own write does.
    if let Some(marker @ ("event_budget" | "empty_native_response")) =
        turn["final_text_unavailable"].as_str()
    {
        data["final_text_unavailable"] = json!(marker);
    }
    if let Some(message) = turn["native_error"]
        .as_str()
        .or_else(|| turn["native_error"]["message"].as_str())
    {
        data["native_error"] = json!({"message": message});
    }
    Readback::Terminal(data)
}
