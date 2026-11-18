use super::{CapturedAgent, Router};
use crate::common::now_ms;
use eliot_core::api::SourceState;
use eliot_core::{Agent, Result, TurnOutcome};
use eliot_store::ActiveTurnRecovery;
use serde_json::{Value, json};
use std::time::Duration;

const RETRY_DELAYS: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
];

fn can_recover_active_turn(agent: &Agent, sources: &[SourceState]) -> bool {
    // These adapters implement exact native turn evidence in agent.snapshot.
    matches!(agent.harness.as_str(), "codex" | "muse" | "opencode")
        && agent.active_turn.is_some()
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

pub(super) async fn recover_active_turn(router: &Router, captured: &CapturedAgent) -> Result<bool> {
    let agent = &captured.agent;
    if !can_recover_active_turn(agent, &captured.source_states) {
        return Ok(false);
    }
    let mut binding = None;
    for delay in RETRY_DELAYS {
        if !delay.is_zero() {
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
        let snapshot = router
            .inner
            .plugin_host
            .probe_snapshot(agent, Some((&current.command_id, &current.turn_id)))
            .await;
        if let Ok(snapshot) = snapshot {
            match readback(agent, &current, &snapshot) {
                Readback::Running => return Ok(false),
                Readback::Terminal(mut terminal) => {
                    router.secret_policy_snapshot().redact_values(&mut terminal);
                    return settle(router, agent, &current, Some(terminal)).await;
                }
                Readback::Inconclusive => {}
            }
        }
        binding = Some(current);
    }
    if let Some(binding) = binding {
        return settle(router, agent, &binding, None).await;
    }
    Ok(false)
}

async fn settle(
    router: &Router,
    agent: &Agent,
    binding: &ActiveTurnRecovery,
    terminal: Option<Value>,
) -> Result<bool> {
    let mut store = router.inner.store.lock().await;
    let events = store.settle_recovered_turn(agent, binding, terminal, now_ms())?;
    if events.is_empty() {
        return Ok(false);
    }
    router.publish_locked(&events);
    router.signal_progress();
    Ok(true)
}

enum Readback {
    Running,
    Terminal(Value),
    Inconclusive,
}

fn readback(agent: &Agent, binding: &ActiveTurnRecovery, snapshot: &Value) -> Readback {
    let Some(evidence) = snapshot["turn_evidence"].as_str().filter(|value| {
        !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
    }) else {
        return Readback::Inconclusive;
    };
    if snapshot["agent_id"].as_str() != Some(agent.id.as_str())
        || snapshot["generation"].as_i64() != Some(agent.generation)
        || snapshot["command_id"].as_str() != Some(binding.command_id.as_str())
        || agent.native_ref.is_none()
        || snapshot["native_ref"].as_str() != agent.native_ref.as_deref()
        || snapshot["native_scope"].as_str() != agent.native_scope.as_deref()
    {
        return Readback::Inconclusive;
    }
    let turn = &snapshot["last_turn"];
    if turn["turn_id"].as_str() != Some(binding.turn_id.as_str())
        || turn["command_id"].as_str() != Some(binding.command_id.as_str())
    {
        return Readback::Inconclusive;
    }
    let state = snapshot["state"].as_str();
    let turn_status = turn["status"].as_str();
    if matches!(state, Some("busy" | "running" | "waiting"))
        && snapshot["active_turn"].as_str() == Some(binding.turn_id.as_str())
        && turn["terminal"].as_bool() == Some(false)
        && matches!(
            turn_status,
            Some("inProgress" | "running" | "busy" | "waiting")
        )
    {
        return Readback::Running;
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
    if let Some(message) = turn["native_error"]
        .as_str()
        .or_else(|| turn["native_error"]["message"].as_str())
    {
        data["native_error"] = json!({"message": message});
    }
    Readback::Terminal(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_source_gap_only_enters_recovery_with_native_turn_readback() {
        let sources = [SourceState {
            agent_id: "a1".to_owned(),
            generation: 1,
            source: "stream".to_owned(),
            families: vec!["turn".to_owned()],
            owners: Vec::new(),
            state: "open".to_owned(),
            health: "stale".to_owned(),
            cursor: None,
            last_store_seq: None,
            last_batch_id: None,
            last_batch_first_seq: None,
            last_batch_last_seq: None,
            gap_id: Some("tool-gap".to_owned()),
            gap_from: None,
            gap_to: None,
            gap_seq: Some(3),
        }];
        for (harness, expected) in [
            ("claude", false),
            ("antigravity", false),
            ("codex", true),
            ("muse", true),
            ("opencode", true),
        ] {
            let mut agent = Agent::new("a1", "project", harness, "profile", 0);
            agent.generation = 1;
            agent.state = eliot_core::AgentState::Busy;
            agent.active_turn = Some("turn-1".to_owned());
            assert_eq!(
                can_recover_active_turn(&agent, &sources),
                expected,
                "{harness}"
            );
        }
    }
}
