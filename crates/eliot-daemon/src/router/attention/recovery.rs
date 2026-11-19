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
/// stays busy still settles.
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
        match snapshot {
            Ok(snapshot) => {
                match readback(agent, &current.command_id, &current.turn_id, &snapshot) {
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
    // failure clears the mark for the next sweep. Checked under the Store
    // lock, so a write that already committed fails the Store recheck.
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
    // for the next sweep. Recovery never races the adapter's terminal.
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
    // The adapter saw the reply but it exceeded the event bound: the
    // recovered terminal keeps that reason, as the adapter's own write does.
    if turn["final_text_unavailable"].as_str() == Some("event_budget") {
        data["final_text_unavailable"] = json!("event_budget");
    }
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
    use crate::plugin_host::{PluginClient, PluginConnection};
    use crate::router::tests::{result_fixture, router};
    use eliot_core::{Error, SourceGap, TurnState};
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;
    use tokio::time::Instant;

    const WINDOW: Duration = Duration::from_secs(900);

    fn stale_turn_source() -> SourceState {
        SourceState {
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
        }
    }

    fn busy_agent(harness: &str) -> Agent {
        let mut agent = Agent::new("a1", "project", harness, "profile", 0);
        agent.generation = 1;
        agent.state = eliot_core::AgentState::Busy;
        agent.active_turn = Some("turn-1".to_owned());
        agent.native_scope = Some("scope".to_owned());
        agent.native_ref = Some("session".to_owned());
        agent
    }

    fn read(agent: &Agent, snapshot: &Value) -> Readback {
        readback(agent, "send-1", "turn-1", snapshot)
    }

    fn snapshot(state: &str, active_turn: Value, last_turn: Value, evidence: &str) -> Value {
        json!({
            "agent_id": "a1",
            "generation": 1,
            "command_id": "send-1",
            "state": state,
            "native_scope": "scope",
            "native_ref": "session",
            "active_turn": active_turn,
            "last_turn": last_turn,
            "turn_evidence": evidence,
        })
    }

    #[test]
    fn every_harness_enters_recovery_on_a_turn_source_gap() {
        let sources = [stale_turn_source()];
        for harness in ["claude", "antigravity", "codex", "muse", "opencode"] {
            assert!(
                can_recover_active_turn(&busy_agent(harness), &sources),
                "{harness}"
            );
        }
        let mut idle = busy_agent("claude");
        idle.active_turn = None;
        assert!(!can_recover_active_turn(&idle, &sources));
    }

    #[test]
    fn stream_adapter_readbacks_keep_running_settle_terminal_or_exit() {
        let agent = busy_agent("antigravity");
        let running = snapshot(
            "busy",
            json!("turn-1"),
            json!({"command_id":"send-1","turn_id":"turn-1","status":"running","terminal":false}),
            "acp session/prompt pending",
        );
        assert!(matches!(read(&agent, &running), Readback::Running));

        let terminal = snapshot(
            "idle",
            Value::Null,
            json!({"command_id":"send-1","turn_id":"turn-1","status":"interrupted",
                "native_status":"cancelled","terminal":true,"final_text":"done"}),
            "acp session/prompt stopReason",
        );
        let Readback::Terminal(data) = read(&agent, &terminal) else {
            panic!("terminal readback");
        };
        assert_eq!(data["outcome"], json!(TurnOutcome::Interrupted));
        assert_eq!(data["native_status"], "cancelled");

        let mut exited = snapshot(
            "lost",
            Value::Null,
            json!({"command_id":"send-1","turn_id":"turn-1","status":"lost","terminal":false}),
            "native exit before result",
        );
        assert!(matches!(read(&agent, &exited), Readback::Inconclusive));
        exited["process_exited"] = json!(true);
        assert!(matches!(read(&agent, &exited), Readback::Exited));
        // Another turn's facts never settle the bound turn.
        exited["last_turn"]["turn_id"] = json!("turn-0");
        assert!(matches!(read(&agent, &exited), Readback::Inconclusive));

        let mut unproven = running;
        unproven["turn_evidence"] = Value::Null;
        assert!(matches!(read(&agent, &unproven), Readback::Inconclusive));
    }

    #[test]
    fn a_terminal_whose_adapter_write_is_in_flight_is_never_settled() {
        let agent = busy_agent("claude");
        let mut terminal = snapshot(
            "idle",
            Value::Null,
            json!({"command_id":"send-1","turn_id":"turn-1","status":"completed",
                "native_status":"completed","terminal":true,"write_pending":true}),
            "stream-json result",
        );
        assert!(matches!(read(&agent, &terminal), Readback::Running));
        // The write resolved without committing: the retained terminal settles.
        terminal["last_turn"]["write_pending"] = json!(false);
        let Readback::Terminal(data) = read(&agent, &terminal) else {
            panic!("terminal readback");
        };
        assert_eq!(data["outcome"], json!(TurnOutcome::Success));
    }

    #[test]
    fn a_recovered_oversized_reply_keeps_its_unavailable_reason() {
        let agent = busy_agent("claude");
        let terminal = snapshot(
            "idle",
            Value::Null,
            json!({"command_id":"send-1","turn_id":"turn-1","status":"completed",
                "native_status":"success","terminal":true,
                "final_text_unavailable":"event_budget"}),
            "stream-json result",
        );
        let Readback::Terminal(data) = read(&agent, &terminal) else {
            panic!("terminal readback");
        };
        assert_eq!(data["final_text_unavailable"], "event_budget");
        assert!(data["final_text"].is_null());
    }

    /// A busy Agent whose `turn-1` has a committed turn-source gap.
    async fn gapped_turn(router: &Router) -> (String, String) {
        let mut store = router.inner.store.lock().await;
        let (agent, send_id) = result_fixture(&mut store);
        store
            .record_source_gap(
                SourceGap {
                    gap_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    families: vec!["turn".to_owned()],
                    reason: "fixture turn source gap".to_owned(),
                    from: None,
                    to: None,
                },
                now_ms(),
            )
            .unwrap();
        (agent, send_id)
    }

    async fn capture(router: &Router, agent: &str) -> CapturedAgent {
        super::super::capture_agent(router, agent, 1, WINDOW)
            .await
            .unwrap()
            .unwrap()
    }

    async fn turn_state(router: &Router, agent: &str) -> TurnState {
        let store = router.inner.store.lock().await;
        store.get_turn(agent, 1, "turn-1").unwrap().unwrap().state
    }

    fn closed_client() -> PluginClient {
        PluginConnection::new(tokio::io::empty(), tokio::io::sink()).client()
    }

    #[tokio::test]
    async fn busy_reads_defer_without_spending_the_budget_until_progress_is_stale() {
        let router = router();
        let (agent, _) = gapped_turn(&router).await;
        let reads = router
            .inner
            .plugin_host
            .test_snapshot_runtime(&agent, closed_client(), || {
                Err(Error::new(
                    "busy",
                    "Agent status snapshot is already in flight",
                ))
            });
        let mut captured = capture(&router, &agent).await;
        // Committed progress within the window: one busy read defers.
        assert!(
            !recover_active_turn(&router, &captured, WINDOW)
                .await
                .unwrap()
        );
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(turn_state(&router, &agent).await, TurnState::Running);
        // No committed progress through the window: busy reads count, so a
        // source that stays busy still settles after the bounded reads.
        captured.progress_basis_at = Some(now_ms() - 901_000);
        assert!(
            recover_active_turn(&router, &captured, WINDOW)
                .await
                .unwrap()
        );
        assert_eq!(reads.load(Ordering::Relaxed), 5);
        assert_eq!(turn_state(&router, &agent).await, TurnState::Unknown);
    }

    #[tokio::test]
    async fn no_settlement_while_the_turn_terminal_write_is_in_flight() {
        let router = router();
        let (agent, send_id) = gapped_turn(&router).await;
        let (host_io, peer_io) = tokio::io::duplex(16 * 1024);
        let (host_read, host_write) = tokio::io::split(host_io);
        let mut connection = PluginConnection::new(host_read, host_write);
        let client = connection.client();
        // A drained-exit readback settles at once, without retry delays.
        let lost = json!({
            "agent_id": agent, "generation": 1, "command_id": send_id,
            "state": "lost", "process_exited": true, "active_turn": null,
            "native_scope": "fixture-scope", "native_ref": format!("fixture-ref-{agent}"),
            "last_turn": {"command_id": send_id, "turn_id": "turn-1",
                "status": "lost", "terminal": false},
            "turn_evidence": "native exit before result",
        });
        let reads =
            router
                .inner
                .plugin_host
                .test_snapshot_runtime(&agent, client.clone(), move || Ok(lost.clone()));
        // The adapter's own turn.completed reached the host, queued for intake.
        let (_peer_read, mut peer_write) = tokio::io::split(peer_io);
        let frame = json!({"jsonrpc":"2.0", "id":"p-1", "method":"events.put", "params":{
            "credential":"fixture", "agent_id":agent, "generation":1, "source":"stream",
            "batch_id":"p-1", "events":[{"kind":"turn.completed", "native_id":"p-1",
                "data":{"command_id":send_id, "turn_id":"turn-1", "status":"completed"}}]}});
        peer_write
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        while !client.terminal_write_in_flight(&agent, 1, "turn-1") {
            assert!(Instant::now() < until, "terminal write was never marked");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let captured = capture(&router, &agent).await;
        assert!(
            !recover_active_turn(&router, &captured, WINDOW)
                .await
                .unwrap()
        );
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(turn_state(&router, &agent).await, TurnState::Running);
        // Intake finished that write without committing it: the next sweep
        // settles from the same readback.
        assert_eq!(connection.next_request().await.unwrap().id, json!("p-1"));
        drop(connection);
        assert!(
            recover_active_turn(&router, &captured, WINDOW)
                .await
                .unwrap()
        );
        assert_eq!(turn_state(&router, &agent).await, TurnState::Unknown);
    }
}
