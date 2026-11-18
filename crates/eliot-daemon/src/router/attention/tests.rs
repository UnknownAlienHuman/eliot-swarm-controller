//! Router-level attention regressions: a sweep that meets a lost Agent must
//! report the deterministic loss identity (never a generic incident id), and
//! saturated re-paging must resume at the first unqueued loss instead of
//! restarting at the newest row.

use super::super::Router;
use super::super::tests::router;
use super::incidents::{loss_incident_id, loss_report_already_complete};
use super::sweep::{
    MAX_PENDING_CHECKS, MAX_PENDING_LOSSES, promote_pending_losses, queue_startup_losses,
};
use super::{AgentKey, AttentionState, IncidentKey, IncidentKind, Trigger, inspect_agent};
use crate::common::now_ms;
use eliot_core::{Agent, AgentState, CommandState, SubmitCommand};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SUSPICION_WINDOW: Duration = Duration::from_secs(900);

/// One controlled launch, dispatched, then fenced as lost by a restart: the
/// command reads back unknown and the Agent lost with retained `agent.lost`
/// evidence. Returns the Agent id and its current generation.
async fn seed_lost_agent(router: &Router, project: &str, now: i64) -> (String, i64) {
    let mut store = router.inner.store.lock().await;
    let admitted = store
        .submit(
            "owner",
            SubmitCommand {
                id: ulid::Ulid::new().to_string(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace",
                    "harness":"codex", "profile":"synthetic", "profile_snapshot":{}}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit launch");
    let agent = admitted.command.target.expect("launch target");
    store
        .dispatch(&admitted.command.id, now)
        .expect("dispatch launch");
    store.recover_on_restart(now + 1).expect("restart recovery");
    assert_eq!(
        store
            .get_command(&admitted.command.id)
            .unwrap()
            .unwrap()
            .state,
        CommandState::Unknown
    );
    let current = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(current.state, AgentState::Lost);
    (agent, current.generation)
}

fn loss_key(agent: &str, generation: i64) -> IncidentKey {
    IncidentKey {
        agent: agent.to_owned(),
        generation,
        kind: IncidentKind::SourceUnhealthy,
        turn_id: None,
    }
}

/// Settled delivery is asynchronous behind the inspect call: the emit waits
/// on a worker and the notices settle after it. Poll the same durable
/// predicate a restart re-report uses.
async fn poll_loss_report_complete(router: &Router, incident_id: &str, agent: &Agent) -> bool {
    for _ in 0..100 {
        if loss_report_already_complete(router, incident_id, agent)
            .await
            .expect("re-report check")
        {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test]
async fn sweep_of_lost_agent_reports_the_deterministic_loss_identity() {
    let router = router();
    let now = now_ms();
    let (agent_id, generation) = seed_lost_agent(&router, "loss-identity", now).await;
    let seq = {
        let store = router.inner.store.lock().await;
        store
            .latest_lost_event_seq(&agent_id, generation)
            .expect("read loss sequence")
            .expect("loss evidence is retained")
    };
    let expected = loss_incident_id(&agent_id, generation, seq);

    // A generic sweep meets the lost Agent first: it must still report the
    // deterministic loss identity instead of minting a random incident id.
    let state = Arc::new(Mutex::new(AttentionState::default()));
    inspect_agent(
        &router,
        Arc::clone(&state),
        &agent_id,
        generation,
        SUSPICION_WINDOW,
        Trigger::Sweep,
    )
    .await
    .expect("sweep inspect of a lost Agent");
    let incident_id = state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .active
        .get(&loss_key(&agent_id, generation))
        .expect("loss incident under its stable key")
        .incident_id
        .clone();
    assert_eq!(
        incident_id, expected,
        "a sweep must not fork the loss identity"
    );

    // The report reached incident delivery end to end: the retained incident
    // event and every expected notice are durable. Notice settlement runs
    // on async workers behind the inspect call, so poll for it.
    let agent = {
        let store = router.inner.store.lock().await;
        store.get_agent(&agent_id).unwrap().unwrap()
    };
    assert!(
        poll_loss_report_complete(&router, &expected, &agent).await,
        "loss report must reach incident delivery"
    );

    // A restart re-report through the exact trigger maps to the same
    // incident and stays a no-op: no second identity, no duplicate delivery.
    let restarted = Arc::new(Mutex::new(AttentionState::default()));
    inspect_agent(
        &router,
        Arc::clone(&restarted),
        &agent_id,
        generation,
        SUSPICION_WINDOW,
        Trigger::AgentLost { event_seq: seq },
    )
    .await
    .expect("restart re-report of the same loss");
    assert!(
        !restarted
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .contains_key(&loss_key(&agent_id, generation)),
        "a completed re-report must not open a second incident"
    );
}

#[tokio::test]
async fn saturated_reconcile_repages_losses_from_the_first_unqueued_row() {
    let router = router();
    let now = now_ms();
    // More losses than one startup page holds.
    const LOSSES: usize = 40;
    let mut seeded = Vec::new();
    {
        let mut store = router.inner.store.lock().await;
        for i in 0..LOSSES {
            let admitted = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"loss-page", "workspace":"synthetic-workspace",
                            "harness":"codex", "profile":"synthetic", "profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now + i as i64,
                )
                .expect("admit loss launch");
            seeded.push(admitted.command.target.expect("launch target"));
            store
                .dispatch(&admitted.command.id, now + i as i64)
                .expect("dispatch loss launch");
        }
        store
            .recover_on_restart(now + 100_000)
            .expect("restart recovery");
    }
    let mut expected = std::collections::HashMap::new();
    {
        let store = router.inner.store.lock().await;
        for agent in &seeded {
            let current = store.get_agent(agent).unwrap().unwrap();
            assert_eq!(current.state, AgentState::Lost);
            let seq = store
                .latest_lost_event_seq(agent, current.generation)
                .expect("read loss sequence")
                .expect("loss evidence is retained");
            expected.insert(agent.clone(), (current.generation, seq));
        }
    }
    // Oldest loss in store order (newest first): it must not starve while
    // newer rows refill freed slots.
    let oldest = {
        let store = router.inner.store.lock().await;
        let mut losses = store
            .startup_lost_agents(LOSSES + 1, None)
            .expect("list losses");
        losses.retain(|loss| seeded.contains(&loss.agent));
        losses.last().expect("oldest loss").agent.clone()
    };

    let state = Arc::new(Mutex::new(AttentionState::default()));
    {
        let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
        for n in 0..MAX_PENDING_CHECKS {
            guard.pending_checks.insert(
                AgentKey {
                    agent: format!("q{n}"),
                    generation: 1,
                },
                Trigger::Sweep,
            );
        }
        for n in 0..(MAX_PENDING_LOSSES - 12) {
            guard.pending_losses.push_back((
                AgentKey {
                    agent: format!("l{n}"),
                    generation: 1,
                },
                n as i64,
            ));
        }
    }
    queue_startup_losses(&router, &state).await;
    // Only 12 slots were free: the 12 newest losses parked with their exact
    // identity, the cursor stopped at the last queued row, and the drop is
    // recorded as overflow (retried from the Store) rather than degrading
    // to a generic sweep.
    {
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
        assert_eq!(guard.pending_losses.len(), MAX_PENDING_LOSSES);
        assert!(guard.losses_overflow);
        assert!(!guard.reconcile_all);
        guard.loss_page_cursor.expect("resume cursor");
    }
    assert_not_queued(&state, &oldest);

    // The next reconcile promotes the queued losses into the freed check
    // map (evicting one sweep each) and re-pages from the cursor: the pass
    // resumes at the first unqueued row instead of restarting at the newest.
    {
        let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
        guard.pending_checks.clear();
        guard.losses_overflow = false;
    }
    promote_pending_losses(&state);
    {
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(guard.pending_losses.len(), 0);
        // The 500 dummy losses and the 12 real ones all fit the freed map.
        assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
        // Room was genuinely free: no sweep was evicted to make it.
        assert!(!guard.reconcile_all);
    }
    queue_startup_losses(&router, &state).await;
    {
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(guard.loss_page_cursor, None);
        assert!(!guard.losses_overflow);
        for (agent, (generation, seq)) in &expected {
            let key = AgentKey {
                agent: agent.clone(),
                generation: *generation,
            };
            let queued = guard
                .pending_checks
                .get(&key)
                .and_then(|trigger| match trigger {
                    Trigger::AgentLost { event_seq } => Some(*event_seq),
                    _ => None,
                })
                .or_else(|| {
                    guard
                        .pending_losses
                        .iter()
                        .find_map(|(pending, event_seq)| (pending == &key).then_some(*event_seq))
                });
            assert_eq!(
                queued,
                Some(*seq),
                "loss for {agent} re-pages with its exact identity"
            );
        }
    }
}

fn assert_not_queued(state: &Arc<Mutex<AttentionState>>, agent: &str) {
    let guard = state.lock().unwrap_or_else(|error| error.into_inner());
    let present = guard.pending_checks.keys().any(|key| key.agent == *agent)
        || guard
            .pending_losses
            .iter()
            .any(|(key, _)| key.agent == *agent);
    assert!(!present, "oldest loss {agent} is not yet queued");
}
