//! Router-level attention regressions: a sweep that meets a lost Agent must
//! report the deterministic loss identity (never a generic incident id), and
//! saturated re-paging must resume at the first unqueued loss instead of
//! restarting at the newest row.

use super::super::Router;
use super::super::tests::router;
use super::incidents::{freeze_report, loss_incident_id, loss_report_already_complete};
use super::sweep::{
    MAX_PENDING_CHECKS, MAX_PENDING_LOSSES, promote_pending_losses, queue_startup_losses,
};
use super::{
    ActiveIncident, AgentKey, AttentionReport, AttentionState, IncidentKey, IncidentKind,
    NativeStatus, ProcessObservation, ReportPhase, Trigger, inspect_agent, inspect_lost_agent,
};
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
    // The sweep must report the deterministic loss identity (checked
    // durably below), and the delivered loss must leave the transient set
    // at once instead of occupying a reservation slot.
    assert!(
        !state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .contains_key(&loss_key(&agent_id, generation)),
        "a delivered loss must not be retained in the active set"
    );
    assert!(
        router
            .inner
            .store
            .lock()
            .await
            .attention_incident_exists(&expected)
            .expect("incident lookup"),
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

/// A delivered loss report releases its reservation slot: one slot more
/// than the 512-incident bound must still all deliver instead of leaving
/// the last loss without an incident.
#[tokio::test]
async fn delivered_losses_release_slots_for_all_retained_losses() {
    let router = router();
    // Seed in the recent past: the deterministic loss mutation ids derive
    // from the retained loss timestamps, and future-dated ids are rejected
    // at admission.
    let base = now_ms() - 120_000;
    // One past the reservation bound.
    const LOSSES: usize = 513;
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
                        args: json!({"project":"loss-capacity", "workspace":"synthetic-workspace",
                            "harness":"codex", "profile":"synthetic", "profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    base + i as i64,
                )
                .expect("admit loss launch");
            seeded.push(admitted.command.target.expect("launch target"));
            store
                .dispatch(&admitted.command.id, base + i as i64)
                .expect("dispatch loss launch");
        }
        store
            .recover_on_restart(base + LOSSES as i64 + 1)
            .expect("restart recovery");
    }
    let mut losses = Vec::new();
    {
        let store = router.inner.store.lock().await;
        for agent in &seeded {
            let current = store.get_agent(agent).unwrap().unwrap();
            assert_eq!(current.state, AgentState::Lost);
            let seq = store
                .latest_lost_event_seq(agent, current.generation)
                .expect("read loss sequence")
                .expect("loss evidence is retained");
            losses.push((agent.clone(), current.generation, seq));
        }
    }
    // One shared transient set, so every inspect competes for the same
    // reservation bound a daemon restart would fill.
    let state = Arc::new(Mutex::new(AttentionState::default()));
    for (index, (agent, generation, seq)) in losses.iter().enumerate() {
        if let Err(error) = inspect_lost_agent(
            &router,
            Arc::clone(&state),
            agent,
            *generation,
            *seq,
            SUSPICION_WINDOW,
        )
        .await
        {
            panic!("loss {index} ({agent}) inspect failed: {error:?}");
        }
        let expected = loss_incident_id(agent, *generation, *seq);
        let exists = {
            router
                .inner
                .store
                .lock()
                .await
                .attention_incident_exists(&expected)
                .expect("incident lookup")
        };
        assert!(
            exists,
            "loss for {agent} must persist past 512 retained losses"
        );
    }
    // Delivered losses drop from the transient set instead of occupying it.
    assert!(
        state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .is_empty(),
        "delivered losses must release their reservation slots"
    );
    // Every expected notice settles end to end. Poll once globally: notice
    // settlement runs on async workers behind the inspect calls.
    let agents = {
        let store = router.inner.store.lock().await;
        losses
            .iter()
            .map(|(agent, _, _)| store.get_agent(agent).unwrap().unwrap())
            .collect::<Vec<_>>()
    };
    let mut incomplete = Vec::new();
    for _ in 0..300 {
        incomplete.clear();
        for ((agent, generation, seq), record) in losses.iter().zip(agents.iter()) {
            let expected = loss_incident_id(agent, *generation, *seq);
            if !loss_report_already_complete(&router, &expected, record)
                .await
                .expect("re-report check")
            {
                incomplete.push(expected);
            }
        }
        if incomplete.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        incomplete.is_empty(),
        "all 513 loss reports must reach incident delivery"
    );
}

/// A pre-existing generic `SourceUnhealthy` incident under the loss key must
/// not swallow the loss identity: the deterministic loss report is persisted
/// whether or not the generic incident still has a pending delivery, and a
/// restart re-report under the same identity stays a no-op.
#[tokio::test]
async fn existing_source_incident_does_not_swallow_loss_identity() {
    for pending in [false, true] {
        let router = router();
        let now = now_ms();
        let (agent_id, generation) = seed_lost_agent(&router, "loss-supersede", now).await;
        let seq = {
            let store = router.inner.store.lock().await;
            store
                .latest_lost_event_seq(&agent_id, generation)
                .expect("read loss sequence")
                .expect("loss evidence is retained")
        };
        let expected = loss_incident_id(&agent_id, generation, seq);
        let agent = {
            let store = router.inner.store.lock().await;
            store.get_agent(&agent_id).unwrap().unwrap()
        };
        // Plant the generic incident the loss key collides with.
        let state = Arc::new(Mutex::new(AttentionState::default()));
        {
            let generic = ActiveIncident {
                incident_id: format!("attention-generic-placeholder-{pending}"),
                baseline_progress_at: None,
                baseline_model_progress_at: None,
                incident_delivery: pending
                    .then(|| freeze_report(dummy_generic_source_report(&agent))),
                recovery_delivery: None,
                is_loss: false,
            };
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active
                .insert(loss_key(&agent_id, generation), generic);
        }
        inspect_lost_agent(
            &router,
            Arc::clone(&state),
            &agent_id,
            generation,
            seq,
            SUSPICION_WINDOW,
        )
        .await
        .expect("loss inspect with a generic incident present");
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .attention_incident_exists(&expected)
                .expect("incident lookup"),
            "loss identity must persist with a generic incident present (pending={pending})"
        );
        assert!(
            poll_loss_report_complete(&router, &expected, &agent).await,
            "loss report must reach incident delivery (pending={pending})"
        );
        // A restart clears the transient set; the re-report must find the
        // same deterministic identity instead of opening a second one.
        let restarted = Arc::new(Mutex::new(AttentionState::default()));
        inspect_lost_agent(
            &router,
            Arc::clone(&restarted),
            &agent_id,
            generation,
            seq,
            SUSPICION_WINDOW,
        )
        .await
        .expect("restart re-report of the same loss");
        assert!(
            !restarted
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active
                .contains_key(&loss_key(&agent_id, generation)),
            "restart re-report must not open a second incident (pending={pending})"
        );
    }
}

/// Stand-in generic report so a planted incident can carry a pending
/// delivery. It binds the real Agent/generation so the trusted commit
/// accepts it as current evidence.
fn dummy_generic_source_report(agent: &Agent) -> AttentionReport {
    AttentionReport {
        schema: 1,
        incident_id: "attention-generic-placeholder".to_owned(),
        phase: ReportPhase::Incident,
        reason: "source_unhealthy".to_owned(),
        agent_id: agent.id.clone(),
        generation: agent.generation,
        agent_state: "lost".to_owned(),
        turn_id: None,
        parent_agent: agent.parent.clone(),
        observed_at: now_ms(),
        suspicion_window_ms: None,
        last_model_progress_at: None,
        progress_basis_at: None,
        progress_age_ms: None,
        source_health: Some(false),
        activity: "state_lost".to_owned(),
        children: eliot_store::NativeChildrenAttention::default(),
        pending_kinds: Vec::new(),
        pending_kinds_truncated: false,
        tool_coverage: "unknown".to_owned(),
        active_tool_count: None,
        sources: Vec::new(),
        sources_truncated: false,
        process_observation: ProcessObservation {
            basis: "test".to_owned(),
            available: false,
            sampled_at: None,
            sample_age_ms: None,
            complete: None,
            coverage: "unknown".to_owned(),
            clock_regressed: None,
            record_count: 0,
            records_truncated: false,
            records: Vec::new(),
            error_code: None,
        },
        native_status: NativeStatus::not_requested(),
        trigger_event_seq: None,
    }
}
