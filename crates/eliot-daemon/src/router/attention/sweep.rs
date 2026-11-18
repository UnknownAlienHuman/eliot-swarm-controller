//! Bounded attention sweeps, check queues, and incident reservations.

use super::{
    ActiveIncident, AgentKey, AttentionState, FrozenReport, IncidentKey, Router, Trigger,
    delivery::{persist_and_notify, persist_loss_delivery},
    incidents::retire_superseded_incidents,
    inspect_agent,
};
use eliot_core::Result;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinSet;

const PAGE_SIZE: usize = 128;
const MAX_CHECKS_IN_FLIGHT: usize = 32;
const MAX_SWEEP_CHECKS: usize = 8;
const MAX_RETAINED_INCIDENTS: usize = 512;
pub(super) const MAX_PENDING_CHECKS: usize = 512;
/// Bound for overflowed loss identities parked outside the check map.
pub(super) const MAX_PENDING_LOSSES: usize = 512;
pub(super) const MAX_STARTUP_LOSSES: usize = 32;

pub(super) fn launch_sweep(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    suspicion_window: Duration,
    tasks: &mut JoinSet<()>,
) {
    use std::sync::atomic::Ordering;

    if tasks.len() >= MAX_CHECKS_IN_FLIGHT {
        request_full_reconcile(&state);
        return;
    }
    if running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let router = router.clone();
    tasks.spawn(async move {
        let _reset = SweepReset(running);
        if let Err(error) =
            scan_attention_agents(&router, Arc::clone(&state), suspicion_window).await
        {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.attention_scan_failed",
                &[],
                Some(&error),
            );
        }
        if let Err(error) = reconcile_attention_state(&router, state, suspicion_window).await {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.attention_reconcile_failed",
                &[],
                Some(&error),
            );
        }
    });
}

struct SweepReset(Arc<std::sync::atomic::AtomicBool>);

impl Drop for SweepReset {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub(super) fn launch_check(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    tasks: &mut JoinSet<()>,
    agent: &str,
    generation: i64,
    suspicion_window: Duration,
    trigger: Trigger,
) {
    let key = AgentKey {
        agent: agent.to_owned(),
        generation,
    };
    if tasks.len() >= MAX_CHECKS_IN_FLIGHT || !reserve_check(&state, &key) {
        queue_check(&state, key, trigger);
        return;
    }
    let router = router.clone();
    tasks.spawn(run_check(router, state, key, suspicion_window, trigger));
}

fn reserve_check(state: &Arc<Mutex<AttentionState>>, key: &AgentKey) -> bool {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if state.in_flight.len() >= MAX_CHECKS_IN_FLIGHT || !state.in_flight.insert(key.clone()) {
        return false;
    }
    true
}

fn queue_check(state: &Arc<Mutex<AttentionState>>, key: AgentKey, trigger: Trigger) {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    if let Some(pending) = state.pending_checks.get_mut(&key) {
        *pending = merge_trigger(*pending, trigger);
    } else if state.pending_checks.len() < MAX_PENDING_CHECKS {
        state.pending_checks.insert(key, trigger);
    } else if let Trigger::AgentLost { event_seq } = trigger {
        // A full check map must never silently drop a loss identity: park
        // the exact Agent/generation/sequence in the bounded loss queue
        // instead. (Sweeps also route lost Agents through the loss
        // pipeline now, but the queued identity must still survive a full
        // map exactly.)
        try_queue_loss_inner(&mut state, key, event_seq);
    } else {
        state.reconcile_all = true;
    }
}

/// Park or merge one loss under the check-map lock. Returns whether the
/// exact identity is queued (check map or loss queue) rather than dropped:
/// only double saturation (map and queue both full) drops, and the retained
/// Store evidence re-pages the drop on the next reconcile.
fn try_queue_loss_inner(state: &mut AttentionState, key: AgentKey, event_seq: i64) -> bool {
    if let Some(pending) = state.pending_checks.get_mut(&key) {
        *pending = merge_trigger(*pending, Trigger::AgentLost { event_seq });
        return true;
    }
    if state.pending_checks.len() < MAX_PENDING_CHECKS {
        state
            .pending_checks
            .insert(key, Trigger::AgentLost { event_seq });
        return true;
    }
    push_pending_loss(state, key.clone(), event_seq);
    state
        .pending_losses
        .iter()
        .any(|(pending, _)| *pending == key)
}

/// Queue one retained loss with its exact `AgentLost` trigger, reporting
/// whether it was retained. Used by Store re-paging, which resumes at the
/// first unqueued row instead of restarting at the newest.
fn try_queue_loss(state: &Arc<Mutex<AttentionState>>, key: AgentKey, event_seq: i64) -> bool {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    try_queue_loss_inner(&mut state, key, event_seq)
}

/// Park one overflowed loss in the bounded resumable queue. A repeat loss
/// for an already-queued Agent refreshes its sequence; when the queue itself
/// is saturated the loss is dropped but `losses_overflow` forces the next
/// reconcile to re-page retained evidence instead of losing it silently.
fn push_pending_loss(state: &mut AttentionState, key: AgentKey, event_seq: i64) {
    if let Some(queued) = state
        .pending_losses
        .iter_mut()
        .find(|(pending, _)| *pending == key)
    {
        queued.1 = event_seq;
    } else if state.pending_losses.len() < MAX_PENDING_LOSSES {
        state.pending_losses.push_back((key, event_seq));
    } else {
        state.losses_overflow = true;
    }
}

/// Move queued losses into the check map with their exact `AgentLost`
/// trigger, evicting one ordinary sweep per loss when the map is full so
/// the bound still holds. A loss moves only once its key is retained in the
/// map; when the map holds nothing evictable the remainder waits for the
/// next reconcile instead of degrading to a sweep.
pub(super) fn promote_pending_losses(state: &Arc<Mutex<AttentionState>>) {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    while let Some((key, event_seq)) = state.pending_losses.front().cloned() {
        let loss = Trigger::AgentLost { event_seq };
        if let Some(pending) = state.pending_checks.get_mut(&key) {
            *pending = merge_trigger(*pending, loss);
            state.pending_losses.pop_front();
        } else if state.pending_checks.len() < MAX_PENDING_CHECKS {
            state.pending_checks.insert(key, loss);
            state.pending_losses.pop_front();
        } else if let Some(evicted) =
            state
                .pending_checks
                .iter()
                .find_map(|(candidate, trigger)| {
                    matches!(trigger, Trigger::Sweep).then(|| candidate.clone())
                })
        {
            state.pending_checks.remove(&evicted);
            state.pending_checks.insert(key, loss);
            state.pending_losses.pop_front();
            // The evicted sweep still needs its visit: fall back to a full
            // scan rather than dropping the check.
            state.reconcile_all = true;
        } else {
            break;
        }
    }
}

/// Whether the Agent already has queued loss evidence: either parked in the
/// overflow queue or retained in the check map with its exact `AgentLost`
/// trigger. Sweeps must skip such keys so a generic check cannot win the
/// incident-id race against the loss-anchored report.
fn has_queued_loss(state: &Arc<Mutex<AttentionState>>, key: &AgentKey) -> bool {
    let state = state.lock().unwrap_or_else(|error| error.into_inner());
    state
        .pending_losses
        .iter()
        .any(|(pending, _)| pending == key)
        || matches!(
            state.pending_checks.get(key),
            Some(Trigger::AgentLost { .. })
        )
}

fn merge_trigger(current: Trigger, incoming: Trigger) -> Trigger {
    match (current, incoming) {
        (_, lost @ Trigger::AgentLost { .. }) => lost,
        (lost @ Trigger::AgentLost { .. }, _) => lost,
        (Trigger::SourceEvent { event_seq: current }, Trigger::SourceEvent { event_seq }) => {
            Trigger::SourceEvent {
                event_seq: current.max(event_seq),
            }
        }
        (Trigger::SourceEvent { .. }, Trigger::Sweep) => current,
        (Trigger::Sweep, trigger) => trigger,
    }
}

pub(super) fn request_full_reconcile(state: &Arc<Mutex<AttentionState>>) {
    state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .reconcile_all = true;
}

/// Drain retained startup losses as `AgentLost` triggers in bounded,
/// resumable pages. Each entry flows through `inspect_lost_agent` (or the
/// child cohort path when the lost manager still has unfinished children),
/// so reporting stays deduplicated per incident by the reservation map and no
/// second incident engine is introduced. Rows whose exact `agent.lost`
/// evidence was pruned carry no sequence and are skipped here, never
/// synthesized. A lost Agent whose evidence went stale before commit is
/// classified and dropped there, never mailed.
///
/// Paging resumes from the retained cursor instead of restarting at the
/// newest row: when saturation drops a row, the cursor stays at the last
/// fully-queued row and the next reconcile retries the first unqueued row
/// first, so the oldest loss cannot starve while newer rows refill freed
/// slots. Queued rows re-merge idempotently on retry.
pub(super) async fn queue_startup_losses(router: &Router, state: &Arc<Mutex<AttentionState>>) {
    loop {
        let after = {
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .loss_page_cursor
        };
        let page = {
            let store = router.inner.store.lock().await;
            store.startup_lost_agents(MAX_STARTUP_LOSSES, after)
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.attention_startup_losses_failed",
                    &[],
                    Some(&error),
                );
                return;
            }
        };
        if page.is_empty() {
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .loss_page_cursor = None;
            return;
        }
        let mut queued_through = after;
        for loss in &page {
            let Some(event_seq) = loss.event_seq else {
                queued_through = Some((loss.updated_at, loss.agent_num));
                continue;
            };
            if !try_queue_loss(
                state,
                AgentKey {
                    agent: loss.agent.clone(),
                    generation: loss.generation,
                },
                event_seq,
            ) {
                break;
            }
            queued_through = Some((loss.updated_at, loss.agent_num));
        }
        {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            // A drop leaves the cursor at the last fully-queued row, so the
            // next pass refetches this page and retries the first unqueued
            // row (already-queued rows re-merge idempotently). A full page
            // advances to its last row; a short page ends the pass.
            state.loss_page_cursor = queued_through;
        }
        if page.len() < MAX_STARTUP_LOSSES
            && queued_through == page.last().map(|loss| (loss.updated_at, loss.agent_num))
        {
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .loss_page_cursor = None;
            return;
        }
        if queued_through == after {
            // No progress on this page (the head row itself was dropped):
            // retry it next reconcile rather than spinning on it here.
            return;
        }
    }
}

fn reserve_pending_checks(
    state: &Arc<Mutex<AttentionState>>,
    limit: usize,
) -> Vec<(AgentKey, Trigger)> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let available = MAX_CHECKS_IN_FLIGHT.saturating_sub(state.in_flight.len());
    let keys = state
        .pending_checks
        .keys()
        .filter(|key| !state.in_flight.contains(*key))
        .take(limit.min(available))
        .cloned()
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| {
            let trigger = state.pending_checks.remove(&key)?;
            state.in_flight.insert(key.clone());
            Some((key, trigger))
        })
        .collect()
}

fn active_reconcile_batch(state: &Arc<Mutex<AttentionState>>, limit: usize) -> Vec<AgentKey> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let mut keys = state
        .active
        .keys()
        .map(|key| AgentKey {
            agent: key.agent.clone(),
            generation: key.generation,
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    keys.sort_by(|left, right| {
        left.agent
            .cmp(&right.agent)
            .then_with(|| left.generation.cmp(&right.generation))
    });
    if keys.is_empty() {
        state.active_reconcile_cursor = 0;
        return keys;
    }
    let count = limit.min(keys.len());
    let start = state.active_reconcile_cursor % keys.len();
    let batch = (0..count)
        .map(|offset| keys[(start + offset) % keys.len()].clone())
        .collect();
    state.active_reconcile_cursor = (start + count) % keys.len();
    batch
}

async fn run_check(
    router: Router,
    state: Arc<Mutex<AttentionState>>,
    key: AgentKey,
    suspicion_window: Duration,
    trigger: Trigger,
) {
    let _guard = CheckGuard {
        state: Arc::clone(&state),
        key: key.clone(),
    };
    if let Err(error) = inspect_agent(
        &router,
        state,
        &key.agent,
        key.generation,
        suspicion_window,
        trigger,
    )
    .await
    {
        crate::logging::record(
            &eliot_core::SecretPolicy::default(),
            "daemon.attention_check_failed",
            &[],
            Some(&error),
        );
    }
}

struct CheckGuard {
    state: Arc<Mutex<AttentionState>>,
    key: AgentKey,
}

impl Drop for CheckGuard {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .in_flight
            .remove(&self.key);
    }
}

pub(super) struct IncidentReservation {
    state: Arc<Mutex<AttentionState>>,
    committed: bool,
}

impl IncidentReservation {
    pub(super) fn commit(&mut self, key: IncidentKey, incident: ActiveIncident) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.reserved_incidents = state.reserved_incidents.saturating_sub(1);
        state.active.insert(key, incident);
        self.committed = true;
    }
}

impl Drop for IncidentReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.reserved_incidents = state.reserved_incidents.saturating_sub(1);
        }
    }
}

async fn scan_attention_agents(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    let mut after: Option<String> = None;
    let mut checks = JoinSet::new();
    loop {
        let page = {
            let store = router.inner.store.lock().await;
            store.list_attention_agents(after.as_deref(), PAGE_SIZE)?
        };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|agent| agent.id.clone());
        for agent in page {
            while checks.len() >= MAX_SWEEP_CHECKS {
                let _ = checks.join_next().await;
            }
            let key = AgentKey {
                agent: agent.id.clone(),
                generation: agent.generation,
            };
            if has_queued_loss(&state, &key) {
                // Loss evidence is already queued with its exact identity;
                // a generic sweep here could win the incident-id race.
                continue;
            }
            if !reserve_check(&state, &key) {
                queue_check(&state, key, Trigger::Sweep);
                continue;
            }
            let router = router.clone();
            checks.spawn(run_check(
                router,
                Arc::clone(&state),
                key,
                suspicion_window,
                Trigger::Sweep,
            ));
        }
        if after.is_none() || checks.len() >= MAX_SWEEP_CHECKS {
            tokio::task::yield_now().await;
        }
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

async fn reconcile_attention_state(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    retire_superseded_incidents(router, &state).await?;
    retry_pending_deliveries(router, Arc::clone(&state)).await;
    let reconcile_all = {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        std::mem::take(&mut state.reconcile_all)
    };
    let losses_pending = {
        let state = state.lock().unwrap_or_else(|error| error.into_inner());
        state.losses_overflow || !state.pending_losses.is_empty()
    };
    if reconcile_all || losses_pending {
        // Re-page retained loss evidence before the scan: overflow, lag, or
        // a crash window may have missed the live `agent.lost` event, and
        // only the exact Agent/generation/sequence reproduces the
        // deterministic loss identity. A saturated map parks losses in the
        // bounded queue instead of dropping them; the flag clears on every
        // re-page and returns if saturation persists.
        {
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            state.losses_overflow = false;
        }
        queue_startup_losses(router, &state).await;
        promote_pending_losses(&state);
    }
    if reconcile_all {
        scan_all_agents(router, Arc::clone(&state), suspicion_window).await?;
    }

    let pending = reserve_pending_checks(&state, MAX_SWEEP_CHECKS);
    let active = active_reconcile_batch(&state, MAX_SWEEP_CHECKS);
    if pending.is_empty() && active.is_empty() {
        return Ok(());
    }
    let mut checks = JoinSet::new();
    for (key, trigger) in pending {
        checks.spawn(run_check(
            router.clone(),
            Arc::clone(&state),
            key,
            suspicion_window,
            trigger,
        ));
    }
    for key in active {
        if reserve_check(&state, &key) {
            checks.spawn(run_check(
                router.clone(),
                Arc::clone(&state),
                key,
                suspicion_window,
                Trigger::Sweep,
            ));
        } else {
            queue_check(&state, key, Trigger::Sweep);
        }
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

fn reserve_pending_deliveries(
    state: &Arc<Mutex<AttentionState>>,
    limit: usize,
) -> Vec<(IncidentKey, String, FrozenReport, bool, bool)> {
    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
    let available = MAX_CHECKS_IN_FLIGHT.saturating_sub(state.in_flight.len());
    let mut selected_agents = HashSet::new();
    let selected = state
        .active
        .iter()
        .filter_map(|(key, incident)| {
            let agent_key = AgentKey {
                agent: key.agent.clone(),
                generation: key.generation,
            };
            if state.in_flight.contains(&agent_key) || selected_agents.contains(&agent_key) {
                return None;
            }
            let (delivery, recovery) = incident
                .incident_delivery
                .as_ref()
                .map(|delivery| (delivery, false))
                .or(incident
                    .recovery_delivery
                    .as_ref()
                    .map(|delivery| (delivery, true)))?;
            selected_agents.insert(agent_key.clone());
            Some((
                key.clone(),
                incident.incident_id.clone(),
                delivery.clone(),
                recovery,
                incident.is_loss,
                agent_key,
            ))
        })
        .take(limit.min(available))
        .collect::<Vec<_>>();
    selected
        .into_iter()
        .map(
            |(key, incident_id, delivery, recovery, is_loss, agent_key)| {
                state.in_flight.insert(agent_key);
                (key, incident_id, delivery, recovery, is_loss)
            },
        )
        .collect()
}

async fn retry_pending_deliveries(router: &Router, state: Arc<Mutex<AttentionState>>) {
    let pending = reserve_pending_deliveries(&state, MAX_SWEEP_CHECKS);
    if pending.is_empty() {
        return;
    }
    let mut tasks = JoinSet::new();
    for (key, incident_id, delivery, recovery, is_loss) in pending {
        let router = router.clone();
        let state = Arc::clone(&state);
        tasks.spawn(async move {
            let _guard = CheckGuard {
                state: Arc::clone(&state),
                key: AgentKey {
                    agent: key.agent.clone(),
                    generation: key.generation,
                },
            };
            // Loss retries reuse the idempotent loss delivery (per-thread
            // presence skips mail that is already delivered); the generic
            // path would treat a retained duplicate mail binding as failure.
            let delivered = if is_loss && !recovery {
                persist_loss_delivery(&router, &delivery).await
            } else {
                persist_and_notify(&router, &delivery).await
            };
            if let Err(error) = delivered {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.attention_delivery_retry_failed",
                    &[],
                    Some(&error),
                );
                return;
            }
            let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
            if !state
                .active
                .get(&key)
                .is_some_and(|incident| incident.incident_id == incident_id)
            {
                return;
            }
            if recovery || is_loss {
                // Loss delivery is terminal: no recovery follows a lost
                // Agent, so drop the entry and free its reservation slot.
                state.active.remove(&key);
            } else if let Some(incident) = state.active.get_mut(&key)
                && incident
                    .incident_delivery
                    .as_ref()
                    .is_some_and(|current| current.emit_command_id == delivery.emit_command_id)
            {
                incident.incident_delivery = None;
            }
        });
    }
    while tasks.join_next().await.is_some() {}
}

async fn scan_all_agents(
    router: &Router,
    state: Arc<Mutex<AttentionState>>,
    suspicion_window: Duration,
) -> Result<()> {
    let mut after: Option<String> = None;
    let mut checks = JoinSet::new();
    loop {
        let page = {
            let store = router.inner.store.lock().await;
            store.list_agents(after.as_deref(), PAGE_SIZE)?.0
        };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|agent| agent.id.clone());
        for agent in page {
            while checks.len() >= MAX_SWEEP_CHECKS {
                let _ = checks.join_next().await;
            }
            let key = AgentKey {
                agent: agent.id,
                generation: agent.generation,
            };
            if has_queued_loss(&state, &key) {
                // Retained loss evidence is already queued (or re-paged
                // above) with its exact identity; only `AgentLost`
                // handling may report it.
                continue;
            }
            if reserve_check(&state, &key) {
                checks.spawn(run_check(
                    router.clone(),
                    Arc::clone(&state),
                    key,
                    suspicion_window,
                    Trigger::Sweep,
                ));
            } else {
                queue_check(&state, key, Trigger::Sweep);
            }
        }
        tokio::task::yield_now().await;
    }
    while checks.join_next().await.is_some() {}
    Ok(())
}

pub(super) fn reserve_incident(
    state: &Arc<Mutex<AttentionState>>,
    key: &IncidentKey,
) -> Option<IncidentReservation> {
    let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
    if guard.active.contains_key(key)
        || guard.active.len() + guard.reserved_incidents >= MAX_RETAINED_INCIDENTS
    {
        return None;
    }
    guard.reserved_incidents += 1;
    drop(guard);
    Some(IncidentReservation {
        state: Arc::clone(state),
        committed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> Arc<Mutex<AttentionState>> {
        Arc::new(Mutex::new(AttentionState::default()))
    }

    fn key(n: u32) -> AgentKey {
        AgentKey {
            agent: format!("a{n}"),
            generation: 1,
        }
    }

    fn lock_loss_identity(state: &Arc<Mutex<AttentionState>>, agent: &str) -> Option<i64> {
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        match guard.pending_checks.get(&AgentKey {
            agent: agent.to_owned(),
            generation: 1,
        }) {
            Some(Trigger::AgentLost { event_seq }) => Some(*event_seq),
            _ => None,
        }
    }

    #[test]
    fn overflowed_loss_keeps_exact_identity_in_loss_queue() {
        let state = state();
        for n in 0..MAX_PENDING_CHECKS as u32 {
            queue_check(&state, key(n), Trigger::Sweep);
        }
        {
            let guard = state.lock().unwrap_or_else(|error| error.into_inner());
            assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
            assert!(!guard.reconcile_all);
        }
        // The full map must not drop the loss identity into a generic sweep.
        queue_check(&state, key(600), Trigger::AgentLost { event_seq: 42 });
        {
            let guard = state.lock().unwrap_or_else(|error| error.into_inner());
            assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
            assert!(!guard.pending_checks.contains_key(&key(600)));
            assert!(!guard.reconcile_all);
            assert_eq!(guard.pending_losses.len(), 1);
            assert_eq!(guard.pending_losses[0], (key(600), 42));
        }
        assert!(has_queued_loss(&state, &key(600)));
        assert!(!has_queued_loss(&state, &key(0)));
    }

    #[test]
    fn repeat_loss_refreshes_queued_sequence_without_duplicates() {
        let state = state();
        for n in 0..MAX_PENDING_CHECKS as u32 {
            queue_check(&state, key(n), Trigger::Sweep);
        }
        queue_check(&state, key(600), Trigger::AgentLost { event_seq: 7 });
        queue_check(&state, key(600), Trigger::AgentLost { event_seq: 9 });
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        assert_eq!(guard.pending_losses.len(), 1);
        assert_eq!(guard.pending_losses[0], (key(600), 9));
        assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
    }

    #[test]
    fn saturated_queues_set_losses_overflow() {
        let state = state();
        for n in 0..MAX_PENDING_CHECKS as u32 {
            queue_check(&state, key(n), Trigger::Sweep);
        }
        {
            let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
            for n in 0..MAX_PENDING_LOSSES as u32 {
                guard.pending_losses.push_back((key(1000 + n), n as i64));
            }
        }
        queue_check(&state, key(999_999), Trigger::AgentLost { event_seq: 1 });
        let guard = state.lock().unwrap_or_else(|error| error.into_inner());
        assert!(guard.losses_overflow);
        assert_eq!(guard.pending_losses.len(), MAX_PENDING_LOSSES);
        assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
    }

    #[test]
    fn promote_retains_loss_trigger_and_evicts_one_sweep() {
        let state = state();
        for n in 0..MAX_PENDING_CHECKS as u32 {
            queue_check(&state, key(n), Trigger::Sweep);
        }
        queue_check(&state, key(600), Trigger::AgentLost { event_seq: 42 });
        promote_pending_losses(&state);
        {
            let guard = state.lock().unwrap_or_else(|error| error.into_inner());
            // The loss moved into the map with its exact trigger; one sweep
            // left to make room and will be revisited by the requested scan.
            assert!(guard.pending_losses.is_empty());
            assert_eq!(guard.pending_checks.len(), MAX_PENDING_CHECKS);
            assert!(guard.reconcile_all);
        }
        assert_eq!(lock_loss_identity(&state, "a600"), Some(42));
    }

    #[test]
    fn promote_merges_into_existing_sweep_without_eviction() {
        let state = state();
        queue_check(&state, key(7), Trigger::Sweep);
        {
            let mut guard = state.lock().unwrap_or_else(|error| error.into_inner());
            guard.pending_losses.push_back((key(7), 42));
        }
        promote_pending_losses(&state);
        {
            let guard = state.lock().unwrap_or_else(|error| error.into_inner());
            assert!(guard.pending_losses.is_empty());
            assert_eq!(guard.pending_checks.len(), 1);
            assert!(!guard.reconcile_all);
        }
        assert_eq!(lock_loss_identity(&state, "a7"), Some(42));
    }

    #[test]
    fn sweeps_skip_only_agents_with_queued_loss() {
        let state = state();
        queue_check(&state, key(1), Trigger::Sweep);
        queue_check(&state, key(2), Trigger::AgentLost { event_seq: 11 });
        assert!(!has_queued_loss(&state, &key(0)));
        assert!(!has_queued_loss(&state, &key(1)));
        assert!(has_queued_loss(&state, &key(2)));
    }
}
