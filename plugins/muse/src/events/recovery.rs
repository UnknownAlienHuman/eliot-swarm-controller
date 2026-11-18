//! Muse projection recovery: bounded readback of the durable view projection.
//!
//! Live event processing owns turn/message/tool facts; this module owns the
//! projection-gap state machine behind narrow `pub(super)` interfaces. Pure
//! move from `super`: signatures, transactions, locking and event order unchanged.
use super::*;
/// Deadline for one recovery `view/page` fetch. The event actor stalls while
/// a page is in flight, so the bound mirrors the patch read: expiry already
/// fired, and a hung page behind a projection-failed host must not stall the
/// loop past one bounded wait per page.
const RECOVERY_PAGE_DEADLINE: Duration = Duration::from_secs(10);
/// One in-flight recovery call must fit inside the host's native stop
/// budget: the stop gate (`recovery_gate`) applies a queued `BeginStop`
/// between calls, so at most one bounded call overlaps a stop and the rest
/// of the budget stays available for native termination.
const _: () = assert!(
    RECOVERY_PAGE_DEADLINE.as_secs() < STOP_NATIVE_BUDGET.as_secs(),
    "one in-flight recovery page must leave stop budget for native termination"
);
/// First retry delay after a projection-gap readback proves nothing (a
/// failed `session/read` or an idle scan with no durable terminal). Later
/// delays double from here up to `EVENT_DEADLINE`, per the monitoring
/// backpressure bound.
const RECOVERY_RETRY_BASE_SECS: u64 = 5;
/// Cap on consecutive unproven readbacks before the gapped turn settles
/// `unknown` through the shared turn-unknown path (#427). An exact
/// `running` observation resets the count, so a provably live turn keeps
/// its binding; without positive native evidence the turn must not stay
/// busy forever.
const MAX_RECOVERY_READBACK_ATTEMPTS: u32 = 5;
/// Bounded queued-frame drain before the capped readback settles: a valid
/// post-head frame arriving during the final `session/read` waits in the
/// frame queue while settlement drains only controls. Give queued frames
/// this many chances to restore observation before writing `turn.unknown`.
const SETTLE_DRAIN_FRAMES: usize = 32;
pub(super) struct ViewRecovery {
    pub(super) head: Option<String>,
    pub(super) head_seen: bool,
    pub(super) deadline: Instant,
    /// Whether an acknowledged unavailable-view episode backs this state.
    /// Only a projection episode may settle `unknown` with the counted
    /// projection prefix; transport-only loss retries with the source stale
    /// and never settles on the projection path.
    pub(super) projection: bool,
    /// Consecutive readbacks that proved nothing since the last exact
    /// `running` observation. Reset on exact running, live recovery and
    /// terminal recovery. Only the cap settles unknown; a fenced
    /// unavailable-view terminal replay proves nothing, so it preserves this
    /// count instead of restarting it.
    pub(super) unproven_attempts: u32,
}

/// Live actor queues, threaded into the bounded recovery readbacks so queued
/// controls are applied between native calls instead of waiting out the
/// whole session/read plus page scan. Nested frame contexts (decide pumps,
/// buffered flushes) pass `None`: they already run inside frame/control
/// processing and must not re-enter the queues.
pub(super) struct ActorQueues<'a> {
    pub(super) frames: &'a mut mpsc::Receiver<NativeFrame>,
    pub(super) controls: &'a mut mpsc::Receiver<Control>,
}

/// Stop-aware outcome of one bounded recovery step. `Yielded` means a stop
/// was requested mid-readback: the armed deadline is already cleared so the
/// stop keeps its budget for native termination, and the caller must return
/// to the actor loop without re-arming or settling. `Shutdown` unwinds to
/// the run loop on `Close`.
pub(super) enum RecoveryPoll<T> {
    Ready(T),
    Yielded,
    Shutdown,
}

/// Whether recovery may spend another bounded native call. A requested stop
/// owns the remaining stop budget for native termination (the retained stop
/// id blocks a cooperative retry once that budget expires), so recovery
/// yields: no new `session/read` or `view/page` once `planned_stop` is set,
/// and at most the single in-flight call — bounded by
/// `RECOVERY_PAGE_DEADLINE`, strictly inside the host's native stop budget —
/// elapses before the queued stop is applied.
fn recovery_may_spend_native_call(planned_stop: bool) -> bool {
    !planned_stop
}

/// Attempt count a fenced unavailable-view terminal carries into its fresh
/// recovery state: the replay proves nothing, so an existing
/// consecutive-failure count survives it instead of restarting at zero.
pub(super) fn fenced_recovery_attempts(existing: Option<&ViewRecovery>) -> u32 {
    existing
        .map(|recovery| recovery.unproven_attempts)
        .unwrap_or(0)
}

/// Error code a recovery drain uses to unwind to the run loop when a queued
/// `Close` arrives mid-readback. The close handshake already completed
/// inside the drain; the code only tells frame handling to skip its gap
/// bookkeeping and exit.
pub(super) const RECOVERY_SHUTDOWN_CODE: &str = "event_shutdown";

pub(super) fn recovery_shutdown() -> Error {
    Error::new(RECOVERY_SHUTDOWN_CODE, "Muse event actor is shutting down")
}

/// Delay before the next bounded `session/read` after `failures` consecutive
/// unproven readbacks (1-based): 5s, 10s, 20s, then the 30s event deadline.
/// A timer only schedules observation; it never supplies a turn outcome.
fn recovery_retry_delay(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(3);
    let secs = RECOVERY_RETRY_BASE_SECS.saturating_mul(1 << shift);
    Duration::from_secs(secs.min(EVENT_DEADLINE.as_secs()))
}
impl EventProcessor {
    pub(super) fn bind_view_session(&mut self, session_id: &str) -> Result<()> {
        if session_id.trim().is_empty()
            || self
                .status
                .session_id
                .as_deref()
                .is_some_and(|owned| owned != session_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Muse view-health readback named another session",
            ));
        }
        // Pull callers already validated the native response against the owned
        // resume/read subject. This binds identity, never readiness or a turn.
        self.status.session_id = Some(session_id.to_owned());
        Ok(())
    }

    pub(super) fn reestablish_idle(&mut self, session_id: &str) -> Result<bool> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse idle re-establishment named another session",
            ));
        }
        if self.status.active_turn_id.is_some() || self.status.active_command_id.is_some() {
            return Ok(false);
        }
        let cleared = self.status.clear_stale_idle_latch();
        // The caller proved the owned session idle with a fresh
        // `session/read` first, so that proof also drops a
        // readback-recovery send gate. Clearing here (and only here, never
        // in `clear_stale_idle_latch`) keeps the gate until exact proof.
        let gate_held = std::mem::replace(&mut self.status.needs_idle_proof, false);
        if cleared {
            self.view_recovery = None;
            self.gap_reported = false;
        }
        Ok(cleared || gate_held)
    }

    /// One read-only `view/subscribe` attempt from the last applied cursor.
    /// `None` when the params cannot be built, the call fails, or the
    /// returned head is blank. Changes no session/turn/input state.
    async fn subscribe_view_head(&self, session_id: &str, after: &str) -> Option<String> {
        let params = NativeViewSubscribeParams {
            session_id: session_id.to_owned(),
            after: after.to_owned(),
        };
        let params = serde_json::to_value(params).ok()?;
        self.native
            .call("view/subscribe", params, Instant::now() + EVENT_DEADLINE)
            .await
            .ok()
            .and_then(|value| serde_json::from_value::<NativeViewSubscribeResult>(value).ok())
            .map(|result| result.view_cursor)
            .filter(|cursor| !cursor.trim().is_empty())
    }

    pub(super) async fn view_unavailable(
        &mut self,
        session_id: &str,
        reason: NativeHistoryNoneReason,
    ) -> Result<()> {
        self.bind_view_session(session_id)?;
        if self.status.view_unavailable {
            return Ok(());
        }
        let can_recover = self.status.ready
            && self.status.active_turn_id.is_some()
            && !self.status.source_unknown
            && !self.writes_blocked;
        self.status.source_unknown = true;
        self.status.view_unavailable = true;
        self.status.view_unavailable_reason = Some(reason);
        // This changes only the subscription, never a session/turn or input.
        // The actor applies no replayed frame until the gap is acknowledged.
        let after = self.last_view_cursor.clone();
        let head = if can_recover && let Some(after) = after.as_ref() {
            self.subscribe_view_head(session_id, after).await
        } else {
            None
        };
        let gap_deadline = Instant::now() + EVENT_DEADLINE;
        let gap_result = if !self.gap_reported && !self.writes_blocked {
            self.stream_source()
                .record_gap_range_until(
                    &format!("{MUSE_VIEW_UNAVAILABLE_REASON_PREFIX} ({})", reason.label()),
                    after.clone(),
                    head.clone(),
                    gap_deadline,
                )
                .await
        } else {
            Ok(())
        };
        self.report_view_stale().await;
        if self.status.active_turn_id.is_some() {
            let recoverable = gap_result.is_ok() && !self.writes_blocked && head.is_some();
            self.view_recovery = Some(ViewRecovery {
                head_seen: head.is_some() && after == head,
                head,
                deadline: if recoverable {
                    Instant::now() + EVENT_DEADLINE
                } else {
                    Instant::now()
                },
                projection: self.projection_episode(),
                unproven_attempts: 0,
            });
        }
        gap_result
    }

    /// Whether an acknowledged unavailable-view gap episode stands: the
    /// view latch is set and the source permits writes. Only such an
    /// episode may settle `unknown` with the counted projection prefix.
    pub(super) fn projection_episode(&self) -> bool {
        self.status.view_unavailable && !self.writes_blocked
    }

    /// Arm recovery after a turn associates while view loss is already
    /// latched. `view_unavailable` creates recovery state only for an
    /// already-associated turn; when the gap lands first (while `turn/start`
    /// awaits its reply), the committed `turn.started` would otherwise have
    /// no readback deadline. Mirrors the recoverable path now
    /// that a turn is active: one subscribe from the last applied cursor
    /// gives live delivery a chance to prove itself, else the deadline
    /// expires immediately to start bounded readback retries. Never re-records
    /// the gap.
    pub(super) async fn arm_recovery_after_associate(&mut self) {
        if !should_arm_recovery_after_associate(&self.status, self.view_recovery.is_some()) {
            return;
        }
        let after = self.last_view_cursor.clone();
        let session_id = self.status.session_id.clone().unwrap_or_default();
        let head = if self.status.ready
            && !self.writes_blocked
            && let Some(after) = after.as_ref()
        {
            self.subscribe_view_head(&session_id, after).await
        } else {
            None
        };
        let recoverable = !self.writes_blocked && head.is_some();
        self.view_recovery = Some(ViewRecovery {
            head_seen: head.is_some() && after == head,
            head,
            deadline: if recoverable {
                Instant::now() + EVENT_DEADLINE
            } else {
                Instant::now()
            },
            projection: self.projection_episode(),
            unproven_attempts: 0,
        });
    }

    /// Yield point between bounded recovery calls: apply controls queued
    /// while the last native call was in flight, so a `BeginStop` arriving
    /// mid-recovery preempts the next page instead of waiting out the whole
    /// session/read plus page scan (~40 s against a 20 s native stop
    /// budget). Reports whether recovery may spend another bounded call.
    pub(super) async fn recovery_gate(
        &mut self,
        queues: &mut Option<ActorQueues<'_>>,
    ) -> RecoveryPoll<()> {
        if let Some(live) = queues.as_mut() {
            while let Ok(control) = live.controls.try_recv() {
                // Box the drain: recovery already runs inside frame/control
                // handling further up the stack, so awaiting the handler
                // inline would recurse the actor future without indirection.
                if !Box::pin(self.handle_control(live.frames, control)).await {
                    self.view_recovery = None;
                    return RecoveryPoll::Shutdown;
                }
            }
        }
        if recovery_may_spend_native_call(self.planned_stop) {
            RecoveryPoll::Ready(())
        } else {
            // An in-flight stop owns the remaining stop budget for native
            // termination (the retained stop id blocks a cooperative retry
            // once that budget expires), so abandon the readback instead of
            // spending it on further pages.
            self.view_recovery = None;
            RecoveryPoll::Yielded
        }
    }

    pub(super) async fn expire_view_recovery(
        &mut self,
        queues: &mut Option<ActorQueues<'_>>,
    ) -> bool {
        match self.recovery_gate(queues).await {
            RecoveryPoll::Shutdown => false,
            RecoveryPoll::Yielded => true,
            RecoveryPoll::Ready(()) => {
                self.status.source_unknown = true;
                self.recheck_projection_gap(queues).await
            }
        }
    }

    /// One backward `view/page` fetch for recovery, newest records first.
    /// `None` cursor starts at the view head; the caller chains
    /// `next_cursor` exactly like the snapshot readback. Read-only: changes
    /// no session/turn/input state.
    async fn read_recovery_page(
        &self,
        session_id: &str,
        cursor: Option<String>,
    ) -> Result<NativeViewPageResult> {
        let params = NativeViewPageParams {
            session_id: session_id.to_owned(),
            limit: VIEW_PAGE_SIZE,
            direction: Some("backward".to_owned()),
            cursor,
        };
        let params = serde_json::to_value(params)
            .map_err(|_| Error::new("internal", "Muse view/page params could not be encoded"))?;
        serde_json::from_value(
            self.native
                .call("view/page", params, Instant::now() + RECOVERY_PAGE_DEADLINE)
                .await?,
        )
        .map_err(|_| Error::new("native_readback", "Muse view/page response was invalid"))
    }

    /// Durable terminal proof for the exact active turn, paged newest-first
    /// from the retained view: the raw `turn/completed` params plus the raw
    /// `item/completed` agentMessage params carrying the latest final text
    /// when the durable view holds one. `None` when no bounded page proves
    /// the terminal — the turn may still be running, the projection may be
    /// gone entirely, or an exact-turn record may be undecodable. Never
    /// synthesizes: only records the native view durably holds.
    async fn readback_projection_terminal(
        &mut self,
        session_id: &str,
        turn_id: &str,
        queues: &mut Option<ActorQueues<'_>>,
    ) -> RecoveryPoll<Option<(Value, Option<Value>)>> {
        let mut scan = RecoveryScan::default();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        for _ in 0..MAX_VIEW_PAGES {
            // A stop queued while the last page was in flight ends the scan
            // here, keeping the remaining stop budget for termination.
            match self.recovery_gate(queues).await {
                RecoveryPoll::Shutdown => return RecoveryPoll::Shutdown,
                RecoveryPoll::Yielded => return RecoveryPoll::Yielded,
                RecoveryPoll::Ready(()) => {}
            }
            let page = match self.read_recovery_page(session_id, cursor.clone()).await {
                Ok(page) => page,
                Err(_) => return RecoveryPoll::Ready(None),
            };
            // Backward pages still contain ascending events on the wire.
            for event in page.events.iter().rev() {
                match fold_recovery_event(&mut scan, event, session_id, turn_id) {
                    RecoveryFold::Complete => {
                        return RecoveryPoll::Ready(
                            scan.turn_params
                                .map(|terminal| (terminal, scan.message_params)),
                        );
                    }
                    RecoveryFold::Unproven => return RecoveryPoll::Ready(None),
                    RecoveryFold::Continue => {}
                }
            }
            match page.next_cursor {
                Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                    cursor = Some(next);
                }
                // No (or a repeated) cursor: no older page can prove more.
                // A proven terminal still returns below; otherwise the
                // terminal stays unproven.
                _ => break,
            }
        }
        RecoveryPoll::Ready(
            scan.turn_params
                .map(|terminal| (terminal, scan.message_params)),
        )
    }

    /// Commit a durably proven terminal for the projection-gapped active
    /// turn through the exact live shapes — the final message first (when
    /// the durable view proved one), then the terminal — and retire the
    /// recovery deadline. Never replays a prompt and never fabricates a
    /// terminal: every committed fact re-parses through the live commit
    /// paths, which re-validate the exact session/turn binding. The recorded
    /// source gap and stale health stay latched (private readback cannot
    /// clear a live-source gap), and later facts for the turn still gap
    /// instead of resurrecting it. Recovered records are marked applied so a
    /// later live replay of the same revision skips normalization instead of
    /// recommitting the native id. Yields `Ready(false)` when nothing was
    /// proven or the terminal would not persist, so the caller keeps retrying;
    /// yields or shutdown unwind without committing.
    pub(super) async fn recover_projection_terminal(
        &mut self,
        queues: &mut Option<ActorQueues<'_>>,
    ) -> RecoveryPoll<bool> {
        let (Some(session_id), Some(turn_id)) = (
            self.status.session_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            return RecoveryPoll::Ready(false);
        };
        let (turn_params, message_params) = match self
            .readback_projection_terminal(&session_id, &turn_id, queues)
            .await
        {
            RecoveryPoll::Shutdown => return RecoveryPoll::Shutdown,
            RecoveryPoll::Yielded => return RecoveryPoll::Yielded,
            RecoveryPoll::Ready(None) => return RecoveryPoll::Ready(false),
            RecoveryPoll::Ready(Some(found)) => found,
        };
        // The commits below are two more bounded calls: re-check for a
        // queued stop before spending them.
        match self.recovery_gate(queues).await {
            RecoveryPoll::Shutdown => return RecoveryPoll::Shutdown,
            RecoveryPoll::Yielded => return RecoveryPoll::Yielded,
            RecoveryPoll::Ready(()) => {}
        }
        // Terminal proof stays separate from message persistence: an
        // optional message the persisted-event bound rejects must not
        // discard the proven terminal the turn commit below still carries.
        if let Some(params) = message_params
            && let Ok(message) =
                serde_json::from_value::<NativeItemCompletedNotification>(params.clone())
            && let Some(text) = message.item.text.clone()
            && self
                .complete_message(&turn_id, &message.item.item_id, &text)
                .await
                .is_ok()
            && let Some(cursor) = params.get("viewCursor").and_then(Value::as_str)
            && !cursor.trim().is_empty()
        {
            self.applied_view_cursors
                .insert(applied_fact_key("item/completed", cursor, &params));
        }
        if self.complete_turn(turn_params.clone(), true).await.is_err() {
            return RecoveryPoll::Ready(false);
        }
        if let Some(cursor) = turn_params.get("viewCursor").and_then(Value::as_str)
            && !cursor.trim().is_empty()
        {
            self.applied_view_cursors.insert(applied_fact_key(
                "turn/completed",
                cursor,
                &turn_params,
            ));
        }
        self.view_recovery = None;
        if let Some(reason) = self.status.view_unavailable_reason {
            self.status.failure = Some(format!(
                "Muse native view is unavailable ({})",
                reason.label()
            ));
        }
        RecoveryPoll::Ready(true)
    }

    /// Recheck a projection-gapped turn. The command plane can prove it still
    /// running: an exact `running` observation keeps the Swarm turn active
    /// with the source stale and resets the readback retry count, so a new
    /// send can never be delivered into the running native turn. Only
    /// explicit idle permits a fresh terminal scan. A readback that proves
    /// nothing (a failed `session/read`, an unexpected shape, or an idle
    /// scan with no durable terminal) re-arms one more bounded read with
    /// backoff; only the capped attempt settles `unknown` through the shared
    /// turn-unknown path, naming the exact readback failure. The plugin host
    /// stays alive and the Agent is not marked lost: only the active turn is
    /// cleared, so a later replayed terminal for the same turn gaps instead
    /// of resurrecting it, and a later stop settles the session normally.
    /// Idempotent: with no active turn there is nothing to settle. The local
    /// binding drops only after the terminal write is acknowledged; a failed
    /// commit (for example a lost gap ack latching `writes_blocked` so
    /// `turn.unknown` cannot be submitted) keeps the binding at the cap and
    /// re-arms an expiry so the next opportunity settles instead of
    /// stranding the Store's running turn while the adapter looks idle.
    async fn recheck_projection_gap(&mut self, queues: &mut Option<ActorQueues<'_>>) -> bool {
        let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            self.view_recovery = None;
            return true;
        };
        let session_id = self.status.session_id.clone().unwrap_or_default();
        let prior_attempts = self
            .view_recovery
            .as_ref()
            .map(|recovery| recovery.unproven_attempts)
            .unwrap_or(0);
        let projection = self
            .view_recovery
            .as_ref()
            .is_some_and(|recovery| recovery.projection);
        // An unavailable projection may synthesize a failed terminal from an
        // unfinished prefix. Exact idle proof is still required before a page
        // can prove the turn's terminal; read errors never supply that proof.
        let (state, read_detail) = self
            .projection_turn_state_checked(&session_id, &turn_id)
            .await;
        if state == ProjectionTurnState::Idle {
            match self.recover_projection_terminal(queues).await {
                RecoveryPoll::Shutdown => return false,
                RecoveryPoll::Yielded => return true,
                RecoveryPoll::Ready(true) => return true,
                RecoveryPoll::Ready(false) => {}
            }
        }
        // A stop or close queued during the read must still preempt recovery.
        match self.recovery_gate(queues).await {
            RecoveryPoll::Shutdown => return false,
            RecoveryPoll::Yielded => return true,
            RecoveryPoll::Ready(()) => {}
        }
        // A queued control can drain a terminal or associate another turn.
        // Never replace that turn's recovery state with this older readback.
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str())
            || self.status.active_command_id.as_deref() != Some(command_id.as_str())
        {
            return true;
        }
        let gap_label = self
            .status
            .view_unavailable_reason
            .map(|reason| reason.label().to_owned())
            .unwrap_or_else(|| "unknown".to_owned());
        // An idle scan that proved nothing names the missing terminal, not
        // the (absent) read failure; any other unproven readback names its
        // exact failure.
        let detail = match state {
            ProjectionTurnState::Idle => "idle session/read proved no active turn but the bounded view/page scan held no terminal for the exact turn".to_owned(),
            _ => read_detail.unwrap_or_else(|| "session/read proved nothing".to_owned()),
        };
        match decide_recovery_step(prior_attempts, state, &gap_label, &detail, projection) {
            RecoveryStep::KeepRunning => {
                self.view_recovery = Some(ViewRecovery {
                    head: self
                        .view_recovery
                        .as_ref()
                        .and_then(|recovery| recovery.head.clone()),
                    head_seen: self
                        .view_recovery
                        .as_ref()
                        .is_some_and(|recovery| recovery.head_seen),
                    deadline: Instant::now() + EVENT_DEADLINE,
                    projection,
                    unproven_attempts: 0,
                });
                true
            }
            RecoveryStep::Retry { attempts } => {
                // Only an acknowledged unavailable-view episode uses the
                // counted projection prefix; transport-only loss names its
                // own pending path and never settles on it.
                self.status.failure = if projection {
                    Some(format!(
                        "Muse view recovery pending: {detail} ({attempts} bounded readbacks); source remains stale"
                    ))
                } else {
                    Some(format!(
                        "Muse transport recovery pending: {detail} ({attempts} bounded readbacks); source remains stale"
                    ))
                };
                self.view_recovery = Some(ViewRecovery {
                    head: self
                        .view_recovery
                        .as_ref()
                        .and_then(|recovery| recovery.head.clone()),
                    head_seen: self
                        .view_recovery
                        .as_ref()
                        .is_some_and(|recovery| recovery.head_seen),
                    deadline: Instant::now() + recovery_retry_delay(attempts),
                    projection,
                    unproven_attempts: attempts,
                });
                true
            }
            RecoveryStep::SettleUnknown { attempts, reason } => {
                self.settle_projection_gap_unknown(
                    &command_id,
                    &turn_id,
                    &session_id,
                    attempts,
                    reason,
                    queues,
                )
                .await
            }
        }
    }

    /// Settle the projection-gapped turn `unknown` through the shared
    /// turn-unknown path, never as success or failure. The plugin host stays
    /// alive and the Agent is not marked lost: only the local active turn is
    /// cleared, so a later replayed terminal for the same turn gaps instead
    /// of resurrecting it, and a later stop settles the session normally.
    /// Idempotent: with no active turn there is nothing to settle. The local
    /// binding drops only after the terminal write is acknowledged; a failed
    /// commit (for example a lost gap ack latching `writes_blocked` so
    /// `turn.unknown` cannot be submitted) keeps the binding at the cap and
    /// re-arms an expiry so the next opportunity settles instead of
    /// stranding the Store turn.
    async fn settle_projection_gap_unknown(
        &mut self,
        command_id: &str,
        turn_id: &str,
        session_id: &str,
        attempts: u32,
        reason: String,
        queues: &mut Option<ActorQueues<'_>>,
    ) -> bool {
        // The unknown commit is another bounded call: re-check for a queued
        // stop first so termination keeps its budget.
        match self.recovery_gate(queues).await {
            RecoveryPoll::Shutdown => return false,
            RecoveryPoll::Yielded => return true,
            RecoveryPoll::Ready(()) => {}
        }
        // A queued control can drain a terminal or associate another turn.
        // Never settle a turn this readback no longer owns.
        if self.status.active_turn_id.as_deref() != Some(turn_id)
            || self.status.active_command_id.as_deref() != Some(command_id)
        {
            return true;
        }
        // A valid post-head frame can arrive during the final readback and
        // wait in the frame queue while settlement drains only controls.
        // Give queued frames a bounded chance to restore observation, then
        // recheck the binding before writing `turn.unknown`.
        if queues.is_some() {
            for _ in 0..SETTLE_DRAIN_FRAMES {
                let frame = match queues.as_mut().and_then(|live| live.frames.try_recv().ok()) {
                    Some(frame) => frame,
                    None => break,
                };
                if !self.handle_frame(Some(frame), queues).await {
                    return false;
                }
            }
            if self.status.active_turn_id.as_deref() != Some(turn_id)
                || self.status.active_command_id.as_deref() != Some(command_id)
            {
                return true;
            }
        }
        // Neither the command plane nor a fresh idle-time scan supplied a
        // proven terminal. Preserve uncertainty instead of a projected verdict.
        let data = json!({
            "command_id": command_id.to_owned(),
            "turn_id": turn_id.to_owned(),
            "reason": reason.clone(),
        });
        let acknowledged = match event_id(session_id, turn_id, turn_id, "turn.unknown") {
            Ok(native_id) => self
                .commit_event("turn.unknown", native_id, data)
                .await
                .is_ok(),
            Err(_) => false,
        };
        // Durable recovery above either committed the proven terminal (and
        // returned) or proved nothing, so only the acknowledged unknown
        // write clears the binding here.
        if !acknowledged {
            self.status.failure = Some(reason);
            let projection = self
                .view_recovery
                .as_ref()
                .is_some_and(|recovery| recovery.projection);
            self.view_recovery = Some(ViewRecovery {
                head: self
                    .view_recovery
                    .as_ref()
                    .and_then(|recovery| recovery.head.clone()),
                head_seen: self
                    .view_recovery
                    .as_ref()
                    .is_some_and(|recovery| recovery.head_seen),
                deadline: Instant::now() + EVENT_DEADLINE,
                projection,
                unproven_attempts: attempts,
            });
            return true;
        }
        self.status.failure = Some(reason);
        self.clear_projection_gap_turn(command_id, turn_id);
        true
    }

    /// Drop the local binding of a projection-gapped turn settled `unknown`.
    /// The recorded source gap and stale health stay latched, and pending
    /// native requests for the dead turn are dropped locally (their Store
    /// rows stay bound to it, so a late resolution cannot address another
    /// turn). A later live terminal for the same turn gaps instead of
    /// resurrecting it.
    fn clear_projection_gap_turn(&mut self, command_id: &str, turn_id: &str) {
        let dropped = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| entry.request.turn_id == turn_id)
            .filter_map(|(id, entry)| {
                pending_native_id(&entry.request).map(|native| (id.clone(), native.to_owned()))
            })
            .collect::<Vec<_>>();
        if !dropped.is_empty() {
            let observed = self.observe();
            for (request_id, native_id) in dropped {
                self.pending_requests.remove(&request_id);
                self.native_index.remove(&native_id);
                self.remember_dropped(native_id, observed);
            }
        }
        self.expected_requests.clear();
        self.messages = TurnMessages::default();
        self.status.last_turn = Some(TurnFacts {
            command_id: command_id.to_owned(),
            turn_id: turn_id.to_owned(),
            status: None,
            final_text: None,
            native_error: None,
            terminal: false,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.view_recovery = None;
        // A late terminal or final message naming this turn drops against
        // the standing gap instead of buffering as a turn awaiting
        // association; the marker clears on the next association.
        self.settled_unknown_turn = Some((command_id.to_owned(), turn_id.to_owned()));
    }

    /// One bounded, read-only command-plane observation, independent of the
    /// unavailable history envelope. It never supplies a terminal outcome.
    /// The detail names the exact readback failure for the bounded retry
    /// accounting; `None` means the observation proved its state.
    pub(super) async fn projection_turn_state_checked(
        &mut self,
        session_id: &str,
        turn_id: &str,
    ) -> (ProjectionTurnState, Option<String>) {
        let params = NativeSessionReadParams {
            session_id: session_id.to_owned(),
            exclude_items: Some(true),
        };
        let Ok(params) = serde_json::to_value(params) else {
            return (
                ProjectionTurnState::Unproven,
                Some("session/read params could not be encoded".to_owned()),
            );
        };
        let result = match self
            .native
            .call(
                "session/read",
                params,
                Instant::now() + RECOVERY_PAGE_DEADLINE,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let detail = match native_error_code(&error) {
                    Some(code) => format!("session/read native error ({code})"),
                    None => format!("session/read transport failed ({})", error.code),
                };
                return (ProjectionTurnState::Unproven, Some(detail));
            }
        };
        let state = projection_turn_state(&result, session_id, turn_id);
        if state == ProjectionTurnState::Unproven {
            return (
                ProjectionTurnState::Unproven,
                Some(describe_unproven_session_read(&result, session_id, turn_id)),
            );
        }
        if let Ok(readback) = serde_json::from_value::<NativeSessionReadResult>(result)
            && readback.session.session_id == session_id
            && let Some(reason) = readback.history.unavailable_reason()
            && self.view_unavailable(session_id, reason).await.is_err()
        {
            return (
                ProjectionTurnState::Unproven,
                Some(
                    "session/read proved the state but re-latching the recorded gap failed"
                        .to_owned(),
                ),
            );
        }
        (state, None)
    }

    pub(super) fn invalidate_view_recovery(&mut self) {
        if let Some(recovery) = self.view_recovery.as_mut() {
            recovery.head = None;
            recovery.deadline = Instant::now();
        }
    }
}
/// Whether a fresh turn association must arm a recovery deadline: view loss
/// is already latched with no recovery state, and a turn is now active to
/// recover. A gap recorded while `turn/start` awaited its reply otherwise
/// leaves the committed `turn.started` with no readback deadline.
fn should_arm_recovery_after_associate(status: &EventStatus, has_recovery: bool) -> bool {
    status.view_unavailable && !has_recovery && status.active_turn_id.is_some()
}

/// One bounded step after a projection-gap readback. Exact native proof the
/// turn still runs always keeps it: a provably live turn never exhausts a
/// retry budget. Anything else counts up, and only the capped attempt of an
/// acknowledged unavailable-view episode settles `unknown` with the exact
/// readback failure. Transport-only loss (`projection == false`) retries
/// with the source stale and never settles on the projection path.
#[derive(Debug, PartialEq, Eq)]
enum RecoveryStep {
    /// Exact native proof the turn still runs: keep the Swarm turn active
    /// with the source stale and reset the retry count.
    KeepRunning,
    /// Nothing proven yet: re-arm one more bounded read after the backoff.
    Retry { attempts: u32 },
    /// Cap exhausted on a projection episode: settle unknown with the exact
    /// readback failure.
    SettleUnknown { attempts: u32, reason: String },
}

fn decide_recovery_step(
    prior_attempts: u32,
    state: ProjectionTurnState,
    gap_label: &str,
    detail: &str,
    projection: bool,
) -> RecoveryStep {
    if state == ProjectionTurnState::Running {
        return RecoveryStep::KeepRunning;
    }
    let attempts = prior_attempts.saturating_add(1);
    if attempts < MAX_RECOVERY_READBACK_ATTEMPTS || !projection {
        RecoveryStep::Retry { attempts }
    } else {
        RecoveryStep::SettleUnknown {
            attempts,
            reason: recovery_unknown_reason(gap_label, detail, attempts),
        }
    }
}

/// Counted `turn.unknown` reason: the shared unknown prefix (so the outcome
/// stays exactly countable from the journal) with the recorded gap label and
/// the exact readback failure that exhausted the bound.
fn recovery_unknown_reason(gap_label: &str, detail: &str, attempts: u32) -> String {
    format!(
        "{MUSE_VIEW_UNKNOWN_REASON_PREFIX} ({gap_label}; {detail} after {attempts} bounded readbacks)"
    )
}
/// Durable terminal proof accumulated across `view/page` records for the
/// exact active turn, newest first. Raw params only: the commit path
/// re-parses and re-validates everything through the exact live shapes, so
/// recovery can never synthesize a terminal the durable view does not hold.
#[derive(Default)]
struct RecoveryScan {
    turn_params: Option<Value>,
    message_params: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProjectionTurnState {
    Running,
    Idle,
    Unproven,
}
/// The installed Session.activeTurnId is required-nullable. Check presence
/// before decoding Option<String>, so omission never becomes idle proof.
fn projection_turn_state(result: &Value, session_id: &str, turn_id: &str) -> ProjectionTurnState {
    let Some(active) = result
        .get("session")
        .and_then(|session| session.get("activeTurnId"))
    else {
        return ProjectionTurnState::Unproven;
    };
    let Ok(readback) = serde_json::from_value::<NativeSessionReadResult>(result.clone()) else {
        return ProjectionTurnState::Unproven;
    };
    if readback.session.session_id != session_id {
        return ProjectionTurnState::Unproven;
    }
    match (readback.session.status.as_str(), active) {
        ("running", Value::String(active)) if active == turn_id => ProjectionTurnState::Running,
        ("idle", Value::Null) => ProjectionTurnState::Idle,
        _ => ProjectionTurnState::Unproven,
    }
}

/// Exact reason a `session/read` result proved nothing about the gapped
/// turn. Mirrors `projection_turn_state` case by case so pending recovery
/// names the readback failure instead of the gap alone.
/// Shapes are described, never quoted verbatim, and the status text is
/// bounded: the wire status is open for evolution.
fn describe_unproven_session_read(result: &Value, session_id: &str, turn_id: &str) -> String {
    let Some(active) = result
        .get("session")
        .and_then(|session| session.get("activeTurnId"))
    else {
        return "session/read omitted activeTurnId".to_owned();
    };
    let Ok(readback) = serde_json::from_value::<NativeSessionReadResult>(result.clone()) else {
        return "session/read response did not match the 1.4 schema".to_owned();
    };
    if readback.session.session_id != session_id {
        return "session/read named a foreign session".to_owned();
    }
    let status = readback.session.status.as_str();
    match (status, active) {
        ("running", Value::String(active)) if active != turn_id => {
            "session/read reported running for another turn".to_owned()
        }
        ("running", _) => "session/read reported running with no active turn".to_owned(),
        ("idle", Value::String(_)) => "session/read reported idle with an active turn".to_owned(),
        ("idle", _) => "session/read reported idle without a null active turn".to_owned(),
        ("notLoaded", _) => "session/read reported the session notLoaded".to_owned(),
        _ => {
            let short = eliot_core::bound_chars(status, 32);
            format!("session/read reported status {short}")
        }
    }
}

/// One recovery-fold step over a single durable view record.
enum RecoveryFold {
    /// Keep paging: nothing decisive yet.
    Continue,
    /// Terminal and final message both proven; stop paging.
    Complete,
    /// An exact-turn record disproves provability (malformed or unknown
    /// terminal): stop paging, the terminal stays unproven.
    Unproven,
}

/// Fold one durable `view/page` record into the recovery scan. The caller
/// iterates newest-first so the first (newest) terminal and final
/// agentMessage win, mirroring live replacement of `final_text`. Records for
/// other sessions or turns are skipped. A record this turn cannot decode —
/// or an unknown terminal for this turn — ends provability instead of
/// guessing, exactly like the snapshot readback treats an undecodable
/// exact-turn record as unproven.
fn fold_recovery_event(
    scan: &mut RecoveryScan,
    event: &NativeUnframedViewEvent,
    session_id: &str,
    turn_id: &str,
) -> RecoveryFold {
    match event.method.as_str() {
        "turn/completed" => {
            let Ok(completed) =
                serde_json::from_value::<NativeTurnCompletedNotification>(event.params.clone())
            else {
                return RecoveryFold::Unproven;
            };
            if completed.session_id != session_id || completed.turn_id != turn_id {
                return RecoveryFold::Continue;
            }
            if completed.terminal == NativeTurnTerminal::Unknown {
                return RecoveryFold::Unproven;
            }
            if scan.turn_params.is_none() {
                scan.turn_params = Some(event.params.clone());
            }
        }
        "item/completed" => {
            let Ok(completed) =
                serde_json::from_value::<NativeItemCompletedNotification>(event.params.clone())
            else {
                return RecoveryFold::Unproven;
            };
            if completed.session_id != session_id
                || completed.item.kind != "agentMessage"
                || completed.item.turn_id.as_deref() != Some(turn_id)
            {
                return RecoveryFold::Continue;
            }
            if scan.message_params.is_none()
                && let Some(text) = completed.item.text.as_deref()
                && text.len() <= MAX_EVENT_BYTES
            {
                scan.message_params = Some(event.params.clone());
            }
        }
        _ => {}
    }
    if scan.turn_params.is_some() && scan.message_params.is_some() {
        RecoveryFold::Complete
    } else {
        RecoveryFold::Continue
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acknowledged_message_skips_recovery_write_after_usage_batch() {
        use eliot_plugin::{Peer, protocol::RpcRequest};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, duplex, split};

        let (host_io, plugin_io) = duplex(64 * 1024);
        let (plugin_read, plugin_write) = split(plugin_io);
        let mut peer = Peer::from_io(plugin_read, plugin_write);
        let host = peer.client();
        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });
        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);

        let acknowledge = async {
            let mut native_ids = HashSet::new();
            for (index, kind) in ["message.completed", "session.usage", "turn.completed"]
                .into_iter()
                .enumerate()
            {
                let mut line = String::new();
                host_reader.read_line(&mut line).await.unwrap();
                let request: RpcRequest = serde_json::from_str(&line).unwrap();
                assert_eq!(request.method, "events.put");
                let event = &request.params["events"][0];
                assert_eq!(event["kind"], kind);
                assert!(native_ids.insert(event["native_id"].as_str().unwrap().to_owned()));
                if kind == "message.completed" {
                    assert_eq!(
                        event["native_id"],
                        event_id("session-1", "turn-1", "message-1", "message.completed").unwrap()
                    );
                    assert_eq!(event["data"]["command_id"], "command-1");
                    assert_eq!(event["data"]["turn_id"], "turn-1");
                }
                if kind == "turn.completed" {
                    assert_eq!(event["data"]["status"], "failed");
                    assert_eq!(event["data"]["final_text"], "committed final answer");
                    assert_eq!(
                        event["data"][VIEW_RECOVERY_MARKER_FIELD],
                        VIEW_RECOVERY_READBACK
                    );
                }
                let seq = index + 1;
                let response = json!({
                    "jsonrpc": "2.0", "id": request.id,
                    "result": {"first_seq": seq, "last_seq": seq, "cursor": null},
                });
                host_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            }
        };
        let commits = async {
            let mut sink = CommitSink::new(host.clone(), "credential");
            let mut writes_blocked = false;
            let mut gap_reported = false;
            let mut gap_number = 0;
            let mut source_unknown = false;
            let mut batch_unknown = false;
            let mut failure = None;
            let mut source = StreamSource {
                sink: &mut sink,
                host: &host,
                credential: "credential",
                agent_id: "a1",
                generation: 1,
                harness: "Muse",
                id_prefix: "muse",
                families: &["session", "turn", "message"],
                writes_blocked: &mut writes_blocked,
                gap_reported: &mut gap_reported,
                gap_number: &mut gap_number,
                source_unknown: &mut source_unknown,
                batch_unknown: &mut batch_unknown,
                failure: &mut failure,
            };
            let mut messages = TurnMessages::default();
            let message_id =
                event_id("session-1", "turn-1", "message-1", "message.completed").unwrap();
            messages
                .complete(
                    &mut source,
                    message_id.clone(),
                    "committed final answer".to_owned(),
                    json!({"command_id": "command-1", "turn_id": "turn-1", "text": "committed final answer"}),
                )
                .await
                .unwrap();
            // In production this advances Store's last-batch replay identity:
            // submitting the old message again would no longer be a last-batch retry.
            let token_usage: NativeSessionTokenUsageNotification = serde_json::from_value(json!({
                "sessionId": "session-1", "turnId": "turn-1",
                "cumulative": {"promptTokens": 120, "outputTokens": 30, "totalTokens": 150},
            }))
            .unwrap();
            let usage = session_usage_from_token_usage(&token_usage, None).unwrap();
            source
                .commit_event(
                    "session.usage",
                    event_id("session-1", "turn-1", "session-1", "session.usage").unwrap(),
                    json!({"usage": usage}),
                )
                .await
                .unwrap();

            let status = EventStatus {
                session_id: Some("session-1".to_owned()),
                active_turn_id: Some("turn-1".to_owned()),
                active_command_id: Some("command-1".to_owned()),
                ..EventStatus::default()
            };
            let terminal = json!({
                "sessionId": "session-1", "turnId": "turn-1", "viewCursor": "rev-9",
                "terminal": "failed", "reason": "incomplete",
            });
            assert!(needs_terminal_fence(&status, &terminal));
            assert_eq!(
                projection_turn_state(
                    &projection_session_read("idle", Value::Null),
                    "session-1",
                    "turn-1"
                ),
                ProjectionTurnState::Idle
            );
            // A newer cursor/revision still names the same Store identity.
            // Its snapshot text must not replace text already acknowledged live.
            let (scan, folded) = fold_recovery_all(
                &[
                    recovery_event("turn/completed", terminal),
                    recovery_event(
                        "item/completed",
                        json!({
                            "sessionId": "session-1", "viewCursor": "rev-8",
                            "item": {"itemId": "message-1", "kind": "agentMessage",
                                     "revision": 2, "status": "completed",
                                     "turnId": "turn-1", "text": "readback snapshot text"},
                        }),
                    ),
                ],
                "session-1",
                "turn-1",
            );
            assert!(matches!(folded, RecoveryFold::Complete));
            let recovered: NativeItemCompletedNotification =
                serde_json::from_value(scan.message_params.unwrap()).unwrap();
            let recovered_text = recovered.item.text.unwrap();
            messages
                .complete(
                    &mut source,
                    event_id(
                        "session-1",
                        "turn-1",
                        &recovered.item.item_id,
                        "message.completed",
                    )
                    .unwrap(),
                    recovered_text.clone(),
                    json!({"command_id": "command-1", "turn_id": "turn-1", "text": recovered_text}),
                )
                .await
                .unwrap();
            assert_eq!(messages.acknowledged.len(), 1);
            assert!(messages.acknowledged.contains(&message_id));
            let terminal: NativeTurnCompletedNotification =
                serde_json::from_value(scan.turn_params.unwrap()).unwrap();
            let mut data = json!({
                "command_id": "command-1", "turn_id": terminal.turn_id,
                "status": turn_status(&terminal.terminal).unwrap(),
                "outcome": turn_outcome(&terminal.terminal),
                "native_status": native_terminal(&terminal.terminal),
            });
            attach_final_text(
                &mut data,
                messages.final_text.as_deref().unwrap(),
                true,
                &SecretPolicy::default(),
            );
            mark_readback_recovery(&mut data);
            source
                .commit_event(
                    "turn.completed",
                    event_id("session-1", "turn-1", "turn-1", "turn.completed").unwrap(),
                    data,
                )
                .await
                .unwrap();
            assert!(!*source.writes_blocked);

            // An unacknowledged completion cannot enter the dedup set or
            // replace the committed text, even if the source is blocked.
            *source.writes_blocked = true;
            assert!(
                messages
                    .complete(
                        &mut source,
                        "unacknowledged".to_owned(),
                        "uncommitted".to_owned(),
                        json!({})
                    )
                    .await
                    .is_err()
            );
            assert_eq!(messages.acknowledged.len(), 1);
            assert_eq!(
                messages.final_text.as_deref(),
                Some("committed final answer")
            );
        };
        timeout_at(Instant::now() + Duration::from_secs(5), async {
            tokio::join!(acknowledge, commits);
        })
        .await
        .expect("message, usage and terminal acknowledgements finish");
        peer_task.abort();
        let _ = peer_task.await;
    }

    #[test]
    fn replayed_view_cursor_skips_second_application() {
        // Live facts A and B queued past `after` alongside their `(after,
        // head]` replay must normalize once: the first application wins and
        // the replayed copy is recognized before normalization.
        let mut applied = AppliedViewCursors::default();
        assert!(!applied.contains("cursor-a"));
        applied.insert("cursor-a".to_owned());
        assert!(applied.contains("cursor-a"));
        assert!(!applied.contains("cursor-b"));
        applied.insert("cursor-b".to_owned());
        assert!(applied.contains("cursor-b"));
        // Re-inserting an applied cursor changes nothing.
        applied.insert("cursor-a".to_owned());
        assert!(applied.contains("cursor-a"));
        assert!(applied.contains("cursor-b"));
    }

    #[test]
    fn turn_completed_sharing_the_final_item_cursor_is_not_a_replay() {
        // Live 2026-11-13: every Muse turn stayed busy after its final
        // message because `turn/completed` reused the revision cursor of the
        // final `item/completed` and was dropped as an applied replay.
        let item = json!({
            "sessionId": "session-1",
            "viewCursor": "rev-9",
            "item": {"itemId": "item-final", "revision": 3, "turnId": "turn-1"},
        });
        let turn = json!({"sessionId": "session-1", "viewCursor": "rev-9", "turnId": "turn-1"});
        let mut applied = AppliedViewCursors::default();
        applied.insert(applied_fact_key("item/completed", "rev-9", &item));
        assert!(!applied.contains(&applied_fact_key("turn/completed", "rev-9", &turn)));
        // A replayed copy of the very same fact is still recognized.
        assert!(applied.contains(&applied_fact_key("item/completed", "rev-9", &item)));
        // Two items in one revision are distinct facts.
        let other = json!({
            "sessionId": "session-1",
            "viewCursor": "rev-9",
            "item": {"itemId": "item-tool", "revision": 1, "turnId": "turn-1"},
        });
        assert!(!applied.contains(&applied_fact_key("item/completed", "rev-9", &other)));
    }

    #[test]
    fn applied_cursor_history_evicts_oldest_first() {
        let mut applied = AppliedViewCursors::default();
        for index in 0..(APPLIED_VIEW_HISTORY + 8) {
            applied.insert(format!("cursor-{index}"));
        }
        // Bounded memory: only the newest window is remembered.
        assert!(applied.order.len() <= APPLIED_VIEW_HISTORY);
        assert!(applied.seen.len() <= APPLIED_VIEW_HISTORY);
        assert!(!applied.contains("cursor-0"));
        assert!(applied.contains(&format!("cursor-{}", APPLIED_VIEW_HISTORY + 7)));
    }

    #[test]
    fn association_arms_readback_only_for_latched_view_loss_without_recovery() {
        // Gap recorded while `turn/start` awaited its reply: no active turn
        // existed to arm recovery for, so the fresh association must arm it.
        let latched = EventStatus {
            view_unavailable: true,
            active_turn_id: Some("turn-1".to_owned()),
            ..EventStatus::default()
        };
        assert!(should_arm_recovery_after_associate(&latched, false));
        // An existing recovery deadline must not be replaced.
        assert!(!should_arm_recovery_after_associate(&latched, true));
        // No latched view loss: nothing to arm.
        let healthy = EventStatus {
            active_turn_id: Some("turn-1".to_owned()),
            ..EventStatus::default()
        };
        assert!(!should_arm_recovery_after_associate(&healthy, false));
        // No active turn: nothing to settle.
        let bare = EventStatus::default();
        assert!(!should_arm_recovery_after_associate(&bare, false));
        let latched_no_turn = EventStatus {
            view_unavailable: true,
            ..EventStatus::default()
        };
        assert!(!should_arm_recovery_after_associate(
            &latched_no_turn,
            false
        ));
    }

    fn projection_session_read(status: &str, active: Value) -> Value {
        json!({
            "session": {"sessionId": "session-1", "status": status, "activeTurnId": active},
            "history": {"mode": "none", "noneReason": "projectionUnavailable", "items": null, "snapshot": null},
            "viewCursor": "cursor-1",
        })
    }

    #[test]
    fn projection_terminal_waits_for_the_exact_running_turn() {
        // A page can contain a synthetic failed/incomplete for a source
        // prefix while the same native run still produces tools/model steps.
        let mut result = projection_session_read("running", json!("turn-1"));
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Running
        );
        result["session"]["activeTurnId"] = json!("other-turn");
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
        result["session"]["activeTurnId"] = Value::Null;
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
        result["session"]["status"] = json!("idle");
        // Idle permits fresh paging; this predicate supplies no verdict.
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Idle
        );
        result["session"]["sessionId"] = json!("foreign-session");
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
    }

    #[test]
    fn projection_idle_proof_requires_explicit_null_and_a_loaded_session() {
        let mut result = projection_session_read("idle", Value::Null);
        result["session"]
            .as_object_mut()
            .unwrap()
            .remove("activeTurnId");
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
        result["session"]["activeTurnId"] = json!(false);
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
        result = projection_session_read("notLoaded", Value::Null);
        assert_eq!(
            projection_turn_state(&result, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
    }

    #[test]
    fn recovery_retry_backs_off_to_the_event_deadline() {
        assert_eq!(recovery_retry_delay(1), Duration::from_secs(5));
        assert_eq!(recovery_retry_delay(2), Duration::from_secs(10));
        assert_eq!(recovery_retry_delay(3), Duration::from_secs(20));
        assert_eq!(recovery_retry_delay(4), Duration::from_secs(30));
        assert_eq!(recovery_retry_delay(5), Duration::from_secs(30));
        assert_eq!(recovery_retry_delay(100), Duration::from_secs(30));
    }

    #[test]
    fn projection_read_failures_settle_unknown_at_the_cap() {
        // #427: an unrecoverable view must not keep the turn busy forever.
        // Only the capped consecutive unproven readback settles unknown;
        // exact running proof always keeps the turn and resets the count.
        let running = projection_session_read("running", json!("turn-1"));
        let idle = projection_session_read("idle", Value::Null);
        let foreign = projection_session_read("running", json!("other-turn"));
        for prior in [0, 3, 4, 100, u32::MAX] {
            assert_eq!(
                decide_recovery_step(
                    prior,
                    projection_turn_state(&running, "session-1", "turn-1"),
                    "projectionUnavailable",
                    "session/read proved the exact turn running",
                    true,
                ),
                RecoveryStep::KeepRunning
            );
        }
        for result in [&idle, &foreign] {
            let state = projection_turn_state(result, "session-1", "turn-1");
            for (prior, expected) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
                assert_eq!(
                    decide_recovery_step(
                        prior,
                        state,
                        "projectionUnavailable",
                        "readback proved nothing",
                        true,
                    ),
                    RecoveryStep::Retry { attempts: expected }
                );
                assert!(recovery_retry_delay(expected) <= EVENT_DEADLINE);
            }
            let RecoveryStep::SettleUnknown { attempts, reason } = decide_recovery_step(
                4,
                state,
                "projectionUnavailable",
                "readback proved nothing",
                true,
            ) else {
                panic!("capped unproven readback must settle unknown");
            };
            assert_eq!(attempts, 5);
            assert_eq!(attempts, MAX_RECOVERY_READBACK_ATTEMPTS);
            assert!(reason.starts_with(MUSE_VIEW_UNKNOWN_REASON_PREFIX));
            assert!(reason.contains("projectionUnavailable"));
        }
    }

    #[test]
    fn transport_loss_retries_past_the_cap_without_settling() {
        // A queue overflow without an acknowledged unavailable-view episode
        // must never settle a turn as a projection gap: the cap and the
        // counted prefix bind to the projection episode only.
        let idle = projection_session_read("idle", Value::Null);
        let state = projection_turn_state(&idle, "session-1", "turn-1");
        for prior in [0, 4, 100, u32::MAX] {
            assert_eq!(
                decide_recovery_step(
                    prior,
                    state,
                    "unknown",
                    "session/read proved nothing",
                    false,
                ),
                RecoveryStep::Retry {
                    attempts: prior.saturating_add(1)
                }
            );
        }
        // Even an exact running observation keeps the turn on both paths.
        assert_eq!(
            decide_recovery_step(
                u32::MAX,
                ProjectionTurnState::Running,
                "unknown",
                "session/read proved the exact turn running",
                false,
            ),
            RecoveryStep::KeepRunning
        );
    }

    #[test]
    fn recovery_unknown_reason_keeps_the_counted_prefix_with_exact_detail() {
        let reason = recovery_unknown_reason(
            "projectionUnavailable",
            "session/read transport failed (timeout)",
            5,
        );
        assert!(reason.starts_with(MUSE_VIEW_UNKNOWN_REASON_PREFIX));
        assert!(reason.contains("projectionUnavailable"));
        assert!(reason.contains("session/read transport failed (timeout)"));
        assert!(reason.contains("5"));
    }

    #[test]
    fn recovery_step_runs_the_native_running_to_idle_sequence() {
        // Real native `session/read` shapes through the real parser: first the
        // turn is still running, then the session is idle with no proven
        // terminal. The step decision must keep the Swarm turn active while
        // running is proven, retry boundedly, and settle unknown only at the
        // cap with the exact readback failure.
        let running = projection_session_read("running", json!("turn-1"));
        assert_eq!(
            projection_turn_state(&running, "session-1", "turn-1"),
            ProjectionTurnState::Running
        );
        // An exact running observation always keeps the turn, even after
        // earlier readback failures.
        for prior in [0, 3, 4, 100] {
            assert_eq!(
                decide_recovery_step(
                    prior,
                    ProjectionTurnState::Running,
                    "projectionUnavailable",
                    "session/read transport failed (timeout)",
                    true,
                ),
                RecoveryStep::KeepRunning
            );
        }
        let idle = projection_session_read("idle", Value::Null);
        assert_eq!(
            projection_turn_state(&idle, "session-1", "turn-1"),
            ProjectionTurnState::Idle
        );
        let detail = "idle session/read proved no active turn but the bounded view/page scan held no terminal for the exact turn";
        for (prior, expected) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
            assert_eq!(
                decide_recovery_step(
                    prior,
                    ProjectionTurnState::Idle,
                    "projectionUnavailable",
                    detail,
                    true
                ),
                RecoveryStep::Retry { attempts: expected }
            );
        }
        let RecoveryStep::SettleUnknown { attempts, reason } = decide_recovery_step(
            4,
            ProjectionTurnState::Idle,
            "projectionUnavailable",
            detail,
            true,
        ) else {
            panic!("capped idle readback must settle unknown");
        };
        assert_eq!(attempts, 5);
        assert!(reason.starts_with(MUSE_VIEW_UNKNOWN_REASON_PREFIX));
        assert!(reason.contains("projectionUnavailable"));
        assert!(reason.contains(detail));
        // An unproven shape retries the same way and settles with its own
        // exact failure at the cap.
        let foreign = projection_session_read("running", json!("other-turn"));
        assert_eq!(
            projection_turn_state(&foreign, "session-1", "turn-1"),
            ProjectionTurnState::Unproven
        );
        let foreign_detail = describe_unproven_session_read(&foreign, "session-1", "turn-1");
        assert_eq!(
            decide_recovery_step(
                0,
                ProjectionTurnState::Unproven,
                "projectionUnavailable",
                &foreign_detail,
                true
            ),
            RecoveryStep::Retry { attempts: 1 }
        );
        let RecoveryStep::SettleUnknown { reason, .. } = decide_recovery_step(
            4,
            ProjectionTurnState::Unproven,
            "projectionUnavailable",
            &foreign_detail,
            true,
        ) else {
            panic!("capped unproven readback must settle unknown");
        };
        assert!(reason.contains("another turn"));
    }

    #[test]
    fn unproven_session_read_names_the_exact_failure() {
        let mut result = projection_session_read("running", json!("other-turn"));
        assert!(
            describe_unproven_session_read(&result, "session-1", "turn-1").contains("another turn")
        );
        result = projection_session_read("notLoaded", Value::Null);
        assert!(
            describe_unproven_session_read(&result, "session-1", "turn-1").contains("notLoaded")
        );
        result = projection_session_read("idle", Value::Null);
        result["session"]["sessionId"] = json!("foreign-session");
        assert!(
            describe_unproven_session_read(&result, "session-1", "turn-1")
                .contains("foreign session")
        );
        result = projection_session_read("idle", Value::Null);
        result["session"]
            .as_object_mut()
            .unwrap()
            .remove("activeTurnId");
        assert!(
            describe_unproven_session_read(&result, "session-1", "turn-1")
                .contains("omitted activeTurnId")
        );
    }

    #[test]
    fn recovery_yields_native_calls_to_an_in_flight_stop() {
        // A requested stop owns the remaining stop budget for native
        // termination (the retained stop id blocks a cooperative retry once
        // it expires), so recovery must not spend another bounded call once
        // `planned_stop` is set: at most the single in-flight call may still
        // overlap the stop.
        assert!(recovery_may_spend_native_call(false));
        assert!(!recovery_may_spend_native_call(true));
        // That in-flight call is bounded by one page deadline, strictly
        // inside the host's native stop budget, so the stop keeps time for
        // native termination and output drain.
        assert!(
            RECOVERY_PAGE_DEADLINE < STOP_NATIVE_BUDGET,
            "one in-flight page must leave stop budget for termination"
        );
    }

    #[test]
    fn fenced_terminal_replay_preserves_the_failure_count() {
        // Four failed readbacks plus one unavailable-view terminal replay
        // must count the next failure as attempt five, not restart at one:
        // the replay proves nothing, so only a proven state resets the
        // consecutive-failure count.
        let armed = ViewRecovery {
            head: None,
            head_seen: false,
            deadline: Instant::now(),
            projection: true,
            unproven_attempts: 4,
        };
        assert_eq!(fenced_recovery_attempts(Some(&armed)), 4);
        assert_eq!(fenced_recovery_attempts(None), 0);
    }

    #[test]
    fn unproven_status_truncation_respects_char_boundaries() {
        // 31 ASCII bytes plus one two-byte `Ä`: byte 32 sits inside the
        // multi-byte char, where `String::truncate(32)` would panic and take
        // down the event task. The bound floors to the last full char.
        let status = format!("{}Ä", "a".repeat(31));
        assert_eq!(status.len(), 33);
        assert_eq!(eliot_core::bound_chars(&status, 32), "a".repeat(31));
        assert_eq!(eliot_core::bound_chars(&"b".repeat(40), 32), "b".repeat(32));
        assert_eq!(eliot_core::bound_chars("short", 32), "short");
        // A four-byte emoji straddling the bound floors the same way.
        let bounded = eliot_core::bound_chars(&format!("{}💥tail", "c".repeat(30)), 32);
        assert_eq!(bounded, "c".repeat(30));
        // The regression through the real path: an unknown non-ASCII status
        // on the wire must describe the failure, never panic.
        let result = projection_session_read(&format!("{}Ä", "a".repeat(31)), Value::Null);
        let detail = describe_unproven_session_read(&result, "session-1", "turn-1");
        assert!(
            detail.contains(&"a".repeat(31)),
            "detail names the bounded status"
        );
        assert!(!detail.contains('Ä'), "detail drops the split char");
    }

    #[test]
    fn synthetic_failure_requires_confirmation_even_without_a_health_push() {
        let mut status = EventStatus {
            session_id: Some("session-1".to_owned()),
            active_turn_id: Some("turn-1".to_owned()),
            ..EventStatus::default()
        };
        let mut terminal = json!({
            "sessionId": "session-1", "turnId": "turn-1", "viewCursor": "cursor-1",
            "terminal": "failed", "reason": "incomplete",
        });
        assert!(needs_terminal_fence(&status, &terminal));
        terminal["error"] = json!({"kind": "internal", "message": "failed", "retryable": false});
        assert!(!needs_terminal_fence(&status, &terminal));
        terminal["terminal"] = json!("completed");
        terminal.as_object_mut().unwrap().remove("error");
        assert!(!needs_terminal_fence(&status, &terminal));
        status.view_unavailable = true;
        assert!(needs_terminal_fence(&status, &terminal));
        terminal["turnId"] = json!("foreign-turn");
        assert!(!needs_terminal_fence(&status, &terminal));
    }

    #[test]
    fn readback_recovery_marks_the_terminal_with_the_counted_signal() {
        let mut data = json!({"command_id": "command", "turn_id": "turn", "status": "completed"});
        mark_readback_recovery(&mut data);
        assert_eq!(
            data.get(VIEW_RECOVERY_MARKER_FIELD),
            Some(&Value::from(VIEW_RECOVERY_READBACK))
        );
        // The marker is the only addition: terminal shape is unchanged.
        let mut expected =
            json!({"command_id": "command", "turn_id": "turn", "status": "completed"});
        expected[VIEW_RECOVERY_MARKER_FIELD] = Value::from(VIEW_RECOVERY_READBACK);
        assert_eq!(data, expected);
    }

    fn recovery_event(method: &str, params: Value) -> NativeUnframedViewEvent {
        NativeUnframedViewEvent {
            method: method.to_owned(),
            params,
        }
    }

    fn fold_recovery_all(
        events: &[NativeUnframedViewEvent],
        session_id: &str,
        turn_id: &str,
    ) -> (RecoveryScan, RecoveryFold) {
        let mut scan = RecoveryScan::default();
        let mut folded = RecoveryFold::Continue;
        for event in events {
            folded = fold_recovery_event(&mut scan, event, session_id, turn_id);
            if matches!(folded, RecoveryFold::Complete | RecoveryFold::Unproven) {
                break;
            }
        }
        (scan, folded)
    }

    #[test]
    fn recovery_fold_proves_terminal_and_final_message_from_real_shapes() {
        // Real 1.4 notification shapes (stable schema): the durable view
        // holds the exact turn's terminal plus its final agentMessage,
        // newest first, with older tool noise and a foreign turn around it.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![
            recovery_event(
                "turn/completed",
                json!({
                    "sessionId": session, "terminal": "completed", "turnId": turn,
                    "viewCursor": "rev-9",
                }),
            ),
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-9",
                    "item": {"itemId": "item-final", "kind": "agentMessage",
                             "revision": 3, "status": "completed",
                             "turnId": turn, "text": "done"},
                }),
            ),
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-7",
                    "item": {"itemId": "item-tool", "kind": "toolCall",
                             "revision": 2, "status": "completed",
                             "turnId": turn, "tool": "shell",
                             "args": "{\"command\":\"cargo build\"}"},
                }),
            ),
            recovery_event(
                "turn/completed",
                json!({
                    "sessionId": session, "terminal": "completed",
                    "turnId": "turn-9", "viewCursor": "rev-6",
                }),
            ),
        ];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Complete));
        let terminal: NativeTurnCompletedNotification =
            serde_json::from_value(scan.turn_params.expect("terminal proven"))
                .expect("recovery must keep the live turn/completed shape");
        assert_eq!(terminal.turn_id, turn);
        assert_eq!(terminal.terminal, NativeTurnTerminal::Completed);
        let message: NativeItemCompletedNotification =
            serde_json::from_value(scan.message_params.expect("message proven"))
                .expect("recovery must keep the live item/completed shape");
        assert_eq!(message.item.text.as_deref(), Some("done"));
    }

    #[test]
    fn recovery_fold_newest_final_message_wins() {
        // Two agentMessages for the exact turn, newest first: live delivery
        // replaces `final_text` on each one, so the scan keeps the latest.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-8",
                    "item": {"itemId": "item-new", "kind": "agentMessage",
                             "revision": 4, "status": "completed",
                             "turnId": turn, "text": "second"},
                }),
            ),
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-7",
                    "item": {"itemId": "item-old", "kind": "agentMessage",
                             "revision": 3, "status": "completed",
                             "turnId": turn, "text": "first"},
                }),
            ),
        ];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Continue));
        assert!(scan.turn_params.is_none());
        let message: NativeItemCompletedNotification =
            serde_json::from_value(scan.message_params.expect("message proven"))
                .expect("live shape");
        assert_eq!(message.item.item_id, "item-new");
        assert_eq!(message.item.text.as_deref(), Some("second"));
    }

    #[test]
    fn recovery_fold_skips_foreign_sessions_and_turns() {
        // Other turns and sessions never prove this turn's terminal, even
        // when their records are well-formed.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![
            recovery_event(
                "turn/completed",
                json!({
                    "sessionId": session, "terminal": "completed",
                    "turnId": "turn-9", "viewCursor": "rev-6",
                }),
            ),
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-6",
                    "item": {"itemId": "item-foreign", "kind": "agentMessage",
                             "revision": 1, "status": "completed",
                             "turnId": "turn-9", "text": "not ours"},
                }),
            ),
            recovery_event(
                "turn/completed",
                json!({
                    "sessionId": "session-2", "terminal": "completed",
                    "turnId": turn, "viewCursor": "rev-5",
                }),
            ),
        ];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Continue));
        assert!(scan.turn_params.is_none());
        assert!(scan.message_params.is_none());
    }

    #[test]
    fn recovery_fold_proves_non_completed_terminals() {
        // Recovery is not success-only: a durably failed terminal commits as
        // `failed`, never as `unknown`.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![recovery_event(
            "turn/completed",
            json!({
                "sessionId": session, "terminal": "failed", "turnId": turn,
                "viewCursor": "rev-9",
                "error": {"kind": "authRequired", "message": "login expired",
                          "retryable": true},
            }),
        )];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Continue));
        let terminal: NativeTurnCompletedNotification =
            serde_json::from_value(scan.turn_params.expect("terminal proven")).expect("live shape");
        assert_eq!(terminal.terminal, NativeTurnTerminal::Failed);
        assert!(scan.message_params.is_none());
    }

    #[test]
    fn busy_stop_cancelled_frame_keeps_exact_turn_and_native_terminal() {
        let events = vec![recovery_event(
            "turn/completed",
            json!({
                "sessionId": "session-1", "turnId": "turn-1",
                "viewCursor": "rev-9", "terminal": "cancelled",
            }),
        )];
        let (scan, folded) = fold_recovery_all(&events, "session-1", "turn-1");
        assert!(matches!(folded, RecoveryFold::Continue));
        let terminal: NativeTurnCompletedNotification =
            serde_json::from_value(scan.turn_params.unwrap()).unwrap();
        assert_eq!(terminal.terminal, NativeTurnTerminal::Cancelled);
        assert_eq!(
            super::super::turn_outcome(&terminal.terminal),
            Some("interrupted")
        );
        let (foreign, _) = fold_recovery_all(&events, "session-1", "other-turn");
        assert!(foreign.turn_params.is_none());
    }

    #[test]
    fn recovery_fold_unknown_terminal_is_unproven() {
        // A wire-open terminal the adapter does not model can never prove
        // the turn: the unknown settlement below stays honest.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![recovery_event(
            "turn/completed",
            json!({
                "sessionId": session, "terminal": "futureTerminal",
                "turnId": turn, "viewCursor": "rev-9",
            }),
        )];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Unproven));
        assert!(scan.turn_params.is_none());
    }

    #[test]
    fn recovery_fold_undecodable_record_is_unproven() {
        // An exact-turn record that does not match the 1.4 schema ends
        // provability instead of guessing, like the snapshot readback.
        let session = "session-1";
        let turn = "turn-1";
        let (scan, folded) = fold_recovery_all(
            &[recovery_event("item/completed", json!({"bogus": true}))],
            session,
            turn,
        );
        assert!(matches!(folded, RecoveryFold::Unproven));
        assert!(scan.turn_params.is_none());
        let (scan, folded) = fold_recovery_all(
            &[recovery_event("turn/completed", json!({"bogus": true}))],
            session,
            turn,
        );
        assert!(matches!(folded, RecoveryFold::Unproven));
        assert!(scan.turn_params.is_none());
    }

    #[test]
    fn recovery_fold_oversize_final_text_keeps_terminal_only() {
        // A final message past the persisted-event bound is skipped, but the
        // proven terminal still commits without it — exactly like live
        // delivery, where an oversize message gaps but the terminal follows.
        let session = "session-1";
        let turn = "turn-1";
        let events = vec![
            recovery_event(
                "turn/completed",
                json!({
                    "sessionId": session, "terminal": "completed",
                    "turnId": turn, "viewCursor": "rev-9",
                }),
            ),
            recovery_event(
                "item/completed",
                json!({
                    "sessionId": session, "viewCursor": "rev-9",
                    "item": {"itemId": "item-huge", "kind": "agentMessage",
                             "revision": 3, "status": "completed",
                             "turnId": turn,
                             "text": "x".repeat(MAX_EVENT_BYTES + 1)},
                }),
            ),
        ];
        let (scan, folded) = fold_recovery_all(&events, session, turn);
        assert!(matches!(folded, RecoveryFold::Continue));
        assert!(scan.turn_params.is_some());
        assert!(scan.message_params.is_none());
    }

    #[test]
    fn recovery_page_uses_the_schema_field_names() {
        // `muse schema generate-json-schema` (stable, 1.4.0): cursor-paged
        // reads of the session view, 1-1000 events, newest first.
        assert_eq!(VIEW_PAGE_SIZE, 1_000);
        let params = NativeViewPageParams {
            session_id: "session-1".to_owned(),
            limit: VIEW_PAGE_SIZE,
            direction: Some("backward".to_owned()),
            cursor: None,
        };
        assert_eq!(
            serde_json::to_value(params).expect("params encode"),
            json!({
                "sessionId": "session-1",
                "limit": 1000,
                "direction": "backward",
            })
        );
        let next = NativeViewPageParams {
            session_id: "session-1".to_owned(),
            limit: VIEW_PAGE_SIZE,
            direction: Some("backward".to_owned()),
            cursor: Some("cursor-9".to_owned()),
        };
        assert_eq!(
            serde_json::to_value(next).expect("params encode")["cursor"],
            json!("cursor-9")
        );
    }
}
