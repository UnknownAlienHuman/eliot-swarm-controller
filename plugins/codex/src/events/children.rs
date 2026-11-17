//! Codex child-event registration, buffering and settlement.
//!
//! Parent turns own their facts; this module owns child-thread tracking,
//! child-frame buffering and child-gap settlement behind narrow `pub(super)`
//! interfaces. Pure move from `super`: signatures, transactions, locking and
//! event order unchanged.
use super::*;
/// Bound on tracked child threads, unconfirmed threads and in-flight spawns per
/// generation; overflow is a source gap.
pub(super) const MAX_CHILD_THREADS: usize = 256;
/// Independent child-gap retry cadence: the event loop drives ONE retained
/// child gap per tick until its canonical ACK or shutdown. Same 1s cadence
/// as the host live poll; private, not a public quota config.
pub(super) const CHILD_GAP_RETRY_INTERVAL: Duration = Duration::from_secs(1);
pub(super) struct ChildState {
    binding: ChildBinding,
    writes_blocked: bool,
    gap_reported: bool,
    gap_number: u64,
    source_unknown: bool,
    batch_unknown: bool,
    failure: Option<String>,
    final_text: Option<String>,
    active_turn_id: Option<String>,
    ready_committed: bool,
    pending_gap: Option<SourceGap>,
}

impl ChildState {
    fn new(binding: ChildBinding) -> Self {
        Self {
            binding,
            writes_blocked: false,
            gap_reported: false,
            gap_number: 0,
            source_unknown: false,
            batch_unknown: false,
            failure: None,
            final_text: None,
            active_turn_id: None,
            ready_committed: false,
            pending_gap: None,
        }
    }

    async fn commit_event(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        kind: &str,
        native_id: String,
        data: Value,
    ) -> Result<()> {
        // Split disjoint field borrows: the source borrows latches, `pending`
        // borrows only the retained gap slot.
        let pending = &mut self.pending_gap;
        let mut source = StreamSource {
            sink,
            host,
            credential,
            agent_id: &self.binding.agent_id,
            generation: self.binding.agent_generation,
            harness: "Codex",
            id_prefix: "codex",
            families: &eliot_core::NATIVE_CHILD_FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        };
        source
            .commit_event_with_retained(kind, native_id, data, pending)
            .await
    }

    async fn record_gap(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        reason: &str,
    ) -> Result<()> {
        let pending = &mut self.pending_gap;
        let mut source = StreamSource {
            sink,
            host,
            credential,
            agent_id: &self.binding.agent_id,
            generation: self.binding.agent_generation,
            harness: "Codex",
            id_prefix: "codex",
            families: &eliot_core::NATIVE_CHILD_FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        };
        source.record_gap_with_retained(reason, pending).await
    }
}

/// Native subagent threads of the owned thread. Their frames are not this
/// generation's lifecycle. A 0.167.1 child's first frames can precede both
/// its `thread/started` and the parent's `spawnAgent` announcement, so frames
/// of an unannounced thread are always taken and held as unconfirmed in the
/// bounded early buffer; a non-owned thread's frame never routes to the owned
/// path. Such a thread must still be confirmed (spawn result or
/// `thread/started` parent link) before the window settles, or the settle
/// records a source gap.
#[derive(Default)]
pub(super) struct ChildThreads {
    confirmed: HashSet<String>,
    pending_spawns: HashMap<String, HashSet<String>>,
    unconfirmed: HashMap<String, Option<String>>,
    bound: HashSet<String>,
    pub(super) parents: HashMap<String, String>,
    pub(super) spawn_items: HashMap<String, String>,
}

impl ChildThreads {
    fn take_frame(&mut self, thread: &str) -> std::result::Result<bool, &'static str> {
        if self.confirmed.contains(thread) || self.bound.contains(thread) {
            return Ok(true);
        }
        if self.unconfirmed.contains_key(thread) {
            return Ok(true);
        }
        // No spawn-window gate: a 0.167.1 child's first frames can arrive
        // before the parent's `spawnAgent` item opens a window. Admit the
        // thread as unconfirmed so the child path buffers it; confirmation
        // (spawn result or `thread/started` parent link) or a settle-point gap
        // decides it later. The owned path never sees this frame.
        if self.unconfirmed.len() >= MAX_CHILD_THREADS && !self.unconfirmed.contains_key(thread) {
            return Err("Codex sent frames from more unannounced threads than are tracked");
        }
        self.unconfirmed.insert(thread.to_owned(), None);
        Ok(true)
    }
    fn has_pending(&self) -> bool {
        self.pending_spawns.values().any(|items| !items.is_empty())
    }
    fn pending_count(&self) -> usize {
        self.pending_spawns.values().map(HashSet::len).sum()
    }
    pub(super) fn pending_items<'a>(&'a self, producer: &'a str) -> Vec<&'a String> {
        self.pending_spawns
            .get(producer)
            .map(|items| items.iter().collect::<Vec<_>>())
            .unwrap_or_default()
    }
    pub(super) fn producers(&self) -> Vec<String> {
        self.pending_spawns
            .iter()
            .filter(|(_, items)| !items.is_empty())
            .map(|(producer, _)| producer.clone())
            .collect()
    }
    fn settle_all(&mut self) -> std::result::Result<(), &'static str> {
        let producers = self.producers();
        if !self.has_pending() && self.unconfirmed.is_empty() {
            return Ok(());
        }
        if !self.has_pending() {
            self.unconfirmed.clear();
            return Err("Codex sent frames from a thread no spawn announced");
        }
        let mut error = None;
        for producer in &producers {
            if let Err(reason) = self.settle(producer)
                && error.is_none()
            {
                error = Some(reason);
            }
        }
        match error {
            Some(reason) => Err(reason),
            None => Ok(()),
        }
    }

    pub(super) fn is_parent(&self, owned: &str, parent: &str) -> bool {
        parent == owned || self.confirmed.contains(parent) || self.bound.contains(parent)
    }

    pub(super) fn confirm(
        &mut self,
        owned: &str,
        thread: &str,
    ) -> std::result::Result<(), &'static str> {
        if thread == owned {
            return Err("Codex named the owned thread as a spawned child");
        }
        self.unconfirmed.remove(thread);
        if self.confirmed.len() >= MAX_CHILD_THREADS && !self.confirmed.contains(thread) {
            return Err("Codex announced more native child threads than are tracked");
        }
        self.confirmed.insert(thread.to_owned());
        self.bound.insert(thread.to_owned());
        Ok(())
    }

    fn spawn_started(
        &mut self,
        producer: &str,
        item_id: String,
    ) -> std::result::Result<(), &'static str> {
        let known = self
            .pending_spawns
            .get(producer)
            .is_some_and(|items| items.contains(&item_id));
        if self.pending_count() >= MAX_CHILD_THREADS && !known {
            return Err("Codex had more native spawns in flight than are tracked");
        }
        self.pending_spawns
            .entry(producer.to_owned())
            .or_default()
            .insert(item_id);
        Ok(())
    }

    fn spawn_completed(
        &mut self,
        producer: &str,
        owned: &str,
        item_id: &str,
        receivers: &[String],
    ) -> std::result::Result<(), &'static str> {
        if let Some(items) = self.pending_spawns.get_mut(producer) {
            items.remove(item_id);
        }
        for receiver in receivers {
            self.confirm(owned, receiver)?;
            self.spawn_items
                .insert(receiver.clone(), item_id.to_owned());
        }
        let items = self.pending_spawns.get(producer);
        if items.is_none_or(|items| items.is_empty()) {
            return self.settle(producer);
        }
        Ok(())
    }

    fn settle(&mut self, producer: &str) -> std::result::Result<(), &'static str> {
        self.settle_with_retain(producer, false)
    }

    fn settle_with_retain(
        &mut self,
        producer: &str,
        retain_unattributed: bool,
    ) -> std::result::Result<(), &'static str> {
        self.pending_spawns.remove(producer);
        if retain_unattributed {
            return Ok(());
        }
        if self.producers().is_empty() {
            let count = self.unconfirmed.len();
            self.unconfirmed.clear();
            if count > 0 {
                return Err("Codex sent frames from a thread no spawn announced");
            }
        }
        Ok(())
    }

    fn is_bound(&self, thread: &str) -> bool {
        self.bound.contains(thread)
    }

    fn is_unconfirmed(&self, thread: &str) -> bool {
        self.unconfirmed.contains_key(thread)
    }

    fn parent_of(&self, thread: &str) -> Option<String> {
        self.parents.get(thread).cloned()
    }

    fn spawn_item_of(&self, thread: &str) -> Option<String> {
        self.spawn_items.get(thread).cloned()
    }
}
impl EventProcessor {
    pub(super) fn settle_spawn_window(&mut self) {
        self.settle_spawn_window_for(self.status.thread_id.clone().unwrap_or_default().as_str());
    }

    fn retain_unattributed_excluding(&self, excluding: &str) -> bool {
        self.child_states
            .iter()
            .any(|(thread, state)| thread != excluding && state.active_turn_id.is_some())
    }

    fn settle_spawn_window_for(&mut self, producer: &str) {
        let retain = self.retain_unattributed_excluding(producer);
        if let Err(reason) = self.children.settle_with_retain(producer, retain) {
            self.defer_gap(reason);
        }
        self.collect_orphan_child_state();
    }

    pub(super) fn settle_child_tracking(&mut self) {
        let producers: Vec<String> = self.children.producers();
        if producers.is_empty() {
            if let Err(reason) = self.children.settle_all() {
                self.defer_gap(reason);
            }
        } else {
            for producer in &producers {
                if let Err(reason) = self.children.settle(producer) {
                    self.defer_gap(reason);
                }
            }
        }
        self.collect_orphan_child_state();
        if !self.child_pending.is_empty() {
            self.defer_gap("Codex child registration acknowledgement is unresolved");
            let abandoned: Vec<String> = self.child_pending.keys().cloned().collect();
            for id in abandoned {
                self.child_pending.remove(&id);
                self.release_child_buffer(&id);
                self.child_early_lost.remove(&id);
                if self.children.is_bound(&id) && self.child_unknown.len() < MAX_CHILD_THREADS {
                    self.child_unknown.insert(id);
                }
            }
            self.collect_orphan_child_state();
        }
    }

    /// Whether this frame is a native-child frame (bounded/unconfirmed/confirmed).
    /// Any frame naming a present non-owned thread routes to the child path;
    /// a non-owned thread's frame is never validated as owned. Still-unknown
    /// threads are held as unconfirmed and buffered; settle points gap them
    /// only when no spawn ever announces them.
    pub(super) fn take_child_frame(&mut self, params: &Value) -> bool {
        let Some(thread) = frame_thread_id(params) else {
            return false;
        };
        if self
            .status
            .thread_id
            .as_deref()
            .is_none_or(|owned| owned == thread)
        {
            return false;
        }
        // A thread already retained as unconfirmed while a spawn window was
        // open must keep routing to the child after that window closes
        // (e.g. the root turn completed while A was still active). New
        // unannounced threads still require an open window below.
        if self.children.is_unconfirmed(thread) {
            return true;
        }
        if self.children.is_bound(thread) || self.child_states.contains_key(thread) {
            return true;
        }
        match self.children.take_frame(thread) {
            Ok(taken) => taken,
            Err(reason) => {
                self.defer_gap(reason);
                true
            }
        }
    }

    /// Track spawnAgent calls; completed receivers register before facts publish.
    pub(super) async fn observe_spawn(&mut self, item: &Value, completed: bool) -> Result<()> {
        let Some((item_id, receivers)) = spawn_call(item) else {
            return Ok(());
        };
        let Some(owned) = self.status.thread_id.clone() else {
            return Ok(());
        };
        if !completed {
            return match self.children.spawn_started(&owned, item_id) {
                Ok(()) => Ok(()),
                Err(reason) => self.record_gap(reason).await,
            };
        }
        let outcome = self
            .children
            .spawn_completed(&owned, &owned, &item_id, &receivers);
        for receiver in &receivers {
            if self.children.is_bound(receiver) {
                self.children
                    .parents
                    .insert(receiver.clone(), owned.clone());
            }
        }
        self.collect_orphan_child_state();
        match outcome {
            Ok(()) => self.ensure_receivers_registered(&receivers).await,
            Err(reason) => {
                let confirmed: Vec<String> = receivers
                    .iter()
                    .filter(|receiver| self.children.is_bound(receiver))
                    .cloned()
                    .collect();
                let ensure_error = self.ensure_receivers_registered(&confirmed).await.err();
                let gap_result = self.record_gap(reason).await;
                if let Some(error) = ensure_error {
                    return Err(error);
                }
                gap_result
            }
        }
    }

    /// v2 child announcement: a `subAgentActivity` item with
    /// `kind: "started"` on the producer thread confirms `agentThreadId` as
    /// a native child of that producer, equivalent to a completed spawn's
    /// receiver. The producer is the owned thread or a confirmed child;
    /// activity from any other thread is buffered upstream and never reaches
    /// here unconfirmed, so anything else is ignored without a gap. Any
    /// other `kind` (`interacted` | `interrupted` | `completed`) is a
    /// lifecycle fact with no state-machine transition: never a gap, never a
    /// confirmation, never a fabricated terminal. Like the spawn path,
    /// confirmation registers through `ensure_child_registered`, which flushes
    /// the thread's buffered early frames once the ACK validates.
    pub(super) async fn observe_sub_agent_activity(
        &mut self,
        producer: &str,
        item: &Value,
    ) -> Result<()> {
        let Some((agent_thread, kind)) = sub_agent_activity(item) else {
            return Ok(());
        };
        if kind != "started" {
            return Ok(());
        }
        let owned = self
            .status
            .thread_id
            .as_deref()
            .unwrap_or(producer)
            .to_owned();
        if producer != owned
            && !self.children.is_parent(&owned, producer)
            && !self.child_states.contains_key(producer)
        {
            return Ok(());
        }
        if let Err(reason) = self.children.confirm(&owned, agent_thread) {
            return self.record_child_spawn_gap(producer, reason).await;
        }
        self.children
            .parents
            .insert(agent_thread.to_owned(), producer.to_owned());
        self.collect_orphan_child_state();
        // Boxed: this helper runs on both the owned and the child frame
        // path, and registration flushes buffered child frames back through
        // the child path, so the future needs the same indirection the
        // nested spawn path uses.
        Box::pin(self.ensure_child_registered(agent_thread)).await
    }

    async fn ensure_receivers_registered(&mut self, receivers: &[String]) -> Result<()> {
        let mut first_error = None;
        for receiver in receivers {
            if let Err(error) = self.ensure_child_registered(receiver).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    /// A child request stays unsupported; it is never forwarded to control.
    pub(super) async fn reject_child_request(&mut self, id: Value, method: &str, params: &Value) {
        let rejected = self
            .native
            .reject_unsupported_request(id, method, params, Instant::now() + EVENT_DEADLINE)
            .await;
        if rejected.is_err() {
            self.defer_gap("Codex child-thread request could not be rejected");
        }
    }
    pub(super) fn has_pending_child_gap(&self) -> bool {
        self.child_states
            .values()
            .any(|state| state.pending_gap.is_some())
    }

    /// Fair rotation over retained child gaps: the next pending child after
    /// the last served (sorted thread order, wrap-around), so one bad child
    /// cannot starve the others. Synchronous; the caller owns the send.
    pub(super) fn next_pending_gap(&mut self) -> Option<(String, SourceGap)> {
        let mut pending: Vec<String> = self
            .child_states
            .iter()
            .filter(|(_, state)| state.pending_gap.is_some())
            .map(|(thread, _)| thread.clone())
            .collect();
        if pending.is_empty() {
            return None;
        }
        pending.sort();
        let start = self
            .gap_last_served
            .as_ref()
            .and_then(|last| pending.iter().position(|thread| thread > last))
            .unwrap_or(0);
        let thread = pending[start % pending.len()].clone();
        let gap = self.child_states.get(&thread)?.pending_gap.clone()?;
        self.gap_last_served = Some(thread.clone());
        Some((thread, gap))
    }

    /// Apply one owned attempt result to the matching child only.
    /// Synchronous; holds `&mut` briefly after the RPC completes, never
    /// across the wait. Reuses the shared latch logic.
    pub(super) fn apply_gap_attempt(
        &mut self,
        child_thread: &str,
        attempted: &SourceGap,
        result: &Result<Value>,
    ) -> bool {
        let Some(state) = self.child_states.get_mut(child_thread) else {
            return false;
        };
        if state.binding.agent_id != attempted.agent_id
            || state.binding.agent_generation != attempted.generation
            || state.pending_gap.as_ref() != Some(attempted)
        {
            return false;
        }
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let pending = &mut state.pending_gap;
        let mut source = StreamSource {
            sink,
            host: &host,
            credential: &credential,
            agent_id: &state.binding.agent_id,
            generation: state.binding.agent_generation,
            harness: "Codex",
            id_prefix: "codex",
            families: &eliot_core::NATIVE_CHILD_FAMILIES,
            writes_blocked: &mut state.writes_blocked,
            gap_reported: &mut state.gap_reported,
            gap_number: &mut state.gap_number,
            source_unknown: &mut state.source_unknown,
            batch_unknown: &mut state.batch_unknown,
            failure: &mut state.failure,
        };
        source.apply_retained_attempt(pending, attempted, result)
    }

    async fn record_child_gap_visible(&mut self, child_thread: &str, reason: &str) {
        let Some(state) = self.child_states.get_mut(child_thread) else {
            return;
        };
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        // Child metadata uncertainty stays in child scope: the retained gap,
        // writes_blocked, source_unknown, and failure all live on the child
        // state. A busy child gap must not set the parent loss_reason (no
        // false parent source gap, healthy parent steer stays admitted).
        // Genuine independent parent loss still sets loss_reason on parent
        // paths; this path never clears it.
        let _ = state.record_gap(sink, &host, &credential, reason).await;
    }

    async fn handle_thread_started_error(&mut self, child_thread: &str, error: Error) {
        if self.child_states.contains_key(child_thread) {
            let (should_gap, reason) = {
                let state = self.child_states.get_mut(child_thread).expect("checked");
                state.source_unknown = true;
                let reason = format!(
                    "Codex child thread/started could not be normalized ({})",
                    error.code
                );
                if state.failure.is_none() {
                    state.failure = Some(*error.message);
                }
                (!state.gap_reported && !state.writes_blocked, reason)
            };
            if should_gap {
                self.record_child_gap_visible(child_thread, &reason).await;
            }
            // A retained gap stands: retries belong to the owned in-flight
            // driver; frame handling never awaits retained-gap RPCs.
            return;
        }
        if self.child_pending.contains_key(child_thread)
            || self.child_unknown.contains(child_thread)
        {
            return;
        }
        let reason = format!(
            "Codex child thread/started could not be normalized ({})",
            error.code
        );
        let _ = self.record_gap(&reason).await;
    }

    pub(super) async fn handle_child_notification(&mut self, method: &str, params: Value) {
        let Some(child_thread) = frame_thread_id(&params).map(str::to_owned) else {
            return;
        };
        if method == "thread/started" {
            if let Err(error) = self.observe_thread_started(&params).await {
                self.handle_thread_started_error(&child_thread, error).await;
            }
            return;
        }
        if self
            .child_states
            .get(&child_thread)
            .is_some_and(|state| state.pending_gap.is_some())
        {
            // Retained gap stands: drop this frame (covered by the gap) and
            // leave the retry to the owned in-flight driver. Never await
            // retained-gap RPCs from the frame path.
            return;
        }
        if !self.child_states.contains_key(&child_thread)
            && !self.children.is_bound(&child_thread)
            && !self.children.is_unconfirmed(&child_thread)
            && !self.child_pending.contains_key(&child_thread)
            && !self.child_unknown.contains(&child_thread)
        {
            return;
        }
        if !self
            .child_states
            .get(&child_thread)
            .is_some_and(|state| state.ready_committed)
            && self.child_unknown.contains(&child_thread)
        {
            return;
        }
        if !self
            .child_states
            .get(&child_thread)
            .is_some_and(|state| state.ready_committed)
        {
            if let Some(state) = self.child_states.get(&child_thread)
                && state.writes_blocked
            {
                return;
            }
            if self.child_pending.contains_key(&child_thread) {
                match self.ensure_child_registered(&child_thread).await {
                    Ok(()) => {
                        if !self
                            .child_states
                            .get(&child_thread)
                            .is_some_and(|state| state.ready_committed)
                        {
                            self.buffer_child_frame(
                                &child_thread,
                                BufferedNativeFrame::Notification {
                                    method: method.to_owned(),
                                    params,
                                },
                            );
                            return;
                        }
                    }
                    Err(_) => {
                        if self.child_unknown.contains(&child_thread) {
                            return;
                        }
                        self.buffer_child_frame(
                            &child_thread,
                            BufferedNativeFrame::Notification {
                                method: method.to_owned(),
                                params,
                            },
                        );
                        return;
                    }
                }
            } else {
                self.buffer_child_frame(
                    &child_thread,
                    BufferedNativeFrame::Notification {
                        method: method.to_owned(),
                        params,
                    },
                );
                return;
            }
        }
        if let Err(error) = self
            .process_child_notification(&child_thread, method, params)
            .await
        {
            let (should_gap, reason) = {
                let Some(state) = self.child_states.get_mut(&child_thread) else {
                    return;
                };
                state.source_unknown = true;
                let reason = format!(
                    "Codex child {method} notification could not be normalized ({})",
                    error.code
                );
                if state.failure.is_none() {
                    state.failure = Some(*error.message);
                }
                (!state.gap_reported && !state.writes_blocked, reason)
            };
            if should_gap {
                self.record_child_gap_visible(&child_thread, &reason).await;
            }
        }
    }

    fn buffer_child_frame(&mut self, child_thread: &str, frame: BufferedNativeFrame) {
        if !self.child_buffers.contains_key(child_thread)
            && self.child_buffers.len() >= MAX_CHILD_THREADS
        {
            self.collect_orphan_child_state();
            let proven = self.children.is_bound(child_thread)
                || self.child_states.contains_key(child_thread)
                || self.child_pending.contains_key(child_thread);
            if !proven && self.child_buffers.len() >= MAX_CHILD_THREADS {
                self.defer_gap("Codex child observation exceeded its bound");
                return;
            }
        }
        let size = child_frame_size(&frame);
        let queue = self
            .child_buffers
            .entry(child_thread.to_owned())
            .or_default();
        if queue.len() >= EARLY_EVENT_LIMIT
            || self.child_buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            let discarded: usize = queue.iter().map(child_frame_size).sum();
            queue.clear();
            self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(discarded);
            let trackable = self.children.is_bound(child_thread)
                || self.children.is_unconfirmed(child_thread)
                || self.child_states.contains_key(child_thread)
                || self.child_pending.contains_key(child_thread);
            if trackable {
                self.child_early_lost.insert(child_thread.to_owned());
            } else {
                self.defer_gap("Codex child observation exceeded its bound");
            }
            return;
        }
        self.child_buffer_bytes = self.child_buffer_bytes.saturating_add(size);
        queue.push_back(frame);
    }

    fn collect_orphan_child_state(&mut self) {
        let orphans: Vec<String> = self
            .child_buffers
            .keys()
            .chain(self.child_early_lost.iter())
            .chain(self.child_unknown.iter())
            .chain(self.child_pending.keys())
            .filter(|id| {
                !self.child_states.contains_key(id.as_str())
                    && !self.children.is_bound(id)
                    && !self.children.is_unconfirmed(id)
                    && !self.child_pending.contains_key(id.as_str())
            })
            .cloned()
            .collect();
        for id in orphans {
            if self.child_states.contains_key(&id)
                || self.children.is_bound(&id)
                || self.children.is_unconfirmed(&id)
                || self.child_pending.contains_key(&id)
            {
                continue;
            }
            if let Some(queue) = self.child_buffers.remove(&id) {
                let discarded: usize = queue.iter().map(child_frame_size).sum();
                self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(discarded);
            }
            self.child_early_lost.remove(&id);
            if !self.children.is_bound(&id) {
                self.child_unknown.remove(&id);
            }
        }
    }

    pub(super) async fn ensure_child_registered(&mut self, child_thread: &str) -> Result<()> {
        if self.child_states.contains_key(child_thread) {
            return Ok(());
        }
        if self.child_unknown.contains(child_thread) {
            return Err(Error::new(
                "not_allowed",
                "Codex child registration was rejected",
            ));
        }
        if let Some(retained) = self.child_pending.get(child_thread).cloned() {
            return self.register_pending_child(child_thread, retained).await;
        }
        let registration = self.child_registration_for(child_thread)?;
        if self.child_pending.len() >= MAX_CHILD_THREADS {
            self.record_gap("Codex pending child registrations exceeded their bound")
                .await?;
            return Err(Error::new(
                "unknown",
                "Codex child registration is unresolved",
            ));
        }
        self.child_pending
            .insert(child_thread.to_owned(), registration.clone());
        self.register_pending_child(child_thread, registration)
            .await
    }

    fn child_registration_for(&mut self, child_thread: &str) -> Result<ChildRegistration> {
        let owned = self
            .status
            .thread_id
            .clone()
            .ok_or_else(|| Error::new("bad_state", "Codex producer thread is not ready"))?;
        let parent_thread = self
            .children
            .parent_of(child_thread)
            .unwrap_or_else(|| owned.clone());
        let (parent_id, parent_generation, parent_native_ref) = if parent_thread == owned {
            (self.agent_id.clone(), self.generation, owned)
        } else {
            let parent_state = self.child_states.get(&parent_thread).ok_or_else(|| {
                Error::new(
                    "bad_state",
                    "Codex nested child parent is not registered yet",
                )
            })?;
            (
                parent_state.binding.agent_id.clone(),
                parent_state.binding.agent_generation,
                parent_thread,
            )
        };
        let spawn_item = self
            .children
            .spawn_item_of(child_thread)
            .unwrap_or_else(|| format!("spawn-{child_thread}"));
        Ok(build_registration(
            &spawn_item,
            &parent_id,
            parent_generation,
            &parent_native_ref,
            &self.native_scope,
            child_thread,
        ))
    }

    async fn register_pending_child(
        &mut self,
        child_thread: &str,
        registration: ChildRegistration,
    ) -> Result<()> {
        let ack = match register_child(&self.host, &self.credential, registration.clone()).await {
            Ok(ack) => ack,
            Err(error) if is_admission_rejection(&error) => {
                self.child_pending.remove(child_thread);
                self.release_child_buffer(child_thread);
                self.child_early_lost.remove(child_thread);
                self.child_unknown.insert(child_thread.to_owned());
                self.record_gap("Codex child registration was rejected")
                    .await?;
                return Err(error);
            }
            Err(error) => {
                return Err(Error::new(
                    error.code.as_str(),
                    format!(
                        "Codex child registration acknowledgement is unresolved: {}",
                        error.message
                    ),
                ));
            }
        };
        self.child_pending.remove(child_thread);
        let binding = binding_from_ack(registration, &ack);
        self.child_bindings
            .insert(child_thread.to_owned(), binding.clone());
        let mut child_state = ChildState::new(binding);
        let native_id = format!("codex:{child_thread}:session.ready");
        let native_scope = self.native_scope.clone();
        if let Err(error) = child_state
            .commit_event(
                &mut self.sink,
                &self.host,
                &self.credential,
                "session.ready",
                native_id,
                json!({
                    "native_scope": native_scope,
                    "native_ref": child_thread,
                }),
            )
            .await
        {
            self.child_states
                .insert(child_thread.to_owned(), child_state);
            self.release_child_buffer(child_thread);
            return Err(error);
        }
        child_state.ready_committed = true;
        self.child_states
            .insert(child_thread.to_owned(), child_state);
        let mut first_error = None;
        if self.child_early_lost.remove(child_thread) {
            let host = self.host.clone();
            let credential = self.credential.clone();
            let sink = &mut self.sink;
            if let Some(state) = self.child_states.get_mut(child_thread)
                && let Err(error) = state
                    .record_gap(
                        sink,
                        &host,
                        &credential,
                        "Codex child early events were lost before registration",
                    )
                    .await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Err(error) = self.flush_child_buffer(child_thread).await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn release_child_buffer(&mut self, child_thread: &str) {
        if let Some(queue) = self.child_buffers.remove(child_thread) {
            let discarded: usize = queue.iter().map(child_frame_size).sum();
            self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(discarded);
        }
    }

    async fn flush_child_buffer(&mut self, child_thread: &str) -> Result<()> {
        let Some(mut queue) = self.child_buffers.remove(child_thread) else {
            return Ok(());
        };
        if self
            .child_states
            .get(child_thread)
            .is_some_and(|state| state.pending_gap.is_some())
        {
            // Retained gap stands: drop buffered (covered by the gap) without
            // awaiting retained-gap RPCs; the owned driver settles.
            let discarded: usize = queue.iter().map(child_frame_size).sum();
            self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(discarded);
            return Err(Error::new(
                "unknown",
                "Codex child acknowledgement is unresolved",
            ));
        }
        let mut first_unresolved = None;
        while let Some(frame) = queue.pop_front() {
            let size = child_frame_size(&frame);
            self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(size);
            if self
                .child_states
                .get(child_thread)
                .is_some_and(|state| state.pending_gap.is_some())
            {
                // A retained gap stood up mid-flush: drop this and the rest
                // (covered by the gap) without further RPCs.
                let rest: usize = queue.iter().map(child_frame_size).sum();
                self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(rest);
                return Err(Error::new(
                    "unknown",
                    "Codex child acknowledgement is unresolved",
                ));
            }
            match frame {
                BufferedNativeFrame::Notification { method, params } => {
                    let result = self
                        .process_child_notification(child_thread, &method, params)
                        .await;
                    if let Err(error) = result {
                        let (should_gap, code) = {
                            let Some(state) = self.child_states.get_mut(child_thread) else {
                                continue;
                            };
                            state.source_unknown = true;
                            let code = error.code.clone();
                            if state.failure.is_none() {
                                state.failure = Some(*error.message);
                            }
                            (!state.gap_reported && !state.writes_blocked, code)
                        };
                        if should_gap {
                            let reason = format!(
                                "Codex child {method} notification could not be normalized ({code})",
                            );
                            let host = self.host.clone();
                            let credential = self.credential.clone();
                            let sink = &mut self.sink;
                            if let Some(state) = self.child_states.get_mut(child_thread)
                                && let Err(gap_error) =
                                    state.record_gap(sink, &host, &credential, &reason).await
                                && first_unresolved.is_none()
                            {
                                first_unresolved = Some(gap_error);
                            }
                        } else {
                            // Retained retries belong to the owned driver;
                            // only note blockage here.
                            let still_blocked = self
                                .child_states
                                .get(child_thread)
                                .is_some_and(|state| state.writes_blocked);
                            if still_blocked && first_unresolved.is_none() {
                                first_unresolved = Some(Error::new(
                                    "unknown",
                                    "Codex child acknowledgement is unresolved",
                                ));
                            }
                        }
                    }
                }
                BufferedNativeFrame::Request { id, method, params } => {
                    self.reject_child_request(id, &method, &params).await;
                }
            }
        }
        if let Some(error) = first_unresolved {
            return Err(error);
        }
        Ok(())
    }

    async fn process_child_notification(
        &mut self,
        child_thread: &str,
        method: &str,
        params: Value,
    ) -> Result<()> {
        match method {
            "turn/started" => {
                let turn_id = params
                    .get("turn")
                    .and_then(|turn| turn.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "native_protocol",
                            "Codex child turn/started omitted turn id",
                        )
                    })?;
                let state = self
                    .child_states
                    .get_mut(child_thread)
                    .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
                state.active_turn_id = Some(turn_id.to_owned());
                state.final_text = None;
                let native_id = event_id(child_thread, turn_id, turn_id, "turn.started")?;
                state
                    .commit_event(
                        &mut self.sink,
                        &self.host,
                        &self.credential,
                        "turn.started",
                        native_id,
                        json!({"turn_id": turn_id}),
                    )
                    .await?;
            }
            "item/started" => {
                let raw_item = params.get("item").cloned().unwrap_or(Value::Null);
                self.observe_child_spawn(child_thread, &raw_item, false)
                    .await?;
                // Depth-2 v2 announcement from a confirmed child: same
                // confirmation as the owned thread's, idempotent on repeat.
                self.observe_sub_agent_activity(child_thread, &raw_item)
                    .await?;
            }
            "item/completed" => {
                let raw_item = params.get("item").cloned().unwrap_or(Value::Null);
                self.observe_child_spawn(child_thread, &raw_item, true)
                    .await?;
                self.observe_sub_agent_activity(child_thread, &raw_item)
                    .await?;
                let item_type = raw_item
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if item_type == "agentMessage" {
                    self.complete_child_message(child_thread, &raw_item, &params)
                        .await?;
                }
            }
            "turn/completed" => {
                self.complete_child_turn(child_thread, params).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn complete_child_message(
        &mut self,
        child_thread: &str,
        item: &Value,
        params: &Value,
    ) -> Result<()> {
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Codex child agentMessage omitted item id",
                )
            })?;
        let text = item.get("text").and_then(Value::as_str).ok_or_else(|| {
            Error::new("native_protocol", "Codex child agentMessage omitted text")
        })?;
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let text = policy.redact_text(text);
        let turn_id = child_message_turn_id(
            params,
            self.child_states
                .get(child_thread)
                .and_then(|state| state.active_turn_id.as_deref()),
        );
        let mut data = json!({
            "turn_id": turn_id.as_str(),
            "text": text,
        });
        if let Some(phase) = item.get("phase").and_then(Value::as_str) {
            data["phase"] = Value::String(phase.to_owned());
        }
        let encoded_len = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes.len(),
            Err(_) => usize::MAX,
        };
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        if encoded_len > MAX_EVENT_BYTES {
            return state
                .record_gap(
                    sink,
                    &host,
                    &credential,
                    "Codex child completed message exceeded persisted-event bound",
                )
                .await;
        }
        let native_id = event_id(child_thread, &turn_id, item_id, "message.completed")?;
        state
            .commit_event(
                sink,
                &host,
                &credential,
                "message.completed",
                native_id,
                data,
            )
            .await
    }

    async fn complete_child_turn(&mut self, child_thread: &str, params: Value) -> Result<()> {
        let notification: NativeTurnCompletedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Codex child turn/completed schema mismatch",
                )
            })?;
        self.settle_spawn_window_for(child_thread);
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        if notification.turn.status == NativeTurnStatus::InProgress {
            return state
                .record_gap(
                    sink,
                    &host,
                    &credential,
                    "Codex child turn/completed carried nonterminal inProgress status",
                )
                .await;
        }
        let status_text = turn_status(&notification.turn.status);
        let mut data = json!({
            "turn_id": notification.turn.id,
            "status": status_text,
        });
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        if let Some(text) = state.final_text.take() {
            attach_final_text(&mut data, &text, &notification.turn.status, &policy);
        }
        attach_native_error(&mut data, notification.turn.error.as_ref(), &policy);
        let native_id = event_id(
            child_thread,
            &notification.turn.id,
            &notification.turn.id,
            "turn.completed",
        )?;
        state
            .commit_event(sink, &host, &credential, "turn.completed", native_id, data)
            .await?;
        state.active_turn_id = None;
        Ok(())
    }

    async fn observe_child_spawn(
        &mut self,
        parent_thread: &str,
        item: &Value,
        completed: bool,
    ) -> Result<()> {
        let Some((item_id, receivers)) = spawn_call(item) else {
            return Ok(());
        };
        if !completed {
            return match self.children.spawn_started(parent_thread, item_id) {
                Ok(()) => Ok(()),
                Err(reason) => self.record_child_spawn_gap(parent_thread, reason).await,
            };
        }
        let owned = self.status.thread_id.as_deref().unwrap_or(parent_thread);
        let outcome = self
            .children
            .spawn_completed(parent_thread, owned, &item_id, &receivers);
        for receiver in &receivers {
            if self.children.is_bound(receiver) {
                self.children
                    .parents
                    .insert(receiver.clone(), parent_thread.to_owned());
            }
        }
        self.collect_orphan_child_state();
        match outcome {
            Ok(()) => self.ensure_nested_receivers(&receivers).await,
            Err(reason) => {
                let confirmed: Vec<String> = receivers
                    .iter()
                    .filter(|receiver| self.children.is_bound(receiver))
                    .cloned()
                    .collect();
                let ensure_error = self.ensure_nested_receivers(&confirmed).await.err();
                let gap_result = self.record_child_spawn_gap(parent_thread, reason).await;
                if let Some(error) = ensure_error {
                    return Err(error);
                }
                gap_result
            }
        }
    }

    async fn ensure_nested_receivers(&mut self, receivers: &[String]) -> Result<()> {
        let mut first_error = None;
        for receiver in receivers {
            if let Err(error) = Box::pin(self.ensure_child_registered(receiver)).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    async fn record_child_spawn_gap(&mut self, parent_thread: &str, reason: &str) -> Result<()> {
        if self.child_states.contains_key(parent_thread) {
            let host = self.host.clone();
            let credential = self.credential.clone();
            let sink = &mut self.sink;
            if let Some(state) = self.child_states.get_mut(parent_thread) {
                return state.record_gap(sink, &host, &credential, reason).await;
            }
        }
        self.record_gap(reason).await
    }
}
/// One owned child-gap attempt: cloned identity plus the RPC future.
/// Polled in `run()` without borrowing `ChildState`; the result applies
/// only to the matching child/gen/retained gap.
pub(super) struct GapAttempt {
    pub(super) child_thread: String,
    pub(super) gap: SourceGap,
    pub(super) future: Pin<Box<dyn Future<Output = Result<Value>> + Send>>,
}
/// A `spawnAgent` collab call: its item id and the spawned thread ids
/// (`receiverThreadIds`, filled once the spawn completes).
fn spawn_call(item: &Value) -> Option<(String, Vec<String>)> {
    if item.get("type").and_then(Value::as_str) != Some("collabAgentToolCall")
        || item.get("tool").and_then(Value::as_str) != Some("spawnAgent")
    {
        return None;
    }
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())?;
    let receivers = item
        .get("receiverThreadIds")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|receiver| !receiver.trim().is_empty())
        .map(str::to_owned)
        .collect();
    Some((item_id.to_owned(), receivers))
}

/// A v2 `subAgentActivity` thread item: the announced child thread and its
/// lifecycle kind. Only `kind: "started"` confirms; every other kind is a
/// fact with no transition. Malformed items (wrong type, missing thread or
/// kind) are not activity at all and stay ignored without a gap.
fn sub_agent_activity(item: &Value) -> Option<(&str, &str)> {
    if item.get("type").and_then(Value::as_str) != Some("subAgentActivity") {
        return None;
    }
    let agent_thread = item
        .get("agentThreadId")
        .and_then(Value::as_str)
        .filter(|thread| !thread.trim().is_empty())?;
    let kind = item
        .get("kind")
        .and_then(Value::as_str)
        .filter(|kind| !kind.trim().is_empty())?;
    Some((agent_thread, kind))
}

/// Encoded size of one buffered child frame, shared by buffering,
/// flushing, and overflow accounting so discarded bytes are released exactly.
fn child_frame_size(frame: &BufferedNativeFrame) -> usize {
    match frame {
        BufferedNativeFrame::Notification { method, params } => {
            serde_json::to_vec(&json!({"method": method, "params": params}))
                .map(|bytes| bytes.len())
                .unwrap_or(256)
        }
        BufferedNativeFrame::Request { id, method, params } => {
            serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))
                .map(|bytes| bytes.len())
                .unwrap_or(256)
        }
    }
}

/// The exact already-selected native turn id for a child message: the
/// notification's `turnId`, else the child's active turn, else empty. Owned
/// so the immutable selection ends before the child state is borrowed mutably.
fn child_message_turn_id(params: &Value, active_turn_id: Option<&str>) -> String {
    params
        .get("turnId")
        .and_then(Value::as_str)
        .or(active_turn_id)
        .unwrap_or_default()
        .to_owned()
}

/// The thread a native frame belongs to: `threadId` on notifications and
/// server requests, or `thread.id` on thread notifications.
fn frame_thread_id(params: &Value) -> Option<&str> {
    params.get("threadId").and_then(Value::as_str).or_else(|| {
        params
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
    })
}

pub(super) fn spawned_item_id(thread: &Value) -> Option<&str> {
    let spawn = thread.get("source")?.get("subAgent")?.get("thread_spawn")?;
    spawn
        .get("call_id")
        .or_else(|| spawn.get("item_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
}

/// The parent of a subagent thread, from the installed 0.167.1 v2
/// `SessionSource` subagent shape
/// `{"subAgent": {"thread_spawn": {"parent_thread_id": ...}}}`.
pub(super) fn spawned_parent_thread(thread: &Value) -> Option<&str> {
    thread
        .get("source")?
        .get("subAgent")?
        .get("thread_spawn")?
        .get("parent_thread_id")?
        .as_str()
        .filter(|parent| !parent.trim().is_empty())
}

/// The parent of a subagent thread, from the installed 0.167.1 v2 `Thread`
/// shape's new top-level `parentThreadId`, which the schema sets only when
/// the thread is a subagent. Accepted as an additional confirmation path
/// alongside [`spawned_parent_thread`].
pub(super) fn top_level_parent_thread(thread: &Value) -> Option<&str> {
    thread
        .get("parentThreadId")
        .and_then(Value::as_str)
        .filter(|parent| !parent.trim().is_empty())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawned_parent_reads_only_the_installed_thread_spawn_shape() {
        let child = json!({"id": "child", "source": {"subAgent": {"thread_spawn": {
            "parent_thread_id": "owned", "depth": 1, "agent_nickname": null
        }}}});
        assert_eq!(spawned_parent_thread(&child), Some("owned"));
        for thread in [
            json!({"id": "t", "source": "appServer"}),
            json!({"id": "t", "source": {"subAgent": "review"}}),
            json!({"id": "t", "source": {"subAgent": {"other": "x"}}}),
            json!({"id": "t", "source": {"subAgent": {"thread_spawn": {"parent_thread_id": " "}}}}),
            json!({"id": "t"}),
        ] {
            assert_eq!(spawned_parent_thread(&thread), None, "{thread}");
        }
        assert_eq!(
            frame_thread_id(&json!({"threadId": "child"})),
            Some("child")
        );
        assert_eq!(
            frame_thread_id(&json!({"thread": {"id": "child"}})),
            Some("child")
        );
    }

    #[test]
    fn child_threads_confirm_held_frames_and_gap_leftovers() {
        let mut children = ChildThreads::default();
        // No spawn in flight: an unknown thread is still taken and held as
        // unconfirmed (0.167.1 child frames can precede the spawn window);
        // the settle below decides it. It never routes to the owned path.
        assert_eq!(children.take_frame("x"), Ok(true));
        assert!(children.is_unconfirmed("x"));

        children
            .spawn_started("owned", "call-1".to_owned())
            .unwrap();
        children
            .spawn_started("owned", "call-2".to_owned())
            .unwrap();
        assert_eq!(children.take_frame("child-1"), Ok(true));
        assert_eq!(children.take_frame("child-2"), Ok(true));
        // The first spawn confirms its receiver; the second is still pending.
        assert_eq!(
            children.spawn_completed("owned", "owned", "call-1", &["child-1".to_owned()]),
            Ok(())
        );
        assert_eq!(children.take_frame("child-1"), Ok(true));
        // child-2 was never named by a spawn: the last completion gaps it.
        assert_eq!(
            children.spawn_completed("owned", "owned", "call-2", &[]),
            Err("Codex sent frames from a thread no spawn announced")
        );
        // The settle cleared it, so a later frame is held as unconfirmed
        // again rather than routed to the owned path.
        assert_eq!(children.take_frame("child-2"), Ok(true));
        assert!(children.is_unconfirmed("child-2"));
        assert!(children.is_parent("owned", "child-1"));
        assert!(!children.is_parent("owned", "child-2"));
    }

    #[test]
    fn child_threads_settle_a_stuck_spawn_and_refuse_the_owned_thread() {
        let mut children = ChildThreads::default();
        children.spawn_started("owned", "call".to_owned()).unwrap();
        assert_eq!(children.settle("owned"), Ok(()));
        // Windowless frames are still held as unconfirmed, never owned.
        assert_eq!(children.take_frame("late"), Ok(true));
        assert!(children.is_unconfirmed("late"));

        children.spawn_started("owned", "call".to_owned()).unwrap();
        assert_eq!(children.take_frame("orphan"), Ok(true));
        assert_eq!(
            children.settle("owned"),
            Err("Codex sent frames from a thread no spawn announced")
        );
        assert_eq!(
            children.confirm("owned", "owned"),
            Err("Codex named the owned thread as a spawned child")
        );
    }

    #[test]
    fn child_threads_are_bounded() {
        let mut children = ChildThreads::default();
        children.spawn_started("owned", "call".to_owned()).unwrap();
        for index in 0..MAX_CHILD_THREADS {
            assert_eq!(children.take_frame(&format!("t{index}")), Ok(true));
        }
        assert_eq!(children.take_frame("t0"), Ok(true));
        assert!(children.take_frame("one-more").is_err());
        for index in 0..MAX_CHILD_THREADS {
            children.confirm("owned", &format!("c{index}")).unwrap();
        }
        assert!(children.confirm("owned", "c-extra").is_err());
        for index in 1..MAX_CHILD_THREADS {
            children
                .spawn_started("owned", format!("call-{index}"))
                .unwrap();
        }
        assert!(
            children
                .spawn_started("owned", "call-extra".to_owned())
                .is_err()
        );
    }

    #[test]
    fn spawn_call_reads_only_spawn_agent_collab_items() {
        let completed = json!({
            "type": "collabAgentToolCall", "id": "call-1", "tool": "spawnAgent",
            "senderThreadId": "owned", "receiverThreadIds": ["child-1", " "],
            "status": "completed", "agentsStates": {}
        });
        assert_eq!(
            spawn_call(&completed),
            Some(("call-1".to_owned(), vec!["child-1".to_owned()]))
        );
        let started = json!({
            "type": "collabAgentToolCall", "id": "call-2", "tool": "spawnAgent",
            "senderThreadId": "owned", "receiverThreadIds": [], "status": "inProgress"
        });
        assert_eq!(
            spawn_call(&started),
            Some(("call-2".to_owned(), Vec::new()))
        );
        for item in [
            json!({"type": "collabAgentToolCall", "id": "c", "tool": "wait"}),
            json!({"type": "commandExecution", "id": "c", "tool": "spawnAgent"}),
            json!({"type": "collabAgentToolCall", "tool": "spawnAgent"}),
        ] {
            assert_eq!(spawn_call(&item), None, "{item}");
        }
    }

    #[test]
    fn child_message_turn_id_prefers_params_then_active_turn() {
        assert_eq!(
            child_message_turn_id(&json!({"turnId": "turn-params"}), Some("turn-active")),
            "turn-params"
        );
        assert_eq!(
            child_message_turn_id(&json!({"turnId": "turn-params"}), None),
            "turn-params"
        );
        assert_eq!(
            child_message_turn_id(&json!({}), Some("turn-active")),
            "turn-active"
        );
        assert_eq!(child_message_turn_id(&json!({}), None), "");
    }

    async fn child_test_processor() -> (crate::native::NativeProcess, EventProcessor) {
        let peer = eliot_plugin::Peer::stdio();
        let launch = crate::native::NativeLaunch {
            program: std::env::current_exe().expect("test exe").into_os_string(),
            args: [std::ffi::OsString::from("--list")].to_vec(),
            cwd: std::env::current_dir().expect("cwd"),
            environment: Vec::new(),
        };
        let native = crate::native::NativeProcess::spawn(
            launch,
            eliot_core::SecretPolicy::new(Vec::<String>::new()),
        )
        .await
        .expect("spawn fixture");
        let processor = EventProcessor::new(
            native.client(),
            peer.client(),
            EventConfig {
                credential: "test-cred".to_owned(),
                agent_id: "a1".to_owned(),
                generation: 1,
                secrets: Vec::new(),
                native_scope: String::new(),
            },
        );
        (native, processor)
    }

    #[tokio::test]
    async fn child_buffer_overflow_releases_bytes_without_suppressing_identity() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor
            .children
            .spawn_started("owned", "call-1".to_owned())
            .expect("spawn window");
        assert_eq!(processor.children.take_frame("child-1"), Ok(true));
        for _ in 0..EARLY_EVENT_LIMIT {
            processor.buffer_child_frame(
                "child-1",
                BufferedNativeFrame::Notification {
                    method: "turn/started".to_owned(),
                    params: json!({"threadId": "child-1"}),
                },
            );
        }
        assert!(processor.child_buffer_bytes > 0);
        processor.buffer_child_frame(
            "child-1",
            BufferedNativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: json!({"threadId": "child-1"}),
            },
        );
        assert!(
            processor
                .child_buffers
                .get("child-1")
                .is_none_or(|queue| queue.is_empty())
        );
        assert_eq!(processor.child_buffer_bytes, 0);
        assert!(processor.child_early_lost.contains("child-1"));
        assert!(
            !processor.child_unknown.contains("child-1"),
            "overflow must not suppress later confirmation"
        );
        processor.buffer_child_frame(
            "child-1",
            BufferedNativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: json!({"threadId": "child-1"}),
            },
        );
        assert_eq!(processor.child_buffers["child-1"].len(), 1);
        assert!(processor.child_buffer_bytes > 0);
    }

    #[tokio::test]
    async fn child_flush_gaps_failed_frame_and_processes_remainder() {
        let (_native, mut processor) = child_test_processor().await;
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        let mut state = ChildState::new(ChildBinding {
            registration,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "child-1".to_owned(),
        });
        state.ready_committed = true;
        state.writes_blocked = true;
        processor.child_states.insert("child-1".to_owned(), state);
        let mut queue = VecDeque::new();
        queue.push_back(BufferedNativeFrame::Notification {
            method: "turn/started".to_owned(),
            params: json!({"threadId": "child-1"}),
        });
        queue.push_back(BufferedNativeFrame::Notification {
            method: "item/started".to_owned(),
            params: json!({
                "threadId": "child-1",
                "item": {"type": "collabAgentToolCall", "id": "call-nested", "tool": "spawnAgent"},
            }),
        });
        processor.child_buffer_bytes = queue.iter().map(child_frame_size).sum();
        processor.child_buffers.insert("child-1".to_owned(), queue);
        let error = processor
            .flush_child_buffer("child-1")
            .await
            .expect_err("unresolved child gap must propagate, not report success");
        assert_eq!(error.code, "unknown");
        let state = processor
            .child_states
            .get("child-1")
            .expect("known child stays visible");
        assert!(state.source_unknown);
        assert!(
            state
                .failure
                .as_deref()
                .is_some_and(|failure| failure.contains("omitted turn id"))
        );
        assert!(
            processor
                .children
                .pending_spawns
                .get("child-1")
                .is_some_and(|items| items.contains("call-nested")),
            "remainder is processed, not dropped"
        );
        assert!(!processor.child_buffers.contains_key("child-1"));
        assert_eq!(processor.child_buffer_bytes, 0);
    }

    #[tokio::test]
    async fn parent_completion_retains_pending_child_registration() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());
        processor.native_scope = "codex:test".to_owned();
        processor.children.confirm("owned", "child-1").unwrap();
        processor
            .children
            .parents
            .insert("child-1".to_owned(), "owned".to_owned());
        processor
            .children
            .spawn_items
            .insert("child-1".to_owned(), "call-1".to_owned());
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        processor
            .child_pending
            .insert("child-1".to_owned(), registration.clone());
        processor.buffer_child_frame(
            "child-1",
            BufferedNativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: json!({"threadId": "child-1"}),
            },
        );
        processor.writes_blocked = true;
        let params = json!({
            "threadId": "owned",
            "turn": {"id": "turn-1", "items": [], "status": "completed"},
        });
        let result = processor.complete_turn(params).await;
        assert!(result.is_err(), "parent commit blocked, settle already ran");
        assert_eq!(
            processor.child_pending.get("child-1"),
            Some(&registration),
            "exact pending retained across parent completion, child continues"
        );
        assert!(!processor.child_unknown.contains("child-1"));
        assert!(processor.child_buffers.contains_key("child-1"));
    }

    async fn connected_child_processor(
        parent_id: &str,
    ) -> (
        crate::native::NativeProcess,
        EventProcessor,
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<()>,
    ) {
        let (host_io, plugin_io) = tokio::io::duplex(64 * 1024);
        let (plugin_read, plugin_write) = tokio::io::split(plugin_io);
        let mut peer = eliot_plugin::Peer::from_io(plugin_read, plugin_write);
        let peer_client = peer.client();
        let peer_task = tokio::spawn(async move { while peer.next_request().await.is_some() {} });
        let launch = crate::native::NativeLaunch {
            program: std::env::current_exe().expect("test exe").into_os_string(),
            args: [std::ffi::OsString::from("--list")].to_vec(),
            cwd: std::env::current_dir().expect("cwd"),
            environment: Vec::new(),
        };
        let native = crate::native::NativeProcess::spawn(
            launch,
            eliot_core::SecretPolicy::new(Vec::<String>::new()),
        )
        .await
        .expect("spawn fixture");
        let processor = EventProcessor::new(
            native.client(),
            peer_client,
            EventConfig {
                credential: "test-cred".to_owned(),
                agent_id: parent_id.to_owned(),
                generation: 1,
                secrets: Vec::new(),
                native_scope: "codex:test".to_owned(),
            },
        );
        (native, processor, host_io, peer_task)
    }

    #[tokio::test]
    async fn ordinary_frame_retries_same_pending_registration_after_lost_ack() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.native_scope = "codex:test".to_owned();
        processor.children.confirm("owned", "child-1").unwrap();
        processor
            .children
            .parents
            .insert("child-1".to_owned(), "owned".to_owned());
        processor
            .children
            .spawn_items
            .insert("child-1".to_owned(), "call-1".to_owned());
        let retained = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        processor
            .child_pending
            .insert("child-1".to_owned(), retained.clone());

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let mut recorded_rpc = Vec::new();

        let fake_host = async {
            let mut line = String::new();

            // 1. Initial attempt: host returns busy for child registration.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read first child registration request");
            let req1: RpcRequest = serde_json::from_str(&line).expect("decode req1");
            recorded_rpc.push((req1.method.clone(), req1.params.clone()));
            assert_eq!(req1.method, "child");
            let busy_reply = json!({
                "jsonrpc": "2.0",
                "id": req1.id,
                "error": {
                    "code": -32020,
                    "message": "host plugin registration is busy",
                    "data": { "code": "busy" }
                }
            });
            host_write
                .write_all(format!("{busy_reply}\n").as_bytes())
                .await
                .expect("send busy reply");

            // 2. Later ordinary frame retries IDENTICAL registration: host answers canonical ACK.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read second child registration request");
            let req2: RpcRequest = serde_json::from_str(&line).expect("decode req2");
            recorded_rpc.push((req2.method.clone(), req2.params.clone()));
            assert_eq!(req2.method, "child");
            let ack = crate::native_child::tests::test_ack(&retained);
            let ack_reply = json!({
                "jsonrpc": "2.0",
                "id": req2.id,
                "result": ack,
            });
            host_write
                .write_all(format!("{ack_reply}\n").as_bytes())
                .await
                .expect("send canonical child ack");

            // 3. Upon registration ACK, processor commits session.ready.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read session.ready commit");
            let req3: RpcRequest = serde_json::from_str(&line).expect("decode req3");
            recorded_rpc.push((req3.method.clone(), req3.params.clone()));
            assert_eq!(req3.method, "events.put");
            let ready_ack = json!({
                "jsonrpc": "2.0",
                "id": req3.id,
                "result": {
                    "first_seq": 1,
                    "last_seq": 1,
                    "cursor": null,
                }
            });
            host_write
                .write_all(format!("{ready_ack}\n").as_bytes())
                .await
                .expect("send session.ready ack");

            // 4. Processor drains buffered child frames (turn-9, then turn-10).
            for (idx, expected_turn) in ["turn-9", "turn-10"].iter().enumerate() {
                line.clear();
                host_reader
                    .read_line(&mut line)
                    .await
                    .expect("read child fact commit");
                let req: RpcRequest = serde_json::from_str(&line).expect("decode child fact");
                recorded_rpc.push((req.method.clone(), req.params.clone()));
                assert_eq!(req.method, "events.put");
                assert_eq!(req.params["agent_id"], "a2");
                assert_eq!(req.params["events"][0]["kind"], "turn.started");
                assert_eq!(req.params["events"][0]["data"]["turn_id"], *expected_turn);
                let turn_ack = json!({
                    "jsonrpc": "2.0",
                    "id": req.id,
                    "result": {
                        "first_seq": 2 + idx as i64,
                        "last_seq": 2 + idx as i64,
                        "cursor": null,
                    }
                });
                host_write
                    .write_all(format!("{turn_ack}\n").as_bytes())
                    .await
                    .expect("send turn.started ack");
            }

            drop(host_write);
            drop(host_reader);
            recorded_rpc
        };

        let driver = async {
            // First ordinary child frame: registration fails busy, frame is buffered, no facts before ACK.
            processor
                .handle_child_notification(
                    "turn/started",
                    json!({"threadId": "child-1", "turn": {"id": "turn-9"}}),
                )
                .await;

            assert_eq!(
                processor
                    .child_pending
                    .get("child-1")
                    .map(|r| &r.registration_id),
                Some(&retained.registration_id),
                "registration retained across lost ACK / busy"
            );
            assert!(!processor.child_unknown.contains("child-1"));
            assert_eq!(
                processor.child_buffers.get("child-1").map(|q| q.len()),
                Some(1)
            );
            assert!(
                !processor.child_states.contains_key("child-1"),
                "no child facts before registration ACK"
            );

            // Later ordinary child frame: retries identical registration, receives canonical ACK.
            processor
                .handle_child_notification(
                    "turn/started",
                    json!({"threadId": "child-1", "turn": {"id": "turn-10"}}),
                )
                .await;
        };

        let (recorded_rpc, ()) = tokio::join!(fake_host, driver);
        peer_task.await.expect("peer task clean shutdown");

        assert_eq!(recorded_rpc.len(), 5);
        assert_eq!(recorded_rpc[0].0, "child");
        assert_eq!(
            recorded_rpc[0].1["registration"]["registration_id"],
            retained.registration_id
        );
        assert_eq!(recorded_rpc[0].1["registration"]["parent_id"], "a1");

        assert_eq!(recorded_rpc[1].0, "child");
        assert_eq!(
            recorded_rpc[1].1, recorded_rpc[0].1,
            "later ordinary frame retries SAME retained registration"
        );

        assert_eq!(recorded_rpc[2].0, "events.put");
        assert_eq!(recorded_rpc[2].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[2].1["events"][0]["kind"], "session.ready");

        assert_eq!(recorded_rpc[3].0, "events.put");
        assert_eq!(recorded_rpc[3].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[3].1["events"][0]["kind"], "turn.started");
        assert_eq!(recorded_rpc[3].1["events"][0]["data"]["turn_id"], "turn-9");

        assert_eq!(recorded_rpc[4].0, "events.put");
        assert_eq!(recorded_rpc[4].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[4].1["events"][0]["kind"], "turn.started");
        assert_eq!(recorded_rpc[4].1["events"][0]["data"]["turn_id"], "turn-10");

        assert!(!processor.child_pending.contains_key("child-1"));
        assert!(processor.child_states.contains_key("child-1"));
        let state = processor.child_states.get("child-1").unwrap();
        assert_eq!(state.binding.agent_id, "a2");
        assert_eq!(state.binding.agent_generation, 1);
        assert!(state.ready_committed);
        assert!(processor.children.pending_spawns.is_empty());
    }

    #[tokio::test]
    async fn child_gap_ack_failure_stays_in_child_scope() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.thread_id = Some("owned".to_owned());
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        let mut state = ChildState::new(ChildBinding {
            registration,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "child-1".to_owned(),
        });
        state.ready_committed = true;
        processor.child_states.insert("child-1".to_owned(), state);

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let mut recorded_rpc = Vec::new();

        let fake_host = async {
            let mut line = String::new();

            // 1. First gap request: busy-before-commit gap.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read first child gap request");
            let req1: RpcRequest = serde_json::from_str(&line).expect("decode req1");
            recorded_rpc.push((req1.method.clone(), req1.params.clone()));
            assert_eq!(req1.method, "source.gap");
            let busy_reply = json!({
                "jsonrpc": "2.0",
                "id": req1.id,
                "error": {
                    "code": -32020,
                    "message": "host source gap is busy",
                    "data": { "code": "busy" }
                }
            });
            host_write
                .write_all(format!("{busy_reply}\n").as_bytes())
                .await
                .expect("send busy gap reply");

            // 2. Owned driver retries SAME gap_id/body (no extra frame for
            // the retry itself): host answers canonical ACK.
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read retried child gap request");
            let req2: RpcRequest = serde_json::from_str(&line).expect("decode req2");
            recorded_rpc.push((req2.method.clone(), req2.params.clone()));
            assert_eq!(req2.method, "source.gap");
            let gap_ack = json!({
                "jsonrpc": "2.0",
                "id": req2.id,
                "result": {
                    "gap_id": req2.params["gap_id"],
                    "seq": 1,
                }
            });
            host_write
                .write_all(format!("{gap_ack}\n").as_bytes())
                .await
                .expect("send canonical gap ack");

            // 3. Frame after settled gap commits its turn.started event
            // (turn-2 arrived while pending and was dropped, covered by the
            // gap; turn-3 arrives after settle and commits).
            line.clear();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read child fact after settled gap");
            let req3: RpcRequest = serde_json::from_str(&line).expect("decode req3");
            recorded_rpc.push((req3.method.clone(), req3.params.clone()));
            assert_eq!(req3.method, "events.put");
            assert_eq!(req3.params["events"][0]["data"]["turn_id"], "turn-3");
            let fact_ack = json!({
                "jsonrpc": "2.0",
                "id": req3.id,
                "result": {
                    "first_seq": 2,
                    "last_seq": 2,
                    "cursor": null,
                }
            });
            host_write
                .write_all(format!("{fact_ack}\n").as_bytes())
                .await
                .expect("send turn fact ack");

            drop(host_write);
            drop(host_reader);
            recorded_rpc
        };

        let driver = async {
            // First frame: normalization failure triggers source.gap, host replies busy.
            processor
                .handle_child_notification("turn/started", json!({"threadId": "child-1"}))
                .await;

            // Observe host-facing child gap / source.state before producer drain:
            let state_before = processor
                .child_states
                .get("child-1")
                .expect("child visible");
            assert!(
                state_before.writes_blocked,
                "writes must be blocked while gap is unresolved"
            );
            assert!(
                state_before.source_unknown,
                "source must be marked unknown on gap ACK failure"
            );
            assert!(
                state_before.pending_gap.is_some(),
                "unresolved gap must be retained"
            );
            assert_eq!(
                processor.loss_reason, None,
                "child metadata uncertainty stays in child scope"
            );
            assert!(
                !processor.status.source_unknown,
                "parent source stays known while only a child gap is busy"
            );
            assert!(
                !processor.gap_reported,
                "parent must not report a gap for a child ACK failure"
            );

            // Frame while pending: dropped (covered by the gap), never
            // triggers a retained-gap RPC from the frame path.
            processor
                .handle_child_notification(
                    "turn/started",
                    json!({"threadId": "child-1", "turn": {"id": "turn-2"}}),
                )
                .await;
            assert!(
                processor.has_pending_child_gap(),
                "still pending; frame path never retries"
            );
            assert_eq!(
                processor.child_states["child-1"].active_turn_id, None,
                "dropped while pending, not processed"
            );

            // Owned attempt for the SAME retained gap (no extra frame for
            // the retry itself), settled by the canonical ACK.
            let (retry_thread, retry_gap) =
                processor.next_pending_gap().expect("pending for driver");
            assert_eq!(retry_thread, "child-1");
            let params =
                eliot_plugin::session::StreamSource::gap_request_params("test-cred", &retry_gap);
            let host = processor.host.clone();
            let result = host
                .call(
                    "source.gap",
                    params,
                    tokio::time::Instant::now() + eliot_plugin::session::EVENT_DEADLINE,
                )
                .await;
            assert!(
                processor.apply_gap_attempt(&retry_thread, &retry_gap, &result),
                "canonical ACK settles the matching retained gap"
            );

            // Frame after settle commits normally.
            processor
                .handle_child_notification(
                    "turn/started",
                    json!({"threadId": "child-1", "turn": {"id": "turn-3"}}),
                )
                .await;
        };

        let (recorded_rpc, ()) = tokio::join!(fake_host, driver);
        peer_task.await.expect("peer task clean shutdown");

        assert_eq!(recorded_rpc.len(), 3);
        // Request 1: first gap attempt
        assert_eq!(recorded_rpc[0].0, "source.gap");
        assert_eq!(recorded_rpc[0].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[0].1["generation"], 1);
        assert_eq!(recorded_rpc[0].1["source"], "stream");
        assert!(
            recorded_rpc[0].1["gap_id"]
                .as_str()
                .unwrap()
                .starts_with("codex:a2:1:gap-")
        );

        // Request 2: retried gap has identical gap_id and reason body
        assert_eq!(recorded_rpc[1].0, "source.gap");
        assert_eq!(recorded_rpc[1].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[1].1["generation"], 1);
        assert_eq!(recorded_rpc[1].1["source"], "stream");
        assert_eq!(
            recorded_rpc[1].1["gap_id"], recorded_rpc[0].1["gap_id"],
            "must retry SAME child gap_id"
        );
        assert_eq!(
            recorded_rpc[1].1["reason"], recorded_rpc[0].1["reason"],
            "must retry SAME child gap body"
        );

        // Request 3: turn-3 committed after settlement (turn-2 was dropped
        // while pending, covered by the gap).
        assert_eq!(recorded_rpc[2].0, "events.put");
        assert_eq!(recorded_rpc[2].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[2].1["events"][0]["kind"], "turn.started");
        assert_eq!(recorded_rpc[2].1["events"][0]["data"]["turn_id"], "turn-3");

        // Observe host-facing child gap / source.state on child state after settlement:
        let state_after = processor
            .child_states
            .get("child-1")
            .expect("child visible");
        assert_eq!(state_after.binding.agent_id, "a2");
        assert_eq!(state_after.binding.agent_generation, 1);
        assert_eq!(state_after.binding.native_ref, "child-1");
        assert!(
            state_after.pending_gap.is_none(),
            "gap settled by canonical ACK"
        );
        assert!(state_after.gap_reported, "gap was reported");
        assert_eq!(state_after.gap_number, 1);
        assert!(
            !state_after.writes_blocked,
            "writes unblocked after canonical ACK"
        );
        assert!(
            state_after.source_unknown,
            "source marked unknown due to gap history"
        );
        assert!(
            state_after
                .failure
                .as_deref()
                .is_some_and(|f| f.contains("could not be normalized")),
            "failure records normalization error"
        );

        // Assert exact child agent / generation / source from host observation:
        assert_eq!(recorded_rpc[1].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[1].1["generation"], 1);
        assert_eq!(recorded_rpc[1].1["source"], "stream");

        // Parent native source has no false gap after the child ACK: the
        // healthy-parent steer gate (host checks status.source_unknown)
        // stays admitted.
        assert_eq!(processor.loss_reason, None);
        assert!(!processor.status.source_unknown);
        assert!(!processor.gap_reported);
        assert!(processor.status.failure.is_none());

        // No native input / spawn replay:
        assert!(processor.children.pending_spawns.is_empty());
    }

    #[tokio::test]
    async fn pending_child_gap_recovers_without_additional_frame_before_drain() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};
        use tokio::sync::{mpsc, oneshot, watch};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.thread_id = Some("owned".to_owned());
        // Healthy parent turn: the steer gate needs an active turn and a
        // known source. The child gap must not disturb either.
        processor.status.active_command_id = Some("send-1".to_owned());
        processor.status.active_turn_id = Some("turn-parent".to_owned());
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        let mut state = ChildState::new(ChildBinding {
            registration,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "child-1".to_owned(),
        });
        state.ready_committed = true;
        processor.child_states.insert("child-1".to_owned(), state);

        // ACTUAL run loop: frames, stream, controls, and status channels.
        // The producer stays alive (frames open, stream Open) throughout.
        let (frames_tx, frames_rx) = mpsc::channel(16);
        let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE);
        let (status_tx, status_rx) = watch::channel(EventStatus::default());
        let run_handle = tokio::spawn(run(frames_rx, stream_rx, processor, control_rx, status_tx));

        // Coordination without timing: held (gap2 received, ACK not yet
        // sent), ack (parent served, now send ACK), fact (turn-after
        // committed, proving unblocked before drain).
        let (held_tx, held_rx) = oneshot::channel::<()>();
        let (ack_tx, ack_rx) = oneshot::channel::<()>();
        let (fact_tx, fact_rx) = oneshot::channel::<()>();

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let mut recorded_rpc = Vec::new();

        let fake_host = async {
            let mut line = String::new();

            // 1. Last child frame's gap: host replies busy immediately.
            line.clear();
            timeout_at(
                Instant::now() + EVENT_DEADLINE,
                host_reader.read_line(&mut line),
            )
            .await
            .expect("host read first child gap within deadline")
            .expect("read first child gap request");
            let req1: RpcRequest = serde_json::from_str(&line).expect("decode req1");
            recorded_rpc.push((req1.method.clone(), req1.params.clone()));
            assert_eq!(req1.method, "source.gap");
            assert_eq!(req1.params["agent_id"], "a2");
            let busy_reply = json!({
                "jsonrpc": "2.0",
                "id": req1.id,
                "error": {
                    "code": -32020,
                    "message": "host source gap is busy",
                    "data": { "code": "busy" }
                }
            });
            host_write
                .write_all(format!("{busy_reply}\n").as_bytes())
                .await
                .expect("send busy gap reply");

            // 2. Owned retry (no extra child frame): SAME gap_id/body.
            // Held: no ACK until the parent control is served.
            line.clear();
            timeout_at(
                Instant::now() + EVENT_DEADLINE,
                host_reader.read_line(&mut line),
            )
            .await
            .expect("host read retried child gap within deadline")
            .expect("read independently retried child gap");
            let req2: RpcRequest = serde_json::from_str(&line).expect("decode req2");
            assert_eq!(req2.method, "source.gap");
            assert_eq!(
                req2.params["gap_id"], req1.params["gap_id"],
                "independent retry keeps SAME gap_id"
            );
            assert_eq!(
                req2.params["reason"], req1.params["reason"],
                "independent retry keeps SAME gap body"
            );
            recorded_rpc.push((req2.method.clone(), req2.params.clone()));
            held_tx.send(()).expect("signal gap held");
            timeout_at(Instant::now() + EVENT_DEADLINE, ack_rx)
                .await
                .expect("ack signal within deadline")
                .expect("wait for parent-served signal");
            let gap_ack = json!({
                "jsonrpc": "2.0",
                "id": req2.id,
                "result": {
                    "gap_id": req2.params["gap_id"],
                    "seq": 1,
                }
            });
            host_write
                .write_all(format!("{gap_ack}\n").as_bytes())
                .await
                .expect("send canonical gap ack");

            // 3. Turn-after commits only after settle (proving unblocked
            // before drain). Any concurrent second gap attempt while held
            // would arrive here instead and fail the method assertion.
            line.clear();
            timeout_at(
                Instant::now() + EVENT_DEADLINE,
                host_reader.read_line(&mut line),
            )
            .await
            .expect("host read turn-after fact within deadline")
            .expect("read turn-after fact");
            let req3: RpcRequest = serde_json::from_str(&line).expect("decode req3");
            assert_eq!(req3.method, "events.put");
            assert_eq!(req3.params["agent_id"], "a2");
            assert_eq!(req3.params["events"][0]["kind"], "turn.started");
            assert_eq!(req3.params["events"][0]["data"]["turn_id"], "turn-after");
            recorded_rpc.push((req3.method.clone(), req3.params.clone()));
            let fact_ack = json!({
                "jsonrpc": "2.0",
                "id": req3.id,
                "result": {
                    "first_seq": 2,
                    "last_seq": 2,
                    "cursor": null,
                }
            });
            host_write
                .write_all(format!("{fact_ack}\n").as_bytes())
                .await
                .expect("send turn fact ack");
            fact_tx.send(()).expect("signal fact committed");

            drop(host_write);
            drop(host_reader);
            recorded_rpc
        };

        let mut status_rx = status_rx;
        let driver = async {
            // Initial state: no pending gaps (Option::None). Serves parent control.
            let (initial_tx, initial_rx) = oneshot::channel();
            control_tx
                .send(Control::BeginStop { reply: initial_tx })
                .await
                .expect("send initial BeginStop");
            let initial_reply = timeout_at(Instant::now() + EVENT_DEADLINE, initial_rx)
                .await
                .expect("initial control within bounded deadline")
                .expect("parent control served with no pending gaps");
            assert!(
                initial_reply.is_ok(),
                "BeginStop stays responsive before gaps"
            );

            // Last child frame: normalization failure, gap busy. No more
            // child frames until after settle (retry is independent).
            frames_tx
                .send(NativeFrame::Notification {
                    method: "turn/started".to_owned(),
                    params: json!({"threadId": "child-1"}),
                })
                .await
                .expect("send last child frame");
            timeout_at(Instant::now() + EVENT_DEADLINE, held_rx)
                .await
                .expect("held signal within bounded deadline")
                .expect("retry started without extra frame");

            // Parent control served WHILE the metadata ACK is held.
            let (reply_tx, reply_rx) = oneshot::channel();
            control_tx
                .send(Control::BeginStop { reply: reply_tx })
                .await
                .expect("send parent BeginStop");
            let reply = timeout_at(Instant::now() + EVENT_DEADLINE, reply_rx)
                .await
                .expect("held control within bounded deadline")
                .expect("parent control served while ACK held");
            assert!(reply.is_ok(), "BeginStop stays responsive");
            let held_status = status_rx.borrow_and_update().clone();
            assert!(
                !held_status.source_unknown,
                "parent source stays known while child ACK held"
            );
            assert_eq!(
                held_status.active_turn_id.as_deref(),
                Some("turn-parent"),
                "healthy parent turn retained while held"
            );

            // Now let the held ACK settle the child, then wait for the
            // loop iteration that applies it (status send follows apply).
            ack_tx.send(()).expect("release held ACK");
            timeout_at(Instant::now() + EVENT_DEADLINE, status_rx.changed())
                .await
                .expect("status change within bounded deadline")
                .expect("settle applied before drain");

            // After canonical ACK settlement: gap_in_flight is None again; serves parent control again.
            let (after_ack_tx, after_ack_rx) = oneshot::channel();
            control_tx
                .send(Control::BeginStop {
                    reply: after_ack_tx,
                })
                .await
                .expect("send parent BeginStop after ACK");
            let after_ack_reply = timeout_at(Instant::now() + EVENT_DEADLINE, after_ack_rx)
                .await
                .expect("after-ack control within bounded deadline")
                .expect("parent control served again after ACK/Option::None");
            assert!(
                after_ack_reply.is_ok(),
                "BeginStop stays responsive after settlement"
            );

            // Subsequent frame commits (proving writes unblocked) before
            // the producer drains (frames still open, stream Open).
            frames_tx
                .send(NativeFrame::Notification {
                    method: "turn/started".to_owned(),
                    params: json!({"threadId": "child-1", "turn": {"id": "turn-after"}}),
                })
                .await
                .expect("send turn-after frame");
            timeout_at(Instant::now() + EVENT_DEADLINE, fact_rx)
                .await
                .expect("fact signal within bounded deadline")
                .expect("turn-after committed");

            let last_status = status_rx.borrow().clone();
            assert!(!last_status.source_unknown);
            assert!(last_status.failure.is_none());

            // Clean shutdown of the actual run loop.
            let (close_tx, close_rx) = oneshot::channel();
            control_tx
                .send(Control::Close { reply: close_tx })
                .await
                .expect("send Close");
            timeout_at(Instant::now() + EVENT_DEADLINE, close_rx)
                .await
                .expect("close within bounded deadline")
                .expect("Close reply");
            last_status
        };

        let (recorded_rpc, last_status) = tokio::join!(fake_host, driver);
        run_handle.await.expect("run clean shutdown");
        peer_task.await.expect("peer task clean shutdown");
        drop(stream_tx);

        // Public child boundary: gap, SAME-gap retry, then turn-after fact.
        // No extra frame between busy and retry; one attempt at a time (no
        // concurrent second gap while held); settled before drain.
        assert_eq!(recorded_rpc.len(), 3);
        assert_eq!(recorded_rpc[0].0, "source.gap");
        assert_eq!(recorded_rpc[0].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[0].1["generation"], 1);
        assert_eq!(recorded_rpc[0].1["source"], "stream");
        assert_eq!(recorded_rpc[1].0, "source.gap");
        assert_eq!(recorded_rpc[1].1["agent_id"], "a2");
        assert_eq!(recorded_rpc[1].1["generation"], 1);
        assert_eq!(recorded_rpc[1].1["source"], "stream");
        assert_eq!(
            recorded_rpc[1].1["gap_id"], recorded_rpc[0].1["gap_id"],
            "independent retry keeps SAME gap_id"
        );
        assert_eq!(
            recorded_rpc[1].1["reason"], recorded_rpc[0].1["reason"],
            "independent retry keeps SAME gap body"
        );
        assert_eq!(recorded_rpc[2].0, "events.put");
        assert_eq!(recorded_rpc[2].1["agent_id"], "a2");

        // Parent native source has no false gap; healthy steer gate
        // (active turn + known source) remains admitted.
        assert!(!last_status.source_unknown);
        assert!(!last_status.batch_unknown);
        assert_eq!(last_status.active_turn_id.as_deref(), Some("turn-parent"));

        // NO child registration / NativeInput / Spawn replay on the wire:
        // only source.gap and the turn-after events.put.
        assert!(
            recorded_rpc
                .iter()
                .all(|(method, _)| matches!(method.as_str(), "source.gap" | "events.put"))
        );
    }

    #[tokio::test]
    async fn pending_gap_rotation_is_fair_and_apply_matches_only() {
        let (_native, mut processor) = child_test_processor().await;
        for (thread, agent, gap_id) in [
            ("child-1", "a2", "codex:a2:1:gap-1"),
            ("child-2", "a3", "codex:a3:1:gap-1"),
        ] {
            let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", thread);
            let mut state = ChildState::new(ChildBinding {
                registration,
                agent_id: agent.to_owned(),
                agent_generation: 1,
                native_ref: thread.to_owned(),
            });
            state.ready_committed = true;
            state.writes_blocked = true;
            state.source_unknown = true;
            state.gap_reported = true;
            state.gap_number = 1;
            state.pending_gap = Some(SourceGap {
                gap_id: gap_id.to_owned(),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                families: vec![
                    "session".to_owned(),
                    "turn".to_owned(),
                    "message".to_owned(),
                ],
                reason: format!("gap for {thread}"),
                from: None,
                to: None,
            });
            processor.child_states.insert(thread.to_owned(), state);
        }
        // Fair rotation: sorted order, round-robin, no starvation.
        let (first, gap1) = processor.next_pending_gap().expect("first");
        assert_eq!(first, "child-1");
        let (second, gap2) = processor.next_pending_gap().expect("second");
        assert_eq!(second, "child-2");
        let (third, _) = processor.next_pending_gap().expect("wraps");
        assert_eq!(third, "child-1");

        // Stale attempts are ignored: wrong child, generation, or gap.
        let good_ack = json!({"gap_id": gap1.gap_id, "seq": 1});
        assert!(!processor.apply_gap_attempt("child-2", &gap1, &Ok(good_ack.clone())));
        assert!(processor.child_states["child-2"].pending_gap.is_some());
        let mut wrong_gen = gap1.clone();
        wrong_gen.generation = 2;
        assert!(!processor.apply_gap_attempt("child-1", &wrong_gen, &Ok(good_ack.clone())));
        let mut stale = gap1.clone();
        stale.gap_id = "codex:a2:1:gap-999".to_owned();
        assert!(!processor.apply_gap_attempt(
            "child-1",
            &stale,
            &Ok(json!({"gap_id": stale.gap_id, "seq": 1}))
        ));
        // Non-canonical ACK and transport errors retain without settling.
        assert!(!processor.apply_gap_attempt(
            "child-1",
            &gap1,
            &Ok(json!({"gap_id": gap1.gap_id, "seq": 0}))
        ));
        assert!(!processor.apply_gap_attempt("child-2", &gap2, &Err(Error::new("busy", "held"))));
        assert!(processor.child_states["child-1"].pending_gap.is_some());
        assert!(processor.child_states["child-2"].pending_gap.is_some());
        // Canonical ACK settles only the matching child.
        assert!(processor.apply_gap_attempt("child-1", &gap1, &Ok(good_ack)));
        assert!(processor.child_states["child-1"].pending_gap.is_none());
        assert!(!processor.child_states["child-1"].writes_blocked);
        assert!(
            processor.child_states["child-2"].pending_gap.is_some(),
            "other child untouched"
        );
        let (remaining, _) = processor.next_pending_gap().expect("remaining");
        assert_eq!(remaining, "child-2");
    }

    #[tokio::test]
    async fn child_orphan_collection_bounds_rejected_unconfirmed() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor
            .children
            .spawn_started("owned", "call-1".to_owned())
            .unwrap();
        for index in 0..MAX_CHILD_THREADS {
            let thread = format!("t{index}");
            assert_eq!(processor.children.take_frame(&thread), Ok(true));
            processor.buffer_child_frame(
                &thread,
                BufferedNativeFrame::Notification {
                    method: "turn/started".to_owned(),
                    params: json!({"threadId": thread}),
                },
            );
        }
        assert_eq!(processor.child_buffers.len(), MAX_CHILD_THREADS);
        assert!(processor.children.take_frame("t-rejected").is_err());
        let rejected_params = json!({"threadId": "t-rejected"});
        assert!(processor.take_child_frame(&rejected_params));
        processor
            .handle_child_notification("turn/started", rejected_params)
            .await;
        assert!(!processor.child_buffers.contains_key("t-rejected"));
        assert!(!processor.child_unknown.contains("t-rejected"));
        assert!(!processor.child_early_lost.contains("t-rejected"));
        assert!(!processor.child_pending.contains_key("t-rejected"));
        processor.children.confirm("owned", "t0").unwrap();
        processor.settle_child_tracking();
        assert!(processor.loss_reason.is_some(), "loss is explicit gap");
        assert!(
            processor.child_buffers.contains_key("t0"),
            "proven child row is preserved"
        );
        assert!(!processor.child_buffers.contains_key("t1"));
        assert!(!processor.child_buffers.contains_key("t-rejected"));
        assert!(processor.child_buffers.len() <= MAX_CHILD_THREADS);
    }

    #[test]
    fn child_window_settle_is_producer_scoped() {
        let mut children = ChildThreads::default();
        children
            .spawn_started("owned", "call-root".to_owned())
            .unwrap();
        children
            .spawn_started("child-a", "call-nested".to_owned())
            .unwrap();
        assert_eq!(children.take_frame("child-b"), Ok(true));
        assert_eq!(children.settle("owned"), Ok(()));
        assert!(children.is_unconfirmed("child-b"));
        assert!(children.has_pending());
        assert_eq!(
            children.spawn_completed("child-a", "owned", "call-nested", &["child-b".to_owned()]),
            Ok(())
        );
        assert!(children.is_bound("child-b"));
        assert!(!children.has_pending());
    }

    #[tokio::test]
    async fn nested_spawn_survives_root_completion() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("root".to_owned());
        processor.status.active_turn_id = Some("turn-r".to_owned());
        processor.status.active_command_id = Some("cmd-r".to_owned());
        processor.native_scope = "codex:test".to_owned();
        processor.children.confirm("root", "a").unwrap();
        processor.agent_id = "a1".to_owned();
        processor.generation = 1;
        let reg_a = build_registration("call-a", "a1", 1, "root", "codex:test", "a");
        let mut state_a = ChildState::new(ChildBinding {
            registration: reg_a,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "a".to_owned(),
        });
        state_a.ready_committed = true;
        state_a.active_turn_id = Some("turn-a".to_owned());
        processor.child_states.insert("a".to_owned(), state_a);
        processor
            .children
            .parents
            .insert("a".to_owned(), "root".to_owned());
        processor
            .children
            .spawn_items
            .insert("a".to_owned(), "call-a".to_owned());
        processor
            .children
            .spawn_started("root", "call-root".to_owned())
            .unwrap();
        assert_eq!(processor.children.take_frame("b"), Ok(true));
        processor.buffer_child_frame(
            "b",
            BufferedNativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: json!({"threadId": "b", "turn": {"id": "turn-b"}}),
            },
        );
        processor.writes_blocked = true;
        let params = json!({
            "threadId": "root",
            "turn": {"id": "turn-r", "items": [], "status": "completed"},
        });
        let _ = processor.complete_turn(params).await;
        assert!(processor.children.is_unconfirmed("b"));
        assert!(processor.child_buffers.contains_key("b"));
        assert!(!processor.child_early_lost.contains("b"));
        processor
            .children
            .spawn_started("a", "call-b".to_owned())
            .unwrap();
        processor.writes_blocked = false;
        processor.children.confirm("root", "b").unwrap();
        processor
            .children
            .parents
            .insert("b".to_owned(), "a".to_owned());
        processor
            .children
            .spawn_items
            .insert("b".to_owned(), "call-b".to_owned());
        let reg_b = processor.child_registration_for("b").unwrap();
        assert_eq!(reg_b.parent_id, "a2");
        assert_eq!(reg_b.parent_native_ref, "a");
        assert_eq!(reg_b.native_ref, "b");
        if let Some(state) = processor.child_states.get_mut("a") {
            state.active_turn_id = None;
        }
        processor.settle_spawn_window_for("a");
        assert!(processor.children.is_bound("b"));
    }

    #[tokio::test]
    async fn nested_spawn_survives_root_completion_when_a_writes_blocked() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("root".to_owned());
        processor.status.active_turn_id = Some("turn-r".to_owned());
        processor.status.active_command_id = Some("cmd-r".to_owned());
        processor.native_scope = "codex:test".to_owned();
        processor.children.confirm("root", "a").unwrap();
        processor.agent_id = "a1".to_owned();
        processor.generation = 1;
        let reg_a = build_registration("call-a", "a1", 1, "root", "codex:test", "a");
        let mut state_a = ChildState::new(ChildBinding {
            registration: reg_a,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "a".to_owned(),
        });
        state_a.active_turn_id = Some("turn-a".to_owned());
        state_a.writes_blocked = true;
        processor.child_states.insert("a".to_owned(), state_a);
        processor
            .children
            .parents
            .insert("a".to_owned(), "root".to_owned());
        processor
            .children
            .spawn_items
            .insert("a".to_owned(), "call-a".to_owned());
        processor
            .children
            .spawn_started("root", "call-root".to_owned())
            .unwrap();
        assert_eq!(processor.children.take_frame("b"), Ok(true));
        processor.buffer_child_frame(
            "b",
            BufferedNativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: json!({"threadId": "b", "turn": {"id": "turn-b"}}),
            },
        );
        processor.writes_blocked = true;
        let params = json!({
            "threadId": "root",
            "turn": {"id": "turn-r", "items": [], "status": "completed"},
        });
        let _ = processor.complete_turn(params).await;
        assert!(processor.children.is_unconfirmed("b"));
        assert!(processor.child_buffers.contains_key("b"));
        assert!(!processor.child_early_lost.contains("b"));
        processor
            .children
            .spawn_started("a", "call-b".to_owned())
            .unwrap();
    }

    #[tokio::test]
    async fn retained_unconfirmed_b_terminal_survives_root_completion_through_handle_frame() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.ready = true;
        processor.status.thread_id = Some("root".to_owned());
        processor.status.active_turn_id = Some("turn-root".to_owned());
        processor.status.active_command_id = Some("cmd-root".to_owned());
        let registration_a = build_registration("call-root", "a1", 1, "root", "codex:test", "a");
        let mut state_a = ChildState::new(ChildBinding {
            registration: registration_a,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "a".to_owned(),
        });
        state_a.ready_committed = true;
        state_a.active_turn_id = Some("turn-a".to_owned());
        processor.child_states.insert("a".to_owned(), state_a);
        processor.children.confirm("root", "a").unwrap();
        processor
            .children
            .parents
            .insert("a".to_owned(), "root".to_owned());
        processor
            .children
            .spawn_items
            .insert("a".to_owned(), "call-root".to_owned());
        processor
            .children
            .spawn_started("root", "call-root".to_owned())
            .unwrap();
        assert_eq!(processor.children.pending_items("root").len(), 1);

        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let fake_host = async {
            let mut requests = Vec::new();
            let mut last_seq = HashMap::<String, i64>::new();
            for _ in 0..5 {
                let mut line = String::new();
                host_reader
                    .read_line(&mut line)
                    .await
                    .expect("read fake-host request");
                let request: RpcRequest = serde_json::from_str(&line).expect("decode request");
                requests.push((request.method.clone(), request.params.clone()));
                let result = match request.method.as_str() {
                    "child" => {
                        let registration: eliot_core::ChildRegistration =
                            serde_json::from_value(request.params["registration"].clone())
                                .expect("decode child registration");
                        let mut ack = crate::native_child::tests::test_ack(&registration);
                        ack.agent.id = "a3".to_owned();
                        ack.agent.root = "a1".to_owned();
                        ack.agent.depth = 2;
                        serde_json::to_value(ack).expect("encode child ACK")
                    }
                    "events.put" => {
                        let agent_id = request.params["agent_id"]
                            .as_str()
                            .expect("agent id")
                            .to_owned();
                        let count = request.params["events"]
                            .as_array()
                            .expect("event batch")
                            .len() as i64;
                        let sequence = last_seq.entry(agent_id).or_default();
                        let first_seq = *sequence + 1;
                        let end = first_seq + count - 1;
                        *sequence = end;
                        json!({
                            "first_seq": first_seq,
                            "last_seq": end,
                            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
                        })
                    }
                    method => panic!("unexpected fake-host request: {method}"),
                };
                let response = json!({"jsonrpc": "2.0", "id": request.id, "result": result});
                host_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .expect("send ACK");
            }
            requests
        };
        let driver = async {
            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: "turn/started".to_owned(),
                    params: json!({"threadId": "b", "turn": {"id": "turn-b"}}),
                }))
                .await;
            assert!(processor.children.is_unconfirmed("b"));
            assert_eq!(
                processor.child_buffers.get("b").map(|queue| queue.len()),
                Some(1)
            );

            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: "turn/completed".to_owned(),
                    params: json!({
                        "threadId": "root",
                        "turn": {"id": "turn-root", "items": [], "status": "completed"},
                    }),
                }))
                .await;
            assert!(processor.status.active_turn_id.is_none());
            assert_eq!(
                processor
                    .status
                    .last_turn
                    .as_ref()
                    .map(|turn| turn.turn_id.as_str()),
                Some("turn-root")
            );
            assert!(processor.children.is_unconfirmed("b"));
            assert!(!processor.children.has_pending());

            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: "turn/completed".to_owned(),
                    params: json!({
                        "threadId": "b",
                        "turn": {"id": "turn-b", "items": [], "status": "completed"},
                    }),
                }))
                .await;
            assert!(processor.buffer.is_empty());
            assert_eq!(
                processor.child_buffers.get("b").map(|queue| queue.len()),
                Some(2)
            );

            processor
                .children
                .spawn_started("a", "call-b".to_owned())
                .unwrap();
            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: "thread/started".to_owned(),
                    params: json!({
                        "thread": {"id": "b", "source": {"subAgent": {"thread_spawn": {
                            "parent_thread_id": "a", "call_id": "call-b",
                        }}}},
                    }),
                }))
                .await;
            assert!(processor.children.is_bound("b"));
            processor
                .children
                .spawn_completed("a", "root", "call-b", &["b".to_owned()])
                .unwrap();
            assert!(
                processor
                    .child_states
                    .get("b")
                    .is_some_and(|state| state.active_turn_id.is_none())
            );
            assert!(
                processor
                    .child_buffers
                    .get("b")
                    .is_none_or(|queue| queue.is_empty())
            );
            assert!(processor.buffer.is_empty());
        };
        let (requests, ()) = tokio::join!(fake_host, driver);
        peer_task.abort();

        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method == "child")
                .count(),
            1
        );
        let mut parent_completions = 0;
        let mut b_completed = false;
        for (method, params) in &requests {
            if method != "events.put" {
                continue;
            }
            for event in params["events"].as_array().expect("recorded event batch") {
                if params["agent_id"] == "a1" && event["kind"] == "turn.completed" {
                    parent_completions += 1;
                }
                if params["agent_id"] == "a3"
                    && event["kind"] == "turn.completed"
                    && event["data"]["turn_id"] == "turn-b"
                {
                    b_completed = true;
                }
            }
        }
        assert_eq!(
            parent_completions, 1,
            "B completion must not create a parent turn"
        );
        assert!(
            b_completed,
            "registered child batch must contain B's completion"
        );
    }

    /// Drives one plausible 0.167.1 frame order for a single native child and
    /// asserts the child's `turn/started` routes to the child path with no
    /// parent gap. `order` permutes spawn `item/started` ("s"),
    /// `item/completed` ("c"), child `thread/started` ("t") and child
    /// `turn/started` ("u") under the causal constraints T-before-U and
    /// S-before-C; on 0.167.1 the child frames may precede the spawn window.
    async fn drive_child_frame_order(order: &[&str]) {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.ready = true;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());

        let spawn_started = json!({
            "threadId": "owned",
            "turnId": "turn-1",
            "item": {
                "type": "collabAgentToolCall", "id": "call-1", "tool": "spawnAgent",
                "senderThreadId": "owned", "receiverThreadIds": [],
                "status": "inProgress", "agentsStates": {},
            },
        });
        let spawn_completed = json!({
            "threadId": "owned",
            "turnId": "turn-1",
            "completedAtMs": 1,
            "item": {
                "type": "collabAgentToolCall", "id": "call-1", "tool": "spawnAgent",
                "senderThreadId": "owned", "receiverThreadIds": ["child-1"],
                "status": "completed", "agentsStates": {},
            },
        });
        // 0.167.1 confirmation under test: the top-level `parentThreadId`
        // names the owned thread, with no legacy `source` link at all.
        let thread_started = json!({"thread": {"id": "child-1", "parentThreadId": "owned"}});
        let turn_started = json!({"threadId": "child-1", "turn": {"id": "turn-c1"}});

        let fake_host = async {
            let (host_read, mut host_write) = split(host_io);
            let mut host_reader = BufReader::new(host_read);
            let mut requests = Vec::new();
            let mut last_seq = HashMap::<String, i64>::new();
            // Exactly one child registration plus four event batches (parent
            // tool start/complete, child session.ready, child turn.started);
            // any gap or extra RPC fails the order.
            for _ in 0..5 {
                let mut line = String::new();
                host_reader
                    .read_line(&mut line)
                    .await
                    .expect("read fake-host request");
                let request: RpcRequest = serde_json::from_str(&line).expect("decode request");
                requests.push((request.method.clone(), request.params.clone()));
                let result = match request.method.as_str() {
                    "child" => {
                        let registration: eliot_core::ChildRegistration =
                            serde_json::from_value(request.params["registration"].clone())
                                .expect("decode child registration");
                        assert_eq!(registration.native_ref, "child-1");
                        serde_json::to_value(crate::native_child::tests::test_ack(&registration))
                            .expect("encode child ACK")
                    }
                    "events.put" => {
                        let agent_id = request.params["agent_id"]
                            .as_str()
                            .expect("agent id")
                            .to_owned();
                        let count = request.params["events"]
                            .as_array()
                            .expect("event batch")
                            .len() as i64;
                        let sequence = last_seq.entry(agent_id).or_default();
                        let first_seq = *sequence + 1;
                        *sequence = first_seq + count - 1;
                        json!({
                            "first_seq": first_seq,
                            "last_seq": *sequence,
                            "cursor": Value::Null,
                        })
                    }
                    method => panic!("unexpected fake-host request: {method}"),
                };
                let response = json!({"jsonrpc": "2.0", "id": request.id, "result": result});
                host_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .expect("send ACK");
            }
            requests
        };
        let driver = async {
            for step in order {
                match *step {
                    "s" => {
                        processor
                            .handle_frame(Some(NativeFrame::Notification {
                                method: "item/started".to_owned(),
                                params: spawn_started.clone(),
                            }))
                            .await;
                    }
                    "c" => {
                        processor
                            .handle_frame(Some(NativeFrame::Notification {
                                method: "item/completed".to_owned(),
                                params: spawn_completed.clone(),
                            }))
                            .await;
                    }
                    "t" => {
                        processor
                            .handle_frame(Some(NativeFrame::Notification {
                                method: "thread/started".to_owned(),
                                params: thread_started.clone(),
                            }))
                            .await;
                    }
                    "u" => {
                        assert!(
                            processor.take_child_frame(&turn_started),
                            "order {order:?}: child turn/started must route to the child path"
                        );
                        processor
                            .handle_frame(Some(NativeFrame::Notification {
                                method: "turn/started".to_owned(),
                                params: turn_started.clone(),
                            }))
                            .await;
                    }
                    step => panic!("unknown order step: {step}"),
                }
            }
            assert!(
                !processor.gap_reported,
                "order {order:?}: no parent gap expected"
            );
            assert!(
                processor.loss_reason.is_none(),
                "order {order:?}: no deferred loss expected"
            );
            assert!(
                processor.status.failure.is_none(),
                "order {order:?}: no owned failure expected"
            );
            assert!(
                !processor.status.source_unknown,
                "order {order:?}: owned source stays known"
            );
            assert!(
                processor.children.is_bound("child-1"),
                "order {order:?}: child must confirm"
            );
            let state = processor
                .child_states
                .get("child-1")
                .expect("order {order:?}: child must register");
            assert_eq!(
                state.active_turn_id.as_deref(),
                Some("turn-c1"),
                "order {order:?}: child turn must open"
            );
        };
        let (requests, ()) = tokio::join!(fake_host, driver);
        peer_task.abort();

        assert!(
            requests.iter().all(|(method, _)| method != "source.gap"),
            "order {order:?}: no gap RPC expected, saw {requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|(method, params)| method == "events.put"
                    && params["agent_id"] == "a2"
                    && params["events"].as_array().is_some_and(|events| events
                        .iter()
                        .any(|event| event["kind"] == "turn.started"
                            && event["data"]["turn_id"] == "turn-c1"))),
            "order {order:?}: child turn.started must commit, saw {requests:?}"
        );
    }

    #[tokio::test]
    async fn child_turn_started_routes_to_child_in_every_plausible_order() {
        for order in [
            ["s", "c", "t", "u"],
            ["s", "t", "c", "u"],
            ["s", "t", "u", "c"],
            ["t", "s", "c", "u"],
            ["t", "s", "u", "c"],
            ["t", "u", "s", "c"],
        ] {
            drive_child_frame_order(&order).await;
        }
    }

    #[tokio::test]
    async fn child_thread_started_accepts_either_parent_link() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        for (label, thread) in [
            (
                "top-level parentThreadId",
                json!({"id": "c1", "parentThreadId": "owned"}),
            ),
            (
                "legacy source.subAgent link",
                json!({"id": "c1", "source": {"subAgent": {"thread_spawn": {
                    "parent_thread_id": "owned", "depth": 1,
                }}}}),
            ),
        ] {
            let (_native, mut processor, host_io, peer_task) =
                connected_child_processor("a1").await;
            processor.status.thread_id = Some("owned".to_owned());
            let params = json!({"thread": thread});
            assert!(
                processor.take_child_frame(&params),
                "{label}: thread/started must route to the child path"
            );
            let fake_host = async {
                let (host_read, mut host_write) = split(host_io);
                let mut host_reader = BufReader::new(host_read);
                let mut count = 0;
                for _ in 0..2 {
                    let mut line = String::new();
                    host_reader
                        .read_line(&mut line)
                        .await
                        .expect("read fake-host request");
                    let request: RpcRequest = serde_json::from_str(&line).expect("decode request");
                    let result = match request.method.as_str() {
                        "child" => {
                            let registration: eliot_core::ChildRegistration =
                                serde_json::from_value(request.params["registration"].clone())
                                    .expect("decode child registration");
                            serde_json::to_value(crate::native_child::tests::test_ack(
                                &registration,
                            ))
                            .expect("encode child ACK")
                        }
                        "events.put" => json!({
                            "first_seq": 1, "last_seq": 1, "cursor": Value::Null,
                        }),
                        method => panic!("unexpected fake-host request: {method}"),
                    };
                    let response = json!({"jsonrpc": "2.0", "id": request.id, "result": result});
                    host_write
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .expect("send ACK");
                    count += 1;
                }
                count
            };
            let driver = async {
                processor
                    .handle_frame(Some(NativeFrame::Notification {
                        method: "thread/started".to_owned(),
                        params,
                    }))
                    .await;
                assert!(
                    processor.children.is_bound("c1"),
                    "{label}: child must confirm"
                );
                assert!(
                    processor.child_states.contains_key("c1"),
                    "{label}: child must register"
                );
                assert!(
                    !processor.gap_reported && processor.loss_reason.is_none(),
                    "{label}: no gap expected"
                );
            };
            let (count, ()) = tokio::join!(fake_host, driver);
            peer_task.abort();
            assert_eq!(count, 2, "{label}: registration plus session.ready");
        }
    }

    #[tokio::test]
    async fn foreign_thread_started_outside_any_window_records_a_gap() {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.thread_id = Some("owned".to_owned());
        let params = json!({"thread": {"id": "foreign-1", "parentThreadId": "someone-else"}});
        assert!(
            processor.take_child_frame(&params),
            "foreign thread/started still routes to the child path, never owned"
        );
        let fake_host = async {
            let (host_read, mut host_write) = split(host_io);
            let mut host_reader = BufReader::new(host_read);
            let mut line = String::new();
            host_reader
                .read_line(&mut line)
                .await
                .expect("read fake-host gap request");
            let request: RpcRequest = serde_json::from_str(&line).expect("decode request");
            assert_eq!(request.method, "source.gap");
            let response = json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": {"gap_id": request.params["gap_id"], "seq": 1},
            });
            host_write
                .write_all(format!("{response}\n").as_bytes())
                .await
                .expect("send gap ACK");
        };
        let driver = async {
            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: "thread/started".to_owned(),
                    params,
                }))
                .await;
            assert!(processor.gap_reported, "foreign thread must record a gap");
            assert!(
                processor
                    .status
                    .failure
                    .as_deref()
                    .is_some_and(|failure| failure.contains("second thread")),
                "gap must name the second-thread cause"
            );
            assert!(!processor.children.is_bound("foreign-1"));
            assert!(!processor.child_states.contains_key("foreign-1"));
        };
        tokio::join!(fake_host, driver);
        peer_task.abort();
    }

    #[tokio::test]
    async fn foreign_turn_started_outside_any_window_gaps_only_at_settle() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.ready = true;
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());
        let params = json!({"threadId": "foreign-9", "turn": {"id": "turn-x"}});
        // No window, no parent link, never seen: held as unconfirmed, never
        // validated as owned, and no gap yet (a later spawn or thread/started
        // can still adopt it).
        assert!(processor.take_child_frame(&params));
        processor
            .handle_frame(Some(NativeFrame::Notification {
                method: "turn/started".to_owned(),
                params: params.clone(),
            }))
            .await;
        assert!(processor.children.is_unconfirmed("foreign-9"));
        assert_eq!(
            processor
                .child_buffers
                .get("foreign-9")
                .map(|queue| queue.len()),
            Some(1)
        );
        assert!(!processor.gap_reported);
        assert!(processor.loss_reason.is_none());
        assert!(processor.status.failure.is_none());
        assert!(!processor.status.source_unknown);
        // No spawn ever announces it: the settle records the source gap.
        processor.settle_child_tracking();
        assert!(
            processor
                .loss_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("no spawn announced")),
            "settle must gap the unattributable thread"
        );
    }

    #[tokio::test]
    async fn sub_agent_activity_and_new_collab_tools_create_no_gap() {
        let (_native, mut processor) = child_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.ready = true;
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());
        // A `subAgentActivity` item with an unknown kind stays ignored like
        // any other unsupported item: no tool family fits, so nothing commits
        // and nothing gaps, on either side of the item lifecycle.
        for method in ["item/started", "item/completed"] {
            let mut params = json!({
                "threadId": "owned",
                "turnId": "turn-1",
                "item": {
                    "type": "subAgentActivity", "id": "sa-1",
                    "kind": "status", "agentThreadId": "child-1",
                },
            });
            if method == "item/completed" {
                params["completedAtMs"] = json!(1);
            }
            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: method.to_owned(),
                    params,
                }))
                .await;
        }
        assert!(!processor.gap_reported);
        assert!(processor.loss_reason.is_none());
        assert!(processor.status.failure.is_none());
        // The new `CollabAgentTool` values ride the existing
        // `collabAgentToolCall` family, but only `spawnAgent` opens a spawn
        // window; the rest never create or confirm children.
        for tool in [
            "sendMessage",
            "followupTask",
            "interruptAgent",
            "listAgents",
        ] {
            let item = json!({
                "type": "collabAgentToolCall", "id": "call-x", "tool": tool,
                "senderThreadId": "owned", "receiverThreadIds": [],
                "status": "inProgress", "agentsStates": {},
            });
            assert_eq!(
                spawn_call(&item),
                None,
                "{tool} must not open a spawn window"
            );
        }
        assert!(is_tool_type("collabAgentToolCall"));
        assert!(!is_tool_type("subAgentActivity"));
        // A registered child also ignores the new item type without a gap.
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        let mut state = ChildState::new(ChildBinding {
            registration,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "child-1".to_owned(),
        });
        state.ready_committed = true;
        processor.child_states.insert("child-1".to_owned(), state);
        processor.children.confirm("owned", "child-1").unwrap();
        processor
            .handle_child_notification(
                "item/completed",
                json!({
                    "threadId": "child-1",
                    "turnId": "turn-c1",
                    "completedAtMs": 1,
                    "item": {
                        "type": "subAgentActivity", "id": "sa-2",
                        "kind": "status", "agentThreadId": "child-1",
                    },
                }),
            )
            .await;
        let state = processor.child_states.get("child-1").expect("child stays");
        assert!(!state.source_unknown);
        assert!(state.pending_gap.is_none());
        assert!(!processor.gap_reported);
    }

    #[test]
    fn sub_agent_activity_reads_only_well_formed_items() {
        assert_eq!(
            sub_agent_activity(&json!({
                "type": "subAgentActivity", "id": "sa-1",
                "agentThreadId": "child-1", "agentPath": "agents/child",
                "kind": "started",
            })),
            Some(("child-1", "started"))
        );
        for item in [
            json!({"type": "commandExecution", "id": "c", "agentThreadId": "x", "kind": "started"}),
            json!({"type": "subAgentActivity", "id": "sa", "kind": "started"}),
            json!({"type": "subAgentActivity", "id": "sa", "agentThreadId": " ", "kind": "started"}),
            json!({"type": "subAgentActivity", "id": "sa", "agentThreadId": "x"}),
            json!({"type": "subAgentActivity", "id": "sa", "agentThreadId": "x", "kind": " "}),
        ] {
            assert_eq!(sub_agent_activity(&item), None, "{item}");
        }
    }

    fn sub_agent_activity_params(method: &str, kind: &str, agent_thread: &str) -> Value {
        let mut params = json!({
            "threadId": "owned",
            "turnId": "turn-1",
            "item": {
                "type": "subAgentActivity", "id": "sa-1",
                "agentThreadId": agent_thread, "agentPath": "agents/child",
                "kind": kind,
            },
        });
        if method == "item/completed" {
            params["completedAtMs"] = json!(1);
        }
        params
    }

    /// Drives the v2 announcement order for one child: either the child's
    /// `turn/started` arrives first and buffers while the thread is
    /// unconfirmed, or the parent's `subAgentActivity` `started` confirms the
    /// thread before any child frame. The announcement rides `method`, then
    /// the other side of the item lifecycle repeats it idempotently. Either
    /// way the child registers, its buffered frames flush, and no gap is
    /// recorded.
    async fn drive_sub_agent_activity_order(method: &str, activity_first: bool) {
        use eliot_plugin::RpcRequest;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.ready = true;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());

        let turn_started = json!({"threadId": "child-1", "turn": {"id": "turn-c1"}});
        let other = if method == "item/started" {
            "item/completed"
        } else {
            "item/started"
        };

        let fake_host = async {
            let (host_read, mut host_write) = split(host_io);
            let mut host_reader = BufReader::new(host_read);
            let mut requests = Vec::new();
            let mut last_seq = HashMap::<String, i64>::new();
            // One child registration plus two event batches (child
            // session.ready, then the flushed or direct child turn.started);
            // the idempotent repeat commits nothing, and no gap ever fires.
            for _ in 0..3 {
                let mut line = String::new();
                host_reader
                    .read_line(&mut line)
                    .await
                    .expect("read fake-host request");
                let request: RpcRequest = serde_json::from_str(&line).expect("decode request");
                requests.push((request.method.clone(), request.params.clone()));
                let result = match request.method.as_str() {
                    "child" => {
                        let registration: eliot_core::ChildRegistration =
                            serde_json::from_value(request.params["registration"].clone())
                                .expect("decode child registration");
                        assert_eq!(registration.native_ref, "child-1");
                        serde_json::to_value(crate::native_child::tests::test_ack(&registration))
                            .expect("encode child ACK")
                    }
                    "events.put" => {
                        let agent_id = request.params["agent_id"]
                            .as_str()
                            .expect("agent id")
                            .to_owned();
                        let count = request.params["events"]
                            .as_array()
                            .expect("event batch")
                            .len() as i64;
                        let sequence = last_seq.entry(agent_id).or_default();
                        let first_seq = *sequence + 1;
                        *sequence = first_seq + count - 1;
                        json!({
                            "first_seq": first_seq,
                            "last_seq": *sequence,
                            "cursor": Value::Null,
                        })
                    }
                    method => panic!("unexpected fake-host request: {method}"),
                };
                let response = json!({"jsonrpc": "2.0", "id": request.id, "result": result});
                host_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .expect("send ACK");
            }
            requests
        };
        let driver = async {
            if !activity_first {
                // v2 order: the child's `turn/started` precedes any
                // announcement, so it buffers while the thread is
                // unconfirmed and never touches the owned path.
                assert!(
                    processor.take_child_frame(&turn_started),
                    "{method} last: child turn/started must route to the child path"
                );
                processor
                    .handle_frame(Some(NativeFrame::Notification {
                        method: "turn/started".to_owned(),
                        params: turn_started.clone(),
                    }))
                    .await;
                assert!(processor.children.is_unconfirmed("child-1"));
                assert_eq!(
                    processor
                        .child_buffers
                        .get("child-1")
                        .map(|queue| queue.len()),
                    Some(1)
                );
            }
            // The announcement rides `method`, then the other side of the
            // item lifecycle repeats it idempotently.
            for (position, announce) in [method, other].iter().enumerate() {
                processor
                    .handle_frame(Some(NativeFrame::Notification {
                        method: (*announce).to_owned(),
                        params: sub_agent_activity_params(announce, "started", "child-1"),
                    }))
                    .await;
                assert!(
                    processor.children.is_bound("child-1"),
                    "{method} announce {position}: activity must confirm the child"
                );
                assert!(
                    processor.child_states.contains_key("child-1"),
                    "{method} announce {position}: activity must register the child"
                );
            }
            if activity_first {
                processor
                    .handle_frame(Some(NativeFrame::Notification {
                        method: "turn/started".to_owned(),
                        params: turn_started.clone(),
                    }))
                    .await;
            }
            assert!(
                !processor.gap_reported,
                "{method} activity_first={activity_first}: no parent gap expected"
            );
            assert!(
                processor.loss_reason.is_none(),
                "{method} activity_first={activity_first}: no deferred loss expected"
            );
            assert!(
                processor.status.failure.is_none(),
                "{method} activity_first={activity_first}: no owned failure expected"
            );
            assert!(
                !processor.status.source_unknown,
                "{method} activity_first={activity_first}: owned source stays known"
            );
            assert!(
                processor.children.is_bound("child-1"),
                "{method} activity_first={activity_first}: child must confirm"
            );
            let state = processor.child_states.get("child-1").unwrap_or_else(|| {
                panic!("{method} activity_first={activity_first}: child must register")
            });
            assert_eq!(
                state.active_turn_id.as_deref(),
                Some("turn-c1"),
                "{method} activity_first={activity_first}: child turn must open"
            );
            assert!(
                processor
                    .child_buffers
                    .get("child-1")
                    .is_none_or(|queue| queue.is_empty()),
                "{method} activity_first={activity_first}: buffered frames must flush"
            );
        };
        let (requests, ()) = tokio::join!(fake_host, driver);
        peer_task.abort();

        assert!(
            requests.iter().all(|(method, _)| method != "source.gap"),
            "{method} activity_first={activity_first}: no gap RPC expected, saw {requests:?}"
        );
        assert!(
            requests
                .iter()
                .any(|(method, params)| method == "events.put"
                    && params["agent_id"] == "a2"
                    && params["events"].as_array().is_some_and(|events| events
                        .iter()
                        .any(|event| event["kind"] == "turn.started"
                            && event["data"]["turn_id"] == "turn-c1"))),
            "{method} activity_first={activity_first}: child turn.started must commit, saw {requests:?}"
        );
    }

    #[tokio::test]
    async fn sub_agent_activity_started_confirms_child_in_either_order() {
        for method in ["item/started", "item/completed"] {
            drive_sub_agent_activity_order(method, false).await;
            drive_sub_agent_activity_order(method, true).await;
        }
    }

    /// `completed` (and the other non-`started` kinds) for a known child is
    /// a lifecycle fact with no state-machine transition: no gap, no
    /// terminal, and the child's open turn is untouched.
    #[tokio::test]
    async fn sub_agent_activity_completed_for_known_child_is_ignored() {
        use tokio::io::{AsyncBufReadExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.ready = true;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());
        processor.children.confirm("owned", "child-1").unwrap();
        processor
            .children
            .parents
            .insert("child-1".to_owned(), "owned".to_owned());
        let registration = build_registration("call-1", "a1", 1, "owned", "codex:test", "child-1");
        let mut state = ChildState::new(ChildBinding {
            registration,
            agent_id: "a2".to_owned(),
            agent_generation: 1,
            native_ref: "child-1".to_owned(),
        });
        state.ready_committed = true;
        state.active_turn_id = Some("turn-c1".to_owned());
        processor.child_states.insert("child-1".to_owned(), state);

        // No host RPC is expected at all: nothing registers, nothing commits,
        // nothing gaps. A stray request fails the timeout read below.
        let (host_read, _host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        for method in ["item/started", "item/completed"] {
            for kind in ["interacted", "interrupted", "completed"] {
                processor
                    .handle_frame(Some(NativeFrame::Notification {
                        method: method.to_owned(),
                        params: sub_agent_activity_params(method, kind, "child-1"),
                    }))
                    .await;
            }
        }
        let state = processor.child_states.get("child-1").expect("child stays");
        assert_eq!(state.active_turn_id.as_deref(), Some("turn-c1"));
        assert!(state.final_text.is_none());
        assert!(!state.source_unknown);
        assert!(state.pending_gap.is_none());
        assert!(!processor.gap_reported);
        assert!(processor.loss_reason.is_none());
        assert!(processor.status.failure.is_none());

        let mut line = String::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), host_reader.read_line(&mut line))
                .await
                .is_err(),
            "lifecycle facts must send no host RPC, saw {line:?}"
        );
        peer_task.abort();
    }

    /// Activity naming a foreign thread with a kind other than `started` is
    /// not a confirmation: nothing registers and nothing gaps.
    #[tokio::test]
    async fn sub_agent_activity_completed_for_unknown_thread_never_confirms() {
        use tokio::io::{AsyncBufReadExt, BufReader, split};

        let (_native, mut processor, host_io, peer_task) = connected_child_processor("a1").await;
        processor.status.ready = true;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor.status.active_command_id = Some("cmd-1".to_owned());

        let (host_read, _host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        for method in ["item/started", "item/completed"] {
            processor
                .handle_frame(Some(NativeFrame::Notification {
                    method: method.to_owned(),
                    params: sub_agent_activity_params(method, "completed", "foreign-9"),
                }))
                .await;
        }
        assert!(!processor.children.is_bound("foreign-9"));
        assert!(!processor.child_states.contains_key("foreign-9"));
        assert!(!processor.child_pending.contains_key("foreign-9"));
        assert!(!processor.children.is_unconfirmed("foreign-9"));
        assert!(!processor.gap_reported);
        assert!(processor.loss_reason.is_none());
        assert!(processor.status.failure.is_none());
        assert!(!processor.status.source_unknown);

        let mut line = String::new();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), host_reader.read_line(&mut line))
                .await
                .is_err(),
            "foreign lifecycle facts must send no host RPC, saw {line:?}"
        );
        peer_task.abort();
    }
}
