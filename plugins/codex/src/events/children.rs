//! Codex child-event registration, buffering and settlement.
//!
//! Parent turns own their facts; this module owns child-thread tracking,
//! usage attribution, frame buffering and gap settlement for child threads.
//! The shared runtime owns child registration and turn state.
use super::*;
use eliot_plugin::native_child::ChildLifecycle;
/// Bound on tracked child threads, unconfirmed threads and in-flight spawns per
/// generation; overflow is a source gap.
pub(super) const MAX_CHILD_THREADS: usize = 256;
pub(super) struct ChildState {
    binding: ChildBinding,
    writes_blocked: bool,
    gap_reported: bool,
    gap_number: u64,
    source_unknown: bool,
    batch_unknown: bool,
    failure: Option<String>,
    final_text: Option<String>,
    lifecycle: ChildLifecycle,
    unclassified_seq: u64,
    /// Baseline and latest cumulative token snapshot for this exact child
    /// turn. The terminal event carries only their difference.
    turn_usage: Option<(String, NativeTokenUsageDelta, NativeTokenUsageDelta)>,
    ready_committed: bool,
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
            lifecycle: ChildLifecycle::default(),
            unclassified_seq: 0,
            turn_usage: None,
            ready_committed: false,
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
        source.commit_event(kind, native_id, data).await
    }

    async fn commit_message(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        native_id: String,
        data: Value,
    ) -> Result<MessageCommit> {
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
        source.commit_message(native_id, data).await
    }

    async fn record_gap(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        reason: &str,
    ) -> Result<()> {
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
        source.record_gap(reason).await
    }

    async fn record_unclassified(
        &mut self,
        child_thread: &str,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        site: &'static str,
        native_tag: &str,
    ) -> Result<()> {
        self.unclassified_seq = self.unclassified_seq.wrapping_add(1);
        let item_id = format!("unclassified-{}", self.unclassified_seq);
        let native_id = event_id(child_thread, "session", &item_id, UNCLASSIFIED_NATIVE_EVENT)?;
        self.commit_event(
            sink,
            host,
            credential,
            UNCLASSIFIED_NATIVE_EVENT,
            native_id,
            unclassified(site, native_tag),
        )
        .await
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
            .any(|(thread, state)| thread != excluding && state.lifecycle.active_turn.is_some())
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
    pub(super) fn take_child_frame(&mut self, params: &Value, allow_unconfirmed: bool) -> bool {
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
        if !allow_unconfirmed {
            return self
                .child_states
                .get(thread)
                .is_some_and(|state| state.ready_committed);
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
    async fn record_child_gap_visible(&mut self, child_thread: &str, reason: &str) {
        let Some(state) = self.child_states.get_mut(child_thread) else {
            return;
        };
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        // A child gap updates only the child latches.
        let _ = state.record_gap(sink, &host, &credential, reason).await;
    }

    async fn handle_thread_started_error(&mut self, child_thread: &str, error: Error) {
        if self.child_states.contains_key(child_thread) {
            let rejected = error.failure_where.as_deref() == Some("plugin.events.put");
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
                (
                    !rejected && !state.gap_reported && !state.writes_blocked,
                    reason,
                )
            };
            if should_gap {
                self.record_child_gap_visible(child_thread, &reason).await;
            }
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
            .is_some_and(|state| state.writes_blocked)
        {
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
            let rejected = error.failure_where.as_deref() == Some("plugin.events.put");
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
                (
                    !rejected && !state.gap_reported && !state.writes_blocked,
                    reason,
                )
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
        let ack = match register_child(
            &self.host,
            &self.credential,
            registration.clone(),
            "codex",
            &eliot_core::CODEX_NATIVE_CHILD_FAMILIES,
        )
        .await
        {
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
            .is_some_and(|state| state.writes_blocked)
        {
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
                .is_some_and(|state| state.writes_blocked)
            {
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
                        let rejected = error.failure_where.as_deref() == Some("plugin.events.put");
                        let (should_gap, code) = {
                            let Some(state) = self.child_states.get_mut(child_thread) else {
                                continue;
                            };
                            state.source_unknown = true;
                            let code = error.code.clone();
                            if state.failure.is_none() {
                                state.failure = Some(*error.message);
                            }
                            (
                                !rejected && !state.gap_reported && !state.writes_blocked,
                                code,
                            )
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
                let native_status = params
                    .pointer("/turn/status")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Error::new("native_protocol", "Codex child turn/started omitted status")
                    })?;
                let unknown_status = (!matches!(
                    native_status,
                    "completed" | "interrupted" | "failed" | "inProgress"
                ))
                .then(|| native_status.to_owned());
                let state = self
                    .child_states
                    .get_mut(child_thread)
                    .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
                state.lifecycle.started(turn_id);
                state.final_text = None;
                state.turn_usage = None;
                if let Some(native_tag) = unknown_status.as_deref() {
                    state
                        .record_unclassified(
                            child_thread,
                            &mut self.sink,
                            &self.host,
                            &self.credential,
                            "codex.native_rpc.turn_status",
                            native_tag,
                        )
                        .await?;
                }
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
                if let Some((site, native_tag, _)) = unclassified_native_item(&raw_item) {
                    let host = self.host.clone();
                    let credential = self.credential.clone();
                    let state = self
                        .child_states
                        .get_mut(child_thread)
                        .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
                    state
                        .record_unclassified(
                            child_thread,
                            &mut self.sink,
                            &host,
                            &credential,
                            site,
                            native_tag,
                        )
                        .await?;
                }
                self.observe_child_spawn(child_thread, &raw_item, false)
                    .await?;
                // Depth-2 v2 announcement from a confirmed child: same
                // confirmation as the owned thread's, idempotent on repeat.
                self.observe_sub_agent_activity(child_thread, &raw_item)
                    .await?;
            }
            "item/completed" => {
                let raw_item = params.get("item").cloned().unwrap_or(Value::Null);
                if let Some((site, native_tag, _)) = unclassified_native_item(&raw_item) {
                    let host = self.host.clone();
                    let credential = self.credential.clone();
                    let state = self
                        .child_states
                        .get_mut(child_thread)
                        .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
                    state
                        .record_unclassified(
                            child_thread,
                            &mut self.sink,
                            &host,
                            &credential,
                            site,
                            native_tag,
                        )
                        .await?;
                }
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
            "thread/tokenUsage/updated" => {
                self.note_child_token_usage(child_thread, &params)?;
            }
            _ => {
                let host = self.host.clone();
                let credential = self.credential.clone();
                let state = self
                    .child_states
                    .get_mut(child_thread)
                    .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
                state
                    .record_unclassified(
                        child_thread,
                        &mut self.sink,
                        &host,
                        &credential,
                        "codex.native_rpc.notification",
                        method,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    fn note_child_token_usage(&mut self, child_thread: &str, params: &Value) -> Result<()> {
        let notification: NativeThreadTokenUsageUpdatedNotification =
            serde_json::from_value(params.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Codex child thread/tokenUsage/updated did not match the 0.167 schema",
                )
            })?;
        if notification.thread_id != child_thread || notification.turn_id.trim().is_empty() {
            return Err(Error::new(
                "source_integrity",
                "Codex child token usage did not belong to its exact thread and turn",
            ));
        }
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        if state.lifecycle.active_turn.as_deref() != Some(notification.turn_id.as_str())
            || !notification.token_usage.last.has_response_counters()
        {
            // A late or stale update must never bind to another child turn.
            return Ok(());
        }
        let Some(total) = notification.token_usage.total else {
            return Ok(());
        };
        match state.turn_usage.as_mut() {
            Some((turn_id, _, latest)) if turn_id == &notification.turn_id => *latest = total,
            _ => {
                let baseline = usage_baseline(&total, &notification.token_usage.last);
                state.turn_usage = Some((notification.turn_id, baseline, total));
            }
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
                .and_then(|state| state.lifecycle.active_turn.as_deref()),
        );
        let mut data = json!({
            "turn_id": turn_id.as_str(),
            "text": text,
        });
        if let Some(phase) = super::native_message_phase(item) {
            data["phase"] = Value::String(phase.to_owned());
        }
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        let native_id = event_id(child_thread, &turn_id, item_id, "message.completed")?;
        state
            .commit_message(sink, &host, &credential, native_id, data)
            .await
            .map(|_| ())
    }

    async fn complete_child_turn(&mut self, child_thread: &str, params: Value) -> Result<()> {
        let native_status = params
            .pointer("/turn/status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
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
        if matches!(&notification.turn.status, NativeTurnStatus::Other) {
            state
                .record_unclassified(
                    child_thread,
                    sink,
                    &host,
                    &credential,
                    "codex.native_rpc.turn_status",
                    &native_status,
                )
                .await?;
        }
        let status_text = turn_status(&notification.turn.status);
        let mut data = json!({
            "turn_id": notification.turn.id,
            "status": status_text,
        });
        if matches!(&notification.turn.status, NativeTurnStatus::Other) {
            data["outcome"] = json!("unknown");
        }
        let turn_usage = state
            .turn_usage
            .take()
            .filter(|(turn_id, _, _)| turn_id == &notification.turn.id)
            .and_then(|(_, baseline, latest)| turn_expenditure(&baseline, &latest));
        if let Some(usage) = turn_usage {
            data["usage"] = serde_json::to_value(usage).map_err(|_| {
                Error::new("internal", "Codex child turn usage could not be encoded")
            })?;
        }
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        attach_native_error(&mut data, notification.turn.error.as_ref(), &policy);
        if let Some(text) = state.final_text.take() {
            attach_final_text(&mut data, &text);
        }
        let native_id = event_id(
            child_thread,
            &notification.turn.id,
            &notification.turn.id,
            "turn.completed",
        )?;
        state
            .commit_event(sink, &host, &credential, "turn.completed", native_id, data)
            .await?;
        state.lifecycle.completed(Some(&status_text));
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
