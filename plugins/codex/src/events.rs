use crate::native_child::{
    ChildBinding, binding_from_ack, build_registration, is_admission_rejection, register_child,
};
use eliot_core::{
    ChildRegistration, ContextItem, Error, ProviderRestriction, Result, SecretPolicy, SourceGap,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    protocol::MAX_COMMAND_ID_BYTES,
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text as shared_attach_final_text,
        attach_native_error as shared_attach_native_error, event_id as shared_event_id,
        native_no_write, next_input_outcomes, rejected_control, unknown_control,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval, timeout_at},
};

use crate::native::{
    NativeClient, NativeFrame, StreamState,
    requests::{self, CorePending},
    types::{
        NativeFileChangeItem, NativeItemCompletedNotification, NativePatchChangeKind,
        NativeServerRequestResolvedNotification, NativeTurnCompletedNotification, NativeTurnError,
        NativeTurnStartedNotification, NativeTurnStatus,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 128;
const RESOLVED_REQUEST_HISTORY: usize = 128;
/// Bound on tracked child threads, unconfirmed threads and in-flight spawns per
/// generation; overflow is a source gap.
const MAX_CHILD_THREADS: usize = 256;
const MAX_CHANGED_PATHS: usize = 1_000;
/// Independent child-gap retry cadence: the event loop drives ONE retained
/// child gap per tick until its canonical ACK or shutdown. Same 1s cadence
/// as the host live poll; private, not a public quota config.
const CHILD_GAP_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub native_drained: bool,
    pub ready: bool,
    pub thread_id: Option<String>,
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub last_turn: Option<TurnFacts>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    pub stopped: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TurnFacts {
    pub command_id: String,
    pub turn_id: String,
    pub status: Option<String>,
    pub final_text: Option<String>,
    pub terminal: bool,
}

pub(crate) struct EventConfig {
    pub credential: String,
    pub agent_id: String,
    pub generation: i64,
    pub secrets: Vec<String>,
    pub native_scope: String,
}

pub(crate) struct EventHandle {
    control: mpsc::Sender<Control>,
    status: watch::Receiver<EventStatus>,
}

enum Control {
    Ready {
        native_scope: String,
        thread_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateTurn {
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
        reply: oneshot::Sender<Result<()>>,
    },
    BeginStop {
        reply: oneshot::Sender<Result<()>>,
    },
    EndStop {
        reply: oneshot::Sender<Result<()>>,
    },
    ReplyRequest {
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
        reply: oneshot::Sender<Result<Value>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
enum BufferedNativeFrame {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Clone)]
struct BufferedFrame {
    frame: BufferedNativeFrame,
    size: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Written,
    Unknown,
}

struct PendingNativeRequest {
    request: CorePending,
    transport_id: Value,
    reply_command_id: Option<String>,
    reply_state: NativeReplyState,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeItemStarted {
    item: Value,
    thread_id: String,
    turn_id: String,
}

struct EventProcessor {
    native: NativeClient,
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    secrets: Vec<String>,
    status: EventStatus,
    buffer: VecDeque<BufferedFrame>,
    buffer_bytes: usize,
    pending_requests: HashMap<String, PendingNativeRequest>,
    transport_requests: HashMap<String, String>,
    resolved_transport_ids: HashSet<String>,
    resolved_transport_order: VecDeque<String>,
    gap_number: u64,
    accept_frames: bool,
    planned_stop: bool,
    loss_reason: Option<String>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    final_text: Option<String>,
    native_scope: String,
    child_bindings: HashMap<String, ChildBinding>,
    child_states: HashMap<String, ChildState>,
    child_buffers: HashMap<String, VecDeque<BufferedNativeFrame>>,
    child_buffer_bytes: usize,
    child_unknown: HashSet<String>,
    child_early_lost: HashSet<String>,
    child_pending: HashMap<String, ChildRegistration>,
    children: ChildThreads,
    gap_last_served: Option<String>,
    /// The native transport itself was lost (queue overflow, unplanned close,
    /// malformed frame, unplanned reader end): facts may be missing, so the source
    /// is marked `stale` once drained, alongside any recorded gap.
    /// Semantic/normalization gaps never set this.
    transport_loss: bool,
    /// The one accompanying `stale` update was attempted (never replayed).
    source_health_attempted: bool,
}

struct ChildState {
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
/// generation's lifecycle. A child's first frames can precede both its
/// `thread/started` and the spawn result, so frames of an unannounced thread
/// are taken only while the owned thread has a `spawnAgent` call in flight;
/// such a thread must be confirmed before that window closes.
#[derive(Default)]
struct ChildThreads {
    confirmed: HashSet<String>,
    pending_spawns: HashMap<String, HashSet<String>>,
    unconfirmed: HashMap<String, Option<String>>,
    bound: HashSet<String>,
    parents: HashMap<String, String>,
    spawn_items: HashMap<String, String>,
}

impl ChildThreads {
    fn take_frame(&mut self, thread: &str) -> std::result::Result<bool, &'static str> {
        if self.confirmed.contains(thread) || self.bound.contains(thread) {
            return Ok(true);
        }
        if self.unconfirmed.contains_key(thread) {
            return Ok(true);
        }
        if !self.has_any_window() {
            return Ok(false);
        }
        if self.unconfirmed.len() >= MAX_CHILD_THREADS && !self.unconfirmed.contains_key(thread) {
            return Err("Codex sent frames from more unannounced threads than are tracked");
        }
        self.unconfirmed.insert(thread.to_owned(), None);
        Ok(true)
    }
    fn has_any_window(&self) -> bool {
        self.has_pending()
    }
    fn has_pending(&self) -> bool {
        self.pending_spawns.values().any(|items| !items.is_empty())
    }
    fn pending_count(&self) -> usize {
        self.pending_spawns.values().map(HashSet::len).sum()
    }
    fn pending_items<'a>(&'a self, producer: &'a str) -> Vec<&'a String> {
        self.pending_spawns
            .get(producer)
            .map(|items| items.iter().collect::<Vec<_>>())
            .unwrap_or_default()
    }
    fn producers(&self) -> Vec<String> {
        self.pending_spawns
            .iter()
            .filter(|(_, items)| !items.is_empty())
            .map(|(producer, _)| producer.clone())
            .collect()
    }
    fn settle_all(&mut self) -> std::result::Result<(), &'static str> {
        let producers = self.producers();
        if producers.is_empty() && self.unconfirmed.is_empty() {
            return Ok(());
        }
        if producers.is_empty() {
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

    fn is_parent(&self, owned: &str, parent: &str) -> bool {
        parent == owned || self.confirmed.contains(parent) || self.bound.contains(parent)
    }

    fn confirm(&mut self, owned: &str, thread: &str) -> std::result::Result<(), &'static str> {
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

pub(crate) fn spawn(
    frames: mpsc::Receiver<NativeFrame>,
    stream: watch::Receiver<StreamState>,
    native: NativeClient,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let processor = EventProcessor::new(native, host, config);
    let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (status_tx, status) = watch::channel(EventStatus::default());
    tokio::spawn(run(frames, stream, processor, control_rx, status_tx));
    EventHandle { control, status }
}

impl EventHandle {
    pub async fn ready(
        &self,
        native_scope: String,
        thread_id: String,
        effective: Value,
    ) -> Result<()> {
        self.send(|reply| Control::Ready {
            native_scope,
            thread_id,
            effective,
            reply,
        })
        .await
    }

    pub async fn associate_turn(
        &self,
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
    ) -> Result<()> {
        self.send(|reply| Control::AssociateTurn {
            command_id,
            turn_id,
            context,
            reply,
        })
        .await
    }

    pub async fn begin_stop(&self) -> Result<()> {
        self.send(|reply| Control::BeginStop { reply }).await
    }

    pub async fn end_stop(&self) -> Result<()> {
        self.send(|reply| Control::EndStop { reply }).await
    }

    pub async fn reply_request(
        &self,
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
    ) -> Result<Value> {
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + EVENT_DEADLINE,
            self.control.send(Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("busy", "Codex event-control queue is full"))?
        .map_err(|_| Error::new("native_not_dispatched", "Codex event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Codex request-reply outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Codex request-reply response was lost"))?
    }

    pub async fn close(&self) {
        let (reply, response) = oneshot::channel();
        if self.control.send(Control::Close { reply }).await.is_ok() {
            let _ = timeout_at(Instant::now() + EVENT_DEADLINE, response).await;
        }
    }

    pub async fn drain_native(&self) -> Result<()> {
        eliot_plugin::native_lifecycle::wait_for_native_tail(&self.status, |s| s.native_drained)
            .await
    }

    pub fn status(&self) -> EventStatus {
        self.status.borrow().clone()
    }

    async fn send(&self, build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control) -> Result<()> {
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + EVENT_DEADLINE,
            self.control.send(build(reply)),
        )
        .await
        .map_err(|_| Error::new("unknown", "Codex event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "Codex event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Codex event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Codex event-control response was lost"))?
    }
}

impl EventProcessor {
    fn new(native: NativeClient, host: PeerClient, config: EventConfig) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
            native,
            host,
            sink,
            credential: config.credential,
            agent_id: config.agent_id,
            generation: config.generation,
            secrets: config.secrets,
            status: EventStatus::default(),
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            pending_requests: HashMap::new(),
            transport_requests: HashMap::new(),
            resolved_transport_ids: HashSet::new(),
            resolved_transport_order: VecDeque::new(),
            gap_number: 0,
            accept_frames: true,
            planned_stop: false,
            loss_reason: None,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            final_text: None,
            native_scope: config.native_scope.clone(),
            child_bindings: HashMap::new(),
            child_states: HashMap::new(),
            child_buffers: HashMap::new(),
            child_buffer_bytes: 0,
            child_unknown: HashSet::new(),
            child_early_lost: HashSet::new(),
            child_pending: HashMap::new(),
            children: ChildThreads::default(),
            gap_last_served: None,
            transport_loss: false,
            source_health_attempted: false,
        }
    }

    async fn handle_control(&mut self, control: Control) -> bool {
        match control {
            Control::Ready {
                native_scope,
                thread_id,
                effective,
                reply,
            } => {
                let result = self.ready(native_scope, thread_id, effective).await;
                let _ = reply.send(result);
                true
            }
            Control::AssociateTurn {
                command_id,
                turn_id,
                context,
                reply,
            } => {
                let result = self.associate_turn(command_id, turn_id, context).await;
                let _ = reply.send(result);
                true
            }
            Control::BeginStop { reply } => {
                self.planned_stop = true;
                let _ = reply.send(Ok(()));
                true
            }
            Control::EndStop { reply } => {
                if self.frames_drained {
                    let result = self.finish_stop().await;
                    let _ = reply.send(result);
                } else if self.pending_end_stop.is_some() {
                    let _ = reply.send(Err(Error::new(
                        "bad_state",
                        "Codex stop is already waiting for native output drain",
                    )));
                } else {
                    self.pending_end_stop = Some(reply);
                }
                true
            }
            Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            } => {
                let result = self
                    .reply_request(&command_id, &request_id, &turn_id, &verb, &args)
                    .await;
                let _ = reply.send(result);
                true
            }
            Control::Close { reply } => {
                if let Some(pending) = self.pending_end_stop.take() {
                    let _ = pending.send(Err(Error::new(
                        "unknown",
                        "Codex native event queue closed before stop settlement",
                    )));
                }
                let _ = reply.send(());
                false
            }
        }
    }

    async fn handle_stream_state(&mut self, current: StreamState) {
        if current == StreamState::Open {
            return;
        }
        match current {
            StreamState::Closed => {
                if !self.planned_stop {
                    self.defer_gap("Codex app-server event stream closed");
                    self.transport_loss = true;
                }
            }
            StreamState::Overflow => {
                self.defer_gap("Codex app-server event queue overflowed");
                self.transport_loss = true;
            }
            StreamState::Malformed(fault) => {
                self.defer_gap(format!(
                    "Codex app-server stream contained an invalid frame ({})",
                    fault.describe()
                ));
                self.transport_loss = true;
            }
            StreamState::Open => {}
        }
    }

    async fn handle_frame(&mut self, frame: Option<NativeFrame>) {
        match frame {
            Some(NativeFrame::Request { id, method, params }) => {
                if self.take_child_frame(&params) {
                    self.reject_child_request(id, &method, &params).await;
                } else if !self.status.ready || self.status.active_turn_id.is_none() {
                    self.buffer_frame(BufferedNativeFrame::Request { id, method, params });
                } else if let Err(error) = self.process_request(id, method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        let _ = self
                            .record_gap("Codex native request could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(error.message);
                    }
                }
            }
            Some(NativeFrame::Notification { method, params }) => {
                if self.take_child_frame(&params) {
                    self.handle_child_notification(&method, params).await;
                    return;
                }
                if self.should_buffer(&method, &params) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(&method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        // Method and error code only: no native payload text.
                        let reason = format!(
                            "Codex {method} notification could not be normalized ({})",
                            error.code
                        );
                        let _ = self.record_gap(&reason).await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(error.message);
                    }
                }
            }
            None => {
                self.frames_drained = true;
                self.accept_frames = false;
                if !self.planned_stop {
                    // Unplanned reader end: the transport is over.
                    self.transport_loss = true;
                }
                if !self.buffer.is_empty() {
                    self.buffer.clear();
                    self.buffer_bytes = 0;
                    self.defer_gap("Codex native events remained unassociated at stream end");
                }
                if !self.pending_requests.is_empty() {
                    self.defer_gap("Codex native requests remained unresolved at stream end");
                }
                if !self.planned_stop && self.loss_reason.is_none() {
                    self.defer_gap("Codex native event reader closed");
                }
                self.settle_child_tracking();
                self.report_gap_if_queue_empty(true).await;
                self.finish_pending_end_stop().await;
            }
        }
    }

    fn defer_gap(&mut self, reason: impl Into<String>) {
        if self.loss_reason.is_none() {
            self.loss_reason = Some(reason.into());
        }
    }

    async fn report_gap_if_queue_empty(&mut self, queue_empty: bool) {
        if queue_empty
            && !self.gap_reported
            && !self.writes_blocked
            && let Some(reason) = self.loss_reason.clone()
        {
            let _ = self.record_gap(&reason).await;
        }
        if queue_empty && self.transport_loss {
            self.mark_source_stale().await;
        }
    }

    /// One explicit `stale` health update for a lost native transport, through the
    /// existing `source.health` mechanism. Attempted once and never replayed.
    async fn mark_source_stale(&mut self) {
        if self.source_health_attempted {
            return;
        }
        self.source_health_attempted = true;
        let request = json!({
            "credential": self.credential.clone(),
            "agent_id": self.agent_id.clone(),
            "generation": self.generation,
            "source": "stream",
            "health": "stale",
        });
        let _ = self
            .host
            .call("source.health", request, Instant::now() + EVENT_DEADLINE)
            .await;
    }

    fn settle_spawn_window(&mut self) {
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

    fn settle_child_tracking(&mut self) {
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
    fn take_child_frame(&mut self, params: &Value) -> bool {
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
    async fn observe_spawn(&mut self, item: &Value, completed: bool) -> Result<()> {
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
    async fn reject_child_request(&mut self, id: Value, method: &str, params: &Value) {
        let rejected = self
            .native
            .reject_unsupported_request(id, method, params, Instant::now() + EVENT_DEADLINE)
            .await;
        if rejected.is_err() {
            self.defer_gap("Codex child-thread request could not be rejected");
        }
    }

    fn should_buffer(&self, method: &str, params: &Value) -> bool {
        if method == "serverRequest/resolved"
            && params
                .get("requestId")
                .and_then(|id| transport_key(id).ok())
                .is_some_and(|key| self.resolved_transport_ids.contains(&key))
        {
            return false;
        }
        matches!(
            method,
            "turn/started"
                | "turn/completed"
                | "item/started"
                | "item/completed"
                | "serverRequest/resolved"
                | "error"
        ) && (!self.status.ready || self.status.active_turn_id.is_none())
    }

    fn buffer_frame(&mut self, frame: BufferedNativeFrame) {
        let encoded = match &frame {
            BufferedNativeFrame::Notification { method, params } => {
                serde_json::to_vec(&json!({"method": method, "params": params}))
            }
            BufferedNativeFrame::Request { id, method, params } => {
                serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))
            }
        };
        let size = encoded
            .map(|bytes| bytes.len())
            .unwrap_or_else(|_| EARLY_EVENT_BYTES.saturating_add(1));
        if self.buffer.len() >= EARLY_EVENT_LIMIT
            || self.buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            self.buffer.clear();
            self.buffer_bytes = 0;
            self.defer_gap("Codex early-event buffer exceeded its bound");
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedFrame { frame, size });
    }

    async fn ready(
        &mut self,
        native_scope: String,
        thread_id: String,
        effective: Value,
    ) -> Result<()> {
        if self.status.ready || thread_id.trim().is_empty() || !effective.is_object() {
            return Err(Error::new(
                "invalid",
                "Codex readiness requires one exact thread and effective settings",
            ));
        }
        if self
            .status
            .thread_id
            .as_deref()
            .is_some_and(|observed| observed != thread_id)
        {
            self.record_gap("Codex thread/start response disagreed with thread/started")
                .await?;
            return Err(Error::new(
                "source_integrity",
                "Codex thread identity did not match its start notification",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Codex event source acknowledgement is unresolved",
            ));
        }
        self.native_scope = native_scope.clone();
        self.status.thread_id = Some(thread_id.clone());
        let native_id = format!("codex:{thread_id}:session.ready");
        self.commit_event(
            "session.ready",
            native_id,
            json!({
                "native_scope": native_scope,
                "native_ref": thread_id,
                "effective": effective,
            }),
        )
        .await?;
        self.status.ready = true;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Codex event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Codex event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    async fn associate_turn(
        &mut self,
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.thread_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Codex turn association requires a ready exact thread and turn",
            ));
        }
        if self.status.active_turn_id.is_some() {
            return Err(Error::new(
                "busy",
                "Codex already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Codex event source acknowledgement is unresolved",
            ));
        }
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        self.final_text = None;
        let thread_id = self.status.thread_id.as_deref().unwrap_or_default();
        let native_id = event_id(thread_id, &turn_id, &turn_id, "turn.started")?;
        let mut data = json!({"command_id": command_id, "turn_id": turn_id});
        if !context.is_empty() {
            data["context_outcomes"] = json!(next_input_outcomes(&context, &command_id));
        }
        self.commit_event("turn.started", native_id, data).await?;
        self.flush_buffer().await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            let should_buffer = match &frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    self.should_buffer(method, params)
                }
                BufferedNativeFrame::Request { .. } => {
                    !self.status.ready || self.status.active_turn_id.is_none()
                }
            };
            if should_buffer {
                self.buffer.push_front(frame);
                self.buffer_bytes = self
                    .buffer_bytes
                    .saturating_add(self.buffer.front().map_or(0, |item| item.size));
                break;
            }
            let child = match &frame.frame {
                BufferedNativeFrame::Notification { params, .. }
                | BufferedNativeFrame::Request { params, .. } => self.take_child_frame(params),
            };
            if child {
                match frame.frame {
                    BufferedNativeFrame::Request { id, method, params } => {
                        self.reject_child_request(id, &method, &params).await;
                    }
                    BufferedNativeFrame::Notification { method, params } => {
                        self.handle_child_notification(&method, params).await;
                    }
                }
                continue;
            }
            let result = match frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    self.process_notification(&method, params).await
                }
                BufferedNativeFrame::Request { id, method, params } => {
                    self.process_request(id, method, params).await
                }
            };
            if let Err(error) = result {
                self.status.source_unknown = true;
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Buffered Codex native frame could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(error.message);
                }
                return Err(Error::new(
                    "unknown",
                    "Buffered Codex event outcome is unresolved",
                ));
            }
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    async fn process_request(&mut self, id: Value, method: String, params: Value) -> Result<()> {
        let thread_id = self
            .status
            .thread_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Codex request has no owned thread"))?;
        let turn_id = self
            .status
            .active_turn_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Codex request has no associated turn"))?;
        let mut request =
            match requests::describe_request(&method, &params, &id, thread_id, turn_id) {
                Ok(request) => request,
                Err(error) => {
                    let rejected = self
                        .native
                        .reject_unsupported_request(
                            id,
                            &method,
                            &params,
                            Instant::now() + EVENT_DEADLINE,
                        )
                        .await;
                    if error.code != "unsupported" || rejected.is_err() {
                        self.defer_gap(if error.code == "unsupported" {
                            "Codex unsupported native request could not be rejected"
                        } else {
                            "Codex native request did not match its exact thread/turn schema"
                        });
                    }
                    return Ok(());
                }
            };
        let transport_id = request
            .details
            .get("transport_id")
            .cloned()
            .ok_or_else(|| Error::new("source_integrity", "Codex request omitted transport id"))?;
        let transport_key = transport_key(&transport_id)?;
        let request_details = request.details.as_object_mut().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "Codex request details are not an object",
            )
        })?;
        request_details.remove("transport_id");
        request_details.remove("native_method");
        let request_id = request.id.clone();
        if let Some(existing) = self.pending_requests.get(&request_id) {
            if existing.request == request && existing.transport_id == transport_id {
                return Ok(());
            }
            return Err(Error::new(
                "source_integrity",
                "Codex reused a pending request id with different native facts",
            ));
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            let _ = self
                .native
                .reject_unsupported_request(id, &method, &params, Instant::now() + EVENT_DEADLINE)
                .await;
            self.defer_gap("Codex pending native request bound was exceeded");
            return Ok(());
        }
        if self.transport_requests.contains_key(&transport_key) {
            return Err(Error::new(
                "source_integrity",
                "Codex reused a native transport id for another pending request",
            ));
        }

        let command_id = self
            .status
            .active_command_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Codex request has no Eliot send command"))?;
        let Ok((public_kind, public_details)) = requests::public_request_view(&request) else {
            self.defer_gap("Codex public request view could not be normalized");
            return Ok(());
        };
        let data = json!({
            "command_id": command_id,
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": public_kind,
            "details": public_details,
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            let _ = self
                .native
                .reject_unsupported_request(id, &method, &params, Instant::now() + EVENT_DEADLINE)
                .await;
            self.defer_gap("Codex normalized request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&request_id, "request.opened") {
            Ok(native_id) => native_id,
            Err(_) => {
                let _ = self
                    .native
                    .reject_unsupported_request(
                        id,
                        &method,
                        &params,
                        Instant::now() + EVENT_DEADLINE,
                    )
                    .await;
                self.defer_gap("Codex native request event identity exceeded its bound");
                return Ok(());
            }
        };
        self.commit_event("request.opened", native_id, data).await?;

        self.pending_requests.insert(
            request_id.clone(),
            PendingNativeRequest {
                request,
                transport_id,
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
            },
        );
        self.transport_requests.insert(transport_key, request_id);
        Ok(())
    }

    async fn reply_request(
        &mut self,
        command_id: &str,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
    ) -> Result<Value> {
        if command_id.trim().is_empty() || request_id.trim().is_empty() || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Codex request reply requires exact command, request, and turn ids",
            ));
        }
        let Some(entry) = self.pending_requests.get_mut(request_id) else {
            return Ok(rejected_control("Codex request is no longer pending"));
        };
        if entry.request.turn_id != turn_id {
            return Ok(rejected_control(
                "Codex request reply does not match the captured turn",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Ok(rejected_control(
                "Codex request is no longer attached to the active turn",
            ));
        }
        match entry.reply_state {
            NativeReplyState::Writing | NativeReplyState::Unknown => {
                return Ok(unknown_control(
                    "Codex request reply outcome is unresolved and will not be repeated",
                ));
            }
            NativeReplyState::Written => {
                return Ok(rejected_control(
                    "Codex request already has a native response and awaits resolution",
                ));
            }
            NativeReplyState::Available => {}
        }
        if self.status.thread_id.is_none() || self.status.stopped {
            return Ok(rejected_control(
                "Codex request session is no longer writable",
            ));
        }
        let response = match requests::reply_for(&entry.request, verb, args) {
            Ok(response) => response,
            Err(error) => return Ok(rejected_control(&error.message)),
        };
        let request = entry.request.clone();
        let transport_id = entry.transport_id.clone();
        entry.reply_state = NativeReplyState::Writing;

        match self
            .native
            .reply_server_request(transport_id, response, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(()) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Written;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(json!({
                    "state": "applied",
                    "command_id": command_id,
                    "request_id": request.id,
                    "turn_id": request.turn_id,
                }))
            }
            Err(error) if native_no_write(&error.code) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                }
                Ok(rejected_control("Codex request reply was not dispatched"))
            }
            Err(_) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(unknown_control(
                    "Codex request reply may have been dispatched; it will not be repeated",
                ))
            }
        }
    }

    async fn resolve_server_request(&mut self, params: Value) -> Result<()> {
        let notification: NativeServerRequestResolvedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Codex serverRequest/resolved did not match the 0.161 schema",
                )
            })?;
        let thread_id =
            self.status.thread_id.as_deref().ok_or_else(|| {
                Error::new("unknown", "Codex request resolution has no owned thread")
            })?;
        if notification.thread_id != thread_id {
            return self
                .record_gap("Codex request resolution named another native thread")
                .await;
        }
        let transport_id = serde_json::to_value(notification.request_id)
            .map_err(|_| Error::new("native_protocol", "Codex request id could not be encoded"))?;
        let key = transport_key(&transport_id)?;
        if self.resolved_transport_ids.contains(&key) {
            return Ok(());
        }
        let Some(request_id) = self.transport_requests.get(&key).cloned() else {
            return self
                .record_gap("Codex request resolution did not match a retained native callback")
                .await;
        };
        let Some(entry) = self.pending_requests.get(&request_id) else {
            return self
                .record_gap("Codex request resolution lost its pending request")
                .await;
        };
        let turn_id = entry.request.turn_id.clone();
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str()) {
            return self
                .record_gap("Codex request resolution arrived outside its active turn")
                .await;
        }
        let (kind, data) = match (entry.reply_state, entry.reply_command_id.as_ref()) {
            (NativeReplyState::Written, Some(command_id)) => (
                "request.resolved",
                json!({
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
            _ => (
                "request.closed",
                json!({
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
        };
        let native_id = request_event_id(&request_id, kind)?;
        self.commit_event(kind, native_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.transport_requests.remove(&key);
        self.remember_resolved_transport(key);
        Ok(())
    }

    fn remember_resolved_transport(&mut self, key: String) {
        self.resolved_transport_ids.insert(key.clone());
        self.resolved_transport_order.push_back(key);
        while self.resolved_transport_order.len() > RESOLVED_REQUEST_HISTORY {
            if let Some(expired) = self.resolved_transport_order.pop_front() {
                self.resolved_transport_ids.remove(&expired);
            }
        }
    }

    async fn process_notification(&mut self, method: &str, params: Value) -> Result<()> {
        match method {
            "turn/started" => {
                let notification: NativeTurnStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex turn/started did not match the 0.161 schema",
                        )
                    })?;
                validate_thread_and_turn(
                    &self.status,
                    &notification.thread_id,
                    &notification.turn.id,
                )?;
                if self.status.active_turn_id.as_deref() != Some(notification.turn.id.as_str()) {
                    return self
                        .record_gap("Codex turn/started did not match the exact submitted turn")
                        .await;
                }
            }
            "item/started" => {
                let notification: NativeItemStarted =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex item/started did not match the 0.161 schema",
                        )
                    })?;
                validate_thread_and_turn(
                    &self.status,
                    &notification.thread_id,
                    &notification.turn_id,
                )?;
                if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
                    return self
                        .record_gap("Codex item/started was outside the exact active turn")
                        .await;
                }
                self.observe_spawn(&notification.item, false).await?;
                let item_type = notification
                    .item
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if is_tool_type(item_type) {
                    let Some((item_id, item_type, _)) = tool_item(&notification.item) else {
                        return self
                            .record_gap("Codex tool start omitted its exact item id")
                            .await;
                    };
                    let command_id = self.status.active_command_id.clone().ok_or_else(|| {
                        Error::new("unknown", "Codex tool start has no exact Eliot command")
                    })?;
                    let native_id = event_id(
                        self.status.thread_id.as_deref().unwrap_or_default(),
                        &notification.turn_id,
                        &item_id,
                        "tool.started",
                    )?;
                    self.commit_event(
                        "tool.started",
                        native_id,
                        json!({
                            "command_id": command_id,
                            "turn_id": notification.turn_id,
                            "item_id": item_id,
                            "tool": item_type,
                        }),
                    )
                    .await?;
                }
            }
            "item/completed" => {
                let notification: NativeItemCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex item/completed did not match the 0.161 schema",
                        )
                    })?;
                validate_thread_and_turn(
                    &self.status,
                    &notification.thread_id,
                    &notification.turn_id,
                )?;
                if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
                    return self
                        .record_gap("Codex item/completed was outside the exact active turn")
                        .await;
                }
                let raw_item = params.get("item").cloned().unwrap_or(Value::Null);
                self.observe_spawn(&raw_item, true).await?;
                let item_type = raw_item
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if item_type == "agentMessage" {
                    self.complete_message(&notification.turn_id, &raw_item)
                        .await?;
                } else if is_tool_type(item_type) {
                    self.complete_tool(&notification.turn_id, &raw_item).await?;
                }
            }
            "turn/completed" => {
                self.complete_turn(params).await?;
            }
            "serverRequest/resolved" => {
                self.resolve_server_request(params).await?;
            }
            "thread/started" => {
                self.observe_thread_started(&params).await?;
            }
            "error" => {
                let thread_id = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let turn_id = params
                    .get("turnId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !thread_id.is_empty()
                    && !turn_id.is_empty()
                    && self.status.active_turn_id.as_deref() == Some(turn_id)
                {
                    validate_thread_and_turn(&self.status, thread_id, turn_id)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn complete_message(&mut self, turn_id: &str, item: &Value) -> Result<()> {
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::new("native_protocol", "Codex agentMessage omitted its item id")
            })?;
        let text = item
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("native_protocol", "Codex agentMessage omitted its text"))?;
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(text);
        let mut data = json!({
            "command_id": self.status.active_command_id.as_deref().unwrap_or_default(),
            "turn_id": turn_id,
            "text": text,
        });
        let phase = item.get("phase").and_then(Value::as_str);
        if let Some(phase) = phase {
            data["phase"] = Value::String(phase.to_owned());
        }
        let encoded_len = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes.len(),
            Err(_) => usize::MAX,
        };
        if encoded_len > MAX_EVENT_BYTES {
            return self
                .record_gap("Codex completed message exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            turn_id,
            item_id,
            "message.completed",
        )?;
        if matches!(phase, Some("final_answer") | None) {
            self.final_text = Some(text);
        }
        self.commit_event("message.completed", native_id, data)
            .await
    }

    async fn complete_tool(&mut self, turn_id: &str, item: &Value) -> Result<()> {
        let Some((item_id, item_type, native_status)) = tool_item(item) else {
            return self
                .record_gap("Codex tool completion omitted its exact item id")
                .await;
        };
        let command_id =
            self.status.active_command_id.clone().ok_or_else(|| {
                Error::new("unknown", "Codex tool completion has no Eliot command")
            })?;
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            turn_id,
            &item_id,
            "tool.completed",
        )?;
        let completed_file_change =
            item_type == "fileChange" && native_status.as_deref() == Some("completed");
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": item_id,
            "tool": item_type,
        });
        if let Some(native_status) = native_status {
            data["status"] = Value::String(native_status);
        }
        if completed_file_change {
            // Installed 0.161 ThreadItem/FileUpdateChange: only native path fields,
            // including update.move_path, are edit hints; diffs are never persisted.
            let file_change: NativeFileChangeItem =
                serde_json::from_value(item.clone()).map_err(|_| {
                    Error::new(
                        "native_protocol",
                        "Codex completed fileChange did not match the 0.161 schema",
                    )
                })?;
            data["changed_paths"] = json!([]);
            data["changed_paths_truncated"] = Value::Bool(false);
            // Reserve the complete envelope and escaped path bytes. `true` is
            // shorter than `false`, so marking truncation cannot exceed this budget.
            let mut metadata_bytes = serde_json::to_vec(&data)
                .map_err(|_| {
                    Error::new("internal", "Codex fileChange metadata could not be encoded")
                })?
                .len();
            let mut changed_paths = Vec::new();
            let mut truncated = false;
            'paths: for change in file_change.changes {
                let move_path = match change.kind {
                    NativePatchChangeKind::Update { move_path } => move_path,
                    _ => None,
                };
                for path in std::iter::once(change.path).chain(move_path) {
                    let path_bytes = serde_json::to_vec(&path)
                        .map_err(|_| {
                            Error::new("internal", "Codex fileChange path could not be encoded")
                        })?
                        .len()
                        + usize::from(!changed_paths.is_empty());
                    if path.is_empty()
                        || changed_paths.len() >= MAX_CHANGED_PATHS
                        || metadata_bytes.saturating_add(path_bytes) > MAX_EVENT_BYTES
                    {
                        truncated = true;
                        break 'paths;
                    }
                    metadata_bytes += path_bytes;
                    changed_paths.push(path);
                }
            }
            data["changed_paths"] = json!(changed_paths);
            data["changed_paths_truncated"] = Value::Bool(truncated);
        }
        self.commit_event("tool.completed", native_id, data).await
    }

    async fn complete_turn(&mut self, params: Value) -> Result<()> {
        let notification: NativeTurnCompletedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Codex turn/completed did not match the 0.161 schema",
                )
            })?;
        validate_thread_and_turn(&self.status, &notification.thread_id, &notification.turn.id)?;
        if self.status.active_turn_id.as_deref() != Some(notification.turn.id.as_str()) {
            return self
                .record_gap("Codex terminal notification did not match the exact active turn")
                .await;
        }
        self.settle_spawn_window();
        if notification.turn.status == NativeTurnStatus::InProgress {
            return self
                .record_gap("Codex turn/completed carried a nonterminal inProgress status")
                .await;
        }
        let status_text = turn_status(&notification.turn.status);
        let command_id = self
            .status
            .active_command_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Codex terminal turn has no Eliot command"))?;
        let provider_restriction = terminal_provider_restriction(&self.status, &notification);
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": notification.turn.id,
            "status": status_text,
            "outcome": turn_outcome(&notification.turn.status),
            "native_status": turn_status(&notification.turn.status),
        });
        // The semantic hold signal gets first claim on the bounded terminal data.
        if let Some(restriction) = provider_restriction {
            attach_provider_restriction(&mut data, restriction, &secrets);
        }
        if let Some(text) = self.final_text.as_ref() {
            attach_final_text(&mut data, text, &notification.turn.status, &secrets);
        }
        attach_native_error(&mut data, notification.turn.error.as_ref(), &secrets);
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            &notification.turn.id,
            &notification.turn.id,
            "turn.completed",
        )?;
        self.commit_event("turn.completed", native_id, data).await?;
        let closed_requests = self
            .transport_requests
            .iter()
            .filter(|(_, request_id)| {
                self.pending_requests
                    .get(request_id.as_str())
                    .is_some_and(|entry| entry.request.turn_id == notification.turn.id)
            })
            .map(|(key, request_id)| (key.clone(), request_id.clone()))
            .collect::<Vec<_>>();
        for (key, request_id) in closed_requests {
            self.pending_requests.remove(&request_id);
            self.transport_requests.remove(&key);
            self.remember_resolved_transport(key);
        }
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: notification.turn.id,
            status: Some(status_text),
            final_text: self.final_text.take(),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        Ok(())
    }

    async fn observe_thread_started(&mut self, params: &Value) -> Result<()> {
        let thread_id = params
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Codex thread/started omitted its exact thread id",
                )
            })?;
        if let Some(owned) = self.status.thread_id.clone()
            && owned != thread_id
        {
            let parent = params.get("thread").and_then(spawned_parent_thread);
            if parent.is_some_and(|parent| self.children.is_parent(&owned, parent)) {
                let parent = parent.unwrap().to_owned();
                let spawn_item = params
                    .get("thread")
                    .and_then(spawned_item_id)
                    .map(str::to_owned)
                    .or_else(|| {
                        let producers = self.children.producers();
                        if producers.len() == 1 {
                            self.children
                                .pending_items(producers[0].as_str())
                                .into_iter()
                                .next()
                                .cloned()
                        } else {
                            None
                        }
                    });
                if let Err(reason) = self.children.confirm(&owned, thread_id) {
                    return self.record_gap(reason).await;
                }
                self.children.parents.insert(thread_id.to_owned(), parent);
                if let Some(spawn_item) = spawn_item {
                    self.children
                        .spawn_items
                        .insert(thread_id.to_owned(), spawn_item);
                }
                return self.ensure_child_registered(thread_id).await;
            }
            return self
                .record_gap("Codex app-server announced a second thread in one Agent generation")
                .await;
        }
        self.status.thread_id = Some(thread_id.to_owned());
        Ok(())
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Codex source acknowledgement is unresolved",
            ));
        }
        let native_id = format!(
            "codex:{}:session.ended",
            self.status.thread_id.as_deref().unwrap_or("unresolved")
        );
        self.commit_event("session.ended", native_id, json!({}))
            .await?;
        if let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            self.status.last_turn = Some(TurnFacts {
                command_id,
                turn_id,
                status: None,
                final_text: None,
                terminal: false,
            });
        }
        self.status.stopped = true;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
        Ok(())
    }

    async fn finish_pending_end_stop(&mut self) {
        if !self.frames_drained {
            return;
        }
        if let Some(reply) = self.pending_end_stop.take() {
            let result = self.finish_stop().await;
            let _ = reply.send(result);
        }
    }

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.stream_source().record_gap(reason).await
    }

    fn has_pending_child_gap(&self) -> bool {
        self.child_states
            .values()
            .any(|state| state.pending_gap.is_some())
    }

    /// Fair rotation over retained child gaps: the next pending child after
    /// the last served (sorted thread order, wrap-around), so one bad child
    /// cannot starve the others. Synchronous; the caller owns the send.
    fn next_pending_gap(&mut self) -> Option<(String, SourceGap)> {
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
    fn apply_gap_attempt(
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
                    state.failure = Some(error.message);
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

    async fn handle_child_notification(&mut self, method: &str, params: Value) {
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
                    state.failure = Some(error.message);
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

    async fn ensure_child_registered(&mut self, child_thread: &str) -> Result<()> {
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
                                state.failure = Some(error.message);
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
            }
            "item/completed" => {
                let raw_item = params.get("item").cloned().unwrap_or(Value::Null);
                self.observe_child_spawn(child_thread, &raw_item, true)
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
    fn stream_source(&mut self) -> StreamSource<'_> {
        StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent_id,
            generation: self.generation,
            harness: "Codex",
            id_prefix: "codex",
            families: &["session", "turn", "message", "tool", "request"],
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.status.source_unknown,
            batch_unknown: &mut self.status.batch_unknown,
            failure: &mut self.status.failure,
        }
    }
}

/// One owned child-gap attempt: cloned identity plus the RPC future.
/// Polled in `run()` without borrowing `ChildState`; the result applies
/// only to the matching child/gen/retained gap.
struct GapAttempt {
    child_thread: String,
    gap: SourceGap,
    future: Pin<Box<dyn Future<Output = Result<Value>> + Send>>,
}

async fn run(
    mut frames: mpsc::Receiver<NativeFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut controls: mpsc::Receiver<Control>,
    status_tx: watch::Sender<EventStatus>,
) {
    let mut gap_retry = interval(CHILD_GAP_RETRY_INTERVAL);
    gap_retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut gap_in_flight: Option<GapAttempt> = None;
    loop {
        let accept_frames = processor.accept_frames;
        let can_start_gap = gap_in_flight.is_none() && processor.has_pending_child_gap();
        let gap_pending = gap_in_flight.is_some();
        tokio::select! {
            changed = stream.changed(), if accept_frames => {
                let current = match changed {
                    Ok(()) => *stream.borrow_and_update(),
                    Err(_) => StreamState::Closed,
                };
                processor.handle_stream_state(current).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(control).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            frame = frames.recv(), if accept_frames => {
                if frame.is_none() {
                    let current = *stream.borrow_and_update();
                    processor.handle_stream_state(current).await;
                }
                processor.handle_frame(frame).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            _ = gap_retry.tick(), if can_start_gap => {
                // Start only; never await the RPC here. One attempt at a
                // time, fair rotation via `next_pending_gap`.
                if let Some((child_thread, gap)) = processor.next_pending_gap() {
                    let host = processor.host.clone();
                    let credential = processor.credential.clone();
                    let params = StreamSource::gap_request_params(&credential, &gap);
                    let deadline = Instant::now() + EVENT_DEADLINE;
                    let future =
                        Box::pin(async move { host.call("source.gap", params, deadline).await });
                    gap_in_flight = Some(GapAttempt {
                        child_thread,
                        gap,
                        future,
                    });
                }
            }
            result = async {
                match gap_in_flight.as_mut() {
                    Some(attempt) => attempt.future.as_mut().await,
                    None => std::future::pending().await,
                }
            }, if gap_pending => {
                let attempt = gap_in_flight.take().expect("guarded by gap_pending");
                processor.apply_gap_attempt(&attempt.child_thread, &attempt.gap, &result);
            }
        }
        processor.status.native_drained = processor.frames_drained;
        let _ = status_tx.send(processor.status.clone());
    }
}

/// Shared bounded native-error attachment, projecting the Codex turn
/// error to its message. Kept under the local path for the terminal-data
/// tests.
fn attach_native_error(data: &mut Value, error: Option<&NativeTurnError>, policy: &SecretPolicy) {
    shared_attach_native_error(data, error.map(|error| error.message.as_str()), policy);
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

fn spawned_item_id(thread: &Value) -> Option<&str> {
    let spawn = thread.get("source")?.get("subAgent")?.get("thread_spawn")?;
    spawn
        .get("call_id")
        .or_else(|| spawn.get("item_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
}

/// The parent of a subagent thread, from the installed 0.161 `Thread.source`
/// shape `{"subAgent": {"thread_spawn": {"parent_thread_id": ...}}}`.
fn spawned_parent_thread(thread: &Value) -> Option<&str> {
    thread
        .get("source")?
        .get("subAgent")?
        .get("thread_spawn")?
        .get("parent_thread_id")?
        .as_str()
        .filter(|parent| !parent.trim().is_empty())
}

fn validate_thread_and_turn(status: &EventStatus, thread_id: &str, turn_id: &str) -> Result<()> {
    if status.thread_id.as_deref() != Some(thread_id) || turn_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Codex notification did not belong to the owned native thread",
        ));
    }
    Ok(())
}

/// Shared native event identity with the Codex prefix, kept under the
/// local path for this processor's call sites.
fn event_id(thread_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    shared_event_id(thread_id, turn_id, item_id, kind, "codex", "Codex")
}

fn request_event_id(request_id: &str, kind: &str) -> Result<String> {
    let prefix = match kind {
        "request.opened" => "codex-o:",
        "request.resolved" => "codex-r:",
        "request.closed" => "codex-c:",
        _ => {
            return Err(Error::new(
                "invalid",
                "Codex request event kind is unsupported",
            ));
        }
    };
    let identity = request_id
        .strip_prefix("codex-request:")
        .filter(|identity| !identity.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "source_integrity",
                "Codex native request id omitted its normalized prefix",
            )
        })?;
    let native_id = format!("{prefix}{identity}");
    if native_id.len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::new(
            "source_integrity",
            "Codex native request event identity exceeded its bound",
        ));
    }
    Ok(native_id)
}

fn transport_key(id: &Value) -> Result<String> {
    serde_json::to_string(id)
        .map_err(|_| Error::new("native_protocol", "Codex transport request id is invalid"))
}

fn tool_item(item: &Value) -> Option<(String, String, Option<String>)> {
    let item_type = item.get("type")?.as_str()?;
    if !is_tool_type(item_type) {
        return None;
    }
    let item_id = item
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())?
        .to_owned();
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some((item_id, item_type.to_owned(), status))
}

fn is_tool_type(item_type: &str) -> bool {
    matches!(
        item_type,
        "commandExecution"
            | "fileChange"
            | "functionCallOutput"
            | "mcpToolCall"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "webSearch"
            | "imageGeneration"
    )
}

fn turn_status(status: &NativeTurnStatus) -> String {
    match status {
        NativeTurnStatus::Completed => "completed".to_owned(),
        NativeTurnStatus::Interrupted => "interrupted".to_owned(),
        NativeTurnStatus::Failed => "failed".to_owned(),
        NativeTurnStatus::InProgress => "inProgress".to_owned(),
    }
}

/// Canonical outcome for the Codex terminal vocabulary
/// (`completed` | `failed` | `interrupted`): decided here at the plugin
/// boundary, with the raw wire status preserved as `native_status`.
fn turn_outcome(status: &NativeTurnStatus) -> &'static str {
    match status {
        NativeTurnStatus::Completed => "success",
        NativeTurnStatus::Failed => "failure",
        NativeTurnStatus::Interrupted => "interrupted",
        // Unreachable: a nonterminal `inProgress` status gaps before settling.
        NativeTurnStatus::InProgress => "unknown",
    }
}

fn terminal_provider_restriction(
    status: &EventStatus,
    notification: &NativeTurnCompletedNotification,
) -> Option<ProviderRestriction> {
    if notification.turn.status != NativeTurnStatus::Failed
        || status.thread_id.as_deref() != Some(notification.thread_id.as_str())
        || status.active_turn_id.as_deref() != Some(notification.turn.id.as_str())
        || status.active_command_id.is_none()
    {
        return None;
    }
    notification
        .turn
        .error
        .as_ref()
        .and_then(NativeTurnError::provider_restriction)
}

fn attach_provider_restriction(
    data: &mut Value,
    restriction: ProviderRestriction,
    policy: &SecretPolicy,
) -> bool {
    let Ok(value) = serde_json::to_value(restriction) else {
        return false;
    };
    // The key and actual value define immutable semantics and may not be redacted.
    if policy.contains_text("provider_restriction")
        || policy.contains_value(&value)
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("provider_restriction"))
    {
        return false;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return false;
    };
    fields.insert("provider_restriction".to_owned(), value.clone());
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return false;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert("provider_restriction".to_owned(), value);
        return true;
    }
    false
}

/// Shared bounded terminal-text attachment, projecting the Codex turn
/// status to its failed bit. Kept under the local path for the
/// terminal-data tests.
fn attach_final_text(
    data: &mut Value,
    final_text: &str,
    status: &NativeTurnStatus,
    policy: &SecretPolicy,
) {
    shared_attach_final_text(
        data,
        final_text,
        *status == NativeTurnStatus::Failed,
        policy,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::ProviderRestrictionKind;

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
        // No spawn in flight: an unknown thread is not taken (caller gaps it).
        assert_eq!(children.take_frame("x"), Ok(false));

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
        assert_eq!(children.take_frame("child-2"), Ok(false));
        assert!(children.is_parent("owned", "child-1"));
        assert!(!children.is_parent("owned", "child-2"));
    }

    #[test]
    fn child_threads_settle_a_stuck_spawn_and_refuse_the_owned_thread() {
        let mut children = ChildThreads::default();
        children.spawn_started("owned", "call".to_owned()).unwrap();
        assert_eq!(children.settle("owned"), Ok(()));
        assert_eq!(children.take_frame("late"), Ok(false));

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

    fn native_error(error_info: Option<Value>, message: &str) -> NativeTurnError {
        NativeTurnError {
            message: message.to_owned(),
            additional_details: Some("usageLimitExceeded 401 HTTP 429".to_owned()),
            codex_error_info: error_info,
        }
    }

    fn rate_limit() -> ProviderRestriction {
        ProviderRestriction {
            kind: ProviderRestrictionKind::RateLimit,
            retry_after: None,
        }
    }

    #[test]
    fn provider_restriction_uses_only_installed_codex_error_info() {
        assert_eq!(
            native_error(Some(json!("rateLimitExceeded")), "ignored").provider_restriction(),
            Some(rate_limit())
        );
        assert_eq!(
            native_error(Some(json!("usageLimitExceeded")), "ignored").provider_restriction(),
            Some(ProviderRestriction {
                kind: ProviderRestrictionKind::Quota,
                retry_after: None,
            })
        );
        assert_eq!(
            native_error(Some(json!("unauthorized")), "ignored").provider_restriction(),
            Some(ProviderRestriction {
                kind: ProviderRestrictionKind::Auth,
                retry_after: None,
            })
        );
        for variant in [
            "httpConnectionFailed",
            "responseStreamConnectionFailed",
            "responseStreamDisconnected",
            "responseTooManyFailedAttempts",
        ] {
            assert_eq!(
                native_error(Some(json!({variant: {"httpStatusCode":429}})), "ignored")
                    .provider_restriction(),
                Some(rate_limit())
            );
        }

        for error_info in [
            None,
            Some(json!("other")),
            Some(json!("rate_limit")),
            Some(json!("modelRemoved")),
            Some(json!({"httpConnectionFailed":{"httpStatusCode":401}})),
            Some(json!({"httpConnectionFailed":{"httpStatusCode":404}})),
            Some(json!({"httpConnectionFailed":{"httpStatusCode":"429"}})),
            Some(json!({"unknownVariant":{"httpStatusCode":429}})),
            Some(json!({
                "httpConnectionFailed":{"httpStatusCode":429},
                "unauthorized":null
            })),
        ] {
            assert_eq!(
                native_error(error_info, "HTTP 401 rateLimitExceeded usageLimitExceeded")
                    .provider_restriction(),
                None
            );
        }
    }

    #[test]
    fn provider_restriction_requires_the_exact_bound_failed_turn() {
        let notification: NativeTurnCompletedNotification = serde_json::from_value(json!({
            "threadId":"thread-1",
            "turn":{
                "id":"turn-1",
                "items":[],
                "status":"failed",
                "error":{"message":"ignored","codexErrorInfo":"rateLimitExceeded"}
            }
        }))
        .unwrap();
        let mut status = EventStatus {
            thread_id: Some("thread-1".to_owned()),
            active_turn_id: Some("turn-1".to_owned()),
            active_command_id: Some("send-1".to_owned()),
            ..EventStatus::default()
        };
        assert_eq!(
            terminal_provider_restriction(&status, &notification),
            Some(rate_limit())
        );

        status.active_turn_id = Some("other-turn".to_owned());
        assert_eq!(terminal_provider_restriction(&status, &notification), None);
        status.active_turn_id = Some("turn-1".to_owned());
        status.active_command_id = None;
        assert_eq!(terminal_provider_restriction(&status, &notification), None);
        status.active_command_id = Some("send-1".to_owned());
        let mut successful = notification;
        successful.turn.status = NativeTurnStatus::Completed;
        assert_eq!(terminal_provider_restriction(&status, &successful), None);
    }

    #[test]
    fn provider_restriction_omits_secret_collisions_without_mutating_terminal_data() {
        let restriction = rate_limit();
        let mut data = json!({"command_id":"send-1", "status":"failed"});
        assert!(attach_provider_restriction(
            &mut data,
            restriction.clone(),
            &SecretPolicy::default()
        ));
        assert_eq!(
            data["provider_restriction"],
            json!({"kind":"rate_limit", "retry_after":null})
        );

        for secret in ["provider_restriction", "kind", "retry_after", "rate_limit"] {
            let original = json!({"command_id":"send-1", "status":"failed"});
            let mut data = original.clone();
            assert!(!attach_provider_restriction(
                &mut data,
                restriction.clone(),
                &SecretPolicy::new([secret]),
            ));
            assert_eq!(data, original);
        }
    }

    #[test]
    fn failed_terminal_reserves_restriction_and_marks_unavailable_final_text() {
        let mut data = json!({
            "command_id":"send-1",
            "turn_id":"turn-1",
            "status":"failed"
        });
        assert!(attach_provider_restriction(
            &mut data,
            rate_limit(),
            &SecretPolicy::default()
        ));
        attach_final_text(
            &mut data,
            &"x".repeat(MAX_EVENT_BYTES),
            &NativeTurnStatus::Failed,
            &SecretPolicy::default(),
        );

        assert_eq!(data["command_id"], "send-1");
        assert_eq!(data["turn_id"], "turn-1");
        assert_eq!(data["status"], "failed");
        assert_eq!(data["provider_restriction"]["kind"], "rate_limit");
        assert_eq!(data["final_text_unavailable"], "event_budget");
        assert!(data.get("final_text").is_none());
        assert!(serde_json::to_vec(&data).unwrap().len() <= MAX_EVENT_BYTES);
    }

    #[test]
    fn failed_terminal_without_restriction_marks_unavailable_final_text_at_event_budget() {
        let mut data = json!({
            "command_id":"send-2",
            "turn_id":"turn-2",
            "status":"failed"
        });
        attach_final_text(
            &mut data,
            &"x".repeat(MAX_EVENT_BYTES),
            &NativeTurnStatus::Failed,
            &SecretPolicy::default(),
        );

        assert_eq!(data["command_id"], "send-2");
        assert_eq!(data["turn_id"], "turn-2");
        assert_eq!(data["status"], "failed");
        assert!(data.get("provider_restriction").is_none());
        assert_eq!(data["final_text_unavailable"], "event_budget");
        assert!(data.get("final_text").is_none());
        assert!(serde_json::to_vec(&data).unwrap().len() <= MAX_EVENT_BYTES);
    }

    #[test]
    fn native_error_omits_secret_keys_without_changing_terminal_data() {
        let error = NativeTurnError {
            message: "native failure".to_owned(),
            additional_details: None,
            codex_error_info: None,
        };
        let original = json!({
            "command_id":"send-1", "turn_id":"turn-1", "status":"failed", "final_text":"existing final",
        });
        for secret in [
            "native_error",
            "message",
            "truncated",
            "_error",
            "mess",
            "trunc",
        ] {
            let policy = SecretPolicy::new([secret]);
            assert!(!policy.contains_value(&original));
            let mut data = original.clone();
            attach_native_error(&mut data, Some(&error), &policy);
            assert_eq!(data, original);
        }
    }

    #[test]
    fn native_error_preserves_failed_turn_reason_without_changing_terminal_data() {
        let error: NativeTurnError = serde_json::from_value(json!({
            "message": "unexpected status 401 Unauthorized: Invalid Authorization header or token",
            "additionalDetails": "private extra data",
            "codexErrorInfo": {"httpConnectionFailed": {"httpStatusCode": 401}},
        }))
        .unwrap();
        let mut data = json!({"command_id":"send-1", "turn_id":"turn-1", "status":"failed"});
        let original = data.clone();
        attach_native_error(&mut data, Some(&error), &SecretPolicy::default());
        assert_eq!(
            data["native_error"],
            json!({"message":error.message, "truncated":false})
        );
        data.as_object_mut().unwrap().remove("native_error");
        assert_eq!(data, original);
        assert!(data.get("final_text").is_none());
        attach_native_error(&mut data, None, &SecretPolicy::default());
        assert_eq!(data, original);
    }

    #[test]
    fn native_error_bounds_encoded_redaction_expansion_and_control_bytes() {
        for (message, secrets) in [
            ("x".repeat(600), vec!["x"]),
            ("\0".repeat(800), vec![]),
            ("密钥🙂x\n".repeat(200), vec!["密钥", "x"]),
            ("R🙂x\0".repeat(250), vec!["R", "x"]),
        ] {
            assert!(message.len() < 4 * 1024);
            let policy = SecretPolicy::new(secrets);
            let error = NativeTurnError {
                message,
                additional_details: None,
                codex_error_info: None,
            };
            let mut data = json!({"command_id":"send-1", "turn_id":"turn-1", "status":"failed"});
            attach_native_error(&mut data, Some(&error), &policy);
            let diagnostic = &data["native_error"];
            assert_eq!(diagnostic["truncated"], true);
            assert!(serde_json::to_vec(diagnostic).unwrap().len() <= 4 * 1024);
            assert!(serde_json::to_vec(&data).unwrap().len() <= MAX_EVENT_BYTES);
            let message = diagnostic["message"].as_str().unwrap();
            assert_eq!(policy.redact_text(message), message);
            assert!(policy.redact_text(&error.message).starts_with(message));
        }
    }

    #[test]
    fn native_error_respects_remaining_event_budget_or_leaves_terminal_data_intact() {
        let error = NativeTurnError {
            message: "diagnostic reason".repeat(100),
            additional_details: None,
            codex_error_info: None,
        };
        for remaining in [0, 1, 16, 48, 128, 4096] {
            let mut data = json!({
                "command_id":"send-1", "turn_id":"turn-1", "status":"failed", "final_text":"",
            });
            let base = serde_json::to_vec(&data).unwrap().len();
            data["final_text"] = json!("t".repeat(MAX_EVENT_BYTES - base - remaining));
            let original = data.clone();
            attach_native_error(&mut data, Some(&error), &SecretPolicy::default());
            assert!(serde_json::to_vec(&data).unwrap().len() <= MAX_EVENT_BYTES);
            if remaining <= 16 {
                assert_eq!(data, original);
            } else {
                let diagnostic = data
                    .as_object_mut()
                    .unwrap()
                    .remove("native_error")
                    .unwrap();
                assert!(serde_json::to_vec(&diagnostic).unwrap().len() <= remaining);
                assert_eq!(data, original);
            }
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
            assert!(!processor.children.has_any_window());

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
}
