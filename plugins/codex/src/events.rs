use crate::native_child::{
    ChildBinding, binding_from_ack, build_registration, is_admission_rejection, register_child,
};
use eliot_core::{Error, ProviderRestriction, Result, SecretPolicy};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    protocol::MAX_COMMAND_ID_BYTES,
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text as shared_attach_final_text,
        attach_native_error as shared_attach_native_error, event_id as shared_event_id,
        native_no_write, rejected_control, unknown_control,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
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

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
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
    loss_reason: Option<&'static str>,
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
    children: ChildThreads,
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
}

/// Native subagent threads of the owned thread. Their frames are not this
/// generation's lifecycle. A child's first frames can precede both its
/// `thread/started` and the spawn result, so frames of an unannounced thread
/// are taken only while the owned thread has a `spawnAgent` call in flight;
/// such a thread must be confirmed before that window closes.
#[derive(Default)]
struct ChildThreads {
    confirmed: HashSet<String>,
    pending_spawns: HashSet<String>,
    unconfirmed: HashSet<String>,
    bound: HashSet<String>,
    parents: HashMap<String, String>,
    spawn_items: HashMap<String, String>,
}

impl ChildThreads {
    fn take_frame(&mut self, thread: &str) -> std::result::Result<bool, &'static str> {
        if self.confirmed.contains(thread) || self.bound.contains(thread) {
            return Ok(true);
        }
        if self.pending_spawns.is_empty() {
            return Ok(false);
        }
        if self.unconfirmed.len() >= MAX_CHILD_THREADS && !self.unconfirmed.contains(thread) {
            return Err("Codex sent frames from more unannounced threads than are tracked");
        }
        self.unconfirmed.insert(thread.to_owned());
        Ok(true)
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

    fn spawn_started(&mut self, item_id: String) -> std::result::Result<(), &'static str> {
        if self.pending_spawns.len() >= MAX_CHILD_THREADS && !self.pending_spawns.contains(&item_id)
        {
            return Err("Codex had more native spawns in flight than are tracked");
        }
        self.pending_spawns.insert(item_id);
        Ok(())
    }

    fn spawn_completed(
        &mut self,
        owned: &str,
        item_id: &str,
        receivers: &[String],
    ) -> std::result::Result<(), &'static str> {
        self.pending_spawns.remove(item_id);
        for receiver in receivers {
            self.spawn_items
                .insert(receiver.clone(), item_id.to_owned());
            self.confirm(owned, receiver)?;
        }
        if self.pending_spawns.is_empty() {
            return self.settle();
        }
        Ok(())
    }

    /// No spawn can confirm a thread any more (last spawn done, turn ended or
    /// stream closed): an unconfirmed thread was never a child.
    fn settle(&mut self) -> std::result::Result<(), &'static str> {
        self.pending_spawns.clear();
        if self.unconfirmed.is_empty() {
            return Ok(());
        }
        self.unconfirmed.clear();
        Err("Codex sent frames from a thread no spawn announced")
    }

    fn is_bound(&self, thread: &str) -> bool {
        self.bound.contains(thread)
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

    pub async fn associate_turn(&self, command_id: String, turn_id: String) -> Result<()> {
        self.send(|reply| Control::AssociateTurn {
            command_id,
            turn_id,
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
            children: ChildThreads::default(),
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
                reply,
            } => {
                let result = self.associate_turn(command_id, turn_id).await;
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
                }
            }
            StreamState::Overflow => self.defer_gap("Codex app-server event queue overflowed"),
            StreamState::Malformed => {
                self.defer_gap("Codex app-server stream contained an invalid frame")
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
                if let Err(reason) = self.children.settle() {
                    self.defer_gap(reason);
                }
                self.report_gap_if_queue_empty(true).await;
                self.finish_pending_end_stop().await;
            }
        }
    }

    fn defer_gap(&mut self, reason: &'static str) {
        if self.loss_reason.is_none() {
            self.loss_reason = Some(reason);
        }
    }

    async fn report_gap_if_queue_empty(&mut self, queue_empty: bool) {
        if queue_empty
            && !self.gap_reported
            && !self.writes_blocked
            && let Some(reason) = self.loss_reason
        {
            let _ = self.record_gap(reason).await;
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
        let outcome = if completed {
            for receiver in &receivers {
                self.children
                    .parents
                    .insert(receiver.clone(), owned.clone());
                self.children
                    .spawn_items
                    .insert(receiver.clone(), item_id.clone());
            }
            self.children.spawn_completed(&owned, &item_id, &receivers)
        } else {
            self.children.spawn_started(item_id)
        };
        match outcome {
            Ok(()) => {
                if completed {
                    for receiver in receivers {
                        let _ = self.ensure_child_registered(&receiver).await;
                    }
                }
                Ok(())
            }
            Err(reason) => self.record_gap(reason).await,
        }
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

    async fn associate_turn(&mut self, command_id: String, turn_id: String) -> Result<()> {
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
        self.commit_event(
            "turn.started",
            native_id,
            json!({"command_id": command_id, "turn_id": turn_id}),
        )
        .await?;
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
        let public_details = request.details.clone();
        let data = json!({
            "command_id": command_id,
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": request.kind,
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
        if let Err(reason) = self.children.settle() {
            self.defer_gap(reason);
        }
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
                let parent = parent.unwrap();
                self.children
                    .parents
                    .insert(thread_id.to_owned(), parent.to_owned());
                if let Some(spawn_item) = params.get("thread").and_then(spawned_item_id) {
                    self.children
                        .spawn_items
                        .insert(thread_id.to_owned(), spawn_item.to_owned());
                } else if self.children.pending_spawns.len() == 1 {
                    if let Some(spawn_item) = self.children.pending_spawns.iter().next().cloned() {
                        self.children
                            .spawn_items
                            .insert(thread_id.to_owned(), spawn_item);
                    }
                }
                if let Err(reason) = self.children.confirm(&owned, thread_id) {
                    return self.record_gap(reason).await;
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

    async fn handle_child_notification(&mut self, method: &str, params: Value) {
        let Some(child_thread) = frame_thread_id(&params).map(str::to_owned) else {
            return;
        };
        if self.child_unknown.contains(&child_thread) {
            return;
        }
        if method == "thread/started" {
            let _ = self.observe_thread_started(&params).await;
            return;
        }
        let is_ready = self
            .child_states
            .get(&child_thread)
            .is_some_and(|state| state.ready_committed);
        if !is_ready {
            self.buffer_child_frame(
                &child_thread,
                BufferedNativeFrame::Notification {
                    method: method.to_owned(),
                    params,
                },
            );
            return;
        }
        if let Err(error) = self
            .process_child_notification(&child_thread, method, params)
            .await
        {
            if let Some(state) = self.child_states.get_mut(&child_thread) {
                state.source_unknown = true;
                if state.failure.is_none() {
                    state.failure = Some(error.message);
                }
                if !state.gap_reported && !state.writes_blocked {
                    let reason = format!(
                        "Codex child {method} notification could not be normalized ({})",
                        error.code
                    );
                    let _ = state
                        .record_gap(&mut self.sink, &self.host, &self.credential, &reason)
                        .await;
                }
            }
        }
    }

    fn buffer_child_frame(&mut self, child_thread: &str, frame: BufferedNativeFrame) {
        let size = match &frame {
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
        };
        let queue = self
            .child_buffers
            .entry(child_thread.to_owned())
            .or_default();
        if queue.len() >= EARLY_EVENT_LIMIT
            || self.child_buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            queue.clear();
            self.child_unknown.insert(child_thread.to_owned());
            return;
        }
        self.child_buffer_bytes = self.child_buffer_bytes.saturating_add(size);
        queue.push_back(frame);
    }

    async fn ensure_child_registered(&mut self, child_thread: &str) -> Result<()> {
        if self.child_states.contains_key(child_thread) {
            return Ok(());
        }
        if self.child_unknown.contains(child_thread) {
            return Ok(());
        }
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
        let registration = build_registration(
            &spawn_item,
            &parent_id,
            parent_generation,
            &parent_native_ref,
            &self.native_scope,
            child_thread,
        );
        let ack = match register_child(&self.host, &self.credential, registration.clone()).await {
            Ok(ack) => ack,
            Err(error) => {
                if is_admission_rejection(&error) {
                    self.child_unknown.insert(child_thread.to_owned());
                    self.child_buffers.remove(child_thread);
                    return Ok(());
                }
                self.child_unknown.insert(child_thread.to_owned());
                return Err(error);
            }
        };
        let binding = binding_from_ack(registration, &ack);
        self.child_bindings
            .insert(child_thread.to_owned(), binding.clone());
        let mut child_state = ChildState::new(binding);
        let native_id = format!("codex:{child_thread}:session.ready");
        child_state
            .commit_event(
                &mut self.sink,
                &self.host,
                &self.credential,
                "session.ready",
                native_id,
                json!({
                    "native_scope": self.native_scope,
                    "native_ref": child_thread,
                }),
            )
            .await?;
        child_state.ready_committed = true;
        self.child_states
            .insert(child_thread.to_owned(), child_state);
        self.flush_child_buffer(child_thread).await
    }

    async fn flush_child_buffer(&mut self, child_thread: &str) -> Result<()> {
        let Some(mut queue) = self.child_buffers.remove(child_thread) else {
            return Ok(());
        };
        while let Some(frame) = queue.pop_front() {
            let size = match &frame {
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
            };
            self.child_buffer_bytes = self.child_buffer_bytes.saturating_sub(size);
            match frame {
                BufferedNativeFrame::Notification { method, params } => {
                    self.process_child_notification(child_thread, &method, params)
                        .await?;
                }
                BufferedNativeFrame::Request { id, method, params } => {
                    self.reject_child_request(id, &method, &params).await;
                }
            }
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
        let turn_id = params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| {
                self.child_states
                    .get(child_thread)
                    .and_then(|state| state.active_turn_id.as_deref())
            })
            .unwrap_or_default();
        let mut data = json!({
            "turn_id": turn_id,
            "text": text,
        });
        if let Some(phase) = item.get("phase").and_then(Value::as_str) {
            data["phase"] = Value::String(phase.to_owned());
        }
        let encoded_len = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes.len(),
            Err(_) => usize::MAX,
        };
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        if encoded_len > MAX_EVENT_BYTES {
            return state
                .record_gap(
                    &mut self.sink,
                    &self.host,
                    &self.credential,
                    "Codex child completed message exceeded persisted-event bound",
                )
                .await;
        }
        let native_id = event_id(child_thread, turn_id, item_id, "message.completed")?;
        state
            .commit_event(
                &mut self.sink,
                &self.host,
                &self.credential,
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
        let state = self
            .child_states
            .get_mut(child_thread)
            .ok_or_else(|| Error::new("bad_state", "Codex child state missing"))?;
        if notification.turn.status == NativeTurnStatus::InProgress {
            return state
                .record_gap(
                    &mut self.sink,
                    &self.host,
                    &self.credential,
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
            .commit_event(
                &mut self.sink,
                &self.host,
                &self.credential,
                "turn.completed",
                native_id,
                data,
            )
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
        let outcome = if completed {
            for receiver in &receivers {
                self.children
                    .parents
                    .insert(receiver.clone(), parent_thread.to_owned());
                self.children
                    .spawn_items
                    .insert(receiver.clone(), item_id.clone());
            }
            let owned = self.status.thread_id.as_deref().unwrap_or(parent_thread);
            self.children.spawn_completed(owned, &item_id, &receivers)
        } else {
            self.children.spawn_started(item_id)
        };
        match outcome {
            Ok(()) => {
                if completed {
                    for receiver in receivers {
                        let _ = self.ensure_child_registered(&receiver).await;
                    }
                }
                Ok(())
            }
            Err(reason) => {
                if let Some(state) = self.child_states.get_mut(parent_thread) {
                    state
                        .record_gap(&mut self.sink, &self.host, &self.credential, reason)
                        .await
                } else {
                    self.record_gap(reason).await
                }
            }
        }
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

async fn run(
    mut frames: mpsc::Receiver<NativeFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut controls: mpsc::Receiver<Control>,
    status_tx: watch::Sender<EventStatus>,
) {
    loop {
        let accept_frames = processor.accept_frames;
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
        }
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

        children.spawn_started("call-1".to_owned()).unwrap();
        children.spawn_started("call-2".to_owned()).unwrap();
        assert_eq!(children.take_frame("child-1"), Ok(true));
        assert_eq!(children.take_frame("child-2"), Ok(true));
        // The first spawn confirms its receiver; the second is still pending.
        assert_eq!(
            children.spawn_completed("owned", "call-1", &["child-1".to_owned()]),
            Ok(())
        );
        assert_eq!(children.take_frame("child-1"), Ok(true));
        // child-2 was never named by a spawn: the last completion gaps it.
        assert_eq!(
            children.spawn_completed("owned", "call-2", &[]),
            Err("Codex sent frames from a thread no spawn announced")
        );
        assert_eq!(children.take_frame("child-2"), Ok(false));
        assert!(children.is_parent("owned", "child-1"));
        assert!(!children.is_parent("owned", "child-2"));
    }

    #[test]
    fn child_threads_settle_a_stuck_spawn_and_refuse_the_owned_thread() {
        let mut children = ChildThreads::default();
        children.spawn_started("call".to_owned()).unwrap();
        assert_eq!(children.settle(), Ok(()));
        assert_eq!(children.take_frame("late"), Ok(false));

        children.spawn_started("call".to_owned()).unwrap();
        assert_eq!(children.take_frame("orphan"), Ok(true));
        assert_eq!(
            children.settle(),
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
        children.spawn_started("call".to_owned()).unwrap();
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
            children.spawn_started(format!("call-{index}")).unwrap();
        }
        assert!(children.spawn_started("call-extra".to_owned()).is_err());
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
}
