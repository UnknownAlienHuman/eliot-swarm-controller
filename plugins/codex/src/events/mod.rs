mod children;
use self::children::{
    CHILD_GAP_RETRY_INTERVAL, ChildState, ChildThreads, GapAttempt, spawned_item_id,
    spawned_parent_thread, top_level_parent_thread,
};
use crate::native_child::{
    ChildBinding, binding_from_ack, build_registration, is_admission_rejection, register_child,
};
use eliot_core::{
    ChildRegistration, ContextItem, Error, ProviderRestriction, Result, SecretPolicy,
    SessionTokenTotals, SessionUsage, SourceGap, TurnUsage,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    protocol::MAX_COMMAND_ID_BYTES,
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text as shared_attach_final_text,
        attach_native_error as shared_attach_native_error, attach_provider_restriction,
        event_id as shared_event_id, native_no_write, next_input_outcomes, rejected_control,
        unknown_control,
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
        NativeServerRequestResolvedNotification, NativeThreadTokenUsage,
        NativeThreadTokenUsageUpdatedNotification, NativeTokenUsageDelta,
        NativeTurnCompletedNotification, NativeTurnError, NativeTurnStartedNotification,
        NativeTurnStatus,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 128;
const RESOLVED_REQUEST_HISTORY: usize = 128;
const MAX_CHANGED_PATHS: usize = 1_000;

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
        model: Option<String>,
        effort: Option<String>,
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
    /// Usage baseline and latest cumulative snapshot for the exact active
    /// turn, from `thread/tokenUsage/updated`. The baseline is the cumulative
    /// `total` minus the first observed per-response `last` (the thread total
    /// as the turn started); the latest is the newest cumulative `total`.
    /// The terminal `turn.completed` reports latest-minus-baseline, so a
    /// repeated notification cannot double count and raw `last` values are
    /// never summed.
    turn_usage: Option<(String, NativeTokenUsageDelta, NativeTokenUsageDelta)>,
    /// Latest merged session-scoped snapshot from
    /// `thread/tokenUsage/updated`: the cumulative per-thread `total` and
    /// `modelContextWindow` capacity. Never turn expenditure; committed as
    /// `session.usage` when accepted and replaced wholesale on every newer
    /// update.
    session_usage: Option<SessionUsage>,
    /// Last committed session snapshot content. Each accepted update commits
    /// the retained snapshot only when it differs, so repeats commit no
    /// extra session fact.
    session_usage_committed: Option<SessionUsage>,
    /// Monotonic counter for `session.usage` native ids so each accepted
    /// update commits under a unique identity.
    session_usage_seq: u64,
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
        model: Option<String>,
        effort: Option<String>,
    ) -> Result<()> {
        self.send(|reply| Control::AssociateTurn {
            command_id,
            turn_id,
            context,
            model,
            effort,
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
            turn_usage: None,
            session_usage: None,
            session_usage_committed: None,
            session_usage_seq: 0,
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
                model,
                effort,
                reply,
            } => {
                let result = self
                    .associate_turn(command_id, turn_id, context, model, effort)
                    .await;
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
                        self.status.failure = Some(*error.message);
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
                        self.status.failure = Some(*error.message);
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

    fn should_buffer(&self, method: &str, params: &Value) -> bool {
        if method == "serverRequest/resolved"
            && params
                .get("requestId")
                .and_then(|id| transport_key(id).ok())
                .is_some_and(|key| self.resolved_transport_ids.contains(&key))
        {
            return false;
        }
        // Session-scoped usage is never held for a turn: once ready it is
        // folded and committed immediately, even while idle.
        if method == "thread/tokenUsage/updated" {
            return !self.status.ready;
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
        model: Option<String>,
        effort: Option<String>,
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
        self.turn_usage = None;
        let thread_id = self.status.thread_id.as_deref().unwrap_or_default();
        let native_id = event_id(thread_id, &turn_id, &turn_id, "turn.started")?;
        let mut data = json!({"command_id": command_id, "turn_id": turn_id});
        // Report the model/effort the native turn request actually carried,
        // so the Store readback shows the carried values instead of the
        // overlay's absent overrides. Omitted when unknown: nothing is
        // invented.
        if let Some(model) = model.as_deref().filter(|model| !model.trim().is_empty()) {
            data["model"] = json!(model);
        }
        if let Some(effort) = effort.as_deref().filter(|effort| !effort.trim().is_empty()) {
            data["effort"] = json!(effort);
        }
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
                    self.status.failure = Some(*error.message);
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
                // v2 child announcement: `kind: "started"` confirms the named
                // thread; any other kind is a no-op lifecycle fact.
                self.observe_sub_agent_activity(
                    notification.thread_id.as_str(),
                    &notification.item,
                )
                .await?;
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
                // v2 child announcement, idempotent with the `item/started`
                // side: `kind: "started"` confirms, anything else is a no-op.
                self.observe_sub_agent_activity(notification.thread_id.as_str(), &raw_item)
                    .await?;
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
            "thread/tokenUsage/updated" => {
                self.note_token_usage(&params).await?;
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

    /// Fold one `thread/tokenUsage/updated` into the generation's retained
    /// session snapshot and, when it names the exact active turn, into that
    /// turn's usage baseline. The first update for a turn captures the
    /// baseline (cumulative `total` minus that update's per-response `last`);
    /// later updates only advance the latest cumulative `total`. An update
    /// naming any other turn (late, stale, or pre-association) leaves the
    /// turn baseline alone without a gap: the terminal turn keeps null usage
    /// rather than another turn's counters. An update without the cumulative
    /// `total` cannot anchor the robust difference, so it leaves the turn
    /// baseline alone. The cumulative per-thread `total` and
    /// `modelContextWindow` capacity always merge into the retained session
    /// snapshot after exact thread validation and commit as `session.usage`
    /// when the snapshot changed: session-scoped facts, never turn
    /// expenditure. Structural mismatch (wrong thread, malformed body) gaps
    /// like any other notification arm.
    async fn note_token_usage(&mut self, params: &Value) -> Result<()> {
        let notification: NativeThreadTokenUsageUpdatedNotification =
            serde_json::from_value(params.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Codex thread/tokenUsage/updated did not match the 0.167 schema",
                )
            })?;
        validate_thread_and_turn(&self.status, &notification.thread_id, &notification.turn_id)?;
        if self.status.active_turn_id.as_deref() == Some(notification.turn_id.as_str())
            && let Some(total) = notification.token_usage.total.clone()
        {
            match self.turn_usage.as_mut() {
                Some((turn_id, _, latest)) if turn_id == &notification.turn_id => {
                    *latest = total;
                }
                _ => {
                    let baseline = usage_baseline(&total, &notification.token_usage.last);
                    self.turn_usage = Some((notification.turn_id.clone(), baseline, total));
                }
            }
        }
        let Some(merged) =
            session_usage_from_token_usage(&notification.token_usage, self.session_usage.as_ref())
        else {
            return Ok(());
        };
        self.session_usage = Some(merged.clone());
        if self.session_usage_committed.as_ref() == Some(&merged) {
            return Ok(());
        }
        self.session_usage_seq = self.session_usage_seq.wrapping_add(1);
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            &notification.turn_id,
            &format!("session-{}", self.session_usage_seq),
            "session.usage",
        )?;
        self.commit_event("session.usage", native_id, json!({"usage": merged}))
            .await?;
        self.session_usage_committed = Some(merged);
        Ok(())
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
        if notification.turn.status == NativeTurnStatus::Failed {
            let origin = notification
                .turn
                .error
                .as_ref()
                .map(|error| error.failure_origin())
                .unwrap_or(eliot_core::FailureOrigin::Unknown);
            let evidence = if origin == eliot_core::FailureOrigin::Provider {
                "CodexErrorInfo reports structured upstream rate, quota, auth, model or HTTP failure"
            } else if origin == eliot_core::FailureOrigin::Harness {
                "native turn error without a structured upstream signal"
            } else {
                "native failed terminal omitted its error classification"
            };
            eliot_plugin::session::attach_failure_origin(
                &mut data, "codex", origin, evidence, &secrets,
            );
        }
        if let Some(restriction) = provider_restriction {
            attach_provider_restriction(&mut data, restriction, &secrets);
        }
        // Optional turn expenditure from `thread/tokenUsage/updated`
        // (latest cumulative snapshot minus the turn-start baseline):
        // attach before `attach_final_text` so the existing bounded
        // terminal-text accounting includes the usage bytes. A repeated
        // notification leaves the difference unchanged instead of double
        // counting, and raw `last` deltas are never summed; a missing or
        // foreign-turn baseline leaves `data.usage` unset.
        let turn_usage = self
            .turn_usage
            .take()
            .filter(|(turn_id, _, _)| *turn_id == notification.turn.id)
            .and_then(|(_, baseline, latest)| turn_expenditure(&baseline, &latest));
        if let Some(usage) = turn_usage {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
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
        // Session facts already committed when each update was accepted, so
        // the terminal carries turn scope only and never re-commits session
        // scope.
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
            let thread = params.get("thread");
            // Two independent confirmations, both verified against the
            // installed 0.167.1 v2 schema: the legacy
            // `source.subAgent.thread_spawn.parent_thread_id` and the new
            // top-level `thread.parentThreadId` (set only for subagents).
            // Either names a known parent (the owned thread or a
            // confirmed child for nesting).
            let legacy_parent = thread.and_then(spawned_parent_thread);
            let top_parent = thread.and_then(top_level_parent_thread);
            let parent = legacy_parent
                .filter(|parent| self.children.is_parent(&owned, parent))
                .or_else(|| top_parent.filter(|parent| self.children.is_parent(&owned, parent)));
            if let Some(parent) = parent {
                let parent = parent.to_owned();
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
        self.turn_usage = None;
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

/// Turn-start baseline from the first observed update: cumulative `total`
/// minus that update's per-response `last`, per counter. A counter is
/// baselined only when both snapshots supply a nonnegative value with the
/// total ahead; otherwise that counter stays unprovable (null), never
/// derived.
fn usage_baseline(
    total: &NativeTokenUsageDelta,
    last: &NativeTokenUsageDelta,
) -> NativeTokenUsageDelta {
    let base = |total: Option<i64>, last: Option<i64>| match (total, last) {
        (Some(total), Some(last)) if total >= 0 && last >= 0 && total >= last => Some(total - last),
        _ => None,
    };
    NativeTokenUsageDelta {
        input_tokens: base(total.input_tokens, last.input_tokens),
        cached_input_tokens: base(total.cached_input_tokens, last.cached_input_tokens),
        output_tokens: base(total.output_tokens, last.output_tokens),
        reasoning_output_tokens: base(total.reasoning_output_tokens, last.reasoning_output_tokens),
        total_tokens: base(total.total_tokens, last.total_tokens),
        cache_write_input_tokens: base(
            total.cache_write_input_tokens,
            last.cache_write_input_tokens,
        ),
    }
}

/// Turn expenditure as latest-minus-baseline per counter. A counter
/// projects only when both snapshots supply it and the latest is still ahead
/// (a backward snapshot stays null, never negative); otherwise that counter
/// is unprovable. Raw `last` deltas are never summed, so a repeated
/// notification cannot inflate the count.
fn turn_expenditure(
    baseline: &NativeTokenUsageDelta,
    latest: &NativeTokenUsageDelta,
) -> Option<TurnUsage> {
    let diff = |latest: Option<i64>, baseline: Option<i64>| match (latest, baseline) {
        (Some(latest), Some(baseline)) if latest >= 0 && baseline >= 0 && latest >= baseline => {
            Some(latest - baseline)
        }
        _ => None,
    };
    turn_usage_from_delta(&NativeTokenUsageDelta {
        input_tokens: diff(latest.input_tokens, baseline.input_tokens),
        cached_input_tokens: diff(latest.cached_input_tokens, baseline.cached_input_tokens),
        output_tokens: diff(latest.output_tokens, baseline.output_tokens),
        reasoning_output_tokens: diff(
            latest.reasoning_output_tokens,
            baseline.reasoning_output_tokens,
        ),
        total_tokens: diff(latest.total_tokens, baseline.total_tokens),
        cache_write_input_tokens: diff(
            latest.cache_write_input_tokens,
            baseline.cache_write_input_tokens,
        ),
    })
}

/// Map one per-turn expenditure delta to `TurnUsage` at `turn` scope. Only
/// genuinely supplied nonnegative counters are mapped; missing values stay
/// null (never zero), and a delta with no usable counter yields no usage at
/// all. `modelContextWindow` capacity never enters this mapping.
fn turn_usage_from_delta(delta: &NativeTokenUsageDelta) -> Option<TurnUsage> {
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let input_tokens = counter(delta.input_tokens);
    let output_tokens = counter(delta.output_tokens);
    let reasoning_tokens = counter(delta.reasoning_output_tokens);
    let cache_read_tokens = counter(delta.cached_input_tokens);
    let cache_write_tokens = counter(delta.cache_write_input_tokens);
    let total_tokens = counter(delta.total_tokens);
    if input_tokens.is_none()
        && output_tokens.is_none()
        && reasoning_tokens.is_none()
        && cache_read_tokens.is_none()
        && cache_write_tokens.is_none()
        && total_tokens.is_none()
    {
        return None;
    }
    Some(TurnUsage {
        scope: Some("turn".to_owned()),
        message_id: None,
        input_tokens,
        output_tokens,
        reasoning_tokens,
        thinking_tokens: None,
        cache_read_tokens,
        cache_write_tokens,
        total_tokens,
    })
}

/// Merge one `thread/tokenUsage/updated` body into the retained session
/// snapshot: the cumulative per-thread `total` replaces the retained
/// cumulative counters (input/output/cache only, never a derived total) and
/// `modelContextWindow` replaces the retained capacity. Yields `None` when
/// the update contributes nothing new, so repeats commit nothing. Negative
/// counters stay null (never zero); an update with no usable cumulative
/// content keeps the retained snapshot.
fn session_usage_from_token_usage(
    token_usage: &NativeThreadTokenUsage,
    previous: Option<&SessionUsage>,
) -> Option<SessionUsage> {
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let mut merged = previous.cloned().unwrap_or(SessionUsage {
        context_used_tokens: None,
        context_window_tokens: None,
        cumulative_tokens: None,
        cumulative_cost: None,
        session_file_bytes: None,
        source: None,
        observed_at: None,
    });
    let mut changed = false;
    if let Some(total) = &token_usage.total {
        let totals = SessionTokenTotals {
            input_tokens: counter(total.input_tokens),
            output_tokens: counter(total.output_tokens),
            cache_read_tokens: counter(total.cached_input_tokens),
            cache_write_tokens: counter(total.cache_write_input_tokens),
        };
        if (totals.input_tokens.is_some()
            || totals.output_tokens.is_some()
            || totals.cache_read_tokens.is_some()
            || totals.cache_write_tokens.is_some())
            && merged.cumulative_tokens.as_ref() != Some(&totals)
        {
            merged.cumulative_tokens = Some(totals);
            changed = true;
        }
        // The cumulative per-thread `total` is the installed schema's only
        // occupancy signal, so it projects to `context_used_tokens` against
        // `modelContextWindow` (never derived into turn expenditure above).
        if let Some(used) = counter(total.total_tokens)
            && merged.context_used_tokens != Some(used)
        {
            merged.context_used_tokens = Some(used);
            changed = true;
        }
    }
    if let Some(window) = counter(token_usage.model_context_window)
        && merged.context_window_tokens != Some(window)
    {
        merged.context_window_tokens = Some(window);
        changed = true;
    }
    if !changed {
        return None;
    }
    merged.source = Some("codex:thread/tokenUsage/updated".to_owned());
    merged.observed_at = None;
    Some(merged)
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
    fn token_usage_delta_maps_counters_at_turn_scope() {
        let delta = NativeTokenUsageDelta {
            input_tokens: Some(10),
            cached_input_tokens: Some(3),
            output_tokens: Some(5),
            reasoning_output_tokens: Some(2),
            total_tokens: Some(15),
            cache_write_input_tokens: Some(1),
        };
        let usage = turn_usage_from_delta(&delta).expect("usable delta maps");
        assert_eq!(usage.scope, Some("turn".to_owned()));
        assert_eq!(usage.message_id, None);
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(5));
        assert_eq!(usage.reasoning_tokens, Some(2));
        assert_eq!(usage.thinking_tokens, None);
        assert_eq!(usage.cache_read_tokens, Some(3));
        assert_eq!(usage.cache_write_tokens, Some(1));
        assert_eq!(usage.total_tokens, Some(15));
    }

    #[test]
    fn token_usage_delta_drops_negative_and_empty_without_zeroes() {
        let delta = NativeTokenUsageDelta {
            input_tokens: Some(-1),
            cached_input_tokens: None,
            output_tokens: Some(0),
            reasoning_output_tokens: None,
            total_tokens: None,
            cache_write_input_tokens: None,
        };
        let usage = turn_usage_from_delta(&delta).expect("zero output still maps");
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, Some(0));
        assert_eq!(usage.cache_read_tokens, None);
        let empty = NativeTokenUsageDelta {
            input_tokens: None,
            cached_input_tokens: None,
            output_tokens: None,
            reasoning_output_tokens: None,
            total_tokens: None,
            cache_write_input_tokens: None,
        };
        assert_eq!(turn_usage_from_delta(&empty), None);
        let negative = NativeTokenUsageDelta {
            input_tokens: Some(-4),
            cached_input_tokens: None,
            output_tokens: None,
            reasoning_output_tokens: None,
            total_tokens: None,
            cache_write_input_tokens: None,
        };
        assert_eq!(turn_usage_from_delta(&negative), None);
    }

    fn breakdown(input: Option<i64>, output: Option<i64>) -> NativeTokenUsageDelta {
        NativeTokenUsageDelta {
            input_tokens: input,
            cached_input_tokens: None,
            output_tokens: output,
            reasoning_output_tokens: None,
            total_tokens: None,
            cache_write_input_tokens: None,
        }
    }

    #[test]
    fn session_usage_from_token_usage_maps_cumulative_capacity_and_occupancy() {
        let body = NativeThreadTokenUsage {
            last: breakdown(Some(1), Some(1)),
            total: Some(NativeTokenUsageDelta {
                input_tokens: Some(110),
                cached_input_tokens: Some(53),
                output_tokens: Some(55),
                reasoning_output_tokens: Some(7),
                total_tokens: Some(165),
                cache_write_input_tokens: Some(9),
            }),
            model_context_window: Some(200_000),
        };
        let merged = session_usage_from_token_usage(&body, None).expect("cumulative maps");
        let totals = merged
            .cumulative_tokens
            .as_ref()
            .expect("cumulative counters");
        assert_eq!(totals.input_tokens, Some(110));
        assert_eq!(totals.output_tokens, Some(55));
        assert_eq!(totals.cache_read_tokens, Some(53));
        assert_eq!(totals.cache_write_tokens, Some(9));
        assert_eq!(merged.context_window_tokens, Some(200_000));
        assert_eq!(merged.context_used_tokens, Some(165));
        assert_eq!(merged.cumulative_cost, None);
        assert_eq!(
            merged.source.as_deref(),
            Some("codex:thread/tokenUsage/updated")
        );
        // A repeat contributes nothing new, so it commits nothing.
        assert_eq!(session_usage_from_token_usage(&body, Some(&merged)), None);
        // A window-only update without the cumulative total keeps the
        // retained counters while replacing the capacity.
        let window_only = NativeThreadTokenUsage {
            last: breakdown(None, None),
            total: None,
            model_context_window: Some(100_000),
        };
        let resized = session_usage_from_token_usage(&window_only, Some(&merged))
            .expect("capacity change merges");
        assert_eq!(resized.context_window_tokens, Some(100_000));
        assert_eq!(
            resized.context_used_tokens,
            Some(165),
            "capacity update must keep the retained occupancy"
        );
        assert_eq!(
            resized
                .cumulative_tokens
                .as_ref()
                .and_then(|totals| totals.input_tokens),
            Some(110),
            "capacity update must keep the retained cumulative counters"
        );
        // Negative counters stay null instead of zero or negative.
        let negative = NativeThreadTokenUsage {
            last: breakdown(None, None),
            total: Some(NativeTokenUsageDelta {
                input_tokens: Some(-4),
                cached_input_tokens: None,
                output_tokens: None,
                reasoning_output_tokens: None,
                total_tokens: None,
                cache_write_input_tokens: None,
            }),
            model_context_window: Some(-1),
        };
        assert_eq!(session_usage_from_token_usage(&negative, None), None);
    }

    #[test]
    fn turn_expenditure_spans_two_model_responses_without_summing_lasts() {
        // Response 1 spends in=10/out=1 on a thread already at in=100/out=50;
        // response 2 spends in=30/out=5. The turn spent 40/6: both responses,
        // not the last response alone (30/5).
        let baseline = usage_baseline(
            &breakdown(Some(110), Some(51)),
            &breakdown(Some(10), Some(1)),
        );
        assert_eq!(baseline.input_tokens, Some(100));
        assert_eq!(baseline.output_tokens, Some(50));
        let usage = turn_expenditure(&baseline, &breakdown(Some(140), Some(56)))
            .expect("two responses map");
        assert_eq!(usage.scope, Some("turn".to_owned()));
        assert_eq!(usage.input_tokens, Some(40));
        assert_eq!(usage.output_tokens, Some(6));
        // A repeated notification holds the difference instead of inflating it.
        let repeated =
            turn_expenditure(&baseline, &breakdown(Some(140), Some(56))).expect("repeat maps");
        assert_eq!(repeated.input_tokens, Some(40));
        // A single-response turn reports exactly that response.
        let single = turn_expenditure(&baseline, &breakdown(Some(110), Some(51)))
            .expect("single response maps");
        assert_eq!(single.input_tokens, Some(10));
        assert_eq!(single.output_tokens, Some(1));
    }

    #[test]
    fn turn_expenditure_leaves_unprovable_counters_null() {
        let baseline = usage_baseline(
            &breakdown(Some(110), Some(51)),
            &breakdown(Some(10), Some(1)),
        );
        // Counter absent from the latest snapshot stays null.
        let missing =
            turn_expenditure(&baseline, &breakdown(Some(140), None)).expect("partial maps");
        assert_eq!(missing.input_tokens, Some(40));
        assert_eq!(missing.output_tokens, None);
        // A backward snapshot stays null, never negative.
        let backward = turn_expenditure(&baseline, &breakdown(Some(90), Some(56)));
        assert!(backward.is_none_or(|usage| usage.input_tokens.is_none()));
    }

    #[tokio::test]
    async fn token_usage_two_responses_accumulate_once_and_duplicates_hold() {
        let (_native, mut processor, host_task, peer_task) = session_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        let update = |input: i64, output: i64, total_in: i64, total_out: i64| {
            json!({
                "threadId": "owned",
                "turnId": "turn-1",
                "tokenUsage": {
                    "last": {"inputTokens": input, "outputTokens": output},
                    "total": {"inputTokens": total_in, "outputTokens": total_out},
                    "modelContextWindow": 200000,
                },
            })
        };
        processor
            .process_notification("thread/tokenUsage/updated", update(10, 1, 110, 51))
            .await
            .expect("first response baselines");
        assert_eq!(processor.session_usage_seq, 1);
        processor
            .process_notification("thread/tokenUsage/updated", update(30, 5, 140, 56))
            .await
            .expect("second response advances");
        assert_eq!(processor.session_usage_seq, 2);
        // A duplicate delivery changes nothing.
        processor
            .process_notification("thread/tokenUsage/updated", update(30, 5, 140, 56))
            .await
            .expect("duplicate holds");
        assert_eq!(
            processor.session_usage_seq, 2,
            "duplicate snapshot commits nothing extra"
        );
        let (turn_id, baseline, latest) = processor.turn_usage.as_ref().expect("snapshots kept");
        assert_eq!(turn_id, "turn-1");
        assert_eq!(baseline.input_tokens, Some(100));
        assert_eq!(baseline.output_tokens, Some(50));
        assert_eq!(latest.input_tokens, Some(140));
        assert_eq!(latest.output_tokens, Some(56));
        let usage = turn_expenditure(baseline, latest).expect("turn maps");
        assert_eq!(usage.input_tokens, Some(40));
        assert_eq!(usage.output_tokens, Some(6));
        let session = processor.session_usage.as_ref().expect("session retained");
        assert_eq!(
            session
                .cumulative_tokens
                .as_ref()
                .and_then(|totals| totals.input_tokens),
            Some(140)
        );
        host_task.abort();
        peer_task.abort();
    }

    #[tokio::test]
    async fn busy_stop_interrupted_frame_preserves_exact_native_evidence() {
        let (_native, mut processor, host_task, peer_task) = session_test_processor().await;
        processor.status.ready = true;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_command_id = Some("send-1".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        let frame = json!({"threadId":"owned", "turn":{
            "id":"turn-1", "status":"interrupted", "items":[], "error":null
        }});
        let mut foreign = frame.clone();
        foreign["threadId"] = json!("other-thread");
        assert!(
            processor
                .process_notification("turn/completed", foreign)
                .await
                .is_err()
        );
        assert_eq!(processor.status.active_turn_id.as_deref(), Some("turn-1"));
        processor
            .process_notification("turn/completed", frame)
            .await
            .unwrap();
        let terminal = processor.status.last_turn.as_ref().unwrap();
        assert_eq!(terminal.turn_id, "turn-1");
        assert_eq!(terminal.command_id, "send-1");
        assert_eq!(terminal.status.as_deref(), Some("interrupted"));
        assert!(terminal.terminal);
        assert!(processor.status.active_turn_id.is_none());
        assert!(!processor.status.source_unknown);
        // No native terminal during the next stop: custody proves only exit.
        processor.status.active_command_id = Some("send-2".to_owned());
        processor.status.active_turn_id = Some("turn-2".to_owned());
        processor.finish_stop().await.unwrap();
        let unknown = processor.status.last_turn.as_ref().unwrap();
        assert_eq!(unknown.turn_id, "turn-2");
        assert_eq!(unknown.command_id, "send-2");
        assert!(!unknown.terminal);
        assert!(unknown.status.is_none());
        host_task.abort();
        peer_task.abort();
    }

    #[tokio::test]
    async fn token_usage_update_for_other_turn_is_dropped_without_gap() {
        let (_native, mut processor, host_task, peer_task) = session_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = Some("turn-1".to_owned());
        processor
            .process_notification(
                "thread/tokenUsage/updated",
                json!({
                    "threadId": "owned",
                    "turnId": "turn-2",
                    "tokenUsage": {
                        "last": {"inputTokens": 7},
                        "total": {"inputTokens": 50, "outputTokens": 9},
                        "modelContextWindow": 200000,
                    },
                }),
            )
            .await
            .expect("foreign-turn update drops cleanly");
        assert!(
            processor.turn_usage.is_none(),
            "another turn's counters must not attach here"
        );
        assert!(
            !processor.status.source_unknown,
            "stale telemetry is not a source gap"
        );
        // Session scope still merges after exact thread validation.
        let session = processor.session_usage.as_ref().expect("session merges");
        assert_eq!(
            session
                .cumulative_tokens
                .as_ref()
                .and_then(|totals| totals.input_tokens),
            Some(50)
        );
        assert_eq!(processor.session_usage_seq, 1);
        host_task.abort();
        peer_task.abort();
    }

    #[tokio::test]
    async fn token_usage_idle_update_merges_session_without_active_turn() {
        let (_native, mut processor, host_task, peer_task) = session_test_processor().await;
        processor.status.thread_id = Some("owned".to_owned());
        processor.status.active_turn_id = None;
        processor
            .process_notification(
                "thread/tokenUsage/updated",
                json!({
                    "threadId": "owned",
                    "turnId": "turn-9",
                    "tokenUsage": {
                        "last": {"inputTokens": 1},
                        "total": {"inputTokens": 11, "outputTokens": 2},
                        "modelContextWindow": 200000,
                    },
                }),
            )
            .await
            .expect("idle update merges");
        assert!(
            processor.turn_usage.is_none(),
            "idle update baselines no turn"
        );
        assert!(
            processor.session_usage.is_some(),
            "idle update still merges session scope"
        );
        assert_eq!(processor.session_usage_seq, 1);
        host_task.abort();
        peer_task.abort();
    }

    /// Processor with a fake host that ACKs every `events.put` commit, so
    /// per-accept `session.usage` commits resolve without a live daemon.
    /// Returns the native handle, the processor, and the two background
    /// tasks (host ACK loop, peer drain) for the caller to abort.
    async fn session_test_processor() -> (
        crate::native::NativeProcess,
        EventProcessor,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};

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
                agent_id: "a1".to_owned(),
                generation: 1,
                secrets: Vec::new(),
                native_scope: String::new(),
            },
        );
        let (host_read, mut host_write) = split(host_io);
        let mut host_reader = BufReader::new(host_read);
        let host_task = tokio::spawn(async move {
            let mut seq: i64 = 0;
            let mut line = String::new();
            loop {
                line.clear();
                let read = host_reader
                    .read_line(&mut line)
                    .await
                    .expect("read fake-host request");
                if read == 0 {
                    break;
                }
                let request: eliot_plugin::RpcRequest =
                    serde_json::from_str(&line).expect("decode request");
                assert_eq!(request.method, "events.put");
                let count = request.params["events"]
                    .as_array()
                    .map(|events| events.len() as i64)
                    .unwrap_or(1);
                let first_seq = seq + 1;
                let end = first_seq + count - 1;
                seq = end;
                let cursor = request.params.get("cursor").cloned().unwrap_or(Value::Null);
                let response = json!({
                    "jsonrpc": "2.0",
                    "id": request.id,
                    "result": {"first_seq": first_seq, "last_seq": end, "cursor": cursor},
                });
                host_write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .expect("send ACK");
            }
        });
        (native, processor, host_task, peer_task)
    }
}
