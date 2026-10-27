use eliot_core::{Error, NativeEvent, Result, SourceBatch};
use eliot_plugin::{CommitSink, MAX_EVENT_BYTES, PeerClient, protocol::MAX_COMMAND_ID_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    NativeClient, NativeFrame, StreamState,
    requests::{self, CorePending},
    types::{
        NativeItemCompletedNotification, NativeServerRequestResolvedNotification,
        NativeTurnCompletedNotification, NativeTurnStartedNotification, NativeTurnStatus,
    },
};

const EVENT_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 128;
const RESOLVED_REQUEST_HISTORY: usize = 128;

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
                if !self.status.ready || self.status.active_turn_id.is_none() {
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
                if self.should_buffer(&method, &params) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        let _ = self
                            .record_gap("Codex notification could not be normalized")
                            .await;
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
            let result = match frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    self.process_notification(method, params).await
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

    async fn process_notification(&mut self, method: String, params: Value) -> Result<()> {
        match method.as_str() {
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
        let text = redact(text, &self.secrets);
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
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": item_id,
            "tool": item_type,
        });
        if let Some(native_status) = native_status {
            data["status"] = Value::String(native_status);
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
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": notification.turn.id,
            "status": status_text,
        });
        if let Some(text) = self.final_text.as_ref() {
            data["final_text"] = Value::String(text.clone());
        }
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
        if self
            .status
            .thread_id
            .as_deref()
            .is_some_and(|existing| existing != thread_id)
        {
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
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Codex event acknowledgement is unresolved",
            ));
        }
        let batch = SourceBatch {
            batch_id: format!("{native_id}:batch"),
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                kind: kind.to_owned(),
                native_id,
                data,
            }],
            cursor: None,
        };
        let error = match self
            .sink
            .commit(batch, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        if self.sink.is_unknown() {
            self.writes_blocked = true;
            self.status.source_unknown = true;
            self.status.failure = Some("Codex event batch acknowledgement is unknown".to_owned());
            self.status.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                "Codex event batch acknowledgement is unknown",
            ));
        }
        let reason = format!("{kind} could not be committed: {}", error.code);
        self.record_gap(&reason).await?;
        Err(Error::new(
            "unknown",
            "Codex native fact was not committed; no replay is allowed",
        ))
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        if self.writes_blocked {
            self.status.source_unknown = true;
            return Err(Error::new(
                "unknown",
                "Codex event acknowledgement is unresolved; no source gap was issued",
            ));
        }
        if self.gap_reported {
            return Err(Error::new(
                "unknown",
                "Codex source already has an unresolved gap",
            ));
        }
        self.status.source_unknown = true;
        self.gap_reported = true;
        self.gap_number = self.gap_number.saturating_add(1);
        let gap_id = format!(
            "codex:{}:{}:gap-{}",
            self.agent_id, self.generation, self.gap_number
        );
        let ack_id = gap_id.clone();
        let result = self
            .host
            .call(
                "source.gap",
                json!({
                    "credential": self.credential,
                    "agent_id": self.agent_id,
                    "generation": self.generation,
                    "source": "stream",
                    "gap_id": gap_id,
                    "families": ["session", "turn", "message", "tool", "request"],
                    "reason": reason,
                    "from": null,
                    "to": null,
                }),
                Instant::now() + EVENT_DEADLINE,
            )
            .await;
        match result {
            Ok(ack)
                if ack.get("gap_id").and_then(Value::as_str) == Some(ack_id.as_str())
                    && ack
                        .get("seq")
                        .and_then(Value::as_i64)
                        .is_some_and(|seq| seq > 0) =>
            {
                self.status.failure = Some(format!("{reason}; terminal outcome is unknown"));
                Ok(())
            }
            Ok(_) | Err(_) => {
                self.writes_blocked = true;
                self.status.failure =
                    Some("Codex source gap acknowledgement is unknown".to_owned());
                Err(Error::new(
                    "unknown",
                    "Codex source gap acknowledgement is unknown",
                ))
            }
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

fn validate_thread_and_turn(status: &EventStatus, thread_id: &str, turn_id: &str) -> Result<()> {
    if status.thread_id.as_deref() != Some(thread_id) || turn_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Codex notification did not belong to the owned native thread",
        ));
    }
    Ok(())
}

fn event_id(thread_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    let native_id = format!("codex:{thread_id}:{turn_id}:{item_id}:{kind}");
    if native_id.len() > MAX_COMMAND_ID_BYTES
        || thread_id.trim().is_empty()
        || item_id.trim().is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            "Codex native event identity exceeded its bound",
        ));
    }
    Ok(native_id)
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

fn native_no_write(code: &str) -> bool {
    matches!(code, "native_not_dispatched" | "busy" | "frame_too_large")
}

fn rejected_control(reason: &str) -> Value {
    json!({"state": "rejected", "reason": reason})
}

fn unknown_control(reason: &str) -> Value {
    json!({"state": "unknown", "reason": reason})
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

fn redact(text: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|secret| secret.len() >= 4)
        .fold(text.to_owned(), |value, secret| {
            value.replace(secret, "[REDACTED]")
        })
}
