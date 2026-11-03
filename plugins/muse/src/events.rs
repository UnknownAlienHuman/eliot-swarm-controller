use eliot_core::{Error, NativeEvent, ProviderRestriction, Result, SecretPolicy, SourceBatch};
use eliot_plugin::{CommitSink, MAX_EVENT_BYTES, PeerClient, protocol::MAX_COMMAND_ID_BYTES};
use serde::Serialize;
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
    requests::{
        CorePending, describe_request, refresh_offer_from_raw, reply_for, same_approval_offer,
    },
    types::{
        NativeApprovalDecideResult, NativeApprovalResolvedNotification,
        NativeApprovalUpdatedNotification, NativeItemCompletedNotification,
        NativeItemStartedNotification, NativeSessionClosedNotification,
        NativeSessionStartedNotification, NativeTurnCompletedNotification, NativeTurnError,
        NativeTurnStartedNotification, NativeTurnTerminal, NativeUserInputAnswerResult,
        NativeUserInputCancelResult, NativeUserInputSettledNotification,
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
    pub session_id: Option<String>,
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub last_turn: Option<TurnFacts>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    pub stopped: bool,
    /// Local observation counter: bumped on every stream observation that
    /// touches the pending-request set. An in-memory race guard for stale
    /// request snapshots (resume pointers, listPending pulls), never a
    /// persisted epoch.
    pub observation: u64,
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
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateTurn {
        command_id: String,
        turn_id: String,
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
    AdoptPulled {
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
        reply: oneshot::Sender<Result<()>>,
    },
    BeginStop {
        reply: oneshot::Sender<Result<()>>,
    },
    EndStop {
        reply: oneshot::Sender<Result<()>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
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
    reply_command_id: Option<String>,
    reply_state: NativeReplyState,
    /// Observation counter at the last stream update of this entry.
    observed_at: u64,
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
    gap_number: u64,
    accept_frames: bool,
    planned_stop: bool,
    loss_reason: Option<&'static str>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    final_text: Option<String>,
    pending_requests: HashMap<String, PendingNativeRequest>,
    /// Native approval/user-input id to public pending id.
    native_index: HashMap<String, String>,
    /// Terminal resolutions by native id: a resolved request is never
    /// re-adopted from any snapshot, however new the snapshot claims to be.
    resolved_native_ids: HashSet<String>,
    resolved_native_order: VecDeque<String>,
    /// Turn-end drops by native id with the observation counter at the drop:
    /// a pulled snapshot entry is adopted only when no newer stream
    /// observation touched its id.
    dropped_native_at: HashMap<String, u64>,
    dropped_native_order: VecDeque<String>,
    /// Resume-pointer ids not yet seen on the stream: a re-issued request
    /// for one that binds to a dead turn is receipted and dropped as an
    /// orphan instead of gapping the source. Cleared silently at turn end.
    expected_requests: HashSet<String>,
    /// Pulled snapshot entries waiting for a live turn to bind to.
    stashed_entries: Vec<StashedPending>,
}

struct StashedPending {
    request: CorePending,
    capture: u64,
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
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        self.send(|reply| Control::Ready {
            native_scope,
            session_id,
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
        .map_err(|_| Error::new("unknown", "Muse event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "Muse event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Muse event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Muse event-control response was lost"))?
    }

    pub async fn adopt_pulled(
        &self,
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
    ) -> Result<()> {
        self.send(|reply| Control::AdoptPulled {
            entries,
            expected,
            capture,
            snapshot_had_undecodable,
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
        .map_err(|_| Error::new("unknown", "Muse event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "Muse event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Muse event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Muse event-control response was lost"))?
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
            gap_number: 0,
            accept_frames: true,
            planned_stop: false,
            loss_reason: None,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            final_text: None,
            pending_requests: HashMap::new(),
            native_index: HashMap::new(),
            resolved_native_ids: HashSet::new(),
            resolved_native_order: VecDeque::new(),
            dropped_native_at: HashMap::new(),
            dropped_native_order: VecDeque::new(),
            expected_requests: HashSet::new(),
            stashed_entries: Vec::new(),
        }
    }

    fn observe(&mut self) -> u64 {
        self.status.observation = self.status.observation.saturating_add(1);
        self.status.observation
    }

    async fn handle_control(&mut self, control: Control) -> bool {
        match control {
            Control::Ready {
                native_scope,
                session_id,
                effective,
                reply,
            } => {
                let result = self.ready(native_scope, session_id, effective).await;
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
            Control::AdoptPulled {
                entries,
                expected,
                capture,
                snapshot_had_undecodable,
                reply,
            } => {
                let result = self
                    .adopt_pulled(entries, expected, capture, snapshot_had_undecodable)
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
                        "Muse stop is already waiting for native output drain",
                    )));
                } else {
                    self.pending_end_stop = Some(reply);
                }
                true
            }
            Control::Close { reply } => {
                if let Some(pending) = self.pending_end_stop.take() {
                    let _ = pending.send(Err(Error::new(
                        "unknown",
                        "Muse native event queue closed before stop settlement",
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
                    self.defer_gap("Muse session host event stream closed");
                }
            }
            StreamState::Overflow => self.defer_gap("Muse session host event queue overflowed"),
            StreamState::Malformed => {
                self.defer_gap("Muse session host stream contained an invalid frame")
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
                            .record_gap("Muse native request could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(error.message);
                    }
                }
            }
            Some(NativeFrame::Notification { method, params }) => {
                if self.should_buffer(&method) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        let _ = self
                            .record_gap("Muse notification could not be normalized")
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
                    self.defer_gap("Muse native events remained unassociated at stream end");
                }
                if !self.pending_requests.is_empty() {
                    self.defer_gap("Muse native requests remained unresolved at stream end");
                }
                if !self.planned_stop && self.loss_reason.is_none() {
                    self.defer_gap("Muse native event reader closed");
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

    fn should_buffer(&self, method: &str) -> bool {
        matches!(
            method,
            "turn/started"
                | "turn/completed"
                | "item/started"
                | "item/updated"
                | "item/completed"
                | "approval/resolved"
                | "approval/updated"
                | "userInput/settled"
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
            self.defer_gap("Muse early-event buffer exceeded its bound");
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedFrame { frame, size });
    }

    async fn ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        if self.status.ready || session_id.trim().is_empty() || !effective.is_object() {
            return Err(Error::new(
                "invalid",
                "Muse readiness requires one exact session and effective settings",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|observed| observed != session_id)
        {
            self.record_gap("Muse start response disagreed with session/started")
                .await?;
            return Err(Error::new(
                "source_integrity",
                "Muse session identity did not match its start notification",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.clone());
        let native_id = format!("muse:{session_id}:session.ready");
        self.commit_event(
            "session.ready",
            native_id,
            json!({
                "native_scope": native_scope,
                "native_ref": session_id,
                "effective": effective,
            }),
        )
        .await?;
        self.status.ready = true;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    async fn associate_turn(&mut self, command_id: String, turn_id: String) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Muse turn association requires a ready exact session and turn",
            ));
        }
        if self.status.active_turn_id.is_some() {
            return Err(Error::new(
                "busy",
                "Muse already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        self.final_text = None;
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = event_id(session_id, &turn_id, &turn_id, "turn.started")?;
        self.commit_event(
            "turn.started",
            native_id,
            json!({"command_id": command_id, "turn_id": turn_id}),
        )
        .await?;
        self.flush_buffer().await?;
        self.apply_stashed_entries().await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            let should_buffer = match &frame.frame {
                BufferedNativeFrame::Notification { method, .. } => self.should_buffer(method),
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
                        .record_gap("Buffered Muse native frame could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(error.message);
                }
                return Err(Error::new(
                    "unknown",
                    "Buffered Muse event outcome is unresolved",
                ));
            }
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    /// Server-initiated must-answer requests. `approval/request` and
    /// `userInput/request` are presented (transport receipt `{}`) and opened
    /// durably; the decision/answer travels later as `approval/decide`,
    /// `userInput/answer` or `userInput/cancel` on the command lane. Unknown
    /// methods are rejected with a typed error, never an invented default
    /// reply, and gap the source: the native turn may be waiting on a request
    /// this slice cannot present.
    async fn process_request(&mut self, id: Value, method: String, params: Value) -> Result<()> {
        let session_id = self
            .status
            .session_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Muse request has no owned session"))?;
        let turn_id = self
            .status
            .active_turn_id
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "Muse request has no associated turn"))?;
        let request = match describe_request(&method, &params, session_id, turn_id) {
            Ok(request) => request,
            Err(error) if error.code == "source_integrity" => {
                // A scope mismatch for a resume-expected id is a routine
                // orphan (the dead generation's turn): receipt it and drop it
                // instead of gapping the source for another generation's past.
                if let Some(native_id) = orphan_request_id(&method, &params)
                    && self.expected_requests.remove(native_id.as_str())
                {
                    // The receipt is fire-and-forget by protocol: a lost
                    // write just means the host re-issues the orphan later,
                    // which drops the same way.
                    let _ = self
                        .native
                        .reply_server_request(id, json!({}), Instant::now() + EVENT_DEADLINE)
                        .await;
                    return Ok(());
                }
                return self.reject_request(id, &method, &params, error).await;
            }
            Err(error) => {
                return self.reject_request(id, &method, &params, error).await;
            }
        };
        // The receipt acknowledges presentation only (SS5.3.3): it changes no
        // state and the decision travels as a command.
        if self
            .native
            .reply_server_request(id.clone(), json!({}), Instant::now() + EVENT_DEADLINE)
            .await
            .is_err()
        {
            self.defer_gap("Muse native request receipt was not written");
            return Ok(());
        }
        let request_id = request.id.clone();
        if let Some(native_id) = pending_native_id(&request)
            && self.resolved_native_ids.contains(native_id)
        {
            self.observe();
            return self
                .record_gap("Muse native request was re-issued after its resolution")
                .await;
        }
        // Re-issue of a retained open: an identical offer is idempotent, a
        // changed approval offer replaces the in-memory snapshot (the Store
        // keeps the opened snapshot; replies always decide from the refreshed
        // offer and its current requirement).
        let reissued = self.pending_requests.contains_key(&request_id);
        if reissued {
            let same = request.kind != "approval"
                || self
                    .pending_requests
                    .get(&request_id)
                    .is_some_and(|existing| {
                        same_approval_offer(&existing.request.details, &params)
                    });
            let observed = self.observe();
            if let Some(existing) = self.pending_requests.get_mut(&request_id) {
                existing.observed_at = observed;
                if !same && refresh_offer_from_raw(&mut existing.request.details, &params).is_err()
                {
                    return self
                        .record_gap("Muse re-issued approval omitted its refreshed offer")
                        .await;
                }
            }
            if let Some(native_id) = orphan_request_id(&method, &params) {
                self.expected_requests.remove(native_id.as_str());
            }
            return Ok(());
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.defer_gap("Muse pending native request bound was exceeded");
            return Ok(());
        }
        let command_id = self
            .status
            .active_command_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Muse request has no Eliot send command"))?;
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let data = json!({
            "command_id": command_id,
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": request.kind,
            "details": redact_request_details_for_commit(&request.details, &policy),
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse normalized request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&request_id, "request.opened") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.defer_gap("Muse native request event identity exceeded its bound");
                return Ok(());
            }
        };
        self.commit_event("request.opened", native_id, data).await?;
        let observed = self.observe();
        if let Some(native_id) = pending_native_id(&request) {
            self.native_index
                .insert(native_id.to_owned(), request_id.clone());
            self.expected_requests.remove(native_id);
        }
        self.pending_requests.insert(
            request_id.clone(),
            PendingNativeRequest {
                request,
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
                observed_at: observed,
            },
        );
        Ok(())
    }

    async fn reject_request(
        &mut self,
        id: Value,
        method: &str,
        params: &Value,
        error: Error,
    ) -> Result<()> {
        let rejected = self
            .native
            .reject_unsupported_request(id, method, params, Instant::now() + EVENT_DEADLINE)
            .await;
        if error.code == "unsupported" {
            // Unknown request family: rejected with a typed error, and the
            // source gaps — the native turn may be waiting on a request this
            // slice cannot present.
            self.defer_gap("Muse native request method is not connected");
        } else if rejected.is_err() {
            self.defer_gap("Muse native request rejection was not written");
        } else {
            self.defer_gap("Muse native request did not match its exact session/turn schema");
        }
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
                "Muse request reply requires exact command, request, and turn ids",
            ));
        }
        let Some(entry) = self.pending_requests.get_mut(request_id) else {
            return Ok(rejected_control("Muse request is no longer pending"));
        };
        if entry.request.turn_id != turn_id {
            return Ok(rejected_control(
                "Muse request reply does not match the captured turn",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Ok(rejected_control(
                "Muse request is no longer attached to the active turn",
            ));
        }
        match entry.reply_state {
            NativeReplyState::Writing | NativeReplyState::Unknown => {
                return Ok(unknown_control(
                    "Muse request reply outcome is unresolved and will not be repeated",
                ));
            }
            NativeReplyState::Written => {
                return Ok(rejected_control(
                    "Muse request already has a native decision and awaits resolution",
                ));
            }
            NativeReplyState::Available => {}
        }
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(rejected_control(
                "Muse request session is no longer writable",
            ));
        };
        if self.status.stopped {
            return Ok(rejected_control(
                "Muse request session is no longer writable",
            ));
        }
        // The native idempotency handle is minted here, before the write: a
        // lost reply leaves the outcome unknown and the decision is never
        // written a second time under another id.
        let native_command_id = crate::host::new_command_id();
        let reply = match reply_for(&entry.request, verb, args, &native_command_id, &session_id) {
            Ok(reply) => reply,
            Err(error) => return Ok(rejected_control(&error.message)),
        };
        let params = reply.params;
        let expected_native_id = match reply.method {
            "approval/decide" => params.get("approvalId").and_then(Value::as_str),
            _ => params.get("userInputId").and_then(Value::as_str),
        }
        .unwrap_or_default()
        .to_owned();
        let request = entry.request.clone();
        entry.reply_state = NativeReplyState::Writing;
        let acknowledged = match self
            .native
            .call(reply.method, params, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(ack) => ack,
            Err(error)
                if crate::native::native_no_write(&error.code)
                    || error.code == "native_rejected" =>
            {
                // Proven not admitted (transport never wrote, or a
                // schema-documented pre-admission/validation error): the
                // request stays available for a later reply.
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                }
                return Ok(rejected_control("Muse request reply was not admitted"));
            }
            Err(_) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                return Ok(unknown_control(
                    "Muse request reply may have been dispatched; it will not be repeated",
                ));
            }
        };
        // Admission proves nothing by itself (the ack echoes the command
        // id, not the decision): the ack must echo the exact submitted
        // command id, the acceptance status, and the exact decided request
        // id. A malformed ack leaves the write unknown.
        let acked = match reply.method {
            "approval/decide" => serde_json::from_value::<NativeApprovalDecideResult>(acknowledged)
                .ok()
                .is_some_and(|ack| {
                    ack.command_id == native_command_id
                        && ack.status == "accepted"
                        && ack.approval_id == expected_native_id
                }),
            "userInput/answer" => {
                serde_json::from_value::<NativeUserInputAnswerResult>(acknowledged)
                    .ok()
                    .is_some_and(|ack| {
                        ack.command_id == native_command_id
                            && ack.status == "accepted"
                            && ack.user_input_id == expected_native_id
                    })
            }
            _ => serde_json::from_value::<NativeUserInputCancelResult>(acknowledged)
                .ok()
                .is_some_and(|ack| {
                    ack.command_id == native_command_id
                        && ack.status == "accepted"
                        && ack.user_input_id == expected_native_id
                }),
        };
        if !acked {
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
                entry.reply_command_id = Some(command_id.to_owned());
            }
            return Ok(unknown_control(
                "Muse request reply did not acknowledge the exact submitted command",
            ));
        }
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

    /// Adopt pulled snapshot entries. Entries that bind to the live turn are
    /// opened; entries for any other turn are stashed until a turn associates
    /// (then adopted or dropped as orphans). A newer stream resolution always
    /// wins over stale snapshot data: terminally resolved ids are never
    /// re-adopted, and an id dropped after the snapshot capture stays dropped.
    async fn adopt_pulled(
        &mut self,
        entries: Vec<CorePending>,
        expected: Vec<String>,
        capture: u64,
        snapshot_had_undecodable: bool,
    ) -> Result<()> {
        // The pull landed but at least one entry could not be described: an
        // explicit gap records the missing presentation instead of silently
        // dropping it. Adoption runs after readiness, so this never fails the
        // resume (a gap acknowledgement failure is ignored the same way).
        if snapshot_had_undecodable {
            let _ = self
                .record_gap("Muse pulled request snapshot contained an undecodable entry")
                .await;
        }
        for id in expected.into_iter().take(MAX_PENDING_REQUESTS) {
            if !id.trim().is_empty() {
                self.expected_requests.insert(id);
            }
        }
        for entry in entries {
            if self.pending_requests.contains_key(&entry.id)
                || self.snapshot_superseded(&entry, capture)
            {
                continue;
            }
            if self
                .status
                .active_turn_id
                .as_deref()
                .is_some_and(|active| active == entry.turn_id)
            {
                self.open_pulled_entry(entry).await?;
            } else if self.stashed_entries.len() < MAX_PENDING_REQUESTS {
                self.stashed_entries.push(StashedPending {
                    request: entry,
                    capture,
                });
            }
        }
        Ok(())
    }

    /// Whether a pulled snapshot entry lost to a newer stream observation: a
    /// terminal resolution always wins, and a drop newer than the snapshot
    /// capture wins too.
    fn snapshot_superseded(&self, entry: &CorePending, capture: u64) -> bool {
        pending_native_id(entry).is_some_and(|native_id| {
            self.resolved_native_ids.contains(native_id)
                || self
                    .dropped_native_at
                    .get(native_id)
                    .is_some_and(|dropped_at| *dropped_at > capture)
        })
    }

    async fn open_pulled_entry(&mut self, entry: CorePending) -> Result<()> {
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.defer_gap("Muse pending native request bound was exceeded");
            return Ok(());
        }
        let Some(command_id) = self.status.active_command_id.clone() else {
            self.defer_gap("Muse pulled request arrived with no active command");
            return Ok(());
        };
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let data = json!({
            "command_id": command_id,
            "request_id": entry.id,
            "turn_id": entry.turn_id,
            "kind": entry.kind,
            "details": redact_request_details_for_commit(&entry.details, &policy),
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse pulled request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&entry.id, "request.opened") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.defer_gap("Muse native request event identity exceeded its bound");
                return Ok(());
            }
        };
        let request_id = entry.id.clone();
        self.commit_event("request.opened", native_id, data).await?;
        let observed = self.observe();
        if let Some(native_id) = pending_native_id(&entry) {
            self.native_index
                .insert(native_id.to_owned(), request_id.clone());
            self.expected_requests.remove(native_id);
        }
        self.pending_requests.insert(
            request_id,
            PendingNativeRequest {
                request: entry,
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
                observed_at: observed,
            },
        );
        Ok(())
    }

    /// Apply stashed snapshot entries once a turn associates: adopt what binds
    /// to the new turn (guarded again — the stream may have adopted or settled
    /// the id meanwhile), drop the rest as orphans of dead turns.
    async fn apply_stashed_entries(&mut self) -> Result<()> {
        let stashed = std::mem::take(&mut self.stashed_entries);
        for stash in stashed {
            let entry = stash.request;
            if self.pending_requests.contains_key(&entry.id)
                || self.snapshot_superseded(&entry, stash.capture)
            {
                continue;
            }
            if self
                .status
                .active_turn_id
                .as_deref()
                .is_some_and(|active| active == entry.turn_id)
            {
                self.open_pulled_entry(entry).await?;
            }
        }
        Ok(())
    }

    async fn process_notification(&mut self, method: String, params: Value) -> Result<()> {
        match method.as_str() {
            "turn/started" => {
                let notification: NativeTurnStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse turn/started did not match the 1.4 schema",
                        )
                    })?;
                validate_session_and_turn(
                    &self.status,
                    &notification.session_id,
                    &notification.turn_id,
                )?;
                if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
                    return self
                        .record_gap("Muse turn/started did not match the exact submitted turn")
                        .await;
                }
            }
            "item/started" => {
                let notification: NativeItemStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/started did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
            }
            "item/updated" => {
                // Full re-emission at a higher revision; PR1 folds only the
                // authoritative terminal object from `item/completed`.
                let notification: NativeItemCompletedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/updated did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
            }
            "item/completed" => {
                let notification: NativeItemCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/completed did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
                if notification.item.kind == "agentMessage" {
                    let text = notification.item.text.as_deref().ok_or_else(|| {
                        Error::new("native_protocol", "Muse agentMessage omitted its text")
                    })?;
                    let turn_id = notification.item.turn_id.as_deref().unwrap_or_default();
                    self.complete_message(turn_id, &notification.item.item_id, text)
                        .await?;
                }
            }
            "turn/completed" => {
                self.complete_turn(params).await?;
            }
            "approval/resolved" => {
                self.resolve_approval(params).await?;
            }
            "approval/updated" => {
                self.refresh_approval(params).await?;
            }
            "userInput/settled" => {
                self.settle_user_input(params).await?;
            }
            "session/started" => {
                self.observe_session_started(&params).await?;
            }
            "session/closed" => {
                let notification: NativeSessionClosedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse session/closed did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str())
                    && !self.planned_stop
                {
                    return self
                        .record_gap("Muse session closed outside a planned stop")
                        .await;
                }
            }
            "view/gap" => {
                let notification: crate::native::types::NativeViewGapNotification =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse view/gap did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str()) {
                    return self
                        .record_gap("Muse view stream reported a delivery gap")
                        .await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Items outside the exact active turn are gaps; `userShell` is the one
    /// kind outside any turn and is ignored in PR1.
    fn validate_item_turn(&self, session_id: &str, turn_id: Option<&String>) -> Result<()> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            ));
        }
        match turn_id.map(String::as_str) {
            None => Ok(()),
            Some(turn_id) if turn_id.trim().is_empty() => Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            )),
            Some(turn_id) => {
                if self.status.active_turn_id.as_deref() != Some(turn_id) {
                    return Err(Error::new(
                        "source_integrity",
                        "Muse item was outside the exact active turn",
                    ));
                }
                Ok(())
            }
        }
    }

    async fn complete_message(&mut self, turn_id: &str, item_id: &str, text: &str) -> Result<()> {
        if item_id.trim().is_empty() {
            return self
                .record_gap("Muse agentMessage omitted its item id")
                .await;
        }
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(text);
        let data = json!({
            "command_id": self.status.active_command_id.as_deref().unwrap_or_default(),
            "turn_id": turn_id,
            "text": text,
        });
        let encoded_len = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes.len(),
            Err(_) => usize::MAX,
        };
        if encoded_len > MAX_EVENT_BYTES {
            return self
                .record_gap("Muse completed message exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            turn_id,
            item_id,
            "message.completed",
        )?;
        // The accumulated reply; a later `agentMessage` replaces an earlier one.
        self.final_text = Some(text);
        self.commit_event("message.completed", native_id, data)
            .await
    }

    async fn complete_turn(&mut self, params: Value) -> Result<()> {
        let notification: NativeTurnCompletedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse turn/completed did not match the 1.4 schema",
                )
            })?;
        validate_session_and_turn(
            &self.status,
            &notification.session_id,
            &notification.turn_id,
        )?;
        if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
            return self
                .record_gap("Muse terminal notification did not match the exact active turn")
                .await;
        }
        let Some(status_text) = turn_status(&notification.terminal) else {
            return self
                .record_gap("Muse turn/completed carried an unknown terminal")
                .await;
        };
        let command_id = self
            .status
            .active_command_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Muse terminal turn has no Eliot command"))?;
        let provider_restriction = terminal_provider_restriction(&self.status, &notification);
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": notification.turn_id,
            "status": status_text,
        });
        // The semantic hold signal gets first claim on the bounded terminal data.
        if let Some(restriction) = provider_restriction {
            attach_provider_restriction(&mut data, restriction, &secrets);
        }
        if let Some(text) = self.final_text.as_ref() {
            attach_final_text(
                &mut data,
                text,
                notification.terminal == NativeTurnTerminal::Failed,
                &secrets,
            );
        }
        attach_native_error(&mut data, notification.error.as_ref(), &secrets);
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &notification.turn_id,
            &notification.turn_id,
            "turn.completed",
        )?;
        self.commit_event("turn.completed", native_id, data).await?;
        let dead_turn_id = notification.turn_id.clone();
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: notification.turn_id,
            status: Some(status_text.to_owned()),
            final_text: self.final_text.take(),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        // The turn's requests die with it: drop them locally (their Store
        // rows stay bound to the dead turn, so no later reply can address
        // them) and swallow their late resolutions. Resume orphans that never
        // materialized clear silently — they belonged to a dead turn too.
        let dropped = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| entry.request.turn_id == dead_turn_id)
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
        Ok(())
    }

    fn remember_resolved(&mut self, native_id: String) {
        self.resolved_native_ids.insert(native_id.clone());
        self.resolved_native_order.push_back(native_id);
        while self.resolved_native_order.len() > RESOLVED_REQUEST_HISTORY {
            if let Some(expired) = self.resolved_native_order.pop_front() {
                self.resolved_native_ids.remove(&expired);
            }
        }
    }

    fn remember_dropped(&mut self, native_id: String, observed: u64) {
        self.dropped_native_at.insert(native_id.clone(), observed);
        self.dropped_native_order.push_back(native_id);
        while self.dropped_native_order.len() > RESOLVED_REQUEST_HISTORY {
            if let Some(expired) = self.dropped_native_order.pop_front() {
                self.dropped_native_at.remove(&expired);
            }
        }
    }

    async fn resolve_approval(&mut self, params: Value) -> Result<()> {
        let notification: NativeApprovalResolvedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse approval/resolved did not match the 1.4 schema",
                )
            })?;
        let session_id = self.status.session_id.as_deref().ok_or_else(|| {
            Error::new("unknown", "Muse approval resolution has no owned session")
        })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse approval resolution named another native session")
                .await;
        }
        let native_id = notification.approval_id.as_str();
        if self.resolved_native_ids.contains(native_id) {
            return Ok(());
        }
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.expected_requests.remove(native_id) {
                // A resume orphan settled before its re-issued open arrived:
                // the stream won, nothing was ever opened, nothing is
                // committed.
                self.observe();
                self.remember_resolved(native_id.to_owned());
                return Ok(());
            }
            if self.dropped_native_at.contains_key(native_id) {
                return Ok(());
            }
            return self
                .record_gap("Muse approval resolution did not match a retained open")
                .await;
        };
        let Some(entry) = self.pending_requests.get(&request_id) else {
            return self
                .record_gap("Muse approval resolution lost its pending request")
                .await;
        };
        if entry.request.kind != "approval" {
            return self
                .record_gap("Muse approval resolution did not match a retained open")
                .await;
        }
        let turn_id = entry.request.turn_id.clone();
        // `request.resolved` binds the exact dispatched Eliot reply; anything
        // else (policy/judge/timeout, or a reply this generation never wrote)
        // closes without a command binding.
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
        let commit_id = request_event_id(&request_id, kind)?;
        self.commit_event(kind, commit_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.native_index.remove(native_id);
        self.observe();
        self.remember_resolved(native_id.to_owned());
        Ok(())
    }

    /// `approval/updated`: one stage was satisfied while further requirements
    /// remain. The in-memory offer is replaced wholesale (the Store keeps the
    /// opened snapshot and has no update event) and a consumed reply resets
    /// to Available so the next stage can be decided; an uncertain reply is
    /// never reset.
    async fn refresh_approval(&mut self, params: Value) -> Result<()> {
        let raw = params.clone();
        let notification: NativeApprovalUpdatedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse approval/updated did not match the 1.4 schema",
                )
            })?;
        let session_id =
            self.status.session_id.as_deref().ok_or_else(|| {
                Error::new("unknown", "Muse approval update has no owned session")
            })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse approval update named another native session")
                .await;
        }
        let native_id = notification.approval_id.as_str();
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.resolved_native_ids.contains(native_id)
                || self.expected_requests.contains(native_id)
            {
                return Ok(());
            }
            return self
                .record_gap("Muse approval update arrived without its retained open")
                .await;
        };
        let observed = self.observe();
        let Some(entry) = self.pending_requests.get_mut(&request_id) else {
            return Ok(());
        };
        entry.observed_at = observed;
        if refresh_offer_from_raw(&mut entry.request.details, &raw).is_err() {
            return self
                .record_gap("Muse approval update omitted its refreshed offer")
                .await;
        }
        if entry.reply_state == NativeReplyState::Written {
            entry.reply_state = NativeReplyState::Available;
            entry.reply_command_id = None;
        }
        Ok(())
    }

    async fn settle_user_input(&mut self, params: Value) -> Result<()> {
        let notification: NativeUserInputSettledNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse userInput/settled did not match the 1.4 schema",
                )
            })?;
        let session_id =
            self.status.session_id.as_deref().ok_or_else(|| {
                Error::new("unknown", "Muse prompt settlement has no owned session")
            })?;
        if notification.session_id != session_id {
            return self
                .record_gap("Muse prompt settlement named another native session")
                .await;
        }
        let native_id = notification.user_input_id.as_str();
        if self.resolved_native_ids.contains(native_id) {
            return Ok(());
        }
        let Some(request_id) = self.native_index.get(native_id).cloned() else {
            if self.expected_requests.remove(native_id) {
                self.observe();
                return Ok(());
            }
            if self.dropped_native_at.contains_key(native_id) {
                return Ok(());
            }
            return self
                .record_gap("Muse prompt settlement did not match a retained open")
                .await;
        };
        let Some(entry) = self.pending_requests.get(&request_id) else {
            return self
                .record_gap("Muse prompt settlement lost its pending request")
                .await;
        };
        if entry.request.kind != "user_input" {
            return self
                .record_gap("Muse prompt settlement did not match a retained open")
                .await;
        }
        let turn_id = entry.request.turn_id.clone();
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
        let commit_id = request_event_id(&request_id, kind)?;
        self.commit_event(kind, commit_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.native_index.remove(native_id);
        self.observe();
        self.remember_resolved(native_id.to_owned());
        Ok(())
    }

    async fn observe_session_started(&mut self, params: &Value) -> Result<()> {
        let notification: NativeSessionStartedNotification = serde_json::from_value(params.clone())
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse session/started omitted its exact session id",
                )
            })?;
        if notification.session.session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Muse session/started omitted its exact session id",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|existing| existing != notification.session.session_id)
        {
            return self
                .record_gap("Muse session host announced a second session in one Agent generation")
                .await;
        }
        self.status.session_id = Some(notification.session.session_id);
        Ok(())
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse source acknowledgement is unresolved",
            ));
        }
        let native_id = format!(
            "muse:{}:session.ended",
            self.status.session_id.as_deref().unwrap_or("unresolved")
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
                "Muse event acknowledgement is unresolved",
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
                ts: None,
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
            self.status.failure = Some("Muse event batch acknowledgement is unknown".to_owned());
            self.status.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                "Muse event batch acknowledgement is unknown",
            ));
        }
        let reason = format!("{kind} could not be committed: {}", error.code);
        self.record_gap(&reason).await?;
        Err(Error::new(
            "unknown",
            "Muse native fact was not committed; no replay is allowed",
        ))
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        if self.writes_blocked {
            self.status.source_unknown = true;
            return Err(Error::new(
                "unknown",
                "Muse event acknowledgement is unresolved; no source gap was issued",
            ));
        }
        if self.gap_reported {
            return Err(Error::new(
                "unknown",
                "Muse source already has an unresolved gap",
            ));
        }
        self.status.source_unknown = true;
        self.gap_reported = true;
        self.gap_number = self.gap_number.saturating_add(1);
        let gap_id = format!(
            "muse:{}:{}:gap-{}",
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
                    "families": ["session", "turn", "message", "request"],
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
                self.status.failure = Some("Muse source gap acknowledgement is unknown".to_owned());
                Err(Error::new(
                    "unknown",
                    "Muse source gap acknowledgement is unknown",
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

fn attach_native_error(data: &mut Value, error: Option<&NativeTurnError>, policy: &SecretPolicy) {
    let Some(error) = error else { return };
    if ["native_error", "message", "truncated"]
        .into_iter()
        .any(|key| policy.contains_text(key))
    {
        return;
    }
    let Some(data) = data.as_object_mut() else {
        return;
    };
    let Ok(encoded) = serde_json::to_vec(&*data) else {
        return;
    };
    let budget = MAX_EVENT_BYTES
        .saturating_sub(encoded.len().saturating_add(r#","native_error":"#.len()))
        .min(4 * 1024);
    let mut message = policy.redact_text(&error.message);
    let mut truncated = false;
    loop {
        let diagnostic = json!({"message": &message, "truncated": truncated});
        let Ok(encoded) = serde_json::to_vec(&diagnostic) else {
            return;
        };
        if encoded.len() <= budget {
            data.insert("native_error".to_owned(), diagnostic);
            return;
        }
        if message.is_empty() {
            return;
        }
        let mut end = message.len() / 2;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        // Preserve complete masks across the host's repeated sanitization.
        let (safe, _) = policy.redact_prefix(&message.as_bytes()[..end], false);
        message = String::from_utf8_lossy(&safe).into_owned();
        truncated = true;
    }
}

fn validate_session_and_turn(status: &EventStatus, session_id: &str, turn_id: &str) -> Result<()> {
    if status.session_id.as_deref() != Some(session_id) || turn_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse notification did not belong to the owned native session",
        ));
    }
    Ok(())
}

fn event_id(session_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    let native_id = format!("muse:{session_id}:{turn_id}:{item_id}:{kind}");
    if native_id.len() > MAX_COMMAND_ID_BYTES
        || session_id.trim().is_empty()
        || item_id.trim().is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            "Muse native event identity exceeded its bound",
        ));
    }
    Ok(native_id)
}

/// The native approval/user-input id carried by raw server-request params, if
/// any. Used only to recognize resume-expected orphans; binding still goes
/// through the typed describe path.
fn orphan_request_id(method: &str, params: &Value) -> Option<String> {
    let key = match method {
        "approval/request" => "approvalId",
        "userInput/request" => "userInputId",
        _ => return None,
    };
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// Redact the free-text offer fields of a described request for the committed
/// `request.opened` event: the approval `subject` (including its `command`
/// string), `questions` and `available_choices`. The in-memory offer stays raw
/// so choice/requirement matching and re-issue comparison keep working on
/// exact native values; only the persisted copy is redacted.
fn redact_request_details_for_commit(details: &Value, policy: &SecretPolicy) -> Value {
    let mut redacted = details.clone();
    if let Some(fields) = redacted.as_object_mut() {
        for key in ["subject", "questions", "available_choices"] {
            if let Some(field) = fields.get_mut(key) {
                policy.redact_values(field);
            }
        }
    }
    redacted
}

/// The native approval/user-input id a described request answers to.
fn pending_native_id(request: &CorePending) -> Option<&str> {
    match request.kind.as_str() {
        "approval" => request.details.get("approval_id").and_then(Value::as_str),
        "user_input" => request.details.get("user_input_id").and_then(Value::as_str),
        _ => None,
    }
}

fn request_event_id(request_id: &str, kind: &str) -> Result<String> {
    let prefix = match kind {
        "request.opened" => "muse-o:",
        "request.resolved" => "muse-r:",
        "request.closed" => "muse-c:",
        _ => {
            return Err(Error::new(
                "invalid",
                "Muse request event kind is unsupported",
            ));
        }
    };
    let native_id = format!("{prefix}{request_id}");
    if native_id.len() > MAX_COMMAND_ID_BYTES || request_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse native request event identity exceeded its bound",
        ));
    }
    Ok(native_id)
}

pub(crate) fn rejected_control(reason: &str) -> Value {
    json!({"state": "rejected", "reason": reason})
}

pub(crate) fn unknown_control(reason: &str) -> Value {
    json!({"state": "unknown", "reason": reason})
}

/// Map the MSP terminal to the stored turn status. `cancelled` folds to
/// `interrupted`, matching the Codex path; an unknown wire terminal settles
/// nothing and the caller records a gap instead.
fn turn_status(terminal: &NativeTurnTerminal) -> Option<&'static str> {
    match terminal {
        NativeTurnTerminal::Completed => Some("completed"),
        NativeTurnTerminal::Failed => Some("failed"),
        NativeTurnTerminal::Cancelled => Some("interrupted"),
        NativeTurnTerminal::Unknown => None,
    }
}

fn terminal_provider_restriction(
    status: &EventStatus,
    notification: &NativeTurnCompletedNotification,
) -> Option<ProviderRestriction> {
    if notification.terminal != NativeTurnTerminal::Failed
        || status.session_id.as_deref() != Some(notification.session_id.as_str())
        || status.active_turn_id.as_deref() != Some(notification.turn_id.as_str())
        || status.active_command_id.is_none()
    {
        return None;
    }
    notification
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

fn attach_final_text(data: &mut Value, final_text: &str, failed: bool, policy: &SecretPolicy) {
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text".to_owned(),
        Value::String(final_text.to_owned()),
    );
    let exceeds_event_bound =
        serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES);
    if failed && exceeds_event_bound {
        attach_final_text_unavailable(data, policy);
        return;
    }
    *data = candidate;
}

fn attach_final_text_unavailable(data: &mut Value, policy: &SecretPolicy) {
    if ["final_text_unavailable", "event_budget"]
        .iter()
        .any(|literal| policy.contains_text(literal))
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("final_text_unavailable"))
    {
        return;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text_unavailable".to_owned(),
        Value::String("event_budget".to_owned()),
    );
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert(
            "final_text_unavailable".to_owned(),
            Value::String("event_budget".to_owned()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_mapping_cancels_to_interrupted() {
        assert_eq!(
            turn_status(&NativeTurnTerminal::Completed),
            Some("completed")
        );
        assert_eq!(turn_status(&NativeTurnTerminal::Failed), Some("failed"));
        assert_eq!(
            turn_status(&NativeTurnTerminal::Cancelled),
            Some("interrupted")
        );
        assert_eq!(turn_status(&NativeTurnTerminal::Unknown), None);
    }

    #[test]
    fn event_id_rejects_blank_or_oversize_identity() {
        assert!(event_id("", "turn", "item", "message.completed").is_err());
        assert!(event_id("session", "turn", "", "message.completed").is_err());
        let long = "x".repeat(MAX_COMMAND_ID_BYTES);
        assert!(event_id(&long, "turn", "item", "message.completed").is_err());
        assert_eq!(
            event_id("session", "turn", "item", "message.completed").unwrap(),
            "muse:session:turn:item:message.completed"
        );
    }

    #[test]
    fn failed_terminal_marks_unavailable_final_text_at_event_budget() {
        let mut data = json!({"command_id": "command", "turn_id": "turn", "status": "failed"});
        let oversize = "x".repeat(MAX_EVENT_BYTES);
        attach_final_text(&mut data, &oversize, true, &SecretPolicy::default());
        assert_eq!(data.get("final_text"), None);
        assert_eq!(
            data.get("final_text_unavailable"),
            Some(&Value::String("event_budget".to_owned()))
        );

        let mut data = json!({"command_id": "command", "turn_id": "turn", "status": "completed"});
        attach_final_text(&mut data, "final", false, &SecretPolicy::default());
        assert_eq!(
            data.get("final_text"),
            Some(&Value::String("final".to_owned()))
        );
    }

    #[test]
    fn provider_restriction_requires_the_exact_bound_failed_turn() {
        let status = EventStatus {
            session_id: Some("session-1".to_owned()),
            active_command_id: Some("command-1".to_owned()),
            active_turn_id: Some("turn-1".to_owned()),
            ..EventStatus::default()
        };
        let notification = NativeTurnCompletedNotification {
            session_id: "session-1".to_owned(),
            terminal: NativeTurnTerminal::Failed,
            turn_id: "turn-1".to_owned(),
            view_cursor: "cursor-1".to_owned(),
            error: Some(NativeTurnError {
                kind: "authRequired".to_owned(),
                message: "login expired".to_owned(),
                retryable: true,
            }),
            reason: None,
        };
        assert!(terminal_provider_restriction(&status, &notification).is_some());

        let mut other_session = notification.clone();
        other_session.session_id = "session-2".to_owned();
        assert_eq!(terminal_provider_restriction(&status, &other_session), None);
        let mut completed = notification.clone();
        completed.terminal = NativeTurnTerminal::Completed;
        assert_eq!(terminal_provider_restriction(&status, &completed), None);
    }
}
