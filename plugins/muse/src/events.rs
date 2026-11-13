#[path = "workflow_child.rs"]
mod workflow_child;

use eliot_core::{
    ContextItem, Error, ProviderRestriction, Result, SecretPolicy, SourceHealthUpdate,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    protocol::MAX_COMMAND_ID_BYTES,
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text, attach_native_error,
        event_id as shared_event_id, native_no_write, next_input_outcomes, rejected_control,
        request_event_id, unknown_control,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use self::workflow_child::{
    MAX_WORKFLOW_CHILDREN, PlannedAction, WorkflowChildKey, WorkflowChildState, binding_from_ack,
    child_event_id, complete_event_data, is_admission_rejection, is_workflow_item_frame,
    muse_child_key, register_child, should_bypass_parent_buffer, start_event_data,
};
use crate::native::{
    NativeClient, NativeFrame, StreamState, native_error_code,
    requests::{
        CorePending, describe_request, is_newer_approval_requirement, public_request_view,
        refresh_offer_from_raw, reply_for, same_approval_offer,
    },
    types::{
        NativeApprovalDecideResult, NativeApprovalResolvedNotification,
        NativeApprovalUpdatedNotification, NativeHistoryNoneReason, NativeItem,
        NativeItemCompletedNotification, NativeItemStartedNotification,
        NativeSessionClosedNotification, NativeSessionStartedNotification,
        NativeSessionViewHealthChangedNotification, NativeTurnCompletedNotification,
        NativeTurnError, NativeTurnStartedNotification, NativeTurnTerminal,
        NativeUserInputAnswerResult, NativeUserInputCancelResult,
        NativeUserInputSettledNotification, NativeViewHealth, NativeViewSubscribeParams,
        NativeViewSubscribeResult, NativeWorkflowChild, NativeWorkflowItemNotification,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 128;
const RESOLVED_REQUEST_HISTORY: usize = 128;

struct ViewRecovery {
    head: Option<String>,
    head_seen: bool,
    deadline: Instant,
}

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
    /// Lost live view; a private history read cannot re-arm that producer.
    pub view_unavailable: bool,
    pub view_unavailable_reason: Option<NativeHistoryNoneReason>,
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
    ViewReadback {
        session_id: String,
        reason: Option<NativeHistoryNoneReason>,
        reply: oneshot::Sender<Result<EventStatus>>,
    },
    Ready {
        native_scope: String,
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateTurn {
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
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
    CheckTurn {
        turn_id: String,
        reply: oneshot::Sender<bool>,
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
    /// Native `commandId` of the outstanding reply while Writing, retained
    /// while its outcome stays Unknown so a later `approval/resolved` can
    /// prove (via `decidedByCommandId`) that this exact decide applied.
    /// Retained on every Unknown outcome (admitted-unproven and ambiguous
    /// alike); cleared only when the entry returns to Available or settles
    /// as Written for a non-decide reply.
    reply_native_command_id: Option<String>,
    reply_state: NativeReplyState,
    /// Observation counter at the last stream update of this entry.
    observed_at: u64,
}

/// Exact binding captured when an `approval/decide` is written, used to
/// qualify the control result against authoritative view frames. Generation
/// rides on the live turn: the captured turn is active at send time, and
/// only frames naming this exact session, turn, and approval can qualify.
#[derive(Clone)]
struct DecideCapture {
    session_id: String,
    turn_id: String,
    approval_id: String,
    decided_seq: i64,
    native_command_id: String,
}

/// Whitelisted view diagnostics for a decide qualification: whether a
/// terminal resolution for this exact decide was observed, and whether it
/// carried a decider id that matched ours. Booleans only, never payload.
#[derive(Clone, Default)]
struct DecideViewDiag {
    resolved_seen: bool,
    decider_present: bool,
    decider_match: bool,
    resolved_decisive: bool,
}

/// Exact decider proof: the resolution names this generation's outstanding
/// decide command id, non-empty on both sides. Anything else — missing,
/// blank, or another decider — proves nothing about our write.
fn resolved_decider_matches(decided_by: Option<&str>, ours: &str) -> bool {
    !ours.trim().is_empty() && decided_by.is_some_and(|id| !id.trim().is_empty() && id == ours)
}

/// How the decide RPC settled before view qualification: a clean admission
/// ack still needs view proof, while an ambiguous outcome additionally
/// carries the RPC error code and the reason to keep if unproven.
enum DecideRpcOutcome<'a> {
    Admitted,
    Ambiguous {
        error_code: Option<i64>,
        unproven_reason: &'a str,
    },
}

/// One decide awaiting view qualification: the exact sent binding plus the
/// control identities the result is settled under.
struct DecideQualification<'a> {
    command_id: &'a str,
    request_id: &'a str,
    turn_id: &'a str,
    capture: &'a DecideCapture,
    deadline: Instant,
    rpc: DecideRpcOutcome<'a>,
}

/// Attach the whitelisted decide diagnostics to a control outcome: the
/// numeric native error code when the RPC itself errored, and the
/// resolved-decider booleans when a terminal resolution was observed.
/// Nothing else is ever attached: no raw errors, payloads, or ids.
fn with_decide_diag(mut outcome: Value, error_code: Option<i64>, diag: &DecideViewDiag) -> Value {
    if let Some(fields) = outcome.as_object_mut() {
        if let Some(code) = error_code {
            fields.insert("native_error_code".to_owned(), Value::from(code));
        }
        if diag.resolved_seen {
            fields.insert(
                "resolved_decider_present".to_owned(),
                Value::from(diag.decider_present),
            );
            fields.insert(
                "resolved_decider_match".to_owned(),
                Value::from(diag.decider_match),
            );
        }
    }
    outcome
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
    loss_reason: Option<String>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    /// The native transport itself was lost (queue overflow, unplanned close,
    /// malformed frame, unplanned reader end): facts may be missing, so the
    /// source is marked `stale` once drained, alongside any recorded gap.
    transport_loss: bool,
    /// The one accompanying `stale` update was attempted (never replayed).
    source_health_attempted: bool,
    /// Advance only after successful processing/commit on this actor.
    last_view_cursor: Option<String>,
    view_recovery: Option<ViewRecovery>,
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
    /// Native scope bound at readiness: workflow-child registration needs the
    /// exact scope the parent session committed.
    native_scope: Option<String>,
    /// Workflow-child tracking by canonical key K. Children outlive the
    /// parent turn: parent completion never clears this map.
    workflow_children: HashMap<WorkflowChildKey, WorkflowChildState>,
    /// Bounded unknown markers for children whose observation was lost
    /// (overflow, rejected registration, stream end). Repeats stay silent;
    /// eviction only re-gaps, never re-hides a proven tracked child.
    workflow_unknown: HashSet<WorkflowChildKey>,
    workflow_unknown_order: VecDeque<WorkflowChildKey>,
    /// Active decide qualification while `pump_decide_proof` pumps frames.
    /// Owning paths (`resolve_approval`, `refresh_approval`,
    /// `process_request`) set the frame proof/diag fields below only after
    /// their own successful validation and required persistence; the pump
    /// reads them only after `handle_frame` returns. `None` outside a pump.
    active_decide: Option<DecideCapture>,
    active_decide_admission: bool,
    frame_decide_proof: bool,
    frame_decide_decisive: bool,
    frame_decide_diag: DecideViewDiag,
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
    pub async fn view_readback(
        &self,
        session_id: String,
        reason: Option<NativeHistoryNoneReason>,
    ) -> Result<EventStatus> {
        self.send(|reply| Control::ViewReadback {
            session_id,
            reason,
            reply,
        })
        .await
    }

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

    /// Authoritative exact-turn gate for steer/interrupt: the check runs
    /// on the event task, serialized behind the terminal commit, so a
    /// control that arrives after the terminal batch always sees the
    /// finished turn even though the watch snapshot still lags it.
    pub async fn check_turn(&self, turn_id: &str) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + EVENT_DEADLINE,
            self.control.send(Control::CheckTurn {
                turn_id: turn_id.to_owned(),
                reply,
            }),
        )
        .await
        .map_err(|_| Error::new("unknown", "Muse event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "Muse event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Muse event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Muse event-control response was lost"))
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

    async fn send<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> Control,
    ) -> Result<T> {
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
            transport_loss: false,
            source_health_attempted: false,
            last_view_cursor: None,
            view_recovery: None,
            final_text: None,
            pending_requests: HashMap::new(),
            native_index: HashMap::new(),
            resolved_native_ids: HashSet::new(),
            resolved_native_order: VecDeque::new(),
            dropped_native_at: HashMap::new(),
            dropped_native_order: VecDeque::new(),
            expected_requests: HashSet::new(),
            stashed_entries: Vec::new(),
            native_scope: None,
            workflow_children: HashMap::new(),
            workflow_unknown: HashSet::new(),
            workflow_unknown_order: VecDeque::new(),
            active_decide: None,
            active_decide_admission: false,
            frame_decide_proof: false,
            frame_decide_decisive: false,
            frame_decide_diag: DecideViewDiag::default(),
        }
    }

    fn observe(&mut self) -> u64 {
        self.status.observation = self.status.observation.saturating_add(1);
        self.status.observation
    }

    async fn handle_control(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        control: Control,
    ) -> bool {
        match control {
            Control::ViewReadback {
                session_id,
                reason,
                reply,
            } => {
                let result = self.bind_view_session(&session_id);
                let result = match result {
                    Ok(()) => self.flush_buffered_health().await,
                    Err(error) => Err(error),
                };
                let result = match (result, reason) {
                    (Ok(()), Some(reason)) => self.view_unavailable(&session_id, reason).await,
                    (result, _) => result,
                };
                let result = result.and_then(|()| {
                    if self.status.session_id.as_deref() != Some(session_id.as_str())
                        || self.status.view_unavailable
                    {
                        Err(Error::new(
                            "unknown",
                            "Muse native observation is unresolved",
                        ))
                    } else {
                        Ok(self.status.clone())
                    }
                });
                let _ = reply.send(result);
                true
            }
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
                context,
                reply,
            } => {
                let result = self.associate_turn(command_id, turn_id, context).await;
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
                    .reply_request(frames, &command_id, &request_id, &turn_id, &verb, &args)
                    .await;
                let _ = reply.send(result);
                true
            }
            Control::CheckTurn { turn_id, reply } => {
                let _ = reply.send(self.status.active_turn_id.as_deref() == Some(turn_id.as_str()));
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
                    self.transport_loss = true;
                }
            }
            StreamState::Overflow => {
                self.defer_gap("Muse session host event queue overflowed");
                self.transport_loss = true;
            }
            StreamState::Malformed(fault) => {
                self.defer_gap(format!(
                    "Muse session host stream fault ({})",
                    fault.label()
                ));
                self.transport_loss = true;
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
                    self.invalidate_view_recovery();
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
                // Workflow children ride the parent stream but outlive the
                // parent turn: route them before active-parent-turn buffering.
                let bypass_buffer = should_bypass_parent_buffer(
                    self.status.ready,
                    self.status.session_id.is_some(),
                    &method,
                    &params,
                );
                if !bypass_buffer && self.should_buffer(&method) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(method, params).await {
                    self.invalidate_view_recovery();
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
                if !self.planned_stop {
                    self.transport_loss = true;
                }
                self.settle_workflow_children().await;
                self.report_gap_if_queue_empty(true).await;
                self.finish_pending_end_stop().await;
            }
        }
    }

    fn defer_gap(&mut self, reason: impl Into<String>) {
        self.invalidate_view_recovery();
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
            let health_deadline = Instant::now() + EVENT_DEADLINE;
            let _ = self
                .stream_source()
                .record_gap_until(&reason, health_deadline)
                .await;
            if self.transport_loss {
                self.mark_source_stale(health_deadline).await;
            }
            return;
        }
        if queue_empty && self.transport_loss {
            self.mark_source_stale(Instant::now() + EVENT_DEADLINE)
                .await;
        }
    }

    fn should_buffer(&self, method: &str) -> bool {
        if method == "session/viewHealthChanged" {
            return self.status.session_id.is_none();
        }
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
        self.native_scope = Some(native_scope.clone());
        self.flush_buffered_health().await?;
        if self.status.view_unavailable {
            return Err(Error::new(
                "unknown",
                "Muse native view is unavailable before readiness",
            ));
        }
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

    async fn associate_turn(
        &mut self,
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
    ) -> Result<()> {
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
        let mut data = json!({"command_id": command_id, "turn_id": turn_id});
        if !context.is_empty() {
            data["context_outcomes"] = json!(next_input_outcomes(&context, &command_id));
        }
        self.commit_event("turn.started", native_id, data).await?;
        self.flush_buffer().await?;
        self.apply_stashed_entries().await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        self.flush_buffered_health().await?;
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            let should_buffer = match &frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    !should_bypass_parent_buffer(
                        self.status.ready,
                        self.status.session_id.is_some(),
                        method,
                        params,
                    ) && self.should_buffer(method)
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
            self.process_buffered_frame(frame.frame).await?;
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    /// Session health is independent of turn association. Keep every request
    /// and other deferred frame in its original order, including on errors.
    async fn flush_buffered_health(&mut self) -> Result<()> {
        if self.status.session_id.is_none() {
            return Ok(());
        }
        while let Some(index) = self.buffer.iter().position(|frame| {
            matches!(
                &frame.frame, BufferedNativeFrame::Notification { method, .. }
                    if method == "session/viewHealthChanged"
            )
        }) {
            let frame = self
                .buffer
                .remove(index)
                .ok_or_else(|| Error::new("internal", "Muse buffered health frame disappeared"))?;
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            self.process_buffered_frame(frame.frame).await?;
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    async fn process_buffered_frame(&mut self, frame: BufferedNativeFrame) -> Result<()> {
        let result = match frame {
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
        let session_id_owned = session_id.to_owned();
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
                let gap_reason = native_request_gap_reason(
                    &method,
                    &error.code,
                    &params,
                    session_id,
                    turn_id,
                    self.status.active_turn_id.is_some(),
                );
                return self
                    .reject_request(id, &method, &params, error, gap_reason)
                    .await;
            }
            Err(error) => {
                let gap_reason = native_request_gap_reason(
                    &method,
                    &error.code,
                    &params,
                    session_id,
                    turn_id,
                    self.status.active_turn_id.is_some(),
                );
                return self
                    .reject_request(id, &method, &params, error, gap_reason)
                    .await;
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
            let (prior, newer) = self
                .pending_requests
                .get(&request_id)
                .map(|existing| {
                    (
                        existing.reply_state,
                        !same && is_newer_approval_requirement(&existing.request.details, &params),
                    )
                })
                .unwrap_or((NativeReplyState::Available, false));
            if same || !newer {
                if let Some(existing) = self.pending_requests.get_mut(&request_id) {
                    existing.observed_at = observed;
                }
                if let Some(native_id) = orphan_request_id(&method, &params) {
                    self.expected_requests.remove(native_id.as_str());
                }
                return Ok(());
            }
            let refreshed = if let Some(existing) = self.pending_requests.get_mut(&request_id) {
                existing.observed_at = observed;
                if refresh_offer_from_raw(&mut existing.request.details, &params).is_err() {
                    None
                } else {
                    if existing.reply_state == NativeReplyState::Written {
                        existing.reply_state = NativeReplyState::Available;
                        existing.reply_command_id = None;
                        existing.reply_native_command_id = None;
                    }
                    Some(existing.request.clone())
                }
            } else {
                None
            };
            let Some(refreshed) = refreshed else {
                return self
                    .record_gap("Muse re-issued approval omitted its refreshed offer")
                    .await;
            };
            if matches!(
                prior,
                NativeReplyState::Available | NativeReplyState::Written
            ) && !self.commit_offer_update(&refreshed).await?
            {
                return Ok(());
            }
            if let (Some(seq), Some(approval_id)) = (
                refreshed.details.get("offer_seq").and_then(Value::as_i64),
                refreshed.details.get("approval_id").and_then(Value::as_str),
            ) {
                self.note_decide_offer_proof(
                    session_id_owned.as_str(),
                    refreshed.turn_id.as_str(),
                    approval_id,
                    seq,
                );
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
        let Ok((public_kind, public_details)) = public_request_view(&request) else {
            self.defer_gap("Muse public request view could not be normalized");
            return Ok(());
        };
        let data = json!({
            "command_id": command_id,
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse normalized request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&request_id, "request.opened", "muse", "Muse") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.defer_gap("Muse native request event identity exceeded its bound");
                return Ok(());
            }
        };
        self.commit_event("request.opened", native_id, data).await?;
        let open_proof: Option<(String, i64, String)> = if request.kind == "approval" {
            match (
                request.details.get("offer_seq").and_then(Value::as_i64),
                request.details.get("approval_id").and_then(Value::as_str),
            ) {
                (Some(seq), Some(approval_id)) => {
                    Some((request.turn_id.clone(), seq, approval_id.to_owned()))
                }
                _ => None,
            }
        } else {
            None
        };
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
                reply_native_command_id: None,
                reply_state: NativeReplyState::Available,
                observed_at: observed,
            },
        );
        if let Some((open_turn, seq, approval_id)) = open_proof {
            self.note_decide_offer_proof(
                session_id_owned.as_str(),
                open_turn.as_str(),
                approval_id.as_str(),
                seq,
            );
        }
        Ok(())
    }

    async fn reject_request(
        &mut self,
        id: Value,
        method: &str,
        params: &Value,
        error: Error,
        gap_reason: String,
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
            self.defer_gap(gap_reason);
        }
        Ok(())
    }

    /// Pump live native frames through the existing frame path while an
    /// `approval/decide` control result stays unqualified, watching for the
    /// authoritative view proof of this exact decide: a terminal resolution
    /// naming our native command id, or — only after a clean admission ack —
    /// an offer that advanced past our decided requirement. Every drained
    /// frame is processed normally in arrival order (`handle_frame` first);
    /// nothing is dropped and no pre-parse of the frame is consulted.
    /// Proof is recorded only by the owning paths (`resolve_approval`,
    /// `refresh_approval`, `process_request`) after their own successful
    /// validation and required persistence, and read only after that frame
    /// has been handled. A rejected frame, a gap, or a failed persistence
    /// therefore never counts as Applied. Returns true once the decide is
    /// proven applied. The single absolute deadline (captured before the
    /// write, never renewed) and stream EOF both end the wait unproven.
    async fn pump_decide_proof(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        capture: &DecideCapture,
        deadline: Instant,
        admission_proven: bool,
        diag: &mut DecideViewDiag,
    ) -> bool {
        self.active_decide = Some(capture.clone());
        self.active_decide_admission = admission_proven;
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        self.frame_decide_diag = DecideViewDiag::default();
        let proven = 'outer: loop {
            while Instant::now() < deadline {
                let Ok(frame) = frames.try_recv() else {
                    break;
                };
                let frame_proven = self.pump_one_decide_frame(frames, frame, diag).await;
                if frame_proven {
                    break 'outer true;
                }
                if self.frame_decide_decisive {
                    break 'outer false;
                }
            }
            if Instant::now() >= deadline {
                break false;
            }
            match timeout_at(deadline, frames.recv()).await {
                Ok(Some(frame)) => {
                    let frame_proven = self.pump_one_decide_frame(frames, frame, diag).await;
                    if frame_proven {
                        break true;
                    }
                    if self.frame_decide_decisive {
                        break false;
                    }
                }
                _ => break false,
            }
        };
        self.active_decide = None;
        self.active_decide_admission = false;
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        proven
    }

    /// Handle one pumped frame in arrival order and merge any proof/diag
    /// the owning path recorded for that exact frame. Returns true only
    /// when the owning path proved this decide applied.
    async fn pump_one_decide_frame(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        frame: NativeFrame,
        diag: &mut DecideViewDiag,
    ) -> bool {
        self.frame_decide_proof = false;
        self.frame_decide_decisive = false;
        self.frame_decide_diag = DecideViewDiag::default();
        self.handle_frame(Some(frame)).await;
        self.report_gap_if_queue_empty(frames.is_empty()).await;
        if self.frame_decide_diag.resolved_seen {
            diag.resolved_seen = true;
            diag.decider_present = self.frame_decide_diag.decider_present;
            diag.decider_match = self.frame_decide_diag.decider_match;
            diag.resolved_decisive = true;
        }
        self.frame_decide_proof
    }

    /// Record non-terminal offer proof from an owning path: only with
    /// admission proven, on the exact captured session/turn/approval, and
    /// only when the owning refreshed offer is strictly past the decided
    /// requirement. Callers invoke this only after their own successful
    /// validation (and, where they persist, successful persistence).
    fn note_decide_offer_proof(
        &mut self,
        session_id: &str,
        turn_id: &str,
        approval_id: &str,
        offer_seq: i64,
    ) {
        let Some(capture) = self.active_decide.clone() else {
            return;
        };
        if !self.active_decide_admission {
            return;
        }
        if capture.session_id != session_id
            || capture.turn_id != turn_id
            || capture.approval_id != approval_id
        {
            return;
        }
        if offer_seq > capture.decided_seq {
            self.frame_decide_proof = true;
        }
    }

    /// Record terminal-resolution proof/diag from `resolve_approval`, only
    /// after its commit succeeded and the entry was removed. A resolution
    /// that does not name our native decide id is decisive the other way.
    fn note_decide_resolved(
        &mut self,
        session_id: &str,
        turn_id: &str,
        approval_id: &str,
        decided_by: Option<&str>,
        bound_to_ours: bool,
    ) {
        let Some(capture) = self.active_decide.clone() else {
            return;
        };
        if capture.session_id != session_id
            || capture.turn_id != turn_id
            || capture.approval_id != approval_id
        {
            return;
        }
        self.frame_decide_diag.resolved_seen = true;
        self.frame_decide_diag.decider_present = decided_by.is_some_and(|id| !id.trim().is_empty());
        self.frame_decide_diag.decider_match =
            resolved_decider_matches(decided_by, &capture.native_command_id);
        self.frame_decide_diag.resolved_decisive = true;
        self.frame_decide_decisive = true;
        self.frame_decide_proof = bound_to_ours && self.frame_decide_diag.decider_match;
    }

    async fn reply_request(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
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
        let decided_offer_seq = (reply.method == "approval/decide")
            .then(|| {
                params
                    .get("requirementId")
                    .and_then(|value| value.get("sourceIndex"))
                    .and_then(Value::as_i64)
            })
            .flatten();
        let is_decide = reply.method == "approval/decide";
        // View qualification needs the exact sent binding. It is read from
        // the same validated values the reply was built from, so it cannot
        // be missing; anything else leaves the request available pre-write.
        let capture: Option<DecideCapture> = if !is_decide {
            None
        } else {
            let (Some(approval_id), Some(sent_session), Some(seq)) = (
                params.get("approvalId").and_then(Value::as_str),
                params.get("sessionId").and_then(Value::as_str),
                decided_offer_seq,
            ) else {
                return Ok(rejected_control(
                    "Muse request reply binding could not be captured",
                ));
            };
            Some(DecideCapture {
                session_id: sent_session.to_owned(),
                turn_id: turn_id.to_owned(),
                approval_id: approval_id.to_owned(),
                decided_seq: seq,
                native_command_id: native_command_id.clone(),
            })
        };
        let request = entry.request.clone();
        entry.reply_state = NativeReplyState::Writing;
        entry.reply_native_command_id = Some(native_command_id.clone());
        if is_decide {
            // The view-wait below may resolve while this RPC is outstanding;
            // the outstanding Eliot command must already be recorded so the
            // resolution can bind it.
            entry.reply_command_id = Some(command_id.to_owned());
        }
        // ONE absolute deadline, captured before the write and carried
        // through the RPC and the view wait alike: never renewed.
        let deadline = Instant::now() + EVENT_DEADLINE;
        let acknowledged = match self.native.call(reply.method, params, deadline).await {
            Ok(ack) => ack,
            Err(error) if native_no_write(&error.code) || error.code == "native_rejected" => {
                // Proven not admitted (transport never wrote, or a
                // schema-documented pre-admission/validation error): the
                // request stays available for a later reply.
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
                return Ok(rejected_control("Muse request reply was not admitted"));
            }
            Err(error) => {
                let error_code = native_error_code(&error);
                if !is_decide {
                    if let Some(entry) = self.pending_requests.get_mut(request_id) {
                        entry.reply_state = NativeReplyState::Unknown;
                        entry.reply_command_id = Some(command_id.to_owned());
                    }
                    return Ok(unknown_control(
                        "Muse request reply may have been dispatched; it will not be repeated",
                    ));
                }
                let Some(capture) = capture else {
                    if let Some(entry) = self.pending_requests.get_mut(request_id) {
                        entry.reply_state = NativeReplyState::Unknown;
                        entry.reply_command_id = Some(command_id.to_owned());
                    }
                    let _ = self
                        .record_gap("Muse decide binding could not be captured")
                        .await;
                    return Ok(unknown_control(
                        "Muse request reply binding is unresolved and will not be repeated",
                    ));
                };
                return self
                    .qualify_decide(
                        frames,
                        DecideQualification {
                            command_id,
                            request_id,
                            turn_id,
                            capture: &capture,
                            deadline,
                            rpc: DecideRpcOutcome::Ambiguous {
                                error_code,
                                unproven_reason:
                                    "Muse request reply may have been dispatched; it will not be repeated",
                            },
                        },
                    )
                    .await;
            }
        };
        // Admission proves nothing by itself (the ack echoes the command
        // id, not the decision): the ack must echo the exact submitted
        // command id, the acceptance status, and the exact decided request
        // id.
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
        if !is_decide {
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
                entry.reply_native_command_id = None;
            }
            return Ok(json!({
                "state": "applied",
                "command_id": command_id,
                "request_id": request.id,
                "turn_id": request.turn_id,
            }));
        }
        let Some(capture) = capture else {
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
                entry.reply_command_id = Some(command_id.to_owned());
            }
            let _ = self
                .record_gap("Muse decide binding could not be captured")
                .await;
            return Ok(unknown_control(
                "Muse request reply binding is unresolved and will not be repeated",
            ));
        };
        // A decide is Applied only on authoritative view proof, never from
        // the admission ack alone; a malformed ack qualifies like an
        // ambiguous RPC error (a resolution naming our decide still proves
        // the write applied).
        if acked {
            self.qualify_decide(
                frames,
                DecideQualification {
                    command_id,
                    request_id,
                    turn_id,
                    capture: &capture,
                    deadline,
                    rpc: DecideRpcOutcome::Admitted,
                },
            )
            .await
        } else {
            self.qualify_decide(
                frames,
                DecideQualification {
                    command_id,
                    request_id,
                    turn_id,
                    capture: &capture,
                    deadline,
                    rpc: DecideRpcOutcome::Ambiguous {
                        error_code: None,
                        unproven_reason:
                            "Muse request reply did not acknowledge the exact submitted command",
                    },
                },
            )
            .await
        }
    }

    /// Settle one written decide against the authoritative view stream: pump
    /// live frames through the existing frame path until this exact decide is
    /// proven applied, proven otherwise decided, the stream ends, or the
    /// single carried deadline expires. Proven terminal proof removed the
    /// entry via the pumped resolution; proven nonterminal proof only
    /// refreshed it, so the stage is consumed here (mirroring the update
    /// reset, which cannot fire while Writing) and the consumed offer is
    /// persisted for the owner projection and the stage gate; if that
    /// required persistence fails or gaps, the result is Unknown, never
    /// Applied. Unproven outcomes stay honest and fenced: an
    /// admitted-but-unproven write stays Unknown with both the Core and
    /// native bindings retained, so only a late resolution naming this
    /// exact native decide id can bind it (otherwise it closes unbound);
    /// an ambiguous write likewise stays fenced unknown. Never resends,
    /// never rearms Unknown.
    async fn qualify_decide(
        &mut self,
        frames: &mut mpsc::Receiver<NativeFrame>,
        qualification: DecideQualification<'_>,
    ) -> Result<Value> {
        let DecideQualification {
            command_id,
            request_id,
            turn_id,
            capture,
            deadline,
            rpc,
        } = qualification;
        let admission_proven = matches!(rpc, DecideRpcOutcome::Admitted);
        let mut diag = DecideViewDiag::default();
        let proven = self
            .pump_decide_proof(frames, capture, deadline, admission_proven, &mut diag)
            .await;
        if proven {
            if let Some(refreshed) = self
                .pending_requests
                .get(request_id)
                .map(|entry| entry.request.clone())
            {
                let live_turn =
                    self.status.active_turn_id.as_deref() == Some(refreshed.turn_id.as_str());
                if live_turn {
                    // The pumped update could not persist while Writing, so
                    // this is the required persistence for non-terminal
                    // proof (terminal proof already removed the entry and
                    // persisted its resolution in the owning path). A
                    // failed or gapped persistence is never Applied.
                    let persisted = matches!(self.commit_offer_update(&refreshed).await, Ok(true));
                    if !persisted {
                        if let Some(entry) = self.pending_requests.get_mut(request_id) {
                            entry.reply_state = NativeReplyState::Unknown;
                            entry.reply_command_id = Some(command_id.to_owned());
                        }
                        let error_code = match rpc {
                            DecideRpcOutcome::Admitted => None,
                            DecideRpcOutcome::Ambiguous { error_code, .. } => error_code,
                        };
                        return Ok(with_decide_diag(
                            unknown_control(
                                "Muse consumed offer update was not persisted; it will not be repeated",
                            ),
                            error_code,
                            &diag,
                        ));
                    }
                }
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
            }
            let outcome = json!({
                "state": "applied",
                "command_id": command_id,
                "request_id": request_id,
                "turn_id": turn_id,
                "decided_offer_seq": capture.decided_seq,
            });
            let error_code = match rpc {
                DecideRpcOutcome::Admitted => None,
                DecideRpcOutcome::Ambiguous { error_code, .. } => error_code,
            };
            return Ok(with_decide_diag(outcome, error_code, &diag));
        }
        match rpc {
            DecideRpcOutcome::Admitted => {
                // Unproven after admission: stay Unknown with both the Core
                // command id and the native decide id retained. Storing
                // Written and clearing the native id would let a later
                // missing/foreign `decidedByCommandId` bind this write at
                // resolution time; Unknown requires exact late proof and
                // otherwise closes unbound. Never replayed or rearmed.
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(with_decide_diag(
                    unknown_control(
                        "Muse request reply was admitted but its decision is unproven; it will not be repeated",
                    ),
                    None,
                    &diag,
                ))
            }
            DecideRpcOutcome::Ambiguous {
                error_code,
                unproven_reason,
            } => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                Ok(with_decide_diag(
                    unknown_control(unproven_reason),
                    error_code,
                    &diag,
                ))
            }
        }
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
        let Ok((public_kind, public_details)) = public_request_view(&entry) else {
            self.defer_gap("Muse public request view could not be normalized");
            return Ok(());
        };
        let data = json!({
            "command_id": command_id,
            "request_id": entry.id,
            "turn_id": entry.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse pulled request exceeded the persisted-event bound");
            return Ok(());
        }
        let native_id = match request_event_id(&entry.id, "request.opened", "muse", "Muse") {
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
                reply_native_command_id: None,
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
        let cursor = self
            .status
            .session_id
            .as_deref()
            .is_some_and(|session_id| {
                params.get("sessionId").and_then(Value::as_str) == Some(session_id)
            })
            .then(|| params.get("viewCursor").and_then(Value::as_str))
            .flatten()
            .filter(|cursor| !cursor.trim().is_empty())
            .map(str::to_owned);
        if let Err(error) = self.apply_notification(method, params).await {
            self.invalidate_view_recovery();
            return Err(error);
        }
        if let Some(cursor) = cursor {
            let live = self.view_recovery.as_mut().is_some_and(|recovery| {
                if recovery.head.as_ref() == Some(&cursor) {
                    recovery.head_seen = true;
                    false
                } else {
                    recovery.head.is_some() && recovery.head_seen
                }
            });
            self.last_view_cursor = Some(cursor);
            if live && !self.writes_blocked {
                if self
                    .set_source_health("ok", Instant::now() + EVENT_DEADLINE)
                    .await
                {
                    self.view_recovery = None;
                    self.status.view_unavailable = false;
                    self.status.view_unavailable_reason = None;
                    self.status.source_unknown = false;
                    self.status.failure = None;
                    self.gap_reported = false;
                    self.source_health_attempted = false;
                } else {
                    self.invalidate_view_recovery();
                }
            }
        }
        Ok(())
    }

    async fn apply_notification(&mut self, method: String, params: Value) -> Result<()> {
        if is_workflow_item_frame(&method, &params) {
            return self.observe_workflow_item(params).await;
        }
        match method.as_str() {
            "session/viewHealthChanged" => {
                let notification: NativeSessionViewHealthChangedNotification =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse view-health notification did not match its schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str())
                    && notification.health == NativeViewHealth::Unavailable
                {
                    self.view_unavailable(
                        &notification.session_id,
                        notification
                            .none_reason
                            .unwrap_or(NativeHistoryNoneReason::Unknown),
                    )
                    .await?;
                }
            }
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
                self.start_tool(&notification.item).await?;
            }
            "item/updated" => {
                // Full re-emission at a higher revision; no per-delta event
                // is folded. The authoritative terminal object from
                // `item/completed` carries every committed tool fact, so an
                // update alone normalizes nothing.
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
                } else {
                    self.complete_tool(&notification.item).await?;
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
                    self.invalidate_view_recovery();
                    return self
                        .stream_source()
                        .record_gap_range_until(
                            "Muse view stream reported a delivery gap",
                            Some(notification.after),
                            Some(notification.next),
                            Instant::now() + EVENT_DEADLINE,
                        )
                        .await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Items outside the exact active turn are gaps. A turn-less item (only
    /// `userShell`) carries no Eliot turn binding and stays unmapped; a
    /// turn-bound `userShell` normalizes like `toolCall` under its kind.
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

    /// Commit `tool.started` for a turn-bound tool item, mirroring the Codex
    /// adapter's start DTO (`command_id`, `turn_id`, `item_id`, `tool`).
    /// Tool calls, command execution and edit-family calls all arrive as
    /// `toolCall`; a turn-bound `userShell` normalizes under its kind. Only
    /// the tool identity is persisted: `callId`, `args` and result payloads
    /// never leave the native frame.
    async fn start_tool(&mut self, item: &NativeItem) -> Result<()> {
        let Some(tool) = tool_label(item) else {
            if tool_item_kind(&item.kind) {
                return self
                    .record_gap("Muse tool item omitted its tool name")
                    .await;
            }
            return Ok(());
        };
        let Some((command_id, turn_id)) = self.tool_binding(item)? else {
            // Turn-less `userShell` carries no Eliot turn binding and stays
            // unmapped, as before.
            return Ok(());
        };
        if item.item_id.trim().is_empty() {
            return self
                .record_gap("Muse tool start omitted its exact item id")
                .await;
        }
        let data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": item.item_id,
            "tool": tool,
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("Muse tool start exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &turn_id,
            &item.item_id,
            "tool.started",
        )?;
        self.commit_event("tool.started", native_id, data).await
    }

    /// Commit `tool.completed` for the terminal revision of a turn-bound
    /// tool item, mirroring the Codex adapter's completion DTO (the start
    /// fields plus the verbatim native `status`). Non-terminal revisions
    /// carry no committed fact; their terminal `item/completed` follows.
    /// The 1.4 item carries no inline workspace paths (`patchRef` names a
    /// stored patch document whose body is fetched out of band, never in
    /// the stream), so no `changed_paths` are attached here.
    async fn complete_tool(&mut self, item: &NativeItem) -> Result<()> {
        let Some(tool) = tool_label(item) else {
            if tool_item_kind(&item.kind) {
                return self
                    .record_gap("Muse tool item omitted its tool name")
                    .await;
            }
            return Ok(());
        };
        if !is_terminal_item_status(&item.status) {
            return Ok(());
        }
        let Some((command_id, turn_id)) = self.tool_binding(item)? else {
            // Turn-less `userShell` carries no Eliot turn binding and stays
            // unmapped, as before.
            return Ok(());
        };
        if item.item_id.trim().is_empty() {
            return self
                .record_gap("Muse tool completion omitted its exact item id")
                .await;
        }
        let data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": item.item_id,
            "tool": tool,
            "status": item.status,
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("Muse tool completion exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &turn_id,
            &item.item_id,
            "tool.completed",
        )?;
        self.commit_event("tool.completed", native_id, data).await
    }

    /// The exact Eliot turn/command a tool item binds to, or `None` for a
    /// turn-less `userShell` (the one kind outside any turn), which stays
    /// unmapped. `validate_item_turn` already proved the session and the
    /// exact active turn; anything else here is a binding failure.
    fn tool_binding(&self, item: &NativeItem) -> Result<Option<(String, String)>> {
        let turn_id = item.turn_id.as_deref().filter(|turn| !turn.is_empty());
        let Some(turn_id) = turn_id else {
            if item.kind == "userShell" {
                return Ok(None);
            }
            return Err(Error::new(
                "source_integrity",
                "Muse toolCall omitted its turn binding",
            ));
        };
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse tool item was outside the exact active turn",
            ));
        }
        let command_id =
            self.status.active_command_id.clone().ok_or_else(|| {
                Error::new("unknown", "Muse tool item has no exact Eliot command")
            })?;
        Ok(Some((command_id, turn_id.to_owned())))
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
        attach_native_error(
            &mut data,
            notification
                .error
                .as_ref()
                .map(|error| error.message.as_str()),
            &secrets,
        );
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
        // Parent completion keeps child tracking: workflow children outlive
        // the parent turn and settle on their own terminal revisions.
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
        // The wire turn must name the exact retained turn before anything
        // is committed, removed, or recorded as proof: a resolution for
        // another turn never qualifies and never mutates this entry.
        if notification.turn_id != entry.request.turn_id {
            return self
                .record_gap("Muse approval resolution named another turn")
                .await;
        }
        let turn_id = entry.request.turn_id.clone();
        // `request.resolved` binds the exact dispatched Eliot reply: an
        // acked decide, or a Writing/Unknown one the resolution itself names
        // via `decidedByCommandId` (resolved-before-qualifying-ACK order,
        // and late proof for an admitted-but-unproven Unknown). Anything
        // else (policy/judge/timeout, another decider, an unnamed resolution,
        // or a reply this generation never wrote) closes without a binding.
        // Writing is reachable here only from inside the decide view-wait,
        // which pumps frames while its own RPC is outstanding; anywhere else
        // the sequential loop keeps Writing and resolution apart.
        let proven_ours = matches!(
            entry.reply_state,
            NativeReplyState::Writing | NativeReplyState::Unknown
        ) && entry.reply_command_id.is_some()
            && entry
                .reply_native_command_id
                .as_deref()
                .is_some_and(|ours| {
                    resolved_decider_matches(notification.decided_by_command_id.as_deref(), ours)
                });
        let (kind, data) = match (entry.reply_state, entry.reply_command_id.as_ref()) {
            (NativeReplyState::Written, Some(command_id)) => (
                "request.resolved",
                json!({
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
            (NativeReplyState::Writing | NativeReplyState::Unknown, Some(command_id))
                if proven_ours =>
            {
                (
                    "request.resolved",
                    json!({
                        "command_id": command_id,
                        "request_id": request_id,
                        "turn_id": turn_id,
                    }),
                )
            }
            _ => (
                "request.closed",
                json!({
                    "request_id": request_id,
                    "turn_id": turn_id,
                }),
            ),
        };
        let commit_id = request_event_id(&request_id, kind, "muse", "Muse")?;
        let bound_to_ours = kind == "request.resolved";
        self.commit_event(kind, commit_id, data).await?;
        self.pending_requests.remove(&request_id);
        self.native_index.remove(native_id);
        self.observe();
        self.remember_resolved(native_id.to_owned());
        // Terminal proof for an active decide is recorded only here, after
        // the owning commit above succeeded. The helper additionally
        // requires an exact decider match, so a bound-but-unnamed or
        // foreign-decider resolution is decisive without being Applied.
        self.note_decide_resolved(
            notification.session_id.as_str(),
            notification.turn_id.as_str(),
            native_id,
            notification.decided_by_command_id.as_deref(),
            bound_to_ours,
        );
        Ok(())
    }

    /// `approval/updated`: one stage was satisfied while further requirements
    /// remain. A genuinely newer requirement replaces the in-memory offer and
    /// is persisted under the exact pending id so the owner projection exposes
    /// the current offer; a consumed reply resets to Available so the next
    /// stage can be decided. Duplicates and stale offers touch nothing but the
    /// observation counter; an uncertain reply is never reset nor persisted.
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
        let (prior, same, newer) = self
            .pending_requests
            .get(&request_id)
            .map(|entry| {
                (
                    entry.reply_state,
                    same_approval_offer(&entry.request.details, &raw),
                    is_newer_approval_requirement(&entry.request.details, &raw),
                )
            })
            .unwrap_or((NativeReplyState::Available, false, false));
        if same || !newer {
            if let Some(entry) = self.pending_requests.get_mut(&request_id) {
                entry.observed_at = observed;
            }
            return Ok(());
        }
        let refreshed = if let Some(entry) = self.pending_requests.get_mut(&request_id) {
            entry.observed_at = observed;
            if refresh_offer_from_raw(&mut entry.request.details, &raw).is_err() {
                None
            } else {
                if entry.reply_state == NativeReplyState::Written {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                    entry.reply_native_command_id = None;
                }
                Some(entry.request.clone())
            }
        } else {
            None
        };
        let Some(refreshed) = refreshed else {
            return self
                .record_gap("Muse approval update omitted its refreshed offer")
                .await;
        };
        if matches!(
            prior,
            NativeReplyState::Available | NativeReplyState::Written
        ) && !self.commit_offer_update(&refreshed).await?
        {
            return Ok(());
        }
        if let Some(seq) = refreshed.details.get("offer_seq").and_then(Value::as_i64) {
            self.note_decide_offer_proof(
                notification.session_id.as_str(),
                refreshed.turn_id.as_str(),
                native_id,
                seq,
            );
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
        let commit_id = request_event_id(&request_id, kind, "muse", "Muse")?;
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

    /// Fold one workflow-item revision: every `children[]` entry drives its
    /// canonical key K independently. One malformed entry gaps the parent
    /// source; proven siblings still fold.
    async fn observe_workflow_item(&mut self, params: Value) -> Result<()> {
        let notification: NativeWorkflowItemNotification =
            serde_json::from_value(params).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse workflow item did not match the installed schema",
                )
            })?;
        if self.status.session_id.as_deref() != Some(notification.session_id.as_str()) {
            return self
                .record_gap("Muse workflow update named another native session")
                .await;
        }
        for child in &notification.item.children {
            if let Err(error) = self.observe_workflow_child(&notification, child).await {
                self.status.source_unknown = true;
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Muse workflow child entry could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(error.message);
                }
            }
        }
        Ok(())
    }

    /// Drive one child entry to its plan. Child-scoped failures become child
    /// gaps; only a malformed binding errors to the parent gap above.
    async fn observe_workflow_child(
        &mut self,
        notification: &NativeWorkflowItemNotification,
        child: &NativeWorkflowChild,
    ) -> Result<()> {
        let key = muse_child_key(
            &notification.session_id,
            &notification.item.workflow_run_id,
            &child.child_id,
            child.attempt,
        )?;
        if self.workflow_unknown.contains(&key) {
            return Ok(());
        }
        if !self.workflow_children.contains_key(&key) {
            if self.workflow_children.len() >= MAX_WORKFLOW_CHILDREN {
                // Detailed tracking is full, but this proven child must stay
                // visible: persist it through one idempotent registration
                // plus an honest observation-unavailable child gap. No state
                // retained, no replay, no eviction, no turn facts.
                self.persist_overflow_child(&key).await;
                self.remember_workflow_unknown(key);
                return Ok(());
            }
            self.workflow_children
                .insert(key.clone(), WorkflowChildState::new());
        }
        if let Some(state) = self.workflow_children.get_mut(&key) {
            state.observe_rev(
                notification.item.revision,
                &notification.item.status,
                notification.source_range.clone(),
            );
        }
        loop {
            let planned = self
                .workflow_children
                .get(&key)
                .map(|state| state.plan(&child.status, child.terminal.as_deref()))
                .unwrap_or(PlannedAction::Ignore);
            match planned {
                PlannedAction::Ignore => return Ok(()),
                PlannedAction::ChildGap {
                    reason,
                    mark_unknown,
                } => {
                    let host = self.host.clone();
                    let credential = self.credential.clone();
                    let sink = &mut self.sink;
                    if let Some(state) = self.workflow_children.get_mut(&key) {
                        let _ = state.record_gap(sink, &host, &credential, reason).await;
                        if mark_unknown {
                            state.unknown = true;
                        }
                    }
                    return Ok(());
                }
                PlannedAction::Register => {
                    if !self.register_workflow_child(&key).await {
                        return Ok(());
                    }
                }
                PlannedAction::Start => {
                    self.start_workflow_child_turn(&key).await;
                    return Ok(());
                }
                PlannedAction::Complete { status } => {
                    self.complete_workflow_child_turn(&key, status, notification.item.revision)
                        .await;
                    return Ok(());
                }
            }
        }
    }

    /// Persist one proven over-limit child without detailed tracking: a
    /// single idempotent registration makes the Agent row visible with
    /// Unknown state, and one child-scoped gap marks turn observation
    /// unavailable. No turn facts are committed, no state is retained, and
    /// an uncertain outcome is never replayed — the unknown marker stops
    /// later revisions, and idempotent re-registration stays safe if the
    /// bounded marker ever expires. Only registration failure falls back to
    /// a parent gap, mirroring the tracked-key rejection path.
    async fn persist_overflow_child(&mut self, key: &WorkflowChildKey) {
        let Some(native_scope) = self.native_scope.clone() else {
            let _ = self
                .record_gap("Muse workflow child registration lacks its native scope")
                .await;
            return;
        };
        let registration =
            match key.build_registration(&self.agent_id, self.generation, &native_scope) {
                Ok(registration) => registration,
                Err(_) => {
                    let _ = self
                        .record_gap("Muse workflow child registration could not be built")
                        .await;
                    return;
                }
            };
        let ack = match register_child(&self.host, &self.credential, registration.clone()).await {
            Ok(ack) => ack,
            Err(error) if is_admission_rejection(&error) => {
                let _ = self
                    .record_gap("Muse workflow child registration was rejected")
                    .await;
                return;
            }
            Err(_) => {
                let _ = self
                    .record_gap("Muse workflow child registration outcome is unknown")
                    .await;
                return;
            }
        };
        let mut ephemeral = WorkflowChildState::new();
        ephemeral.binding = Some(binding_from_ack(registration, &ack));
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        // Best-effort explicit unavailability: even if this RPC is lost, the
        // visible Unknown row with no turns stays honest (nothing claimed),
        // while a parent gap here would misattribute parent-source loss.
        let _ = ephemeral
            .record_gap(
                sink,
                &host,
                &credential,
                "Muse workflow-child observation unavailable: tracking exceeded its bound",
            )
            .await;
    }

    /// Register one workflow child, retrying the retained identical
    /// registration after a lost ACK. Returns true when the caller should
    /// re-plan the same revision against the new binding.
    async fn register_workflow_child(&mut self, key: &WorkflowChildKey) -> bool {
        let Some(native_scope) = self.native_scope.clone() else {
            // Readiness always binds the scope before workflow routing; a
            // missing scope retries on the next revision, never unknowns.
            let _ = self
                .record_gap("Muse workflow child registration lacks its native scope")
                .await;
            return false;
        };
        let retained = self
            .workflow_children
            .get(key)
            .and_then(|state| state.pending_registration.clone());
        let registration = match retained {
            Some(registration) => registration,
            None => match key.build_registration(&self.agent_id, self.generation, &native_scope) {
                Ok(registration) => registration,
                Err(_) => {
                    if let Some(state) = self.workflow_children.get_mut(key) {
                        state.unknown = true;
                    }
                    let _ = self
                        .record_gap("Muse workflow child registration could not be built")
                        .await;
                    return false;
                }
            },
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            state.pending_registration = Some(registration.clone());
        }
        match register_child(&self.host, &self.credential, registration.clone()).await {
            Ok(ack) => {
                let binding = binding_from_ack(registration, &ack);
                if let Some(state) = self.workflow_children.get_mut(key) {
                    state.pending_registration = None;
                    state.binding = Some(binding);
                }
                true
            }
            Err(error) if is_admission_rejection(&error) => {
                if let Some(state) = self.workflow_children.get_mut(key) {
                    state.pending_registration = None;
                    state.unknown = true;
                }
                let _ = self
                    .record_gap("Muse workflow child registration was rejected")
                    .await;
                false
            }
            Err(_) => false,
        }
    }

    /// Commit the ONE `turn.started` for a registered child. A failed commit
    /// already recorded its child gap; the key unknowns and stops.
    async fn start_workflow_child_turn(&mut self, key: &WorkflowChildKey) {
        let native_id = match child_event_id(key, "turn.started") {
            Ok(native_id) => native_id,
            Err(_) => {
                let host = self.host.clone();
                let credential = self.credential.clone();
                let sink = &mut self.sink;
                if let Some(state) = self.workflow_children.get_mut(key) {
                    let _ = state
                        .record_gap(
                            sink,
                            &host,
                            &credential,
                            "Muse workflow child turn identity exceeded its bound",
                        )
                        .await;
                    state.unknown = true;
                }
                return;
            }
        };
        let data = start_event_data(key);
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let committed = match self.workflow_children.get_mut(key) {
            Some(state) => state
                .commit_event(sink, &host, &credential, "turn.started", native_id, data)
                .await
                .is_ok(),
            None => return,
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            if committed {
                state.started = true;
            } else {
                state.unknown = true;
            }
        }
    }

    /// Commit the ONE `turn.completed`, starting first when the live states
    /// were never observed: an explicit terminal proves the turn ran.
    async fn complete_workflow_child_turn(
        &mut self,
        key: &WorkflowChildKey,
        status: &'static str,
        revision: i64,
    ) {
        if self
            .workflow_children
            .get(key)
            .is_some_and(|state| !state.started)
        {
            self.start_workflow_child_turn(key).await;
            if self
                .workflow_children
                .get(key)
                .is_some_and(|state| !state.started)
            {
                return;
            }
        }
        let native_id = match child_event_id(key, "turn.completed") {
            Ok(native_id) => native_id,
            Err(_) => {
                let host = self.host.clone();
                let credential = self.credential.clone();
                let sink = &mut self.sink;
                if let Some(state) = self.workflow_children.get_mut(key) {
                    let _ = state
                        .record_gap(
                            sink,
                            &host,
                            &credential,
                            "Muse workflow child turn identity exceeded its bound",
                        )
                        .await;
                    state.unknown = true;
                }
                return;
            }
        };
        let data = complete_event_data(key, status);
        let host = self.host.clone();
        let credential = self.credential.clone();
        let sink = &mut self.sink;
        let committed = match self.workflow_children.get_mut(key) {
            Some(state) => state
                .commit_event(sink, &host, &credential, "turn.completed", native_id, data)
                .await
                .is_ok(),
            None => return,
        };
        if let Some(state) = self.workflow_children.get_mut(key) {
            if committed {
                state.completed_status = Some(status.to_owned());
                state.completed_revision = Some(revision);
            } else {
                state.unknown = true;
            }
        }
    }

    /// Stream end settles child tracking: completed children keep their
    /// terminal, open children go unknown — their terminal can never be
    /// observed now. A pending registration is a parent gap.
    async fn settle_workflow_children(&mut self) {
        let open: Vec<WorkflowChildKey> = self
            .workflow_children
            .iter()
            .filter(|entry| entry.1.completed_status.is_none() && !entry.1.unknown)
            .map(|entry| entry.0.clone())
            .collect();
        if open.is_empty() {
            return;
        }
        let mut pending_lost = false;
        for key in &open {
            if let Some(state) = self.workflow_children.get_mut(key) {
                if state.pending_registration.is_some() {
                    state.pending_registration = None;
                    pending_lost = true;
                }
                state.unknown = true;
            }
        }
        for key in &open {
            let running = self.workflow_children.get(key).is_some_and(|state| {
                state.started && state.completed_status.is_none() && state.binding.is_some()
            });
            if !running {
                continue;
            }
            let host = self.host.clone();
            let credential = self.credential.clone();
            let sink = &mut self.sink;
            if let Some(state) = self.workflow_children.get_mut(key) {
                let _ = state
                    .record_gap(
                        sink,
                        &host,
                        &credential,
                        "Muse workflow child observation ended before its terminal",
                    )
                    .await;
            }
        }
        if pending_lost {
            let _ = self
                .record_gap("Muse workflow child registration acknowledgement is unresolved")
                .await;
        }
    }

    fn remember_workflow_unknown(&mut self, key: WorkflowChildKey) {
        if self.workflow_unknown.insert(key.clone()) {
            self.workflow_unknown_order.push_back(key);
            while self.workflow_unknown_order.len() > MAX_WORKFLOW_CHILDREN {
                if let Some(expired) = self.workflow_unknown_order.pop_front() {
                    self.workflow_unknown.remove(&expired);
                }
            }
        }
    }

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await
    }

    /// Persist a refreshed offer. Returns `Ok(true)` only after the actual
    /// required `request.opened` commit succeeded. `Ok(false)` means the
    /// call was handled but nothing was persisted (a gap was recorded, or
    /// an oversized envelope was deferred); `Err` means the commit or gap
    /// acknowledgement itself is unknown. Callers that need persistence
    /// proof must require `Ok(true)` — never infer it from gap latches.
    async fn commit_offer_update(&mut self, refreshed: &CorePending) -> Result<bool> {
        let Some(command_id) = self.status.active_command_id.clone() else {
            self.record_gap("Muse refreshed offer arrived with no active command")
                .await?;
            return Ok(false);
        };
        let (Some(seq), Some(cursor)) = (
            refreshed.details.get("offer_seq").and_then(Value::as_i64),
            refreshed.details.get("view_cursor").and_then(Value::as_str),
        ) else {
            self.record_gap("Muse refreshed offer omitted its identity")
                .await?;
            return Ok(false);
        };
        if cursor.trim().is_empty() {
            self.record_gap("Muse refreshed offer omitted its identity")
                .await?;
            return Ok(false);
        }
        let native_id = format!("muse-o:{}:s{seq}:{cursor}", refreshed.id);
        if native_id.len() > MAX_COMMAND_ID_BYTES {
            self.record_gap("Muse refreshed offer identity exceeded its bound")
                .await?;
            return Ok(false);
        }
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let Ok((public_kind, public_details)) = public_request_view(refreshed) else {
            self.record_gap("Muse refreshed public offer could not be normalized")
                .await?;
            return Ok(false);
        };
        let data = json!({
            "command_id": command_id,
            "request_id": refreshed.id,
            "turn_id": refreshed.turn_id,
            "kind": public_kind,
            "details": redact_request_details_for_commit(&public_details, &policy),
        });
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            self.defer_gap("Muse refreshed offer exceeded the persisted-event bound");
            return Ok(false);
        }
        self.commit_event("request.opened", native_id, data).await?;
        Ok(true)
    }

    fn bind_view_session(&mut self, session_id: &str) -> Result<()> {
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

    async fn view_unavailable(
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
        let subscribe_deadline = Instant::now() + EVENT_DEADLINE;
        let head = if can_recover && let Some(after) = after.as_ref() {
            let params = NativeViewSubscribeParams {
                session_id: session_id.to_owned(),
                after: after.clone(),
            };
            match serde_json::to_value(params) {
                Ok(params) => self
                    .native
                    .call("view/subscribe", params, subscribe_deadline)
                    .await
                    .ok()
                    .and_then(|value| {
                        serde_json::from_value::<NativeViewSubscribeResult>(value).ok()
                    })
                    .map(|result| result.view_cursor)
                    .filter(|cursor| !cursor.trim().is_empty()),
                Err(_) => None,
            }
        } else {
            None
        };
        let health_deadline = Instant::now() + EVENT_DEADLINE;
        let gap_result = if !self.gap_reported && !self.writes_blocked {
            self.stream_source()
                .record_gap_range_until(
                    &format!("Muse native view is unavailable ({})", reason.label()),
                    after.clone(),
                    head.clone(),
                    health_deadline,
                )
                .await
        } else {
            Ok(())
        };
        self.mark_source_stale(health_deadline).await;
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
            });
        }
        gap_result
    }

    async fn expire_view_recovery(&mut self) {
        self.view_recovery = None;
        self.status.source_unknown = true;
        self.status.failure = Some(
            "Muse native view recovery did not prove live delivery after the recorded gap"
                .to_owned(),
        );
    }

    fn invalidate_view_recovery(&mut self) {
        if let Some(recovery) = self.view_recovery.as_mut() {
            recovery.head = None;
            recovery.deadline = Instant::now();
        }
    }

    /// One explicit `stale` health update for a degraded or lost native stream/view,
    /// through the existing `source.health` mechanism. Attempted once and settled
    /// through `source.state` readback on an uncertain acknowledgement; never replayed.
    async fn mark_source_stale(&mut self, deadline: Instant) {
        if self.source_health_attempted {
            return;
        }
        self.source_health_attempted = true;
        self.set_source_health("stale", deadline).await;
    }

    async fn set_source_health(&mut self, health: &str, deadline: Instant) -> bool {
        let update =
            SourceHealthUpdate::new(self.agent_id.clone(), self.generation, "stream", health);
        let Ok(mut request) = serde_json::to_value(&update) else {
            self.status.source_unknown = true;
            return false;
        };
        if update.validate().is_err() {
            self.status.source_unknown = true;
            return false;
        }
        request["credential"] = json!(self.credential);
        let acknowledgement = self.host.call("source.health", request, deadline).await;
        if acknowledgement.as_ref().is_ok_and(|value| {
            serde_json::from_value::<SourceHealthUpdate>(value.clone())
                .is_ok_and(|ack| ack == update)
        }) {
            return true;
        }

        // A lost health acknowledgement is settled by readback, never by replay.
        let readback = self
            .host
            .call(
                "source.state",
                json!({
                    "credential": self.credential,
                    "agent_id": self.agent_id,
                    "generation": self.generation,
                    "source": "stream",
                }),
                deadline,
            )
            .await;
        let confirmed = readback.as_ref().is_ok_and(|state| {
            state.get("agent_id").and_then(Value::as_str) == Some(self.agent_id.as_str())
                && state.get("generation").and_then(Value::as_i64) == Some(self.generation)
                && state.get("source").and_then(Value::as_str) == Some("stream")
                && state.get("state").and_then(Value::as_str) == Some("open")
                && state.get("health").and_then(Value::as_str) == Some(health)
        });
        if !confirmed {
            self.status.source_unknown = true;
        }
        confirmed
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.invalidate_view_recovery();
        self.stream_source().record_gap(reason).await
    }

    /// The shared stream-commit half of this processor.
    fn stream_source(&mut self) -> StreamSource<'_> {
        StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent_id,
            generation: self.generation,
            harness: "Muse",
            id_prefix: "muse",
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
        let recovery_deadline = processor
            .view_recovery
            .as_ref()
            .map(|recovery| recovery.deadline);
        // Frames win over controls when both are ready: a `CheckTurn`
        // arriving after the queued terminal frame must see the finished
        // turn, never a still-live one.
        tokio::select! {
            biased;
            frame = frames.recv(), if accept_frames => {
                if frame.is_none() {
                    let current = *stream.borrow_and_update();
                    processor.handle_stream_state(current).await;
                }
                processor.handle_frame(frame).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            changed = stream.changed(), if accept_frames => {
                let current = match changed {
                    Ok(()) => *stream.borrow_and_update(),
                    Err(_) => StreamState::Closed,
                };
                processor.handle_stream_state(current).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            _ = tokio::time::sleep_until(recovery_deadline.unwrap_or_else(Instant::now)), if recovery_deadline.is_some() => {
                processor.expire_view_recovery().await;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(&mut frames, control).await {
                            break;
                        }
                        // Associate flushes buffered frames inline and request
                        // handling defers loss the same way frames do; report
                        // here too so a deferred gap never waits on a frame
                        // that never comes.
                        processor.report_gap_if_queue_empty(frames.is_empty()).await;
                    }
                    None => break,
                }
            }
        }
        let _ = status_tx.send(processor.status.clone());
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

/// Shared native event identity, kept under the local path for the
/// `event_id_rejects_blank_or_oversize_identity` test.
fn event_id(session_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    shared_event_id(session_id, turn_id, item_id, kind, "muse", "Muse")
}

/// Builds a gap reason from fixed classes and boolean comparisons only.
fn native_request_gap_reason(
    method: &str,
    error_code: &str,
    params: &Value,
    expected_session: &str,
    active_turn: &str,
    active_parent_turn_present: bool,
) -> String {
    let method_class = match method {
        "approval/request" => "approval",
        "userInput/request" => "userInput",
        _ => "other",
    };
    let error_class = match error_code {
        "native_protocol" => "native_protocol",
        "source_integrity" => "source_integrity",
        "unsupported" => "unsupported",
        _ => "other",
    };
    let session_id = params.get("sessionId");
    let session_value = session_id.and_then(Value::as_str);
    let turn_id = params.get("turnId");
    let turn_value = turn_id.and_then(Value::as_str);

    format!(
        "Muse native request validation: method_class={method_class} error_code={error_class} active_parent_turn_present={active_parent_turn_present} session_id_present={} session_id_nonblank={} session_id_matches_expected={} turn_id_present={} turn_id_nonblank={} turn_id_matches_active={} subagent_origin_present={}",
        session_id.is_some(),
        session_value.is_some_and(|value| !value.trim().is_empty()),
        session_value.is_some_and(|value| value == expected_session),
        turn_id.is_some(),
        turn_value.is_some_and(|value| !value.trim().is_empty()),
        turn_value.is_some_and(|value| value == active_turn),
        params.get("subagentOrigin").is_some(),
    )
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

/// Redact the free-text offer fields of a normalized public request view for
/// the committed `request.opened` event: the approval `summary`, `questions`
/// and approval `choices`. The in-memory offer stays raw so choice/
/// requirement matching and re-issue comparison keep working on exact native
/// values; only the persisted copy is redacted.
fn redact_request_details_for_commit(details: &Value, policy: &SecretPolicy) -> Value {
    let mut redacted = details.clone();
    if let Some(fields) = redacted.as_object_mut() {
        for key in ["summary", "questions", "choices"] {
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

/// MSP 1.4 item kinds normalized into the shared tool events, mirroring the
/// Codex adapter's tool-item coverage: `toolCall` (tool calls, including
/// command execution and edit-family calls) and `userShell` (command
/// execution; bound only when it names the exact active turn). Every other
/// kind keeps its existing mapping (`agentMessage` to `message.completed`,
/// `workflow` to the workflow-child observer) or stays unmapped; unknown
/// future kinds never become tool facts.
fn tool_item_kind(kind: &str) -> bool {
    matches!(kind, "toolCall" | "userShell")
}

/// Display tool for a normalized item: the `toolCall` tool name, or the
/// `userShell` kind itself. A `userShell` `commandText` is a full shell
/// invocation and is never persisted; a blank `toolCall` tool name proves
/// nothing and the caller gaps instead.
fn tool_label(item: &NativeItem) -> Option<String> {
    match item.kind.as_str() {
        "toolCall" => item.tool.clone().filter(|tool| !tool.trim().is_empty()),
        "userShell" => Some("userShell".to_owned()),
        _ => None,
    }
}

/// Terminal item revisions commit tool facts. The schema folds terminal as
/// anything other than `inProgress`; unknown statuses stay
/// terminal-unknown and still commit under their verbatim status.
fn is_terminal_item_status(status: &str) -> bool {
    status != "inProgress"
}

/// Map the MSP terminal to the stored turn status. `cancelled` folds to
/// `interrupted`, matching the Codex path; an unknown wire terminal settles
/// nothing and the caller records a gap instead. Shared with the
/// workflow-child observer: child terminals use this same mapping.
pub(crate) fn turn_status(terminal: &NativeTurnTerminal) -> Option<&'static str> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_plugin::protocol::MAX_COMMAND_ID_BYTES;

    #[test]
    fn native_request_gap_reason_keeps_only_bounded_metadata() {
        let session_marker = "SESSION_VALUE_MUST_NOT_BE_RECORDED";
        let active_turn_marker = "ACTIVE_TURN_VALUE_MUST_NOT_BE_RECORDED";
        let params = json!({
            "sessionId": session_marker,
            "turnId": "STALE_TURN_VALUE_MUST_NOT_BE_RECORDED",
            "approvalId": "APPROVAL_ID_MUST_NOT_BE_RECORDED",
            "toolCallId": "TOOL_CALL_ID_MUST_NOT_BE_RECORDED",
            "rawArgs": "TOOL_ARGUMENTS_MUST_NOT_BE_RECORDED",
            "token": "TOKEN_MUST_NOT_BE_RECORDED",
            "subagentOrigin": {
                "childRunId": "CHILD_RUN_ID_MUST_NOT_BE_RECORDED",
                "childSessionId": "CHILD_SESSION_ID_MUST_NOT_BE_RECORDED",
            },
        });

        let reason = native_request_gap_reason(
            "approval/request",
            "source_integrity",
            &params,
            session_marker,
            active_turn_marker,
            true,
        );

        assert!(reason.len() <= 512);
        assert!(reason.contains("method_class=approval"));
        assert!(reason.contains("error_code=source_integrity"));
        assert!(reason.contains("active_parent_turn_present=true"));
        assert!(reason.contains("session_id_present=true"));
        assert!(reason.contains("session_id_nonblank=true"));
        assert!(reason.contains("session_id_matches_expected=true"));
        assert!(reason.contains("turn_id_present=true"));
        assert!(reason.contains("turn_id_nonblank=true"));
        assert!(reason.contains("turn_id_matches_active=false"));
        assert!(reason.contains("subagent_origin_present=true"));
        for marker in [
            session_marker,
            active_turn_marker,
            "STALE_TURN_VALUE_MUST_NOT_BE_RECORDED",
            "APPROVAL_ID_MUST_NOT_BE_RECORDED",
            "TOOL_CALL_ID_MUST_NOT_BE_RECORDED",
            "TOOL_ARGUMENTS_MUST_NOT_BE_RECORDED",
            "TOKEN_MUST_NOT_BE_RECORDED",
            "CHILD_RUN_ID_MUST_NOT_BE_RECORDED",
            "CHILD_SESSION_ID_MUST_NOT_BE_RECORDED",
        ] {
            assert!(!reason.contains(marker));
        }

        let unknown = native_request_gap_reason(
            "untrusted method value",
            "untrusted error value",
            &Value::Null,
            "expected-session",
            "active-turn",
            false,
        );
        assert!(unknown.contains("method_class=other"));
        assert!(unknown.contains("error_code=other"));
        assert!(!unknown.contains("untrusted method value"));
        assert!(!unknown.contains("untrusted error value"));
    }

    #[test]
    fn tool_kinds_cover_tool_calls_and_turn_bound_shell() {
        for kind in ["toolCall", "userShell"] {
            assert!(tool_item_kind(kind), "kind={kind}");
        }
        for kind in [
            "userMessage",
            "agentMessage",
            "reasoning",
            "subagent",
            "workflow",
            "reminderChild",
            "compaction",
            "futureKind",
            "",
        ] {
            assert!(!tool_item_kind(kind), "kind={kind}");
        }
    }

    #[test]
    fn tool_label_keeps_arguments_and_payloads_out() {
        let call = NativeItem {
            item_id: "item-1".to_owned(),
            kind: "toolCall".to_owned(),
            revision: 1,
            status: "inProgress".to_owned(),
            text: None,
            tool: Some("shell".to_owned()),
            turn_id: Some("turn-1".to_owned()),
        };
        assert_eq!(tool_label(&call), Some("shell".to_owned()));
        let blank = NativeItem {
            tool: Some("  ".to_owned()),
            ..call.clone()
        };
        assert_eq!(tool_label(&blank), None);
        let shell = NativeItem {
            kind: "userShell".to_owned(),
            tool: None,
            ..call.clone()
        };
        assert_eq!(tool_label(&shell), Some("userShell".to_owned()));
        let message = NativeItem {
            kind: "agentMessage".to_owned(),
            ..call
        };
        assert_eq!(tool_label(&message), None);
    }

    #[test]
    fn tool_facts_commit_on_any_non_in_progress_status() {
        assert!(!is_terminal_item_status("inProgress"));
        for status in [
            "completed",
            "failed",
            "cancelled",
            "rejected",
            "timedOut",
            "future",
        ] {
            assert!(is_terminal_item_status(status), "status={status}");
        }
    }

    #[test]
    fn tool_items_decode_the_schema_field_names() {
        // `muse schema generate-json-schema` (stable, 1.4.0-R4161.1): the
        // `toolCall` tool name, `userShell` command execution and the common
        // identity fields. Per-kind payloads (`args`, `commandText`,
        // `patchRef`, `visibleOutput`, ...) stay unparsed and must not
        // break decoding.
        let call: NativeItem = serde_json::from_value(json!({
            "itemId": "item-1",
            "kind": "toolCall",
            "revision": 1,
            "status": "inProgress",
            "turnId": "turn-1",
            "tool": "shell",
            "callId": "call-1",
            "args": "{\"command\":\"cargo build\"}",
        }))
        .expect("toolCall decodes");
        assert_eq!(tool_label(&call), Some("shell".to_owned()));
        let shell: NativeItem = serde_json::from_value(json!({
            "itemId": "item-2",
            "kind": "userShell",
            "revision": 2,
            "status": "completed",
            "turnId": null,
            "commandText": "cargo test",
            "exitCode": 0,
        }))
        .expect("userShell decodes");
        assert_eq!(tool_label(&shell), Some("userShell".to_owned()));
        assert_eq!(shell.turn_id, None);
    }

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
