mod children;
use self::children::{
    ChildState, ChildThreads, spawned_item_id, spawned_parent_thread, top_level_parent_thread,
};
use crate::native_child::{
    ChildBinding, binding_from_ack, build_registration, is_admission_rejection, register_child,
};
use eliot_core::{
    ChildRegistration, ContextItem, ContextWindowTracker, Error, ProviderRestriction, Result,
    SecretPolicy, SessionTokenTotals, SessionUsage, TurnUsage,
    api::{NativeNotice, NativeNoticeKind},
    limits::{MAX_TOOL_COMMAND_BYTES, MAX_TOOL_ERROR_TAIL_BYTES},
    turn_started_data,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    event_actor::EventActor,
    protocol::MAX_COMMAND_ID_BYTES,
    session::{
        EVENT_DEADLINE, MessageCommit, StreamSource, attach_final_text,
        attach_native_error as shared_attach_native_error, attach_provider_restriction,
        event_id as shared_event_id, native_no_write, next_input_outcomes, rejected_control,
        request_reply_applied, unknown_control,
    },
    usage::{FirstCallUsage, call_input_tokens},
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
        NativeCommandExecutionItem, NativeErrorNotification, NativeFileChangeItem,
        NativeItemCompletedNotification, NativePatchChangeKind,
        NativeServerRequestResolvedNotification, NativeThreadItem, NativeThreadTokenUsage,
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
    pub compaction: Option<CompactionProgress>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CompactionProgress {
    pub command_id: String,
    pub baseline_turn_id: Option<String>,
    pub accepted: bool,
    pub turn_id: Option<String>,
    pub item_id: Option<String>,
    pub item_started: bool,
    pub item_completed: bool,
    pub terminal_status: Option<String>,
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
    actor: EventActor<Control, EventStatus>,
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
    BeginCompaction {
        command_id: String,
        baseline_turn_id: Option<String>,
        reply: oneshot::Sender<Result<()>>,
    },
    AcceptCompaction {
        command_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    ClearCompaction {
        command_id: String,
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
    turn_has_work_or_uncertain_effects: bool,
    /// Usage baseline and latest cumulative snapshot for the exact active
    /// turn, from `thread/tokenUsage/updated`. The baseline is the cumulative
    /// `total` minus the first observed per-response `last` (the thread total
    /// as the turn started); the latest is the newest cumulative `total`.
    /// The terminal `turn.completed` reports latest-minus-baseline, so a
    /// repeated notification cannot double count and raw `last` values are
    /// never summed.
    turn_usage: Option<(String, NativeTokenUsageDelta, NativeTokenUsageDelta)>,
    first_call_usage: FirstCallUsage,
    thread_model: Option<String>,
    usage_window: ContextWindowTracker,
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
    /// Unique native id sequence for transient retry/error notices.
    notice_seq: u64,
    native_scope: String,
    child_bindings: HashMap<String, ChildBinding>,
    child_states: HashMap<String, ChildState>,
    child_buffers: HashMap<String, VecDeque<BufferedNativeFrame>>,
    child_buffer_bytes: usize,
    child_unknown: HashSet<String>,
    child_early_lost: HashSet<String>,
    child_pending: HashMap<String, ChildRegistration>,
    children: ChildThreads,
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
    let actor = EventActor::spawn(
        CONTROL_QUEUE,
        EventStatus::default(),
        move |control_rx, status_tx| async move {
            run(frames, stream, processor, control_rx, status_tx).await;
        },
    );
    EventHandle { actor }
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

    pub async fn begin_compaction(
        &self,
        command_id: String,
        baseline_turn_id: Option<String>,
    ) -> Result<()> {
        self.send(|reply| Control::BeginCompaction {
            command_id,
            baseline_turn_id,
            reply,
        })
        .await
    }

    pub async fn accept_compaction(&self, command_id: String) -> Result<()> {
        self.send(|reply| Control::AcceptCompaction { command_id, reply })
            .await
    }

    pub async fn clear_compaction(&self, command_id: String) -> Result<()> {
        self.send(|reply| Control::ClearCompaction { command_id, reply })
            .await
    }

    pub async fn wait_compaction(
        &self,
        command_id: &str,
        deadline: Instant,
    ) -> Result<CompactionProgress> {
        let mut status = self.actor.watch().clone();
        loop {
            let snapshot = status.borrow().clone();
            if let Some(progress) = snapshot.compaction
                && progress.command_id == command_id
                && progress.terminal_status.is_some()
            {
                return Ok(progress);
            }
            if snapshot.source_unknown || snapshot.failure.is_some() {
                return Err(Error::new(
                    "unknown",
                    "Codex event source became uncertain during compaction",
                ));
            }
            timeout_at(deadline, status.changed())
                .await
                .map_err(|_| Error::new("unknown", "Codex compaction lifecycle deadline elapsed"))?
                .map_err(|_| {
                    Error::new(
                        "unknown",
                        "Codex event status stream closed during compaction",
                    )
                })?;
        }
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
        self.actor
            .call_with_dispatch(|reply| Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            })
            .await?
    }

    pub async fn close(&self) {
        let _ = self.actor.call(|reply| Control::Close { reply }).await;
    }

    pub async fn drain_native(&self) -> Result<()> {
        eliot_plugin::native_lifecycle::wait_for_native_tail(self.actor.watch(), |s| {
            s.native_drained
        })
        .await
    }

    pub fn status(&self) -> EventStatus {
        self.actor.watch().borrow().clone()
    }

    async fn send(&self, build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control) -> Result<()> {
        self.actor.call(build).await?
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
            turn_has_work_or_uncertain_effects: false,
            turn_usage: None,
            first_call_usage: FirstCallUsage::default(),
            thread_model: None,
            usage_window: ContextWindowTracker::default(),
            session_usage: None,
            session_usage_committed: None,
            session_usage_seq: 0,
            notice_seq: 0,
            native_scope: config.native_scope.clone(),
            child_bindings: HashMap::new(),
            child_states: HashMap::new(),
            child_buffers: HashMap::new(),
            child_buffer_bytes: 0,
            child_unknown: HashSet::new(),
            child_early_lost: HashSet::new(),
            child_pending: HashMap::new(),
            children: ChildThreads::default(),
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
            Control::BeginCompaction {
                command_id,
                baseline_turn_id,
                reply,
            } => {
                let result = self.begin_compaction(command_id, baseline_turn_id);
                let _ = reply.send(result);
                true
            }
            Control::AcceptCompaction { command_id, reply } => {
                let result = self.accept_compaction(&command_id).await;
                let _ = reply.send(result);
                true
            }
            Control::ClearCompaction { command_id, reply } => {
                let result = self.clear_compaction(&command_id).await;
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
                    if !self.gap_reported
                        && !self.writes_blocked
                        && error.failure_where.as_deref() != Some("plugin.events.put")
                    {
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
                    if !self.gap_reported
                        && !self.writes_blocked
                        && error.failure_where.as_deref() != Some("plugin.events.put")
                    {
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
            self.report_native_transport_loss().await;
        }
    }

    /// One explicit `stale` health update for a lost native transport, through the
    /// existing `source.health` mechanism. Attempted once and never replayed.
    async fn report_native_transport_loss(&mut self) {
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
        if self.compaction_notification_matches(method, params) {
            return false;
        }
        (matches!(
            method,
            "turn/started"
                | "turn/completed"
                | "item/started"
                | "item/completed"
                | "serverRequest/resolved"
                | "error"
        ) || turn_activity_notification(method))
            && (!self.status.ready || self.status.active_turn_id.is_none())
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
        self.thread_model = effective["model"].as_str().map(str::to_owned);
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
        self.turn_has_work_or_uncertain_effects = false;
        self.turn_usage = None;
        self.first_call_usage.associate_turn(&turn_id);
        self.usage_window
            .associate_turn(&turn_id, model.as_deref().or(self.thread_model.as_deref()));
        let thread_id = self.status.thread_id.as_deref().unwrap_or_default();
        let native_id = event_id(thread_id, &turn_id, &turn_id, "turn.started")?;
        // Report the model/effort the native turn request actually carried,
        // so the Store readback shows the carried values instead of the
        // overlay's absent overrides. Omitted when unknown: nothing is
        // invented.
        let data = turn_started_data(
            &command_id,
            &turn_id,
            model.as_deref(),
            effort.as_deref(),
            &next_input_outcomes(&context, &command_id),
        );
        self.commit_event("turn.started", native_id, data).await?;
        self.flush_buffer().await
    }

    fn begin_compaction(
        &mut self,
        command_id: String,
        baseline_turn_id: Option<String>,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.thread_id.is_none()
            || self.status.active_turn_id.is_some()
            || command_id.trim().is_empty()
            || self.writes_blocked
            || self.status.source_unknown
            || self
                .status
                .compaction
                .as_ref()
                .is_some_and(|progress| progress.terminal_status.is_none())
        {
            return Err(Error::new(
                "bad_state",
                "Codex compact requires a ready idle thread and resolved event source",
            ));
        }
        self.status.compaction = Some(CompactionProgress {
            command_id,
            baseline_turn_id,
            ..CompactionProgress::default()
        });
        Ok(())
    }

    async fn accept_compaction(&mut self, command_id: &str) -> Result<()> {
        let progress = self
            .status
            .compaction
            .as_mut()
            .filter(|progress| progress.command_id == command_id)
            .ok_or_else(|| Error::new("bad_state", "Codex compact reservation is missing"))?;
        if progress.accepted {
            return Err(Error::new(
                "bad_state",
                "Codex compact was already accepted",
            ));
        }
        progress.accepted = true;
        self.flush_buffer().await
    }

    async fn clear_compaction(&mut self, command_id: &str) -> Result<()> {
        let Some(progress) = self.status.compaction.as_ref() else {
            return Ok(());
        };
        if progress.command_id != command_id {
            return Err(Error::new("bad_state", "Codex compact reservation changed"));
        }
        if progress.turn_id.is_some() || progress.terminal_status.is_some() {
            return Err(Error::new(
                "unknown",
                "Codex compact already has native lifecycle evidence",
            ));
        }
        self.status.compaction = None;
        self.flush_buffer().await
    }

    fn compaction_notification_matches(&self, method: &str, params: &Value) -> bool {
        if method == "thread/tokenUsage/updated" {
            return false;
        }
        let Some(progress) = self
            .status
            .compaction
            .as_ref()
            .filter(|progress| progress.accepted)
        else {
            return false;
        };
        if params.get("threadId").and_then(Value::as_str) != self.status.thread_id.as_deref() {
            return false;
        }
        if method == "turn/started" {
            let Some(turn_id) = params.pointer("/turn/id").and_then(Value::as_str) else {
                return false;
            };
            return progress.baseline_turn_id.as_deref() != Some(turn_id)
                && progress
                    .turn_id
                    .as_deref()
                    .is_none_or(|known| known == turn_id);
        }
        let turn_id = params
            .get("turnId")
            .and_then(Value::as_str)
            .or_else(|| params.pointer("/turn/id").and_then(Value::as_str));
        progress
            .turn_id
            .as_deref()
            .is_some_and(|known| Some(known) == turn_id)
    }

    async fn process_compaction_notification(
        &mut self,
        method: &str,
        params: &Value,
    ) -> Result<bool> {
        if !self.compaction_notification_matches(method, params) {
            return Ok(false);
        }
        match method {
            "turn/started" => {
                let notification: NativeTurnStartedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex compact turn/started did not match the installed schema",
                        )
                    })?;
                if notification.turn.status != NativeTurnStatus::InProgress {
                    let progress = self.status.compaction.as_mut().unwrap();
                    progress.turn_id = Some(notification.turn.id);
                    progress.terminal_status = Some("unknown".to_owned());
                    return Ok(true);
                }
                self.status.compaction.as_mut().unwrap().turn_id = Some(notification.turn.id);
            }
            "item/started" => {
                let notification: NativeItemStarted = serde_json::from_value(params.clone())
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex compact item/started did not match the installed schema",
                        )
                    })?;
                if notification.item.get("type").and_then(Value::as_str)
                    == Some("contextCompaction")
                {
                    let item_id = notification
                        .item
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.trim().is_empty())
                        .ok_or_else(|| {
                            Error::new(
                                "native_protocol",
                                "Codex contextCompaction start omitted its item id",
                            )
                        })?;
                    let progress = self.status.compaction.as_mut().unwrap();
                    if progress.item_started || progress.item_completed {
                        progress.terminal_status = Some("unknown".to_owned());
                    } else {
                        progress.item_id = Some(item_id.to_owned());
                        progress.item_started = true;
                    }
                }
            }
            "item/completed" => {
                let notification: NativeItemCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex compact item/completed did not match the installed schema",
                        )
                    })?;
                if let NativeThreadItem::ContextCompaction(item) = &notification.item {
                    let confirmed = {
                        let progress = self.status.compaction.as_mut().unwrap();
                        if !progress.item_started
                            || progress.item_id.as_deref() != Some(item.id.as_str())
                        {
                            progress.terminal_status = Some("unknown".to_owned());
                            false
                        } else {
                            progress.item_completed = true;
                            true
                        }
                    };
                    if confirmed {
                        self.commit_context_compacted(
                            &notification.thread_id,
                            &notification.turn_id,
                            &item.id,
                        )
                        .await?;
                    }
                }
            }
            "turn/completed" => {
                let notification: NativeTurnCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Codex compact turn/completed did not match the installed schema",
                        )
                    })?;
                let progress = self.status.compaction.as_mut().unwrap();
                let status = match notification.turn.status {
                    NativeTurnStatus::Completed => "completed",
                    NativeTurnStatus::Failed => "failed",
                    NativeTurnStatus::Interrupted => "interrupted",
                    NativeTurnStatus::InProgress => "unknown",
                };
                progress.terminal_status =
                    Some(
                        if status == "completed"
                            && (!progress.item_started || !progress.item_completed)
                        {
                            "lifecycle_missing"
                        } else {
                            status
                        }
                        .to_owned(),
                    );
            }
            _ => {}
        }
        Ok(true)
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
                if error.failure_where.as_deref() == Some("plugin.events.put") {
                    return Err(error);
                }
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
        self.turn_has_work_or_uncertain_effects = true;
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
        let mut data = json!({
            "command_id": command_id,
            "request_id": request.id,
            "turn_id": request.turn_id,
            "kind": public_kind,
            "details": public_details,
        });
        if request.kind == "command_execution_approval"
            && let Some(command) = request.details["command"].as_str()
        {
            data[eliot_core::COMMAND_GUARD_CONTEXT_FIELD] = json!({"commands": [command]});
        }
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
                Ok(request_reply_applied(
                    command_id,
                    &request.id,
                    &request.turn_id,
                ))
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

    async fn note_native_error(&mut self, params: Value) -> Result<()> {
        let Ok(notification) = serde_json::from_value::<NativeErrorNotification>(params) else {
            return Ok(());
        };
        let (Some(thread_id), Some(turn_id), Some(error)) = (
            notification.thread_id.as_deref(),
            notification.turn_id.as_deref(),
            notification.error.as_ref(),
        ) else {
            return Ok(());
        };
        if self.status.active_turn_id.as_deref() != Some(turn_id)
            || self.status.active_command_id.is_none()
        {
            return Ok(());
        }
        validate_thread_and_turn(&self.status, thread_id, turn_id)?;
        // Codex emits structured 429/403 notices as reconnect attempts.
        // An explicit refusal is known even when the transport retries it.
        self.turn_has_work_or_uncertain_effects |= error.provider_restriction().is_none();

        self.notice_seq = self
            .notice_seq
            .checked_add(1)
            .ok_or_else(|| Error::new("source_integrity", "Codex notice sequence overflow"))?;
        let kind = native_error_notice_kind(error, notification.will_retry == Some(true));
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let data = native_notice_data(turn_id, kind, error, &secrets)?;
        let native_id = event_id(
            thread_id,
            turn_id,
            &format!("error-{}", self.notice_seq),
            "turn.notice",
        )?;
        self.commit_event("turn.notice", native_id, data).await
    }

    async fn process_notification(&mut self, method: &str, params: Value) -> Result<()> {
        if self
            .process_compaction_notification(method, &params)
            .await?
        {
            return Ok(());
        }
        if turn_activity_notification(method)
            && let Some(turn) = self.status.active_turn_id.as_deref()
            && params.get("turnId").and_then(Value::as_str) == Some(turn)
        {
            self.turn_has_work_or_uncertain_effects = true;
        }
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
                self.turn_has_work_or_uncertain_effects |=
                    has_non_user_items(&notification.turn.items);
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
                self.turn_has_work_or_uncertain_effects |= item_type != "userMessage";
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
                    let mut data = json!({
                        "command_id": command_id,
                        "turn_id": notification.turn_id,
                        "item_id": item_id,
                        "tool": item_type,
                    });
                    if let Some(command) = command_text(&notification.item) {
                        data["command"] = Value::String(
                            SecretPolicy::new(self.secrets.iter().cloned())
                                .redact_bounded(&command, MAX_TOOL_COMMAND_BYTES),
                        );
                    }
                    self.commit_event("tool.started", native_id, data).await?;
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
                self.turn_has_work_or_uncertain_effects |= item_type != "userMessage";
                if let NativeThreadItem::ContextCompaction(item) = &notification.item {
                    self.commit_context_compacted(
                        &notification.thread_id,
                        &notification.turn_id,
                        &item.id,
                    )
                    .await?;
                } else if item_type == "agentMessage" {
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
                self.note_native_error(params).await?;
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
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            turn_id,
            item_id,
            "message.completed",
        )?;
        let is_final = matches!(phase, Some("final_answer") | None);
        let commit = self.stream_source().commit_message(native_id, data).await?;
        // A blank reply is no message, so it never overwrites the terminal text.
        if commit == MessageCommit::Committed && is_final {
            self.final_text = Some(text);
        }
        Ok(())
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
        if let Some(ref native_status) = native_status {
            data["status"] = Value::String(native_status.clone());
        }
        if item_type == "commandExecution" {
            // A native shape change must not fail the event: the completion is
            // committed with its status alone, exactly as before this mapping.
            if let Ok(command) = serde_json::from_value::<NativeCommandExecutionItem>(item.clone())
            {
                if let Some(exit_code) = command.exit_code {
                    data["exit_code"] = json!(exit_code);
                }
                // A failure without its reason is a bare `failed`; keep the tail
                // that usually says why. Success keeps its exit code only.
                if native_status.as_deref() == Some("failed")
                    && let Some(output) = command.aggregated_output.as_deref()
                {
                    let tail = failed_output_tail(output, &self.secrets);
                    if let Some(tail) = tail {
                        data["error_tail"] = Value::String(tail);
                    }
                }
            }
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
        let last = &notification.token_usage.last;
        let response = last.has_response_counters();
        self.first_call_usage.observe_for_turn(
            self.status.active_turn_id.as_deref(),
            &notification.turn_id,
            response,
            call_input_tokens("codex", last.input_tokens, None, None),
        );
        if response
            && self.status.active_turn_id.as_deref() == Some(notification.turn_id.as_str())
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
        let Some(merged) = session_usage_from_token_usage(
            &notification.token_usage,
            self.session_usage.as_ref(),
            &mut self.usage_window,
            &notification.turn_id,
        ) else {
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
            if !self.status.source_unknown
                && !self.transport_loss
                && !self.turn_has_work_or_uncertain_effects
                && !has_non_user_items(&notification.turn.items)
                && notification
                    .turn
                    .error
                    .as_ref()
                    .is_some_and(NativeTurnError::upstream_request_refused)
            {
                data["provider_request_refused_before_work"] = Value::Bool(true);
            }
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
        if let Some(usage) = self.first_call_usage.finish("turn", turn_usage) {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
        }
        attach_native_error(&mut data, notification.turn.error.as_ref(), &secrets);
        if let Some(text) = self.final_text.as_ref() {
            attach_final_text(&mut data, text);
        }
        let native_id = event_id(
            self.status.thread_id.as_deref().unwrap_or_default(),
            &notification.turn.id,
            &notification.turn.id,
            "turn.completed",
        )?;
        if let Err(error) = self.commit_event("turn.completed", native_id, data).await {
            if error.failure_where.as_deref() != Some("plugin.events.put") {
                return Err(error);
            }
            // The host owns the gap. The terminal notification still proves
            // these native handles can be released.
            self.status.source_unknown = true;
        }
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
        self.first_call_usage = FirstCallUsage::default();
        Ok(())
    }

    async fn commit_context_compacted(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<()> {
        validate_thread_and_turn(&self.status, thread_id, turn_id)?;
        let native_id = event_id(thread_id, turn_id, item_id, "session.context.compacted")?;
        let mut data = json!({"native_turn_id": turn_id});
        if self.status.active_turn_id.as_deref() == Some(turn_id) {
            data["turn_id"] = json!(turn_id);
        }
        self.commit_event("session.context.compacted", native_id, data)
            .await
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

fn turn_activity_notification(method: &str) -> bool {
    (method.starts_with("item/") && !matches!(method, "item/started" | "item/completed"))
        || matches!(method, "turn/diff/updated" | "turn/plan/updated")
}

fn has_non_user_items(items: &[NativeThreadItem]) -> bool {
    items
        .iter()
        .any(|item| !matches!(item, NativeThreadItem::UserMessage))
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

fn native_error_notice_kind(error: &NativeTurnError, will_retry: bool) -> NativeNoticeKind {
    if !will_retry {
        return NativeNoticeKind::Error;
    }
    let reconnecting = error
        .codex_error_info
        .as_ref()
        .and_then(Value::as_object)
        .filter(|variants| variants.len() == 1)
        .is_some_and(|variants| {
            variants.keys().any(|variant| {
                matches!(
                    variant.as_str(),
                    "httpConnectionFailed"
                        | "responseStreamConnectionFailed"
                        | "responseStreamDisconnected"
                )
            })
        });
    if reconnecting {
        NativeNoticeKind::Reconnecting
    } else {
        NativeNoticeKind::Retrying
    }
}

fn native_notice_data(
    turn_id: &str,
    kind: NativeNoticeKind,
    error: &NativeTurnError,
    policy: &SecretPolicy,
) -> Result<Value> {
    let mut data = json!({});
    shared_attach_native_error(&mut data, Some(&error.message), policy);
    let native_error = &data["native_error"];
    serde_json::to_value(NativeNotice {
        turn_id: turn_id.to_owned(),
        kind,
        provider_kind: error
            .provider_restriction()
            .map(|restriction| restriction.kind),
        message: native_error["message"]
            .as_str()
            .filter(|message| !message.trim().is_empty())
            .map(str::to_owned),
        truncated: native_error["truncated"].as_bool().unwrap_or(false),
    })
    .map_err(|_| Error::new("internal", "Codex native notice could not be encoded"))
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
    TurnUsage::from_native(
        "turn",
        TurnUsage {
            input_tokens: delta.input_tokens,
            output_tokens: delta.output_tokens,
            reasoning_tokens: delta.reasoning_output_tokens,
            cache_read_tokens: delta.cached_input_tokens,
            cache_write_tokens: delta.cache_write_input_tokens,
            total_tokens: delta.total_tokens,
            ..TurnUsage::default()
        },
    )
}

/// Merge one `thread/tokenUsage/updated` body into the retained session
/// snapshot: the cumulative per-thread `total` replaces the retained
/// cumulative counters (input/output/cache only, never a derived total),
/// The latest response can use an earlier window for the same model or turn.
fn session_usage_from_token_usage(
    token_usage: &NativeThreadTokenUsage,
    previous: Option<&SessionUsage>,
    context: &mut ContextWindowTracker,
    turn_id: &str,
) -> Option<SessionUsage> {
    let counter = |value: Option<i64>| value.filter(|value| *value >= 0);
    let mut merged = previous.cloned().unwrap_or_default();
    let mut changed = false;
    let last = &token_usage.last;
    let has_response = last.has_response_counters();
    if has_response && let Some(total) = &token_usage.total {
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
    }
    changed |= context.observe(
        &mut merged,
        turn_id,
        has_response,
        last.total_tokens,
        token_usage.model_context_window,
    );
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

/// The command a `commandExecution` item runs, as Codex parsed it
/// (`commandActions[].command`, one per line, without the shell wrapper),
/// else its raw `command` string; `None` for other items or a blank one.
fn command_text(item: &Value) -> Option<String> {
    if item.get("type").and_then(Value::as_str) != Some("commandExecution") {
        return None;
    }
    let parsed: Vec<&str> = item
        .get("commandActions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|action| action.get("command").and_then(Value::as_str))
        .map(str::trim)
        .filter(|command| !command.is_empty())
        .collect();
    let text = if parsed.is_empty() {
        item.get("command")
            .and_then(Value::as_str)?
            .trim()
            .to_owned()
    } else {
        parsed.join("\n")
    };
    (!text.is_empty()).then_some(text)
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

/// The tail of a failed tool's output: the whole text is redacted first, then
/// the LAST `MAX_TOOL_ERROR_TAIL_BYTES` at a character boundary, starting at a
/// line boundary when the retained bytes already hold one. A long build keeps
/// the lines that actually fail, not its first banner. `None` when nothing
/// usable remains.
fn failed_output_tail(output: &str, secrets: &[String]) -> Option<String> {
    let safe = SecretPolicy::new(secrets.iter().cloned()).redact_text(output);
    let max = MAX_TOOL_ERROR_TAIL_BYTES;
    let tail = if safe.len() <= max {
        safe.as_str()
    } else {
        let start = safe.floor_char_boundary(safe.len() - max);
        let boundary = safe[start..]
            .find(['\n', '\r'])
            .map(|offset| start + offset + 1)
            .unwrap_or(start);
        &safe[boundary..]
    };
    (!tail.trim().is_empty()).then(|| tail.to_owned())
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
