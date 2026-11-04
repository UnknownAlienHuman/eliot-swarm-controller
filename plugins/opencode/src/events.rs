//! Native SSE facts to durable Eliot events.
//!
//! The event task owns the controlled `stream` source: it routes every SSE
//! envelope by exact session, commits one native record per batch
//! (`session.ready`, `turn.started`, `message.completed`, `turn.completed`,
//! `session.ended`), and records an explicit gap on any loss or
//! unmodelled native behavior. Streaming detail (`text`/`reasoning`/`tool`
//! deltas, steps, usage, ephemeral status) is intentionally folded into
//! the terminal message readback, never published as its own facts.

use std::collections::{HashSet, VecDeque};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    native_stdio::StreamState,
    session::{EVENT_DEADLINE, StreamSource, attach_final_text, attach_native_error, event_id},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    client::OpenCodeHttp,
    sse::SseFrame,
    types::{
        AssistantContent, ExecutionEventData, InboxEventData, Message, SessionLifecycleData,
        SseEnvelope, StructuredError,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_PAGES: usize = 4;

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub ready: bool,
    pub connected: bool,
    pub session_id: Option<String>,
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
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateInbox {
        command_id: String,
        inbox_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    AbandonAssociation {
        command_id: String,
        reply: oneshot::Sender<()>,
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

#[derive(Clone)]
struct BufferedEnvelope {
    envelope: SseEnvelope,
    size: usize,
}

struct PendingInbox {
    command_id: String,
    inbox_id: String,
}

struct EventProcessor {
    http: OpenCodeHttp,
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    secrets: Vec<String>,
    status: EventStatus,
    buffer: VecDeque<BufferedEnvelope>,
    buffer_bytes: usize,
    gap_number: u64,
    accept_frames: bool,
    planned_stop: bool,
    loss_reason: Option<&'static str>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    pending: Option<PendingInbox>,
    /// Inbox id of the currently open turn: the prompt response id, which
    /// is the user message id the terminal readback bounds against.
    open_inbox_id: Option<String>,
    /// Inbox ids observed as enqueued or delivered, by id. Bounded by the
    /// per-generation command history the host keeps.
    seen_inbox: HashSet<String>,
    /// `session.execution.started` event id waiting for its Eliot command.
    /// At most one: a second unclaimed start is unmodelled native behavior.
    stashed_started: Option<String>,
    /// Next expected durable sequence of our session aggregate. `Some(0)`
    /// from readiness: our aggregate starts at `session.created` seq 0 and
    /// every durable event reaches every subscriber in order.
    expected_seq: Option<u64>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<SseFrame>,
    stream: watch::Receiver<StreamState>,
    http: OpenCodeHttp,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let processor = EventProcessor::new(http, host, config);
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

    pub async fn associate_inbox(&self, command_id: String, inbox_id: String) -> Result<()> {
        self.send(|reply| Control::AssociateInbox {
            command_id,
            inbox_id,
            reply,
        })
        .await
    }

    pub async fn abandon_association(&self, command_id: String) {
        let (reply, response) = oneshot::channel();
        if self
            .control
            .send(Control::AbandonAssociation { command_id, reply })
            .await
            .is_ok()
        {
            let _ = timeout_at(Instant::now() + EVENT_DEADLINE, response).await;
        }
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

    /// Wait until `ready` for the association of `command_id`: its turn
    /// opened (applied), the source failed/stopped (unknown), or the
    /// deadline elapsed (unknown; the admitted input is never resent).
    pub async fn await_association(&self, command_id: &str, deadline: Instant) -> Result<String> {
        // One watcher for the whole wait: cloning per iteration could miss
        // a status version that lands between the check and the clone.
        let mut watcher = self.status.clone();
        loop {
            {
                let status = watcher.borrow_and_update();
                if status.active_command_id.as_deref() == Some(command_id)
                    && let Some(turn_id) = status.active_turn_id.clone()
                {
                    return Ok(turn_id);
                }
                if status.failure.is_some() || status.stopped {
                    return Err(Error::new(
                        "unknown",
                        "OpenCode send association did not complete",
                    ));
                }
            }
            match timeout_at(deadline, watcher.changed()).await {
                Ok(Ok(())) => {}
                _ => {
                    return Err(Error::new(
                        "unknown",
                        "OpenCode send was admitted but its execution association is unknown",
                    ));
                }
            }
        }
    }

    async fn send(&self, build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control) -> Result<()> {
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + EVENT_DEADLINE,
            self.control.send(build(reply)),
        )
        .await
        .map_err(|_| Error::new("unknown", "OpenCode event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "OpenCode event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "OpenCode event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "OpenCode event-control response was lost"))?
    }
}

impl EventProcessor {
    fn new(http: OpenCodeHttp, host: PeerClient, config: EventConfig) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
            http,
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
            pending: None,
            open_inbox_id: None,
            seen_inbox: HashSet::new(),
            stashed_started: None,
            expected_seq: None,
        }
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
            Control::AssociateInbox {
                command_id,
                inbox_id,
                reply,
            } => {
                let result = self.associate_inbox(command_id, inbox_id).await;
                let _ = reply.send(result);
                true
            }
            Control::AbandonAssociation { command_id, reply } => {
                if self.pending.as_ref().is_some_and(|pending| {
                    pending.command_id == command_id
                        && self.status.active_command_id.as_deref() != Some(command_id.as_str())
                }) {
                    self.pending = None;
                }
                let _ = reply.send(());
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
                        "OpenCode stop is already waiting for native output drain",
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
                        "OpenCode native event queue closed before stop settlement",
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
                    self.defer_gap("OpenCode event stream closed");
                }
            }
            StreamState::Overflow => self.defer_gap("OpenCode event queue overflowed"),
            StreamState::Malformed => {
                self.defer_gap("OpenCode event stream contained an invalid envelope")
            }
            StreamState::Open => {}
        }
    }

    async fn handle_frame(&mut self, frame: Option<SseFrame>) {
        match frame {
            Some(SseFrame::Event(envelope)) => {
                if self.should_buffer(&envelope.event_type) {
                    self.buffer_envelope(envelope);
                } else {
                    // Processing may open the turn; flush what that unlocks.
                    // `process_envelope` never flushes itself, so this loop
                    // always terminates.
                    if let Err(error) = self.process_envelope(envelope).await {
                        self.status.source_unknown = true;
                        if !self.gap_reported && !self.writes_blocked {
                            let _ = self
                                .record_gap("OpenCode native event could not be normalized")
                                .await;
                        }
                        if self.status.failure.is_none() {
                            self.status.failure = Some(error.message);
                        }
                    } else if let Err(error) = self.flush_buffer().await
                        && self.status.failure.is_none()
                    {
                        self.status.failure = Some(error.message);
                        self.status.source_unknown = true;
                    }
                }
            }
            Some(SseFrame::Malformed) => {
                // Unreachable: the pump converts this marker into the
                // stream state and ends instead of forwarding it.
            }
            None => {
                self.frames_drained = true;
                self.accept_frames = false;
                if !self.buffer.is_empty() {
                    self.buffer.clear();
                    self.buffer_bytes = 0;
                    self.defer_gap("OpenCode native events remained unassociated at stream end");
                }
                if !self.planned_stop && self.loss_reason.is_none() {
                    self.defer_gap("OpenCode native event reader closed");
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

    /// Before readiness every session envelope buffers (no send can be
    /// in flight yet), so the durable sequence tracker — which starts at
    /// `Some(0)` in `ready` — observes the whole aggregate from its first
    /// record, in order. After readiness, terminal envelopes wait for
    /// association: a terminal that arrives before its `AssociateInbox`
    /// control must not read as stray. Inbox and `started` envelopes
    /// always process immediately — they are the association facts that
    /// open the turn.
    fn should_buffer(&self, event_type: &str) -> bool {
        if !self.status.ready {
            return event_type != "server.connected";
        }
        matches!(
            event_type,
            "session.inbox.cancelled"
                | "session.execution.succeeded"
                | "session.execution.failed"
                | "session.execution.interrupted"
        ) && self.status.active_turn_id.is_none()
    }

    fn buffer_envelope(&mut self, envelope: SseEnvelope) {
        let size = serde_json::to_vec(&envelope)
            .map(|bytes| bytes.len())
            .unwrap_or_else(|_| EARLY_EVENT_BYTES.saturating_add(1));
        if self.buffer.len() >= EARLY_EVENT_LIMIT
            || self.buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            self.buffer.clear();
            self.buffer_bytes = 0;
            self.defer_gap("OpenCode early-event buffer exceeded its bound");
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedEnvelope { envelope, size });
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
                "OpenCode readiness requires one exact session and effective settings",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "OpenCode event source acknowledgement is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.clone());
        self.expected_seq = Some(0);
        let native_id = format!("opencode:{session_id}:session.ready");
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
                "OpenCode event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "OpenCode event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    async fn associate_inbox(&mut self, command_id: String, inbox_id: String) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || inbox_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "OpenCode inbox association requires a ready exact session and inbox",
            ));
        }
        if self.status.active_turn_id.is_some() || self.pending.is_some() {
            return Err(Error::new(
                "busy",
                "OpenCode already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "OpenCode event source acknowledgement is unresolved",
            ));
        }
        self.pending = Some(PendingInbox {
            command_id,
            inbox_id,
        });
        self.try_open_turn().await?;
        self.flush_buffer().await
    }

    /// Open the turn once both association facts hold: our inbox was
    /// observed (enqueued or delivered) and an execution started. Either
    /// order is accepted; the turn id is the exact `started` event id.
    async fn try_open_turn(&mut self) -> Result<()> {
        let Some(pending) = self.pending.as_ref() else {
            return Ok(());
        };
        if self.status.active_turn_id.is_some() || !self.seen_inbox.contains(&pending.inbox_id) {
            return Ok(());
        }
        let Some(turn_id) = self.stashed_started.clone() else {
            return Ok(());
        };
        self.stashed_started = None;
        let command_id = pending.command_id.clone();
        self.open_inbox_id = Some(pending.inbox_id.clone());
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        self.pending = None;
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = event_id(
            session_id,
            &turn_id,
            &turn_id,
            "turn.started",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event(
            "turn.started",
            native_id,
            json!({"command_id": command_id, "turn_id": turn_id}),
        )
        .await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            if self.should_buffer(&frame.envelope.event_type) {
                self.buffer_bytes = self.buffer_bytes.saturating_add(frame.size);
                self.buffer.push_front(frame);
                return Ok(());
            }
            self.process_envelope(frame.envelope).await?;
        }
        Ok(())
    }

    async fn process_envelope(&mut self, envelope: SseEnvelope) -> Result<()> {
        self.check_sequence(&envelope).await?;
        match envelope.event_type.as_str() {
            "server.connected" => {
                self.status.connected = true;
                Ok(())
            }
            "session.created" => self.observe_session_created(&envelope.data).await,
            "session.deleted" => self.observe_session_deleted(&envelope.data).await,
            "session.inbox.enqueued" | "session.inbox.delivered" => {
                self.observe_inbox(&envelope.data).await
            }
            "session.inbox.cancelled" => self.observe_inbox_cancelled(&envelope.data).await,
            "session.execution.started" => self.observe_execution_started(&envelope).await,
            "session.execution.succeeded"
            | "session.execution.failed"
            | "session.execution.interrupted" => self.complete_turn(&envelope).await,
            // Streaming detail (text/reasoning/tool deltas, steps, usage),
            // ephemeral status, and any future type fold into the terminal
            // readback or stay unconnected; they are never gaps.
            _ => Ok(()),
        }
    }

    /// Durable envelopes of our session aggregate carry a contiguous
    /// per-aggregate sequence. A forward skip proves transport loss;
    /// anything else (ephemeral events, foreign aggregates) is untracked.
    async fn check_sequence(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        let Some(durable) = envelope.durable.as_ref() else {
            return Ok(());
        };
        if durable.aggregate_id != session_id {
            return Ok(());
        }
        match self.expected_seq {
            Some(expected) if durable.seq == expected => {
                self.expected_seq = Some(expected.saturating_add(1));
                Ok(())
            }
            Some(expected) if durable.seq < expected => {
                // A duplicate replay of an already-seen record: ignore.
                Ok(())
            }
            _ => {
                self.record_gap("OpenCode event stream skipped durable sequence numbers")
                    .await?;
                Err(Error::new(
                    "unknown",
                    "OpenCode native facts were lost in transport",
                ))
            }
        }
    }

    async fn observe_session_created(&mut self, data: &Value) -> Result<()> {
        let created: SessionLifecycleData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode session.created omitted its exact session id",
            )
        })?;
        if created.session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "OpenCode session.created omitted its exact session id",
            ));
        }
        // The session id binds only from the create response (the Ready
        // control): a `session.created` never adopts an id, so a foreign one
        // is unmodelled native behavior.
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|existing| existing != created.session_id)
        {
            return self
                .record_gap("OpenCode server announced a second session in one Agent generation")
                .await;
        }
        Ok(())
    }

    async fn observe_session_deleted(&mut self, data: &Value) -> Result<()> {
        let deleted: SessionLifecycleData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode session.deleted omitted its exact session id",
            )
        })?;
        if self.status.session_id.as_deref() != Some(deleted.session_id.as_str()) {
            return Ok(());
        }
        if self.planned_stop || self.status.stopped {
            return Ok(());
        }
        self.record_gap("OpenCode native session ended outside Eliot stop")
            .await?;
        Err(Error::new(
            "unknown",
            "OpenCode native session is gone; its outcome is unknown",
        ))
    }

    async fn observe_inbox(&mut self, data: &Value) -> Result<()> {
        let inbox: InboxEventData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode inbox event omitted its exact session or inbox id",
            )
        })?;
        if self.status.session_id.as_deref() != Some(inbox.session_id.as_str())
            || inbox.inbox_id.trim().is_empty()
        {
            return Ok(());
        }
        self.seen_inbox.insert(inbox.inbox_id);
        if self.seen_inbox.len() > 1024 {
            // Defensive bound; a generation holds at most 32 commands.
            self.seen_inbox.clear();
        }
        self.try_open_turn().await
    }

    async fn observe_inbox_cancelled(&mut self, data: &Value) -> Result<()> {
        let inbox: InboxEventData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode inbox event omitted its exact session or inbox id",
            )
        })?;
        if self.status.session_id.as_deref() != Some(inbox.session_id.as_str()) {
            return Ok(());
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| pending.inbox_id == inbox.inbox_id)
        {
            self.record_gap("OpenCode dropped an Eliot input before execution")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode dropped the admitted input; its outcome is unknown",
            ));
        }
        Ok(())
    }

    async fn observe_execution_started(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let started: ExecutionEventData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "OpenCode execution.started omitted its exact session id",
                )
            })?;
        if self.status.session_id.as_deref() != Some(started.session_id.as_str()) {
            return Ok(());
        }
        if self.status.active_turn_id.is_some() {
            // A retry or continuation of the open turn: the turn closes at
            // the terminal event and the readback covers the whole run.
            return Ok(());
        }
        if self.pending.is_some() {
            self.stashed_started = Some(envelope.id.clone());
            return self.try_open_turn().await;
        }
        if self.stashed_started.is_some() {
            self.record_gap("OpenCode started a second execution with no Eliot send")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode execution has no Eliot input; its outcome is unknown",
            ));
        }
        self.stashed_started = Some(envelope.id.clone());
        Ok(())
    }

    async fn complete_turn(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let terminal: ExecutionEventData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "OpenCode execution terminal omitted its exact session id",
                )
            })?;
        if self.status.session_id.as_deref() != Some(terminal.session_id.as_str()) {
            return Ok(());
        }
        let (command_id, turn_id) = match (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            (Some(command_id), Some(turn_id)) => (command_id, turn_id),
            // Buffered until association; reaching here unassociated means a
            // terminal with no turn, which is a transcript gap.
            _ => {
                self.record_gap("OpenCode terminal event matched no exact active turn")
                    .await?;
                return Err(Error::new(
                    "unknown",
                    "OpenCode turn completed outside any Eliot send",
                ));
            }
        };
        let status = match envelope.event_type.as_str() {
            "session.execution.succeeded" => "completed",
            "session.execution.failed" => "failed",
            _ => "interrupted",
        };
        let readback = self
            .read_terminal_messages(&terminal.session_id)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!("OpenCode terminal readback failed: {}", error.message),
                )
            })?;
        if readback.outcome != status {
            self.record_gap("OpenCode terminal event disagreed with its idle message")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode turn outcome is contradictory",
            ));
        }
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        if let Some(text) = readback.final_text.as_deref() {
            let data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "text": secrets.redact_text(text),
            });
            if serde_json::to_vec(&data)
                .map(|bytes| bytes.len())
                .unwrap_or(usize::MAX)
                > MAX_EVENT_BYTES
            {
                self.record_gap("OpenCode completed message exceeded the persisted-event bound")
                    .await?;
                return Err(Error::new("unknown", "OpenCode reply exceeded its bound"));
            }
            let native_id = event_id(
                &terminal.session_id,
                &turn_id,
                &readback.idle_id,
                "message.completed",
                "opencode",
                "OpenCode",
            )?;
            self.commit_event("message.completed", native_id, data)
                .await?;
        }
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": turn_id,
            "status": status,
        });
        if let Some(text) = readback.final_text.as_deref() {
            attach_final_text(
                &mut data,
                &secrets.redact_text(text),
                status == "failed",
                &secrets,
            );
        }
        let native_error = terminal.error.as_ref().or(readback.idle_error.as_ref());
        attach_native_error(
            &mut data,
            native_error.map(|error| error.message.as_str()),
            &secrets,
        );
        let native_id = event_id(
            &terminal.session_id,
            &turn_id,
            &turn_id,
            "turn.completed",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event("turn.completed", native_id, data).await?;
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: turn_id.clone(),
            status: Some(status.to_owned()),
            final_text: readback.final_text,
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.open_inbox_id = None;
        Ok(())
    }

    /// Terminal readback: page the newest-first message list (bounded) and
    /// find our user message plus the newest `idle` message after it. Final
    /// text is the concatenated assistant text parts strictly between the
    /// two, in ascending time order. Anything else (missing user message,
    /// missing idle, contradictory outcome) is a gap, never an inference.
    async fn read_terminal_messages(&self, session_id: &str) -> Result<TerminalReadback> {
        let inbox_id = self
            .open_inbox_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode turn has no exact input association"))?;
        let mut cursor: Option<String> = None;
        let mut user_position: Option<usize> = None;
        let mut idle: Option<(String, String, Option<StructuredError>)> = None;
        let mut texts: Vec<(i64, String, String)> = Vec::new();
        let mut scanned: Vec<Message> = Vec::new();
        for _ in 0..MAX_MESSAGE_PAGES {
            let page = self
                .http
                .list_messages(
                    session_id,
                    cursor.as_deref(),
                    Instant::now() + EVENT_DEADLINE,
                )
                .await?;
            if page.data.is_empty() {
                break;
            }
            for message in &page.data {
                match message {
                    Message::User(user) if user.id == inbox_id => {
                        user_position = Some(scanned.len());
                    }
                    Message::Assistant(assistant) => {
                        for part in &assistant.content {
                            if let AssistantContent::Text(text) = part
                                && let Some(body) = text.text.as_deref()
                            {
                                texts.push((
                                    assistant.time.created,
                                    assistant.id.clone(),
                                    body.to_owned(),
                                ));
                            }
                        }
                    }
                    Message::Idle(idle_message) if idle.is_none() && user_position.is_none() => {
                        // Newest-first: the first idle seen before our
                        // user message is the newest idle after it.
                        idle = Some((
                            idle_message.id.clone(),
                            idle_message.outcome.clone(),
                            idle_message.error.clone(),
                        ));
                    }
                    _ => {}
                }
                scanned.push(message.clone());
            }
            if user_position.is_some() {
                break;
            }
            cursor = page.cursor.next.clone();
            if cursor.is_none() {
                break;
            }
        }
        let Some(user_position) = user_position else {
            return Err(Error::new(
                "unknown",
                "OpenCode terminal readback found no exact user message",
            ));
        };
        let Some((idle_id, outcome, idle_error)) = idle else {
            return Err(Error::new(
                "unknown",
                "OpenCode terminal readback found no terminal idle message",
            ));
        };
        let user_created = match &scanned[user_position] {
            Message::User(user) => user.time.created,
            _ => {
                return Err(Error::new(
                    "unknown",
                    "OpenCode terminal readback found no exact user message",
                ));
            }
        };
        // Assistant text strictly between our input and the terminal idle.
        // `scanned` is newest-first; positions before `user_position` are
        // newer than our input.
        let mut selected: Vec<(i64, String, String)> = texts
            .into_iter()
            .filter(|(created, _, _)| *created > user_created)
            .collect();
        selected.sort();
        let mut final_text = String::new();
        for (_, _, body) in selected {
            if !final_text.is_empty() {
                final_text.push('\n');
            }
            final_text.push_str(&body);
        }
        Ok(TerminalReadback {
            idle_id,
            outcome: match outcome.as_str() {
                "succeeded" => "completed".to_owned(),
                "failed" => "failed".to_owned(),
                _ => "interrupted".to_owned(),
            },
            idle_error,
            final_text: if final_text.is_empty() {
                None
            } else {
                Some(final_text)
            },
        })
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "OpenCode source acknowledgement is unresolved",
            ));
        }
        let native_id = format!(
            "opencode:{}:session.ended",
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
        self.pending = None;
        self.open_inbox_id = None;
        self.stashed_started = None;
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

    /// The shared stream-commit half of this processor.
    fn stream_source(&mut self) -> StreamSource<'_> {
        StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent_id,
            generation: self.generation,
            harness: "OpenCode",
            id_prefix: "opencode",
            families: &["session", "turn", "message"],
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.status.source_unknown,
            batch_unknown: &mut self.status.batch_unknown,
            failure: &mut self.status.failure,
        }
    }
}

struct TerminalReadback {
    idle_id: String,
    outcome: String,
    idle_error: Option<StructuredError>,
    final_text: Option<String>,
}

async fn run(
    mut frames: mpsc::Receiver<SseFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut controls: mpsc::Receiver<Control>,
    status_tx: watch::Sender<EventStatus>,
) {
    loop {
        let accept_frames = processor.accept_frames;
        // Frames win over controls when both are ready: association and
        // terminal handling must observe stream order first.
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
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(control).await {
                            break;
                        }
                        processor.report_gap_if_queue_empty(frames.is_empty()).await;
                    }
                    None => break,
                }
            }
        }
        let _ = status_tx.send(processor.status.clone());
    }
    // Keep the host/client alive until the task ends; dropping them here
    // would close shared connection state early.
    let _ = (&processor.host, &processor.http);
}
