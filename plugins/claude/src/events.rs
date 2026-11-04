use eliot_core::{Error, NativeEvent, Result, SecretPolicy, SourceBatch};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient, native_stdio::StreamState,
    protocol::MAX_COMMAND_ID_BYTES,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    ClaudeClient, ClaudeFrame,
    types::{CanUseToolBody, CommandLifecycle, PermissionResult, ResultRecord, assistant_text},
};

const EVENT_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 16;
const DENY_MESSAGE: &str = "Eliot denied this tool call.";

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

/// Outcome of one `ExpectTurn` wait, decided by the exact stream facts.
pub(crate) enum TurnOutcome {
    Associated,
    Rejected(String),
    Unknown,
}

enum Control {
    Ready {
        native_scope: String,
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    ExpectTurn {
        command_id: String,
        turn_id: String,
        registered: oneshot::Sender<Result<()>>,
        outcome: oneshot::Sender<TurnOutcome>,
    },
    CancelTurn {
        command_id: String,
        reply: oneshot::Sender<()>,
    },
    CheckTurn {
        turn_id: String,
        reply: oneshot::Sender<bool>,
    },
    ReplyRequest {
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
        native: ClaudeClient,
        reply: oneshot::Sender<Result<Value>>,
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
struct BufferedRecord {
    record: Value,
    size: usize,
}

struct PendingTurn {
    command_id: String,
    turn_id: String,
    outcome: Option<oneshot::Sender<TurnOutcome>>,
}

/// One open `can_use_tool` permission, keyed by the CLI's exact native
/// `request_id`. `input` is the original asked value (allow echoes it back
/// verbatim); only the redacted copy is ever committed.
struct PendingNativeRequest {
    turn_id: String,
    tool_name: String,
    tool_use_id: Option<String>,
    input: Value,
    reply_command_id: Option<String>,
    reply_state: NativeReplyState,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Unknown,
}

struct EventProcessor {
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    secrets: Vec<String>,
    status: EventStatus,
    buffer: VecDeque<BufferedRecord>,
    buffer_bytes: usize,
    gap_number: u64,
    pending_turn: Option<PendingTurn>,
    pending_requests: HashMap<String, PendingNativeRequest>,
    message_seq: u64,
    accept_frames: bool,
    planned_stop: bool,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    final_text: Option<String>,
    closed: bool,
    status_tx: watch::Sender<EventStatus>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<ClaudeFrame>,
    stream: watch::Receiver<StreamState>,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (status_tx, status) = watch::channel(EventStatus::default());
    let processor = EventProcessor::new(host, config, status_tx.clone());
    tokio::spawn(run(frames, stream, processor, control_rx, status_tx));
    EventHandle { control, status }
}

impl EventHandle {
    /// Commit readiness only after the SessionStart proof is observed: the
    /// initialize reply and the session_start record race, and readiness
    /// must compare against the observed session, never assume it.
    pub async fn ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        let deadline = Instant::now() + EVENT_DEADLINE;
        loop {
            {
                let status = self.status.borrow();
                if status.session_id.is_some() || status.failure.is_some() {
                    break;
                }
            }
            match timeout_at(deadline, self.status.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(Error::new(
                        "unknown",
                        "Claude event task ended before session proof",
                    ));
                }
                Err(_) => {
                    return Err(Error::new(
                        "unknown",
                        "Claude session proof was not observed before readiness",
                    ));
                }
            }
        }
        self.send(|reply| Control::Ready {
            native_scope,
            session_id,
            effective,
            reply,
        })
        .await
    }

    /// Register one pending input BEFORE its stdin write, then wait for the
    /// exact `command_lifecycle` association. Registration is synchronous so
    /// an early lifecycle can never miss its waiter.
    pub async fn expect_turn(
        &self,
        command_id: String,
        turn_id: String,
    ) -> Result<oneshot::Receiver<TurnOutcome>> {
        let (registered_tx, registered_rx) = oneshot::channel();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        self.control
            .send(Control::ExpectTurn {
                command_id,
                turn_id,
                registered: registered_tx,
                outcome: outcome_tx,
            })
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))?;
        registered_rx
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))??;
        Ok(outcome_rx)
    }

    /// Withdraw a pending turn whose input was proven never dispatched
    /// (rejected), so the session never wedges `busy` behind it. Only the
    /// exact pending command is cleared; an associated turn is untouched.
    /// The ack is awaited so a following send cannot race the clear.
    pub async fn cancel_turn(&self, command_id: String) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .control
            .send(Control::CancelTurn {
                command_id,
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return;
        }
        let _ = reply_rx.await;
    }

    /// Whether `turn_id` is the exact live turn, answered behind any queued
    /// terminal frame so a control never targets a finished turn.
    pub async fn check_turn(&self, turn_id: &str) -> bool {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self
            .control
            .send(Control::CheckTurn {
                turn_id: turn_id.to_owned(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            return false;
        }
        reply_rx.await.unwrap_or(false)
    }

    /// Decide one open `can_use_tool` permission. Runs on the event task,
    /// serialized behind queued frames, so a reply racing the terminal sees
    /// the finished turn. Returns the outcome value (`applied`, `rejected`
    /// or `unknown` state); only a lost task surfaces as `Err`.
    pub async fn reply_request(
        &self,
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
        native: ClaudeClient,
    ) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                native,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))?;
        reply_rx
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))?
    }

    pub async fn begin_stop(&self) -> Result<()> {
        self.send(|reply| Control::BeginStop { reply }).await
    }

    pub async fn end_stop(&self) -> Result<()> {
        self.send(|reply| Control::EndStop { reply }).await
    }

    pub async fn close(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self.control.send(Control::Close { reply: reply_tx }).await;
        let _ = timeout_at(Instant::now() + EVENT_DEADLINE, reply_rx).await;
    }

    pub fn status(&self) -> EventStatus {
        self.status.borrow().clone()
    }

    async fn send(
        &self,
        control: impl FnOnce(oneshot::Sender<Result<()>>) -> Control,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.control
            .send(control(reply_tx))
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))?;
        reply_rx
            .await
            .map_err(|_| Error::new("unknown", "Claude event task is unavailable"))?
    }
}

async fn run(
    mut frames: mpsc::Receiver<ClaudeFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut control: mpsc::Receiver<Control>,
    status: watch::Sender<EventStatus>,
) {
    // Once the native reader ends, its channel stays closed and its stream
    // watch stays dropped: both branches would resolve immediately forever
    // and (biased first) starve every control. Disable them after the drain
    // so `EndStop`/`Close` are still served. This is separate from
    // `accept_frames`, which only diverts trailing records into the buffer.
    let mut frames_open = true;
    loop {
        // Frames win over controls: a queued terminal settles before any
        // `CheckTurn` behind it is answered.
        tokio::select! {
            biased;
            frame = frames.recv(), if frames_open => {
                let Some(frame) = frame else {
                    frames_open = false;
                    // Observe the terminal stream state exactly once: an
                    // overflow that landed with the last frame still gaps.
                    let terminal = *stream.borrow_and_update();
                    processor.note_stream_state(terminal).await;
                    processor.note_frames_drained().await;
                    if processor.closed {
                        break;
                    }
                    publish(&processor, &status);
                    continue;
                };
                let ClaudeFrame::Record(record) = frame;
                if processor.accept_frames {
                    processor.handle_record(record).await;
                } else {
                    processor.buffer_record(record);
                }
            }
            _ = stream.changed(), if frames_open => {
                let state: StreamState = *stream.borrow();
                processor.note_stream_state(state).await;
            }
            request = control.recv() => {
                let Some(request) = request else { break };
                match request {
                    Control::Ready { native_scope, session_id, effective, reply } => {
                        let result = processor.ready(native_scope, session_id, effective).await;
                        publish(&processor, &status);
                        let _ = reply.send(result);
                    }
                    Control::ExpectTurn { command_id, turn_id, registered, outcome } => {
                        let result = processor.register_turn(command_id, turn_id, outcome);
                        publish(&processor, &status);
                        let _ = registered.send(result);
                    }
                    Control::CancelTurn { command_id, reply } => {
                        processor.cancel_turn(&command_id);
                        publish(&processor, &status);
                        let _ = reply.send(());
                    }
                    Control::CheckTurn { turn_id, reply } => {
                        let live = processor.status.active_turn_id.as_deref() == Some(turn_id.as_str())
                            && processor.status.failure.is_none()
                            && !processor.status.stopped;
                        let _ = reply.send(live);
                    }
                    Control::ReplyRequest {
                        command_id,
                        request_id,
                        turn_id,
                        verb,
                        args,
                        native,
                        reply,
                    } => {
                        let result = processor
                            .reply_request(
                                &command_id,
                                &request_id,
                                &turn_id,
                                &verb,
                                &args,
                                &native,
                            )
                            .await;
                        publish(&processor, &status);
                        let _ = reply.send(result);
                    }
                    Control::BeginStop { reply } => {
                        processor.planned_stop = true;
                        processor.accept_frames = false;
                        publish(&processor, &status);
                        let _ = reply.send(Ok(()));
                    }
                    Control::EndStop { reply } => {
                        if processor.frames_drained {
                            let result = processor.finish_stop().await;
                            publish(&processor, &status);
                            let _ = reply.send(result);
                        } else if processor.pending_end_stop.is_some() {
                            let _ = reply.send(Err(Error::new(
                                "unknown",
                                "Claude stop settlement is already pending",
                            )));
                        } else {
                            processor.pending_end_stop = Some(reply);
                        }
                    }
                    Control::Close { reply } => {
                        processor.closed = true;
                        processor.finish_pending_end_stop().await;
                        let _ = reply.send(());
                        break;
                    }
                }
            }
        }
        publish(&processor, &status);
    }
    publish(&processor, &status);
}

fn publish(processor: &EventProcessor, status: &watch::Sender<EventStatus>) {
    let _ = status.send_replace(processor.status.clone());
}

impl EventProcessor {
    fn new(host: PeerClient, config: EventConfig, status_tx: watch::Sender<EventStatus>) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
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
            pending_turn: None,
            pending_requests: HashMap::new(),
            message_seq: 0,
            accept_frames: true,
            planned_stop: false,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            final_text: None,
            closed: false,
            status_tx,
        }
    }

    /// Publish the current status immediately, ahead of the run loop's
    /// end-of-iteration publish. Commits await a host round trip; without
    /// this the host can receive a batch, ack it and snapshot before the
    /// status the batch proves is visible.
    fn publish_now(&self) {
        let _ = self.status_tx.send_replace(self.status.clone());
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
                "Claude readiness requires one exact session and effective settings",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|observed| observed != session_id)
        {
            self.record_gap("Claude stream announced a different session than the minted id")
                .await?;
            return Err(Error::new(
                "source_integrity",
                "Claude session identity did not match its minted session id",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Claude event source acknowledgement is unresolved",
            ));
        }
        if let Some(failure) = self.status.failure.clone() {
            return Err(Error::new(
                "unknown",
                format!("Claude event source failed before readiness: {failure}"),
            ));
        }
        self.status.session_id = Some(session_id.clone());
        let native_id = format!("claude:{session_id}:session.ready");
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
                "Claude event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Claude event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    fn register_turn(
        &mut self,
        command_id: String,
        turn_id: String,
        outcome: oneshot::Sender<TurnOutcome>,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Claude turn association requires a ready exact session and turn",
            ));
        }
        if self.status.active_turn_id.is_some() || self.pending_turn.is_some() {
            return Err(Error::new(
                "busy",
                "Claude already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Claude event source acknowledgement is unresolved",
            ));
        }
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Claude event source has an unresolved gap",
            ));
        }
        self.pending_turn = Some(PendingTurn {
            command_id,
            turn_id,
            outcome: Some(outcome),
        });
        Ok(())
    }

    fn cancel_turn(&mut self, command_id: &str) {
        let pending_matches = self
            .pending_turn
            .as_ref()
            .is_some_and(|pending| pending.command_id == command_id);
        if pending_matches {
            // The input was proven never dispatched, so no lifecycle for its
            // turn can ever arrive; dropping the waiter unwedges the session.
            // The caller already answered, so the dropped outcome is unheard.
            self.pending_turn = None;
        }
    }

    fn buffer_record(&mut self, record: Value) {
        let size = serde_json::to_string(&record).map_or(usize::MAX, |line| line.len());
        if self.buffer.len() >= EARLY_EVENT_LIMIT
            || self.buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
            || size == usize::MAX
        {
            // Overflow is settled as a gap by the next commit path instead of
            // inventing completion; mark the source now.
            self.status.source_unknown = true;
            if self.status.failure.is_none() {
                self.status.failure = Some("Claude early event buffer overflowed".to_owned());
            }
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedRecord { record, size });
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            if self.should_buffer(&frame.record) {
                self.buffer.push_front(frame);
                if let Some(front) = self.buffer.front() {
                    self.buffer_bytes = self.buffer_bytes.saturating_add(front.size);
                }
                break;
            }
            if let Err(error) = self.process_record(frame.record).await {
                self.status.source_unknown = true;
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Buffered Claude native record could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(error.message);
                }
                return Err(Error::new(
                    "unknown",
                    "Buffered Claude event outcome is unresolved",
                ));
            }
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    /// Assistant text may arrive before its lifecycle association; hold it
    /// until the exact turn binds. A permission ask may likewise arrive while
    /// its association is still in flight; hold it for the same reason. A
    /// permission with no association in flight is processed inline (and gaps
    /// outside the exact active turn). Everything else is processed inline.
    fn should_buffer(&self, record: &Value) -> bool {
        let kind = record.get("type").and_then(Value::as_str);
        if kind == Some("assistant") {
            return !self.status.ready
                || (self.status.active_turn_id.is_none() && self.pending_turn.is_some());
        }
        if kind == Some("control_request")
            && record
                .get("request")
                .and_then(|request| request.get("subtype"))
                .and_then(Value::as_str)
                == Some("can_use_tool")
        {
            return self.status.active_turn_id.is_none() && self.pending_turn.is_some();
        }
        false
    }

    async fn handle_record(&mut self, record: Value) {
        if self.should_buffer(&record) {
            self.buffer_record(record);
            return;
        }
        if let Err(error) = self.process_record(record).await {
            self.status.source_unknown = true;
            if self.status.failure.is_none() {
                self.status.failure = Some(error.message);
            }
            return;
        }
        // An association may have unblocked buffered assistant text.
        if let Err(error) = self.flush_buffer().await
            && self.status.failure.is_none()
        {
            self.status.failure = Some(error.message);
        }
    }

    async fn process_record(&mut self, record: Value) -> Result<()> {
        let kind = record
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match kind {
            "system" => self.observe_system(&record).await,
            "command_lifecycle" => self.observe_lifecycle(&record).await,
            "assistant" => self.observe_assistant(&record).await,
            "result" => self.complete_turn(&record).await,
            "user" => Ok(()),
            "control_request" => self.observe_control_request(&record).await,
            _ => Ok(()),
        }
    }

    /// Every `system` line naming a session must name the exact minted id,
    /// whether it is a pre-input SessionStart hook or a post-input init.
    async fn observe_system(&mut self, record: &Value) -> Result<()> {
        let Some(session_id) = record.get("session_id").and_then(Value::as_str) else {
            return Ok(());
        };
        if session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Claude system record carried an empty session id",
            ));
        }
        if let Some(observed) = self.status.session_id.as_deref() {
            if observed != session_id {
                self.record_gap("Claude stream announced a second session in one Agent generation")
                    .await?;
                return Err(Error::new(
                    "source_integrity",
                    "Claude system record did not belong to the owned session",
                ));
            }
            return Ok(());
        }
        if self.status.ready {
            // Ready without a session id cannot happen; treat it as unknown.
            return Err(Error::new(
                "unknown",
                "Claude session identity is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.to_owned());
        Ok(())
    }

    async fn observe_lifecycle(&mut self, record: &Value) -> Result<()> {
        let parsed: CommandLifecycle = serde_json::from_value(record.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "Claude command_lifecycle did not match its envelope",
            )
        })?;
        if parsed.command_uuid.trim().is_empty() || parsed.state.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Claude command_lifecycle omitted its turn association",
            ));
        }
        let pending_match = self
            .pending_turn
            .as_ref()
            .is_some_and(|pending| pending.turn_id == parsed.command_uuid);
        if pending_match {
            return self.settle_pending_lifecycle(parsed).await;
        }
        if self.status.active_turn_id.as_deref() == Some(parsed.command_uuid.as_str()) {
            if parsed.state == "cancelled" {
                return self.complete_cancelled_turn().await;
            }
            return Ok(());
        }
        if self
            .status
            .last_turn
            .as_ref()
            .is_some_and(|last| last.turn_id == parsed.command_uuid)
        {
            return Ok(());
        }
        self.record_gap("Claude lifecycle did not match the exact submitted turn")
            .await
    }

    async fn settle_pending_lifecycle(&mut self, parsed: CommandLifecycle) -> Result<()> {
        let Some(pending) = self.pending_turn.take() else {
            return Ok(());
        };
        let PendingTurn {
            command_id,
            turn_id,
            outcome,
        } = pending;
        let settle = |outcome: Option<oneshot::Sender<TurnOutcome>>, settled: TurnOutcome| {
            if let Some(sender) = outcome {
                let _ = sender.send(settled);
            }
        };
        match parsed.state.as_str() {
            "queued" | "started" => {
                if self.writes_blocked {
                    settle(outcome, TurnOutcome::Unknown);
                    return Err(Error::new(
                        "unknown",
                        "Claude event source acknowledgement is unresolved",
                    ));
                }
                self.status.active_command_id = Some(command_id.clone());
                self.status.active_turn_id = Some(turn_id.clone());
                self.final_text = None;
                self.publish_now();
                let session_id = self.status.session_id.as_deref().unwrap_or_default();
                let native_id = format!("claude:{session_id}:{turn_id}:turn.started");
                let data = json!({
                    "command_id": command_id,
                    "turn_id": turn_id,
                });
                if let Err(error) = self.commit_event("turn.started", native_id, data).await {
                    settle(outcome, TurnOutcome::Unknown);
                    return Err(error);
                }
                settle(outcome, TurnOutcome::Associated);
                // Buffered assistant text drains in the caller's trailing
                // flush, not here, keeping the commit path non-recursive.
                Ok(())
            }
            "cancelled" => {
                settle(
                    outcome,
                    TurnOutcome::Rejected("Claude did not admit the input".to_owned()),
                );
                Ok(())
            }
            _ => {
                settle(outcome, TurnOutcome::Unknown);
                self.record_gap("Claude lifecycle carried an unknown turn state")
                    .await
            }
        }
    }

    fn settle_pending(&mut self, pending: PendingTurn, outcome: TurnOutcome) {
        if let Some(sender) = pending.outcome {
            let _ = sender.send(outcome);
        }
    }

    /// A queued turn the CLI cancels reaches its terminal state instead of
    /// staying active forever. The cancellation is the terminal fact; it
    /// folds through the same commit path as a result record.
    async fn complete_cancelled_turn(&mut self) -> Result<()> {
        let session_id = self.status.session_id.clone().unwrap_or_default();
        self.complete_turn(&json!({
            "type": "result",
            "terminal_reason": "cancelled",
            "session_id": session_id,
        }))
        .await
    }

    async fn observe_assistant(&mut self, record: &Value) -> Result<()> {
        if let Some(session_id) = record.get("session_id").and_then(Value::as_str) {
            self.check_session(session_id)?;
        }
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            if self.pending_turn.is_some() || !self.status.ready {
                self.buffer_record(record.clone());
                return Ok(());
            }
            return self
                .record_gap("Claude message arrived outside the exact active turn")
                .await;
        };
        let text = assistant_text(record);
        if text.is_empty() {
            return Ok(());
        }
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(&text);
        let command_id = self.status.active_command_id.as_deref().unwrap_or_default();
        let data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "text": text.clone(),
        });
        if serde_json::to_vec(&data).map_or(usize::MAX, |bytes| bytes.len()) > MAX_EVENT_BYTES {
            return self
                .record_gap("Claude completed message exceeded the persisted-event bound")
                .await;
        }
        self.message_seq = self.message_seq.saturating_add(1);
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:message:{}", self.message_seq);
        match &mut self.final_text {
            Some(accumulated) => accumulated.push_str(&text),
            None => self.final_text = Some(text),
        }
        self.commit_event("message.completed", native_id, data)
            .await
    }

    /// One CLI `control_request`: only the `can_use_tool` permission ask is
    /// connected. It binds to the exact active turn and opens a retained
    /// request under the CLI's exact native `request_id`; any other subtype
    /// is an explicit gap, as before.
    async fn observe_control_request(&mut self, record: &Value) -> Result<()> {
        let body = record
            .get("request")
            .cloned()
            .map(serde_json::from_value::<CanUseToolBody>)
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Claude control_request omitted its request body",
                )
            })?
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Claude control_request did not match its envelope",
                )
            })?;
        if body.subtype != "can_use_tool" {
            return self
                .record_gap("Claude native request type is unconnected in this slice")
                .await;
        }
        let request_id = record
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Claude can_use_tool omitted its exact native request id",
                )
            })?;
        if body.tool_name.trim().is_empty() || body.input.is_none() {
            return Err(Error::new(
                "native_protocol",
                "Claude can_use_tool omitted its tool name or input",
            ));
        }
        // A re-issued open is idempotent when it repeats the retained ask;
        // the same native id with another ask is an integrity gap.
        if let Some(existing) = self.pending_requests.get(request_id) {
            let same = existing.tool_name == body.tool_name
                && existing.tool_use_id == body.tool_use_id
                && Some(&existing.input) == body.input.as_ref();
            if same {
                return Ok(());
            }
            return self
                .record_gap("Claude permission ask reused its native request id")
                .await;
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            return self
                .record_gap("Claude pending native request bound was exceeded")
                .await;
        }
        let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            return self
                .record_gap("Claude permission ask arrived outside the exact active turn")
                .await;
        };
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let mut details = json!({
            "tool_name": body.tool_name.clone(),
            "tool_use_id": body.tool_use_id.clone(),
            "input": body.input.clone().unwrap_or(Value::Null),
        });
        policy.redact_values(&mut details);
        let data = json!({
            "command_id": command_id.clone(),
            "request_id": request_id,
            "turn_id": turn_id.clone(),
            "kind": "permission",
            "details": details,
        });
        if serde_json::to_vec(&data).map_or(usize::MAX, |bytes| bytes.len()) > MAX_EVENT_BYTES {
            return self
                .record_gap("Claude normalized request exceeded the persisted-event bound")
                .await;
        }
        let native_id = match request_event_id(request_id, "request.opened") {
            Ok(native_id) => native_id,
            Err(_) => {
                return self
                    .record_gap("Claude native request event identity exceeded its bound")
                    .await;
            }
        };
        self.commit_event("request.opened", native_id, data).await?;
        self.pending_requests.insert(
            request_id.to_owned(),
            PendingNativeRequest {
                turn_id,
                tool_name: body.tool_name,
                tool_use_id: body.tool_use_id,
                input: body.input.unwrap_or(Value::Null),
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
            },
        );
        Ok(())
    }

    /// Decide one open permission: write the exact `control_response`, then
    /// commit the exact `request.resolved`. The write has no transport ack,
    /// so a queued write is the delivery; only a proven no-write keeps the
    /// ask available, anything uncertain latches unknown without a rewrite.
    async fn reply_request(
        &mut self,
        command_id: &str,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
        native: &ClaudeClient,
    ) -> Result<Value> {
        if command_id.trim().is_empty() || request_id.trim().is_empty() || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Claude request reply requires exact command, request, and turn ids",
            ));
        }
        let Some(entry) = self.pending_requests.get_mut(request_id) else {
            return Ok(rejected_control("Claude request is no longer pending"));
        };
        if entry.turn_id != turn_id {
            return Ok(rejected_control(
                "Claude request reply does not match the captured turn",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Ok(rejected_control(
                "Claude request is no longer attached to the active turn",
            ));
        }
        match entry.reply_state {
            NativeReplyState::Writing | NativeReplyState::Unknown => {
                return Ok(unknown_control(
                    "Claude request reply outcome is unresolved and will not be repeated",
                ));
            }
            NativeReplyState::Available => {}
        }
        if self.status.session_id.is_none() || self.status.stopped {
            return Ok(rejected_control(
                "Claude request session is no longer writable",
            ));
        }
        if !matches!(verb, "approve" | "deny") {
            return Ok(rejected_control(
                "Claude request reply verb is not implemented",
            ));
        }
        if !args.as_object().is_some_and(|args| args.is_empty()) {
            return Ok(rejected_control(
                "Claude approval replies accept only empty arguments",
            ));
        }
        let result = match verb {
            "approve" => PermissionResult::Allow {
                updated_input: entry.input.clone(),
            },
            _ => PermissionResult::Deny {
                message: DENY_MESSAGE.to_owned(),
            },
        };
        entry.reply_state = NativeReplyState::Writing;
        if let Err(error) = native
            .respond_control(request_id, result, Instant::now() + EVENT_DEADLINE)
            .await
        {
            // Proven not written (bound, encode, or deadline before
            // dispatch): the ask stays available for a later reply.
            if matches!(
                error.code.as_str(),
                "native_not_dispatched" | "frame_too_large" | "native_encode"
            ) {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                }
                return Ok(rejected_control("Claude request reply was not admitted"));
            }
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
                entry.reply_command_id = Some(command_id.to_owned());
            }
            return Ok(unknown_control(
                "Claude request reply may have been dispatched; it will not be repeated",
            ));
        }
        // The decision is delivered: bind the exact resolution to this reply.
        // A failed commit still latches unknown without a rewrite — the CLI
        // already holds the decision.
        let data = json!({
            "command_id": command_id,
            "request_id": request_id,
            "turn_id": turn_id,
        });
        let native_id = match request_event_id(request_id, "request.resolved") {
            Ok(native_id) => native_id,
            Err(_) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                    entry.reply_command_id = Some(command_id.to_owned());
                }
                self.record_gap("Claude native request event identity exceeded its bound")
                    .await?;
                return Ok(unknown_control(
                    "Claude request resolution identity exceeded its bound",
                ));
            }
        };
        if let Err(error) = self.commit_event("request.resolved", native_id, data).await {
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
                entry.reply_command_id = Some(command_id.to_owned());
            }
            return Ok(unknown_control(&format!(
                "Claude request resolution was not committed: {}",
                error.message
            )));
        }
        self.pending_requests.remove(request_id);
        Ok(json!({
            "state": "applied",
            "command_id": command_id,
            "request_id": request_id,
            "turn_id": turn_id,
        }))
    }

    /// Close every retained ask of one finished turn (or of the stopped
    /// session) without a command binding: the CLI ended the turn without
    /// consuming them, so they resolve to nothing. Runs before the terminal
    /// commit it precedes.
    async fn close_pending_requests(&mut self, turn_id: Option<&str>) -> Result<()> {
        let ids: Vec<String> = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| turn_id.is_none_or(|turn| entry.turn_id == turn))
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in ids {
            let Some(entry) = self.pending_requests.get(&request_id) else {
                continue;
            };
            let data = json!({
                "request_id": request_id,
                "turn_id": entry.turn_id,
            });
            let native_id = request_event_id(&request_id, "request.closed")?;
            self.commit_event("request.closed", native_id, data).await?;
            self.pending_requests.remove(&request_id);
        }
        Ok(())
    }

    async fn complete_turn(&mut self, record: &Value) -> Result<()> {
        let parsed: ResultRecord = serde_json::from_value(record.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "Claude result did not match its envelope",
            )
        })?;
        if let Some(session_id) = parsed.session_id.as_deref() {
            self.check_session(session_id)?;
        }
        if parsed.terminal_reason.trim().is_empty() {
            return self
                .record_gap("Claude result omitted its terminal reason")
                .await;
        }
        let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            if let Some(pending) = self.pending_turn.take() {
                // An uncorrelated terminal before association cannot prove the
                // pending input ran; its fate stays unknown.
                self.settle_pending(pending, TurnOutcome::Unknown);
            }
            return self
                .record_gap("Claude terminal record did not match the exact active turn")
                .await;
        };
        // Unanswered asks die with their turn: close them before the terminal
        // commit, without a command binding.
        self.close_pending_requests(Some(&turn_id)).await?;
        if self
            .status
            .last_turn
            .as_ref()
            .is_some_and(|last| last.turn_id == turn_id && last.terminal)
        {
            return Ok(());
        }
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let terminal_reason = secrets.redact_text(&parsed.terminal_reason);
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": turn_id.clone(),
            "status": terminal_reason.clone(),
        });
        if let Some(text) = self.final_text.as_ref() {
            attach_final_text(&mut data, text, &secrets);
        }
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:turn.completed");
        // Publish the observed terminal BEFORE the commit resolves: the host
        // may snapshot as soon as it receives the batch, and a control
        // racing the terminal must see the finished turn (same order as the
        // association path, which binds active before its own commit). A
        // failed commit still latches source_unknown with this terminal.
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id,
            status: Some(terminal_reason),
            final_text: self.final_text.take(),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.publish_now();
        self.commit_event("turn.completed", native_id, data).await?;
        Ok(())
    }

    fn check_session(&self, session_id: &str) -> Result<()> {
        if session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Claude record carried an empty session id",
            ));
        }
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Claude record did not belong to the owned native session",
            ));
        }
        Ok(())
    }

    async fn note_frames_drained(&mut self) {
        self.frames_drained = true;
        if self.planned_stop || self.closed {
            self.finish_pending_end_stop().await;
            return;
        }
        if self.status.stopped {
            return;
        }
        // The reader ended the stream outside a planned stop: the buffered
        // tail is processed first, then the loss is an explicit gap.
        if let Err(error) = self.flush_buffer().await
            && self.status.failure.is_none()
        {
            self.status.failure = Some(error.message);
        }
        if !self.gap_reported && !self.writes_blocked {
            if let Some(pending) = self.pending_turn.take() {
                self.settle_pending(pending, TurnOutcome::Unknown);
            }
            let _ = self
                .record_gap("Claude event stream closed outside a planned stop")
                .await;
        } else if let Some(pending) = self.pending_turn.take() {
            self.settle_pending(pending, TurnOutcome::Unknown);
        }
    }

    async fn note_stream_state(&mut self, state: StreamState) {
        match state {
            StreamState::Open => {}
            StreamState::Overflow => {
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Claude native event queue overflowed")
                        .await;
                }
            }
            StreamState::Closed | StreamState::Malformed => {
                // Settlement happens in `note_frames_drained` once the
                // buffered tail is processed.
            }
        }
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Claude source acknowledgement is unresolved",
            ));
        }
        // The stopped session consumes no more asks: close every retained one
        // before `session.ended`.
        self.close_pending_requests(None).await?;
        let native_id = format!(
            "claude:{}:session.ended",
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
        if let Some(pending) = self.pending_turn.take() {
            self.settle_pending(pending, TurnOutcome::Unknown);
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
                "Claude event acknowledgement is unresolved",
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
            self.status.failure = Some("Claude event batch acknowledgement is unknown".to_owned());
            self.status.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                "Claude event batch acknowledgement is unknown",
            ));
        }
        let reason = format!("{kind} could not be committed: {}", error.code);
        self.record_gap(&reason).await?;
        Err(Error::new(
            "unknown",
            "Claude native fact was not committed; no replay is allowed",
        ))
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        if self.writes_blocked {
            self.status.source_unknown = true;
            return Err(Error::new(
                "unknown",
                "Claude event acknowledgement is unresolved; no source gap was issued",
            ));
        }
        if self.gap_reported {
            return Err(Error::new(
                "unknown",
                "Claude source already has an unresolved gap",
            ));
        }
        self.status.source_unknown = true;
        self.gap_reported = true;
        self.gap_number = self.gap_number.saturating_add(1);
        let gap_id = format!(
            "claude:{}:{}:gap-{}",
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
                self.status.source_unknown = true;
                self.status.batch_unknown = true;
                self.status.failure =
                    Some("Claude source gap acknowledgement is unknown".to_owned());
                Err(Error::new(
                    "unknown",
                    "Claude source gap acknowledgement is unknown",
                ))
            }
        }
    }
}

/// Native identity for one request event under the CLI's exact native
/// `request_id`, bounded like every command identity.
fn request_event_id(request_id: &str, kind: &str) -> Result<String> {
    let prefix = match kind {
        "request.opened" => "claude-o:",
        "request.resolved" => "claude-r:",
        "request.closed" => "claude-c:",
        _ => {
            return Err(Error::new(
                "invalid",
                "Claude request event kind is unsupported",
            ));
        }
    };
    let native_id = format!("{prefix}{request_id}");
    if native_id.len() > MAX_COMMAND_ID_BYTES || request_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Claude native request event identity exceeded its bound",
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

/// Bounded terminal text: when the accumulated reply cannot fit the persisted
/// event, omit it with the explicit marker instead of truncating.
fn attach_final_text(data: &mut Value, text: &str, secrets: &SecretPolicy) {
    let redacted = secrets.redact_text(text);
    let candidate = json!({
        "command_id": data.get("command_id").cloned().unwrap_or(Value::Null),
        "turn_id": data.get("turn_id").cloned().unwrap_or(Value::Null),
        "status": data.get("status").cloned().unwrap_or(Value::Null),
        "final_text": redacted,
    });
    if serde_json::to_vec(&candidate).map_or(usize::MAX, |bytes| bytes.len()) <= MAX_EVENT_BYTES {
        data["final_text"] = Value::String(redacted);
        return;
    }
    let marker = json!({
        "command_id": data.get("command_id").cloned().unwrap_or(Value::Null),
        "turn_id": data.get("turn_id").cloned().unwrap_or(Value::Null),
        "status": data.get("status").cloned().unwrap_or(Value::Null),
        "final_text_unavailable": "event_budget",
    });
    if serde_json::to_vec(&marker).map_or(usize::MAX, |bytes| bytes.len()) <= MAX_EVENT_BYTES {
        data["final_text_unavailable"] = Value::String("event_budget".to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_plugin::Peer;

    fn ready_processor() -> EventProcessor {
        // The writer task pends unwritten; registration and cancel never
        // touch the host, so no fixture I/O is needed.
        let peer = Peer::stdio();
        let (status_tx, _status) = watch::channel(EventStatus::default());
        let mut processor = EventProcessor::new(
            peer.client(),
            EventConfig {
                credential: "test-credential".to_owned(),
                agent_id: "test-agent".to_owned(),
                generation: 1,
                secrets: Vec::new(),
            },
            status_tx,
        );
        processor.status.ready = true;
        processor.status.session_id = Some("test-session".to_owned());
        processor
    }

    #[tokio::test]
    async fn cancelled_pending_turn_unwedges_the_session() {
        let mut processor = ready_processor();
        let (outcome_tx, _outcome_rx) = oneshot::channel();
        processor
            .register_turn("cmd-1".to_owned(), "turn-1".to_owned(), outcome_tx)
            .expect("first registration");
        let (outcome_tx, _outcome_rx) = oneshot::channel();
        let busy = processor
            .register_turn("cmd-2".to_owned(), "turn-2".to_owned(), outcome_tx)
            .expect_err("pending turn wedges the next send");
        assert_eq!(busy.code, "busy");
        // A foreign cancel is a noop; the exact cancel unwedges.
        processor.cancel_turn("cmd-unknown");
        assert!(processor.pending_turn.is_some());
        processor.cancel_turn("cmd-1");
        assert!(processor.pending_turn.is_none());
        let (outcome_tx, _outcome_rx) = oneshot::channel();
        processor
            .register_turn("cmd-2".to_owned(), "turn-2".to_owned(), outcome_tx)
            .expect("registration works after the cancel");
    }
}
