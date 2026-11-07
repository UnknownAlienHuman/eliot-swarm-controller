use eliot_core::{
    Error, Result, SecretPolicy,
    requests::{ApprovalChoice, ApprovalDecision, ApprovalRequest},
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    native_stdio::StreamState,
    session::{
        EVENT_DEADLINE, StreamSource, approval_reply_args, attach_final_text, rejected_control,
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

use crate::native::{
    ClaudeClient, ClaudeFrame,
    types::{
        CanUseToolBody, CommandLifecycle, PermissionResult, ResultRecord, assistant_model,
        assistant_text, result_usage,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 16;
const DENY_MESSAGE: &str = "Eliot denied this tool call.";
const APPROVE_CHOICE_ID: &str = "approve";
const DENY_CHOICE_ID: &str = "deny";

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
        native: ClaudeClient,
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

/// Compose a decision-relevant plain-text summary of a `can_use_tool` ask:
/// the tool name plus its semantic string arguments (command, description,
/// cwd, file/path targets) flattened from the native input. This is plain
/// text, not a native record: transport ids and the verbatim input stay
/// private in the pending struct above. No summary cap beyond the owning
/// event-size bound: truncation would hide decision-critical suffixes and,
/// if done before redaction, could split a configured secret and defeat
/// full-secret redaction. Callers redact this composed text before the
/// existing event-size decision.
fn approval_summary(tool_name: &str, input: Option<&Value>) -> String {
    let mut parts = vec![format!("{tool_name} tool permission request")];
    let mut flattened = Vec::new();
    collect_summary_args(input, &mut flattened);
    flattened.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    for (key, value) in flattened {
        parts.push(format!("{key}: {value}"));
    }
    parts.join("; ")
}

/// Flatten the string leaves of a native input value into owned (key,
/// value) pairs, keeping the field label for string arrays (e.g. a path
/// array reads "paths: <joined>") so operation fields survive without a
/// JSON dump. Dropped: transport envelope ids and any non-string leaf.
fn collect_summary_args(input: Option<&Value>, out: &mut Vec<(String, String)>) {
    collect_summary_args_labeled(None, input, out);
}

fn collect_summary_args_labeled(
    label: Option<&str>,
    input: Option<&Value>,
    out: &mut Vec<(String, String)>,
) {
    let Some(input) = input else { return };
    match input {
        Value::String(text) if !text.trim().is_empty() => {
            if let Some(label) = label {
                out.push((label.to_owned(), text.clone()));
            }
        }
        Value::Object(map) => {
            for (key, value) in map {
                match value {
                    Value::String(text) if !text.trim().is_empty() => {
                        out.push((key.clone(), text.clone()));
                    }
                    Value::Object(_) | Value::Array(_) => {
                        collect_summary_args_labeled(Some(key), Some(value), out)
                    }
                    _ => {}
                }
            }
        }
        Value::Array(items) => {
            let strings: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if !strings.is_empty()
                && let Some(label) = label
            {
                out.push((label.to_owned(), strings.join(", ")));
            }
            for item in items {
                collect_summary_args_labeled(label, Some(item), out);
            }
        }
        _ => {}
    }
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
    deny_uncertain: HashSet<String>,
    permission_admission_fenced: bool,
    message_seq: u64,
    accept_frames: bool,
    planned_stop: bool,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    final_text: Option<String>,
    closed: bool,
    native: Option<ClaudeClient>,
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
    /// must compare against the observed session, never assume it. The
    /// native client travels with readiness so a permission ask outside the
    /// exact active turn still gets its explicit deny.
    pub async fn ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
        native: ClaudeClient,
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
            native,
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
                if dispatch_control(&mut processor, &status, request).await {
                    break;
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

/// Serve one control behind any queued frame. True ends the run loop.
async fn dispatch_control(
    processor: &mut EventProcessor,
    status: &watch::Sender<EventStatus>,
    request: Control,
) -> bool {
    match request {
        Control::Ready {
            native_scope,
            session_id,
            effective,
            native,
            reply,
        } => {
            let result = processor
                .ready(native_scope, session_id, effective, native)
                .await;
            publish(processor, status);
            let _ = reply.send(result);
        }
        Control::ExpectTurn {
            command_id,
            turn_id,
            registered,
            outcome,
        } => {
            let result = processor.register_turn(command_id, turn_id, outcome);
            publish(processor, status);
            let _ = registered.send(result);
        }
        Control::CancelTurn { command_id, reply } => {
            processor.cancel_turn(&command_id);
            publish(processor, status);
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
                .reply_request(&command_id, &request_id, &turn_id, &verb, &args, &native)
                .await;
            publish(processor, status);
            let _ = reply.send(result);
        }
        Control::BeginStop { reply } => {
            processor.planned_stop = true;
            processor.accept_frames = false;
            publish(processor, status);
            let _ = reply.send(Ok(()));
        }
        Control::EndStop { reply } => {
            if processor.frames_drained {
                let result = processor.finish_stop().await;
                publish(processor, status);
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
            return true;
        }
    }
    false
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
            deny_uncertain: HashSet::new(),
            permission_admission_fenced: false,
            message_seq: 0,
            accept_frames: true,
            planned_stop: false,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            final_text: None,
            closed: false,
            native: None,
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
        native: ClaudeClient,
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
        self.native = Some(native);
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
        let observed_model = assistant_model(record);
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(&text);
        let command_id = self.status.active_command_id.as_deref().unwrap_or_default();
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "text": text.clone(),
        });
        if let Some(model) = observed_model {
            data["model"] = Value::String(model);
        }
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

    /// One native callback exists per request ID; a possible write fences it.
    /// Neither a conflicting ask nor a turn result proves that ID safe to reuse.
    async fn deny_stray_request(&mut self, request_id: &str) {
        if self.permission_admission_fenced || self.deny_uncertain.contains(request_id) {
            return;
        }
        if self.pending_requests.get(request_id).is_some_and(|entry| {
            matches!(
                entry.reply_state,
                NativeReplyState::Writing | NativeReplyState::Unknown
            )
        }) {
            self.retain_uncertain_id(request_id).await;
            return;
        }
        let Some(native) = self.native.clone() else {
            return;
        };
        if self.status.session_id.is_none() || self.status.stopped {
            return;
        }
        if !self.retain_uncertain_id(request_id).await {
            return;
        }
        let result = native
            .respond_control(
                request_id,
                PermissionResult::Deny {
                    message: DENY_MESSAGE.to_owned(),
                },
                Instant::now() + EVENT_DEADLINE,
            )
            .await;
        if let Err(error) = result
            && matches!(
                error.code.as_str(),
                "native_not_dispatched" | "frame_too_large" | "native_encode"
            )
        {
            self.deny_uncertain.remove(request_id);
        }
    }

    async fn retain_uncertain_id(&mut self, request_id: &str) -> bool {
        if self.permission_admission_fenced {
            return false;
        }
        if self.deny_uncertain.contains(request_id) {
            return true;
        }
        if self.deny_uncertain.len() >= MAX_PENDING_REQUESTS {
            self.fence_permission_session().await;
            return false;
        }
        self.deny_uncertain.insert(request_id.to_owned());
        true
    }

    async fn fence_permission_session(&mut self) {
        if self.permission_admission_fenced {
            return;
        }
        self.permission_admission_fenced = true;
        self.status.source_unknown = true;
        self.status.failure = Some(
            "Claude permission identity tracking is exhausted; native outcome is unknown"
                .to_owned(),
        );
        self.publish_now();
        if !self.gap_reported && !self.writes_blocked {
            let _ = self
                .record_gap("Claude permission identity tracking is exhausted")
                .await;
        }
        if let Some(native) = self.native.clone() {
            // EOF fences further callback delivery. Only host custody/stop can
            // establish process exit and publish session.ended after its drain.
            let _ = native.shutdown_input(Instant::now() + EVENT_DEADLINE).await;
        }
    }

    async fn retain_unknown_reply(&mut self, request_id: &str, command_id: &str) {
        if let Some(entry) = self.pending_requests.get_mut(request_id) {
            entry.reply_state = NativeReplyState::Unknown;
            entry.reply_command_id = Some(command_id.to_owned());
        }
        self.retain_uncertain_id(request_id).await;
    }

    /// Identical retained asks are no-ops. A conflicting ID cannot establish
    /// whether the original or replacement callback would consume a decision;
    /// an in-flight or uncertain owner reply therefore forbids a second write.
    async fn handle_retained_duplicate(
        &mut self,
        request_id: &str,
        body: &CanUseToolBody,
    ) -> Option<Result<()>> {
        let existing = self.pending_requests.get(request_id)?;
        let same = existing.tool_name == body.tool_name
            && existing.tool_use_id == body.tool_use_id
            && Some(&existing.input) == body.input.as_ref();
        if same {
            return Some(Ok(()));
        }
        if self.deny_uncertain.contains(request_id)
            || matches!(
                existing.reply_state,
                NativeReplyState::Writing | NativeReplyState::Unknown
            )
        {
            if let Some(entry) = self.pending_requests.get_mut(request_id) {
                entry.reply_state = NativeReplyState::Unknown;
            }
            self.retain_uncertain_id(request_id).await;
            return Some(
                self.record_gap("Claude permission ask reused a fenced native request id")
                    .await,
            );
        }
        self.deny_stray_request(request_id).await;
        if let Some(entry) = self.pending_requests.get_mut(request_id) {
            entry.reply_state = NativeReplyState::Unknown;
        }
        Some(
            self.record_gap("Claude permission ask reused its native request id")
                .await,
        )
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
        if self.permission_admission_fenced {
            return Err(Error::new(
                "unknown",
                "Claude native permission admission is fenced",
            ));
        }
        if let Some(result) = self.handle_retained_duplicate(request_id, &body).await {
            return result;
        }
        if self.deny_uncertain.contains(request_id) {
            return self
                .record_gap("Claude permission ask reused a fenced native request id")
                .await;
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.deny_stray_request(request_id).await;
            return self
                .record_gap("Claude pending native request bound was exceeded")
                .await;
        }
        let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            self.deny_stray_request(request_id).await;
            return self
                .record_gap("Claude permission ask arrived outside the exact active turn")
                .await;
        };
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        // The public approval fact carries only a neutral summary (the
        // decision-relevant operation: command/description/cwd and file/path
        // targets from the native ask, composed as plain text, not a native
        // record. The native transport ids and the full native input stay
        // private in the retained pending struct below. The composed
        // summary is redacted before commit and bounded; an oversized event
        // takes the existing event-bound gap path via the size check below.
        let summary = policy.redact_text(&approval_summary(&body.tool_name, body.input.as_ref()));
        let approval = ApprovalRequest {
            summary,
            choices: vec![
                ApprovalChoice {
                    id: APPROVE_CHOICE_ID.to_owned(),
                    label: "Approve".to_owned(),
                    decision: ApprovalDecision::Approve,
                    feedback_allowed: false,
                },
                ApprovalChoice {
                    id: DENY_CHOICE_ID.to_owned(),
                    label: "Deny".to_owned(),
                    decision: ApprovalDecision::Deny,
                    feedback_allowed: false,
                },
            ],
            feedback_allowed: false,
        };
        let details = serde_json::to_value(&approval)
            .map_err(|_| Error::new("internal", "Claude approval details could not be encoded"))?;
        let data = json!({
            "command_id": command_id.clone(),
            "request_id": request_id,
            "turn_id": turn_id.clone(),
            "kind": "approval",
            "details": details,
        });
        if serde_json::to_vec(&data).map_or(usize::MAX, |bytes| bytes.len()) > MAX_EVENT_BYTES {
            self.deny_stray_request(request_id).await;
            return self
                .record_gap("Claude normalized request exceeded the persisted-event bound")
                .await;
        }
        let native_id = match request_event_id(request_id, "request.opened", "claude", "Claude") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.deny_stray_request(request_id).await;
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

    /// Refuse a stale, unbound, or malformed request reply before any
    /// native write, else plan the exact `control_response` decision. `Err`
    /// is the early outcome value to answer with.
    fn check_request_reply(
        &self,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
    ) -> std::result::Result<PermissionResult, Value> {
        if self.permission_admission_fenced {
            return Err(unknown_control(
                "Claude native permission admission is fenced and unresolved",
            ));
        }
        let Some(entry) = self.pending_requests.get(request_id) else {
            return Err(rejected_control("Claude request is no longer pending"));
        };
        if entry.turn_id != turn_id {
            return Err(rejected_control(
                "Claude request reply does not match the captured turn",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Err(rejected_control(
                "Claude request is no longer attached to the active turn",
            ));
        }
        match entry.reply_state {
            NativeReplyState::Writing | NativeReplyState::Unknown => {
                return Err(unknown_control(
                    "Claude request reply outcome is unresolved and will not be repeated",
                ));
            }
            NativeReplyState::Available => {}
        }
        if self.status.session_id.is_none() || self.status.stopped {
            return Err(rejected_control(
                "Claude request session is no longer writable",
            ));
        }
        if !matches!(verb, "approve" | "deny") {
            return Err(rejected_control(
                "Claude request reply verb is not implemented",
            ));
        }
        let args = match approval_reply_args(args) {
            Ok(args) => args,
            Err(error) => return Err(rejected_control(&error.message)),
        };
        let choice_id = match args.get("choice_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(choice)) if !choice.trim().is_empty() => Some(choice.as_str()),
            Some(_) => {
                return Err(rejected_control(
                    "Claude approval choice_id must be a non-empty string",
                ));
            }
        };
        // The binary native offer: an explicit canonical choice, when
        // provided, must name one of these two ids and must match the verb.
        // No autochoice: an omitted choice follows the verb, as before, and
        // a choice that disagrees with the verb is rejected, never silently
        // remapped to the other native decision.
        let decision = match (verb, choice_id) {
            ("approve", None | Some("approve")) => PermissionResult::Allow {
                updated_input: entry.input.clone(),
            },
            ("deny", None | Some("deny")) => PermissionResult::Deny {
                message: DENY_MESSAGE.to_owned(),
            },
            _ => {
                return Err(rejected_control(
                    "Claude approval choice does not match the reply verb",
                ));
            }
        };
        if args.contains_key("feedback") {
            return Err(rejected_control("Claude approval feedback is not allowed"));
        }
        if self.frames_drained && !self.planned_stop {
            return Err(unknown_control(
                "Claude event stream was lost before the request reply; outcome is unresolved and will not be repeated",
            ));
        }
        Ok(decision)
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
        let result = match self.check_request_reply(request_id, turn_id, verb, args) {
            Ok(result) => result,
            Err(outcome) => return Ok(outcome),
        };
        // The destination may be unavailable after admission but before
        // any write: an unavailable destination is unknown, never a live
        // refusal, and no permission frame is attempted.
        if native.process_unavailable() {
            self.retain_unknown_reply(request_id, command_id).await;
            return Ok(unknown_control(
                "Claude request reply destination is unavailable before dispatch; it will not be repeated",
            ));
        }
        let Some(entry) = self.pending_requests.get_mut(request_id) else {
            return Ok(rejected_control("Claude request is no longer pending"));
        };
        entry.reply_state = NativeReplyState::Writing;
        if let Err(error) = native
            .respond_control(request_id, result, Instant::now() + EVENT_DEADLINE)
            .await
        {
            // Proven not written (bound, encode, or deadline before
            // dispatch): the ask stays available for a later reply. An
            // unavailable destination under the write leaves a stale ask:
            // latch unknown instead of restoring availability.
            if matches!(
                error.code.as_str(),
                "native_not_dispatched" | "frame_too_large" | "native_encode"
            ) {
                if native.process_unavailable() {
                    self.retain_unknown_reply(request_id, command_id).await;
                    return Ok(unknown_control(
                        "Claude request reply destination is unavailable; it will not be repeated",
                    ));
                }
                if let Some(entry) = self.pending_requests.get_mut(request_id)
                    && entry.reply_state == NativeReplyState::Writing
                {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                }
                return Ok(rejected_control("Claude request reply was not admitted"));
            }
            self.retain_unknown_reply(request_id, command_id).await;
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
        let native_id = match request_event_id(request_id, "request.resolved", "claude", "Claude") {
            Ok(native_id) => native_id,
            Err(_) => {
                self.retain_unknown_reply(request_id, command_id).await;
                self.record_gap("Claude native request event identity exceeded its bound")
                    .await?;
                return Ok(unknown_control(
                    "Claude request resolution identity exceeded its bound",
                ));
            }
        };
        if let Err(error) = self.commit_event("request.resolved", native_id, data).await {
            self.retain_unknown_reply(request_id, command_id).await;
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
            let native_id = request_event_id(&request_id, "request.closed", "claude", "Claude")?;
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
        // Callback fences outlive turn completion while this transport exists.
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
        // Optional native main-loop turn usage: absent/malformed usage never
        // blocks or gaps this terminal; it simply leaves `data.usage` unset.
        // Attaches before `attach_final_text` so the existing bounded
        // terminal-text accounting includes the usage bytes.
        if let Some(usage) = result_usage(record) {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
        }
        if let Some(text) = self.final_text.as_ref() {
            // Stream-json carries no failed flag: keep the PR1 omit-on-overflow
            // behavior for every terminal by reporting the bound case as failed.
            attach_final_text(&mut data, &secrets.redact_text(text), true, &secrets);
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
        // The CLI died with these asks open: latch every still-available
        // one unknown so later replies never attempt native I/O. The
        // retained asks still close with the turn or stop.
        let lost: Vec<String> = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| entry.reply_state == NativeReplyState::Available)
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in lost {
            if let Some(entry) = self.pending_requests.get_mut(&request_id) {
                entry.reply_state = NativeReplyState::Unknown;
            }
            self.retain_uncertain_id(&request_id).await;
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
            StreamState::Closed | StreamState::Malformed(_) => {
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
        // Keep generation fences until this drained processor is dropped.
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
            harness: "Claude",
            id_prefix: "claude",
            families: &["session", "turn", "message", "request"],
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.status.source_unknown,
            batch_unknown: &mut self.status.batch_unknown,
            failure: &mut self.status.failure,
        }
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
