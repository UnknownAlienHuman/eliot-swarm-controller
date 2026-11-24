mod hooks;
pub(crate) use hooks::TOOL_HOOK_TIMEOUT_SECONDS;

use self::hooks::{
    HookToolFacts, approval_summary, hook_callback_response_fits, hook_deadline, hook_tool_facts,
};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, Result, SecretPolicy, TurnUsage,
    limits::{MAX_COMMAND_ID_BYTES, MAX_TOOL_COMMAND_BYTES, NATIVE_TERMINAL_DEADLINE_REASON},
    requests::{ApprovalChoice, ApprovalDecision, ApprovalRequest},
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    event_actor::EventActor,
    event_buffer::EarlyEventBuffer,
    native_stdio::StreamState,
    protocol::{HookEvent, HookResponse},
    session::{
        EVENT_DEADLINE, StreamSource, approval_reply_args, attach_final_text, attach_native_error,
        is_blank_native_text, native_no_write, native_terminal_inactivity, next_input_outcomes,
        rejected_control, request_event_id, request_reply_applied, unknown_control,
        with_claimed_context,
    },
    usage::FirstCallUsage,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::native::{
    ClaudeClient, ClaudeFrame,
    types::{
        CanUseToolBody, CommandLifecycle, ControlCancelRequest, HookCallbackBody,
        HookCallbackResponse, InitMcpServer, PermissionResult, ResultRecord, assistant_call_input,
        assistant_model, assistant_text, assistant_tool_use_count, assistant_tool_use_ids,
        assistant_tool_uses, attach_mcp_readback, init_mcp_servers, result_session_usage,
        result_usage, transcript_config_roots, transcript_usage_in, user_tool_results,
    },
};

const CONTROL_QUEUE: usize = 16;
const MAX_PENDING_REQUESTS: usize = 16;
const MAX_PENDING_HOOKS: usize = 16;
const MAX_TOOL_USE_BINDINGS: usize = 128;
const MAX_BACKGROUND_TOOLS: usize = 64;
const DENY_MESSAGE: &str = "Eliot denied this tool call.";
const APPROVE_CHOICE_ID: &str = "approve";
const DENY_CHOICE_ID: &str = "deny";

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub native_drained: bool,
    pub background_continuation: bool,
    pub ready: bool,
    pub session_id: Option<String>,
    /// The send bound to the active turn, if any.
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    /// The active turn is self-started and no send has bound to it, so it
    /// can take one input and its facts name no command.
    pub native_unbound: bool,
    pub wake_cancel_pending: bool,
    pub last_turn: Option<TurnFacts>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    pub stopped: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TurnFacts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    pub turn_id: String,
    /// Universal terminal status (`completed`, `interrupted` or `failed`);
    /// absent while non-terminal or when the native reason maps to no
    /// known outcome.
    pub status: Option<String>,
    /// The native `result.terminal_reason`, redacted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_status: Option<String>,
    pub final_text: Option<String>,
    pub terminal: bool,
    /// The terminal is published before its own `turn.completed` write
    /// resolves (a send racing the terminal must see the finished turn);
    /// true until that write commits or fails, so recovery never races it.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub write_pending: bool,
}

pub(crate) struct EventConfig {
    pub credential: String,
    pub agent_id: String,
    pub generation: i64,
    pub secrets: Vec<String>,
    pub native: ClaudeClient,
    /// Owned session workspace: hook tool paths are normalized against it.
    pub workspace: PathBuf,
}

pub(crate) struct EventHandle {
    actor: EventActor<Control, EventStatus>,
    hook_callback_id: String,
}

/// Outcome of one `ExpectTurn` wait, decided by the exact stream facts.
pub(crate) enum TurnOutcome {
    /// Bound to this exact turn: its own, or the running native turn the
    /// CLI merged the input into.
    Associated(String),
    Rejected(String),
    Unknown(String),
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
        context: Vec<ContextItem>,
        registered: oneshot::Sender<Result<()>>,
        outcome: oneshot::Sender<TurnOutcome>,
    },
    CancelTurn {
        command_id: String,
        reply: oneshot::Sender<()>,
    },
    CheckTurn {
        turn_id: String,
        reply: oneshot::Sender<Option<bool>>,
    },
    MarkInterrupted {
        turn_id: String,
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
    HookResponseReady {
        request_id: String,
        consumer_id: String,
        result: Result<Vec<ContextItem>>,
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

struct PendingTurn {
    command_id: String,
    turn_id: String,
    overlaps_native: bool,
    context: Vec<ContextItem>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeTerminalReason {
    Success,
    Interrupted,
    Failure,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativeControlRequestSubtype {
    CanUseTool,
    HookCallback,
    Other,
}

impl NativeControlRequestSubtype {
    fn from_native(value: &str) -> Self {
        match value {
            "can_use_tool" => Self::CanUseTool,
            "hook_callback" => Self::HookCallback,
            _ => Self::Other,
        }
    }
}

/// Classify one Claude terminal reason without assigning an outcome to a new
/// native value. The raw reason remains available for the neutral diagnostic
/// and `native_status` readback.
fn native_terminal_reason(reason: &str) -> NativeTerminalReason {
    match reason.trim() {
        "success" | "completed" => NativeTerminalReason::Success,
        "cancelled" | "interrupted" | "aborted_streaming" | "aborted_tools" => {
            NativeTerminalReason::Interrupted
        }
        "error" | "failed" | "failure" => NativeTerminalReason::Failure,
        reason => {
            let lower = reason.to_lowercase();
            if lower.starts_with("api_error")
                || lower.starts_with("error")
                || lower.starts_with("failed")
                || lower.starts_with("failure")
            {
                NativeTerminalReason::Failure
            } else {
                NativeTerminalReason::Other
            }
        }
    }
}

/// Canonical outcome for one Claude terminal reason, decided here at the
/// plugin boundary; unrecognized reasons remain explicitly unknown.
fn claude_outcome(reason: &str) -> &'static str {
    match native_terminal_reason(reason) {
        NativeTerminalReason::Success => "success",
        NativeTerminalReason::Interrupted => "interrupted",
        NativeTerminalReason::Failure => "failure",
        NativeTerminalReason::Other => "unknown",
    }
}

/// The universal terminal status of a canonical outcome; `unknown` has none.
fn universal_status(outcome: &str) -> Option<String> {
    match outcome {
        "success" => Some("completed"),
        "interrupted" => Some("interrupted"),
        "failure" => Some("failed"),
        _ => None,
    }
    .map(str::to_owned)
}

/// Classify the original terminal reason first, then redact recognized values
/// for persistence: a configured secret must not change its outcome. An
/// unrecognized value is persisted only as `other`; its bounded diagnostic is
/// emitted separately.
fn terminal_outcome_and_status(
    reason: &str,
    secrets: &SecretPolicy,
    failure: Option<&Error>,
) -> (&'static str, String) {
    let terminal_reason = native_terminal_reason(reason);
    let mut outcome = claude_outcome(reason);
    if outcome == "success" && failure.is_some_and(|error| error.code == "quota_exhausted") {
        outcome = "failure";
    }
    let status = if terminal_reason == NativeTerminalReason::Other {
        "other".to_owned()
    } else {
        secrets.redact_text(reason)
    };
    (outcome, status)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HookConsumerState {
    Available,
    InFlight,
    Finished,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HookPoint {
    BeforeTool,
    AfterTool,
}

impl HookPoint {
    fn native_event_name(self) -> &'static str {
        match self {
            Self::BeforeTool => "PreToolUse",
            Self::AfterTool => "PostToolUse",
        }
    }

    fn event_kind(self) -> &'static str {
        match self {
            Self::BeforeTool => "hook.before_tool",
            Self::AfterTool => "hook.after_tool",
        }
    }

    fn event_name(self) -> &'static str {
        match self {
            Self::BeforeTool => "before_tool",
            Self::AfterTool => "after_tool",
        }
    }
}

#[derive(Clone)]
struct ToolUseBinding {
    turn_id: String,
    prompt_id: Option<String>,
    before_hook_state: HookConsumerState,
    before_consumer_id: Option<String>,
    after_hook_state: HookConsumerState,
    after_consumer_id: Option<String>,
}

impl ToolUseBinding {
    fn hook_state(&self, hook: HookPoint) -> HookConsumerState {
        match hook {
            HookPoint::BeforeTool => self.before_hook_state,
            HookPoint::AfterTool => self.after_hook_state,
        }
    }

    fn is_active_consumer(&self, hook: HookPoint, consumer_id: &str) -> bool {
        let active = match hook {
            HookPoint::BeforeTool => self.before_consumer_id.as_deref(),
            HookPoint::AfterTool => self.after_consumer_id.as_deref(),
        };
        active == Some(consumer_id)
    }

    fn start_hook(&mut self, hook: HookPoint, consumer_id: String) {
        match hook {
            HookPoint::BeforeTool => {
                self.before_hook_state = HookConsumerState::InFlight;
                self.before_consumer_id = Some(consumer_id);
            }
            HookPoint::AfterTool => {
                self.after_hook_state = HookConsumerState::InFlight;
                self.after_consumer_id = Some(consumer_id);
            }
        }
    }

    fn finish_hook(&mut self, hook: HookPoint, consumer_id: &str) {
        if !self.is_active_consumer(hook, consumer_id) {
            return;
        }
        match hook {
            HookPoint::BeforeTool => {
                self.before_hook_state = HookConsumerState::Finished;
                self.before_consumer_id = None;
            }
            HookPoint::AfterTool => {
                self.after_hook_state = HookConsumerState::Finished;
                self.after_consumer_id = None;
            }
        }
    }

    fn finish_available_hook(&mut self, hook: HookPoint) {
        match hook {
            HookPoint::BeforeTool if self.before_hook_state == HookConsumerState::Available => {
                self.before_hook_state = HookConsumerState::Finished;
            }
            HookPoint::AfterTool if self.after_hook_state == HookConsumerState::Available => {
                self.after_hook_state = HookConsumerState::Finished;
            }
            _ => {}
        }
    }
}

struct PendingHookCallback {
    generation: i64,
    hook: HookPoint,
    turn_id: String,
    tool_use_id: String,
    consumer_id: String,
    tool: HookToolFacts,
    deadline: Instant,
    native: ClaudeClient,
    task: JoinHandle<()>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Unknown,
}

struct PendingRateLimitEvent {
    session_id: String,
    turn_id: String,
    retry_after: Option<i64>,
}

struct EventProcessor {
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    hook_callback_id: String,
    secrets: Vec<String>,
    workspace: PathBuf,
    status: EventStatus,
    buffer: EarlyEventBuffer<Value>,
    gap_number: u64,
    pending_turn: Option<PendingTurn>,
    empty_after_tool_hooks: u64,
    interrupted_turn: Option<String>,
    pending_requests: HashMap<String, PendingNativeRequest>,
    cancelled_hook_requests: HashSet<String>,
    tool_use_turns: HashMap<String, ToolUseBinding>,
    tool_use_tracking_fenced: bool,
    /// Native tool names by invocation id for the exact active turn, taken
    /// from the assistant `tool_use` blocks. The matching `user`
    /// `tool_result` blocks carry no name, so completions read it back here.
    tool_names: HashMap<String, String>,
    /// Invocations with a committed `tool.started` in this turn.
    tools_started: HashSet<String>,
    /// Invocations with a committed `tool.completed` in this turn.
    tools_completed: HashSet<String>,
    /// Running `run_in_background` calls and the turn that started each.
    /// Unlike the per-turn sets, these outlive their turn.
    background_tools: HashMap<String, String>,
    /// At most one tool-coverage gap per turn; it does not invalidate the
    /// separately associated native terminal or future input.
    tool_coverage_gap_reported: bool,
    /// A tracked background task ended, so Claude wakes its main loop without
    /// a SEND: the next init with no input UUID is that self-started turn.
    task_notification_pending: bool,
    /// Input UUID the active turn must answer before it can end. A native
    /// result that does not name it only ends the merged prefix.
    active_input: Option<String>,
    /// Input merged into the last finished turn, for its late lifecycle.
    last_input: Option<String>,
    /// A native result ended while the bound input was still queued.
    native_prefix_ended: bool,
    /// Main-loop usage of those earlier native results in this turn.
    prefix_usage: Option<TurnUsage>,
    deny_uncertain: HashSet<String>,
    permission_admission_fenced: bool,
    message_seq: u64,
    unclassified_sequence: u64,
    accept_frames: bool,
    planned_stop: bool,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    terminal_deadline: Option<Instant>,
    terminal_inactivity: std::time::Duration,
    /// Sticky first record-local fault awaiting its gap (planned-stop drain
    /// must not erase it if the immediate record was blocked).
    pending_fault_gap: Option<String>,
    final_text: Option<String>,
    native_failure: Option<Error>,
    /// Exact assistant error fact retained only through its bound terminal.
    native_provider_restriction: Option<eliot_core::ProviderRestriction>,
    /// Exact SDK rate-limit observation bound to the session and active turn.
    native_rate_limit_event: Option<PendingRateLimitEvent>,
    /// Proven MCP surface from the owned stream's `system`/`init` record
    /// (names plus statuses). Retained on observation and attached to the
    /// next turn terminal so the reducer merges it into the effective set;
    /// absent until the first post-input init arrives.
    init_mcp_servers: Option<Vec<InitMcpServer>>,
    /// Served model and native message ID of the latest main-thread call.
    latest_context_message: Option<(String, String)>,
    first_call_usage: FirstCallUsage,
    closed: bool,
    native: Option<ClaudeClient>,
    status_tx: watch::Sender<EventStatus>,
    control_actor: EventActor<Control, EventStatus>,
    pending_hooks: HashMap<String, PendingHookCallback>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<ClaudeFrame>,
    stream: watch::Receiver<StreamState>,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let hook_callback_id = ulid::Ulid::new().to_string();
    let processor_callback_id = hook_callback_id.clone();
    let actor = EventActor::spawn_with_actor(
        CONTROL_QUEUE,
        EventStatus::default(),
        move |control_actor, control_rx, status_tx| async move {
            let processor = EventProcessor::new(
                host,
                config,
                processor_callback_id,
                status_tx.clone(),
                control_actor,
            );
            run(frames, stream, processor, control_rx, status_tx).await;
        },
    );
    EventHandle {
        actor,
        hook_callback_id,
    }
}

impl EventHandle {
    /// Private callback id installed by this session's native initialize.
    pub fn hook_callback_id(&self) -> &str {
        &self.hook_callback_id
    }

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
        let mut status = self.actor.watch().clone();
        loop {
            {
                let current = status.borrow();
                if current.session_id.is_some() || current.failure.is_some() {
                    break;
                }
            }
            match timeout_at(deadline, status.changed()).await {
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
        self.actor
            .call(|reply| Control::Ready {
                native_scope,
                session_id,
                effective,
                native,
                reply,
            })
            .await?
    }

    /// Register one pending input BEFORE its stdin write, then wait for the
    /// exact `command_lifecycle` association. Registration is synchronous so
    /// an early lifecycle can never miss its waiter.
    pub async fn expect_turn(
        &self,
        command_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
    ) -> Result<oneshot::Receiver<TurnOutcome>> {
        let mut status = self.actor.watch().clone();
        loop {
            status
                .wait_for(|view| {
                    !view.wake_cancel_pending || view.failure.is_some() || view.stopped
                })
                .await
                .map_err(|_| {
                    Error::new(
                        "unavailable",
                        "Claude event task closed before input registration",
                    )
                })?;
            let (outcome_tx, outcome_rx) = oneshot::channel();
            match self
                .actor
                .call(|registered| Control::ExpectTurn {
                    command_id: command_id.clone(),
                    turn_id: turn_id.clone(),
                    context: context.clone(),
                    registered,
                    outcome: outcome_tx,
                })
                .await?
            {
                Ok(()) => return Ok(outcome_rx),
                // The actor did not reserve this input. A wake that started
                // after the watch check can only defer this pre-write step.
                Err(error) if error.code == "wake_cancel_pending" => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Withdraw a pending turn whose input was proven never dispatched
    /// (rejected), so the session never wedges `busy` behind it. Only the
    /// exact pending command is cleared; an associated turn is untouched.
    /// The ack is awaited so a following send cannot race the clear.
    pub async fn cancel_turn(&self, command_id: String) {
        let _ = self
            .actor
            .call(|reply| Control::CancelTurn { command_id, reply })
            .await;
    }

    /// The exact live turn's interrupt status, or None if the turn ended.
    /// An actor timeout or loss remains an error, never proof of a finished turn.
    pub async fn turn_interrupt_status(&self, turn_id: &str) -> Result<Option<bool>> {
        self.actor
            .call(|reply| Control::CheckTurn {
                turn_id: turn_id.to_owned(),
                reply,
            })
            .await
    }

    pub async fn mark_interrupted(&self, turn_id: &str) {
        self.actor
            .deliver(Control::MarkInterrupted {
                turn_id: turn_id.to_owned(),
            })
            .await;
    }

    /// Decide one open `can_use_tool` permission. Runs on the event task,
    /// serialized behind queued frames, so a reply racing the terminal sees
    /// the finished turn. Returns the outcome value (`applied`, `rejected`
    /// or `unknown` state); a lost task or expired event-control deadline
    /// surfaces as `Err`.
    pub async fn reply_request(
        &self,
        command_id: String,
        request_id: String,
        turn_id: String,
        verb: String,
        args: Value,
        native: ClaudeClient,
    ) -> Result<Value> {
        self.actor
            .call(|reply| Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                native,
                reply,
            })
            .await?
    }

    pub async fn begin_stop(&self) -> Result<()> {
        self.send(|reply| Control::BeginStop { reply }).await
    }

    pub async fn end_stop(&self) -> Result<()> {
        self.send(|reply| Control::EndStop { reply }).await
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
        let mut status = self.actor.watch().borrow().clone();
        if self.actor.is_closed() && status.failure.is_none() {
            status.failure = Some("Claude event task is unavailable".to_owned());
        }
        status
    }

    async fn send(
        &self,
        control: impl FnOnce(oneshot::Sender<Result<()>>) -> Control,
    ) -> Result<()> {
        self.actor.call(control).await?
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
    let mut stream_open = true;
    loop {
        let terminal_deadline = processor.terminal_deadline;
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
            changed = stream.changed(), if frames_open && stream_open => {
                if changed.is_err() {
                    stream_open = false;
                }
                let state: StreamState = *stream.borrow();
                processor.note_stream_state(state).await;
            }
            _ = eliot_plugin::retry::wait_until(terminal_deadline) => {
                if let Err(error) = processor.expire_terminal_deadline().await {
                    processor.status.source_unknown = true;
                    processor.status.failure.get_or_insert(*error.message);
                }
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
    processor.cancel_all_hook_callbacks();
    publish(&processor, &status);
}

fn publish(processor: &EventProcessor, status: &watch::Sender<EventStatus>) {
    let mut current = processor.status.clone();
    current.native_drained = processor.frames_drained;
    let _ = status.send_replace(current);
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
            context,
            registered,
            outcome,
        } => {
            let mut result = processor.register_turn(command_id, turn_id, outcome);
            if result.is_ok() {
                if let Some(pending) = processor.pending_turn.as_mut() {
                    pending.context = context;
                } else {
                    result = Err(Error::new(
                        "internal",
                        "Claude pending turn disappeared during context registration",
                    ));
                }
            }
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
            let _ = reply.send(
                live.then(|| processor.interrupted_turn.as_deref() == Some(turn_id.as_str())),
            );
        }
        Control::MarkInterrupted { turn_id } => {
            if processor.status.active_turn_id.as_deref() == Some(turn_id.as_str()) {
                processor.interrupted_turn = Some(turn_id);
            }
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
        Control::HookResponseReady {
            request_id,
            consumer_id,
            result,
        } => {
            if let Err(error) = processor
                .finish_hook_callback(&request_id, &consumer_id, result)
                .await
            {
                processor.status.source_unknown = true;
                if processor.status.failure.is_none() {
                    processor.status.failure = Some(*error.message);
                }
            }
            publish(processor, status);
        }
        Control::BeginStop { reply } => {
            processor.planned_stop = true;
            processor.accept_frames = false;
            processor.cancel_all_hook_callbacks();
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
            processor.cancel_all_hook_callbacks();
            if let Some(pending) = processor.pending_turn.take() {
                processor.settle_pending(
                    pending,
                    TurnOutcome::Unknown(
                        "Claude event task closed before the input association was observed"
                            .to_owned(),
                    ),
                );
            }
            processor.finish_pending_end_stop().await;
            let _ = reply.send(());
            return true;
        }
    }
    false
}

impl EventProcessor {
    fn new(
        host: PeerClient,
        config: EventConfig,
        hook_callback_id: String,
        status_tx: watch::Sender<EventStatus>,
        control_actor: EventActor<Control, EventStatus>,
    ) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
            host,
            sink,
            credential: config.credential,
            agent_id: config.agent_id,
            generation: config.generation,
            hook_callback_id,
            native: Some(config.native),
            secrets: config.secrets,
            workspace: config.workspace,
            status: EventStatus::default(),
            buffer: EarlyEventBuffer::new(),
            gap_number: 0,
            pending_turn: None,
            empty_after_tool_hooks: 0,
            interrupted_turn: None,
            pending_requests: HashMap::new(),
            cancelled_hook_requests: HashSet::new(),
            tool_use_turns: HashMap::new(),
            tool_use_tracking_fenced: false,
            tool_names: HashMap::new(),
            tools_started: HashSet::new(),
            tools_completed: HashSet::new(),
            background_tools: HashMap::new(),
            tool_coverage_gap_reported: false,
            task_notification_pending: false,
            active_input: None,
            last_input: None,
            native_prefix_ended: false,
            prefix_usage: None,
            deny_uncertain: HashSet::new(),
            permission_admission_fenced: false,
            message_seq: 0,
            unclassified_sequence: 0,
            accept_frames: true,
            planned_stop: false,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            terminal_deadline: None,
            terminal_inactivity: native_terminal_inactivity(),
            pending_fault_gap: None,
            final_text: None,
            native_failure: None,
            native_provider_restriction: None,
            native_rate_limit_event: None,
            init_mcp_servers: None,
            latest_context_message: None,
            first_call_usage: FirstCallUsage::default(),
            closed: false,
            status_tx,
            control_actor,
            pending_hooks: HashMap::new(),
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
        if (self.status.active_turn_id.is_some() && !self.status.native_unbound)
            || self.pending_turn.is_some()
        {
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
        if self.status.wake_cancel_pending {
            return Err(Error::new(
                "wake_cancel_pending",
                "Claude is cancelling a guarded native wake",
            ));
        }
        self.pending_turn = Some(PendingTurn {
            command_id,
            turn_id,
            overlaps_native: self.status.native_unbound || self.task_notification_pending,
            context: Vec::new(),
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
            self.cancelled_hook_requests.clear();
        }
    }

    fn buffer_record(&mut self, record: Value) {
        if !self.buffer.try_push(record) {
            // Overflow is settled as a gap by the next commit path instead of
            // inventing completion; mark the source now.
            self.status.source_unknown = true;
            if self.status.failure.is_none() {
                self.status.failure = Some("Claude early event buffer overflowed".to_owned());
            }
        }
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(front) = self.buffer.front() {
            if self.should_buffer(front) {
                break;
            }
            let record = self.buffer.pop_front().expect("buffer head was checked");
            if let Err(error) = self.process_record(record).await {
                return Err(self
                    .stream_source()
                    .buffered_failure(
                        error,
                        "Buffered Claude native record could not be normalized",
                    )
                    .await);
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
        if kind == Some("assistant") || kind == Some("user") {
            if !is_main_thread(record) {
                return false;
            }
            return !self.status.ready
                || (self.status.active_turn_id.is_none() && self.pending_turn.is_some());
        }
        if kind == Some("control_request") {
            let subtype = record
                .get("request")
                .and_then(|request| request.get("subtype"))
                .and_then(Value::as_str);
            if matches!(subtype, Some("can_use_tool" | "hook_callback")) {
                return self.status.active_turn_id.is_none() && self.pending_turn.is_some();
            }
        }
        false
    }

    async fn handle_record(&mut self, record: Value) {
        if self.should_buffer(&record) {
            self.buffer_record(record);
            return;
        }
        if let Err(error) = self.process_record(record).await {
            // The host settles a definite write rejection. Only unresolved
            // observations latch the local source unknown.
            if error.failure_where.as_deref() != Some("plugin.events.put") {
                self.status.source_unknown = true;
                if self.status.failure.is_none() {
                    self.status.failure = Some(*error.message);
                }
            }
            return;
        }
        // An association may have unblocked buffered assistant text.
        let _ = self.flush_buffer().await;
    }

    async fn process_record(&mut self, record: Value) -> Result<()> {
        let kind = record
            .get("type")
            .and_then(Value::as_str)
            .filter(|kind| !kind.trim().is_empty())
            .ok_or_else(|| Error::new("native_protocol", "Claude record omitted its type"))?;
        // Sidechains can outlive the parent's turn. Filter before session,
        // association and tool-table checks, never against a later active turn.
        if matches!(kind, "assistant" | "user") && !is_main_thread(&record) {
            return Ok(());
        }
        match kind {
            "system" => self.observe_system(&record).await,
            "command_lifecycle" => self.observe_lifecycle(&record).await,
            "assistant" => self.observe_assistant(&record).await,
            "result" => self.complete_turn(&record).await,
            "user" => self.observe_user(&record).await,
            "control_request" => self.observe_control_request(&record).await,
            "control_cancel_request" => {
                self.cancel_hook_callback(&record);
                Ok(())
            }
            "rate_limit_event" => self.observe_rate_limit_event(&record).await,
            _ => {
                if record
                    .get("session_id")
                    .is_some_and(|id| id.as_str() != self.status.session_id.as_deref())
                {
                    return Ok(());
                }
                if self.status.active_turn_id.is_some()
                    && is_main_thread(&record)
                    && record.get("session_id").and_then(Value::as_str).is_some()
                    && record.get("turn_id").map_or(kind == "stream_event", |id| {
                        id.as_str() == self.status.active_turn_id.as_deref()
                    })
                {
                    self.refresh_terminal_deadline();
                }
                self.record_unclassified("claude.record.type", kind).await
            }
        }
    }

    /// Keep an SDKRateLimitEvent tied to its exact session and active turn;
    /// it contributes to the provider fact only if that turn later fails.
    async fn observe_rate_limit_event(&mut self, record: &Value) -> Result<()> {
        let Some(session_id) = record.get("session_id").and_then(Value::as_str) else {
            return Ok(());
        };
        self.check_session(session_id)?;
        let (Some(turn_id), Some(rate_limit_info)) = (
            self.status.active_turn_id.clone(),
            record.get("rate_limit_info"),
        ) else {
            return Ok(());
        };
        let Some(status) = rate_limit_info.get("status").and_then(Value::as_str) else {
            return Ok(());
        };
        match status {
            "rejected" => {
                let previous_reset = self
                    .native_rate_limit_event
                    .as_ref()
                    .filter(|event| event.session_id == session_id && event.turn_id == turn_id)
                    .and_then(|event| event.retry_after);
                let retry_after = rate_limit_info
                    .get("resetsAt")
                    .and_then(Value::as_i64)
                    .filter(|resets_at| *resets_at >= 0)
                    .map(|resets_at| resets_at.saturating_mul(1_000))
                    .max(previous_reset);
                self.native_rate_limit_event = Some(PendingRateLimitEvent {
                    session_id: session_id.to_owned(),
                    turn_id,
                    retry_after,
                });
            }
            "allowed" | "allowed_warning" => {
                if self
                    .native_rate_limit_event
                    .as_ref()
                    .is_some_and(|event| event.session_id == session_id && event.turn_id == turn_id)
                {
                    self.native_rate_limit_event = None;
                }
            }
            _ => {
                self.record_unclassified("claude.rate_limit_info.status", status)
                    .await?;
            }
        }
        Ok(())
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
            self.commit_background_task_end(record).await?;
            self.observe_task_boundary(record).await?;
            self.retain_init_mcp_servers(record);
            return self.observe_compaction_boundary(record, session_id).await;
        }
        if self.status.ready {
            // Ready without a session id cannot happen; treat it as unknown.
            return Err(Error::new(
                "unknown",
                "Claude session identity is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.to_owned());
        self.retain_init_mcp_servers(record);
        self.observe_compaction_boundary(record, session_id).await
    }

    async fn observe_compaction_boundary(
        &mut self,
        record: &Value,
        session_id: &str,
    ) -> Result<()> {
        if record.get("subtype").and_then(Value::as_str) != Some("compact_boundary") {
            return Ok(());
        }
        let Some(native_event_id) = record
            .get("uuid")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
        else {
            self.record_gap("Claude compact boundary omitted its native event id")
                .await?;
            return Ok(());
        };
        let native_id = format!("claude:{session_id}:{native_event_id}:session.context.compacted");
        if native_id.len() > MAX_COMMAND_ID_BYTES {
            self.record_gap("Claude compact boundary native event id exceeded its bound")
                .await?;
            return Ok(());
        }
        let mut data = json!({});
        if !self.status.source_unknown
            && !self.writes_blocked
            && let Some(turn_id) = self.status.active_turn_id.as_deref()
        {
            data["turn_id"] = Value::String(turn_id.to_owned());
        }
        self.commit_event("session.context.compacted", native_id, data)
            .await
    }

    /// Claude announces a background task notification before its init.
    /// An init without that signal cannot prove an autonomous start.
    async fn observe_task_boundary(&mut self, record: &Value) -> Result<()> {
        let subtype = record.get("subtype").and_then(Value::as_str);
        if subtype == Some("task_notification") {
            self.task_notification_pending = true;
            if let Some(pending) = self.pending_turn.as_mut() {
                pending.overlaps_native = true;
            }
        } else if subtype == Some("init") {
            let notified = std::mem::take(&mut self.task_notification_pending);
            let names_input = record
                .get("user_message_uuids")
                .and_then(Value::as_array)
                .is_some_and(|uuids| !uuids.is_empty());
            if notified
                && !names_input
                && self.status.active_turn_id.is_none()
                && self.status.ready
                && !self.planned_stop
            {
                self.start_native_turn(record).await?;
            }
        }
        self.status.background_continuation = self.task_notification_pending;
        Ok(())
    }

    /// Commit a turn Claude started by itself. Its id is the native init
    /// record's UUID; it has no command until a send merges into it.
    async fn start_native_turn(&mut self, record: &Value) -> Result<()> {
        let Some(turn_id) = record
            .get("uuid")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty() && id.len() <= 128)
            .map(str::to_owned)
        else {
            return self
                .record_coverage_gap(
                    "Claude self-started turn omitted its native id",
                    &["message", "tool"],
                )
                .await;
        };
        self.status.active_turn_id = Some(turn_id.clone());
        self.status.native_unbound = true;
        self.active_input = None;
        self.latest_context_message = None;
        self.first_call_usage = FirstCallUsage::default();
        self.native_failure = None;
        self.native_provider_restriction = None;
        self.native_rate_limit_event = None;
        self.final_text = None;
        self.empty_after_tool_hooks = 0;
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:turn.started");
        let cancel = self
            .stream_source()
            .commit_native_wake(native_id, &turn_id, "task-notification")
            .await?;
        self.status.wake_cancel_pending = cancel.is_some();
        self.publish_now();
        if cancel.is_some() {
            let native = self.native.as_ref().ok_or_else(|| {
                Error::new("unknown", "Claude wake cancellation lost its native client")
            })?;
            let deadline = Instant::now() + Duration::from_secs(5);
            if let Err(error) = native.control("interrupt", json!({}), deadline).await {
                let _ = native
                    .shutdown_input(Instant::now() + Duration::from_secs(5))
                    .await;
                return Err(error);
            }
            self.interrupted_turn = Some(turn_id);
        }
        Ok(())
    }

    /// Facts of a self-started turn with no bound send name no command.
    fn scope_turn_command(&self, data: &mut Value) {
        if self.status.active_command_id.is_none()
            && let Some(fields) = data.as_object_mut()
        {
            fields.remove("command_id");
        }
    }

    /// Retain the proven MCP surface from the owned session's
    /// `system`/`init` record. Only an observed inventory is kept (with
    /// secrets redacted); anything unprovable leaves the retained set
    /// untouched so the effective servers stay unknown, never guessed.
    fn retain_init_mcp_servers(&mut self, record: &Value) {
        if record.get("subtype").and_then(Value::as_str) != Some("init") {
            return;
        }
        let Some(servers) = init_mcp_servers(record) else {
            return;
        };
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let redacted = servers
            .into_iter()
            .map(|server| InitMcpServer {
                name: policy.redact_text(&server.name),
                status: policy.redact_text(&server.status),
            })
            .collect::<Vec<_>>();
        if redacted
            .iter()
            .any(|server| server.name.trim().is_empty() || server.status.trim().is_empty())
        {
            return;
        }
        self.init_mcp_servers = Some(redacted);
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
        let matches_observed_turn = self
            .pending_turn
            .as_ref()
            .is_some_and(|pending| pending.turn_id == parsed.command_uuid)
            || self.status.active_turn_id.as_deref() == Some(parsed.command_uuid.as_str())
            || self.active_input.as_deref() == Some(parsed.command_uuid.as_str())
            || self.last_input.as_deref() == Some(parsed.command_uuid.as_str())
            || self
                .status
                .last_turn
                .as_ref()
                .is_some_and(|last| last.turn_id == parsed.command_uuid);
        if !matches_observed_turn {
            return self
                .record_gap("Claude lifecycle did not match the exact submitted turn")
                .await;
        }
        if self.status.active_turn_id.as_deref() == Some(parsed.command_uuid.as_str())
            || self.active_input.as_deref() == Some(parsed.command_uuid.as_str())
        {
            self.refresh_terminal_deadline();
        }
        let state = crate::native::types::CommandLifecycleState::from_native(&parsed.state);
        if state == crate::native::types::CommandLifecycleState::Other {
            self.record_unclassified("claude.command_lifecycle.state", &parsed.state)
                .await?;
        }
        if let Some(pending) = self.pending_turn.as_ref()
            && pending.turn_id == parsed.command_uuid
        {
            // A queued frame can follow the native result. Queue admission
            // alone cannot start a new turn after that result.
            if state == crate::native::types::CommandLifecycleState::Queued
                && pending.overlaps_native
                && !self.status.native_unbound
            {
                return Ok(());
            }
            return self.settle_pending_lifecycle(state).await;
        }
        if self.status.active_turn_id.as_deref() == Some(parsed.command_uuid.as_str()) {
            if state == crate::native::types::CommandLifecycleState::Cancelled {
                return self.complete_cancelled_turn().await;
            }
            return Ok(());
        }
        if self.active_input.as_deref() == Some(parsed.command_uuid.as_str()) {
            // A merged input Claude dropped: the running native turn still
            // ends with its own result, unless that result already arrived.
            if state == crate::native::types::CommandLifecycleState::Cancelled {
                self.active_input = None;
                if self.native_prefix_ended {
                    return self.complete_cancelled_turn().await;
                }
            }
            return Ok(());
        }
        if self.last_input.as_deref() == Some(parsed.command_uuid.as_str())
            || self
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

    async fn settle_pending_lifecycle(
        &mut self,
        state: crate::native::types::CommandLifecycleState,
    ) -> Result<()> {
        let Some(pending) = self.pending_turn.take() else {
            return Ok(());
        };
        let PendingTurn {
            command_id,
            turn_id,
            context,
            outcome,
            ..
        } = pending;
        let settle = |outcome: Option<oneshot::Sender<TurnOutcome>>, settled: TurnOutcome| {
            if let Some(sender) = outcome {
                let _ = sender.send(settled);
            }
        };
        match state {
            crate::native::types::CommandLifecycleState::Queued
            | crate::native::types::CommandLifecycleState::Started => {
                if self.writes_blocked {
                    settle(
                        outcome,
                        TurnOutcome::Unknown(
                            "Claude event source acknowledgement is unresolved".to_owned(),
                        ),
                    );
                    return Err(Error::new(
                        "unknown",
                        "Claude event source acknowledgement is unresolved",
                    ));
                }
                if self.status.native_unbound
                    && let Some(native_turn) = self.status.active_turn_id.clone()
                {
                    return self
                        .bind_input_to_native_turn(
                            command_id,
                            turn_id,
                            native_turn,
                            context,
                            outcome,
                        )
                        .await;
                }
                self.status.active_command_id = Some(command_id.clone());
                self.status.active_turn_id = Some(turn_id.clone());
                self.active_input = Some(turn_id.clone());
                self.latest_context_message = None;
                self.first_call_usage = FirstCallUsage::default();
                self.native_failure = None;
                self.native_provider_restriction = None;
                self.native_rate_limit_event = None;
                self.final_text = None;
                self.publish_now();
                let session_id = self.status.session_id.as_deref().unwrap_or_default();
                let native_id = format!("claude:{session_id}:{turn_id}:turn.started");
                let context_outcomes = next_input_outcomes(&context, &command_id);
                let mut data = json!({
                    "command_id": command_id,
                    "turn_id": turn_id,
                });
                if !context_outcomes.is_empty() {
                    data["context_outcomes"] = json!(context_outcomes);
                }
                if let Err(error) = self.commit_event("turn.started", native_id, data).await {
                    self.settle_turn_started_persistence(outcome, &error);
                    return Err(error);
                }
                settle(outcome, TurnOutcome::Associated(turn_id));
                // Buffered assistant text drains in the caller's trailing
                // flush, not here, keeping the commit path non-recursive.
                Ok(())
            }
            crate::native::types::CommandLifecycleState::Cancelled => {
                self.cancelled_hook_requests.clear();
                settle(
                    outcome,
                    TurnOutcome::Rejected("Claude did not admit the input".to_owned()),
                );
                Ok(())
            }
            crate::native::types::CommandLifecycleState::Completed
            | crate::native::types::CommandLifecycleState::Other => {
                self.cancelled_hook_requests.clear();
                settle(
                    outcome,
                    TurnOutcome::Unknown(
                        "Claude lifecycle carried an unknown turn state".to_owned(),
                    ),
                );
                self.record_gap("Claude lifecycle carried an unknown turn state")
                    .await
            }
        }
    }

    /// Claude queued this input into its running self-started turn, so the
    /// send joins that turn. The turn stays open until a result names the
    /// input; its command then carries the terminal and final text.
    async fn bind_input_to_native_turn(
        &mut self,
        command_id: String,
        input_id: String,
        turn_id: String,
        context: Vec<ContextItem>,
        outcome: Option<oneshot::Sender<TurnOutcome>>,
    ) -> Result<()> {
        self.status.active_command_id = Some(command_id.clone());
        self.status.native_unbound = false;
        self.status.wake_cancel_pending = false;
        self.active_input = Some(input_id.clone());
        self.publish_now();
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{input_id}:turn.input_bound");
        let context_outcomes = next_input_outcomes(&context, &command_id);
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "input_id": input_id,
        });
        if !context_outcomes.is_empty() {
            data["context_outcomes"] = json!(context_outcomes);
        }
        if let Err(error) = self.commit_event("turn.input_bound", native_id, data).await {
            self.settle_turn_started_persistence(outcome, &error);
            return Err(error);
        }
        if let Some(sender) = outcome {
            let _ = sender.send(TurnOutcome::Associated(turn_id));
        }
        Ok(())
    }

    /// Settle a post-dispatch `turn.started` persistence failure. The input
    /// was already written to the CLI, so even a definite batch rejection
    /// proves only that the batch was never committed (a gap covers it),
    /// never a definite native outcome. The command stays `Unknown` with its
    /// exact binding, and fresh input is fenced via `source_unknown`/`failure`
    /// until reconciliation. Only `session.ready` before any input may settle
    /// definite `failed`; the commit latch itself stays clear (no
    /// `writes_blocked`/`batch_unknown`) because the batch is known
    /// uncommitted.
    fn settle_turn_started_persistence(
        &mut self,
        outcome: Option<oneshot::Sender<TurnOutcome>>,
        error: &Error,
    ) {
        let reason = (*error.message).clone();
        self.status.source_unknown = true;
        if self.status.failure.is_none() {
            self.status.failure = Some(reason.clone());
        }
        if let Some(sender) = outcome {
            let _ = sender.send(TurnOutcome::Unknown(reason));
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
            "user_message_uuid": self.active_input,
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
                .record_coverage_gap(
                    "Claude message arrived outside an associated input; message was not attributed",
                    &["message"],
                )
                .await;
        };
        self.refresh_terminal_deadline();
        let main_thread = is_main_thread_assistant(record);
        let model = assistant_model(record);
        let model_output = main_thread
            && record.get("error").is_none_or(Value::is_null)
            && model.as_deref() != Some("<synthetic>");
        self.first_call_usage
            .observe(model_output, assistant_call_input(record));
        if main_thread {
            self.latest_context_message = if model_output {
                model.clone().zip(
                    record["message"]["id"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .map(str::to_owned),
                )
            } else {
                None
            };
        }
        let has_tool_uses = main_thread && assistant_tool_use_count(record) > 0;
        let text = assistant_text(record);
        if main_thread {
            let native_error_kind = record.get("error").and_then(Value::as_str);
            self.native_failure =
                native_error_kind.and_then(|kind| claude_api_failure(kind, &text));
            self.native_provider_restriction =
                native_error_kind.and_then(claude_provider_restriction);
            if has_tool_uses {
                self.final_text = None;
            }
            self.bind_tool_uses(record, &turn_id);
            self.observe_tool_uses(record).await?;
        }
        let observed_model = model.filter(|_| model_output);
        if is_blank_native_text(&text) {
            if let Some(model) = observed_model {
                self.message_seq = self.message_seq.saturating_add(1);
                let session_id = self.status.session_id.as_deref().unwrap_or_default();
                let native_id = format!("claude:{session_id}:{turn_id}:model:{}", self.message_seq);
                let mut data = json!({
                    "command_id": self.status.active_command_id,
                    "turn_id": turn_id,
                    "model": model,
                });
                self.scope_turn_command(&mut data);
                self.commit_event("turn.model_observed", native_id, data)
                    .await?;
            }
            return Ok(());
        }
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(&text);
        let command_id = self.status.active_command_id.as_deref();
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "text": text.clone(),
        });
        self.scope_turn_command(&mut data);
        if let Some(model) = observed_model {
            data["model"] = Value::String(model);
        }
        if !model_output {
            data["model_output"] = Value::Bool(false);
        }
        self.message_seq = self.message_seq.saturating_add(1);
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:message:{}", self.message_seq);
        // Keep every assistant message in the event stream, but expose only
        // the last visible message after the turn's final tool call.
        if model_output && !has_tool_uses {
            self.final_text = Some(text.clone());
        }
        // A message-event gap does not erase text already observed for the
        // terminal payload; the shared final-text boundary bounds that field.
        self.stream_source().commit_message(native_id, data).await?;
        Ok(())
    }

    /// Commit `tool.started` for the main-thread `tool_use` blocks of one
    /// `assistant` record, bound to the exact active turn and generation
    /// with the native `tool_use.id` as the item id. Runs only while the
    /// id binding is unfenced; a fenced turn reports incomplete coverage,
    /// never a misbound tool record. Nameless invocations cannot
    /// label a tool record and are reported as one coverage gap.
    async fn observe_tool_uses(&mut self, record: &Value) -> Result<()> {
        if assistant_tool_use_count(record) > assistant_tool_uses(record).len() {
            self.record_tool_coverage_gap("Claude tool use omitted its tool name or id")
                .await?;
        }
        if self.tool_use_tracking_fenced {
            return self
                .record_tool_coverage_gap(
                    "Claude tool tracking bound or identity conflict fenced this turn",
                )
                .await;
        }
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            return Ok(());
        };
        let command_id = self.status.active_command_id.clone();
        for tool_use in assistant_tool_uses(record) {
            self.tool_names
                .entry(tool_use.id.clone())
                .or_insert_with(|| tool_use.name.clone());
            if !self.tools_started.insert(tool_use.id.clone()) {
                continue;
            }
            let mut data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "item_id": tool_use.id,
                "tool": tool_use.name,
            });
            self.scope_turn_command(&mut data);
            if let Some(at) = eliot_plugin::quota::rfc3339_ms(record.get("timestamp")) {
                data["native_at_ms"] = json!(at);
            }
            // Past the bound the end of this task could not be reported, so
            // the call stays an ordinary one: a duplicate wake, never a lost one.
            if tool_use.run_in_background && self.background_tools.len() < MAX_BACKGROUND_TOOLS {
                data["native_background"] = json!(true);
                self.background_tools
                    .insert(tool_use.id.clone(), turn_id.clone());
            }
            if let Some(command) = &tool_use.command {
                let policy = SecretPolicy::new(self.secrets.iter().cloned());
                let redacted = policy.redact_text(command);
                data["command_truncated"] = json!(redacted.len() > MAX_TOOL_COMMAND_BYTES);
                data["command"] =
                    Value::String(policy.redact_bounded(&redacted, MAX_TOOL_COMMAND_BYTES));
            }
            if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
                return self
                    .record_gap("Claude tool start exceeded the persisted-event bound")
                    .await;
            }
            let session_id = self.status.session_id.as_deref().unwrap_or_default();
            let native_id = format!("claude:{session_id}:{turn_id}:tool.started:{}", tool_use.id);
            self.commit_event("tool.started", native_id, data).await?;
        }
        Ok(())
    }

    /// Commit `tool.completed` for the main-thread `tool_result` blocks of
    /// one `user` record, bound to the exact active turn and generation
    /// with the native `tool_use.id` as the item id. The result blocks
    /// carry no tool name, so the completion reads it back from the
    /// matching `tool_use` block seen on this turn; a result with no known
    /// invocation is a coverage gap, never a guessed tool. The native
    /// status is the frame's own verdict: `error` when `is_error` is true,
    /// `completed` otherwise.
    async fn observe_user(&mut self, record: &Value) -> Result<()> {
        let results = user_tool_results(record);
        if results.is_empty() {
            return Ok(());
        }
        if let Some(session_id) = record.get("session_id").and_then(Value::as_str) {
            self.check_session(session_id)?;
        }
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            if self.pending_turn.is_some() || !self.status.ready {
                self.buffer_record(record.clone());
                return Ok(());
            }
            return self
                .record_tool_coverage_gap(
                    "Claude tool result arrived outside the exact active turn",
                )
                .await;
        };
        if !is_main_thread_user(record) {
            return Ok(());
        }
        // Hook identity tracking deliberately clears its bounded tables when
        // fenced. A legitimate later result cannot consult that erased table.
        if self.tool_use_tracking_fenced {
            return self
                .record_tool_coverage_gap(
                    "Claude tool tracking bound or identity conflict fenced this turn",
                )
                .await;
        }
        let command_id = self.status.active_command_id.clone();
        let mut unknown = false;
        for result in results {
            let Some(tool) = self.tool_names.get(&result.tool_use_id).cloned() else {
                unknown = true;
                continue;
            };
            if !self.tools_completed.insert(result.tool_use_id.clone()) {
                continue;
            }
            let mut data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "item_id": result.tool_use_id,
                "tool": tool,
                "status": tool_result_status(result.is_error),
            });
            self.scope_turn_command(&mut data);
            if let Some(at) = eliot_plugin::quota::rfc3339_ms(record.get("timestamp")) {
                data["native_at_ms"] = json!(at);
            }
            if result.is_error == Some(true) {
                attach_native_error(
                    &mut data,
                    result.failure_output.as_deref(),
                    &SecretPolicy::new(self.secrets.iter().cloned()),
                );
            }
            if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
                return self
                    .record_gap("Claude tool completion exceeded the persisted-event bound")
                    .await;
            }
            let session_id = self.status.session_id.as_deref().unwrap_or_default();
            let native_id = format!(
                "claude:{session_id}:{turn_id}:tool.completed:{}",
                result.tool_use_id
            );
            self.commit_event("tool.completed", native_id, data).await?;
        }
        if unknown {
            self.record_tool_coverage_gap("Claude tool result did not match a known tool use")
                .await?;
        }
        Ok(())
    }

    /// Commit the end of a native background task started by a tracked
    /// `run_in_background` call. The task can outlive its turn, so the fact
    /// names the turn that started it.
    async fn commit_background_task_end(&mut self, record: &Value) -> Result<()> {
        if record.get("subtype").and_then(Value::as_str) != Some("task_notification") {
            return Ok(());
        }
        let Some(item_id) = record.get("tool_use_id").and_then(Value::as_str) else {
            return Ok(());
        };
        let Some(turn_id) = self.background_tools.remove(item_id) else {
            return Ok(());
        };
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:tool.background_ended:{item_id}");
        let data = json!({ "turn_id": turn_id, "item_id": item_id });
        self.commit_event("tool.background_ended", native_id, data)
            .await
    }

    /// Drop this turn's tool-name and tool-record tracking. Runs wherever
    /// the id binding clears so a later turn never reuses an earlier
    /// invocation's name or suppresses its own records as duplicates.
    fn clear_tool_tracking(&mut self) {
        self.tool_names.clear();
        self.tools_started.clear();
        self.tools_completed.clear();
    }

    async fn record_tool_coverage_gap(&mut self, reason: &str) -> Result<()> {
        if self.tool_coverage_gap_reported {
            return Ok(());
        }
        self.record_coverage_gap(reason, &["tool"]).await?;
        self.tool_coverage_gap_reported = true;
        Ok(())
    }

    /// Keep a bounded, generation-local association from Claude invocation
    /// ids to the exact Eliot turn that supplied their assistant tool-use
    /// blocks. Hook callbacks never fall back to whichever turn is active.
    fn bind_tool_uses(&mut self, record: &Value, turn_id: &str) {
        if self.tool_use_tracking_fenced {
            return;
        }
        let prompt_id = assistant_prompt_id(record);
        for tool_use_id in assistant_tool_use_ids(record) {
            if let Some(existing) = self.tool_use_turns.get(&tool_use_id).cloned() {
                let prompt_conflict = matches!(
                    (existing.prompt_id.as_deref(), prompt_id.as_deref()),
                    (Some(existing), Some(observed)) if existing != observed
                );
                if existing.turn_id != turn_id || prompt_conflict {
                    self.tool_use_turns.clear();
                    self.clear_tool_tracking();
                    self.tool_use_tracking_fenced = true;
                    return;
                }
                if existing.prompt_id.is_none()
                    && let Some(prompt_id) = prompt_id.as_ref()
                    && let Some(existing) = self.tool_use_turns.get_mut(&tool_use_id)
                {
                    existing.prompt_id = Some(prompt_id.clone());
                }
                continue;
            }
            if self.tool_use_turns.len() >= MAX_TOOL_USE_BINDINGS {
                // Preserve exact bindings for callbacks already in flight.
                // Stop admitting new ids; telemetry reports the bounded loss.
                self.tool_use_tracking_fenced = true;
                return;
            }
            self.tool_use_turns.insert(
                tool_use_id,
                ToolUseBinding {
                    turn_id: turn_id.to_owned(),
                    prompt_id: prompt_id.clone(),
                    before_hook_state: HookConsumerState::Available,
                    before_consumer_id: None,
                    after_hook_state: HookConsumerState::Available,
                    after_consumer_id: None,
                },
            );
        }
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

    async fn observe_hook_callback(&mut self, record: &Value) -> Result<()> {
        if self.frames_drained || self.planned_stop || self.closed {
            return Ok(());
        }
        let request_id = record
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Claude hook_callback omitted its exact native request id",
                )
            })?;
        if self.cancelled_hook_requests.contains(request_id) {
            return Ok(());
        }
        if self.pending_hooks.contains_key(request_id) {
            return Ok(());
        }
        let body = record
            .get("request")
            .cloned()
            .and_then(|request| serde_json::from_value::<HookCallbackBody>(request).ok());
        let deadline = hook_deadline(body.as_ref().and_then(|body| body.deadline_ms));
        let Some(native) = self.native.clone() else {
            return Err(Error::new(
                "unknown",
                "Claude hook_callback arrived before the native session was ready",
            ));
        };
        let Some(body) = body else {
            return self.write_hook_pass(&native, request_id, deadline).await;
        };
        let hook = match body.input.get("hook_event_name").and_then(Value::as_str) {
            Some("PreToolUse") => HookPoint::BeforeTool,
            Some("PostToolUse") => HookPoint::AfterTool,
            _ => return self.write_hook_pass(&native, request_id, deadline).await,
        };
        if body.subtype != "hook_callback"
            || body.callback_id != self.hook_callback_id
            || body
                .input
                .get("agent_id")
                .is_some_and(|value| !value.is_null())
            || body
                .input
                .get("parent_tool_use_id")
                .is_some_and(|value| !value.is_null())
        {
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let input_tool_use_id = body
            .input
            .get("tool_use_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty());
        let outer_tool_use_id = body
            .tool_use_id
            .as_deref()
            .filter(|id| !id.trim().is_empty());
        let tool_use_id = match (input_tool_use_id, outer_tool_use_id) {
            (Some(input), Some(outer)) if input == outer => input.to_owned(),
            (Some(input), None) => input.to_owned(),
            (None, Some(outer)) => outer.to_owned(),
            _ => return self.write_hook_pass(&native, request_id, deadline).await,
        };
        let input_prompt_id = body
            .input
            .get("prompt_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty());
        let Some(binding) = self.tool_use_turns.get(&tool_use_id).cloned() else {
            return self.write_hook_pass(&native, request_id, deadline).await;
        };
        if binding.hook_state(hook) != HookConsumerState::Available {
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        if let (Some(input_prompt_id), Some(assistant_prompt_id)) =
            (input_prompt_id, binding.prompt_id.as_deref())
            && input_prompt_id != assistant_prompt_id
        {
            self.finish_available_tool_use_hook(&tool_use_id, hook);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let turn_id = binding.turn_id.clone();
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str())
            || !self.status.ready
            || self.status.failure.is_some()
            || self.status.stopped
        {
            self.finish_available_tool_use_hook(&tool_use_id, hook);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        if self.pending_hooks.len() >= MAX_PENDING_HOOKS {
            self.finish_available_tool_use_hook(&tool_use_id, hook);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let control_actor = self.control_actor.clone();
        let consumer_id = ulid::Ulid::new().to_string();
        // Bounded progress diagnostics from the native hook input only. The
        // matched invocation id is already proven above; the name, operation
        // and path are optional and stay absent when the hook omits them.
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let tool = hook_tool_facts(
            &body.input,
            &tool_use_id,
            &self.workspace,
            &policy,
            hook == HookPoint::AfterTool,
        );
        let event = HookEvent {
            hook: hook.event_name().to_owned(),
            turn_id: turn_id.clone(),
            consumer_id: consumer_id.clone(),
        };
        let mut params = match serde_json::to_value(event) {
            Ok(params) => params,
            Err(_) => {
                self.finish_available_tool_use_hook(&tool_use_id, hook);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
        };
        let Some(params_object) = params.as_object_mut() else {
            self.finish_available_tool_use_hook(&tool_use_id, hook);
            return self.write_hook_pass(&native, request_id, deadline).await;
        };
        // This private IPC envelope field is stripped by the host before the
        // shared HookEvent DTO is decoded. It never enters native input or
        // persisted event data.
        params_object.insert(
            "credential".to_owned(),
            Value::String(self.credential.clone()),
        );
        if let Some(binding) = self.tool_use_turns.get_mut(&tool_use_id) {
            binding.start_hook(hook, consumer_id.clone());
        } else {
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let host = self.host.clone();
        let callback_request_id = request_id.to_owned();
        let callback_consumer_id = consumer_id.clone();
        let callback_deadline = deadline;
        let task = tokio::spawn(async move {
            let result = match host.call("hook.event", params, callback_deadline).await {
                Ok(response) => serde_json::from_value::<HookResponse>(response)
                    .map(|response| response.context)
                    .map_err(|_| Error::new("invalid", "Claude hook response was malformed")),
                Err(error) => Err(error),
            };
            control_actor
                .deliver(Control::HookResponseReady {
                    request_id: callback_request_id,
                    consumer_id: callback_consumer_id,
                    result,
                })
                .await;
        });
        self.pending_hooks.insert(
            request_id.to_owned(),
            PendingHookCallback {
                generation: self.generation,
                hook,
                turn_id,
                tool_use_id,
                consumer_id,
                tool,
                deadline,
                native,
                task,
            },
        );
        Ok(())
    }

    async fn finish_hook_callback(
        &mut self,
        request_id: &str,
        expected_consumer_id: &str,
        result: Result<Vec<ContextItem>>,
    ) -> Result<()> {
        let Some(pending) = self
            .pending_hooks
            .get(request_id)
            .filter(|pending| pending.consumer_id == expected_consumer_id)
        else {
            return Ok(());
        };
        let native = pending.native.clone();
        let deadline = pending.deadline;
        let tool_use_id = pending.tool_use_id.clone();
        let consumer_id = pending.consumer_id.clone();
        let hook = pending.hook;
        let context = match result {
            Ok(context) => context,
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
        };
        if context.is_empty() {
            return self
                .finish_hook_pass(
                    request_id,
                    &native,
                    deadline,
                    hook,
                    &tool_use_id,
                    &consumer_id,
                )
                .await;
        }
        let context_text = match with_claimed_context("", &context, "Claude") {
            Ok(text) => text.strip_suffix("\n\n").unwrap_or(&text).to_owned(),
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
        };
        let response = HookCallbackResponse::with_context(hook.native_event_name(), context_text);
        if !hook_callback_response_fits(request_id, &response) {
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
            return Err(Error::new(
                "frame_too_large",
                "Claude hook context exceeds the 1 MiB encoded response bound",
            ));
        }
        // Recheck the monotonic deadline, cancellation (pending-map membership),
        // generation and exact invocation-to-turn binding immediately before
        // the only native write that can carry additionalContext.
        if !self.hook_callback_is_live(request_id) {
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        match native
            .respond_hook_callback(request_id, response, deadline)
            .await
        {
            Ok(()) => {}
            Err(error) if native_no_write(&error.code) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, hook, &consumer_id);
                return Err(Error::new(
                    "unknown",
                    "Claude hook response write outcome is unknown",
                ));
            }
        }
        let Some(pending) = self.pending_hooks.remove(request_id) else {
            return Err(Error::new(
                "unknown",
                "Claude hook binding disappeared after the native response write",
            ));
        };
        if !self.hook_context_was_claimed(&context, &pending) {
            self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
            return Err(Error::new(
                "unknown",
                "Claude hook context was written without an exact live binding",
            ));
        }
        self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
        let Some(session_id) = self.status.session_id.as_deref() else {
            return Err(Error::new(
                "unknown",
                "Claude session identity disappeared after the hook response write",
            ));
        };
        let outcome = ContextOutcome {
            mechanism: format!(
                "claude.{}.additionalContext",
                pending.hook.native_event_name()
            ),
            application_point: InjectPoint::TurnSafePoint,
            consumer_id: pending.consumer_id.clone(),
        };
        let context_outcome = serde_json::to_value(outcome)
            .map_err(|_| Error::new("internal", "Claude context outcome could not be encoded"))?;
        let mut data = json!({
            "turn_id": pending.turn_id,
            "consumer_id": pending.consumer_id,
            "context_outcome": context_outcome,
        });
        pending.tool.attach(&mut data);
        let native_id = format!(
            "claude:{session_id}:{}:{}:{}",
            pending.turn_id,
            pending.hook.event_kind(),
            pending.consumer_id
        );
        self.commit_event(pending.hook.event_kind(), native_id, data)
            .await
    }

    /// The pass path for a hook that selected no context: write the empty
    /// response and commit the empty receipt.
    async fn finish_hook_pass(
        &mut self,
        request_id: &str,
        native: &ClaudeClient,
        deadline: Instant,
        hook: HookPoint,
        tool_use_id: &str,
        consumer_id: &str,
    ) -> Result<()> {
        if !self.hook_callback_is_live(request_id) {
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(tool_use_id, hook, consumer_id);
            return self.write_hook_pass(native, request_id, deadline).await;
        }
        match native
            .respond_hook_callback(request_id, HookCallbackResponse::default(), deadline)
            .await
        {
            Ok(()) => {}
            Err(error) if native_no_write(&error.code) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(tool_use_id, hook, consumer_id);
                return Ok(());
            }
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(tool_use_id, hook, consumer_id);
                return Err(Error::new(
                    "unknown",
                    "Claude hook pass response write outcome is unknown",
                ));
            }
        }
        let Some(pending) = self.pending_hooks.remove(request_id) else {
            return Err(Error::new(
                "unknown",
                "Claude hook binding disappeared after the native pass write",
            ));
        };
        if !self.hook_empty_selection_was_claimed(&pending) {
            self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
            return Err(Error::new(
                "unknown",
                "Claude empty hook selection lost its exact callback binding",
            ));
        }
        self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
        self.commit_empty_hook_event(
            pending.hook,
            pending.turn_id,
            pending.consumer_id,
            &pending.tool,
        )
        .await
    }

    fn hook_callback_is_live(&self, request_id: &str) -> bool {
        let Some(pending) = self.pending_hooks.get(request_id) else {
            return false;
        };
        Instant::now() < pending.deadline
            && pending.generation == self.generation
            && self.status.active_turn_id.as_deref() == Some(pending.turn_id.as_str())
            && self.status.failure.is_none()
            && !self.status.stopped
            && self
                .tool_use_turns
                .get(&pending.tool_use_id)
                .is_some_and(|binding| {
                    binding.turn_id == pending.turn_id
                        && binding.hook_state(pending.hook) == HookConsumerState::InFlight
                        && binding.is_active_consumer(pending.hook, &pending.consumer_id)
                })
    }

    fn hook_context_was_claimed(
        &self,
        context: &[ContextItem],
        pending: &PendingHookCallback,
    ) -> bool {
        !context.is_empty()
            && pending.generation == self.generation
            && self
                .tool_use_turns
                .get(&pending.tool_use_id)
                .is_some_and(|binding| {
                    binding.turn_id == pending.turn_id
                        && binding.hook_state(pending.hook) == HookConsumerState::InFlight
                        && binding.is_active_consumer(pending.hook, &pending.consumer_id)
                })
            && pending.consumer_id.trim() != ""
    }

    fn hook_empty_selection_was_claimed(&self, pending: &PendingHookCallback) -> bool {
        pending.generation == self.generation
            && self.status.active_turn_id.as_deref() == Some(pending.turn_id.as_str())
            && self.status.failure.is_none()
            && !self.status.stopped
            && self
                .tool_use_turns
                .get(&pending.tool_use_id)
                .is_some_and(|binding| {
                    binding.turn_id == pending.turn_id
                        && binding.hook_state(pending.hook) == HookConsumerState::InFlight
                        && binding.is_active_consumer(pending.hook, &pending.consumer_id)
                })
            && !pending.consumer_id.trim().is_empty()
    }

    async fn commit_empty_hook_event(
        &mut self,
        hook: HookPoint,
        turn_id: String,
        consumer_id: String,
        tool: &HookToolFacts,
    ) -> Result<()> {
        let kind = if hook == HookPoint::AfterTool {
            self.empty_after_tool_hooks = self.empty_after_tool_hooks.saturating_add(1);
            if !tool.has_edit() {
                return Ok(());
            }
            "tool.file.edited"
        } else {
            hook.event_kind()
        };
        let Some(session_id) = self.status.session_id.as_deref() else {
            return Err(Error::new(
                "unknown",
                "Claude session identity disappeared after the native pass write",
            ));
        };
        let mut data = json!({
            "turn_id": turn_id,
            "consumer_id": consumer_id,
        });
        tool.attach(&mut data);
        let native_id = format!("claude:{session_id}:{turn_id}:{}:{consumer_id}", kind);
        self.commit_event(kind, native_id, data).await
    }

    fn finish_tool_use_hook(&mut self, tool_use_id: &str, hook: HookPoint, consumer_id: &str) {
        if let Some(binding) = self.tool_use_turns.get_mut(tool_use_id) {
            binding.finish_hook(hook, consumer_id);
        }
    }

    fn finish_available_tool_use_hook(&mut self, tool_use_id: &str, hook: HookPoint) {
        if let Some(binding) = self.tool_use_turns.get_mut(tool_use_id) {
            binding.finish_available_hook(hook);
        }
    }

    async fn write_hook_pass(
        &self,
        native: &ClaudeClient,
        request_id: &str,
        deadline: Instant,
    ) -> Result<()> {
        if Instant::now() >= deadline {
            return Ok(());
        }
        match native
            .respond_hook_callback(request_id, HookCallbackResponse::default(), deadline)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) if native_no_write(&error.code) => Ok(()),
            Err(_) => Err(Error::new(
                "unknown",
                "Claude hook pass response outcome is unknown",
            )),
        }
    }

    fn cancel_hook_callback(&mut self, record: &Value) {
        let Ok(cancel) = serde_json::from_value::<ControlCancelRequest>(record.clone()) else {
            return;
        };
        if cancel.kind != "control_cancel_request" || cancel.request_id.trim().is_empty() {
            return;
        }
        if !self.frames_drained
            && !self.planned_stop
            && (self.status.active_turn_id.is_some() || self.pending_turn.is_some())
        {
            self.cancelled_hook_requests
                .insert(cancel.request_id.clone());
        }
        self.remove_buffered_hook_callback(&cancel.request_id);
        if let Some(pending) = self.pending_hooks.remove(&cancel.request_id) {
            pending.task.abort();
            self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
        }
    }

    fn remove_buffered_hook_callback(&mut self, request_id: &str) {
        self.buffer.retain(|record| {
            let request = record.get("request");
            let cancelled = record.get("type").and_then(Value::as_str) == Some("control_request")
                && record.get("request_id").and_then(Value::as_str) == Some(request_id)
                && request
                    .and_then(|request| request.get("subtype"))
                    .and_then(Value::as_str)
                    == Some("hook_callback");
            !cancelled
        });
    }

    fn cancel_hook_callbacks_for_turn(&mut self, turn_id: &str) {
        let request_ids: Vec<String> = self
            .pending_hooks
            .iter()
            .filter(|(_, pending)| pending.turn_id == turn_id)
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in request_ids {
            if let Some(pending) = self.pending_hooks.remove(&request_id) {
                pending.task.abort();
                self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
            }
        }
    }

    fn cancel_all_hook_callbacks(&mut self) {
        let pending_hooks = std::mem::take(&mut self.pending_hooks);
        for pending in pending_hooks.into_values() {
            pending.task.abort();
            self.finish_tool_use_hook(&pending.tool_use_id, pending.hook, &pending.consumer_id);
        }
        self.cancelled_hook_requests.clear();
    }

    /// One CLI `control_request`: only the `can_use_tool` permission ask is
    /// connected. It binds to the exact active turn and opens a retained
    /// request under the CLI's exact native `request_id`; any other subtype
    /// is retained as an unclassified observation without inventing a reply.
    async fn observe_control_request(&mut self, record: &Value) -> Result<()> {
        if let Some(session_id) = record.get("session_id").and_then(Value::as_str)
            && self.status.session_id.is_some()
        {
            self.check_session(session_id)?;
        }
        let subtype = record
            .get("request")
            .and_then(|request| request.get("subtype"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if subtype.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Claude control_request omitted its subtype",
            ));
        }
        let subtype_kind = NativeControlRequestSubtype::from_native(subtype);
        if subtype_kind == NativeControlRequestSubtype::HookCallback {
            return self.observe_hook_callback(record).await;
        }
        let request_id = record
            .get("request_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "native_protocol",
                    "Claude control_request omitted its exact native request id",
                )
            })?;
        if subtype_kind == NativeControlRequestSubtype::Other {
            if !self.deny_uncertain.contains(request_id)
                && self.retain_uncertain_id(request_id).await
            {
                let Some(native) = self.native.clone() else {
                    return Err(Error::new(
                        "unknown",
                        "Claude could not refuse an unsupported control request",
                    ));
                };
                if let Err(error) = native
                    .refuse_control(
                        request_id,
                        "Unsupported Claude control request subtype",
                        Instant::now() + EVENT_DEADLINE,
                    )
                    .await
                {
                    if matches!(
                        error.code.as_str(),
                        "native_not_dispatched" | "frame_too_large" | "native_encode"
                    ) {
                        self.deny_uncertain.remove(request_id);
                    }
                    return Err(error);
                }
            }
            return self
                .record_unclassified("claude.control_request.subtype", subtype)
                .await;
        }
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
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            self.deny_stray_request(request_id).await;
            return self
                .record_gap("Claude permission ask arrived outside the exact active turn")
                .await;
        };
        self.refresh_terminal_deadline();
        let command_id = self.status.active_command_id.clone();
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
            guard_refusal: None,
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
        let guard_command =
            eliot_plugin::session::shell_permission_command(&body.tool_name, body.input.as_ref());
        let mut event_data = data;
        self.scope_turn_command(&mut event_data);
        if let Some(command) = guard_command {
            event_data[eliot_core::COMMAND_GUARD_CONTEXT_FIELD] = json!({"commands": [command]});
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
        let refusal = self.commit_request_event(native_id, event_data).await?;
        if refusal
            .as_ref()
            .is_some_and(|refusal| refusal.request_id != request_id || refusal.turn_id != turn_id)
        {
            return Err(Error::new(
                "unknown",
                "Claude guard refusal did not bind to its exact permission request",
            ));
        }
        self.pending_requests.insert(
            request_id.to_owned(),
            PendingNativeRequest {
                turn_id: turn_id.clone(),
                tool_name: body.tool_name,
                tool_use_id: body.tool_use_id,
                input: body.input.unwrap_or(Value::Null),
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
            },
        );
        if let Some(refusal) = refusal {
            self.refuse_guarded_permission(request_id, &turn_id, &refusal.message)
                .await?;
        }
        Ok(())
    }

    /// Deliver a daemon refusal through Claude's one-shot can_use_tool
    /// response. A write with uncertain delivery is retained and never
    /// retried; the persisted guard refusal also blocks a later approval.
    async fn refuse_guarded_permission(
        &mut self,
        request_id: &str,
        turn_id: &str,
        message: &str,
    ) -> Result<()> {
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Err(Error::new(
                "unknown",
                "Claude guard refusal lost its exact turn",
            ));
        }
        let Some(native) = self.native.clone() else {
            self.record_gap("Claude guard refusal has no live native control channel")
                .await?;
            return Ok(());
        };
        if let Some(entry) = self.pending_requests.get_mut(request_id) {
            if entry.turn_id != turn_id || entry.reply_state != NativeReplyState::Available {
                return Err(Error::new(
                    "unknown",
                    "Claude guarded permission binding changed",
                ));
            }
            entry.reply_state = NativeReplyState::Writing;
        } else {
            return Err(Error::new(
                "unknown",
                "Claude guarded permission was not retained",
            ));
        }
        let result = PermissionResult::Deny {
            message: message.to_owned(),
        };
        match native
            .respond_control(request_id, result, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(()) => {
                let native_id = request_event_id(request_id, "request.closed", "claude", "Claude")?;
                if let Err(error) = self
                    .commit_event(
                        "request.closed",
                        native_id,
                        json!({"request_id": request_id, "turn_id": turn_id}),
                    )
                    .await
                {
                    if let Some(entry) = self.pending_requests.get_mut(request_id) {
                        entry.reply_state = NativeReplyState::Unknown;
                    }
                    return Err(error);
                }
                self.pending_requests.remove(request_id);
            }
            Err(error) if native_no_write(&error.code) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Available;
                }
                self.record_gap(
                    "Claude guard refusal was not dispatched; guarded request remains blocked from approval",
                )
                .await?;
            }
            Err(_) => {
                if let Some(entry) = self.pending_requests.get_mut(request_id) {
                    entry.reply_state = NativeReplyState::Unknown;
                }
                self.record_gap(
                    "Claude guard refusal outcome is unresolved and will not be repeated",
                )
                .await?;
            }
        }
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
        Ok(request_reply_applied(command_id, request_id, turn_id))
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
        self.task_notification_pending = false;
        self.status.background_continuation = false;
        if parsed.terminal_reason.trim().is_empty() {
            return self
                .record_gap("Claude result omitted its terminal reason")
                .await;
        }
        // A result may be the first frame to prove that Claude consumed the
        // pending send inside the native run. Bind before committing it.
        if self.status.native_unbound
            && self
                .pending_turn
                .as_ref()
                .is_some_and(|pending| parsed.names_input(&pending.turn_id))
        {
            self.settle_pending_lifecycle(crate::native::types::CommandLifecycleState::Started)
                .await?;
        }
        // Only a result naming other inputs accounts for the prefix. A
        // result without input UUIDs can finish an already-associated turn.
        if self
            .active_input
            .as_deref()
            .is_some_and(|input| parsed.names_only_other_inputs(input))
        {
            self.native_prefix_ended = true;
            self.prefix_usage = TurnUsage::sum_native(
                "main_loop_turn",
                self.prefix_usage
                    .take()
                    .into_iter()
                    .chain(result_usage(record)),
            );
            return Ok(());
        }
        if self.status.active_turn_id.is_none()
            && parsed
                .origin
                .as_ref()
                .is_some_and(|origin| origin.kind == "task-notification")
        {
            return self
                .record_coverage_gap(
                    "Claude task-notification result had no observed native turn start",
                    &["message", "tool"],
                )
                .await;
        }
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            if let Some(pending) = self.pending_turn.take() {
                // An uncorrelated terminal before association cannot prove the
                // pending input ran; its fate stays unknown.
                self.settle_pending(
                    pending,
                    TurnOutcome::Unknown(
                        "Claude terminal record did not match the exact active turn".to_owned(),
                    ),
                );
            }
            return self
                .record_gap("Claude terminal record did not match the exact active turn")
                .await;
        };
        let command_id = self.status.active_command_id.clone();
        self.cancel_hook_callbacks_for_turn(&turn_id);
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
        if native_terminal_reason(&parsed.terminal_reason) == NativeTerminalReason::Other {
            self.record_unclassified("claude.result.terminal_reason", &parsed.terminal_reason)
                .await?;
        }
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let (outcome, terminal_reason) = terminal_outcome_and_status(
            &parsed.terminal_reason,
            &secrets,
            self.native_failure.as_ref(),
        );
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": turn_id.clone(),
            "status": if outcome == "failure" { "failed" } else { &terminal_reason },
            "outcome": outcome,
            "native_status": terminal_reason.clone(),
        });
        self.scope_turn_command(&mut data);
        let mut provider_restriction = self.native_provider_restriction.take();
        let rate_limit_event = self.native_rate_limit_event.take();
        if outcome == "failure" {
            let error = self.native_failure.take();
            if let Some(rate_limit_event) = rate_limit_event.filter(|event| {
                self.status.session_id.as_deref() == Some(event.session_id.as_str())
                    && event.turn_id == turn_id
            }) {
                match provider_restriction.as_mut() {
                    Some(restriction)
                        if restriction.kind == eliot_core::ProviderRestrictionKind::RateLimit =>
                    {
                        restriction.retry_after =
                            restriction.retry_after.max(rate_limit_event.retry_after);
                    }
                    None => {
                        provider_restriction = Some(eliot_core::ProviderRestriction {
                            kind: eliot_core::ProviderRestrictionKind::RateLimit,
                            retry_after: rate_limit_event.retry_after,
                            code: Some("rate_limit_event".to_owned()),
                            http_status: None,
                        });
                    }
                    _ => {}
                }
            }
            if error
                .as_ref()
                .is_some_and(|error| error.code == "quota_exhausted")
            {
                data["native_error"] =
                    json!({"code":"quota_exhausted", "message":"provider billing/quota refusal"});
            } else if let Some(error) = &error {
                attach_native_error(&mut data, Some(error.message.as_str()), &secrets);
                if let Some(native_error) = data["native_error"].as_object_mut() {
                    native_error.insert("code".to_owned(), json!(error.code));
                }
            }
            let failure_origin = error
                .as_ref()
                .and_then(|error| error.origin)
                .or_else(|| {
                    provider_restriction
                        .as_ref()
                        .map(|_| eliot_core::FailureOrigin::Provider)
                })
                .unwrap_or(eliot_core::FailureOrigin::Unknown);
            let failure_evidence = error
                .as_ref()
                .and_then(|error| error.origin_evidence.as_deref())
                .unwrap_or(if provider_restriction.is_some() {
                    "rate_limit_event reported rejected status for this session and turn"
                } else {
                    "terminal reason lacks a structured assistant API error"
                });
            if let Some(provider_restriction) = provider_restriction {
                eliot_plugin::session::attach_provider_restriction(
                    &mut data,
                    provider_restriction,
                    &secrets,
                );
            }
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "claude",
                failure_origin,
                failure_evidence,
                &secrets,
            );
        }
        // Absent or malformed native usage leaves `data.usage` unset.
        // Attaches before `attach_final_text` so the existing bounded
        // terminal-text accounting includes the usage bytes.
        let turn_usage = match self.prefix_usage.take() {
            Some(prefix) => TurnUsage::sum_native(
                "main_loop_turn",
                std::iter::once(prefix).chain(result_usage(record)),
            ),
            None => result_usage(record),
        };
        if let Some(usage) = self.first_call_usage.finish("main_loop_turn", turn_usage) {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
        }
        // Proven MCP surface from the owned stream's `system`/`init`
        // record: attached once observed, so the first terminal after init
        // carries the readback into the effective set. Unobserved stays
        // unattached and the effective set stays unknown.
        attach_mcp_readback(&mut data, self.init_mcp_servers.as_deref());
        // Optional session-scoped fact from the terminal record (latest
        // main-thread single-call context, cumulative `modelUsage` counters,
        // cumulative `total_cost_usd`, and model window where reported), held
        // for the session fact that commits after the accepted terminal.
        // The stream binds the latest assistant message. Its finalized
        // transcript entry provides occupancy and the result supplies capacity.
        let session_id = self.status.session_id.clone().unwrap_or_default();
        let roots = transcript_config_roots();
        let latest_message = self.latest_context_message.take();
        let latest_model = latest_message.as_ref().map(|(model, _)| model.clone());
        let transcript = tokio::task::spawn_blocking(move || {
            transcript_usage_in(&roots, &session_id, latest_message)
        })
        .await
        .ok()
        .flatten();
        let mut session_usage = result_session_usage(
            record,
            latest_model.as_deref(),
            transcript.and_then(|(_, context)| context),
        );
        if let Some(usage) = session_usage.as_mut() {
            usage.session_file_bytes = transcript.map(|(size, _)| size);
        }
        let final_text = self.final_text.take();
        if self.empty_after_tool_hooks > 0 {
            data["empty_after_tool_hooks"] =
                json!(std::mem::take(&mut self.empty_after_tool_hooks));
        }
        if let Some(text) = final_text {
            // Attach the one final native message; the shared boundary
            // accounts for its event payload size.
            attach_final_text(&mut data, &secrets.redact_text(&text));
        }
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:turn.completed");
        // Publish the observed terminal BEFORE the commit resolves: the host
        // may snapshot as soon as it receives the batch, and a control
        // racing the terminal must see the finished turn (same order as the
        // association path, which binds active before its own commit). It
        // stays `write_pending` until the commit resolves; a failed commit
        // keeps this terminal as the turn's evidence.
        let session_turn_id = turn_id.clone();
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id,
            status: universal_status(outcome),
            native_status: Some(terminal_reason),
            final_text: data
                .get("final_text")
                .and_then(Value::as_str)
                .map(str::to_owned),
            terminal: true,
            write_pending: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.status.native_unbound = false;
        self.status.wake_cancel_pending = false;
        self.last_input = self.active_input.take();
        self.native_prefix_ended = false;
        self.interrupted_turn = None;
        self.tool_use_turns.clear();
        self.cancelled_hook_requests.clear();
        self.clear_tool_tracking();
        self.tool_coverage_gap_reported = false;
        self.tool_use_tracking_fenced = false;
        self.publish_now();
        let committed = self.commit_event("turn.completed", native_id, data).await;
        if let Some(last) = self.status.last_turn.as_mut() {
            last.write_pending = false;
        }
        self.publish_now();
        committed?;
        // Session-scoped tokens/cost commit as their own `session.usage`
        // fact after the accepted terminal, so session scope and turn scope
        // can never be confused.
        if let Some(session_usage) = session_usage {
            let session_id = self.status.session_id.as_deref().unwrap_or_default();
            let native_id = format!("claude:{session_id}:{session_turn_id}:session.usage");
            self.commit_event("session.usage", native_id, json!({"usage": session_usage}))
                .await?;
        }
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
        self.cancel_all_hook_callbacks();
        if self.planned_stop || self.closed {
            // A sticky record-local fault survives planned stop: flush its
            // gap before end-stop settlement instead of erasing it.
            if let Some(reason) = self.pending_fault_gap.take()
                && !self.gap_reported
                && !self.writes_blocked
            {
                let _ = self.record_gap(&reason).await;
            }
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
            self.status.failure = Some(*error.message);
        }
        if !self.gap_reported && !self.writes_blocked {
            let reason = self
                .pending_fault_gap
                .take()
                .unwrap_or_else(|| "Claude event stream closed outside a planned stop".to_owned());
            if let Some(pending) = self.pending_turn.take() {
                self.settle_pending(pending, TurnOutcome::Unknown(reason.clone()));
            }
            let _ = self.record_gap(&reason).await;
        } else if let Some(pending) = self.pending_turn.take() {
            self.settle_pending(
                pending,
                TurnOutcome::Unknown(self.status.failure.clone().unwrap_or_else(|| {
                    "Claude event source acknowledgement is unresolved".to_owned()
                })),
            );
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
            StreamState::Malformed(fault) => {
                // Active record-local fault: record its gap immediately with
                // the safe typed label; keep it sticky for the planned-stop
                // drain in case this immediate record is blocked.
                let reason = format!(
                    "Claude stream contained an invalid frame ({})",
                    fault.describe()
                );
                if self.pending_fault_gap.is_none() {
                    self.pending_fault_gap = Some(reason.clone());
                }
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self.record_gap(&reason).await;
                }
            }
            StreamState::Closed => {
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
        if let Some(turn_id) = self.status.active_turn_id.clone() {
            self.status.last_turn = Some(TurnFacts {
                command_id: self.status.active_command_id.clone(),
                turn_id,
                status: None,
                native_status: None,
                final_text: None,
                terminal: false,
                write_pending: false,
            });
        }
        if let Some(pending) = self.pending_turn.take() {
            self.settle_pending(
                pending,
                TurnOutcome::Unknown(
                    "Claude stopped before the input association was observed".to_owned(),
                ),
            );
        }
        self.status.stopped = true;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
        self.status.native_unbound = false;
        self.status.wake_cancel_pending = false;
        self.active_input = None;
        self.native_prefix_ended = false;
        self.prefix_usage = None;
        self.interrupted_turn = None;
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

    async fn commit_request_event(
        &mut self,
        native_id: String,
        data: Value,
    ) -> Result<Option<eliot_core::PermissionRefusal>> {
        self.stream_source()
            .commit_request_event(native_id, data)
            .await
    }

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        let current = data["turn_id"]
            .as_str()
            .is_some_and(|turn| self.status.active_turn_id.as_deref() == Some(turn));
        if matches!(kind, "turn.completed" | "session.ended") {
            self.terminal_deadline = None;
        }
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await?;
        if kind == "turn.started" {
            self.terminal_deadline = Some(Instant::now() + self.terminal_inactivity);
        } else if current && kind != "turn.unknown" {
            self.refresh_terminal_deadline();
        }
        Ok(())
    }

    fn refresh_terminal_deadline(&mut self) {
        if let Some(deadline) = &mut self.terminal_deadline {
            *deadline = Instant::now() + self.terminal_inactivity;
        }
    }

    async fn expire_terminal_deadline(&mut self) -> Result<()> {
        self.terminal_deadline = None;
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            return Ok(());
        };
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:turn.unknown");
        let mut data = json!({
            "command_id": self.status.active_command_id,
            "turn_id": turn_id,
            "reason": NATIVE_TERMINAL_DEADLINE_REASON,
        });
        self.scope_turn_command(&mut data);
        self.commit_event("turn.unknown", native_id, data).await
    }

    async fn record_unclassified(&mut self, site: &'static str, native_tag: &str) -> Result<()> {
        // An observed session-start record is only a candidate until Ready
        // compares it with the exact id minted for this native session.
        if !self.status.ready {
            return Ok(());
        }
        let sequence = self.unclassified_sequence;
        self.unclassified_sequence = sequence.checked_add(1).ok_or_else(|| {
            Error::new("internal", "Claude unclassified observation id exhausted")
        })?;
        let native_id = format!("claude:g{}:native_unclassified:{sequence}", self.generation);
        self.commit_event(
            eliot_plugin::native_data::UNCLASSIFIED_NATIVE_EVENT,
            native_id,
            eliot_plugin::native_data::unclassified(site, native_tag),
        )
        .await
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.stream_source().record_gap(reason).await
    }

    async fn record_coverage_gap(&mut self, reason: &str, families: &[&str]) -> Result<()> {
        self.stream_source()
            .record_coverage_gap(reason, families)
            .await
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
            families: &["session", "turn", "message", "request", "tool"],
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.status.source_unknown,
            batch_unknown: &mut self.status.batch_unknown,
            failure: &mut self.status.failure,
        }
    }
}

fn is_main_thread_assistant(record: &Value) -> bool {
    is_main_thread(record)
}

/// Main-thread `user` records carry this turn's tool results. Subagent
/// text forwarded with `parent_tool_use_id` (see `--forward-subagent-text`)
/// stays out of this turn's tool records, exactly like assistant text.
fn is_main_thread_user(record: &Value) -> bool {
    is_main_thread(record)
}

fn is_main_thread(record: &Value) -> bool {
    let message = record.get("message");
    [
        record.get("parent_tool_use_id"),
        record.get("agent_id"),
        message.and_then(|message| message.get("parent_tool_use_id")),
        message.and_then(|message| message.get("agent_id")),
    ]
    .into_iter()
    .all(|field| field.is_none_or(Value::is_null))
}

/// Native verdict for one `tool_result` block: the frame's own `is_error`
/// flag, with an unreported verdict reading as completion. The flag is the
/// only status the stream-json `user` record carries; there is no second
/// native status to preserve.
fn tool_result_status(is_error: Option<bool>) -> &'static str {
    if is_error == Some(true) {
        "error"
    } else {
        "completed"
    }
}

fn assistant_prompt_id(record: &Value) -> Option<String> {
    record
        .get("prompt_id")
        .or_else(|| {
            record
                .get("message")
                .and_then(|message| message.get("prompt_id"))
        })
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .map(ToOwned::to_owned)
}

/// SDKAssistantMessage.error enum, verified against the installed Claude Code
/// 2.1.278 bundle on 2026-11-23. Free text and generic api_error are not evidence.
fn claude_provider_restriction(kind: &str) -> Option<eliot_core::ProviderRestriction> {
    let restriction_kind = match kind {
        "billing_error" => eliot_core::ProviderRestrictionKind::Quota,
        "rate_limit" => eliot_core::ProviderRestrictionKind::RateLimit,
        _ => return None,
    };
    Some(eliot_core::ProviderRestriction {
        kind: restriction_kind,
        retry_after: None,
        code: Some(kind.to_owned()),
        http_status: None,
    })
}

fn claude_api_failure(kind: &str, text: &str) -> Option<Error> {
    use eliot_core::FailureOrigin;
    if kind == "billing_error" {
        return Some(Error::provider(
            eliot_core::ProviderRestrictionKind::Quota,
            "plugin.claude.turn",
            "assistant.error upstream billing/quota refusal",
        ));
    }
    let (origin, evidence) = match kind {
        "authentication_failed" | "oauth_org_not_allowed" | "account_on_hold" => (
            FailureOrigin::Provider,
            "assistant.error upstream authentication refusal",
        ),
        "rate_limit" | "overloaded" => (
            FailureOrigin::Provider,
            "assistant.error upstream rate/capacity refusal",
        ),
        "model_not_found" => (
            FailureOrigin::Provider,
            "assistant.error upstream model refusal",
        ),
        "server_error" => (
            FailureOrigin::Provider,
            "assistant.error upstream server failure",
        ),
        "cloud_credential_error" => (
            FailureOrigin::Environment,
            "local AWS or Google credential acquisition failed before an upstream request",
        ),
        "invalid_request" | "max_output_tokens" => (
            FailureOrigin::Harness,
            "native refusal without structured upstream evidence",
        ),
        "api_error" => (
            FailureOrigin::Unknown,
            "assistant.error reported an unclassified API error",
        ),
        _ => return None,
    };
    let message = if is_blank_native_text(text) {
        kind.to_owned()
    } else {
        format!("{kind}: {}", text.trim())
    };
    Some(Error::new(kind, message).with_origin(origin, evidence))
}
