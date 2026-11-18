mod hooks;

use self::hooks::{
    HookToolFacts, approval_summary, hook_callback_response_fits, hook_deadline, hook_tool_facts,
};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, Result, SecretPolicy,
    requests::{ApprovalChoice, ApprovalDecision, ApprovalRequest},
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, MAX_TEXT_BYTES, PeerClient,
    native_stdio::StreamState,
    protocol::{HookEvent, HookResponse},
    session::{
        EVENT_DEADLINE, StreamSource, approval_reply_args, attach_final_text,
        attach_final_text_unavailable, native_no_write, next_input_outcomes, rejected_control,
        request_event_id, unknown_control, with_claimed_context,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
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
        HookCallbackResponse, InitMcpServer, PermissionResult, ResultRecord, assistant_model,
        assistant_text, assistant_tool_use_count, assistant_tool_use_ids, assistant_tool_uses,
        attach_mcp_readback, frame_context_window, init_mcp_servers, result_session_usage,
        result_usage, transcript_bytes_in, transcript_config_roots, user_tool_results,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_PENDING_REQUESTS: usize = 16;
const MAX_PENDING_HOOKS: usize = 16;
const MAX_TOOL_USE_BINDINGS: usize = 128;
const DENY_MESSAGE: &str = "Eliot denied this tool call.";
const APPROVE_CHOICE_ID: &str = "approve";
const DENY_CHOICE_ID: &str = "deny";

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub native_drained: bool,
    pub background_continuation: bool,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_text_unavailable: Option<String>,
    pub terminal: bool,
}

pub(crate) struct EventConfig {
    pub credential: String,
    pub agent_id: String,
    pub generation: i64,
    pub secrets: Vec<String>,
    /// Owned session workspace: hook tool paths are normalized against it.
    pub workspace: PathBuf,
}

pub(crate) struct EventHandle {
    control: mpsc::Sender<Control>,
    status: watch::Receiver<EventStatus>,
    hook_callback_id: String,
}

/// Outcome of one `ExpectTurn` wait, decided by the exact stream facts.
pub(crate) enum TurnOutcome {
    Associated,
    Rejected(String),
    Unknown(String),
}

/// Outcome of one queued steer wait, decided by the exact hook facts. Only
/// `Applied` means the native `PreToolUse.additionalContext` response carried
/// the steer text; `Stale` means the captured turn ended first and the text
/// was dropped, never leaked into another turn.
pub(crate) enum SteerOutcome {
    Applied,
    Rejected(String),
    Unknown(String),
    Stale,
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
    Steer {
        command_id: String,
        turn_id: String,
        text: String,
        registered: oneshot::Sender<Result<()>>,
        outcome: oneshot::Sender<SteerOutcome>,
    },
    CancelSteer {
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

#[derive(Clone)]
struct BufferedRecord {
    record: Value,
    size: usize,
}

struct PendingTurn {
    command_id: String,
    turn_id: String,
    context: Vec<ContextItem>,
    outcome: Option<oneshot::Sender<TurnOutcome>>,
}

/// One steer queued for its exact captured turn. The text rides the next live
/// `PreToolUse.additionalContext` response for that turn only; turn end,
/// stop, or stream loss settles it without ever carrying it forward.
struct PendingSteer {
    command_id: String,
    turn_id: String,
    text: String,
    outcome: Option<oneshot::Sender<SteerOutcome>>,
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

/// Canonical outcome for one Claude terminal reason, decided here at the
/// plugin boundary; the verbatim reason is preserved as `native_status`
/// next to it. `success` (2.1.278) and `completed` (live 2.1.321 terminal
/// frame) are the recognized successes. `cancelled` and `interrupted` are
/// interrupts; supported native failures (`error`, `failed`, `failure`
/// and the `api_error...` family) fail. Anything else is unknown: an
/// unrecognized reason never defaults to success.
fn claude_outcome(reason: &str) -> &'static str {
    match reason.trim() {
        "success" | "completed" => "success",
        "cancelled" | "interrupted" | "aborted_streaming" => "interrupted",
        "error" | "failed" | "failure" => "failure",
        reason => {
            let lower = reason.to_lowercase();
            if lower.starts_with("api_error")
                || lower.starts_with("error")
                || lower.starts_with("failed")
                || lower.starts_with("failure")
            {
                "failure"
            } else {
                "unknown"
            }
        }
    }
}

/// Classify the original terminal reason first, then redact for persistence:
/// a configured secret inside the reason must not change its outcome (a
/// secret `error` redacts `api_error` into `api_[REDACTED]`, i.e. unknown).
/// Returns the canonical outcome and the sanitized reason.
fn terminal_outcome_and_status(
    reason: &str,
    secrets: &SecretPolicy,
    failure: Option<&Error>,
) -> (&'static str, String) {
    let mut outcome = claude_outcome(reason);
    if outcome == "success" && failure.is_some_and(|error| error.code == "quota_exhausted") {
        outcome = "failure";
    }
    (outcome, secrets.redact_text(reason))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HookConsumerState {
    Available,
    InFlight,
    Finished,
}

#[derive(Clone)]
struct ToolUseBinding {
    turn_id: String,
    prompt_id: Option<String>,
    hook_state: HookConsumerState,
    active_consumer_id: Option<String>,
}

struct PendingHookCallback {
    generation: i64,
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
    buffer: VecDeque<BufferedRecord>,
    buffer_bytes: usize,
    gap_number: u64,
    pending_turn: Option<PendingTurn>,
    pending_steer: Option<PendingSteer>,
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
    /// At most one tool-coverage gap per turn; it does not invalidate the
    /// separately associated native terminal or future input.
    tool_coverage_gap_reported: bool,
    /// A background completion can wake the native main loop without a SEND.
    /// Its next unassociated init/output is never assigned to a queued input.
    task_notification_pending: bool,
    autonomous_output: bool,
    deny_uncertain: HashSet<String>,
    permission_admission_fenced: bool,
    message_seq: u64,
    accept_frames: bool,
    planned_stop: bool,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    /// Sticky first record-local fault awaiting its gap (planned-stop drain
    /// must not erase it if the immediate record was blocked).
    pending_fault_gap: Option<String>,
    final_text: Option<String>,
    final_text_unavailable: bool,
    native_failure: Option<Error>,
    /// Proven MCP surface from the owned stream's `system`/`init` record
    /// (names plus statuses). Retained on observation and attached to the
    /// next turn terminal so the reducer merges it into the effective set;
    /// absent until the first post-input init arrives.
    init_mcp_servers: Option<Vec<InitMcpServer>>,
    /// Model context window from the owned stream's `system`/`init` record
    /// where the harness reports one. Retained on observation and merged
    /// into the terminal session fact when the result frame carries none;
    /// absent until observed, never guessed.
    init_context_window: Option<i64>,
    closed: bool,
    native: Option<ClaudeClient>,
    status_tx: watch::Sender<EventStatus>,
    control_tx: Option<mpsc::Sender<Control>>,
    pending_hooks: HashMap<String, PendingHookCallback>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<ClaudeFrame>,
    stream: watch::Receiver<StreamState>,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (status_tx, status) = watch::channel(EventStatus::default());
    let mut processor = EventProcessor::new(host, config, status_tx.clone());
    let hook_callback_id = processor.hook_callback_id.clone();
    processor.control_tx = Some(control.clone());
    tokio::spawn(run(frames, stream, processor, control_rx, status_tx));
    EventHandle {
        control,
        status,
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
        context: Vec<ContextItem>,
    ) -> Result<oneshot::Receiver<TurnOutcome>> {
        let (registered_tx, registered_rx) = oneshot::channel();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        self.control
            .send(Control::ExpectTurn {
                command_id,
                turn_id,
                context,
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

    /// Queue one steer for its exact captured turn. Registration is
    /// synchronous behind any queued terminal frame, so a steer racing
    /// completion refuses instead of queueing behind a dead turn. The
    /// returned waiter resolves only when the native hook response confirms
    /// the text rode it (`Applied`), the turn ends first (`Stale`), or the
    /// write proves itself unapplied or unresolved.
    pub async fn steer(
        &self,
        command_id: String,
        turn_id: String,
        text: String,
    ) -> Result<oneshot::Receiver<SteerOutcome>> {
        let (registered_tx, registered_rx) = oneshot::channel();
        let (outcome_tx, outcome_rx) = oneshot::channel();
        self.control
            .send(Control::Steer {
                command_id,
                turn_id,
                text,
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

    /// Queue exact-command steer withdrawal on the event task without making
    /// the host control dispatcher wait behind a native callback write/receipt.
    /// The returned receiver resolves when that task processes the request.
    pub fn request_cancel_steer(&self, command_id: String) -> oneshot::Receiver<bool> {
        let control = self.control.clone();
        let (completed_tx, completed_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (reply_tx, reply_rx) = oneshot::channel();
            let completed = control
                .send(Control::CancelSteer {
                    command_id,
                    reply: reply_tx,
                })
                .await
                .is_ok()
                && reply_rx.await.is_ok();
            let _ = completed_tx.send(completed);
        });
        completed_rx
    }

    /// Whether `turn_id` is the exact live turn, answered behind any queued
    /// terminal frame so a control never targets a finished turn.
    pub async fn check_turn(&self, turn_id: &str) -> bool {
        self.turn_interrupt_status(turn_id).await.is_some()
    }

    /// The exact live turn's interrupt status, or None if the turn ended.
    pub async fn turn_interrupt_status(&self, turn_id: &str) -> Option<bool> {
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
            return None;
        }
        reply_rx.await.ok().flatten()
    }

    pub async fn mark_interrupted(&self, turn_id: &str) {
        let _ = self
            .control
            .send(Control::MarkInterrupted {
                turn_id: turn_id.to_owned(),
            })
            .await;
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

    pub async fn drain_native(&self) -> Result<()> {
        eliot_plugin::native_lifecycle::wait_for_native_tail(&self.status, |s| s.native_drained)
            .await
    }

    pub fn status(&self) -> EventStatus {
        let mut status = self.status.borrow().clone();
        if self.control.is_closed() && status.failure.is_none() {
            status.failure = Some("Claude event task is unavailable".to_owned());
        }
        status
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
    let mut stream_open = true;
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
            changed = stream.changed(), if frames_open && stream_open => {
                if changed.is_err() {
                    stream_open = false;
                }
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
        Control::Steer {
            command_id,
            turn_id,
            text,
            registered,
            outcome,
        } => {
            let result = processor.register_steer(command_id, turn_id, text, outcome);
            publish(processor, status);
            let _ = registered.send(result);
        }
        Control::CancelSteer { command_id, reply } => {
            processor.cancel_steer(&command_id);
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
            // Still-queued here proves never written (same as cancel): the
            // serial actor settles every taken steer before this control runs.
            processor.fail_pending_steer(SteerOutcome::Rejected(
                "Claude stopped before PreToolUse confirmed steer".to_owned(),
            ));
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
            processor.fail_pending_steer(SteerOutcome::Rejected(
                "Claude event task closed before PreToolUse confirmed steer".to_owned(),
            ));
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
            hook_callback_id: ulid::Ulid::new().to_string(),
            secrets: config.secrets,
            workspace: config.workspace,
            status: EventStatus::default(),
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            gap_number: 0,
            pending_turn: None,
            pending_steer: None,
            interrupted_turn: None,
            pending_requests: HashMap::new(),
            cancelled_hook_requests: HashSet::new(),
            tool_use_turns: HashMap::new(),
            tool_use_tracking_fenced: false,
            tool_names: HashMap::new(),
            tools_started: HashSet::new(),
            tools_completed: HashSet::new(),
            tool_coverage_gap_reported: false,
            task_notification_pending: false,
            autonomous_output: false,
            deny_uncertain: HashSet::new(),
            permission_admission_fenced: false,
            message_seq: 0,
            accept_frames: true,
            planned_stop: false,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            pending_fault_gap: None,
            final_text: None,
            final_text_unavailable: false,
            native_failure: None,
            init_mcp_servers: None,
            init_context_window: None,
            closed: false,
            native: None,
            status_tx,
            control_tx: None,
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

    /// Queue one steer behind any queued terminal frame: only the exact live
    /// turn is accepted, and only one steer waits at a time (a second is
    /// `busy`, never silently dropped or reordered).
    fn register_steer(
        &mut self,
        command_id: String,
        turn_id: String,
        text: String,
        outcome: oneshot::Sender<SteerOutcome>,
    ) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
            || text.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Claude steer requires a ready exact session, turn, and text",
            ));
        }
        if self.pending_steer.is_some() {
            return Err(Error::new(
                "busy",
                "Claude already has a steer waiting for PreToolUse",
            ));
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str()) {
            return Err(Error::new(
                "stale_turn",
                "Claude steer does not match the exact active turn",
            ));
        }
        // All three refusals below fire before insertion, so no native write
        // is possible: they carry the explicit `native_not_dispatched` proof
        // (reasons preserved) instead of generic unknown.
        if self.planned_stop || self.closed {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude session is stopping; steer will not be applied",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude event source acknowledgement is unresolved",
            ));
        }
        if self.status.failure.is_some() {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude event source has an unresolved gap",
            ));
        }
        self.pending_steer = Some(PendingSteer {
            command_id,
            turn_id,
            text,
            outcome: Some(outcome),
        });
        Ok(())
    }

    /// Withdraw the exact queued steer at the host's deadline, so a later hook
    /// cannot apply text the caller no longer owns. The serial actor settles
    /// every taken steer before this cancel runs, so a still-queued steer
    /// provably never rode a hook response: its removal reports an explicit
    /// no-effect rejection, never a dropped sender (which would misclassify as
    /// Unknown). A settled or foreign command is untouched.
    fn cancel_steer(&mut self, command_id: &str) {
        let pending_matches = self
            .pending_steer
            .as_ref()
            .is_some_and(|pending| pending.command_id == command_id);
        if pending_matches && let Some(steer) = self.pending_steer.take() {
            Self::settle_steer(
                steer,
                SteerOutcome::Rejected(
                    "Claude steer was withdrawn before PreToolUse applied it".to_owned(),
                ),
            );
        }
    }

    /// Take the queued steer for the exact turn about to be written.
    fn take_pending_steer(&mut self, turn_id: &str) -> Option<PendingSteer> {
        if self
            .pending_steer
            .as_ref()
            .is_some_and(|steer| steer.turn_id == turn_id)
        {
            self.pending_steer.take()
        } else {
            None
        }
    }

    fn settle_steer(steer: PendingSteer, outcome: SteerOutcome) {
        if let Some(sender) = steer.outcome {
            let _ = sender.send(outcome);
        }
    }

    fn fail_pending_steer(&mut self, outcome: SteerOutcome) {
        if let Some(steer) = self.pending_steer.take() {
            Self::settle_steer(steer, outcome);
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
                if error.failure_where.as_deref() == Some("plugin.events.put") {
                    return Err(error);
                }
                self.status.source_unknown = true;
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Buffered Claude native record could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(*error.message);
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
        if kind == Some("assistant") || kind == Some("user") {
            if self.autonomous_output || !is_main_thread(record) {
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
        if let Err(error) = self.flush_buffer().await
            && error.failure_where.as_deref() != Some("plugin.events.put")
            && self.status.failure.is_none()
        {
            self.status.failure = Some(*error.message);
        }
    }

    async fn process_record(&mut self, record: Value) -> Result<()> {
        let kind = record
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // Sidechains can outlive the parent's turn. Filter before session,
        // association and tool-table checks, never against a later active turn.
        if matches!(kind, "assistant" | "user")
            && (self.autonomous_output || !is_main_thread(&record))
        {
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
            self.observe_task_boundary(record);
            self.retain_init_mcp_servers(record);
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
        self.retain_init_mcp_servers(record);
        Ok(())
    }

    fn observe_task_boundary(&mut self, record: &Value) {
        match record.get("subtype").and_then(Value::as_str) {
            Some("task_notification") => self.task_notification_pending = true,
            Some("init") if self.task_notification_pending => {
                self.autonomous_output = true;
                self.task_notification_pending = false;
                // Until the terminal supplies provenance, even a lifecycle
                // for a queued input cannot disambiguate this output prefix.
                self.final_text = None;
            }
            _ => {}
        }
        self.status.background_continuation =
            self.task_notification_pending || self.autonomous_output;
    }

    /// Retain the proven MCP surface from the owned session's
    /// `system`/`init` record. Only an observed inventory is kept (with
    /// secrets redacted); anything unprovable leaves the retained set
    /// untouched so the effective servers stay unknown, never guessed.
    fn retain_init_mcp_servers(&mut self, record: &Value) {
        if record.get("subtype").and_then(Value::as_str) != Some("init") {
            return;
        }
        if let Some(window) = frame_context_window(record) {
            self.init_context_window = Some(window);
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
            context,
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
                self.status.active_command_id = Some(command_id.clone());
                self.status.active_turn_id = Some(turn_id.clone());
                self.native_failure = None;
                self.final_text = None;
                self.final_text_unavailable = false;
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
                settle(outcome, TurnOutcome::Associated);
                // Buffered assistant text drains in the caller's trailing
                // flush, not here, keeping the commit path non-recursive.
                Ok(())
            }
            "cancelled" => {
                self.cancelled_hook_requests.clear();
                settle(
                    outcome,
                    TurnOutcome::Rejected("Claude did not admit the input".to_owned()),
                );
                Ok(())
            }
            _ => {
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
        if is_main_thread_assistant(record) {
            self.native_failure = record
                .get("error")
                .and_then(Value::as_str)
                .and_then(claude_api_failure);
            self.bind_tool_uses(record, &turn_id);
            self.observe_tool_uses(record).await?;
        }
        let observed_model = if record.get("error").is_none_or(Value::is_null) {
            assistant_model(record).filter(|model| model != "<synthetic>")
        } else {
            None
        };
        let text = assistant_text(record);
        if text.is_empty() {
            if let Some(model) = observed_model {
                self.message_seq = self.message_seq.saturating_add(1);
                let session_id = self.status.session_id.as_deref().unwrap_or_default();
                let native_id = format!("claude:{session_id}:{turn_id}:model:{}", self.message_seq);
                self.commit_event(
                    "turn.model_observed",
                    native_id,
                    json!({
                        "command_id": self.status.active_command_id,
                        "turn_id": turn_id,
                        "model": model,
                    }),
                )
                .await?;
            }
            return Ok(());
        }
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
            self.final_text = None;
            self.final_text_unavailable = true;
            return self
                .record_gap("Claude completed message exceeded the persisted-event bound")
                .await;
        }
        self.message_seq = self.message_seq.saturating_add(1);
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:message:{}", self.message_seq);
        self.accumulate_final_text(&text);
        self.commit_event("message.completed", native_id, data)
            .await
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
        let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) else {
            return Ok(());
        };
        for tool_use in assistant_tool_uses(record) {
            self.tool_names
                .entry(tool_use.id.clone())
                .or_insert_with(|| tool_use.name.clone());
            if !self.tools_started.insert(tool_use.id.clone()) {
                continue;
            }
            let data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "item_id": tool_use.id,
                "tool": tool_use.name,
            });
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
        let Some(command_id) = self.status.active_command_id.clone() else {
            return self
                .record_gap("Claude tool result has no exact Eliot command")
                .await;
        };
        let mut unknown = false;
        for result in results {
            let Some(tool) = self.tool_names.get(&result.tool_use_id).cloned() else {
                unknown = true;
                continue;
            };
            if !self.tools_completed.insert(result.tool_use_id.clone()) {
                continue;
            }
            let data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "item_id": result.tool_use_id,
                "tool": tool,
                "status": tool_result_status(result.is_error),
            });
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

    fn accumulate_final_text(&mut self, text: &str) {
        if self.final_text_unavailable {
            return;
        }
        let bytes = self
            .final_text
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(text.len());
        if bytes > MAX_EVENT_BYTES {
            self.final_text = None;
            self.final_text_unavailable = true;
            return;
        }
        let accumulated = self.final_text.get_or_insert_default();
        accumulated.push_str(text);
        let mut data = json!({
            "command_id": self.status.active_command_id.as_deref().unwrap_or_default(),
            "turn_id": self.status.active_turn_id.as_deref().unwrap_or_default(),
        });
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        attach_final_text(&mut data, accumulated, true, &secrets);
        if data.get("final_text").is_none() {
            self.final_text = None;
            self.final_text_unavailable = true;
        }
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
                    hook_state: HookConsumerState::Available,
                    active_consumer_id: None,
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
        if body.subtype != "hook_callback"
            || body.callback_id != self.hook_callback_id
            || body.input.get("hook_event_name").and_then(Value::as_str) != Some("PreToolUse")
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
        if binding.hook_state != HookConsumerState::Available {
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        if let (Some(input_prompt_id), Some(assistant_prompt_id)) =
            (input_prompt_id, binding.prompt_id.as_deref())
            && input_prompt_id != assistant_prompt_id
        {
            self.finish_available_tool_use_hook(&tool_use_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let turn_id = binding.turn_id.clone();
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str())
            || !self.status.ready
            || self.status.failure.is_some()
            || self.status.stopped
        {
            self.finish_available_tool_use_hook(&tool_use_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        if self.pending_hooks.len() >= MAX_PENDING_HOOKS {
            self.finish_available_tool_use_hook(&tool_use_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        let Some(control_tx) = self.control_tx.clone() else {
            self.finish_available_tool_use_hook(&tool_use_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        };
        let consumer_id = ulid::Ulid::new().to_string();
        // Bounded progress diagnostics from the native hook input only. The
        // matched invocation id is already proven above; the name, operation
        // and path are optional and stay absent when the hook omits them.
        let policy = SecretPolicy::new(self.secrets.iter().cloned());
        let tool = hook_tool_facts(&body.input, &tool_use_id, &self.workspace, &policy);
        let event = HookEvent {
            hook: "before_tool".to_owned(),
            turn_id: turn_id.clone(),
            consumer_id: consumer_id.clone(),
        };
        let mut params = match serde_json::to_value(event) {
            Ok(params) => params,
            Err(_) => {
                self.finish_available_tool_use_hook(&tool_use_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
        };
        let Some(params_object) = params.as_object_mut() else {
            self.finish_available_tool_use_hook(&tool_use_id);
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
            binding.hook_state = HookConsumerState::InFlight;
            binding.active_consumer_id = Some(consumer_id.clone());
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
            let _ = control_tx
                .send(Control::HookResponseReady {
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
        let hook_turn_id = pending.turn_id.clone();
        let context = match result {
            Ok(context) => context,
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, &consumer_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
        };
        let context_text = if context.is_empty() {
            String::new()
        } else {
            match with_claimed_context("", &context, "Claude") {
                Ok(text) => text.strip_suffix("\n\n").unwrap_or(&text).to_owned(),
                Err(_) => {
                    self.pending_hooks.remove(request_id);
                    self.finish_tool_use_hook(&tool_use_id, &consumer_id);
                    return self.write_hook_pass(&native, request_id, deadline).await;
                }
            }
        };
        // Neither FIFO context nor a queued steer for this exact turn: the
        // existing pass path below. A queued steer takes the combined path
        // even when the hook selected no context.
        let steer_waiting = self
            .pending_steer
            .as_ref()
            .is_some_and(|steer| steer.turn_id == hook_turn_id);
        if context.is_empty() && !steer_waiting {
            return self
                .finish_hook_pass(request_id, &native, deadline, &tool_use_id, &consumer_id)
                .await;
        }
        if !self.hook_callback_is_live(request_id) {
            // Not live: the steer (if any) stays queued for the next
            // PreToolUse of this same turn; turn end settles it, never
            // leaks it into another turn.
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(&tool_use_id, &consumer_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        // Take the queued steer only now, immediately before the only native
        // write that can carry additionalContext. FIFO context comes first,
        // then the steer text; a raw-text or complete-frame overflow rejects
        // the steer and keeps the full FIFO context when its frame fits.
        let mut steer = self.take_pending_steer(&hook_turn_id);
        let mut combined = context_text.clone();
        if let Some(waiting) = steer.as_ref() {
            combined = if combined.trim().is_empty() {
                waiting.text.clone()
            } else {
                format!("{}\n\n{}", combined, waiting.text)
            };
            if combined.len() > MAX_TEXT_BYTES {
                if let Some(waiting) = steer.take() {
                    Self::settle_steer(
                        waiting,
                        SteerOutcome::Rejected(
                            "Claude steer with hook context exceeds 512 KiB".to_owned(),
                        ),
                    );
                }
                combined = context_text.clone();
            }
        }
        let mut response = HookCallbackResponse::with_context(combined.clone());
        if !hook_callback_response_fits(request_id, &response)
            && let Some(waiting) = steer.take()
        {
            Self::settle_steer(
                waiting,
                SteerOutcome::Rejected(
                    "Claude steer with hook context exceeds the 1 MiB encoded response bound"
                        .to_owned(),
                ),
            );
            combined = context_text.clone();
            response = HookCallbackResponse::with_context(combined.clone());
        }
        if context.is_empty() && steer.is_none() {
            return self
                .finish_hook_pass(request_id, &native, deadline, &tool_use_id, &consumer_id)
                .await;
        }
        if !hook_callback_response_fits(request_id, &response) {
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(&tool_use_id, &consumer_id);
            return Err(Error::new(
                "frame_too_large",
                "Claude FIFO hook context exceeds the 1 MiB encoded response bound",
            ));
        }
        // Recheck the monotonic deadline, cancellation (pending-map membership),
        // generation and exact invocation-to-turn binding immediately before
        // the only native write that can carry additionalContext.
        if !self.hook_callback_is_live(request_id) {
            if let Some(waiting) = steer.take() {
                // The callback died between the take and the write without
                // the turn ending: no write happened, so the steer is
                // rejected (proven unapplied), never silently requeued.
                Self::settle_steer(
                    waiting,
                    SteerOutcome::Rejected(
                        "Claude hook callback ended before steer applied".to_owned(),
                    ),
                );
            }
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(&tool_use_id, &consumer_id);
            return self.write_hook_pass(&native, request_id, deadline).await;
        }
        match native
            .respond_hook_callback(request_id, response, deadline)
            .await
        {
            Ok(()) => {}
            Err(error) if native_no_write(&error.code) => {
                if let Some(waiting) = steer.take() {
                    Self::settle_steer(
                        waiting,
                        SteerOutcome::Rejected(
                            "Claude steer response was not dispatched".to_owned(),
                        ),
                    );
                }
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, &consumer_id);
                return self.write_hook_pass(&native, request_id, deadline).await;
            }
            Err(_) => {
                if let Some(waiting) = steer.take() {
                    Self::settle_steer(
                        waiting,
                        SteerOutcome::Unknown(
                            "Claude steer response write outcome is unknown".to_owned(),
                        ),
                    );
                }
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(&tool_use_id, &consumer_id);
                return Err(Error::new(
                    "unknown",
                    "Claude hook response write outcome is unknown",
                ));
            }
        }
        let Some(pending) = self.pending_hooks.remove(request_id) else {
            if let Some(waiting) = steer.take() {
                Self::settle_steer(
                    waiting,
                    SteerOutcome::Unknown(
                        "Claude hook binding disappeared after the steer write".to_owned(),
                    ),
                );
            }
            return Err(Error::new(
                "unknown",
                "Claude hook binding disappeared after the native response write",
            ));
        };
        if context.is_empty() {
            if !self.hook_empty_selection_was_claimed(&pending) {
                if let Some(waiting) = steer.take() {
                    Self::settle_steer(
                        waiting,
                        SteerOutcome::Unknown(
                            "Claude steer lost its exact callback binding".to_owned(),
                        ),
                    );
                }
                self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
                return Err(Error::new(
                    "unknown",
                    "Claude empty hook selection lost its exact callback binding",
                ));
            }
            self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
            if let Some(waiting) = steer.take() {
                Self::settle_steer(waiting, SteerOutcome::Applied);
            }
            return self
                .commit_empty_hook_event(pending.turn_id, pending.consumer_id, &pending.tool)
                .await;
        }
        if !self.hook_context_was_claimed(&context, &pending) {
            if let Some(waiting) = steer.take() {
                Self::settle_steer(
                    waiting,
                    SteerOutcome::Unknown(
                        "Claude steer lost its exact callback binding".to_owned(),
                    ),
                );
            }
            self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
            return Err(Error::new(
                "unknown",
                "Claude hook context was written without an exact live binding",
            ));
        }
        self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
        if let Some(waiting) = steer.take() {
            Self::settle_steer(waiting, SteerOutcome::Applied);
        }
        let Some(session_id) = self.status.session_id.as_deref() else {
            return Err(Error::new(
                "unknown",
                "Claude session identity disappeared after the hook response write",
            ));
        };
        let outcome = ContextOutcome {
            mechanism: "hook_callback".to_owned(),
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
            "claude:{session_id}:{}:hook.before_tool:{}",
            pending.turn_id, pending.consumer_id
        );
        self.commit_event("hook.before_tool", native_id, data).await
    }

    /// The pass path for a hook that selected no context and carries no
    /// steer: write the empty response and commit the empty receipt. This is
    /// the original empty-context block, unchanged.
    async fn finish_hook_pass(
        &mut self,
        request_id: &str,
        native: &ClaudeClient,
        deadline: Instant,
        tool_use_id: &str,
        consumer_id: &str,
    ) -> Result<()> {
        if !self.hook_callback_is_live(request_id) {
            self.pending_hooks.remove(request_id);
            self.finish_tool_use_hook(tool_use_id, consumer_id);
            return self.write_hook_pass(native, request_id, deadline).await;
        }
        match native
            .respond_hook_callback(request_id, HookCallbackResponse::default(), deadline)
            .await
        {
            Ok(()) => {}
            Err(error) if native_no_write(&error.code) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(tool_use_id, consumer_id);
                return Ok(());
            }
            Err(_) => {
                self.pending_hooks.remove(request_id);
                self.finish_tool_use_hook(tool_use_id, consumer_id);
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
            self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
            return Err(Error::new(
                "unknown",
                "Claude empty hook selection lost its exact callback binding",
            ));
        }
        self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
        self.commit_empty_hook_event(pending.turn_id, pending.consumer_id, &pending.tool)
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
                        && binding.hook_state == HookConsumerState::InFlight
                        && binding.active_consumer_id.as_deref()
                            == Some(pending.consumer_id.as_str())
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
                        && binding.hook_state == HookConsumerState::InFlight
                        && binding.active_consumer_id.as_deref()
                            == Some(pending.consumer_id.as_str())
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
                        && binding.hook_state == HookConsumerState::InFlight
                        && binding.active_consumer_id.as_deref()
                            == Some(pending.consumer_id.as_str())
                })
            && !pending.consumer_id.trim().is_empty()
    }

    async fn commit_empty_hook_event(
        &mut self,
        turn_id: String,
        consumer_id: String,
        tool: &HookToolFacts,
    ) -> Result<()> {
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
        let native_id = format!("claude:{session_id}:{turn_id}:hook.before_tool:{consumer_id}");
        self.commit_event("hook.before_tool", native_id, data).await
    }

    fn finish_tool_use_hook(&mut self, tool_use_id: &str, consumer_id: &str) {
        if let Some(binding) = self.tool_use_turns.get_mut(tool_use_id)
            && binding.active_consumer_id.as_deref() == Some(consumer_id)
        {
            binding.hook_state = HookConsumerState::Finished;
            binding.active_consumer_id = None;
        }
    }

    fn finish_available_tool_use_hook(&mut self, tool_use_id: &str) {
        if let Some(binding) = self.tool_use_turns.get_mut(tool_use_id)
            && binding.hook_state == HookConsumerState::Available
        {
            binding.hook_state = HookConsumerState::Finished;
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
            self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
        }
    }

    fn remove_buffered_hook_callback(&mut self, request_id: &str) {
        let mut removed_bytes = 0_usize;
        self.buffer.retain(|frame| {
            let request = frame.record.get("request");
            let cancelled = frame.record.get("type").and_then(Value::as_str)
                == Some("control_request")
                && frame.record.get("request_id").and_then(Value::as_str) == Some(request_id)
                && request
                    .and_then(|request| request.get("subtype"))
                    .and_then(Value::as_str)
                    == Some("hook_callback");
            if cancelled {
                removed_bytes = removed_bytes.saturating_add(frame.size);
            }
            !cancelled
        });
        self.buffer_bytes = self.buffer_bytes.saturating_sub(removed_bytes);
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
                self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
            }
        }
    }

    fn cancel_all_hook_callbacks(&mut self) {
        let pending_hooks = std::mem::take(&mut self.pending_hooks);
        for pending in pending_hooks.into_values() {
            pending.task.abort();
            self.finish_tool_use_hook(&pending.tool_use_id, &pending.consumer_id);
        }
        self.cancelled_hook_requests.clear();
    }

    /// One CLI `control_request`: only the `can_use_tool` permission ask is
    /// connected. It binds to the exact active turn and opens a retained
    /// request under the CLI's exact native `request_id`; any other subtype
    /// is an explicit gap, as before.
    async fn observe_control_request(&mut self, record: &Value) -> Result<()> {
        if record
            .get("request")
            .and_then(|request| request.get("subtype"))
            .and_then(Value::as_str)
            == Some("hook_callback")
        {
            return self.observe_hook_callback(record).await;
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
        let omitted_output = self.autonomous_output;
        self.autonomous_output = false;
        self.task_notification_pending = false;
        self.status.background_continuation = false;
        if parsed
            .origin
            .as_ref()
            .is_some_and(|origin| origin.kind == "task-notification")
        {
            let answers_input = self.status.active_turn_id.as_deref().is_some_and(|turn| {
                parsed.user_message_uuid.as_deref() == Some(turn)
                    || parsed.user_message_uuids.iter().any(|uuid| uuid == turn)
            });
            // Background task wakeups are native autonomous work, not a SEND
            // association or a terminal for whichever Eliot turn is current.
            // Any pre-association message prefix belongs to this wakeup and
            // must not drain into a later input's lifecycle.
            if !answers_input {
                self.buffer.retain(|frame| {
                    !matches!(
                        frame.record.get("type").and_then(Value::as_str),
                        Some("assistant" | "user")
                    )
                });
                self.buffer_bytes = self.buffer.iter().map(|frame| frame.size).sum();
            }
            self.record_coverage_gap(
                "Claude autonomous task-notification output was not attributed to an Eliot input",
                &["message", "tool"],
            )
            .await?;
            if !answers_input {
                return Ok(());
            }
        } else if omitted_output {
            self.record_coverage_gap(
                "Claude output after a task notification had ambiguous input provenance and was omitted",
                &["message", "tool"],
            )
            .await?;
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
        self.cancel_hook_callbacks_for_turn(&turn_id);
        // A steer the turn ended before its next PreToolUse never applies:
        // drop it here so no text leaks into another turn.
        self.fail_pending_steer(SteerOutcome::Stale);
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
        // Optional native main-loop turn usage: absent/malformed usage never
        if outcome == "failure" {
            let error = self.native_failure.take();
            if error
                .as_ref()
                .is_some_and(|error| error.code == "quota_exhausted")
            {
                data["native_error"] =
                    json!({"code":"quota_exhausted", "message":"provider billing/quota refusal"});
                eliot_plugin::session::attach_provider_restriction(
                    &mut data,
                    eliot_core::ProviderRestriction {
                        kind: eliot_core::ProviderRestrictionKind::Quota,
                        retry_after: None,
                    },
                    &secrets,
                );
            }
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "claude",
                error
                    .as_ref()
                    .and_then(|error| error.origin)
                    .unwrap_or(eliot_core::FailureOrigin::Unknown),
                error
                    .as_ref()
                    .and_then(|error| error.origin_evidence.as_deref())
                    .unwrap_or("terminal reason lacks a structured assistant API error"),
                &secrets,
            );
        }
        // blocks or gaps this terminal; it simply leaves `data.usage` unset.
        // Attaches before `attach_final_text` so the existing bounded
        // terminal-text accounting includes the usage bytes.
        if let Some(usage) = result_usage(record) {
            data["usage"] = serde_json::to_value(&usage).unwrap_or(Value::Null);
        }
        // Proven MCP surface from the owned stream's `system`/`init`
        // record: attached once observed, so the first terminal after init
        // carries the readback into the effective set. Unobserved stays
        // unattached and the effective set stays unknown.
        attach_mcp_readback(&mut data, self.init_mcp_servers.as_deref());
        // Optional session-scoped fact from the terminal record (occupancy
        // sample from `usage`, cumulative `modelUsage` counters, cumulative
        // `total_cost_usd`, model window where a frame reports one), held
        // for the session fact that commits after the accepted terminal.
        // The window prefers the terminal frame and falls back to the
        // retained init value; the transcript file size comes from a
        // read-only scan for the exact session filename (never a guessed
        // slug). Absent quarters simply contribute nothing; with none there
        // is no session fact.
        let mut session_usage = result_session_usage(record);
        if let Some(usage) = session_usage.as_mut() {
            if usage.context_window_tokens.is_none() {
                usage.context_window_tokens = self.init_context_window;
            }
            let session_id = self.status.session_id.clone().unwrap_or_default();
            if !session_id.is_empty() {
                let roots = transcript_config_roots();
                usage.session_file_bytes =
                    tokio::task::spawn_blocking(move || transcript_bytes_in(&roots, &session_id))
                        .await
                        .ok()
                        .flatten();
            }
        }
        let final_text = self.final_text.take();
        if self.final_text_unavailable {
            attach_final_text_unavailable(&mut data, &secrets);
        } else if let Some(text) = final_text {
            // Use omit-on-overflow accounting for every native terminal. The
            // helper's flag does not change the observed terminal status.
            attach_final_text(&mut data, &secrets.redact_text(&text), true, &secrets);
            if data.get("final_text").is_none() {
                self.final_text_unavailable = true;
            }
        }
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = format!("claude:{session_id}:{turn_id}:turn.completed");
        // Publish the observed terminal BEFORE the commit resolves: the host
        // may snapshot as soon as it receives the batch, and a control
        // racing the terminal must see the finished turn (same order as the
        // association path, which binds active before its own commit). A
        // failed commit still latches source_unknown with this terminal.
        let session_turn_id = turn_id.clone();
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id,
            status: Some(terminal_reason),
            final_text: data
                .get("final_text")
                .and_then(Value::as_str)
                .map(str::to_owned),
            final_text_unavailable: data
                .get("final_text_unavailable")
                .and_then(Value::as_str)
                .map(str::to_owned),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.interrupted_turn = None;
        self.tool_use_turns.clear();
        self.cancelled_hook_requests.clear();
        self.clear_tool_tracking();
        self.tool_coverage_gap_reported = false;
        self.tool_use_tracking_fenced = false;
        self.publish_now();
        self.commit_event("turn.completed", native_id, data).await?;
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
        // No hook will ever confirm a queued steer after the drain: planned
        // stop already settled it at BeginStop; an unplanned loss settles it
        // rejected here (still queued proves never written), never replayed.
        if !self.planned_stop && !self.closed {
            self.fail_pending_steer(SteerOutcome::Rejected(
                "Claude event stream was lost before PreToolUse confirmed steer".to_owned(),
            ));
        }
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
        if let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            let mut text_status = json!({});
            if self.final_text_unavailable {
                attach_final_text_unavailable(
                    &mut text_status,
                    &SecretPolicy::new(self.secrets.iter().cloned()),
                );
            }
            self.status.last_turn = Some(TurnFacts {
                command_id,
                turn_id,
                status: None,
                final_text: None,
                final_text_unavailable: text_status
                    .get("final_text_unavailable")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                terminal: false,
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
        // BeginStop already settled this; the take is a no-op then.
        self.fail_pending_steer(SteerOutcome::Rejected(
            "Claude stopped before PreToolUse confirmed steer".to_owned(),
        ));
        self.status.stopped = true;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
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

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.stream_source().record_gap(reason).await
    }

    async fn record_coverage_gap(&mut self, reason: &str, families: &[&str]) -> Result<()> {
        let mut source = self.stream_source();
        source.families = families;
        source.record_coverage_gap(reason).await
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

/// SDKAssistantMessage.error enum, verified in the vendor stream-json/SDK
/// reference on 2026-11-16. Free text and generic api_error are not evidence.
fn claude_api_failure(kind: &str) -> Option<Error> {
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
        _ => return None,
    };
    Some(Error::new("native_failure", "native API failure").with_origin(origin, evidence))
}

#[cfg(test)]
#[test]
fn structured_claude_api_errors_classify_without_message_guessing() {
    for kind in [
        "rate_limit",
        "billing_error",
        "authentication_failed",
        "model_not_found",
        "server_error",
    ] {
        assert_eq!(
            claude_api_failure(kind).unwrap().origin,
            Some(eliot_core::FailureOrigin::Provider)
        );
    }
    assert_eq!(
        claude_api_failure("cloud_credential_error").unwrap().origin,
        Some(eliot_core::FailureOrigin::Environment)
    );
    assert!(claude_api_failure("api_error provider quota text").is_none());
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_plugin::Peer;

    #[tokio::test]
    async fn busy_stop_native_cancel_commits_exact_interrupted_turn() {
        for no_terminal in [false, true] {
            use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
            let (host_io, plugin_io) = tokio::io::duplex(64 * 1024);
            let (read, write) = tokio::io::split(plugin_io);
            let mut peer = eliot_plugin::Peer::from_io(read, write);
            let client = peer.client();
            let peer_task =
                tokio::spawn(async move { while peer.next_request().await.is_some() {} });
            let mut processor = ready_processor();
            processor.host = client.clone();
            processor.sink = eliot_plugin::CommitSink::new(client, "test-credential".to_owned());
            processor.status.active_command_id = Some("send-1".to_owned());
            processor.status.active_turn_id = Some("turn-1".to_owned());
            processor.interrupted_turn = Some("turn-1".to_owned());
            let cancelled = json!({"type":"control_cancel_request","request_id":"hook-1"});
            processor.cancel_hook_callback(&cancelled);
            assert!(processor.cancelled_hook_requests.contains("hook-1"));
            if no_terminal {
                processor.planned_stop = true;
                processor.note_frames_drained().await;
            }
            let frame = serde_json::json!({"type":"command_lifecycle","command_uuid":"turn-1","state":"cancelled"});
            let terminal = async {
                if no_terminal {
                    processor.finish_stop().await
                } else {
                    processor.observe_lifecycle(&frame).await
                }
            };
            let (read, mut write) = tokio::io::split(host_io);
            let (result, ()) = tokio::join!(terminal, async {
                let mut reader = BufReader::new(read);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let request: eliot_plugin::RpcRequest = serde_json::from_str(&line).unwrap();
                assert_eq!(request.method, "events.put");
                let event = &request.params["events"][0];
                assert_eq!(
                    event["kind"],
                    if no_terminal {
                        "session.ended"
                    } else {
                        "turn.completed"
                    }
                );
                if !no_terminal {
                    assert_eq!(event["data"]["turn_id"], "turn-1");
                    assert_eq!(event["data"]["command_id"], "send-1");
                    assert_eq!(event["data"]["outcome"], "interrupted");
                }
                let response = serde_json::json!({"jsonrpc":"2.0","id":request.id,"result":{"first_seq":1,"last_seq":1,"cursor":null}});
                write
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .unwrap();
            });
            result.unwrap();
            assert!(processor.status.active_turn_id.is_none());
            assert!(processor.interrupted_turn.is_none());
            assert!(processor.cancelled_hook_requests.is_empty());
            processor.cancel_hook_callback(&cancelled);
            assert!(processor.cancelled_hook_requests.is_empty());
            let turn = processor.status.last_turn.as_ref().unwrap();
            assert_eq!(turn.turn_id, "turn-1");
            assert_eq!(turn.terminal, !no_terminal);
            if no_terminal {
                assert!(turn.status.is_none());
            }
            assert!(!processor.status.source_unknown);
            peer_task.abort();
        }
    }

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
                workspace: PathBuf::from("/work/alpha"),
            },
            status_tx,
        );
        processor.status.ready = true;
        processor.status.session_id = Some("test-session".to_owned());
        processor
    }

    #[test]
    fn terminal_reasons_classify_without_defaulting_to_success() {
        assert_eq!(claude_outcome("success"), "success");
        assert_eq!(claude_outcome("completed"), "success");
        assert_eq!(claude_outcome("cancelled"), "interrupted");
        assert_eq!(claude_outcome("interrupted"), "interrupted");
        // Installed 2.1.325 busy-stop capture, 2026-11-17: native result
        // terminal_reason after an acknowledged interrupt control.
        assert_eq!(claude_outcome("aborted_streaming"), "interrupted");
        for reason in [
            "api_error",
            "api_error_rate_limited",
            "error",
            "failed",
            "failure",
        ] {
            assert_eq!(claude_outcome(reason), "failure", "reason={reason}");
        }
        for reason in ["", "end_turn", "inProgress", "bogus"] {
            assert_eq!(claude_outcome(reason), "unknown", "reason={reason}");
        }
    }

    #[test]
    fn terminal_outcome_uses_the_pre_redaction_reason() {
        // A configured secret inside the reason must not change its outcome:
        // a secret `error` redacts `api_error` into `api_[REDACTED]` (unknown
        // on its own), but the persisted outcome stays `failure` while only
        // sanitized text is stored.
        let secrets = SecretPolicy::new(["error"]);
        let (outcome, status) = terminal_outcome_and_status("api_error", &secrets, None);
        assert_eq!(outcome, "failure");
        assert_eq!(status, "api_[REDACTED]");
        assert_eq!(claude_outcome(&status), "unknown");
        let plain = SecretPolicy::new(Vec::<String>::new());
        let (outcome, status) = terminal_outcome_and_status("api_error", &plain, None);
        assert_eq!(outcome, "failure");
        assert_eq!(status, "api_error");
    }

    #[test]
    fn tool_result_status_uses_only_the_native_verdict() {
        assert_eq!(tool_result_status(Some(true)), "error");
        assert_eq!(tool_result_status(Some(false)), "completed");
        assert_eq!(tool_result_status(None), "completed");
    }

    #[test]
    fn tool_records_stay_on_the_main_thread() {
        assert!(is_main_thread_user(&json!({
            "type": "user",
            "message": {"content": [
                {"type": "tool_result", "tool_use_id": "toolu-1"},
            ]},
        })));
        for forwarded in [
            json!({"type": "user", "parent_tool_use_id": "toolu-0"}),
            json!({"type": "user", "message": {
                "content": [{"type": "tool_result", "tool_use_id": "toolu-1"}],
                "parent_tool_use_id": "toolu-0",
            }}),
        ] {
            assert!(!is_main_thread_user(&forwarded));
        }
    }

    #[tokio::test]
    async fn late_sidechain_records_do_not_touch_a_pending_followup() {
        let mut processor = ready_processor();
        let (outcome, _receiver) = oneshot::channel();
        processor
            .register_turn("next-command".into(), "next-turn".into(), outcome)
            .expect("followup registered");
        // Shape captured from installed 2.1.325: a sidechain can finish
        // after the parent's terminal and before a followup associates.
        for record in [
            json!({"type":"assistant", "parent_tool_use_id":"parent-tool", "agent_id":"child",
                "message":{"content":[{"type":"tool_use", "id":"child-read", "name":"Read"}]}}),
            json!({"type":"user", "parent_tool_use_id":"parent-tool", "agent_id":"child",
                "message":{"content":[{"type":"tool_result", "tool_use_id":"child-read"}]}}),
        ] {
            assert!(!processor.should_buffer(&record));
            processor
                .process_record(record)
                .await
                .expect("sidechain omitted");
        }
        assert!(processor.buffer.is_empty());
        assert!(processor.tool_use_turns.is_empty());
        assert!(processor.tool_names.is_empty());
        assert!(processor.status.failure.is_none());
        assert_eq!(
            processor.pending_turn.as_ref().unwrap().turn_id,
            "next-turn"
        );
    }

    #[tokio::test]
    async fn tool_binding_capacity_retains_existing_callback_identities() {
        let mut processor = ready_processor();
        for index in 0..=MAX_TOOL_USE_BINDINGS {
            processor.bind_tool_uses(
                &json!({"message":{"content":[{
                    "type":"tool_use", "id":format!("tool-{index}"), "name":"Read"
                }]}}),
                "turn-1",
            );
        }
        assert!(processor.tool_use_tracking_fenced);
        assert_eq!(processor.tool_use_turns.len(), MAX_TOOL_USE_BINDINGS);
        assert_eq!(processor.tool_use_turns["tool-0"].turn_id, "turn-1");
        assert!(!processor.tool_use_turns.contains_key("tool-128"));
        assert!(processor.status.failure.is_none());
    }

    #[tokio::test]
    async fn autonomous_wakeup_prefix_does_not_bind_to_a_queued_input() {
        let mut processor = ready_processor();
        processor.observe_task_boundary(&json!({"subtype":"task_notification"}));
        let (outcome, _receiver) = oneshot::channel();
        processor
            .register_turn("next-command".into(), "next-turn".into(), outcome)
            .expect("followup registered");
        processor.observe_task_boundary(&json!({"subtype":"init"}));
        let record = json!({"type":"assistant", "parent_tool_use_id":null,
            "message":{"content":[{"type":"text", "text":"autonomous reply"}]}});
        assert!(!processor.should_buffer(&record));
        processor
            .process_record(record)
            .await
            .expect("wakeup omitted");
        assert!(processor.autonomous_output);
        assert!(processor.status.background_continuation);
        assert!(processor.buffer.is_empty());
        assert!(processor.final_text.is_none());
        assert_eq!(
            processor.pending_turn.as_ref().unwrap().turn_id,
            "next-turn"
        );
    }

    #[test]
    fn structured_billing_refusal_overrides_a_normal_terminal_only() {
        let assistant = json!({"type":"assistant", "error":"billing_error",
            "parent_tool_use_id":null, "message":{"content":[{"type":"text","text":"quota refusal"}]}});
        let failure = claude_api_failure(assistant["error"].as_str().unwrap()).unwrap();
        let secrets = SecretPolicy::new(Vec::<String>::new());
        assert_eq!(
            terminal_outcome_and_status("completed", &secrets, Some(&failure)),
            ("failure", "completed".into())
        );
        assert_eq!(
            terminal_outcome_and_status("interrupted", &secrets, Some(&failure)).0,
            "interrupted"
        );
        assert_eq!(
            terminal_outcome_and_status("future_stop", &secrets, Some(&failure)).0,
            "unknown"
        );
        assert_eq!(
            terminal_outcome_and_status("completed", &secrets, None).0,
            "success"
        );
        assert_eq!(failure.origin, Some(eliot_core::FailureOrigin::Provider));
    }

    #[tokio::test]
    async fn a_recovered_main_assistant_clears_a_billing_refusal() {
        let mut processor = ready_processor();
        processor.status.active_command_id = Some("command-1".into());
        processor.status.active_turn_id = Some("turn-1".into());
        processor
            .observe_assistant(&json!({"type":"assistant", "error":"billing_error",
            "parent_tool_use_id":null, "message":{"content":[]}}))
            .await
            .unwrap();
        assert_eq!(
            processor.native_failure.as_ref().unwrap().code,
            "quota_exhausted"
        );
        processor
            .observe_assistant(&json!({"type":"assistant",
            "parent_tool_use_id":null, "message":{"content":[]}}))
            .await
            .unwrap();
        assert!(processor.native_failure.is_none());
        let secrets = SecretPolicy::new(Vec::<String>::new());
        assert_eq!(
            terminal_outcome_and_status("completed", &secrets, processor.native_failure.as_ref()).0,
            "success"
        );
    }

    #[test]
    fn result_terminal_completed_settles_success() {
        // Live 2.1.321 terminal frame (captured from `claude -p
        // --input-format stream-json --output-format stream-json --verbose`;
        // trimmed to the decision-relevant fields plus unknown
        // forward-compat fields the parser must tolerate). The proven
        // completion carries `terminal_reason: "completed"`.
        let record = json!({
            "type": "result",
            "subtype": "success",
            "terminal_reason": "completed",
            "stop_reason": "end_turn",
            "is_error": false,
            "num_turns": 1,
            "result": "hello",
            "session_id": "11111111-2222-4333-8444-555555555555",
            "total_cost_usd": 0.00260901,
            "usage": {
                "input_tokens": 2,
                "output_tokens": 4,
                "cache_read_input_tokens": 20781,
                "cache_creation_input_tokens": 11995,
            },
            "modelUsage": {},
            "permission_denials": [],
            "queued_turn_count": 0,
            "result_index": 0,
            "duration_ms": 1442,
            "duration_api_ms": 1370,
            "uuid": "6dd2511c-8daf-4c59-ada1-7069ad908df2",
        });
        let parsed: ResultRecord =
            serde_json::from_value(record.clone()).expect("2.1.321 result decodes");
        assert_eq!(parsed.terminal_reason, "completed");
        let plain = SecretPolicy::new(Vec::<String>::new());
        let (outcome, status) = terminal_outcome_and_status(&parsed.terminal_reason, &plain, None);
        assert_eq!(outcome, "success");
        assert_eq!(status, "completed");
        let usage = result_usage(&record).expect("turn counters map");
        assert_eq!(usage.input_tokens, Some(2));
        assert_eq!(usage.output_tokens, Some(4));
        assert_eq!(usage.cache_read_tokens, Some(20781));
        assert_eq!(usage.cache_write_tokens, Some(11995));
        assert_eq!(usage.scope.as_deref(), Some("main_loop_turn"));
        assert!(result_session_usage(&record).is_some());
        // Native 2.1.325 busy-stop terminal, captured 2026-11-17. A control
        // ack alone cannot establish this; the result carries the abort.
        let mut aborted = record;
        aborted["terminal_reason"] = json!("aborted_streaming");
        let parsed: ResultRecord = serde_json::from_value(aborted).unwrap();
        let (outcome, native_status) =
            terminal_outcome_and_status(&parsed.terminal_reason, &plain, None);
        assert_eq!(outcome, "interrupted");
        assert_eq!(native_status, "aborted_streaming");
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

    #[tokio::test]
    async fn definite_turn_started_rejection_stays_unknown_and_fenced() {
        // Post-dispatch persistence failure keeps the command Unknown with
        // its exact binding (never Rejected) and fences fresh input, even
        // for the four definite batch-rejection codes. Only `session.ready`
        // before any input may settle definite `failed`.
        for code in [
            "secret_in_payload",
            "invalid",
            "not_allowed",
            "stale_generation",
            "unknown",
        ] {
            let mut processor = ready_processor();
            let (tx, rx) = oneshot::channel();
            let error = eliot_core::Error::new(code, format!("{code} fixture"));
            processor.settle_turn_started_persistence(Some(tx), &error);
            match rx.await.expect("persistence failure settles the waiter") {
                TurnOutcome::Unknown(reason) => assert!(
                    reason.contains(code),
                    "reason keeps the typed cause: {reason}"
                ),
                TurnOutcome::Associated | TurnOutcome::Rejected(_) => {
                    panic!("turn.started persistence failure must stay Unknown, code={code}")
                }
            }
            assert!(
                processor.status.source_unknown,
                "post-dispatch failure fences snapshots, code={code}"
            );
            assert!(
                processor.status.failure.is_some(),
                "post-dispatch failure fences fresh input, code={code}"
            );
            assert!(
                !processor.status.batch_unknown,
                "definite batch stays known-uncommitted, code={code}"
            );
            assert!(
                !processor.writes_blocked,
                "commit latch stays clear for a definite rejection, code={code}"
            );
        }
    }

    #[tokio::test]
    async fn close_settles_pending_turn_as_unknown() {
        let mut processor = ready_processor();
        let (status_tx, _status_rx) = watch::channel(EventStatus::default());
        let (outcome_tx, outcome_rx) = oneshot::channel();
        processor
            .register_turn("cmd-1".to_owned(), "turn-1".to_owned(), outcome_tx)
            .expect("registration holds the waiter");
        let (reply_tx, reply_rx) = oneshot::channel();
        assert!(
            dispatch_control(
                &mut processor,
                &status_tx,
                Control::Close { reply: reply_tx }
            )
            .await
        );
        assert!(reply_rx.await.is_ok());
        assert!(
            processor.pending_turn.is_none(),
            "close retires the exact waiter"
        );
        match outcome_rx.await.expect("close settles the waiter") {
            TurnOutcome::Unknown(reason) => assert_eq!(
                reason,
                "Claude event task closed before the input association was observed"
            ),
            TurnOutcome::Associated | TurnOutcome::Rejected(_) => {
                panic!("close must not invent an association or refusal")
            }
        }
    }
}
