//! Native SSE facts to durable Eliot events.
//!
//! The event task owns the controlled `stream` source: it routes every SSE
//! envelope by exact session, commits one native record per batch
//! (`session.ready`, `turn.started`, `tool.started`, `tool.completed`,
//! `message.completed`, `turn.completed`, `session.ended`), records an
//! explicit gap on loss, and retains unfamiliar bound tags as neutral native
//! diagnostics. Tool lifecycle
//! facts map from the installed `session.tool.*` events; the remaining
//! streaming detail (`text`/`reasoning` deltas, tool input deltas and
//! progress, steps, usage, ephemeral status) is folded into the terminal
//! message readback. Its latest complete per-call context count is also
//! committed as the generation's latest `session.usage` fact.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, ProviderRestriction, ProviderRestrictionKind,
    Result, SecretPolicy, SessionUsage, TurnUsage,
    api::{AGENT_NOTICE_MESSAGE_CHARS, NativeNotice, NativeNoticeKind},
    limits::{
        MAX_NATIVE_INBOUND_FRAME_BYTES, MAX_TOOL_COMMAND_BYTES, NATIVE_TERMINAL_DEADLINE_REASON,
    },
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    event_actor::EventActor,
    native_data::{UNCLASSIFIED_NATIVE_EVENT, unclassified},
    native_stdio::StreamState,
    protocol::{HookEvent, HookResponse},
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text, attach_native_error, event_id,
        is_blank_native_text, native_terminal_inactivity, next_input_outcomes,
        redact_request_details_for_commit, with_claimed_context,
    },
};

use crate::context_hooks::{self, ContextHookSignal};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    client::{HttpOutcome, OpenCodeHttp},
    requests::{
        CorePending, ReplyRoute, StagedReply, describe_form, describe_permission,
        public_request_view, reply_for,
    },
    sse::SseFrame,
    types::{
        AgentSelectedData, AssistantContent, AssistantTokens, EliotContextHook,
        EliotContextResolveInput, ExecutionEventData, FormCancelledData, FormCreatedData,
        FormRepliedData, InboxEventData, Message, ModelRef, ModelSelectedData, PermissionAskedData,
        PermissionRepliedData, RetryScheduledData, SessionLifecycleData, SseEnvelope,
        StepStartedData, StructuredError, ToolEventData, ToolFailedData, ToolInputStartedData,
    },
};

const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_PAGES: usize = 4;
const MAX_PENDING_REQUESTS: usize = 128;
const MAX_RESOLVED_REQUESTS: usize = 128;
const MAX_STEER_WATCHES: usize = 128;
const MAX_TOOL_PARTS: usize = 128;
const MAX_TOOL_FAILURE_CONTENT_BLOCKS: usize = 32;
const MAX_TOOL_FAILURE_OUTPUT_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy)]
enum NativeSessionStatus<'a> {
    Idle,
    Retry,
    Busy,
    Other(&'a str),
}

impl<'a> NativeSessionStatus<'a> {
    fn from_tag(tag: &'a str) -> Self {
        match tag {
            "idle" => Self::Idle,
            "retry" => Self::Retry,
            "busy" => Self::Busy,
            other => Self::Other(other),
        }
    }
}

#[derive(Clone, Copy)]
enum NativeCompactionReason<'a> {
    Auto,
    Manual,
    Other(&'a str),
    Missing,
}

impl NativeCompactionReason<'_> {
    fn projection(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Manual => "manual",
            Self::Other(_) | Self::Missing => "unknown",
        }
    }
}

fn is_known_streaming_detail(event_type: &str) -> bool {
    matches!(
        event_type,
        "session.compaction.delta"
            | "session.idle"
            | "session.reasoning.delta"
            | "session.reasoning.ended"
            | "session.reasoning.started"
            | "session.step.ended"
            | "session.step.failed"
            | "session.step.streamed"
            | "session.text.delta"
            | "session.text.ended"
            | "session.text.started"
            | "session.tool.input.delta"
            | "session.tool.input.ended"
            | "session.tool.progress"
            | "session.usage.recorded"
            | "session.usage.updated"
    )
}

fn native_tool_failure_output(data: &Value) -> (Option<String>, bool) {
    let mut truncated = data
        .get("metadata")
        .and_then(|metadata| metadata.get("truncated"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let Some(blocks) = data.get("content").and_then(Value::as_array) else {
        return (None, true);
    };
    if blocks.len() > MAX_TOOL_FAILURE_CONTENT_BLOCKS {
        truncated = true;
    }
    let mut output = String::new();
    for block in blocks.iter().take(MAX_TOOL_FAILURE_CONTENT_BLOCKS) {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            truncated = true;
            continue;
        }
        let Some(text) = block.get("text").and_then(Value::as_str) else {
            truncated = true;
            continue;
        };
        let remaining = MAX_TOOL_FAILURE_OUTPUT_BYTES.saturating_sub(output.len());
        if text.len() > remaining {
            let mut taken = 0usize;
            for character in text.chars() {
                if taken.saturating_add(character.len_utf8()) > remaining {
                    break;
                }
                output.push(character);
                taken += character.len_utf8();
            }
            truncated = true;
            break;
        }
        output.push_str(text);
    }
    ((!output.is_empty()).then_some(output), truncated)
}
const REPLY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// Per-part tool tracking for the exact active turn: the native name
/// from `session.tool.input.started` (the only installed tool event
/// carrying it) plus the committed start/terminal flags. Bounded to the
/// first [`MAX_TOOL_PARTS`] parts per turn without eviction, so an
/// in-flight part never loses its flags; facts for later parts drop with
/// one tool-family coverage gap per turn. Cleared when the turn fences.
#[derive(Default)]
struct ToolPart {
    name: Option<String>,
    write_path: Option<String>,
    started: bool,
    completed: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub native_drained: bool,
    pub ready: bool,
    pub connected: bool,
    pub session_id: Option<String>,
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub last_turn: Option<TurnFacts>,
    pub turn_input: Option<TurnInput>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    pub compaction_in_flight: bool,
    pub stopped: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct TurnInput {
    pub command_id: String,
    pub turn_id: String,
    pub inbox_id: String,
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
    actor: EventActor<Control, EventStatus>,
}

enum Control {
    Ready {
        native_scope: String,
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    ResumeReady {
        native_scope: String,
        session_id: String,
        effective: Value,
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
    AssociateInbox {
        command_id: String,
        inbox_id: String,
        context: Vec<ContextItem>,
        reply: oneshot::Sender<Result<()>>,
    },
    AbandonAssociation {
        command_id: String,
        reply: oneshot::Sender<()>,
    },
    WatchSteer {
        command_id: String,
        turn_id: String,
        inbox_id: String,
        reply: oneshot::Sender<Result<oneshot::Receiver<SteerVerdict>>>,
    },
    AbandonSteer {
        command_id: String,
        reply: oneshot::Sender<()>,
    },
    CheckInbox {
        inbox_id: String,
        reply: oneshot::Sender<InboxRecord>,
    },
    BeginCompact {
        command_id: String,
        terminal: oneshot::Sender<(String, bool)>,
        reply: oneshot::Sender<Result<()>>,
    },
    RefuseCompact {
        reply: oneshot::Sender<Result<()>>,
    },
    BeginStop {
        reply: oneshot::Sender<Result<()>>,
    },
    EndStop {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Close the native SSE source after draining queued frames, then publish
    /// the planned session end. The owned OpenCode server remains alive so
    /// the daemon Job, rather than this plugin, owns its eventual custody.
    DrainStop {
        reply: oneshot::Sender<Result<()>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
    SnapshotStatus {
        reply: oneshot::Sender<EventStatus>,
    },
}

/// Messages flowing from the context poll task and claim/resolve workers
/// into the single event loop. Only this loop commits durable facts, so
/// hook receipts share the `StreamSource` counter with every other event.
pub(crate) enum ContextHookMsg {
    Arrived(ContextHookSignal),
    Done(ContextHookDone),
}

/// The vendor resolve verdict for one hook. Only `Applied`/`Empty` confirm
/// a native effect and produce a durable receipt; everything else leaves
/// claimed items frozen (never requeued, never applied on a later turn).
enum HookResolveOutcome {
    Applied,
    Empty,
    NoReceipt,
}

pub(crate) struct ContextHookDone {
    hook_id: String,
    turn_id: String,
    consumer_id: String,
    claimed: Vec<ContextItem>,
    outcome: HookResolveOutcome,
}

/// One offered context hook waiting for its exact inbox-to-turn binding.
/// The vendor hook expires on its own deadline; an unbound hook is
/// cancelled, never claimed and never applied.
struct PendingContextHook {
    hook: EliotContextHook,
    deadline: Instant,
}

#[derive(Clone)]
struct BufferedEnvelope {
    envelope: SseEnvelope,
    size: usize,
}

struct PendingInbox {
    command_id: String,
    context: Vec<ContextItem>,
    inbox_id: String,
}

struct CompactWatch {
    command_id: String,
    input: Option<String>,
    result: Option<bool>,
    terminal: oneshot::Sender<(String, bool)>,
}

struct CompactionStart {
    seq: u64,
    reason: &'static str,
    activity: Option<eliot_core::CompactionActivity>,
}

/// Verdict for one steered input watched against its exact turn.
/// Delivered-while-active proves native joined the input to that turn;
/// anything else leaves consumption unproven.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SteerVerdict {
    Applied,
    Missed,
}

struct SteerWatch {
    turn_id: String,
    inbox_id: String,
    reply: Option<oneshot::Sender<SteerVerdict>>,
}

/// Retained stream record for one inbox id: whether native delivered it
/// into an execution or dropped it before delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InboxRecord {
    pub delivered: bool,
    pub cancelled: bool,
}

/// One retained open native request: the exact offered snapshot the owner
/// decides from, plus the reply lane state. The public request id IS the
/// native `per_...`/`frm_...` id, so no index map is needed.
struct PendingNativeRequest {
    request: CorePending,
    reply_command_id: Option<String>,
    reply_state: NativeReplyState,
    /// True when the staged write is a native cancel (form deny): the
    /// settle commits `request.closed` without a command binding, never
    /// `request.resolved` — a cancel is not an answer.
    reply_is_cancel: bool,
}

/// Reply lane: at most one native write per request, ever.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Written,
    Unknown,
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
    close_frames: bool,
    writes_blocked: bool,
    gap_reported: bool,
    terminal_deadline: Option<Instant>,
    terminal_inactivity: std::time::Duration,
    pending: Option<PendingInbox>,
    compaction: Option<CompactWatch>,
    compaction_start: Option<CompactionStart>,
    last_compaction_terminal: Option<String>,
    /// Inbox id of the currently open turn: the prompt response id, which
    /// is the user message id the terminal readback bounds against.
    active_model: Option<ModelRef>,
    /// Per-part tool tracking for the exact active turn (name plus
    /// committed start/terminal flags), bounded to [`MAX_TOOL_PARTS`].
    tool_parts: HashMap<String, ToolPart>,
    /// One tool-family coverage gap per turn when the tracking bound is
    /// full; later part facts drop instead of growing without bound.
    tool_parts_overflowed: bool,
    /// Inbox ids observed as enqueued or delivered, by id. Bounded by the
    /// per-generation command history the host keeps.
    seen_inbox: HashSet<String>,
    /// Inbox ids observed as delivered: the steer-consumption proof is a
    /// delivery observed while the exact turn is still active. Cleared
    /// with `seen_inbox` at the shared bound.
    seen_delivered: HashSet<String>,
    /// Inbox ids observed as cancelled before delivery: with absence
    /// from the inbox-list readback, the removal proof that lets a
    /// missed steer settle without latching the session.
    seen_cancelled: HashSet<String>,
    /// Exact turn each delivered input joined, retained across terminal
    /// handling: a steer watch registering after its turn's terminal
    /// still verdicts from this binding instead of forgetting it.
    delivered_turn: HashMap<String, String>,
    /// Steer watches by command id. Lane-less: unlike the send `pending`
    /// lane, watches never open turns, never block each other, and never
    /// touch the next-input association.
    steer_watches: HashMap<String, SteerWatch>,
    /// `session.execution.started` event id waiting for its Eliot command.
    /// At most one: a second unclaimed start is unmodelled native behavior.
    stashed_started: Option<String>,
    /// Next expected durable sequence of our session aggregate. `Some(0)`
    /// from launch readiness: our aggregate starts at `session.created`
    /// seq 0 and every durable event reaches every subscriber in order.
    /// `None` from resume readiness: the live edge is adopted from the
    /// first observed durable event (the unobserved prefix is history, not
    /// a gap).
    expected_seq: Option<u64>,
    /// Open native requests by public (= native) id.
    pending_requests: HashMap<String, PendingNativeRequest>,
    /// Terminally resolved native ids (bounded): a re-issued ask after
    /// resolution gaps, it never reopens.
    resolved_native_ids: HashSet<String>,
    resolved_native_order: VecDeque<String>,
    /// True once a resumed session finished readiness: pre-send terminals
    /// are the dead generation's close and drop instead of gapping.
    resumed: bool,
    /// True once a send associated in this generation.
    saw_send: bool,
    /// Context-hook channel into this loop: the poll task forwards offers
    /// and claim/resolve workers report verdicts. The processor keeps one
    /// sender alive so the receiver never closes mid-generation.
    context_tx: mpsc::Sender<ContextHookMsg>,
    context_rx: Option<mpsc::Receiver<ContextHookMsg>>,
    /// Owned-generation context poll task; aborted at stop/drop.
    context_poll_abort: Option<tokio::task::AbortHandle>,
    /// The one offered hook awaiting its exact binding (at most one).
    pending_context_hook: Option<PendingContextHook>,
    /// Hooks with a claim/resolve worker in flight, by hook id.
    context_hook_workers: HashMap<String, tokio::task::AbortHandle>,
    /// Hooks whose receipt already settled (bounded): a duplicate offer
    /// never re-claims or re-resolves.
    settled_hook_ids: HashSet<String>,
    settled_hook_order: VecDeque<String>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<SseFrame>,
    stream: watch::Receiver<StreamState>,
    http: OpenCodeHttp,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let processor = EventProcessor::new(http, host, config);
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
    pub async fn begin_compaction(
        &self,
        command_id: &str,
    ) -> Result<oneshot::Receiver<(String, bool)>> {
        let (terminal, result) = oneshot::channel();
        self.actor
            .call(|reply| Control::BeginCompact {
                command_id: command_id.to_owned(),
                terminal,
                reply,
            })
            .await??;
        Ok(result)
    }

    pub async fn refuse_compaction(&self) -> Result<()> {
        self.actor
            .call(|reply| Control::RefuseCompact { reply })
            .await?
    }

    pub async fn ready(
        &self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        self.actor
            .call(|reply| Control::Ready {
                native_scope,
                session_id,
                effective,
                reply,
            })
            .await?
    }

    pub async fn associate_inbox(
        &self,
        command_id: String,
        inbox_id: String,
        context: Vec<ContextItem>,
    ) -> Result<()> {
        self.actor
            .call(|reply| Control::AssociateInbox {
                command_id,
                inbox_id,
                context,
                reply,
            })
            .await?
    }

    pub async fn resume_ready(
        &self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        self.actor
            .call(|reply| Control::ResumeReady {
                native_scope,
                session_id,
                effective,
                reply,
            })
            .await?
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
            .call(|reply| Control::ReplyRequest {
                command_id,
                request_id,
                turn_id,
                verb,
                args,
                reply,
            })
            .await?
    }

    /// Authoritative exact-turn gate for interrupt: the check runs on the
    /// event task, serialized behind the terminal commit, so a control that
    /// arrives after the terminal batch always sees the finished turn even
    /// though the watch snapshot still lags it.
    pub async fn check_turn(&self, turn_id: &str) -> Result<bool> {
        self.actor
            .call(|reply| Control::CheckTurn {
                turn_id: turn_id.to_owned(),
                reply,
            })
            .await
    }

    pub async fn abandon_association(&self, command_id: String) {
        let _ = self
            .actor
            .call(|reply| Control::AbandonAssociation { command_id, reply })
            .await;
    }

    /// Watch one admitted steer input against its exact turn. The returned
    /// receiver resolves `Applied` when the input is delivered while the
    /// exact turn is still active, or `Missed` when the turn ends (or the
    /// input is cancelled) first. A dropped receiver is unknown on the
    /// host side; the host abandons the watch so it cannot linger.
    pub async fn watch_steer(
        &self,
        command_id: String,
        turn_id: String,
        inbox_id: String,
    ) -> Result<oneshot::Receiver<SteerVerdict>> {
        self.actor
            .call(|reply| Control::WatchSteer {
                command_id,
                turn_id,
                inbox_id,
                reply,
            })
            .await?
    }

    pub async fn abandon_steer(&self, command_id: String) {
        let _ = self
            .actor
            .call(|reply| Control::AbandonSteer { command_id, reply })
            .await;
    }

    /// The event task's retained stream record for one inbox id,
    /// serialized behind terminal and delivery handling like the
    /// exact-turn gate.
    pub async fn check_inbox(&self, inbox_id: &str) -> Result<InboxRecord> {
        self.actor
            .call(|reply| Control::CheckInbox {
                inbox_id: inbox_id.to_owned(),
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

    /// Drain the queued native SSE frames, publish the exact session end and
    /// close the stream receiver without terminating the owned server.
    pub async fn drain_stop(&self) -> Result<()> {
        self.send(|reply| Control::DrainStop { reply }).await
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

    /// Wait until `ready` for the association of `command_id`: its turn
    /// opened (applied), the source failed/stopped (unknown), or the
    /// deadline elapsed (unknown; the admitted input is never resent). A
    /// turn that already completed (its terminal facts retained) counts as
    /// opened: when the terminal flushes synchronously inside the
    /// `AssociateInbox` control, open and close land in one event-loop
    /// iteration and only the closed state is ever published, so a watcher
    /// that starts after it must not wait for an active window that will
    /// never come again.
    pub async fn await_association(&self, command_id: &str, deadline: Instant) -> Result<String> {
        // One watcher for the whole wait: cloning per iteration could miss
        // a status version that lands between the check and the clone.
        let mut watcher = self.actor.watch().clone();
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
                if let Some(turn) = status.last_turn.as_ref()
                    && turn.command_id == command_id
                    && turn.terminal
                {
                    return Ok(turn.turn_id.clone());
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

    /// The event task's CURRENT status, read synchronously through the
    /// control queue: a snapshot racing the close publish must observe
    /// post-commit state, not the last published watch value (which can
    /// still show the active turn after the caller already observed
    /// turn.completed).
    pub async fn current_status(&self) -> Result<EventStatus> {
        self.actor
            .call(|reply| Control::SnapshotStatus { reply })
            .await
    }

    async fn send(&self, build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control) -> Result<()> {
        self.actor.call(build).await?
    }
}

impl EventProcessor {
    fn new(http: OpenCodeHttp, host: PeerClient, config: EventConfig) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        let (context_tx, context_rx) = mpsc::channel(16);
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
            close_frames: false,
            writes_blocked: false,
            gap_reported: false,
            terminal_deadline: None,
            terminal_inactivity: native_terminal_inactivity(),
            pending: None,
            compaction: None,
            compaction_start: None,
            last_compaction_terminal: None,
            active_model: None,
            tool_parts: HashMap::new(),
            tool_parts_overflowed: false,
            seen_inbox: HashSet::new(),
            seen_delivered: HashSet::new(),
            seen_cancelled: HashSet::new(),
            delivered_turn: HashMap::new(),
            steer_watches: HashMap::new(),
            stashed_started: None,
            expected_seq: None,
            pending_requests: HashMap::new(),
            resolved_native_ids: HashSet::new(),
            resolved_native_order: VecDeque::new(),
            resumed: false,
            saw_send: false,
            context_tx,
            context_rx: Some(context_rx),
            context_poll_abort: None,
            pending_context_hook: None,
            context_hook_workers: HashMap::new(),
            settled_hook_ids: HashSet::new(),
            settled_hook_order: VecDeque::new(),
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
            Control::ResumeReady {
                native_scope,
                session_id,
                effective,
                reply,
            } => {
                let result = self.resume_ready(native_scope, session_id, effective).await;
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
            Control::CheckTurn { turn_id, reply } => {
                let _ = reply.send(self.status.active_turn_id.as_deref() == Some(turn_id.as_str()));
                true
            }
            Control::AssociateInbox {
                command_id,
                inbox_id,
                context,
                reply,
            } => {
                let result = self.associate_inbox(command_id, inbox_id, context).await;
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
            Control::WatchSteer {
                command_id,
                turn_id,
                inbox_id,
                reply,
            } => {
                let result = self.watch_steer(command_id, turn_id, inbox_id);
                let _ = reply.send(result);
                true
            }
            Control::AbandonSteer { command_id, reply } => {
                self.steer_watches.remove(&command_id);
                let _ = reply.send(());
                true
            }
            Control::CheckInbox { inbox_id, reply } => {
                let _ = reply.send(InboxRecord {
                    delivered: self.seen_delivered.contains(&inbox_id),
                    cancelled: self.seen_cancelled.contains(&inbox_id),
                });
                true
            }
            Control::BeginCompact {
                command_id,
                terminal,
                reply,
            } => {
                let result = if self.status.active_turn_id.is_some()
                    || self.pending.is_some()
                    || self.stashed_started.is_some()
                    || self.compaction.is_some()
                    || !self.status.ready
                    || !self.status.connected
                    || self.status.source_unknown
                    || self.status.batch_unknown
                    || self.writes_blocked
                {
                    Err(Error::new(
                        "native_not_dispatched",
                        "OpenCode compaction requires a proven idle session",
                    ))
                } else {
                    self.status.compaction_in_flight = true;
                    self.compaction = Some(CompactWatch {
                        command_id,
                        input: None,
                        result: None,
                        terminal,
                    });
                    Ok(())
                };
                let _ = reply.send(result);
                true
            }
            Control::RefuseCompact { reply } => {
                self.compaction = None;
                self.status.compaction_in_flight = false;
                let _ = reply.send(Ok(()));
                true
            }
            Control::BeginStop { reply } => {
                self.planned_stop = true;
                self.abort_context_hooks();
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
            Control::DrainStop { reply } => {
                self.planned_stop = true;
                self.abort_context_hooks();
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
                    self.close_frames = true;
                }
                true
            }
            Control::Close { reply } => {
                self.abort_context_hooks();
                if let Some(pending) = self.pending_end_stop.take() {
                    let _ = pending.send(Err(Error::new(
                        "unknown",
                        "OpenCode native event queue closed before stop settlement",
                    )));
                }
                let _ = reply.send(());
                false
            }
            Control::SnapshotStatus { reply } => {
                let _ = reply.send(self.status.clone());
                true
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
            StreamState::Malformed(_) => {
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
                        if !self.gap_reported
                            && !self.writes_blocked
                            && error.failure_where.as_deref() != Some("plugin.events.put")
                        {
                            let _ = self
                                .record_gap("OpenCode native event could not be normalized")
                                .await;
                        }
                        if self.status.failure.is_none() {
                            self.status.failure = Some(*error.message);
                        }
                    } else if let Err(error) = self.flush_buffer().await
                        && self.status.failure.is_none()
                    {
                        self.status.failure = Some(*error.message);
                        self.status.source_unknown = true;
                    }
                }
            }
            Some(SseFrame::Malformed) => {
                // Unreachable: the pump converts this marker into the
                // stream state and ends instead of forwarding it.
            }
            Some(SseFrame::Oversized { size, method }) => {
                let method = method.as_deref().unwrap_or("unknown");
                let reason = format!(
                    "OpenCode SSE notification oversized (frame_oversized len={size} limit={MAX_NATIVE_INBOUND_FRAME_BYTES} kind=notification method={method})"
                );
                let result = self
                    .stream_source()
                    .record_coverage_gap(
                        &reason,
                        &["session", "turn", "message", "tool", "request"],
                    )
                    .await;
                if let Err(error) = result {
                    self.status.source_unknown = true;
                    if self.status.failure.is_none() {
                        self.status.failure = Some(*error.message);
                    }
                } else {
                    // The dropped notification may itself carry a durable
                    // position. Adopt the next retained position after the
                    // recorded hole, then resume contiguous checks.
                    self.expected_seq = None;
                }
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
        // Idle compaction has no pending send. Its execution terminal must
        // reach the watcher while the native start is still stashed.
        if self.compaction.is_some() {
            return false;
        }
        if matches!(
            event_type,
            "session.execution.succeeded"
                | "session.execution.failed"
                | "session.execution.interrupted"
                | "session.retry.scheduled"
                | "session.step.started"
                | "session.tool.input.started"
                | "session.tool.called"
                | "session.tool.success"
                | "session.tool.failed"
        ) {
            // A step, tool fact or terminal buffers only while an association
            // may still open its turn: our send is in flight, or a start awaits its
            // association. A turn-less terminal with neither coming is
            // classifiable now — the dead generation's close (dropped) or a
            // transcript gap — and must never wait to be misbound to a
            // later turn.
            return self.status.active_turn_id.is_none()
                && (self.pending.is_some() || self.stashed_started.is_some());
        }
        if event_type == "session.inbox.cancelled" {
            // A cancel buffers only while the send lane may still open
            // its turn; a turn-less cancel with no send in flight is
            // classifiable now (a steer-cancel confirmation or a foreign
            // drop) and must never wait in the buffer.
            return self.status.active_turn_id.is_none() && self.pending.is_some();
        }
        // An ask racing our send association buffers until the turn opens:
        // with a send in flight, or a started execution awaiting its
        // association (frames beat the AssociateInbox control), the ask
        // belongs to our execution. With neither it is foreign and drops
        // at processing instead of buffering.
        if matches!(event_type, "permission.asked" | "form.created") {
            return self.pending.is_some() || self.stashed_started.is_some();
        }
        false
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
        self.commit_ready(native_scope, session_id, effective, Some(0))
            .await
    }

    /// Resume readiness: like launch readiness, except the durable live
    /// edge is adopted (not reset to 0) and any inbox, execution-start or
    /// request fact buffered during the host preflight refuses the resume:
    /// native activity while no send is in flight means the retained
    /// session was not idle. Frames win over this control, so activity
    /// racing the readback is always buffered first. Buffered terminals
    /// still flush: the dead generation's close drops instead of gapping.
    async fn resume_ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        if self.loss_reason.is_some() {
            return Err(Error::new(
                "unknown",
                "OpenCode retained session event stream lost native frames during resume",
            ));
        }
        if self.stashed_started.is_some()
            || !self.seen_inbox.is_empty()
            || self.preflight_activity()
        {
            return Err(Error::new(
                "unknown",
                "OpenCode retained session showed native activity during resume",
            ));
        }
        self.resumed = true;
        self.commit_ready(native_scope, session_id, effective, None)
            .await
    }

    /// Pre-readiness envelopes proving the retained session is not idle:
    /// inbox, execution-start, tool-lifecycle and request facts. Terminals,
    /// lifecycle and status/detail events are history, not activity.
    fn preflight_activity(&self) -> bool {
        self.buffer.iter().any(|frame| {
            matches!(
                frame.envelope.event_type.as_str(),
                "session.inbox.enqueued"
                    | "session.inbox.delivered"
                    | "session.inbox.cancelled"
                    | "session.execution.started"
                    | "session.tool.input.started"
                    | "session.tool.called"
                    | "session.tool.success"
                    | "session.tool.failed"
                    | "permission.asked"
                    | "permission.replied"
                    | "form.created"
                    | "form.replied"
                    | "form.cancelled"
            )
        })
    }

    async fn commit_ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
        expected_seq: Option<u64>,
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
        self.expected_seq = expected_seq;
        let advertises_turn_safe = effective
            .get("context_points")
            .and_then(|points| serde_json::from_value::<Vec<InjectPoint>>(points.clone()).ok())
            .is_some_and(|points| points.contains(&InjectPoint::TurnSafePoint));
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
        if advertises_turn_safe {
            self.start_context_poll();
        }
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

    async fn associate_inbox(
        &mut self,
        command_id: String,
        inbox_id: String,
        context: Vec<ContextItem>,
    ) -> Result<()> {
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
            context,
            inbox_id,
        });
        self.saw_send = true;
        self.try_open_turn().await?;
        self.flush_buffer().await
    }

    /// Register a steer watch, deciding immediately from retained state
    /// when the race already settled: frames win over this control, so a
    /// delivery or terminal that landed between the host gate and this
    /// registration must verdict now, never hang until the host deadline.
    fn watch_steer(
        &mut self,
        command_id: String,
        turn_id: String,
        inbox_id: String,
    ) -> Result<oneshot::Receiver<SteerVerdict>> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
            || inbox_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "OpenCode steer watch requires a ready exact turn and inbox",
            ));
        }
        if self.steer_watches.len() >= MAX_STEER_WATCHES {
            return Err(Error::new(
                "busy",
                "OpenCode already watches the maximum steered inputs",
            ));
        }
        let (reply, verdict) = oneshot::channel();
        // Retained proof first: the input was delivered while this exact
        // turn was active, even if the terminal has since fenced it.
        if self
            .delivered_turn
            .get(&inbox_id)
            .is_some_and(|turn| turn == &turn_id)
        {
            let _ = reply.send(SteerVerdict::Applied);
            return Ok(verdict);
        }
        if self.status.active_turn_id.as_deref() != Some(turn_id.as_str()) {
            let _ = reply.send(SteerVerdict::Missed);
            return Ok(verdict);
        }
        self.steer_watches.insert(
            command_id,
            SteerWatch {
                turn_id,
                inbox_id,
                reply: Some(reply),
            },
        );
        Ok(verdict)
    }

    /// Resolve one steer watch exactly once; a dropped host receiver is
    /// silent (the host already settled unknown).
    fn resolve_steer(&mut self, command_id: &str, verdict: SteerVerdict) {
        if let Some(mut watch) = self.steer_watches.remove(command_id)
            && let Some(reply) = watch.reply.take()
        {
            let _ = reply.send(verdict);
        }
    }

    /// Settle every watch on one delivered inbox: applied when the exact
    /// turn is still active, missed otherwise (the input joined later
    /// work, not the named turn).
    fn settle_delivered_steers(&mut self, inbox_id: &str) {
        let resolved: Vec<(String, SteerVerdict)> = self
            .steer_watches
            .iter()
            .filter(|(_, watch)| watch.inbox_id == inbox_id)
            .map(|(command_id, watch)| {
                let verdict =
                    if self.status.active_turn_id.as_deref() == Some(watch.turn_id.as_str()) {
                        SteerVerdict::Applied
                    } else {
                        SteerVerdict::Missed
                    };
                (command_id.clone(), verdict)
            })
            .collect();
        for (command_id, verdict) in resolved {
            self.resolve_steer(&command_id, verdict);
        }
    }

    /// A terminal for one turn misses every watch still naming it: the
    /// input was not delivered into that turn.
    fn miss_ended_steers(&mut self, turn_id: &str) {
        let ended: Vec<String> = self
            .steer_watches
            .iter()
            .filter(|(_, watch)| watch.turn_id == turn_id)
            .map(|(command_id, _)| command_id.clone())
            .collect();
        for command_id in ended {
            self.resolve_steer(&command_id, SteerVerdict::Missed);
        }
    }

    /// A cancelled inbox misses every watch naming it: native dropped the
    /// input before any execution could consume it.
    fn miss_cancelled_steers(&mut self, inbox_id: &str) {
        let cancelled: Vec<String> = self
            .steer_watches
            .iter()
            .filter(|(_, watch)| watch.inbox_id == inbox_id)
            .map(|(command_id, _)| command_id.clone())
            .collect();
        for command_id in cancelled {
            self.resolve_steer(&command_id, SteerVerdict::Missed);
        }
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
        let context = pending.context.clone();
        let inbox_id = pending.inbox_id.clone();
        self.status.turn_input = Some(TurnInput {
            command_id: command_id.clone(),
            turn_id: turn_id.clone(),
            inbox_id: inbox_id.clone(),
        });
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        // A delivery observed before the turn opened still bound the exact
        // input to this turn: retain it so context hooks can prove the
        // binding through actual delivery, never copy a message id blindly.
        if self.seen_delivered.contains(&inbox_id) {
            self.delivered_turn.insert(inbox_id, turn_id.clone());
        }
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
        let mut data = json!({"command_id": command_id, "turn_id": turn_id});
        if !context.is_empty() {
            data["context_outcomes"] = json!(next_input_outcomes(&context, &command_id));
        }
        self.commit_event("turn.started", native_id, data).await?;
        self.terminal_deadline = Some(Instant::now() + self.terminal_inactivity);
        Ok(())
    }

    async fn expire_terminal_deadline(&mut self) -> Result<()> {
        self.terminal_deadline = None;
        let Some(input) =
            self.status.turn_input.clone().filter(|input| {
                self.status.active_turn_id.as_deref() == Some(input.turn_id.as_str())
            })
        else {
            return Ok(());
        };
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &input.turn_id,
            &input.turn_id,
            "turn.unknown",
            "opencode",
            "OpenCode",
        )?;
        let data = json!({
            "command_id": input.command_id,
            "turn_id": input.turn_id,
            "reason": NATIVE_TERMINAL_DEADLINE_REASON,
        });
        self.commit_event("turn.unknown", native_id, data).await
    }

    /// Start the owned-generation context poll once readiness proved the
    /// v1 binding. Only bound sessions poll; unbound ones never advertise
    /// `turn_safe_point` and this stays idle.
    fn start_context_poll(&mut self) {
        if self.context_poll_abort.is_some() {
            return;
        }
        let Some(session_id) = self.status.session_id.clone() else {
            return;
        };
        let handle = context_hooks::spawn_context_poll(
            self.http.clone(),
            session_id,
            self.context_tx.clone(),
        );
        self.context_poll_abort = Some(handle.abort_handle());
    }

    /// Abort every context task (poll and in-flight claim/resolve workers)
    /// and drop any unbound pending hook without claiming. Called at stop
    /// and before the event loop ends so no task outlives its generation.
    fn abort_context_hooks(&mut self) {
        if let Some(abort) = self.context_poll_abort.take() {
            abort.abort();
        }
        for (_, abort) in self.context_hook_workers.drain() {
            abort.abort();
        }
        self.pending_context_hook = None;
    }

    /// Handle one poll or worker message in this loop.
    async fn handle_context_msg(&mut self, message: ContextHookMsg) {
        match message {
            ContextHookMsg::Arrived(signal) => self.offer_context_hook(signal.hook),
            ContextHookMsg::Done(done) => self.settle_context_hook(done).await,
        }
    }

    /// Accept one offered hook, unless one is already pending/in flight or
    /// this exact hook already settled. At most one hook is ever in flight;
    /// a second concurrent offer is dropped (the producer safe-passes it).
    fn offer_context_hook(&mut self, hook: EliotContextHook) {
        if !self.status.ready || self.status.stopped || self.writes_blocked {
            // An unclaimed offer expires on its own vendor deadline.
            return;
        }
        if hook.session_id.trim().is_empty()
            || hook.hook_id.trim().is_empty()
            || hook.message_id.trim().is_empty()
        {
            return;
        }
        if self.status.session_id.as_deref() != Some(hook.session_id.as_str()) {
            return;
        }
        if self.settled_hook_ids.contains(&hook.hook_id) {
            return;
        }
        if self.pending_context_hook.is_some() || !self.context_hook_workers.is_empty() {
            return;
        }
        let ttl_ms = hook.expires_in_ms.clamp(0, 10_000) as u64;
        self.pending_context_hook = Some(PendingContextHook {
            hook,
            deadline: Instant::now() + std::time::Duration::from_millis(ttl_ms),
        });
        self.try_advance_pending_hook();
    }

    /// The exact inbox-to-current-turn binding for one hook message id,
    /// proven only through actual delivery (`delivered_turn`). A missing
    /// entry, an ended turn, or a delivery for future work never binds.
    fn bound_turn_for_message(&self, message_id: &str) -> Option<String> {
        if !self.status.ready || self.status.stopped || self.status.failure.is_some() {
            return None;
        }
        let turn_id = self.delivered_turn.get(message_id)?;
        if self.status.active_turn_id.as_deref() == Some(turn_id.as_str()) {
            Some(turn_id.clone())
        } else {
            None
        }
    }

    /// Advance the pending hook: claim when bound, keep waiting while
    /// unbound inside its deadline, cancel once the deadline passes.
    fn try_advance_pending_hook(&mut self) {
        if self.pending_context_hook.is_none() {
            return;
        }
        let Some(pending) = self.pending_context_hook.as_ref() else {
            return;
        };
        let message_id = pending.hook.message_id.clone();
        let deadline = pending.deadline;
        match self.bound_turn_for_message(&message_id) {
            Some(turn_id) => {
                let hook = self
                    .pending_context_hook
                    .take()
                    .expect("pending hook retained")
                    .hook;
                self.spawn_context_claim(hook, turn_id, deadline);
            }
            None if Instant::now() >= deadline => {
                let hook = self
                    .pending_context_hook
                    .take()
                    .expect("pending hook retained")
                    .hook;
                self.spawn_context_cancel(hook);
            }
            None => {}
        }
    }

    /// Expire every unbound pending hook whose deadline has passed. Called
    /// on each loop iteration so a hook never waits past its bound even
    /// when no delivery frame arrives.
    fn expire_pending_hooks(&mut self) {
        self.try_advance_pending_hook();
    }

    /// Spawn the one Store claim, vendor resolve, and report back through
    /// this loop. The worker never retries an ambiguous resolve and never
    /// commits; the receipt below is the only durable write.
    fn spawn_context_claim(&mut self, hook: EliotContextHook, turn_id: String, deadline: Instant) {
        let hook_id = hook.hook_id.clone();
        let consumer_id = ulid::Ulid::new().to_string();
        let http = self.http.clone();
        let host = self.host.clone();
        let credential = self.credential.clone();
        let done_tx = self.context_tx.clone();
        let pending_hook_id = hook_id.clone();
        let task = tokio::spawn(async move {
            let done =
                run_context_claim(http, host, credential, hook, turn_id, consumer_id, deadline)
                    .await;
            // Keep the completion until the consumer accepts it or exits.
            let _ = done_tx
                .send(ContextHookMsg::Done(ContextHookDone {
                    hook_id: pending_hook_id,
                    turn_id: done.turn_id,
                    consumer_id: done.consumer_id,
                    claimed: done.claimed,
                    outcome: done.outcome,
                }))
                .await;
        });
        self.context_hook_workers
            .insert(hook_id, task.abort_handle());
    }

    /// Spawn a best-effort unblock for a hook that never bound: one cancel
    /// resolve, never retried, with no receipt. The vendor hook expires on
    /// its own deadline even if this write is lost.
    fn spawn_context_cancel(&mut self, hook: EliotContextHook) {
        let hook_id = hook.hook_id.clone();
        let http = self.http.clone();
        let done_tx = self.context_tx.clone();
        let cancel_deadline = Instant::now() + std::time::Duration::from_secs(30);
        let done_hook_id = hook_id.clone();
        let task = tokio::spawn(async move {
            let input = EliotContextResolveInput {
                hook_id: hook.hook_id.clone(),
                session_id: hook.session_id.clone(),
                message_id: hook.message_id.clone(),
                context: None,
                cancel: Some(true),
            };
            let _ = http.eliot_context_resolve(input, cancel_deadline).await;
            let _ = done_tx
                .send(ContextHookMsg::Done(ContextHookDone {
                    hook_id: done_hook_id,
                    turn_id: String::new(),
                    consumer_id: String::new(),
                    claimed: Vec::new(),
                    outcome: HookResolveOutcome::NoReceipt,
                }))
                .await;
        });
        self.context_hook_workers
            .insert(hook_id, task.abort_handle());
    }

    /// Commit the receipt for one settled hook through the shared source.
    /// `Applied` carries its outcome; `Empty` carries none and requires the
    /// persisted empty selection; anything else commits nothing and leaves
    /// claimed items frozen.
    async fn settle_context_hook(&mut self, done: ContextHookDone) {
        self.context_hook_workers.remove(&done.hook_id);
        if self.settled_hook_ids.contains(&done.hook_id) {
            return;
        }
        self.remember_settled_hook(done.hook_id.clone());
        match done.outcome {
            HookResolveOutcome::NoReceipt => {}
            HookResolveOutcome::Applied => {
                if done.claimed.is_empty()
                    || done.turn_id.trim().is_empty()
                    || done.consumer_id.trim().is_empty()
                {
                    return;
                }
                let Some(session_id) = self.status.session_id.clone() else {
                    return;
                };
                let outcome = ContextOutcome {
                    mechanism: "context_hook".to_owned(),
                    application_point: InjectPoint::TurnSafePoint,
                    consumer_id: done.consumer_id.clone(),
                };
                let context_outcome = match serde_json::to_value(&outcome) {
                    Ok(value) => value,
                    Err(_) => return,
                };
                let native_id = format!(
                    "opencode:{session_id}:{}:hook.before_model:{}",
                    done.turn_id, done.consumer_id
                );
                let data = json!({
                    "turn_id": done.turn_id,
                    "consumer_id": done.consumer_id,
                    "context_outcome": context_outcome,
                });
                let _ = self
                    .commit_event("hook.before_model", native_id, data)
                    .await;
            }
            HookResolveOutcome::Empty => {
                if done.turn_id.trim().is_empty() || done.consumer_id.trim().is_empty() {
                    return;
                }
                let Some(session_id) = self.status.session_id.clone() else {
                    return;
                };
                let native_id = format!(
                    "opencode:{session_id}:{}:hook.before_model:{}",
                    done.turn_id, done.consumer_id
                );
                let data = json!({
                    "turn_id": done.turn_id,
                    "consumer_id": done.consumer_id,
                });
                let _ = self
                    .commit_event("hook.before_model", native_id, data)
                    .await;
            }
        }
    }

    fn remember_settled_hook(&mut self, hook_id: String) {
        if self.settled_hook_ids.insert(hook_id.clone()) {
            self.settled_hook_order.push_back(hook_id);
        }
        while self.settled_hook_order.len() > 128 {
            if let Some(evicted) = self.settled_hook_order.pop_front() {
                self.settled_hook_ids.remove(&evicted);
            }
        }
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
        if matches!(
            envelope.event_type.as_str(),
            "session.compaction.started" | "session.compaction.ended" | "session.compaction.failed"
        ) && envelope.durable.as_ref().is_some_and(|durable| {
            self.status.session_id.as_deref() == Some(durable.aggregate_id.as_str())
                && self.expected_seq.is_some_and(|next| durable.seq < next)
        }) {
            // Keep a replay bound to the original observation, even after its turn ends.
            return Ok(());
        }
        let next_retry = envelope.event_type == "session.retry.scheduled"
            && envelope.durable.as_ref().is_some_and(|durable| {
                self.expected_seq == Some(durable.seq)
                    && self.status.session_id.as_deref() == Some(durable.aggregate_id.as_str())
            });
        let active_progress = self.is_active_turn_progress(&envelope);
        self.check_sequence(&envelope).await?;
        if active_progress {
            self.refresh_terminal_deadline();
        }
        match envelope.event_type.as_str() {
            "server.connected" => {
                self.status.connected = true;
                Ok(())
            }
            "session.created" => self.observe_session_created(&envelope.data).await,
            "session.deleted" => self.observe_session_deleted(&envelope.data).await,
            "session.inbox.enqueued" => self.observe_inbox(&envelope.data, false).await,
            "session.inbox.delivered" => self.observe_inbox(&envelope.data, true).await,
            "session.inbox.cancelled" => self.observe_inbox_cancelled(&envelope.data).await,
            "session.execution.started" => self.observe_execution_started(&envelope).await,
            "session.compaction.started"
            | "session.compaction.ended"
            | "session.compaction.failed" => self.observe_compaction(&envelope).await,
            "session.retry.scheduled" if next_retry => {
                self.observe_retry_scheduled(&envelope).await
            }
            "session.step.started" => {
                if self.status.active_turn_id.is_some()
                    && let Ok(step) = serde_json::from_value::<StepStartedData>(envelope.data)
                    && self.status.session_id.as_deref() == Some(step.session_id.as_str())
                {
                    self.active_model = Some(step.model);
                }
                Ok(())
            }
            "session.tool.input.started" => self.observe_tool_input_started(&envelope.data).await,
            "session.tool.called" => self.observe_tool_called(&envelope).await,
            "session.tool.success" | "session.tool.failed" => self.complete_tool(&envelope).await,
            "session.execution.succeeded"
            | "session.execution.failed"
            | "session.execution.interrupted" => self.complete_turn(&envelope).await,
            "permission.asked" => self.open_permission(&envelope.data).await,
            "permission.replied" => self.settle_permission(&envelope.data).await,
            "form.created" => self.open_form(&envelope.data).await,
            "form.replied" => self.settle_form(&envelope.data).await,
            "form.cancelled" => self.cancel_form(&envelope.data).await,
            "session.model.selected" | "session.agent.selected" => {
                self.observe_selected(&envelope).await
            }
            "session.status" => self.observe_session_status(&envelope).await,
            // Known streaming details are intentionally folded into terminal
            // readback or left unmapped. New event tags are retained below as
            // neutral diagnostics after exact session binding.
            _ if is_known_streaming_detail(&envelope.event_type) => Ok(()),
            _ => self.commit_unclassified_event(&envelope).await,
        }
    }

    async fn observe_session_status(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        if envelope.data.get("sessionID").and_then(Value::as_str) != Some(session_id.as_str()) {
            return Ok(());
        }
        let Some(tag) = envelope
            .data
            .get("status")
            .and_then(|status| status.get("type"))
            .and_then(Value::as_str)
        else {
            return Ok(());
        };
        match NativeSessionStatus::from_tag(tag) {
            NativeSessionStatus::Idle | NativeSessionStatus::Retry | NativeSessionStatus::Busy => {
                Ok(())
            }
            NativeSessionStatus::Other(tag) => {
                self.commit_unclassified_native(
                    &session_id,
                    "",
                    &envelope.id,
                    "opencode.session.status.type",
                    tag,
                )
                .await
            }
        }
    }

    async fn commit_unclassified_event(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        let data_session = envelope.data.get("sessionID").and_then(Value::as_str);
        if envelope.data.get("sessionID").is_some() && data_session.is_none() {
            return Ok(());
        }
        let durable_session = envelope
            .durable
            .as_ref()
            .map(|durable| durable.aggregate_id.as_str());
        if data_session.is_some_and(|native_id| native_id != session_id.as_str())
            || durable_session.is_some_and(|native_id| native_id != session_id.as_str())
            || (data_session != Some(session_id.as_str())
                && durable_session != Some(session_id.as_str()))
        {
            return Ok(());
        }
        self.commit_unclassified_native(
            &session_id,
            "",
            &envelope.id,
            "opencode.sse.event_type",
            &envelope.event_type,
        )
        .await
    }

    async fn observe_compaction(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        if envelope.data["sessionID"].as_str() != Some(session_id.as_str()) {
            if envelope
                .durable
                .as_ref()
                .is_some_and(|durable| durable.aggregate_id.as_str() == session_id.as_str())
            {
                self.record_source_gap_once(
                    "OpenCode compaction event session identity disagreed with its durable aggregate",
                )
                .await?;
            }
            return Ok(());
        }
        if envelope
            .durable
            .as_ref()
            .is_some_and(|durable| durable.aggregate_id.as_str() != session_id.as_str())
        {
            self.record_source_gap_once(
                "OpenCode compaction event durability disagreed with its session",
            )
            .await?;
            return Ok(());
        }
        if envelope.id.trim().is_empty() {
            self.record_source_gap_once("OpenCode compaction event omitted its native identity")
                .await?;
            return Ok(());
        }

        let native_reason = match envelope.data["reason"].as_str() {
            Some("auto") => NativeCompactionReason::Auto,
            Some("manual") => NativeCompactionReason::Manual,
            Some(other) => NativeCompactionReason::Other(other),
            None => NativeCompactionReason::Missing,
        };
        if let NativeCompactionReason::Other(tag) = native_reason {
            self.commit_unclassified_native(
                &session_id,
                "",
                &envelope.id,
                "opencode.session.compaction.reason",
                tag,
            )
            .await?;
        }
        let reason = native_reason.projection();
        let input_id = envelope.data["inputID"]
            .as_str()
            .filter(|input| !input.trim().is_empty());

        if self.stashed_started.is_some()
            && reason == "manual"
            && envelope.durable.is_some()
            && let Some(watch) = self.compaction.as_mut()
        {
            match envelope.event_type.as_str() {
                "session.compaction.started" if watch.input.is_none() => {
                    watch.input = input_id.map(str::to_owned);
                }
                "session.compaction.ended"
                    if watch.input.is_some()
                        && (input_id.is_none() || watch.input.as_deref() == input_id) =>
                {
                    watch.result = Some(true);
                }
                "session.compaction.failed"
                    if watch.input.as_deref().is_some() && watch.input.as_deref() == input_id =>
                {
                    watch.result = Some(false);
                }
                _ => {}
            }
        }

        match envelope.event_type.as_str() {
            "session.compaction.started" => {
                self.compaction_start = envelope.durable.as_ref().map(|durable| CompactionStart {
                    seq: durable.seq,
                    reason,
                    activity: self.compaction_activity(),
                });
            }
            "session.compaction.ended" | "session.compaction.failed" => {
                self.compaction_start = None;
            }
            _ => {}
        }

        if envelope.event_type == "session.compaction.ended" {
            let native_id = event_id(
                &session_id,
                "session",
                &envelope.id,
                "session.context.compacted",
                "opencode",
                "OpenCode",
            )?;
            let mut data = json!({"reason": reason});
            if envelope.durable.is_none() {
                data["durability"] = json!("unknown");
            } else if let Some(turn_id) = self.status.active_turn_id.as_deref() {
                data["turn_id"] = json!(turn_id);
            }
            self.commit_event("session.context.compacted", native_id, data)
                .await?;
        }
        Ok(())
    }

    fn compaction_activity(&self) -> Option<eliot_core::CompactionActivity> {
        if let Some(turn_id) = &self.status.active_turn_id {
            return Some(eliot_core::CompactionActivity::Turn {
                turn_id: turn_id.clone(),
            });
        }
        let watch = self.compaction.as_ref()?;
        (self.stashed_started.is_some() && watch.input.is_some()).then(|| {
            eliot_core::CompactionActivity::Command {
                command_id: watch.command_id.clone(),
            }
        })
    }

    async fn observe_retry_scheduled(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let (Some(turn_id), Ok(retry)) = (
            self.status.active_turn_id.clone(),
            serde_json::from_value::<RetryScheduledData>(envelope.data.clone()),
        ) else {
            return Ok(());
        };
        if self.status.session_id.as_deref() != Some(retry.session_id.as_str()) {
            return Ok(());
        }
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let message = secrets.redact_text(&retry.error.message);
        let message = message.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut chars = message.chars();
        let message: String = chars.by_ref().take(AGENT_NOTICE_MESSAGE_CHARS).collect();
        let notice = NativeNotice {
            turn_id,
            kind: NativeNoticeKind::Retrying,
            provider_kind: match retry.error.error_type.as_str() {
                "provider.rate-limit" => Some(ProviderRestrictionKind::RateLimit),
                "provider.quota" => Some(ProviderRestrictionKind::Quota),
                _ => None,
            },
            message: (!message.is_empty()).then_some(message),
            truncated: chars.next().is_some(),
        };
        let native_id = event_id(
            &retry.session_id,
            &notice.turn_id,
            &envelope.id,
            "turn.notice",
            "opencode",
            "OpenCode",
        )?;
        let data = serde_json::to_value(notice)
            .map_err(|_| Error::new("internal", "OpenCode retry notice could not be encoded"))?;
        self.commit_event("turn.notice", native_id, data).await
    }

    /// Durable envelopes of our session aggregate carry a contiguous
    /// per-aggregate sequence. A forward skip proves transport loss;
    /// anything else (ephemeral events, foreign aggregates) is untracked.
    /// After resume readiness the live edge is adopted from the first
    /// observed durable event instead.
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
            None => {
                // Resume live edge: the unobserved prefix is history.
                self.expected_seq = Some(durable.seq.saturating_add(1));
                Ok(())
            }
            Some(expected) if durable.seq == expected => {
                self.expected_seq = Some(expected.saturating_add(1));
                Ok(())
            }
            Some(expected) if durable.seq < expected => {
                // A duplicate replay of an already-seen record: ignore.
                Ok(())
            }
            Some(expected) => {
                let kind = envelope.event_type.as_str();
                let kind = if kind.len() <= 96
                    && kind.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    }) {
                    kind
                } else {
                    "unknown"
                };
                let reason = format!(
                    "OpenCode event stream skipped durable sequence numbers: expected {expected}, saw {} ({kind})",
                    durable.seq
                );
                if self.gap_reported {
                    return Err(Error::new(
                        "unknown",
                        "OpenCode native source already has a gap",
                    ));
                }
                if matches!(
                    kind,
                    "session.compaction.ended" | "session.compaction.failed"
                ) && envelope.data["sessionID"].as_str() == Some(session_id.as_str())
                {
                    let activity = self.compaction_activity();
                    let start_seq = self
                        .compaction_start
                        .as_ref()
                        .filter(|start| {
                            envelope.data["reason"].as_str() == Some(start.reason)
                                && start.activity == activity
                        })
                        .map(|start| start.seq);
                    let boundary = eliot_core::CompactionGapBoundary {
                        native_ref: session_id,
                        start_seq,
                        activity,
                        usage: compaction_usage_echo(&envelope.data),
                    };
                    if self
                        .stream_source()
                        .record_compaction_gap(&reason, expected, durable.seq, boundary)
                        .await?
                    {
                        self.expected_seq = Some(durable.seq.saturating_add(1));
                        return Ok(());
                    }
                } else {
                    self.stream_source()
                        .record_gap_range_until(
                            &reason,
                            Some(expected.to_string()),
                            Some(durable.seq.to_string()),
                            Instant::now() + REPLY_DEADLINE,
                        )
                        .await?;
                }
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

    async fn observe_inbox(&mut self, data: &Value, delivered: bool) -> Result<()> {
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
        self.seen_inbox.insert(inbox.inbox_id.clone());
        if delivered {
            self.seen_delivered.insert(inbox.inbox_id.clone());
            // Bind the delivery to the running turn, if any: a delivery
            // with no active turn belongs to future work, never to a
            // fenced one.
            if let Some(active) = self.status.active_turn_id.clone() {
                self.delivered_turn.insert(inbox.inbox_id.clone(), active);
            }
            self.settle_delivered_steers(&inbox.inbox_id);
        }
        if self.seen_inbox.len() > 1024 {
            // Defensive bound; a generation holds at most 32 commands.
            self.seen_inbox.clear();
            self.seen_delivered.clear();
            self.seen_cancelled.clear();
            self.delivered_turn.clear();
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
        if !inbox.inbox_id.trim().is_empty() {
            self.seen_cancelled.insert(inbox.inbox_id.clone());
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
        self.miss_cancelled_steers(&inbox.inbox_id);
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

    /// `session.tool.input.started`: remember the native tool name for the
    /// exact active turn. This is the only installed tool event carrying
    /// the name; `called`/`success`/`failed` read it back here, exactly
    /// like the vendor TUI reducer. A fact with no active turn or for a
    /// foreign session drops; a corrupt payload gaps like the other
    /// lifecycle arms.
    async fn observe_tool_input_started(&mut self, data: &Value) -> Result<()> {
        let started: ToolInputStartedData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode tool input start omitted its exact identity or name",
            )
        })?;
        if self.status.active_turn_id.is_none()
            || self.status.session_id.as_deref() != Some(started.session_id.as_str())
            || started.id.trim().is_empty()
            || started.name.trim().is_empty()
        {
            return Ok(());
        }
        let Some(part) = self.tool_part(&started.id).await? else {
            return Ok(());
        };
        part.name = Some(started.name);
        Ok(())
    }

    /// `session.tool.called`: the input is complete and native execution
    /// starts (the vendor sets the part to `running` on exactly this
    /// event). Commit `tool.started` bound to the exact active turn with
    /// the native part id as the item id. A string `input.command` on any
    /// tool input (the installed shell tool) is redacted and bounded like
    /// the other plugins; other arguments and results are never
    /// persisted. A replayed `called` for an already-started part never
    /// re-commits.
    async fn observe_tool_called(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let called: ToolEventData =
            serde_json::from_value(envelope.data.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "OpenCode tool call omitted its exact identity",
                )
            })?;
        if self.status.session_id.as_deref() != Some(called.session_id.as_str())
            || called.id.trim().is_empty()
        {
            return Ok(());
        }
        let Some((command_id, turn_id)) = self.active_tool_turn().await? else {
            return Ok(());
        };
        let Some(part) = self.tool_part(&called.id).await? else {
            return Ok(());
        };
        if part.started {
            return Ok(());
        }
        let tool = part.name.clone().unwrap_or_else(|| "tool".to_owned());
        if matches!(tool.as_str(), "write" | "edit") {
            part.write_path = envelope.data["input"]["path"]
                .as_str()
                .filter(|path| path.len() <= 4096)
                .map(str::to_owned);
        }
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": called.id,
            "tool": tool,
        });
        if let Some(command) = envelope
            .data
            .get("input")
            .and_then(|input| input.get("command"))
            .and_then(Value::as_str)
            .filter(|command| !command.trim().is_empty())
        {
            let policy = SecretPolicy::new(self.secrets.iter().cloned());
            let redacted = policy.redact_text(command);
            data["command_truncated"] = json!(redacted.len() > MAX_TOOL_COMMAND_BYTES);
            data["command"] =
                Value::String(policy.redact_bounded(&redacted, MAX_TOOL_COMMAND_BYTES));
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("OpenCode tool start exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            &called.session_id,
            &turn_id,
            &called.id,
            "tool.started",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event("tool.started", native_id, data).await?;
        if let Some(part) = self.tool_parts.get_mut(&called.id) {
            part.started = true;
        }
        Ok(())
    }

    /// Translate the installed native terminal verdict: session.tool.failed
    /// is an error, and session.tool.success is an error only when the
    /// structured completed metadata carries a nonzero integer exit code.
    /// Persist only bounded redacted textual output on failures. Native
    /// truncation stays explicit, so incomplete output cannot match an exact
    /// repeated failure. A terminal without an observed start is a coverage
    /// gap, never a guessed tool row; a replay never re-commits.
    async fn complete_tool(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let failed_event = envelope.event_type == "session.tool.failed";
        let (terminal, failed, native_error, output_truncated) = if failed_event {
            let failure: ToolFailedData =
                serde_json::from_value(envelope.data.clone()).map_err(|_| {
                    Error::new(
                        "native_protocol",
                        "OpenCode tool failure omitted its exact identity",
                    )
                })?;
            (
                ToolEventData {
                    session_id: failure.session_id,
                    id: failure.id,
                },
                true,
                failure.error.map(|error| error.message),
                false,
            )
        } else {
            let terminal: ToolEventData =
                serde_json::from_value(envelope.data.clone()).map_err(|_| {
                    Error::new(
                        "native_protocol",
                        "OpenCode tool success omitted its exact identity",
                    )
                })?;
            let metadata = envelope.data.get("metadata");
            let failed = metadata
                .and_then(|metadata| metadata.get("status"))
                .and_then(Value::as_str)
                == Some("completed")
                && metadata
                    .and_then(|metadata| metadata.get("exit"))
                    .and_then(Value::as_i64)
                    .is_some_and(|exit| exit != 0);
            let (output, truncated) = if failed {
                native_tool_failure_output(&envelope.data)
            } else {
                (None, false)
            };
            (terminal, failed, output, truncated)
        };
        if self.status.session_id.as_deref() != Some(terminal.session_id.as_str())
            || terminal.id.trim().is_empty()
        {
            return Ok(());
        }
        let Some((command_id, turn_id)) = self.active_tool_turn().await? else {
            return Ok(());
        };
        let Some(part) = self.tool_part(&terminal.id).await? else {
            return Ok(());
        };
        if part.completed {
            return Ok(());
        }
        if !part.started {
            part.completed = true;
            return self
                .stream_source()
                .record_coverage_gap(
                    "OpenCode tool terminal did not match an observed tool start",
                    &["tool"],
                )
                .await;
        }
        let tool = part.name.clone().unwrap_or_else(|| "tool".to_owned());
        let write_path = part.write_path.take();
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let mut data = json!({
            "command_id": command_id,
            "turn_id": turn_id,
            "item_id": terminal.id,
            "tool": tool,
            "status": if failed { "error" } else { "completed" },
        });
        attach_native_error(&mut data, native_error.as_deref(), &secrets);
        if output_truncated && let Some(native_error) = data.get_mut("native_error") {
            native_error["truncated"] = json!(true);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return self
                .record_gap("OpenCode tool completion exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            &terminal.session_id,
            &turn_id,
            &terminal.id,
            "tool.completed",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event("tool.completed", native_id, data).await?;
        if !failed && envelope.data["executed"] != true {
            self.commit_file_edits(
                &terminal,
                &turn_id,
                &tool,
                write_path.as_deref(),
                &envelope.data,
                &secrets,
            )
            .await?;
        }
        if let Some(part) = self.tool_parts.get_mut(&terminal.id) {
            part.completed = true;
        }
        Ok(())
    }

    async fn commit_file_edits(
        &mut self,
        terminal: &ToolEventData,
        turn_id: &str,
        tool: &str,
        write_path: Option<&str>,
        data: &Value,
        policy: &SecretPolicy,
    ) -> Result<()> {
        let files = data["metadata"]["files"].as_array();
        let paths: Vec<_> = match (tool, files) {
            ("edit" | "patch", Some(files)) => files
                .iter()
                .filter_map(|file| file["file"].as_str())
                .collect(),
            ("write" | "edit", _) => write_path.into_iter().collect(),
            _ => return Ok(()),
        };
        for (index, path) in paths.iter().take(MAX_TOOL_PARTS).enumerate() {
            if let Some(edit_path) =
                eliot_plugin::edit_path::evidence(self.http.directory(), path, policy)
            {
                let id = event_id(
                    &terminal.session_id,
                    turn_id,
                    &format!("{}:{index}", terminal.id),
                    "tool.file.edited",
                    "opencode",
                    "OpenCode",
                )?;
                self.commit_event(
                    "tool.file.edited",
                    id,
                    json!({"turn_id":turn_id,"edit_path":edit_path}),
                )
                .await?;
            }
        }
        if paths.len() > MAX_TOOL_PARTS {
            self.stream_source()
                .record_coverage_gap(
                    "OpenCode file edits overflowed the tool fact bound",
                    &["tool"],
                )
                .await?;
        }
        Ok(())
    }

    /// Look up (or open) the tracking slot for one tool part id. The map
    /// retains the first [`MAX_TOOL_PARTS`] parts of the turn without
    /// eviction; a fact for a later part drops after one tool-family
    /// coverage gap per turn, never growing the map without bound.
    async fn tool_part(&mut self, id: &str) -> Result<Option<&mut ToolPart>> {
        if !self.tool_parts.contains_key(id) {
            if self.tool_parts.len() >= MAX_TOOL_PARTS {
                if !self.tool_parts_overflowed {
                    self.tool_parts_overflowed = true;
                    self.stream_source()
                        .record_coverage_gap(
                            "OpenCode tool parts overflowed their per-turn bound",
                            &["tool"],
                        )
                        .await?;
                }
                return Ok(None);
            }
            self.tool_parts.insert(id.to_owned(), ToolPart::default());
        }
        Ok(self.tool_parts.get_mut(id))
    }

    /// Exact active-turn binding shared by the tool facts. A tool fact
    /// with no turn before this generation's first send is the dead
    /// generation's tail: drop it, never attribute it (same rule as the
    /// execution terminal). Any other turn-less tool fact is a tool-family
    /// coverage gap that leaves the turn outcome alone.
    async fn active_tool_turn(&mut self) -> Result<Option<(String, String)>> {
        match (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            (Some(command_id), Some(turn_id)) => Ok(Some((command_id, turn_id))),
            _ if self.resumed && !self.saw_send => Ok(None),
            _ => {
                self.stream_source()
                    .record_coverage_gap(
                        "OpenCode tool event matched no exact active turn",
                        &["tool"],
                    )
                    .await?;
                Ok(None)
            }
        }
    }

    /// Drop the per-turn tool bookkeeping when the turn fences.
    fn clear_tool_facts(&mut self) {
        self.tool_parts.clear();
        self.tool_parts_overflowed = false;
    }

    /// Bind a terminal event of our session to its exact Eliot turn.
    /// A terminal with no turn before this generation's first send is the
    /// dead generation's close: drop it (`None`), never attribute it. Any
    /// other turn-less terminal is a transcript gap.
    async fn resolve_terminal_turn(&mut self) -> Result<Option<(String, String)>> {
        match (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            (Some(command_id), Some(turn_id)) => Ok(Some((command_id, turn_id))),
            // Before this generation's first send a resumed terminal is the
            // dead generation's close: drop it, never attribute it. After a
            // send (or on a fresh session) a terminal with no turn is a
            // transcript gap.
            _ if self.resumed && !self.saw_send => Ok(None),
            _ => {
                self.record_gap("OpenCode terminal event matched no exact active turn")
                    .await?;
                Err(Error::new(
                    "unknown",
                    "OpenCode turn completed outside any Eliot send",
                ))
            }
        }
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
        if self.last_compaction_terminal.as_ref() == Some(&envelope.id) {
            return Ok(());
        }
        if envelope.event_type == "session.execution.interrupted"
            && let Some(reason) = terminal.reason.as_deref()
            && !matches!(reason, "user" | "shutdown" | "superseded" | "inactivity")
        {
            self.commit_unclassified_native(
                &terminal.session_id,
                "",
                &envelope.id,
                "opencode.session.execution.interrupted.reason",
                reason,
            )
            .await?;
        }
        if let Some(watch) = self.compaction.take() {
            self.status.compaction_in_flight = false;
            self.stashed_started = None;
            self.last_compaction_terminal = Some(envelope.id.clone());
            if let (Some(input), Some(result)) = (watch.input, watch.result) {
                let applied = result && envelope.event_type == "session.execution.succeeded";
                let _ = watch.terminal.send((input, applied));
            }
            return Ok(());
        }
        let Some((command_id, turn_id)) = self.resolve_terminal_turn().await? else {
            return Ok(());
        };
        self.terminal_deadline = None;
        // Fence the captured turn the moment its terminal is observed,
        // independent of readback success: controls interleaving with
        // the readback await below must never see it active again, and
        // watches still naming it can never apply.
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        self.miss_ended_steers(&turn_id);
        self.clear_tool_facts();
        // Canonical outcome decided here at the plugin boundary from the
        // OpenCode execution vocabulary; the raw SSE event type is preserved
        // as `native_status` next to it.
        let (status, outcome) = match envelope.event_type.as_str() {
            "session.execution.succeeded" => ("completed", "success"),
            "session.execution.failed" => ("failed", "failure"),
            "session.execution.interrupted" => ("interrupted", "interrupted"),
            _ => {
                return Err(Error::new(
                    "invalid",
                    "OpenCode terminal event type is not classified",
                ));
            }
        };
        // The exactly associated stream terminal proves status independently
        // of optional HTTP text/usage enrichment. A dead server cannot revoke
        // terminal evidence already received on this generation's stream.
        let input =
            self.status.turn_input.as_ref().ok_or_else(|| {
                Error::new("unknown", "OpenCode turn has no exact input association")
            })?;
        let readback = optional_terminal_readback(
            read_turn_messages(
                &self.http,
                &terminal.session_id,
                &input.inbox_id,
                Instant::now() + EVENT_DEADLINE,
            )
            .await,
        )?;
        if readback
            .as_ref()
            .is_some_and(|readback| readback.outcome != status)
        {
            self.record_gap("OpenCode terminal event disagreed with its idle message")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode turn outcome is contradictory",
            ));
        }
        let input = input.clone();
        if let Err(error) = self
            .commit_terminal_facts(
                &input,
                &terminal,
                &envelope.event_type,
                envelope.created,
                (status, outcome),
                readback.as_ref(),
            )
            .await
        {
            if error.failure_where.as_deref() != Some("plugin.events.put") {
                return Err(error);
            }
            // The host recorded the gap. The native execution already ended,
            // so its local handles can be released while the daemon settles it.
            self.status.source_unknown = true;
            self.active_model = None;
        }
        // The completed turn clears its Eliot pendings; drop the retained
        // native offers with it so a later ask reopens instead of reviving.
        self.drop_turn_requests(&turn_id);
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: turn_id.clone(),
            status: Some(status.to_owned()),
            final_text: readback
                .and_then(|readback| readback.final_text)
                .filter(|text| !is_blank_native_text(text)),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        Ok(())
    }

    async fn commit_terminal_facts(
        &mut self,
        input: &TurnInput,
        terminal: &ExecutionEventData,
        event_type: &str,
        terminal_event_created_ms: Option<i64>,
        (status, outcome): (&str, &str),
        readback: Option<&TerminalReadback>,
    ) -> Result<()> {
        let command_id = input.command_id.as_str();
        let turn_id = input.turn_id.as_str();
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        if let Some(readback) = readback
            && let Some(text) = readback.final_text.as_deref()
        {
            let data = json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "text": secrets.redact_text(text),
            });
            let native_id = event_id(
                &terminal.session_id,
                turn_id,
                &readback.idle_id,
                "message.completed",
                "opencode",
                "OpenCode",
            )?;
            // A message-event gap does not erase exact terminal text
            // obtained from readback; terminal_turn_data bounds final_text.
            let _message_commit = self.stream_source().commit_message(native_id, data).await?;
        }
        let mut data = terminal_turn_data(
            command_id,
            turn_id,
            status,
            terminal,
            readback,
            terminal_event_created_ms,
            &secrets,
        );
        if let Some(model) = self.active_model.take() {
            data["effective_model"] =
                json!(secrets.redact_text(&format!("{}/{}", model.provider_id, model.id)));
        }
        data["outcome"] = json!(outcome);
        data["native_status"] = json!(event_type);
        let usage = SessionUsage {
            context_used_tokens: readback.and_then(|readback| readback.context_used_tokens),
            source: Some("opencode:assistant_message_tokens".to_owned()),
            ..SessionUsage::default()
        };
        let native_id = event_id(
            &terminal.session_id,
            turn_id,
            readback.map_or(turn_id, |readback| readback.idle_id.as_str()),
            "session.usage",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event("session.usage", native_id, json!({"usage": usage}))
            .await?;
        let native_id = event_id(
            &terminal.session_id,
            turn_id,
            turn_id,
            "turn.completed",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event("turn.completed", native_id, data).await
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
        self.terminal_deadline = None;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
        self.clear_tool_facts();
        self.pending = None;
        self.status.turn_input = None;
        self.active_model = None;
        self.stashed_started = None;
        self.pending_requests.clear();
        self.abort_context_hooks();
        // Dropping the senders settles every steer waiter as unknown on
        // the host side; no verdict is knowable after the source ends.
        self.steer_watches.clear();
        self.seen_delivered.clear();
        self.seen_cancelled.clear();
        self.delivered_turn.clear();
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
        let current = data["turn_id"]
            .as_str()
            .is_some_and(|turn| self.status.active_turn_id.as_deref() == Some(turn));
        self.stream_source()
            .commit_event(kind, native_id, data)
            .await?;
        if current && kind != "turn.unknown" {
            self.refresh_terminal_deadline();
        }
        Ok(())
    }

    fn refresh_terminal_deadline(&mut self) {
        if let Some(deadline) = &mut self.terminal_deadline {
            *deadline = Instant::now() + self.terminal_inactivity;
        }
    }

    /// OpenCode has one execution lane per session. Its ordered execution
    /// deltas belong to the associated turn, including the deltas we do not
    /// publish to the Store.
    fn is_active_turn_progress(&self, envelope: &SseEnvelope) -> bool {
        let (Some(session), Some(turn)) = (
            self.status.session_id.as_deref(),
            self.status.active_turn_id.as_deref(),
        ) else {
            return false;
        };
        let named_session = envelope.data.get("sessionID");
        let durable = envelope.durable.as_ref();
        if named_session.is_some_and(|id| id.as_str() != Some(session))
            || durable.is_some_and(|record| {
                record.aggregate_id != session
                    || self.expected_seq.is_some_and(|next| record.seq < next)
            })
        {
            return false;
        }
        let kind = envelope.event_type.as_str();
        if let Some(id) = envelope.data.get("turnID") {
            return id.as_str() == Some(turn) && (named_session.is_some() || durable.is_some());
        }
        (named_session.is_some() || durable.is_some())
            && (kind.starts_with("session.step.")
                || kind.starts_with("session.tool.")
                || kind.starts_with("session.text.")
                || kind.starts_with("session.reasoning.")
                || kind == "session.retry.scheduled")
    }

    async fn commit_unclassified_native(
        &mut self,
        session_id: &str,
        turn_id: &str,
        item_id: &str,
        site: &'static str,
        native_tag: &str,
    ) -> Result<()> {
        let native_id = event_id(
            session_id,
            turn_id,
            item_id,
            UNCLASSIFIED_NATIVE_EVENT,
            "opencode",
            "OpenCode",
        )?;
        self.commit_event(
            UNCLASSIFIED_NATIVE_EVENT,
            native_id,
            unclassified(site, native_tag),
        )
        .await
    }

    /// `session.model.selected` / `session.agent.selected`: the switch
    /// is already proven by its synchronous 204 plus the `session.get`
    /// readback, so the shape is pinned and the event ignored. A corrupt
    /// payload for a foreign or malformed selection never gaps: nothing we
    /// consume changed.
    async fn observe_selected(&mut self, envelope: &SseEnvelope) -> Result<()> {
        let session_id = if envelope.event_type == "session.model.selected" {
            serde_json::from_value::<ModelSelectedData>(envelope.data.clone())
                .ok()
                .map(|data| data.session_id)
        } else {
            serde_json::from_value::<AgentSelectedData>(envelope.data.clone())
                .ok()
                .map(|data| data.session_id)
        };
        if session_id.as_deref() != self.status.session_id.as_deref() {
            return Ok(());
        }
        Ok(())
    }

    /// `permission.asked`: open an `approval` request bound to the exact
    /// session, the active turn and the native request id. An ask with no
    /// active turn is foreign or pre-readiness and drops (the resume legs
    /// prove no pre-readiness ask is pending); a re-ask of an open id is
    /// idempotent when the offer is identical and a gap when it changed
    /// (offers are static — no updated event exists); a re-ask after
    /// resolution gaps, it never reopens.
    async fn open_permission(&mut self, data: &Value) -> Result<()> {
        let asked: PermissionAskedData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode permission ask omitted its exact identity",
            )
        })?;
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            return Ok(());
        };
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        if asked.session_id != session_id {
            return Ok(());
        }
        let pending = describe_permission(&asked, &session_id, &turn_id)
            .map_err(|error| Error::new(error.code, format!("OpenCode {}", error.message)))?;
        self.open_request(pending).await
    }

    /// `form.created`: open a `question` request under the same binding
    /// and idempotency rules as a permission ask.
    async fn open_form(&mut self, data: &Value) -> Result<()> {
        let created: FormCreatedData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode form creation omitted its exact identity",
            )
        })?;
        let Some(turn_id) = self.status.active_turn_id.clone() else {
            return Ok(());
        };
        let Some(session_id) = self.status.session_id.clone() else {
            return Ok(());
        };
        if created.form.session_id != session_id {
            return Ok(());
        }
        let pending = describe_form(&created.form, &session_id, &turn_id)
            .map_err(|error| Error::new(error.code, format!("OpenCode {}", error.message)))?;
        self.open_request(pending).await
    }

    /// Durably open a described native request. Details commit redacted;
    /// the raw offer stays in memory for choice matching.
    async fn open_request(&mut self, pending: CorePending) -> Result<()> {
        if self.resolved_native_ids.contains(&pending.id) {
            self.record_gap("OpenCode native request was re-issued after resolution")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode native request identity is ambiguous",
            ));
        }
        if let Some(existing) = self.pending_requests.get(&pending.id) {
            if existing.request.turn_id != pending.turn_id
                || existing.request.kind != pending.kind
                || existing.request.details != pending.details
            {
                self.record_gap("OpenCode native request offer changed after opening")
                    .await?;
                return Err(Error::new(
                    "unknown",
                    "OpenCode native request identity is ambiguous",
                ));
            }
            return Ok(());
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            self.record_gap("OpenCode pending native requests overflowed their bound")
                .await?;
            return Err(Error::new(
                "unknown",
                "OpenCode native request overflowed its bound",
            ));
        }
        let Some(session_id) = self.status.session_id.clone() else {
            return Err(Error::new("unknown", "OpenCode session is unresolved"));
        };
        let command_id = self
            .status
            .active_command_id
            .clone()
            .filter(|_| self.status.active_turn_id.as_deref() == Some(pending.turn_id.as_str()))
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "OpenCode native request matched no exact active input",
                )
            })?;
        let native_id = event_id(
            &session_id,
            &pending.turn_id,
            &pending.id,
            "request.opened",
            "opencode",
            "OpenCode",
        )?;
        let Ok((public_kind, public_details)) = public_request_view(&pending) else {
            return Err(Error::new(
                "unknown",
                "OpenCode public request view could not be normalized",
            ));
        };
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let committed = redact_request_details_for_commit(&public_details, &secrets);
        let guard_commands = (pending.kind == "approval"
            && pending.details["action"].as_str().is_some_and(|action| {
                action.eq_ignore_ascii_case("shell") || action.eq_ignore_ascii_case("bash")
            }))
        .then(|| {
            pending.details["resources"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|commands| !commands.is_empty());
        let mut data = json!({
            "command_id": command_id,
            "request_id": pending.id,
            "turn_id": pending.turn_id,
            "kind": public_kind,
            "details": committed,
        });
        if let Some(commands) = guard_commands {
            data[eliot_core::COMMAND_GUARD_CONTEXT_FIELD] = json!({"commands": commands});
        }
        let refusal = self
            .stream_source()
            .commit_request_event(native_id, data)
            .await?;
        if refusal.as_ref().is_some_and(|refusal| {
            refusal.request_id != pending.id || refusal.turn_id != pending.turn_id
        }) {
            return Err(Error::new(
                "unknown",
                "OpenCode guard refusal did not bind to its exact permission request",
            ));
        }
        let request_id = pending.id.clone();
        let turn_id = pending.turn_id.clone();
        self.pending_requests.insert(
            request_id.clone(),
            PendingNativeRequest {
                request: pending,
                reply_command_id: None,
                reply_state: NativeReplyState::Available,
                reply_is_cancel: false,
            },
        );
        if let Some(refusal) = refusal {
            self.refuse_guarded_permission(&request_id, &turn_id, &refusal.message)
                .await?;
        }
        Ok(())
    }

    /// Reject one guarded Bash permission only when OpenCode readback proves
    /// this is the session's sole pending permission: native `reject` closes
    /// every pending permission in the session.
    async fn refuse_guarded_permission(
        &mut self,
        request_id: &str,
        turn_id: &str,
        message: &str,
    ) -> Result<()> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Err(Error::new(
                "unknown",
                "OpenCode guard refusal has no owned session",
            ));
        };
        if self.status.active_turn_id.as_deref() != Some(turn_id) {
            return Err(Error::new(
                "unknown",
                "OpenCode guard refusal lost its exact turn",
            ));
        }
        let listed = match self
            .http
            .list_permissions(&session_id, Instant::now() + REPLY_DEADLINE)
            .await
        {
            Ok(listed) => listed,
            Err(_) => {
                self.record_gap(
                    "OpenCode guard refusal was withheld because pending permissions could not be read back",
                )
                .await?;
                return Ok(());
            }
        };
        if listed.len() != 1 || listed[0].id != request_id {
            self.record_gap(
                "OpenCode guard refusal was withheld because native reject would close sibling permissions",
            )
            .await?;
            return Ok(());
        }
        let Some(pending) = self.pending_requests.get_mut(request_id) else {
            return Err(Error::new(
                "unknown",
                "OpenCode guarded permission was not retained",
            ));
        };
        if pending.request.turn_id != turn_id || pending.reply_state != NativeReplyState::Available
        {
            return Err(Error::new(
                "unknown",
                "OpenCode guarded permission binding changed",
            ));
        }
        pending.reply_state = NativeReplyState::Writing;
        match self
            .http
            .reply_permission(
                &session_id,
                request_id,
                "reject",
                Some(message.to_owned()),
                Instant::now() + REPLY_DEADLINE,
            )
            .await
        {
            Ok(()) => {
                if let Some(pending) = self.pending_requests.get_mut(request_id) {
                    pending.reply_state = NativeReplyState::Written;
                    pending.reply_command_id = None;
                }
            }
            Err(_) => {
                if let Some(pending) = self.pending_requests.get_mut(request_id) {
                    pending.reply_state = NativeReplyState::Unknown;
                    pending.reply_command_id = None;
                }
                self.record_gap(
                    "OpenCode guard refusal reply outcome is unresolved and will not be repeated",
                )
                .await?;
            }
        }
        Ok(())
    }

    /// `permission.replied`: settle the retained open request. Our own
    /// written reply resolves with its command binding; anything else (the
    /// owner, or a sibling swept up by a `reject`, which the installed
    /// server settles alongside) closes without a command binding. Unknown
    /// ids ignore: the ask predates our subscription or never opened.
    async fn settle_permission(&mut self, data: &Value) -> Result<()> {
        let replied: PermissionRepliedData =
            serde_json::from_value(data.clone()).map_err(|_| {
                Error::new(
                    "native_protocol",
                    "OpenCode permission reply omitted its exact identity",
                )
            })?;
        if self.status.session_id.as_deref() != Some(replied.session_id.as_str()) {
            return Ok(());
        }
        self.settle_request(&replied.request_id).await
    }

    /// `form.replied`: settle under the same rules as a permission reply.
    async fn settle_form(&mut self, data: &Value) -> Result<()> {
        let replied: FormRepliedData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode form reply omitted its exact identity",
            )
        })?;
        if self.status.session_id.as_deref() != Some(replied.session_id.as_str()) {
            return Ok(());
        }
        self.settle_request(&replied.id).await
    }

    /// `form.cancelled`: settle under the same rules as a form reply (the
    /// native cancel may be ours, the owner's, or the dead execution's).
    /// Our own cancel write still closes without a command binding: a
    /// cancel is not an answer.
    async fn cancel_form(&mut self, data: &Value) -> Result<()> {
        let cancelled: FormCancelledData = serde_json::from_value(data.clone()).map_err(|_| {
            Error::new(
                "native_protocol",
                "OpenCode form cancellation omitted its exact identity",
            )
        })?;
        if self.status.session_id.as_deref() != Some(cancelled.session_id.as_str()) {
            return Ok(());
        }
        self.settle_request(&cancelled.id).await
    }

    /// Settle one retained open request from its native settle event.
    async fn settle_request(&mut self, native_id: &str) -> Result<()> {
        let Some(pending) = self.pending_requests.remove(native_id) else {
            return Ok(());
        };
        let Some(session_id) = self.status.session_id.clone() else {
            self.pending_requests.insert(native_id.to_owned(), pending);
            return Err(Error::new("unknown", "OpenCode session is unresolved"));
        };
        let turn_id = pending.request.turn_id.clone();
        if pending.reply_state == NativeReplyState::Written
            && !pending.reply_is_cancel
            && let Some(command_id) = pending.reply_command_id.clone()
        {
            let native_event = event_id(
                &session_id,
                &turn_id,
                native_id,
                "request.resolved",
                "opencode",
                "OpenCode",
            )?;
            self.commit_event(
                "request.resolved",
                native_event,
                json!({
                    "command_id": command_id,
                    "request_id": native_id,
                    "turn_id": turn_id,
                }),
            )
            .await?;
        } else {
            let native_event = event_id(
                &session_id,
                &turn_id,
                native_id,
                "request.closed",
                "opencode",
                "OpenCode",
            )?;
            self.commit_event(
                "request.closed",
                native_event,
                json!({
                    "request_id": native_id,
                    "turn_id": turn_id,
                }),
            )
            .await?;
        }
        self.remember_resolved(native_id);
        Ok(())
    }

    /// Drop this turn's retained offers at turn end. `turn.completed`
    /// clears the Eliot pendings; the native offers drop with them.
    fn drop_turn_requests(&mut self, turn_id: &str) {
        self.pending_requests
            .retain(|_, pending| pending.request.turn_id != turn_id);
    }

    fn remember_resolved(&mut self, native_id: &str) {
        if self.resolved_native_ids.insert(native_id.to_owned()) {
            self.resolved_native_order.push_back(native_id.to_owned());
        }
        while self.resolved_native_order.len() > MAX_RESOLVED_REQUESTS {
            if let Some(evicted) = self.resolved_native_order.pop_front() {
                self.resolved_native_ids.remove(&evicted);
            }
        }
    }

    /// Stage and write one native reply. Guards bind the command to the
    /// exact retained offer; the write runs once per request, ever: 204
    /// applies and the native settle event resolves, 400/404/409 prove
    /// non-admission and reject (a gone target also closes the retained
    /// offer), and a lost reply settles unknown and is never rewritten.
    async fn reply_request(
        &mut self,
        command_id: &str,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
    ) -> Result<Value> {
        let pending = self.pending_requests.get(request_id).ok_or_else(|| {
            Error::new("not_found", "OpenCode native request is no longer pending")
        })?;
        if pending.request.turn_id != turn_id {
            return Err(Error::new(
                "stale_turn",
                "OpenCode reply named a finished turn",
            ));
        }
        match pending.reply_state {
            NativeReplyState::Available => {}
            NativeReplyState::Written | NativeReplyState::Unknown => {
                return Err(Error::new(
                    "rejected",
                    "OpenCode native request already has a native decision",
                ));
            }
            NativeReplyState::Writing => {
                return Err(Error::new(
                    "busy",
                    "OpenCode native request reply is already running",
                ));
            }
        }
        let staged = match reply_for(&pending.request, verb, args) {
            Ok(staged) => staged,
            // A staging failure is a rejected reply, never an error: the
            // caller's choice did not match the offered set, no write ran,
            // and the lane stays available for the offered answer.
            Err(error) => {
                return Ok(json!({
                    "state": "rejected",
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                    "reason": format!("OpenCode reply was not admitted: {}", error.message),
                }));
            }
        };
        let pending = self
            .pending_requests
            .get_mut(request_id)
            .expect("retained offer survives staging");
        pending.reply_state = NativeReplyState::Writing;
        pending.reply_command_id = Some(command_id.to_owned());
        pending.reply_is_cancel = matches!(staged.route, ReplyRoute::FormCancel);
        let outcome = self.write_staged_reply(&staged).await;
        self.finish_reply_write(request_id, command_id, turn_id, outcome)
            .await
    }

    /// Perform the one native write a staged reply names.
    async fn write_staged_reply(
        &self,
        staged: &StagedReply,
    ) -> std::result::Result<(), HttpOutcome> {
        let Some(session_id) = self.status.session_id.clone() else {
            return Err(HttpOutcome::Unknown {
                reason: "OpenCode session is unresolved".to_owned(),
            });
        };
        let deadline = Instant::now() + REPLY_DEADLINE;
        match &staged.route {
            ReplyRoute::Permission { decision, message } => {
                self.http
                    .reply_permission(
                        &session_id,
                        &staged.native_id,
                        decision,
                        message.clone(),
                        deadline,
                    )
                    .await
            }
            ReplyRoute::Form { answer } => {
                self.http
                    .reply_form(&session_id, &staged.native_id, answer.clone(), deadline)
                    .await
            }
            ReplyRoute::FormCancel => {
                self.http
                    .delete_form(&session_id, &staged.native_id, deadline)
                    .await
            }
        }
    }

    /// Settle the reply lane after its one write: applied on 204, rejected
    /// on proven non-admission (closing the retained offer when the target
    /// is gone), unknown on anything ambiguous — including a provably-unsent
    /// (`NotDispatched`) write, which latches unknown like a lost reply.
    async fn finish_reply_write(
        &mut self,
        request_id: &str,
        command_id: &str,
        turn_id: &str,
        outcome: std::result::Result<(), HttpOutcome>,
    ) -> Result<Value> {
        match outcome {
            Ok(()) => {
                if let Some(pending) = self.pending_requests.get_mut(request_id) {
                    pending.reply_state = NativeReplyState::Written;
                }
                Ok(json!({
                    "state": "applied",
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                }))
            }
            Err(HttpOutcome::Rejected { status, reason }) => {
                let gone = matches!(status, Some(404) | Some(409));
                if gone {
                    self.close_gone_request(request_id).await?;
                } else if let Some(pending) = self.pending_requests.get_mut(request_id) {
                    pending.reply_state = NativeReplyState::Available;
                    pending.reply_command_id = None;
                    pending.reply_is_cancel = false;
                }
                Ok(json!({
                    "state": "rejected",
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                    "reason": reason,
                }))
            }
            Err(HttpOutcome::Unknown { reason }) => {
                self.latch_reply_unknown(request_id);
                Ok(json!({
                    "state": "unknown",
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                    "reason": reason,
                }))
            }
            Err(HttpOutcome::NotDispatched { reason }) => {
                self.latch_reply_unknown(request_id);
                Ok(json!({
                    "state": "unknown",
                    "command_id": command_id,
                    "request_id": request_id,
                    "turn_id": turn_id,
                    "reason": reason,
                }))
            }
        }
    }

    /// Latch a reply lane unknown after an ambiguous write — or a
    /// provably-unsent one (`NotDispatched` latches unknown too): the lane
    /// never reopens, so the reply is never written again for this request.
    fn latch_reply_unknown(&mut self, request_id: &str) {
        if let Some(pending) = self.pending_requests.get_mut(request_id) {
            pending.reply_state = NativeReplyState::Unknown;
        }
    }

    /// Close a retained offer the native side proved gone (404/409 on
    /// reply): nobody can answer it anymore, so it closes without a
    /// command binding instead of rotting open.
    async fn close_gone_request(&mut self, request_id: &str) -> Result<()> {
        let Some(pending) = self.pending_requests.remove(request_id) else {
            return Ok(());
        };
        let Some(session_id) = self.status.session_id.clone() else {
            self.pending_requests.insert(request_id.to_owned(), pending);
            return Err(Error::new("unknown", "OpenCode session is unresolved"));
        };
        let native_event = event_id(
            &session_id,
            &pending.request.turn_id,
            request_id,
            "request.closed",
            "opencode",
            "OpenCode",
        )?;
        self.commit_event(
            "request.closed",
            native_event,
            json!({
                "request_id": request_id,
                "turn_id": pending.request.turn_id,
            }),
        )
        .await?;
        self.remember_resolved(request_id);
        Ok(())
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.stream_source().record_gap(reason).await
    }

    async fn record_source_gap_once(&mut self, reason: &str) -> Result<()> {
        if !self.gap_reported {
            self.record_gap(reason).await?;
        }
        Ok(())
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

fn compaction_usage_echo(data: &Value) -> Option<eliot_core::CompactionUsageEcho> {
    let tokens = &data["tokens"];
    Some(eliot_core::CompactionUsageEcho {
        cost: data["cost"].as_f64()?,
        input_tokens: tokens["input"].as_f64()?,
        output_tokens: tokens["output"].as_f64()?,
        reasoning_tokens: tokens["reasoning"].as_f64()?,
        cache_read_tokens: tokens["cache"]["read"].as_f64()?,
        cache_write_tokens: tokens["cache"]["write"].as_f64()?,
    })
}

fn optional_terminal_readback(result: Result<TurnReadback>) -> Result<Option<TerminalReadback>> {
    match result {
        Ok(TurnReadback::Terminal(readback)) => Ok(Some(*readback)),
        Ok(TurnReadback::Running) => Err(Error::new(
            "unknown",
            "OpenCode terminal readback found no terminal idle message",
        )),
        Err(error) if error.code == "native_readback" => Ok(None),
        Err(error) => Err(error),
    }
}

fn terminal_turn_data(
    command_id: &str,
    turn_id: &str,
    status: &str,
    terminal: &ExecutionEventData,
    readback: Option<&TerminalReadback>,
    terminal_event_created_ms: Option<i64>,
    secrets: &SecretPolicy,
) -> Value {
    let mut data = json!({
        "command_id": command_id,
        "turn_id": turn_id,
        "status": status,
    });
    if let Some(usage) = readback.and_then(|readback| readback.usage.as_ref()) {
        data["usage"] = serde_json::to_value(usage).unwrap_or(Value::Null);
    }
    let native_error = terminal
        .error
        .as_ref()
        .or_else(|| readback.and_then(|readback| readback.idle_error.as_ref()));
    attach_native_error(
        &mut data,
        native_error.map(|error| error.message.as_str()),
        secrets,
    );
    // OpenCode 2.2.7 resolves the requested model before the first inference
    // step. Its no-route class also covers unrelated setup errors, so require
    // the model resolver's specific error on the execution terminal itself.
    if terminal.error.as_ref().is_some_and(|error| {
        error.error_type == "provider.no-route" && error.message.starts_with("Model unavailable: ")
    }) {
        data["model_unavailable"] = Value::Bool(true);
    }
    if status == "failed" {
        let origin = native_error
            .map(|error| error.failure_origin())
            .unwrap_or(eliot_core::FailureOrigin::Unknown);
        if let Some(restriction) = native_error
            .and_then(|error| terminal_provider_restriction(error, terminal_event_created_ms))
        {
            eliot_plugin::session::attach_provider_restriction(&mut data, restriction, secrets);
        }
        eliot_plugin::session::attach_failure_origin(
            &mut data,
            "opencode",
            origin,
            if origin == eliot_core::FailureOrigin::Provider
                && native_error.is_some_and(|error| error.status.is_some())
            {
                "Session.StructuredError provider type with upstream HTTP error status"
            } else if origin == eliot_core::FailureOrigin::Provider {
                "Session.StructuredError exact provider error type"
            } else {
                "native terminal lacks a structured upstream failure status"
            },
            secrets,
        );
    }
    if let Some(text) = readback.and_then(|readback| readback.final_text.as_deref()) {
        attach_final_text(&mut data, &secrets.redact_text(text));
    }
    data
}

fn terminal_provider_restriction(
    error: &crate::native::types::StructuredError,
    terminal_event_created_ms: Option<i64>,
) -> Option<ProviderRestriction> {
    let kind = match error.error_type.as_str() {
        "provider.rate-limit" => ProviderRestrictionKind::RateLimit,
        "provider.quota"
        | "provider.free-limit"
        | "provider.chatgpt-limit"
        | "provider.go-limit"
        | "provider.budget-limit"
        | "provider.model-budget-limit" => ProviderRestrictionKind::Quota,
        _ => return None,
    };
    let retry_after = error.retry_after_ms.and_then(|delay_ms| {
        let reset_base_ms = terminal_event_created_ms
            .filter(|created_ms| *created_ms >= 0)
            .or_else(current_unix_time_ms)?;
        Some(reset_base_ms.saturating_add(delay_ms))
    });
    Some(ProviderRestriction {
        kind,
        retry_after,
        code: Some(error.error_type.clone()),
        http_status: error.status,
    })
}

fn current_unix_time_ms() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
}

/// Terminal readback: page the newest-first message list (bounded) and
/// find our user message plus the newest `idle` message after it. Final
/// text is the concatenated assistant text parts strictly between the
/// two in newest-first page order, reversed to chronological order
/// with each message's native `content` array order preserved (page
/// position decides; timestamps never reorder it). Usage sums distinct
/// assistant messages in that same exact user-to-selected-idle slice.
/// The newest such message also supplies a per-call context count when input
/// and both cache counters are complete.
/// A healthy read with the exact user message and no newer idle proves
/// running. A missing user message, invalid history or HTTP failure does
/// not establish either state.
pub(crate) async fn read_turn_messages(
    http: &OpenCodeHttp,
    session_id: &str,
    inbox_id: &str,
    deadline: Instant,
) -> Result<TurnReadback> {
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut user_position: Option<usize> = None;
    let mut idle: Option<(String, String, Option<StructuredError>, usize)> = None;
    let mut assistant_messages: Vec<(usize, Vec<String>)> = Vec::new();
    let mut usages: HashMap<String, (usize, Option<AssistantTokens>)> = HashMap::new();
    let mut position: usize = 0;
    for _ in 0..MAX_MESSAGE_PAGES {
        let page = http
            .list_messages(session_id, cursor.as_deref(), deadline)
            .await?;
        if page.data.is_empty() {
            break;
        }
        for message in &page.data {
            match message {
                Message::User(user) if user.id == inbox_id => {
                    user_position = Some(position);
                    break;
                }
                Message::Assistant(assistant) => {
                    let mut message_text = Vec::new();
                    for part in &assistant.content {
                        match part {
                            AssistantContent::Text(text) => {
                                if let Some(body) = text.text.as_deref() {
                                    message_text.push(body.to_owned());
                                }
                            }
                            AssistantContent::Tool => message_text.clear(),
                            AssistantContent::Other => {}
                        }
                    }
                    assistant_messages.push((position, message_text));
                    usages
                        .entry(assistant.id.clone())
                        .or_insert_with(|| (position, assistant.tokens.clone()));
                }
                Message::Idle(idle_message) if idle.is_none() => {
                    // Newest-first: the first idle seen before our
                    // user message is the newest idle after it.
                    idle = Some((
                        idle_message.id.clone(),
                        idle_message.outcome.clone(),
                        idle_message.error.clone(),
                        position,
                    ));
                }
                _ => {}
            }
            position += 1;
        }
        if user_position.is_some() {
            break;
        }
        match page.cursor.next {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                cursor = Some(next)
            }
            Some(_) => {
                return Err(Error::new(
                    "native_readback",
                    "OpenCode message cursor did not advance",
                ));
            }
            None => break,
        }
    }
    let Some(user_position) = user_position else {
        return Err(Error::new(
            "unknown",
            "OpenCode terminal readback found no exact user message",
        ));
    };
    let Some((idle_id, outcome, idle_error, idle_position)) = idle else {
        return Ok(TurnReadback::Running);
    };
    // The newest assistant message strictly between our input and selected
    // idle supplies final_text; earlier messages remain separate events.
    let final_text = project_terminal_text(assistant_messages, idle_position, user_position);
    Ok(TurnReadback::Terminal(Box::new(TerminalReadback {
        idle_id,
        outcome: match outcome.as_str() {
            "succeeded" => "completed".to_owned(),
            "failed" => "failed".to_owned(),
            "interrupted" => "interrupted".to_owned(),
            _ => {
                return Err(Error::new(
                    "unknown",
                    "OpenCode idle message has an unknown outcome",
                ));
            }
        },
        idle_error,
        final_text,
        usage: main_loop_usage(&usages, idle_position, user_position),
        context_used_tokens: latest_call_context_tokens(&usages, idle_position, user_position),
    })))
}

pub(crate) enum TurnReadback {
    Running,
    Terminal(Box<TerminalReadback>),
}

pub(crate) struct TerminalReadback {
    idle_id: String,
    pub outcome: String,
    pub idle_error: Option<StructuredError>,
    pub final_text: Option<String>,
    usage: Option<TurnUsage>,
    context_used_tokens: Option<i64>,
}

/// Project only the newest assistant message in the exact user-to-idle
/// slice. Content parts before its final tool call are excluded, and a
/// newer textless assistant message prevents fallback to an earlier reply.
fn project_terminal_text(
    messages: Vec<(usize, Vec<String>)>,
    idle_position: usize,
    user_position: usize,
) -> Option<String> {
    let (_, parts) = messages
        .into_iter()
        .filter(|(position, _)| *position > idle_position && *position < user_position)
        .min_by_key(|(position, _)| *position)?;
    let final_text = parts.join("\n");
    if final_text.is_empty() {
        None
    } else {
        Some(final_text)
    }
}

/// A counter is proven only when every assistant message in the slice
/// supplies it. The Store normalizes these vendor counters on read.
fn main_loop_usage(
    usages: &HashMap<String, (usize, Option<AssistantTokens>)>,
    idle_position: usize,
    user_position: usize,
) -> Option<TurnUsage> {
    let messages = usages.values().map(|(position, tokens)| {
        let usage = tokens
            .as_ref()
            .map_or_else(TurnUsage::default, |tokens| TurnUsage {
                input_tokens: tokens.input,
                output_tokens: tokens.output,
                reasoning_tokens: tokens.reasoning,
                cache_read_tokens: tokens.cache.as_ref().and_then(|cache| cache.read),
                cache_write_tokens: tokens.cache.as_ref().and_then(|cache| cache.write),
                ..TurnUsage::default()
            });
        (*position, usage)
    });
    eliot_plugin::usage::summarize_newest_first_usage(
        "opencode",
        "main_loop_turn",
        idle_position,
        user_position,
        messages,
    )
}

/// The newest assistant message in the exact input-to-idle slice supplies one
/// per-call context count only when all disjoint counters are present.
fn latest_call_context_tokens(
    usages: &HashMap<String, (usize, Option<AssistantTokens>)>,
    idle_position: usize,
    user_position: usize,
) -> Option<i64> {
    let (_, tokens) = usages
        .values()
        .filter(|entry| entry.0 > idle_position && entry.0 < user_position)
        .min_by_key(|entry| entry.0)?;
    let tokens = tokens.as_ref()?;
    let cache = tokens.cache.as_ref()?;
    [
        tokens.input,
        cache.read,
        cache.write,
        tokens.output,
        tokens.reasoning,
    ]
    .into_iter()
    .try_fold(0i64, |total, count| {
        total.checked_add(count.filter(|value| *value >= 0)?)
    })
}

/// One Store claim plus one vendor resolve for a bound hook. The Store
/// claim commits before the vendor resolve; the resolve runs exactly once
/// and is never retried on an uncertain outcome. Only a confirmed
/// `applied`/`empty` vendor status produces a receipt; expiry, cancel,
/// mismatch, closure, transport loss and claim failure produce none.
async fn run_context_claim(
    http: OpenCodeHttp,
    host: PeerClient,
    credential: String,
    hook: EliotContextHook,
    turn_id: String,
    consumer_id: String,
    deadline: Instant,
) -> ContextHookDone {
    let hook_id = hook.hook_id.clone();
    let claimed = match claim_context_for_hook(&host, &credential, &turn_id, &consumer_id, deadline)
        .await
    {
        Ok(claimed) => claimed,
        Err(_) => {
            // No Store claim: one cancel unblocks the vendor hook; no
            // receipt and no applied claim ever follow.
            let input = EliotContextResolveInput {
                hook_id: hook.hook_id.clone(),
                session_id: hook.session_id.clone(),
                message_id: hook.message_id.clone(),
                context: None,
                cancel: Some(true),
            };
            let _ = http
                .eliot_context_resolve(input, Instant::now() + std::time::Duration::from_secs(30))
                .await;
            return ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed: Vec::new(),
                outcome: HookResolveOutcome::NoReceipt,
            };
        }
    };
    if claimed.is_empty() {
        let input = EliotContextResolveInput {
            hook_id: hook.hook_id.clone(),
            session_id: hook.session_id.clone(),
            message_id: hook.message_id.clone(),
            context: None,
            cancel: None,
        };
        return match http.eliot_context_resolve(input, deadline).await {
            Ok(output) if output.status == "empty" => ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed,
                outcome: HookResolveOutcome::Empty,
            },
            _ => ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed,
                outcome: HookResolveOutcome::NoReceipt,
            },
        };
    }
    let formatted = match with_claimed_context("", &claimed, "OpenCode") {
        Ok(text) => text.strip_suffix("\n\n").unwrap_or(&text).to_owned(),
        Err(_) => {
            let input = EliotContextResolveInput {
                hook_id: hook.hook_id.clone(),
                session_id: hook.session_id.clone(),
                message_id: hook.message_id.clone(),
                context: None,
                cancel: None,
            };
            let _ = http
                .eliot_context_resolve(input, Instant::now() + std::time::Duration::from_secs(30))
                .await;
            return ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed,
                outcome: HookResolveOutcome::NoReceipt,
            };
        }
    };
    if formatted.trim().is_empty() {
        let input = EliotContextResolveInput {
            hook_id: hook.hook_id.clone(),
            session_id: hook.session_id.clone(),
            message_id: hook.message_id.clone(),
            context: None,
            cancel: None,
        };
        return match http.eliot_context_resolve(input, deadline).await {
            Ok(output) if output.status == "empty" => ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed: Vec::new(),
                outcome: HookResolveOutcome::Empty,
            },
            _ => ContextHookDone {
                hook_id,
                turn_id,
                consumer_id,
                claimed: Vec::new(),
                outcome: HookResolveOutcome::NoReceipt,
            },
        };
    }
    let input = EliotContextResolveInput {
        hook_id: hook.hook_id.clone(),
        session_id: hook.session_id.clone(),
        message_id: hook.message_id.clone(),
        context: Some(formatted),
        cancel: None,
    };
    match http.eliot_context_resolve(input, deadline).await {
        Ok(output) if output.status == "applied" => ContextHookDone {
            hook_id,
            turn_id,
            consumer_id,
            claimed,
            outcome: HookResolveOutcome::Applied,
        },
        _ => ContextHookDone {
            hook_id,
            turn_id,
            consumer_id,
            claimed,
            outcome: HookResolveOutcome::NoReceipt,
        },
    }
}

/// Claim exactly this callback's queued context through the authenticated
/// private hook channel. `hook=before_model` with the captured turn and
/// this consumer's own ULID: Store commits the FIFO claim before the
/// vendor resolve runs.
async fn claim_context_for_hook(
    host: &PeerClient,
    credential: &str,
    turn_id: &str,
    consumer_id: &str,
    deadline: Instant,
) -> Result<Vec<ContextItem>> {
    let event = HookEvent {
        hook: "before_model".to_owned(),
        turn_id: turn_id.to_owned(),
        consumer_id: consumer_id.to_owned(),
    };
    let mut params = serde_json::to_value(&event)
        .map_err(|_| Error::new("internal", "OpenCode hook claim could not be encoded"))?;
    let Some(params_object) = params.as_object_mut() else {
        return Err(Error::new(
            "internal",
            "OpenCode hook claim could not be encoded",
        ));
    };
    // Same private IPC envelope as the harness callbacks: stripped by the
    // host before the shared DTO is decoded, never persisted.
    params_object.insert(
        "credential".to_owned(),
        serde_json::Value::String(credential.to_owned()),
    );
    let response = host.call("hook.event", params, deadline).await?;
    let claimed: HookResponse = serde_json::from_value(response)
        .map_err(|_| Error::new("invalid", "OpenCode hook claim response was malformed"))?;
    Ok(claimed.context)
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
        let terminal_deadline = processor.terminal_deadline;
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
            _ = async {
                match terminal_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(error) = processor.expire_terminal_deadline().await {
                    processor.status.source_unknown = true;
                    processor.status.failure.get_or_insert(*error.message);
                }
            }
            changed = stream.changed(), if accept_frames => {
                let current = match changed {
                    Ok(()) => *stream.borrow_and_update(),
                    Err(_) => StreamState::Closed,
                };
                processor.handle_stream_state(current).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            message = async {
                match processor.context_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(message) = message {
                    processor.handle_context_msg(message).await;
                }
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(control).await {
                            break;
                        }
                        if processor.close_frames {
                            frames.close();
                            processor.close_frames = false;
                        }
                        processor.report_gap_if_queue_empty(frames.is_empty()).await;
                    }
                    None => break,
                }
            }
        }
        processor.expire_pending_hooks();
        processor.status.native_drained = processor.frames_drained;
        let _ = status_tx.send(processor.status.clone());
    }
    processor.abort_context_hooks();
    // Keep the host/client alive until the task ends; dropping them here
    // would close shared connection state early.
    let _ = (&processor.host, &processor.http);
}
