use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, Result, SecretPolicy, SessionUsage,
    SourceHealthUpdate,
    requests::{ApprovalArgs, ApprovalChoice, ApprovalDecision, ApprovalRequest},
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, MAX_FRAME_BYTES, PeerClient,
    native_transport::{
        NativeStreamFault, ProcessBirthIdentity, StreamState, process_birth_boundary,
        process_birth_identity,
    },
    protocol::{HookEvent, HookResponse},
    session::{
        EVENT_DEADLINE, StreamSource, attach_final_text, event_id, is_blank_native_text,
        next_input_outcomes, rejected_control, request_event_id, unknown_control,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::Instant,
};

use crate::{
    hooks::{
        HookCallback, HookInput, HookOutput, HookServer, HookStep, IpcQueryResponse,
        MAX_HOOK_FRAME_BYTES,
    },
    native::{
        AcpUpdate, AgyClient, AgyFrame, FencedGuard, FencedKind, PermissionAsk, PermissionOption,
        PermissionOptionKind, PromptResult, PromptTerminalStatus, ToolDiffPaths, classify_update,
        default_permission_option, parse_permission_request, permission_option_grants,
        prompt_turn_usage, session_usage_from_update,
    },
};

mod checkpoints;
use ulid::Ulid;

pub(crate) const FAMILIES: &[&str] = &["session", "turn", "message", "tool", "request"];
pub(crate) const STEER_MECHANISM: &str = "antigravity.PreInvocation.injectSteps.systemMessage";
pub(crate) type Shared = Arc<Mutex<State>>;

/// Maximum redacted assistant text retained for a final response. If the
/// response grows beyond this suffix buffer, the shared terminal bound marks
/// its bounded value with the truncation marker.
const MAX_TURN_TEXT_BYTES: usize = 256 * 1024;
const MAX_UNBOUND_UPDATES: usize = 32;
const MAX_UNBOUND_UPDATE_BYTES: usize = 256 * 1024;
const MAX_TOOL_ID_BYTES: usize = 96;
const MAX_TOOL_TITLE_BYTES: usize = 2048;
const MAX_LIVE_TOOLS: usize = 64;
const MAX_BACKGROUND_TOOLS: usize = 64;
const MAX_STOP_REASON_BYTES: usize = 64;
const MAX_STOP_ERROR_BYTES: usize = 2_048;
const NATIVE_TRIGGER: &str = "PreInvocation";

fn stop_status(reason: &str, has_error: bool) -> &'static str {
    if has_error || reason.eq_ignore_ascii_case("error") {
        return "failed";
    }
    if ["no_tool_call", "model_stop", "max_steps_exceeded"]
        .iter()
        .any(|known| reason.eq_ignore_ascii_case(known))
    {
        "completed"
    } else {
        "unknown"
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ToolStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Unknown,
    Other,
}

impl ToolStatus {
    fn parse(value: &str) -> Self {
        match value {
            "pending" => Self::Pending,
            "in_progress" => Self::InProgress,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => Self::Other,
        }
    }
}

struct NativeToolState {
    title: String,
    status: ToolStatus,
    native_file_edit_tool: Option<bool>,
    edit_kind: bool,
    diff_paths: ToolDiffPaths,
}

/// Native file mutation tool names documented by the AGY CLI.
const NATIVE_FILE_EDIT_TOOLS: &[&str] = &[
    "write_file",
    "write_to_file",
    "replace_file_content",
    "multi_replace_file_content",
];

pub(crate) type LoadOutcome = (String, Result<Value>);

struct QueuedUpdate {
    session_id: String,
    update: AcpUpdate,
}

#[derive(Clone, Serialize)]
pub(crate) struct TurnFacts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    pub turn_id: String,
    pub status: String,
    /// The raw ACP `stopReason` of the terminal `session/prompt` reply.
    pub native_status: String,
    /// Empty while no bounded reply text is known; never presented as one.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub final_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_error: Option<Value>,
    /// Why no reply text is presented (native_update_gap or
    /// empty_native_response), the same marker the terminal write carries,
    /// so a recovered terminal keeps it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_text_unavailable: Option<&'static str>,
    pub terminal: bool,
}

struct Pending {
    /// Absent for a self-started Antigravity run.
    command: Option<String>,
    turn: String,
    opened_at: u64,
    context: Vec<ContextItem>,
    associated: bool,
    tool_gap_reported: bool,
    reply: Option<oneshot::Sender<Result<()>>>,
}

struct NativeStop {
    turn_id: String,
    status: &'static str,
    native_status: String,
    error: Option<Value>,
    readback: bool,
}

struct UnknownNativeTurn {
    turn_id: String,
    opened_at: u64,
}

struct PendingSteer {
    command_id: String,
    turn_id: String,
    text: String,
    reply: oneshot::Sender<Result<Value>>,
}

/// Upper bound on concurrently open native permission asks. Beyond this
/// the ask is refused and gapped, exactly like the sibling harnesses.
const MAX_PENDING_REQUESTS: usize = 16;

/// Delivery state of one pending permission answer. A reply that may
/// have reached the native is never rewritten; a proven no-write keeps
/// the ask answerable.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NativeReplyState {
    Available,
    Writing,
    Unknown,
}

/// One open `session/request_permission` ask: the exact native round
/// (verbatim JSON-RPC id plus offered options) bound to its Swarm turn.
/// The public `request.opened` carries only the redacted summary plus
/// the shared approval choices; the native transport binding stays here.
struct PendingNativeRequest {
    turn_id: String,
    native_id: Value,
    tool_call_id: String,
    options: Vec<PermissionOption>,
    reply_command_id: Option<String>,
    reply_state: NativeReplyState,
}

impl PendingNativeRequest {
    fn begin_cancel(&mut self, command_id: &str) -> Option<Value> {
        if self.reply_state != NativeReplyState::Available {
            return None;
        }
        self.reply_state = NativeReplyState::Writing;
        self.reply_command_id = Some(command_id.to_owned());
        Some(self.native_id.clone())
    }
}

struct CancellationIntent {
    turn_id: String,
    command_id: String,
}

/// Why a terminal presents no reply text: native output was gapped, or
/// the native ended the turn normally but supplied no assistant text at all.
/// Consumers distinguish the latter from a real reply by the absent
/// message.completed plus this marker.
fn reply_unavailable(
    reason: Option<&'static str>,
    status: &str,
    text: &str,
) -> Option<&'static str> {
    reason.or_else(|| {
        (status == "completed" && is_blank_native_text(text)).then_some("empty_native_response")
    })
}

/// The ACP `cancelled` permission outcome: the prompt turn ended before
/// the user responded. Used for cancelling interrupt/stop answers and
/// for safe fallbacks that must never grant anything.
fn cancelled_permission_outcome() -> Value {
    json!({"outcome": {"outcome": "cancelled"}})
}

/// Least-privilege fallback for an ask that can never be presented: a
/// one-time refusal when the offer names one (else a standing refusal),
/// else `cancelled`. Never selects a grant.
fn fallback_permission_outcome(ask: &PermissionAsk) -> Value {
    let selected = ask
        .options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::RejectOnce)
        .or_else(|| {
            ask.options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::RejectAlways)
        })
        .map(|option| option.option_id.clone());
    match selected {
        Some(option_id) => json!({"outcome": {"outcome": "selected", "optionId": option_id}}),
        None => cancelled_permission_outcome(),
    }
}

#[derive(Clone)]
struct HookTurnBinding {
    conversation: String,
    turn: String,
    generation: i64,
    prompt_opened_at: u64,
    origin: ProcessBirthIdentity,
}

struct AcceptedHook {
    callback: HookCallback,
    binding: Option<HookTurnBinding>,
}

pub(crate) struct State {
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent: String,
    generation: i64,
    policy: SecretPolicy,
    pub conversation: String,
    workspace: String,
    pending: Option<Pending>,
    native_stop: Option<NativeStop>,
    stop_barrier_request: Option<String>,
    stop_barrier_guard: Option<FencedGuard>,
    unknown_native_turn: Option<UnknownNativeTurn>,
    session_ready: bool,
    pending_steer: Option<PendingSteer>,
    /// Lifecycle transitions already emitted per tool call, keyed
    /// `"{tool_call_id}:{kind}"`. Each transition is emitted once;
    /// later metadata deltas never re-emit.
    tool_emitted: HashSet<String>,
    /// Tool calls whose successful terminal event was committed in this turn.
    tool_completed_successfully: HashSet<String>,
    tool_edit_gap_reported: bool,
    /// Exact native diff paths already represented by a committed edit fact.
    tool_file_edit_emitted: HashSet<(String, String)>,
    /// Partial tool-call deltas merged per vendor id before any event is
    /// derived. Updates may omit any field, so absence alone is never a
    /// protocol gap.
    tools: HashMap<String, NativeToolState>,
    /// AGY calls left open at turn end become native background tasks.
    background_tools: HashMap<String, String>,
    /// RAII registration of the outstanding fenced prompt call. Exactly
    /// one owner (this state); clearing it retires both transport maps.
    /// Held until the actual in-band terminal reply, an error, a stream
    /// fault, stop, or drop — stream silence alone never expires it.
    fenced_guard: Option<FencedGuard>,
    /// Exact fenced `session/load` request whose ordered reply is pending.
    /// Matching-session updates in this window are validated history and
    /// discarded, never republished.
    load_request_id: Option<String>,
    /// RAII registration of the outstanding fenced load call.
    load_guard: Option<FencedGuard>,
    /// Ordered load outcome for `Session::start`, published exactly once
    /// by the event task when the fenced load reply is consumed.
    load_tx: watch::Sender<Option<LoadOutcome>>,
    /// Updates received before `session/new` returns its session id. They
    /// are validated and applied in wire order once that exact id is known.
    unbound_updates: VecDeque<QueuedUpdate>,
    unbound_update_bytes: usize,
    text_accum: String,
    assistant_message_seq: u64,
    text_redaction_pending: Vec<u8>,
    text_truncated: bool,
    text_unavailable: Option<&'static str>,
    idle_message_gap_reported: bool,
    idle_tool_gap_reported: bool,
    writes_blocked: bool,
    gap_reported: bool,
    gap_number: u64,
    stream_loss_reported: bool,
    /// Open native permission asks by distinct local request id. The ask stays open
    /// (and the Agent waiting) until an explicit approve/deny reply, a
    /// cancelling interrupt/stop, or the turn closing it.
    pending_requests: HashMap<String, PendingNativeRequest>,
    /// Retained until a new input is prepared; never applies to a later turn.
    cancellation: Option<CancellationIntent>,
    pub failure: Option<String>,
    pub source_unknown: bool,
    pub batch_unknown: bool,
    pub stopping: bool,
    pub stopped: bool,
    pub last_turn: Option<TurnFacts>,
    /// Terminal settlement of the most recent prepared turn:
    /// `(turn_id, status)` where status is the mapped turn status or
    /// `"unknown"`. Interrupt waits on this instead of trusting its own
    /// notification write.
    settlement: watch::Sender<Option<(String, bool)>>,
    /// Latest merged session-scoped snapshot from `usage_update`: context
    /// occupancy/capacity and cumulative session cost. Never turn expenditure.
    session_usage: Option<SessionUsage>,
    /// Exact pending turn that supplied the latest sample, if any.
    session_usage_turn: Option<String>,
    checkpoints: Option<checkpoints::Checkpoints>,
    checkpoint_root: Option<std::path::PathBuf>,
    checkpoint_resume: bool,
}

impl State {
    pub fn live_turn_for_command(&self, command_id: &str) -> Option<&str> {
        self.pending
            .as_ref()
            .filter(|pending| pending.command.as_deref() == Some(command_id))
            .map(|pending| pending.turn.as_str())
            .or_else(|| {
                self.pending_steer
                    .as_ref()
                    .filter(|pending| pending.command_id == command_id)
                    .map(|pending| pending.turn_id.as_str())
            })
    }

    pub fn latest_terminal_turn_for_command(&self, command_id: &str) -> Option<&str> {
        self.last_turn
            .as_ref()
            .filter(|turn| turn.command_id.as_deref() == Some(command_id) && turn.terminal)
            .map(|turn| turn.turn_id.as_str())
    }

    pub fn new(
        host: PeerClient,
        credential: String,
        agent: String,
        generation: i64,
        policy: SecretPolicy,
        conversation: String,
        workspace: String,
    ) -> Self {
        let (load_tx, _) = watch::channel(None);
        let (settlement, _) = watch::channel(None);
        Self {
            sink: CommitSink::new(host.clone(), credential.clone()),
            host,
            credential,
            agent,
            generation,
            policy,
            conversation,
            workspace,
            pending: None,
            native_stop: None,
            stop_barrier_request: None,
            stop_barrier_guard: None,
            unknown_native_turn: None,
            session_ready: false,
            pending_steer: None,
            tool_emitted: HashSet::new(),
            tool_completed_successfully: HashSet::new(),
            tool_edit_gap_reported: false,
            tool_file_edit_emitted: HashSet::new(),
            tools: HashMap::new(),
            background_tools: HashMap::new(),
            fenced_guard: None,
            load_request_id: None,
            load_guard: None,
            load_tx,
            unbound_updates: VecDeque::new(),
            unbound_update_bytes: 0,
            text_accum: String::new(),
            assistant_message_seq: 0,
            text_redaction_pending: Vec::new(),
            text_truncated: false,
            text_unavailable: None,
            idle_message_gap_reported: false,
            idle_tool_gap_reported: false,
            writes_blocked: false,
            gap_reported: false,
            gap_number: 0,
            stream_loss_reported: false,
            pending_requests: HashMap::new(),
            cancellation: None,
            failure: None,
            source_unknown: false,
            batch_unknown: false,
            stopping: false,
            stopped: false,
            last_turn: None,
            settlement,
            session_usage: None,
            session_usage_turn: None,
            checkpoints: None,
            checkpoint_root: None,
            checkpoint_resume: false,
        }
    }

    pub fn subscribe_settlement(&self) -> watch::Receiver<Option<(String, bool)>> {
        self.settlement.subscribe()
    }

    fn record_settlement(&self, turn: String, cancelled: bool) {
        self.settlement.send_replace(Some((turn, cancelled)));
    }

    fn has_settlement_for(&self, turn: &str) -> bool {
        self.settlement
            .borrow()
            .as_ref()
            .is_some_and(|(settled, _)| settled == turn)
    }

    /// Bind the exact session identity, then validate and apply any updates
    /// that arrived before the `session/new` reply in their original order.
    pub async fn bind_conversation(&mut self, conversation: &str) -> Result<()> {
        if conversation.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Antigravity session id is empty",
            ));
        }
        if self.source_unknown || self.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Antigravity session identity is unresolved",
            ));
        }
        if !self.conversation.is_empty() && self.conversation != conversation {
            return Err(Error::new(
                "source_integrity",
                "Antigravity session identity changed during startup",
            ));
        }
        self.conversation = conversation.to_owned();
        let queued = std::mem::take(&mut self.unbound_updates);
        self.unbound_update_bytes = 0;
        for queued in queued {
            self.apply_update(queued.session_id, queued.update).await?;
        }
        Ok(())
    }

    pub fn bind_checkpoint_transcript(
        &mut self,
        root: Option<&std::path::Path>,
        checkpoint_root: Option<&std::path::Path>,
        resumed: bool,
    ) {
        self.checkpoint_resume = resumed;
        self.checkpoint_root = checkpoint_root.map(std::path::Path::to_path_buf);
        self.checkpoints =
            root.and_then(|root| checkpoints::Checkpoints::bind(root, &self.conversation, resumed));
    }

    /// Bind the retained session and arm both the exact load filter and
    /// its outcome watcher before the caller starts the native write.
    pub async fn arm_load(
        &mut self,
        request_id: String,
        conversation: &str,
    ) -> Result<watch::Receiver<Option<LoadOutcome>>> {
        self.bind_conversation(conversation).await?;
        if self.load_request_id.is_some() {
            return Err(Error::new(
                "bad_state",
                "Antigravity already has a pending load call",
            ));
        }
        self.load_tx.send_replace(None);
        let receiver = self.load_tx.subscribe();
        self.load_request_id = Some(request_id);
        Ok(receiver)
    }

    /// Take the registration only while its exact load remains pending.
    /// A reply consumed during the native write clears the id first, so a
    /// late returned guard is dropped immediately here.
    pub fn attach_load_guard(&mut self, request_id: &str, guard: FencedGuard) {
        if self.failure.is_none()
            && !self.source_unknown
            && self.load_request_id.as_deref() == Some(request_id)
        {
            self.load_guard = Some(guard);
        }
    }

    /// Retire a load that never reached the ordered reply (dispatch
    /// failure or start timeout): loading mode ends without history
    /// having been validated, so startup fails instead of going live.
    pub fn abort_load(&mut self, request_id: &str) {
        if self.load_request_id.as_deref() == Some(request_id) {
            self.load_request_id = None;
            self.load_guard = None;
        }
    }

    /// Take ownership of the fenced prompt registration — but only if
    /// the expected prepared turn is still pending and live. The fenced
    /// reply may have settled (or the stream may have been lost) while
    /// the dispatch write was in flight; in that case the guard is
    /// retired immediately by drop. The associated oneshot already
    /// proves association, so a confirmed terminal settlement is never
    /// downgraded by a late guard.
    pub fn arm_fenced(&mut self, guard: FencedGuard, expected_turn: &str) {
        let live = self.failure.is_none()
            && !self.source_unknown
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.turn == expected_turn);
        if live {
            self.fenced_guard = Some(guard);
        }
    }

    fn clear_fenced(&mut self) {
        self.fenced_guard = None;
    }

    pub fn active_turn(&self) -> Option<&str> {
        self.pending.as_ref().map(|p| p.turn.as_str())
    }

    pub fn native_run_is(&self, turn_id: &str) -> bool {
        self.pending.as_ref().is_some_and(|pending| {
            pending.command.is_none() && pending.turn == turn_id
        })
    }

    pub fn native_unknown_turn_is(&self, turn_id: &str) -> bool {
        self.unknown_native_turn
            .as_ref()
            .is_some_and(|unknown| unknown.turn_id == turn_id)
    }

    pub(crate) async fn settle_unanswered_cancel(&mut self, turn_id: &str) -> Result<()> {
        if !self.native_run_is(turn_id) || self.failure.is_some() {
            return Ok(());
        }
        let opened_at = self
            .pending
            .as_ref()
            .filter(|pending| pending.turn == turn_id)
            .map(|pending| pending.opened_at)
            .ok_or_else(|| Error::new("stale_turn", "Antigravity native turn is no longer live"))?;
        if let Some(stop) = self.native_stop.as_mut() {
            if stop.turn_id != turn_id {
                return Ok(());
            }
            // A hook without its ordered reply remains only readback evidence.
            stop.readback = true;
        }
        self.close_pending_requests(Some(turn_id)).await?;
        self.commit(
            "turn.unknown",
            turn_id,
            "unknown",
            json!({
                "turn_id": turn_id,
                "reason": "Antigravity reported no terminal after session/cancel",
            }),
        )
        .await?;
        self.reject_pending_steer("Antigravity run has no native terminal after its cancel");
        self.close_open_tools_at_terminal().await?;
        self.unknown_native_turn = Some(UnknownNativeTurn {
            turn_id: turn_id.to_owned(),
            opened_at,
        });
        self.pending = None;
        Ok(())
    }

    /// True while `turn_id` is the exact prepared turn of `command_id`.
    pub fn pending_is(&self, command_id: &str, turn_id: &str) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| {
                pending.command.as_deref() == Some(command_id) && pending.turn == turn_id
            })
    }

    /// True while the exact turn's fenced `session/prompt` call awaits its
    /// terminal reply on a healthy stream.
    pub fn prompt_outstanding(&self, command_id: &str, turn_id: &str) -> bool {
        self.fenced_guard.is_some()
            && self.pending_is(command_id, turn_id)
            && self.failure.is_none()
            && !self.source_unknown
            && !self.batch_unknown
            && !self.writes_blocked
    }

    pub fn contains_secret(&self, text: &str) -> bool {
        self.policy.contains_text(text)
    }

    fn capture_hook_turn(
        &self,
        input: &HookInput,
        origin: ProcessBirthIdentity,
    ) -> Option<HookTurnBinding> {
        if input.is_stop()
            || self.conversation != input.conversation_id
            || input.conversation_id.trim().is_empty()
        {
            return None;
        }
        if self.failure.is_some()
            || self.source_unknown
            || self.batch_unknown
            || self.writes_blocked
            || self.stopping
            || self.stopped
        {
            return None;
        }
        let pending = self.pending.as_ref()?;
        if origin.start_time <= pending.opened_at {
            return None;
        }
        Some(HookTurnBinding {
            conversation: input.conversation_id.clone(),
            turn: pending.turn.clone(),
            generation: self.generation,
            prompt_opened_at: pending.opened_at,
            origin,
        })
    }

    fn hook_turn_is_live(&self, conversation: &str, turn: &str) -> bool {
        self.conversation == conversation
            && self.failure.is_none()
            && !self.source_unknown
            && !self.batch_unknown
            && !self.writes_blocked
            && !self.stopping
            && !self.stopped
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.turn == turn)
    }

    fn hook_binding_is_live(&self, binding: &HookTurnBinding) -> bool {
        binding.generation == self.generation
            && self.hook_turn_is_live(&binding.conversation, &binding.turn)
            && self.pending.as_ref().is_some_and(|pending| {
                pending.turn == binding.turn && pending.opened_at == binding.prompt_opened_at
            })
    }

    pub fn stop_order_pending(&self) -> bool {
        self.native_stop.is_some() && self.stop_barrier_request.is_some()
    }

    async fn begin_native_run(
        &mut self,
        input: &HookInput,
        origin: ProcessBirthIdentity,
    ) -> Result<Option<HookTurnBinding>> {
        if !input.starts_execution()
            || input.conversation_id != self.conversation
            || !self.session_ready
            || self.load_request_id.is_some()
            || self.failure.is_some()
            || self.source_unknown
            || self.batch_unknown
            || self.writes_blocked
            || self.stopping
            || self.stopped
            || self.pending.is_some()
            || self.native_stop.is_some()
            || self.unknown_native_turn.is_some()
        {
            return Ok(None);
        }
        self.idle_message_gap_reported = false;
        self.idle_tool_gap_reported = false;
        self.cancellation = None;
        self.tool_emitted.clear();
        self.tool_completed_successfully.clear();
        self.tool_edit_gap_reported = false;
        self.tool_file_edit_emitted.clear();
        self.tools.clear();
        self.clear_fenced();
        self.reset_assistant_text();
        self.assistant_message_seq = 0;
        let turn = format!("agy-{}", Ulid::new());
        self.pending = Some(Pending {
            command: None,
            turn: turn.clone(),
            opened_at: origin.start_time,
            context: Vec::new(),
            associated: true,
            tool_gap_reported: false,
            reply: None,
        });
        if let Err(error) = self
            .commit(
                "turn.started",
                &turn,
                "started",
                json!({"turn_id": turn.clone(), "native_trigger": NATIVE_TRIGGER}),
            )
            .await
        {
            self.pending = None;
            return Err(error);
        }
        Ok(Some(HookTurnBinding {
            conversation: self.conversation.clone(),
            turn,
            generation: self.generation,
            prompt_opened_at: origin.start_time,
            origin,
        }))
    }

    async fn observe_stop(
        &mut self,
        input: &HookInput,
        origin: ProcessBirthIdentity,
        client: &AgyClient,
    ) -> Result<()> {
        if !input.is_stop()
            || input.conversation_id != self.conversation
            || self.failure.is_some()
            || self.source_unknown
            || self.batch_unknown
            || self.writes_blocked
            || self.stopping
            || self.stopped
            || self.native_stop.is_some()
        {
            return Ok(());
        }
        let (turn_id, readback) = match self.pending.as_ref() {
            Some(pending)
                if pending.command.is_none() && origin.start_time > pending.opened_at =>
            {
                (pending.turn.clone(), false)
            }
            _ => match self.unknown_native_turn.as_ref() {
                Some(unknown) if origin.start_time > unknown.opened_at => {
                    (unknown.turn_id.clone(), true)
                }
                _ => return Ok(()),
            },
        };
        let reason = self.policy.redact_bounded(
            input.termination_reason.as_deref().unwrap_or_default(),
            MAX_STOP_REASON_BYTES,
        );
        let native_status = if reason.trim().is_empty() {
            "unknown".to_owned()
        } else {
            reason
        };
        let error = input
            .error
            .as_deref()
            .map(str::trim)
            .filter(|error| !error.is_empty())
            .map(|error| {
                json!({
                    "message": self.policy.redact_bounded(error, MAX_STOP_ERROR_BYTES),
                    "truncated": error.len() > MAX_STOP_ERROR_BYTES,
                })
            });
        let status = stop_status(&native_status, error.is_some());
        let prepared = client.prepare_fenced(
            "session/list",
            json!({"sessionId": self.conversation}),
            FencedKind::StopBarrier,
        )?;
        let request_id = prepared.request_id().to_owned();
        let (write, guard) = prepared.split();
        self.native_stop = Some(NativeStop {
            turn_id,
            status,
            native_status,
            error,
            readback,
        });
        self.stop_barrier_request = Some(request_id);
        self.stop_barrier_guard = Some(guard);
        client
            .dispatch_fenced_write(write, Instant::now() + EVENT_DEADLINE)
            .await?;
        Ok(())
    }

    async fn stop_barrier_passed(
        &mut self,
        request_id: &str,
        result: Result<Value>,
    ) -> Result<()> {
        if self.stop_barrier_request.as_deref() != Some(request_id) {
            return Err(Error::new(
                "source_integrity",
                "Antigravity Stop barrier did not match its exact request",
            ));
        }
        self.stop_barrier_request = None;
        self.stop_barrier_guard = None;
        if let Err(error) = result {
            return Err(error.with_code("unknown"));
        }
        let stop = self
            .native_stop
            .take()
            .ok_or_else(|| Error::new("source_integrity", "Antigravity Stop fact disappeared"))?;
        if stop.readback {
            self.finish_assistant_text();
            let final_text = if self.text_unavailable.is_none() {
                self.policy.redact_text(&self.text_accum)
            } else {
                String::new()
            };
            self.last_turn = Some(TurnFacts {
                command_id: None,
                turn_id: stop.turn_id,
                status: stop.status.to_owned(),
                native_status: stop.native_status,
                final_text_unavailable: reply_unavailable(
                    self.text_unavailable,
                    stop.status,
                    &final_text,
                ),
                final_text,
                native_error: stop.error,
                terminal: true,
            });
            return Ok(());
        }
        self.settle_native_stop(stop).await
    }

    async fn settle_native_stop(&mut self, stop: NativeStop) -> Result<()> {
        if self
            .pending
            .as_ref()
            .is_none_or(|pending| pending.command.is_some() || pending.turn != stop.turn_id)
        {
            return Err(Error::new(
                "stale_turn",
                "Antigravity Stop does not match the exact self-started turn",
            ));
        }
        self.finish_assistant_text();
        let observed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok());
        let restriction = (self.text_unavailable.is_none()
            && !self.text_truncated
            && matches!(stop.status, "completed" | "failed"))
        .then(|| crate::quota::turn_restriction(&self.text_accum, observed_at.unwrap_or(0)))
        .flatten()
        .map(|mut restriction| {
            if observed_at.is_none() {
                restriction.retry_after = None;
            }
            restriction
        });
        let status = if restriction.is_some() { "failed" } else { stop.status };
        self.last_turn = Some(TurnFacts {
            command_id: None,
            turn_id: stop.turn_id.clone(),
            status: status.to_owned(),
            native_status: stop.native_status.clone(),
            final_text: String::new(),
            native_error: stop.error.clone(),
            final_text_unavailable: reply_unavailable(
                self.text_unavailable,
                status,
                &self.text_accum,
            ),
            terminal: true,
        });
        self.close_pending_requests(Some(&stop.turn_id)).await?;
        self.close_open_tools_at_terminal().await?;
        let text = self.policy.redact_text(&self.text_accum);
        let missing_text = self.text_unavailable;
        let outcome = match status {
            "completed" => "success",
            "failed" => "failure",
            _ => "unknown",
        };
        let mut data = json!({
            "turn_id": stop.turn_id.clone(),
            "status": status,
            "outcome": outcome,
            "native_status": stop.native_status.clone(),
        });
        if let Some(error) = stop.error.clone() {
            data["native_error"] = error;
        }
        if let Some(restriction) = restriction {
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "antigravity",
                eliot_core::FailureOrigin::Provider,
                "captured Antigravity native quota warning",
                &self.policy,
            );
            data["native_error"] = json!({"code":"quota_exhausted", "message":"provider quota exhausted"});
            eliot_plugin::session::attach_provider_restriction(
                &mut data,
                restriction,
                &self.policy,
            );
        } else if status == "failed" {
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "antigravity",
                eliot_core::FailureOrigin::Harness,
                "Antigravity Stop hook reported a failure",
                &self.policy,
            );
        }
        self.policy.redact_values(&mut data);
        if missing_text.is_none() && status != "failed" {
            self.commit_assistant_message(&text).await?;
        }
        if let Some(marker) = reply_unavailable(missing_text, status, &text) {
            data["final_text_unavailable"] = json!(marker);
        }
        if (self.text_truncated || !is_blank_native_text(&text)) && missing_text.is_none() {
            attach_final_text(&mut data, &text);
        }
        self.commit("turn.completed", &stop.turn_id, "completed", data.clone())
            .await?;
        let session_usage = self.commit_turn_session_usage(&stop.turn_id).await;
        self.reject_pending_steer("Antigravity run ended before its next PreInvocation");
        if let Some(last) = self.last_turn.as_mut() {
            last.final_text = data
                .get("final_text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
        }
        self.record_settlement(stop.turn_id, false);
        self.pending = None;
        self.unknown_native_turn = None;
        session_usage
    }

    pub fn arm_steer(
        &mut self,
        command_id: String,
        turn_id: String,
        text: String,
    ) -> Result<oneshot::Receiver<Result<Value>>> {
        if self.pending_steer.is_some() {
            return Err(Error::new(
                "busy",
                "Antigravity already has a steer waiting for PreInvocation",
            ));
        }
        if !self.hook_turn_is_live(&self.conversation, &turn_id) {
            return Err(Error::new(
                "stale_turn",
                "Antigravity steer does not match the exact active prompt",
            ));
        }
        let (reply, receiver) = oneshot::channel();
        self.pending_steer = Some(PendingSteer {
            command_id,
            turn_id,
            text,
            reply,
        });
        Ok(receiver)
    }

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

    fn defer_pending_steer(&mut self, steer: PendingSteer) -> Result<()> {
        if self.pending_steer.is_some() {
            return Err(Error::new(
                "busy",
                "Antigravity already has a steer waiting for PreInvocation",
            ));
        }
        if !self.hook_turn_is_live(&self.conversation, &steer.turn_id) {
            let _ = steer.reply.send(Err(Error::new(
                "stale_turn",
                "Antigravity prompt ended before PreInvocation confirmed steer",
            )));
            return Err(Error::new(
                "stale_turn",
                "Antigravity prompt ended before PreInvocation confirmed steer",
            ));
        }
        self.pending_steer = Some(steer);
        Ok(())
    }

    pub fn cancel_pending_steer(&mut self) {
        if let Some(steer) = self.pending_steer.take() {
            let _ = steer.reply.send(Err(Error::new(
                "unknown",
                "Antigravity stopped before PreInvocation confirmed steer",
            )));
        }
    }

    fn fail_pending_steer(&mut self, error: Error) {
        if let Some(steer) = self.pending_steer.take() {
            let _ = steer.reply.send(Err(error));
        }
    }

    fn reject_pending_steer(&mut self, reason: &str) {
        if let Some(steer) = self.pending_steer.take() {
            let outcome = json!({
                "state": "rejected",
                "command_id": steer.command_id,
                "generation": self.generation,
                "turn_id": steer.turn_id,
                "mechanism": STEER_MECHANISM,
                "native_hook": "PreInvocation",
                "reason_code": "stale_turn",
                "reason": reason,
            });
            let _ = steer.reply.send(Ok(outcome));
        }
    }

    /// Register correlation before the write. This local turn id is never
    /// sent to AGY or presented as a vendor-supplied id.
    pub fn prepare(
        &mut self,
        command: String,
        turn: String,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        if self.source_unknown || self.failure.is_some() {
            return Err(Error::new("unknown", "Antigravity stream is unresolved"));
        }
        if self.native_stop.is_some() || self.unknown_native_turn.is_some() {
            return Err(Error::new("unknown", "Antigravity native turn is unresolved"));
        }
        if self.stopping || self.stopped {
            return Err(Error::new("bad_state", "Antigravity session is stopping"));
        }
        if self.pending.is_some() {
            return Err(Error::new("busy", "Antigravity has one outstanding input"));
        }
        let opened_at = process_birth_boundary()?;
        let (reply, result) = oneshot::channel();
        self.pending = Some(Pending {
            command: Some(command),
            turn,
            opened_at,
            context: Vec::new(),
            associated: false,
            tool_gap_reported: false,
            reply: Some(reply),
        });
        self.idle_message_gap_reported = false;
        self.idle_tool_gap_reported = false;
        self.cancellation = None;
        self.tool_emitted.clear();
        self.tool_completed_successfully.clear();
        self.tool_edit_gap_reported = false;
        self.tool_file_edit_emitted.clear();
        self.tools.clear();
        self.clear_fenced();
        self.reset_assistant_text();
        self.assistant_message_seq = 0;
        Ok(result)
    }

    /// Carry the send's FIFO-claimed context into the pending input. Called
    /// under the same lock as [`State::prepare`], before the native write;
    /// refused once the input already associated, so outcomes are never
    /// attached late or faked.
    pub fn attach_context(&mut self, command: &str, context: Vec<ContextItem>) -> Result<()> {
        match self.pending.as_mut() {
            Some(pending)
                if pending.command.as_deref() == Some(command) && !pending.associated =>
            {
                pending.context = context;
                Ok(())
            }
            _ => Err(Error::new(
                "unknown",
                "Antigravity input association is unresolved",
            )),
        }
    }

    pub fn withdraw_unwritten(&mut self, command: &str) -> Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.command.as_deref() == Some(command) && !p.associated)
        {
            self.clear_fenced();
            self.pending = None;
            return Ok(());
        }
        Err(Error::new(
            "unknown",
            "Antigravity emitted a turn despite an unwritten input",
        ))
    }

    pub async fn ready(&mut self, scope: &str, effective: Value) -> Result<()> {
        if self.source_unknown || self.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Antigravity session state is unresolved before startup",
            ));
        }
        self.commit(
            "session.ready",
            "session",
            "ready",
            json!({
                "native_scope": scope, "native_ref": self.conversation, "effective": effective,
            }),
        )
        .await?;
        self.session_ready = true;
        Ok(())
    }

    /// Bind the prepared input to its turn by committing `turn.started`.
    /// Called only by the ordered native event owner on correlated progress,
    /// a permission ask or the fenced terminal response, never by the writer.
    pub(crate) async fn associate(&mut self) -> Result<()> {
        let pending = self.pending.as_ref().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "Antigravity output has no outstanding input",
            )
        })?;
        if pending.associated {
            return Ok(());
        }
        let turn = pending.turn.clone();
        let mut data = json!({"turn_id": turn});
        if let Some(command) = pending.command.as_deref() {
            data["command_id"] = json!(command);
        }
        if !pending.context.is_empty()
            && let Some(command) = pending.command.as_deref()
        {
            data["context_outcomes"] =
                json!(next_input_outcomes(&pending.context, command));
        }
        let result = self.commit("turn.started", &turn, "started", data).await;
        if let Some(pending) = self.pending.as_mut() {
            pending.associated = result.is_ok();
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(result.clone());
            }
        }
        result
    }

    /// Open one native `session/request_permission` ask as a Swarm
    /// approval request bound to the exact generation, turn and tool
    /// call. Never auto-answers: the native outcome is written only by
    /// an explicit approve/deny reply or a cancelling interrupt/stop, so
    /// an unanswered ask keeps the turn busy (and waiting) until the
    /// owner, the manager, or a cancel resolves it.
    async fn open_permission_request(
        &mut self,
        client: &AgyClient,
        native_id: Value,
        params: &Value,
    ) -> Result<()> {
        let Some(ask) = parse_permission_request(params) else {
            return self
                .gap_unanswerable_request(
                    client,
                    native_id,
                    "Antigravity permission ask did not match its envelope",
                )
                .await;
        };
        let bound = self
            .pending
            .as_ref()
            .map(|pending| (pending.command.clone(), pending.turn.clone()));
        let session_ok = !self.conversation.is_empty()
            && params.get("sessionId").and_then(Value::as_str) == Some(self.conversation.as_str());
        let Some((command_id, turn_id)) = bound else {
            return self
                .stray_permission_request(
                    client,
                    native_id,
                    Some(&ask),
                    "Antigravity permission ask arrived with no prepared input",
                )
                .await;
        };
        if !session_ok {
            return self
                .stray_permission_request(
                    client,
                    native_id,
                    Some(&ask),
                    "Antigravity permission ask did not match the exact session",
                )
                .await;
        }
        if !self.valid_tool_id(&ask.tool_call_id, &turn_id) {
            return self
                .stray_permission_request(
                    client,
                    native_id,
                    Some(&ask),
                    "Antigravity permission tool correlation is invalid",
                )
                .await;
        }
        if let Some(entry) = self
            .pending_requests
            .values()
            .find(|entry| entry.native_id == native_id)
        {
            return if entry.turn_id == turn_id && entry.tool_call_id == ask.tool_call_id {
                Ok(())
            } else {
                Err(Error::new(
                    "source_integrity",
                    "Antigravity native request id changed binding",
                ))
            };
        }
        for native_tag in &ask.unclassified_option_kinds {
            self.record_unclassified("session.request_permission.option.kind", native_tag)
                .await?;
        }
        if self.pending_requests.len() >= MAX_PENDING_REQUESTS {
            return self
                .stray_permission_request(
                    client,
                    native_id,
                    Some(&ask),
                    "Antigravity pending native request bound was exceeded",
                )
                .await;
        }
        if !ask
            .options
            .iter()
            .any(|option| permission_option_grants(&option.kind).is_some())
        {
            return self
                .gap_unanswerable_request(
                    client,
                    native_id,
                    "Antigravity permission offer carried no usable option",
                )
                .await;
        }
        let summary = self.permission_summary(&ask);
        let choices: Vec<ApprovalChoice> = ask
            .options
            .iter()
            .filter_map(|option| {
                let decision = match permission_option_grants(&option.kind)? {
                    true => ApprovalDecision::Approve,
                    false => ApprovalDecision::Deny,
                };
                Some(ApprovalChoice {
                    id: option.option_id.clone(),
                    label: option.name.clone(),
                    decision,
                    feedback_allowed: false,
                })
            })
            .collect();
        let details = serde_json::to_value(&ApprovalRequest {
            summary,
            choices,
            feedback_allowed: false,
            guard_refusal: None,
        })
        .map_err(|_| {
            Error::new(
                "internal",
                "Antigravity approval details could not be encoded",
            )
        })?;
        let mut data = json!({
            "turn_id": turn_id,
            "kind": "approval",
            "details": details,
        });
        if let Some(command_id) = command_id {
            data["command_id"] = json!(command_id);
        }
        if (ask.tool_kind.as_deref() == Some("execute")
            || ask.tool_name.as_deref() == Some("run_command"))
            && let Some(command) = ask
                .raw_input
                .get("command")
                .or_else(|| ask.raw_input.get("CommandLine"))
                .unwrap_or(&ask.raw_input)
                .as_str()
        {
            data[eliot_core::COMMAND_GUARD_CONTEXT_FIELD] = json!({"commands": [command]});
        }
        let publication = self
            .publish_permission_request(
                data,
                PendingNativeRequest {
                    turn_id: turn_id.clone(),
                    native_id: native_id.clone(),
                    tool_call_id: ask.tool_call_id.clone(),
                    options: ask.options.clone(),
                    reply_command_id: None,
                    reply_state: NativeReplyState::Available,
                },
            )
            .await;
        if publication
            .as_ref()
            .is_err_and(|error| error.code == "frame_too_large")
        {
            return self
                .stray_permission_request(
                    client,
                    native_id,
                    Some(&ask),
                    "Antigravity normalized request exceeded the persisted-event bound",
                )
                .await;
        }
        publication?;
        let known_tool = self.tools.contains_key(&ask.tool_call_id);
        if known_tool || self.tools.len() < MAX_LIVE_TOOLS {
            self.merge_tool(
                &ask.tool_call_id,
                &ask.title,
                if known_tool { None } else { Some("pending") },
                ask.tool_kind.as_deref(),
                ask.tool_name.as_deref(),
                &ToolDiffPaths::default(),
            );
        } else {
            self.record_unattributed_tool().await?;
        }
        if let Some(command_id) = self.cancellation_for_pending().map(str::to_owned) {
            self.answer_pending_cancelled(client, &command_id, Some(&turn_id))
                .await;
        }
        Ok(())
    }

    async fn publish_permission_request(
        &mut self,
        mut data: Value,
        entry: PendingNativeRequest,
    ) -> Result<()> {
        if !self.valid_tool_id(&entry.tool_call_id, &entry.turn_id) {
            return Err(Error::new(
                "native_protocol",
                "Antigravity permission tool correlation is invalid",
            ));
        }
        if self.pending.as_ref().is_none_or(|pending| {
            pending.turn != entry.turn_id
                || data["turn_id"] != pending.turn
                || data.get("command_id").and_then(Value::as_str)
                    != pending.command.as_deref()
        }) {
            return Err(Error::new(
                "source_integrity",
                "Antigravity permission turn binding changed",
            ));
        }
        let request_id = format!("agy-request-{}", Ulid::new());
        data["request_id"] = json!(request_id);
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity normalized request exceeded its bound",
            ));
        }
        let native_event = request_event_id(&request_id, "request.opened", "agy", "Antigravity")?;
        // The session and pending binding were validated before publication.
        // A native ask is itself evidence that AGY accepted this input.
        self.associate().await?;
        self.source()
            .commit_event("request.opened", native_event, data)
            .await?;
        self.pending_requests.insert(request_id, entry);
        Ok(())
    }

    pub fn latch_cancellation(&mut self, command_id: &str, turn_id: Option<&str>) {
        if let Some(pending) = self.pending.as_ref()
            && turn_id.is_none_or(|turn| turn == pending.turn)
        {
            self.cancellation = Some(CancellationIntent {
                turn_id: pending.turn.clone(),
                command_id: command_id.to_owned(),
            });
        }
    }

    fn cancellation_for_pending(&self) -> Option<&str> {
        self.cancellation.as_ref().and_then(|intent| {
            self.pending
                .as_ref()
                .filter(|pending| pending.turn == intent.turn_id)
                .map(|_| intent.command_id.as_str())
        })
    }

    /// Compose the public approval summary from the decision-relevant
    /// operation only (title plus tool name/kind and compact raw input,
    /// as plain text, never the native record), redacted and bounded.
    /// Native transport ids stay private in the retained pending entry.
    fn permission_summary(&self, ask: &PermissionAsk) -> String {
        let mut parts = vec![ask.title.clone()];
        if let Some(name) = ask.tool_name.as_deref().filter(|name| !name.is_empty()) {
            parts.push(format!("tool: {name}"));
        }
        if let Some(kind) = ask.tool_kind.as_deref().filter(|kind| !kind.is_empty()) {
            parts.push(format!("kind: {kind}"));
        }
        let raw = match &ask.raw_input {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            Value::Number(_) | Value::Bool(_) => ask.raw_input.to_string(),
            Value::Array(_) | Value::Object(_) => {
                serde_json::to_string(&ask.raw_input).unwrap_or_default()
            }
        };
        let raw = self.policy.redact_text(&raw);
        let raw: String = raw.chars().take(2048).collect();
        if !raw.trim().is_empty() {
            parts.push(format!("input: {raw}"));
        }
        self.policy.redact_text(&parts.join("; "))
    }

    /// Safe fallback for an ask that reached the native but can never be
    /// presented: record the gap, then answer the least-privilege outcome
    /// (a one-time refusal when the offer names one, else `cancelled`)
    /// so the turn ends visibly instead of hanging on an undecidable ask.
    /// Never grants anything.
    async fn stray_permission_request(
        &mut self,
        client: &AgyClient,
        native_id: Value,
        ask: Option<&PermissionAsk>,
        reason: &str,
    ) -> Result<()> {
        let mut source = self.source();
        let _ = source
            .record_gap_until(reason, Instant::now() + EVENT_DEADLINE)
            .await;
        let outcome = ask.map(fallback_permission_outcome);
        client
            .respond_permission(
                native_id,
                outcome.unwrap_or_else(cancelled_permission_outcome),
                Instant::now() + EVENT_DEADLINE,
            )
            .await
    }

    /// Safe fallback for a malformed ask: record the gap, then answer
    /// `cancelled` (no grant; the turn ends visibly instead of hanging).
    async fn gap_unanswerable_request(
        &mut self,
        client: &AgyClient,
        native_id: Value,
        reason: &str,
    ) -> Result<()> {
        let mut source = self.source();
        let _ = source
            .record_gap_until(reason, Instant::now() + EVENT_DEADLINE)
            .await;
        client
            .respond_permission(
                native_id,
                cancelled_permission_outcome(),
                Instant::now() + EVENT_DEADLINE,
            )
            .await
    }

    /// Decide one open permission: validate the exact binding, then write
    /// the ACP outcome for the explicit approve/deny verb and commit the
    /// exact `request.resolved`. The write has no transport ack, so a
    /// queued write is the delivery; only a proven no-write keeps the ask
    /// available, anything uncertain latches unknown without a rewrite.
    pub async fn reply_request(
        &mut self,
        command_id: &str,
        request_id: &str,
        turn_id: &str,
        verb: &str,
        args: &Value,
        client: &AgyClient,
    ) -> Result<Value> {
        if command_id.trim().is_empty() || request_id.trim().is_empty() || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Antigravity request reply requires exact command, request, and turn ids",
            ));
        }
        let (turn, native_id, options) = match self.pending_requests.get(request_id) {
            Some(entry) => (
                entry.turn_id.clone(),
                entry.native_id.clone(),
                entry.options.clone(),
            ),
            None => {
                return Ok(rejected_control("Antigravity request is no longer pending"));
            }
        };
        if turn != turn_id {
            return Ok(rejected_control(
                "Antigravity request reply does not match the captured turn",
            ));
        }
        if self
            .pending
            .as_ref()
            .is_none_or(|pending| pending.turn != turn_id)
            || self.has_settlement_for(turn_id)
        {
            return Ok(rejected_control(
                "Antigravity request is no longer attached to the active turn",
            ));
        }
        match self
            .pending_requests
            .get(request_id)
            .map(|entry| entry.reply_state)
        {
            Some(NativeReplyState::Writing) | Some(NativeReplyState::Unknown) => {
                return Ok(unknown_control(
                    "Antigravity request reply outcome is unresolved and will not be repeated",
                ));
            }
            _ => {}
        }
        if self.conversation.is_empty()
            || self.stopped
            || self.stopping
            || self.cancellation_for_pending().is_some()
        {
            return Ok(rejected_control(
                "Antigravity request session is no longer writable",
            ));
        }
        if self.source_unknown || self.failure.is_some() {
            self.retain_unknown_reply(request_id, command_id);
            return Ok(unknown_control(
                "Antigravity request session outcome is unresolved; it will not be repeated",
            ));
        }
        if !matches!(verb, "approve" | "deny") {
            return Ok(rejected_control(
                "Antigravity request reply verb is not implemented",
            ));
        }
        let parsed = match ApprovalArgs::parse(args) {
            Ok(parsed) => parsed,
            Err(_) => {
                return Ok(rejected_control(
                    "Antigravity approval reply args are invalid",
                ));
            }
        };
        if parsed.feedback.is_some() {
            return Ok(rejected_control(
                "Antigravity approvals do not accept feedback",
            ));
        }
        let approve = verb == "approve";
        let selected = match parsed.choice_id.as_deref() {
            Some(choice) => match options.iter().find(|option| option.option_id == choice) {
                Some(found) if permission_option_grants(&found.kind) == Some(approve) => found,
                _ => {
                    return Ok(rejected_control(
                        "Antigravity approval choice does not match the reply verb",
                    ));
                }
            },
            None => match default_permission_option(&options, approve) {
                Some(found) => found,
                None => {
                    return Ok(rejected_control(
                        "Antigravity approval has no offered option for the reply verb",
                    ));
                }
            },
        };
        let result = json!({"outcome": {"outcome": "selected", "optionId": selected.option_id}});
        if let Some(entry) = self.pending_requests.get_mut(request_id) {
            entry.reply_state = NativeReplyState::Writing;
            entry.reply_command_id = Some(command_id.to_owned());
        }
        if let Err(error) = client
            .respond_permission(native_id, result, Instant::now() + EVENT_DEADLINE)
            .await
        {
            if matches!(
                error.code.as_str(),
                "native_not_dispatched" | "frame_too_large"
            ) {
                if let Some(entry) = self.pending_requests.get_mut(request_id)
                    && entry.reply_state == NativeReplyState::Writing
                {
                    entry.reply_state = NativeReplyState::Available;
                    entry.reply_command_id = None;
                }
                return Ok(rejected_control(
                    "Antigravity request reply was not admitted",
                ));
            }
            self.retain_unknown_reply(request_id, command_id);
            return Ok(unknown_control(
                "Antigravity request reply may have been dispatched; it will not be repeated",
            ));
        }
        let data = json!({
            "command_id": command_id,
            "request_id": request_id,
            "turn_id": turn_id,
        });
        let native_event =
            match request_event_id(request_id, "request.resolved", "agy", "Antigravity") {
                Ok(native_event) => native_event,
                Err(_) => {
                    self.retain_unknown_reply(request_id, command_id);
                    return Ok(unknown_control(
                        "Antigravity request resolution identity exceeded its bound",
                    ));
                }
            };
        if self
            .source()
            .commit_event("request.resolved", native_event, data)
            .await
            .is_err()
        {
            self.retain_unknown_reply(request_id, command_id);
            return Ok(unknown_control(
                "Antigravity request resolution was not committed",
            ));
        }
        self.pending_requests.remove(request_id);
        Ok(json!({
            "state": "applied",
            "command_id": command_id,
            "request_id": request_id,
            "turn_id": turn_id,
        }))
    }

    pub(crate) fn retain_unknown_reply(&mut self, request_id: &str, command_id: &str) {
        if let Some(entry) = self.pending_requests.get_mut(request_id) {
            entry.reply_state = NativeReplyState::Unknown;
            entry.reply_command_id = Some(command_id.to_owned());
        }
    }

    /// Answer every pending ask of the exact turn (or all asks when
    /// `turn_id` is `None`) with the ACP `cancelled` outcome, then close
    /// each answered ask without a command binding: the store binds
    /// `request.resolved` to an explicit approve/deny/answer reply only,
    /// so an interrupt/stop-driven cancel closes the Swarm request it
    /// never decided. Best-effort per ask: a proven no-write leaves the
    /// ask available for the terminal to close; any uncertain write
    /// latches unknown without a rewrite.
    pub async fn answer_pending_cancelled(
        &mut self,
        client: &AgyClient,
        command_id: &str,
        turn_id: Option<&str>,
    ) {
        let ids: Vec<String> = self
            .pending_requests
            .iter()
            .filter(|(_, entry)| turn_id.is_none_or(|turn| entry.turn_id == turn))
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in ids {
            let Some(entry) = self.pending_requests.get_mut(&request_id) else {
                continue;
            };
            let turn = entry.turn_id.clone();
            let Some(native_id) = entry.begin_cancel(command_id) else {
                continue;
            };
            match client
                .respond_permission(
                    native_id,
                    cancelled_permission_outcome(),
                    Instant::now() + EVENT_DEADLINE,
                )
                .await
            {
                Ok(()) => {
                    let data = json!({
                        "request_id": request_id,
                        "turn_id": turn,
                    });
                    match request_event_id(&request_id, "request.closed", "agy", "Antigravity") {
                        Ok(native_event) => {
                            if self
                                .source()
                                .commit_event("request.closed", native_event, data)
                                .await
                                .is_ok()
                            {
                                self.pending_requests.remove(&request_id);
                                continue;
                            }
                            self.retain_unknown_reply(&request_id, command_id);
                        }
                        Err(_) => self.retain_unknown_reply(&request_id, command_id),
                    }
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "native_not_dispatched" | "frame_too_large"
                    ) =>
                {
                    if let Some(entry) = self.pending_requests.get_mut(&request_id)
                        && entry.reply_state == NativeReplyState::Writing
                    {
                        entry.reply_state = NativeReplyState::Available;
                        entry.reply_command_id = None;
                    }
                }
                Err(_) => self.retain_unknown_reply(&request_id, command_id),
            }
        }
    }

    /// Close every retained ask of one finished turn (or of the stopped
    /// session) without a command binding: the native ended without
    /// consuming them, so they resolve to nothing. Runs before the
    /// terminal commit it precedes.
    pub(crate) async fn close_pending_requests(&mut self, turn_id: Option<&str>) -> Result<()> {
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
            let native_id = request_event_id(&request_id, "request.closed", "agy", "Antigravity")?;
            self.source()
                .commit_event("request.closed", native_id, data)
                .await?;
            self.pending_requests.remove(&request_id);
        }
        Ok(())
    }

    async fn observe(&mut self, frame: AgyFrame, client: &AgyClient) -> Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        match frame {
            AgyFrame::OversizedUpdate(detail) => {
                if self.pending.is_some() {
                    self.text_unavailable = Some("native_update_gap");
                    self.text_redaction_pending.clear();
                }
                self.record_coverage_gap(
                    &format!(
                        "Antigravity omitted a native update ({})",
                        detail.describe()
                    ),
                    &["session", "message", "tool"],
                )
                .await
            }
            AgyFrame::StreamFault(fault) => {
                self.lose(&format!(
                    "Antigravity stream contained an invalid frame ({})",
                    fault.describe()
                ))
                .await;
                Ok(())
            }
            AgyFrame::Update(params) => self.update(&params).await,
            AgyFrame::UnclassifiedNotification { method, params } => {
                if self.conversation.is_empty() {
                    return Ok(());
                }
                if let Some(session_id) = params.as_ref().and_then(|params| params.get("sessionId"))
                {
                    let Some(session_id) = session_id.as_str() else {
                        return Err(Error::new(
                            "native_protocol",
                            "Antigravity notification session binding was invalid",
                        ));
                    };
                    self.check_conversation(session_id)?;
                }
                self.record_unclassified("jsonrpc.notification.method", &method)
                    .await
            }
            // The ordered load boundary: consumed after every history
            // update ahead of it on the wire. Publish the outcome for
            // `Session::start` and leave loading mode; nothing here
            // republishes history or creates a turn.
            AgyFrame::LoadResponse { request_id, result } => {
                if self.load_request_id.as_deref() != Some(request_id.as_str()) {
                    return Err(Error::new(
                        "source_integrity",
                        "Antigravity load reply did not match its armed request",
                    ));
                }
                self.load_request_id = None;
                self.load_guard = None;
                let _ = self.load_tx.send_replace(Some((request_id, result)));
                Ok(())
            }
            AgyFrame::StopBarrierResponse { request_id, result } => {
                self.stop_barrier_passed(&request_id, result).await
            }
            // The fenced terminal reply, consumed in wire order behind
            // every preceding update: it settles the prepared turn only
            // after those updates have been applied.
            AgyFrame::PromptResponse(result) => match result {
                Ok(value) => match serde_json::from_value::<PromptResult>(value) {
                    Ok(prompt) => {
                        self.complete_prompt(&prompt).await;
                        Ok(())
                    }
                    Err(_) => {
                        self.fail_prompt(Error::new(
                            "unknown",
                            "Antigravity prompt response did not match its envelope",
                        ))
                        .await;
                        Ok(())
                    }
                },
                Err(error) => {
                    self.fail_prompt(error).await;
                    Ok(())
                }
            },
            AgyFrame::ServerRequest { id, method, params } => {
                // `session/request_permission` bridges to an explicit
                // owner/manager decision; every other agent request
                // (`fs/*`, `terminal/*`, `elicitation/create` and anything
                // unknown) is an explicit unsupported rejection, never an
                // auto-approval. A failed reply means the stream itself is
                // broken and the turn outcome is unknown.
                if method == "session/request_permission" {
                    self.open_permission_request(client, id, &params).await
                } else {
                    client
                        .reject_unsupported_request(
                            id,
                            &method,
                            &params,
                            Instant::now() + EVENT_DEADLINE,
                        )
                        .await
                }
            }
        }
    }

    fn check_conversation(&self, conversation: &str) -> Result<()> {
        if conversation == self.conversation {
            return Ok(());
        }
        Err(Error::new(
            "source_integrity",
            "Antigravity frame belongs to another conversation",
        ))
    }

    async fn update(&mut self, params: &Value) -> Result<()> {
        let Some((session_id, update)) = classify_update(params) else {
            return Err(Error::new(
                "native_protocol",
                "Antigravity session/update did not match its envelope",
            ));
        };
        if self.conversation.is_empty() {
            let bytes = params.to_string().len();
            if self.unbound_updates.len() >= MAX_UNBOUND_UPDATES
                || self.unbound_update_bytes.saturating_add(bytes) > MAX_UNBOUND_UPDATE_BYTES
            {
                return Err(Error::new(
                    "frame_too_large",
                    "Antigravity pre-session updates exceeded their startup bound",
                ));
            }
            self.unbound_update_bytes += bytes;
            self.unbound_updates
                .push_back(QueuedUpdate { session_id, update });
            return Ok(());
        }
        self.apply_update(session_id, update).await
    }

    async fn apply_update(&mut self, session_id: String, update: AcpUpdate) -> Result<()> {
        self.check_conversation(&session_id)?;
        // Validated load history: matching-session updates before the
        // ordered load reply are history, not live output. Discard them
        // without association, republication, or turn creation. Live
        // updates after the reply take the normal path below.
        if self.load_request_id.is_some() {
            return Ok(());
        }
        let unknown_tool_status = match &update {
            AcpUpdate::ToolCall { status, .. } => Some(status.as_deref().unwrap_or("missing")),
            AcpUpdate::ToolCallUpdate {
                status: Some(status),
                ..
            } => Some(status.as_str()),
            _ => None,
        }
        .filter(|status| !matches!(*status, "pending" | "in_progress" | "completed" | "failed"));
        if let Some(native_tag) = unknown_tool_status {
            self.record_unclassified("session.update.tool_call.status", native_tag)
                .await?;
        }
        match update {
            // AGY can continue natively when an asynchronous tool finishes
            // after the fenced terminal reply. An idle update has no Eliot turn.
            AcpUpdate::MessageChunk { .. } if self.pending.is_none() => {
                if !self.idle_message_gap_reported {
                    self.record_coverage_gap(
                        "Antigravity assistant output arrived outside an Eliot prompt",
                        &["message"],
                    )
                    .await?;
                    self.idle_message_gap_reported = true;
                }
                Ok(())
            }
            AcpUpdate::MessageChunk { text } => {
                self.associate().await?;
                self.append_assistant_text(&text);
                Ok(())
            }
            // Bookkeeping: neither input acknowledgement nor assistant output.
            AcpUpdate::ThoughtChunk | AcpUpdate::UserChunk | AcpUpdate::Ignored => Ok(()),
            AcpUpdate::SessionUsage {
                used,
                size,
                cost_amount,
                cost_unit,
            } => {
                // A full usage update from the exact session can acknowledge
                // the pending input before the live sample is published.
                if used.is_some() && size.is_some() && self.pending.is_some() {
                    self.associate().await?;
                }
                if let Some(merged) = session_usage_from_update(
                    used,
                    size,
                    cost_amount,
                    cost_unit,
                    self.session_usage.as_ref(),
                ) {
                    self.sample_session_usage(merged).await;
                }
                Ok(())
            }
            AcpUpdate::ToolCall { .. } if self.pending.is_none() => {
                self.record_unattributed_tool().await
            }
            AcpUpdate::ToolCall {
                tool_call_id,
                title,
                tool_kind,
                tool_name,
                diff_paths,
                status,
            } => {
                let turn = &self.pending.as_ref().expect("prepared tool call").turn;
                if !self.valid_tool_id(&tool_call_id, turn)
                    || (!self.tools.contains_key(&tool_call_id)
                        && self.tools.len() >= MAX_LIVE_TOOLS)
                {
                    return self.record_unattributed_tool().await;
                }
                if !self.tools.contains_key(&tool_call_id) {
                    self.finish_message_before_tool().await?;
                }
                self.merge_tool(
                    &tool_call_id,
                    &title,
                    status.as_deref().or(Some("missing")),
                    tool_kind.as_deref(),
                    tool_name.as_deref(),
                    &diff_paths,
                );
                self.publish_tool_status(&tool_call_id).await
            }
            AcpUpdate::ToolCallUpdate {
                tool_call_id,
                title,
                tool_kind,
                tool_name,
                diff_paths,
                status,
            } => {
                if tool_call_id.trim().is_empty() {
                    return Err(Error::new(
                        "native_protocol",
                        "Antigravity tool call id is absent",
                    ));
                }
                if let Some(turn) = self.background_tools.get(&tool_call_id).cloned() {
                    if matches!(
                        status.as_deref().map(ToolStatus::parse),
                        Some(ToolStatus::Completed | ToolStatus::Failed)
                    ) {
                        self.background_tools.remove(&tool_call_id);
                        let item = format!("tool-{tool_call_id}");
                        self.commit(
                            "tool.background_ended",
                            &turn,
                            &item,
                            json!({"turn_id": turn, "item_id": item}),
                        )
                        .await?;
                    }
                    return Ok(());
                }
                // ACP deltas update an existing call. AGY can finish an
                // asynchronous tool after its prompt has already ended.
                // Only calls introduced in the current prompt can receive
                // turn events or establish its association.
                if self.pending.is_none() || !self.tools.contains_key(&tool_call_id) {
                    return self.record_unattributed_tool().await;
                }
                self.merge_tool(
                    &tool_call_id,
                    title.as_deref().unwrap_or(""),
                    status.as_deref(),
                    tool_kind.as_deref(),
                    tool_name.as_deref(),
                    &diff_paths,
                );
                self.publish_tool_status(&tool_call_id).await
            }
            AcpUpdate::Other(native_tag) => {
                self.record_unclassified("session.update.sessionUpdate", &native_tag)
                    .await
            }
        }
    }

    async fn record_unattributed_tool(&mut self) -> Result<()> {
        if self.pending.is_some() {
            self.finish_message_before_tool().await?;
        }
        if self
            .pending
            .as_ref()
            .map_or(self.idle_tool_gap_reported, |pending| {
                pending.tool_gap_reported
            })
        {
            return Ok(());
        }
        self.record_coverage_gap(
            "Antigravity tool output did not match a tool in the current Eliot prompt",
            &["tool"],
        )
        .await?;
        if let Some(pending) = self.pending.as_mut() {
            pending.tool_gap_reported = true;
        } else {
            self.idle_tool_gap_reported = true;
        }
        Ok(())
    }

    async fn publish_tool_status(&mut self, tool_call_id: &str) -> Result<()> {
        match self
            .tools
            .get(tool_call_id)
            .map(|tool| tool.status)
            .unwrap_or(ToolStatus::Unknown)
        {
            ToolStatus::Pending | ToolStatus::InProgress => {
                self.tool_event(tool_call_id, "tool.started", "running")
                    .await
            }
            ToolStatus::Completed => {
                self.tool_event(tool_call_id, "tool.completed", "completed")
                    .await?;
                self.tool_file_edited(tool_call_id).await
            }
            ToolStatus::Failed => {
                self.tool_event(tool_call_id, "tool.completed", "failed")
                    .await
            }
            ToolStatus::Unknown | ToolStatus::Other => Ok(()),
        }
    }

    /// Keep calls that Antigravity moved to native background tasks open;
    /// their exact later completion is `tool.background_ended`.
    async fn close_open_tools_at_terminal(&mut self) -> Result<()> {
        let turn = self
            .pending
            .as_ref()
            .map(|pending| pending.turn.clone())
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let open_tool_ids: Vec<String> = self
            .tools
            .keys()
            .filter(|tool_call_id| {
                self.tool_emitted
                    .contains(&format!("{tool_call_id}:tool.started"))
                    && !self
                        .tool_emitted
                        .contains(&format!("{tool_call_id}:tool.completed"))
            })
            .cloned()
            .collect();
        for tool_call_id in open_tool_ids {
            if self.background_tools.len() < MAX_BACKGROUND_TOOLS {
                self.tools.remove(&tool_call_id);
                self.background_tools.insert(tool_call_id, turn.clone());
            } else {
                self.record_coverage_gap(
                    "Antigravity background tool tracking bound was exceeded",
                    &["tool"],
                )
                .await?;
                break;
            }
        }
        Ok(())
    }

    async fn sample_session_usage(&mut self, usage: SessionUsage) {
        let turn = self.pending.as_ref().map(|pending| pending.turn.clone());
        if self.session_usage.as_ref() == Some(&usage) && self.session_usage_turn == turn {
            return;
        }
        self.session_usage = Some(usage.clone());
        self.session_usage_turn = turn.clone();
        if self
            .pending
            .as_ref()
            .is_some_and(|pending| !pending.associated)
        {
            // Partial counters cannot acknowledge input. Keep them for this
            // turn's eventual terminal sample.
            return;
        }
        let _ = self
            .source()
            .report_session_usage_sample(usage, turn.as_deref())
            .await;
    }

    async fn commit_turn_session_usage(&mut self, turn: &str) -> Result<()> {
        if self.session_usage_turn.as_deref() != Some(turn) {
            return Ok(());
        }
        let Some(usage) = self.session_usage.clone() else {
            return Ok(());
        };
        self.commit("session.usage", turn, "terminal", json!({"usage": usage}))
            .await?;
        self.session_usage_turn = None;
        Ok(())
    }

    fn reset_assistant_text(&mut self) {
        self.text_accum.clear();
        self.text_redaction_pending.clear();
        self.text_truncated = false;
        self.text_unavailable = None;
    }

    async fn finish_message_before_tool(&mut self) -> Result<()> {
        self.finish_assistant_text();
        let text = std::mem::take(&mut self.text_accum);
        if self.text_unavailable.is_none() {
            self.commit_assistant_message(&text).await?;
        }
        self.reset_assistant_text();
        Ok(())
    }

    async fn commit_assistant_message(&mut self, text: &str) -> Result<()> {
        if is_blank_native_text(text) {
            return Ok(());
        }
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        self.assistant_message_seq += 1;
        let native_id = event_id(
            &self.conversation,
            &pending.turn,
            &format!("assistant:{}", self.assistant_message_seq),
            "message.completed",
            "agy",
            "Antigravity",
        )?;
        let mut data = json!({
            "turn_id": pending.turn,
            "text": text,
        });
        if let Some(command) = pending.command.as_deref() {
            data["command_id"] = json!(command);
        }
        self.policy.redact_values(&mut data);
        self.source().commit_message(native_id, data).await?;
        Ok(())
    }

    /// Redact native chunks as a stream before retaining only a bounded suffix.
    /// The raw pending bytes contain only the unfinished secret or marker prefix.
    fn append_assistant_text(&mut self, text: &str) {
        if self.text_unavailable.is_some() {
            return;
        }

        const REDACTION_CHUNK_BYTES: usize = 8 * 1024;
        let mut offset = 0;
        while offset < text.len() {
            let mut end = (offset + REDACTION_CHUNK_BYTES).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            self.text_redaction_pending
                .extend_from_slice(&text.as_bytes()[offset..end]);
            offset = end;
            self.redact_pending_text(false);
            if self.text_unavailable.is_some() {
                return;
            }
        }
    }

    fn finish_assistant_text(&mut self) {
        if self.text_unavailable.is_some() {
            self.text_redaction_pending.clear();
        } else {
            self.redact_pending_text(true);
        }
    }

    fn redact_pending_text(&mut self, eof: bool) {
        let (safe, consumed) = self.policy.redact_prefix(&self.text_redaction_pending, eof);
        self.text_redaction_pending.drain(..consumed);
        let safe = String::from_utf8_lossy(&safe);
        self.append_assistant_text_suffix(&safe);
    }

    fn append_assistant_text_suffix(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if text.len() > MAX_TURN_TEXT_BYTES {
            let mut start = text.len() - MAX_TURN_TEXT_BYTES;
            while !text.is_char_boundary(start) {
                start += 1;
            }
            self.text_accum.clear();
            self.text_accum.push_str(&text[start..]);
            self.text_truncated = true;
            return;
        }

        let retained_prefix = MAX_TURN_TEXT_BYTES - text.len();
        if self.text_accum.len() > retained_prefix {
            let mut start = self.text_accum.len() - retained_prefix;
            while !self.text_accum.is_char_boundary(start) {
                start += 1;
            }
            self.text_accum.drain(..start);
            self.text_truncated = true;
        }
        self.text_accum.push_str(text);
    }

    /// Merge partial tool-call facts into the known per-id state. Empty title
    /// and omitted status/name/kind inherit their last known values; exact
    /// diff paths accumulate until the turn is cleared.
    fn merge_tool(
        &mut self,
        tool_call_id: &str,
        title: &str,
        status: Option<&str>,
        tool_kind: Option<&str>,
        tool_name: Option<&str>,
        diff_paths: &ToolDiffPaths,
    ) {
        let known = self
            .tools
            .entry(tool_call_id.to_owned())
            .or_insert_with(|| NativeToolState {
                title: String::new(),
                status: ToolStatus::Unknown,
                native_file_edit_tool: None,
                edit_kind: false,
                diff_paths: ToolDiffPaths::default(),
            });
        if !title.is_empty() {
            known.title = self.policy.redact_bounded(title, MAX_TOOL_TITLE_BYTES);
        }
        if let Some(status) = status {
            let next = ToolStatus::parse(status);
            let is_other = matches!(next, ToolStatus::Other);
            let was_terminal = matches!(known.status, ToolStatus::Completed | ToolStatus::Failed);
            if !is_other || !was_terminal {
                known.status = next;
            }
        }
        if let Some(name) = tool_name {
            known.native_file_edit_tool = Some(
                known.native_file_edit_tool.unwrap_or(true)
                    && NATIVE_FILE_EDIT_TOOLS.contains(&name),
            );
        }
        if let Some(tool_kind) = tool_kind {
            known.edit_kind = tool_kind == "edit";
        }
        known.diff_paths.overflowed |= diff_paths.overflowed;
        for path in &diff_paths.paths {
            known.diff_paths.insert(path);
        }
    }

    fn valid_tool_id(&self, tool_call_id: &str, turn: &str) -> bool {
        !tool_call_id.trim().is_empty()
            && tool_call_id.len() <= MAX_TOOL_ID_BYTES
            && !tool_call_id.chars().any(char::is_control)
            && !self.policy.contains_text(tool_call_id)
            && !self.background_tools.contains_key(tool_call_id)
            && event_id(
                &self.conversation,
                turn,
                &format!("tool-{tool_call_id}"),
                "tool.completed",
                "agy",
                "Antigravity",
            )
            .is_ok_and(|id| !self.policy.contains_text(&id))
    }

    async fn tool_event(&mut self, tool_call_id: &str, kind: &str, status: &str) -> Result<()> {
        let item = format!("tool-{tool_call_id}");
        // Each lifecycle transition is emitted once per tool call. A
        // later delta for an already-emitted transition is metadata
        // only (already merged) and never re-emits or poisons the
        // retained comparison.
        let marker = format!("{tool_call_id}:{kind}");
        if self.tool_emitted.contains(&marker) {
            return Ok(());
        }
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let command = pending.command.clone();
        let turn = pending.turn.clone();
        let title = self
            .tools
            .get(tool_call_id)
            .map(|tool| tool.title.clone())
            .unwrap_or_default();
        let mut data = json!({"turn_id": turn,
            "item_id": item, "tool": title, "status": status});
        if let Some(command) = command.as_deref() {
            data["command_id"] = json!(command);
        }
        if kind == "tool.started" {
            data["native_auto_background"] = json!(true);
            data["native_wake_scope"] =
                json!(eliot_core::NATIVE_WAKE_SCOPE_PROCESS_TREE);
        }
        self.policy.redact_values(&mut data);
        self.commit(kind, &turn, &item, data).await?;
        self.tool_emitted.insert(marker);
        if kind == "tool.completed" && status == "completed" {
            self.tool_completed_successfully
                .insert(tool_call_id.to_owned());
        }
        Ok(())
    }

    async fn tool_file_edited(&mut self, tool_call_id: &str) -> Result<()> {
        if !self.tool_completed_successfully.contains(tool_call_id) {
            return Ok(());
        }
        let Some(tool) = self.tools.get(tool_call_id) else {
            return Ok(());
        };
        // AGY can classify the mutation by ACP kind without sending a tool name.
        if !tool.native_file_edit_tool.unwrap_or(tool.edit_kind) {
            return Ok(());
        }
        let Some(pending) = self.pending.as_ref() else {
            return Ok(());
        };
        let command = pending.command.clone();
        let turn = pending.turn.clone();
        let diff_paths = tool.diff_paths.paths.clone();
        if tool.diff_paths.overflowed && !self.tool_edit_gap_reported {
            self.record_coverage_gap(
                "Antigravity native edit paths overflowed the per-tool bound",
                &["tool"],
            )
            .await?;
            self.tool_edit_gap_reported = true;
        }

        for (index, path) in diff_paths.into_iter().enumerate() {
            let emitted_key = (tool_call_id.to_owned(), path.clone());
            if self.tool_file_edit_emitted.contains(&emitted_key) {
                continue;
            }
            let Some(edit_path) =
                eliot_plugin::edit_path::evidence(&self.workspace, &path, &self.policy)
            else {
                continue;
            };
            let base_item = format!("tool-{tool_call_id}");
            let item = format!("{base_item}:edit:{index}");
            let mut data = json!({
                "turn_id": turn,
                "item_id": base_item,
                "status": "completed",
                "edit_path": edit_path,
            });
            if let Some(command) = command.as_deref() {
                data["command_id"] = json!(command);
            }
            self.commit("tool.file.edited", &turn, &item, data).await?;
            self.tool_file_edit_emitted.insert(emitted_key);
        }
        Ok(())
    }

    /// Settle the prepared turn from the terminal `session/prompt`
    /// response. Only the actual stop reason settles the turn: `cancelled`
    /// is the interrupt settlement and is never inferred from silence.
    pub async fn complete_prompt(&mut self, prompt: &PromptResult) {
        if self.failure.is_some() {
            return;
        }
        match self.settle_prompt(prompt).await {
            Ok(()) => {}
            Err(error) => self.lose_observation(&error).await,
        }
    }

    async fn settle_prompt(&mut self, result: &PromptResult) -> Result<()> {
        let (mut status, native_status) = match result.terminal_status() {
            PromptTerminalStatus::Completed => {
                ("completed", self.policy.redact_text(&result.stop_reason))
            }
            PromptTerminalStatus::Cancelled => {
                ("cancelled", self.policy.redact_text(&result.stop_reason))
            }
            PromptTerminalStatus::Failed => {
                ("failed", self.policy.redact_text(&result.stop_reason))
            }
            PromptTerminalStatus::Other => {
                self.record_unclassified("session.prompt.stopReason", &result.stop_reason)
                    .await?;
                ("unknown", "other".to_owned())
            }
        };
        self.finish_assistant_text();
        let observed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok());
        let restriction = (self.text_unavailable.is_none()
            && !self.text_truncated
            && matches!(status, "completed" | "failed"))
        .then(|| crate::quota::turn_restriction(&self.text_accum, observed_at.unwrap_or(0)))
        .flatten()
        .map(|mut restriction| {
            if observed_at.is_none() {
                restriction.retry_after = None;
            }
            restriction
        });
        let quota_warning = restriction.is_some();
        if quota_warning {
            status = "failed";
        }
        // The reply answers the fenced `session/prompt` of the exact prepared
        // turn: it is that turn's native terminal evidence. Retain its bound
        // status and raw stopReason before any write (even `turn.started`),
        // so a failed write costs only its own fact and the exact-turn
        // snapshot still proves the outcome.
        if let Some(pending) = self.pending.as_ref() {
            self.last_turn = Some(TurnFacts {
                command_id: pending.command.clone(),
                turn_id: pending.turn.clone(),
                status: status.to_owned(),
                native_status: native_status.clone(),
                final_text: String::new(),
                native_error: None,
                final_text_unavailable: reply_unavailable(
                    self.text_unavailable,
                    status,
                    &self.text_accum,
                ),
                terminal: true,
            });
        }
        // A terminal response alone suffices: no intermediate update is
        // required, matching the old stream-json rule.
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let (command, turn) = (pending.command.clone(), pending.turn.clone());
        if let Some(checkpoints) = &mut self.checkpoints {
            let records = checkpoints.read_new();
            if checkpoints.take_identity_gap() {
                self.record_coverage_gap(
                    "Antigravity transcript exceeded the checkpoint identity bound",
                    &["session"],
                )
                .await?;
            }
            for checkpoint in records {
                // The transcript proves a session checkpoint, even if its
                // write reached disk after an earlier prompt settled.
                self.commit(
                    "session.context.compacted",
                    "session",
                    &format!("checkpoint-{}", checkpoint.identity),
                    json!({"step_index": checkpoint.step_index}),
                )
                .await?;
            }
        }
        let failed = status == "failed";
        // Canonical outcome decided here at the plugin boundary from the
        // Antigravity terminal vocabulary (`completed` | `failed` |
        // `cancelled`). The grouped normal stop reasons (`end_turn`,
        // `max_tokens`, `max_turn_requests`) all complete normally: ordinary
        // native termination is success with its native reason, not Goal
        // fulfilment. The raw `stopReason` is already preserved as
        // `native_status` next to it.
        let outcome = match status {
            "completed" => "success",
            "failed" => "failure",
            "cancelled" => "interrupted",
            _ => "unknown",
        };
        // Unanswered asks die with their turn: close them before the
        // terminal commit, without a command binding.
        self.close_pending_requests(Some(&turn)).await?;
        // This is the exact fenced `session/prompt` terminal for the bound
        // conversation. Close only this State's still-open tool lifecycles;
        // a terminal response cannot establish their native outcome.
        self.close_open_tools_at_terminal().await?;
        let text = self.policy.redact_text(&self.text_accum);
        let missing_text = self.text_unavailable;
        let mut data = json!({"turn_id": turn,
            "status": status, "outcome": outcome, "native_status": native_status});
        if let Some(command) = command.as_deref() {
            data["command_id"] = json!(command);
        }
        if let Some(restriction) = restriction {
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "antigravity",
                eliot_core::FailureOrigin::Provider,
                "agy 1.5.10 complete native quota warning; ACP returns end_turn without a structured error",
                &self.policy,
            );
            data["native_error"] =
                json!({"code":"quota_exhausted", "message":"provider quota exhausted"});
            eliot_plugin::session::attach_provider_restriction(
                &mut data,
                restriction,
                &self.policy,
            );
        } else if failed {
            eliot_plugin::session::attach_failure_origin(
                &mut data,
                "antigravity",
                eliot_core::FailureOrigin::Harness,
                "native ACP terminal refusal; documented ACP surface exposes no structured upstream error",
                &self.policy,
            );
        }
        self.policy.redact_values(&mut data);
        if missing_text.is_none() && !quota_warning {
            self.commit_assistant_message(&text).await?;
        }
        let unavailable = reply_unavailable(missing_text, status, &text);
        if let Some(marker) = unavailable {
            data["final_text_unavailable"] = json!(marker);
        }
        if let Some(last) = self.last_turn.as_mut() {
            last.final_text_unavailable = unavailable;
        }
        // Installed agy 1.5.9 reports the turn's model usage on the
        // terminal response (older servers send none): attach it when
        // usable, never block completion on it.
        if let Some(usage) = prompt_turn_usage(result) {
            data["usage"] = json!(usage);
        }
        if (self.text_truncated || !is_blank_native_text(&text))
            && missing_text.is_none()
            && !quota_warning
        {
            attach_final_text(&mut data, &text);
        }
        self.commit("turn.completed", &turn, "completed", data.clone())
            .await?;
        let session_usage = self.commit_turn_session_usage(&turn).await;
        self.reject_pending_steer(
            "Antigravity turn ended before its next PreInvocation steer point",
        );
        let final_text = data
            .get("final_text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if let Some(last) = self.last_turn.as_mut() {
            last.final_text = final_text;
        }
        self.record_settlement(turn, status == "cancelled");
        self.clear_fenced();
        self.pending = None;
        session_usage
    }

    /// A correlated ACP validation refusal can withdraw an unassociated
    /// prompt. All other errors leave delivery or settlement unproven.
    pub async fn fail_prompt(&mut self, error: Error) {
        if self.failure.is_some() {
            return;
        }
        if error.code == "native_rejected"
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| !pending.associated)
        {
            self.clear_fenced();
            self.fail_pending_steer(Error::new(
                "stale_turn",
                "Antigravity prompt was rejected before admission",
            ));
            if let Some(mut pending) = self.pending.take()
                && let Some(reply) = pending.reply.take()
            {
                let _ = reply.send(Err(error));
            }
            return;
        }
        let reason = if error.code == "native_rejected" {
            "Antigravity refused a prompt with committed progress"
        } else {
            &error.message
        };
        self.lose(reason).await;
    }

    pub async fn ended(&mut self) -> Result<()> {
        self.clear_fenced();
        if let Some(request_id) = self.load_request_id.take() {
            self.load_guard = None;
            let _ = self.load_tx.send_replace(Some((
                request_id,
                Err(Error::new("unknown", "Antigravity ACP load ended")),
            )));
        }
        if !self.stopped {
            if self.pending.is_some() {
                self.lose("Antigravity stopped with an unresolved input")
                    .await;
            }
            self.commit("session.ended", "session", "ended", json!({}))
                .await?;
            self.stopped = true;
        }
        // An unfinished input stays unresolved; process exit does not complete it.
        Ok(())
    }

    async fn commit(&mut self, kind: &str, turn: &str, item: &str, mut data: Value) -> Result<()> {
        self.policy.redact_values(&mut data);
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity normalized fact exceeds event bound",
            ));
        }
        let id = event_id(&self.conversation, turn, item, kind, "agy", "Antigravity")?;
        self.source().commit_event(kind, id, data).await
    }

    async fn record_unclassified(&mut self, site: &'static str, native_tag: &str) -> Result<()> {
        let item = format!("native-unclassified-{}", Ulid::new());
        let native_tag = if native_tag.is_empty() {
            "empty"
        } else {
            native_tag
        };
        self.commit(
            eliot_plugin::native_data::UNCLASSIFIED_NATIVE_EVENT,
            "session",
            &item,
            eliot_plugin::native_data::unclassified(site, native_tag),
        )
        .await
    }

    pub async fn lose(&mut self, reason: &str) {
        self.lose_inner(reason, true).await;
    }

    async fn lose_observation(&mut self, error: &Error) {
        self.lose_inner(
            &error.message,
            error.failure_where.as_deref() != Some("plugin.events.put"),
        )
        .await;
    }

    async fn lose_inner(&mut self, reason: &str, report_gap: bool) {
        if !self.writes_blocked
            && self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.associated)
        {
            // Reconcile the still-bound tool lifecycle before publishing loss
            // and allowing Store to settle the prompt unknown.
            let _ = self.close_open_tools_at_terminal().await;
            // A cancel notification can be followed by stream loss without a
            // prompt terminal. Keep the observed usage without inferring a turn outcome.
            if self.cancellation_for_pending().is_some()
                && let Some(turn) = self.pending.as_ref().map(|pending| pending.turn.clone())
            {
                let _ = self.commit_turn_session_usage(&turn).await;
            }
        }
        self.source_unknown = true;
        self.fail_pending_steer(Error::new(
            "unknown",
            format!("Antigravity stream was lost before PreInvocation steer settled: {reason}"),
        ));
        self.clear_fenced();
        if let Some(request_id) = self.load_request_id.take() {
            // Stream loss during load: publish the failure to the load
            // waiter instead of letting startup hang to its deadline.
            self.load_guard = None;
            let _ = self.load_tx.send_replace(Some((
                request_id,
                Err(Error::new(
                    "unknown",
                    "Antigravity ACP load failed with the stream",
                )),
            )));
        }
        if let Some(turn) = self.pending.as_ref().map(|pending| pending.turn.clone()) {
            self.record_settlement(turn, false);
        }
        if report_gap && !self.gap_reported && !self.writes_blocked {
            let mut source = self.source();
            let _ = source.record_gap(reason).await;
        }
        if report_gap && !std::mem::replace(&mut self.stream_loss_reported, true) {
            let update = SourceHealthUpdate::new(&self.agent, self.generation, "stream", "stale");
            if let Ok(mut request) = serde_json::to_value(update) {
                request["credential"] = json!(self.credential);
                let _ = self
                    .host
                    .call("source.health", request, Instant::now() + EVENT_DEADLINE)
                    .await;
            }
        }
        self.failure.get_or_insert_with(|| reason.to_owned());
        if let Some(reply) = self.pending.as_mut().and_then(|p| p.reply.take()) {
            let _ = reply.send(Err(Error::new(
                "unknown",
                format!("Antigravity input outcome is unknown: {reason}"),
            )));
        }
    }

    async fn record_coverage_gap(&mut self, reason: &str, families: &[&str]) -> Result<()> {
        self.source().record_coverage_gap(reason, families).await
    }

    fn source(&mut self) -> StreamSource<'_> {
        StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent,
            generation: self.generation,
            harness: "Antigravity",
            id_prefix: "agy",
            families: FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        }
    }
}

pub(crate) fn spawn(
    mut frames: mpsc::Receiver<AgyFrame>,
    mut stream: watch::Receiver<StreamState>,
    state: Shared,
    client: AgyClient,
    hooks: HookServer,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (hook_tx, mut hook_rx) = mpsc::channel(1);
        let hook_state = state.clone();
        let hook_accept_task = tokio::spawn(async move {
            loop {
                let callback = match hooks.accept().await {
                    Ok(callback) => callback,
                    Err(_) => break,
                };
                let input = callback.input().clone();
                let origin = *callback.origin();
                let origin_is_live =
                    process_birth_identity(origin.pid).is_ok_and(|observed| observed == origin);
                let binding = if origin_is_live {
                    hook_state.lock().await.capture_hook_turn(&input, origin)
                } else {
                    None
                };
                if hook_tx
                    .send(AcceptedHook { callback, binding })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        // Observe watch faults while active, including with no incoming
        // frames: a record-local Malformed keeps the reader open, so an
        // EOF-only check would let it vanish while later facts commit.
        // No model-turn timer lives here: stream silence is not staleness
        // and a wait timeout never cancels admitted work; the fenced
        // terminal reply, an error, a stream fault, stop, or drop are the
        // only settlements.
        let mut stream_open = true;
        let mut hooks_open = true;
        let mut deferred: Option<AcceptedHook> = None;
        loop {
            enum Next {
                Frame(Option<AgyFrame>),
                StreamClosed(bool),
                Hook(Option<AcceptedHook>),
            }
            let next = tokio::select! {
                biased;
                frame = frames.recv() => Next::Frame(frame),
                changed = stream.changed(), if stream_open => {
                    Next::StreamClosed(changed.is_err())
                }
                callback = hook_rx.recv(), if hooks_open => Next::Hook(callback),
            };
            match next {
                Next::StreamClosed(closed) => {
                    if closed {
                        stream_open = false;
                    }
                    let current = *stream.borrow();
                    // Snapshot the fault before checking the queue. The pump
                    // publishes its ordered marker before the watch update.
                    // Fatal/overflow faults have no accepted suffix: drain their
                    // retained prefix before applying this out-of-band gap.
                    if frames.is_empty() {
                        note_stream_fault(&state, current).await;
                    }
                    continue;
                }
                Next::Hook(Some(callback)) => {
                    let starts_execution = callback.callback.input().starts_execution();
                    let stop_pending = state.lock().await.stop_order_pending();
                    if starts_execution && stop_pending {
                        if deferred.is_none() {
                            deferred = Some(callback);
                        } else {
                            let _ = callback.callback.respond(HookOutput::empty()).await;
                            state
                                .lock()
                                .await
                                .lose("Antigravity received overlapping PreInvocation hooks across a Stop fence")
                                .await;
                        }
                        continue;
                    }
                    if let Err(error) = handle_hook_callback(
                        callback.callback,
                        callback.binding,
                        &state,
                        &client,
                    )
                    .await
                    {
                        state.lock().await.lose_observation(&error).await;
                    }
                    continue;
                }
                Next::Hook(None) => {
                    hooks_open = false;
                    continue;
                }
                Next::Frame(None) => break,
                Next::Frame(Some(frame)) => {
                    let mut state_guard = state.lock().await;
                    if let Err(error) = state_guard.observe(frame, &client).await {
                        state_guard.lose_observation(&error).await;
                    }
                    drop(state_guard);
                    let stop_pending = state.lock().await.stop_order_pending();
                    if deferred.is_some() && !stop_pending {
                        if let Some(callback) = deferred.take()
                            && let Err(error) = handle_hook_callback(
                                callback.callback,
                                callback.binding,
                                &state,
                                &client,
                            )
                            .await
                        {
                            state.lock().await.lose_observation(&error).await;
                        }
                    }
                    let current = *stream.borrow();
                    if frames.is_empty() {
                        note_stream_fault(&state, current).await;
                    }
                }
            }
        }
        hook_accept_task.abort();
        let _ = hook_accept_task.await;
        if let Some(callback) = deferred.take() {
            let _ = callback.callback.respond(HookOutput::empty()).await;
        }
        // All queued stdout is consumed before concluding loss or planned EOF.
        let current = *stream.borrow();
        if matches!(current, StreamState::Overflow | StreamState::Malformed(_)) {
            note_stream_fault(&state, current).await;
        } else {
            let mut state = state.lock().await;
            if !state.stopping {
                state
                    .lose("Antigravity stream closed outside a planned stop")
                    .await;
            }
        }
    })
}

async fn note_stream_fault(state: &Shared, current: StreamState) {
    match current {
        StreamState::Overflow => {
            state
                .lock()
                .await
                .lose("Antigravity native queue overflowed")
                .await
        }
        StreamState::Malformed(NativeStreamFault::FrameOversized(detail)) => {
            state
                .lock()
                .await
                .lose(&format!(
                    "Antigravity stream ended on an oversized frame ({})",
                    detail.describe(),
                ))
                .await;
        }
        StreamState::Malformed(fault) => {
            state
                .lock()
                .await
                .lose(&format!(
                    "Antigravity stream contained an invalid frame ({})",
                    fault.describe()
                ))
                .await;
        }
        _ => {}
    }
}

async fn handle_hook_callback(
    callback: HookCallback,
    mut binding: Option<HookTurnBinding>,
    state: &Shared,
    client: &AgyClient,
) -> Result<()> {
    let input = callback.input().clone();
    let origin = *callback.origin();
    let origin_is_live =
        process_birth_identity(origin.pid).is_ok_and(|observed| observed == origin);
    if input.is_stop() {
        let _ = callback.respond(HookOutput::empty()).await;
        if origin_is_live {
            state
                .lock()
                .await
                .observe_stop(&input, origin, client)
                .await?;
        }
        return Ok(());
    }
    let opened = if origin_is_live && input.starts_execution() {
        state.lock().await.begin_native_run(&input, origin).await?
    } else {
        None
    };
    binding = opened.or(binding);
    let Some(binding) = binding else {
        let _ = callback.respond(HookOutput::empty()).await;
        return Ok(());
    };
    let live_binding = {
        let mut state = state.lock().await;
        if origin_is_live && state.hook_binding_is_live(&binding) {
            if state.checkpoints.is_none()
                && let Some(path) = callback.input().transcript_path.as_deref()
                && let Some(root) = state.checkpoint_root.as_deref()
            {
                state.checkpoints = checkpoints::Checkpoints::from_hook(
                    std::path::Path::new(path),
                    root,
                    &state.conversation,
                    state.checkpoint_resume,
                );
            }
            Some((
                state.take_pending_steer(&binding.turn),
                state.host.clone(),
                state.credential.clone(),
            ))
        } else {
            None
        }
    };
    let Some((steer, host, credential)) = live_binding else {
        let _ = callback.respond(HookOutput::empty()).await;
        return Ok(());
    };

    let consumer_id = Ulid::new().to_string();
    let event = HookEvent {
        hook: "before_model".to_owned(),
        turn_id: binding.turn.clone(),
        consumer_id: consumer_id.clone(),
    };
    let mut params = match serde_json::to_value(event) {
        Ok(params) => params,
        Err(_) => {
            let _ = callback.respond(HookOutput::empty()).await;
            return Ok(());
        }
    };
    let Some(params) = params.as_object_mut() else {
        let _ = callback.respond(HookOutput::empty()).await;
        return Ok(());
    };
    params.insert(
        "credential".to_owned(),
        serde_json::Value::String(credential),
    );

    let (context, selection_known) = match host
        .call(
            "hook.event",
            serde_json::Value::Object(std::mem::take(params)),
            Instant::now() + EVENT_DEADLINE,
        )
        .await
    {
        Ok(response) => match serde_json::from_value::<HookResponse>(response) {
            Ok(response) => (response.context, true),
            Err(_) => (Vec::new(), false),
        },
        Err(_) => (Vec::new(), false),
    };

    // The authenticated Core response fits MAX_FRAME_BYTES. Converting its
    // ContextItems to system-message steps removes each command id and expiry
    // field, so the context portion remains within that same shared bound.
    // Steer was independently preflighted against MAX_FRAME_BYTES before it
    // was armed. The private frame reserves both complete bounds plus the
    // IpcQueryResponse envelope; this runtime check includes that envelope.
    let mut output = context_hook_output(&context);
    let mut steer = steer;
    if let Some(pending) = &steer {
        output
            .inject_steps
            .push(HookStep::system_message(pending.text.clone()));
    }
    if !hook_ipc_response_fits(&output) {
        if let Some(pending) = steer.take() {
            state.lock().await.defer_pending_steer(pending)?;
        }
        output = context_hook_output(&context);
        if !hook_ipc_response_fits(&output) {
            return Err(Error::new(
                "frame_too_large",
                format!(
                    "Antigravity context response exceeds its reserved combined frame (shared={MAX_FRAME_BYTES}, private={MAX_HOOK_FRAME_BYTES})"
                ),
            ));
        }
    }

    let origin_is_live =
        process_birth_identity(binding.origin.pid).is_ok_and(|observed| observed == binding.origin);
    let live = origin_is_live && state.lock().await.hook_binding_is_live(&binding);
    if !live {
        let _ = callback.respond(HookOutput::empty()).await;
        if let Some(steer) = steer {
            let _ = steer.reply.send(Err(Error::new(
                "stale_turn",
                "Antigravity prompt ended before PreInvocation applied steer",
            )));
        }
        return Ok(());
    }
    if callback.respond(output).await.is_err() {
        if let Some(steer) = steer {
            let result = unknown_steer_result(
                &steer,
                "Antigravity helper did not confirm its stdout response",
            );
            let _ = steer.reply.send(Ok(result));
        }
        return Ok(());
    }

    if let Some(steer) = steer {
        let result = serde_json::json!({
            "state": "applied",
            "command_id": steer.command_id,
            "turn_id": steer.turn_id,
            "mechanism": STEER_MECHANISM,
            "native_hook": "PreInvocation",
        });
        let _ = steer.reply.send(Ok(result));
    }

    if !selection_known {
        return Ok(());
    }

    let mut data = serde_json::json!({
        "turn_id": binding.turn.clone(),
        "consumer_id": consumer_id.clone(),
    });
    if !context.is_empty() {
        let outcome = ContextOutcome {
            mechanism: STEER_MECHANISM.to_owned(),
            application_point: InjectPoint::TurnSafePoint,
            consumer_id: consumer_id.clone(),
        };
        let outcome = serde_json::to_value(outcome)
            .map_err(|_| Error::new("internal", "Antigravity hook outcome could not be encoded"))?;
        data.as_object_mut()
            .ok_or_else(|| Error::new("internal", "Antigravity hook receipt is not an object"))?
            .insert("context_outcome".to_owned(), outcome);
    }
    state
        .lock()
        .await
        .commit("hook.before_model", &binding.turn, &consumer_id, data)
        .await
}

fn unknown_steer_result(steer: &PendingSteer, reason: &str) -> Value {
    serde_json::json!({
        "state": "unknown",
        "command_id": steer.command_id,
        "turn_id": steer.turn_id,
        "mechanism": STEER_MECHANISM,
        "native_hook": "PreInvocation",
        "reason": reason,
    })
}

fn context_hook_output(context: &[ContextItem]) -> HookOutput {
    HookOutput {
        inject_steps: context
            .iter()
            .map(|item| HookStep::system_message(item.text.clone()))
            .collect(),
    }
}

fn hook_ipc_response_fits(output: &HookOutput) -> bool {
    let response = IpcQueryResponse {
        accepted: true,
        output: output.clone(),
    };
    serde_json::to_vec(&response)
        .is_ok_and(|bytes| bytes.len().saturating_add(1) <= MAX_HOOK_FRAME_BYTES)
}
