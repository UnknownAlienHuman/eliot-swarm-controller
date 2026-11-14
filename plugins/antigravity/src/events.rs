use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, Result, SecretPolicy, SessionUsage,
    SourceHealthUpdate,
};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, MAX_FRAME_BYTES, PeerClient,
    native_transport::{
        ProcessBirthIdentity, StreamState, process_birth_boundary, process_birth_identity,
    },
    protocol::{HookEvent, HookResponse},
    session::{EVENT_DEADLINE, StreamSource, attach_final_text, event_id, next_input_outcomes},
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
        AcpUpdate, AgyClient, AgyFrame, FencedGuard, PromptResult, classify_update,
        session_usage_from_update,
    },
};
use ulid::Ulid;

pub(crate) const FAMILIES: &[&str] = &["session", "turn", "message", "tool"];
pub(crate) const STEER_MECHANISM: &str = "antigravity.PreInvocation.injectSteps.systemMessage";
pub(crate) type Shared = Arc<Mutex<State>>;

/// Upper bound on accumulated assistant text per turn. Beyond this the
/// tail is dropped and the turn carries `final_text_unavailable` instead
/// of an unbounded payload.
const MAX_TURN_TEXT_BYTES: usize = 256 * 1024;
const MAX_UNBOUND_UPDATES: usize = 32;
const MAX_UNBOUND_UPDATE_BYTES: usize = 256 * 1024;

pub(crate) type LoadOutcome = (String, Result<Value>);

struct QueuedUpdate {
    session_id: String,
    update: AcpUpdate,
}

#[derive(Clone, Serialize)]
pub(crate) struct TurnFacts {
    pub command_id: String,
    pub turn_id: String,
    pub status: String,
    pub final_text: String,
    pub terminal: bool,
}

struct Pending {
    command: String,
    turn: String,
    opened_at: u64,
    context: Vec<ContextItem>,
    associated: bool,
    reply: Option<oneshot::Sender<Result<()>>>,
}

struct PendingSteer {
    command_id: String,
    turn_id: String,
    text: String,
    reply: oneshot::Sender<Result<Value>>,
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
    pending: Option<Pending>,
    pending_steer: Option<PendingSteer>,
    /// Lifecycle transitions already emitted per tool call, keyed
    /// `"{tool_call_id}:{kind}"`. Each transition is emitted once;
    /// later metadata deltas never re-emit.
    tool_emitted: std::collections::HashSet<String>,
    /// Partial tool-call deltas merged per vendor id before any event is
    /// derived: `(title, status)` as last known. Updates may omit either
    /// field, so absence alone is never a protocol gap.
    tools: HashMap<String, (String, String)>,
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
    text_truncated: bool,
    writes_blocked: bool,
    gap_reported: bool,
    gap_number: u64,
    source_health_attempted: bool,
    pub source_health_stale: bool,
    pub source_health_unknown: bool,
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
    pub settlement: Option<(String, String)>,
    /// Latest merged session-scoped snapshot from `usage_update`: context
    /// occupancy/capacity and cumulative session cost. Never turn expenditure;
    /// committed as `session.usage` when accepted and replaced wholesale on
    /// newer updates.
    session_usage: Option<SessionUsage>,
    /// Last committed session snapshot content. Each accepted update commits
    /// only when it differs, so repeats commit no extra session fact.
    session_usage_committed: Option<SessionUsage>,
    /// Monotonic counter for `session.usage` native ids so each accepted
    /// update commits under a unique identity.
    session_usage_seq: u64,
}

impl State {
    pub fn new(
        host: PeerClient,
        credential: String,
        agent: String,
        generation: i64,
        policy: SecretPolicy,
        conversation: String,
    ) -> Self {
        let (load_tx, _) = watch::channel(None);
        Self {
            sink: CommitSink::new(host.clone(), credential.clone()),
            host,
            credential,
            agent,
            generation,
            policy,
            conversation,
            pending: None,
            pending_steer: None,
            tool_emitted: std::collections::HashSet::new(),
            tools: HashMap::new(),
            fenced_guard: None,
            load_request_id: None,
            load_guard: None,
            load_tx,
            unbound_updates: VecDeque::new(),
            unbound_update_bytes: 0,
            text_accum: String::new(),
            text_truncated: false,
            writes_blocked: false,
            gap_reported: false,
            gap_number: 0,
            source_health_attempted: false,
            source_health_stale: false,
            source_health_unknown: false,
            failure: None,
            source_unknown: false,
            batch_unknown: false,
            stopping: false,
            stopped: false,
            last_turn: None,
            settlement: None,
            session_usage: None,
            session_usage_committed: None,
            session_usage_seq: 0,
        }
    }

    pub fn settlement_for(&self, turn: &str) -> Option<&str> {
        self.settlement
            .as_ref()
            .filter(|(settled, _)| settled == turn)
            .map(|(_, status)| status.as_str())
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

    pub fn contains_secret(&self, text: &str) -> bool {
        self.policy.contains_text(text)
    }

    fn capture_hook_turn(
        &self,
        input: &HookInput,
        origin: ProcessBirthIdentity,
    ) -> Option<HookTurnBinding> {
        if self.conversation != input.conversation_id || input.conversation_id.trim().is_empty() {
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
        if self.stopping || self.stopped {
            return Err(Error::new("bad_state", "Antigravity session is stopping"));
        }
        if self.pending.is_some() {
            return Err(Error::new("busy", "Antigravity has one outstanding input"));
        }
        let opened_at = process_birth_boundary()?;
        let (reply, result) = oneshot::channel();
        self.pending = Some(Pending {
            command,
            turn,
            opened_at,
            context: Vec::new(),
            associated: false,
            reply: Some(reply),
        });
        self.tool_emitted.clear();
        self.tools.clear();
        self.clear_fenced();
        self.text_accum.clear();
        self.text_truncated = false;
        Ok(result)
    }

    /// Carry the send's FIFO-claimed context into the pending input. Called
    /// under the same lock as [`State::prepare`], before the native write;
    /// refused once the input already associated, so outcomes are never
    /// attached late or faked.
    pub fn attach_context(&mut self, command: &str, context: Vec<ContextItem>) -> Result<()> {
        match self.pending.as_mut() {
            Some(pending) if pending.command == command && !pending.associated => {
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
            .is_some_and(|p| p.command == command && !p.associated)
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
        .await
    }

    async fn associate(&mut self) -> Result<()> {
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
        let mut data = json!({"command_id": pending.command, "turn_id": turn});
        if !pending.context.is_empty() {
            data["context_outcomes"] =
                json!(next_input_outcomes(&pending.context, &pending.command));
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

    async fn observe(&mut self, frame: AgyFrame, client: &AgyClient) -> Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        match frame {
            AgyFrame::StreamFault(fault) => {
                self.lose(&format!(
                    "Antigravity stream contained an invalid frame ({})",
                    fault.describe()
                ))
                .await;
                Ok(())
            }
            AgyFrame::Update(params) => self.update(&params).await,
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
            // The fenced terminal reply, consumed in wire order behind
            // every preceding update: it settles the prepared turn only
            // after those updates have been applied.
            AgyFrame::PromptResponse(result) => match result {
                Ok(value) => match serde_json::from_value::<PromptResult>(value) {
                    Ok(prompt) => {
                        self.complete_prompt(&prompt.stop_reason).await;
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
                // No approve/answer channel is connected on this route, so
                // every agent request is an explicit unsupported rejection
                // (never an auto-approval). A failed reply means the stream
                // itself is broken and the turn outcome is unknown.
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
        match update {
            AcpUpdate::MessageChunk { text } => {
                self.associate().await?;
                if !self.text_truncated {
                    if self.text_accum.len().saturating_add(text.len()) > MAX_TURN_TEXT_BYTES {
                        self.text_truncated = true;
                    } else {
                        self.text_accum.push_str(&text);
                    }
                }
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
                // Fold into the retained session snapshot and commit it as
                // `session.usage` when accepted, with `observed_at` captured
                // at acceptance. Session-scoped facts, never per-turn
                // input/output totals.
                let Some(merged) = session_usage_from_update(
                    used,
                    size,
                    cost_amount,
                    cost_unit,
                    self.session_usage.as_ref(),
                ) else {
                    return Ok(());
                };
                self.session_usage = Some(merged.clone());
                self.commit_session_usage_now(merged).await
            }
            AcpUpdate::ToolCall {
                tool_call_id,
                title,
                status,
            } => {
                self.merge_tool(&tool_call_id, &title, &status);
                self.tool_event(&tool_call_id, "tool.started", "running")
                    .await
            }
            AcpUpdate::ToolCallUpdate {
                tool_call_id,
                title,
                status,
            } => {
                self.merge_tool(&tool_call_id, title.as_deref().unwrap_or(""), &status);
                let (_, merged_status) = self.tools.get(&tool_call_id).cloned().unwrap_or_default();
                match merged_status.as_str() {
                    "pending" => Ok(()),
                    "in_progress" => {
                        self.tool_event(&tool_call_id, "tool.started", "running")
                            .await
                    }
                    "completed" => {
                        self.tool_event(&tool_call_id, "tool.completed", "completed")
                            .await
                    }
                    "failed" => {
                        self.tool_event(&tool_call_id, "tool.completed", "failed")
                            .await
                    }
                    _ => Err(Error::new(
                        "native_protocol",
                        "Antigravity tool call status is unknown",
                    )),
                }
            }
            AcpUpdate::Unknown(kind) => Err(Error::new(
                "native_protocol",
                format!("Antigravity session update is unconnected: {kind}"),
            )),
        }
    }

    /// Commit one accepted session snapshot as its own `session.usage` fact,
    /// so session scope and turn scope can never be confused and stream loss
    /// before the terminal cannot lose an accepted fact. Commits only when
    /// the snapshot differs from the last committed fact. Occupancy changes
    /// (compaction can reduce `used`) never read as token expenditure. Uses
    /// the pending turn when one is prepared, else the idle `session` scope.
    async fn commit_session_usage_now(&mut self, usage: SessionUsage) -> Result<()> {
        if self.session_usage_committed.as_ref() == Some(&usage) {
            return Ok(());
        }
        self.session_usage_seq = self.session_usage_seq.wrapping_add(1);
        let turn = self
            .pending
            .as_ref()
            .map(|pending| pending.turn.clone())
            .unwrap_or_else(|| "session".to_owned());
        self.commit(
            "session.usage",
            &turn,
            &format!("session-{}", self.session_usage_seq),
            json!({"usage": usage}),
        )
        .await?;
        self.session_usage_committed = Some(usage);
        Ok(())
    }

    /// Merge one partial tool-call delta into the known per-id state.
    /// Empty title/status mean "unchanged", never "cleared": an update
    /// that omits a field inherits the last known value, so only a delta
    /// that still has no known status is a real protocol gap.
    fn merge_tool(&mut self, tool_call_id: &str, title: &str, status: &str) {
        if tool_call_id.trim().is_empty() {
            return;
        }
        let known = self.tools.entry(tool_call_id.to_owned()).or_default();
        if !title.is_empty() {
            known.0 = title.to_owned();
        }
        if !status.is_empty() {
            known.1 = status.to_owned();
        }
    }

    async fn tool_event(&mut self, tool_call_id: &str, kind: &str, status: &str) -> Result<()> {
        if tool_call_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Antigravity tool call id is absent",
            ));
        }
        // Each lifecycle transition is emitted once per tool call. A
        // later delta for an already-emitted transition is metadata
        // only (already merged) and never re-emits or poisons the
        // retained comparison.
        if !self.tool_emitted.insert(format!("{tool_call_id}:{kind}")) {
            return Ok(());
        }
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let turn = pending.turn.clone();
        let title = self
            .tools
            .get(tool_call_id)
            .map(|(title, _)| title.clone())
            .unwrap_or_default();
        let item = format!("tool-{tool_call_id}");
        let mut data = json!({"command_id": pending.command, "turn_id": turn,
            "item_id": item, "tool": title, "status": status});
        self.policy.redact_values(&mut data);
        self.commit(kind, &turn, &item, data).await
    }

    /// Settle the prepared turn from the terminal `session/prompt`
    /// response. Only the actual stop reason settles the turn: `cancelled`
    /// is the interrupt settlement and is never inferred from silence.
    pub async fn complete_prompt(&mut self, stop_reason: &str) {
        if self.failure.is_some() {
            return;
        }
        let result = PromptResult {
            stop_reason: stop_reason.to_owned(),
        };
        match self.settle_prompt(&result).await {
            Ok(()) => {}
            Err(error) => self.lose(&error.message).await,
        }
    }

    async fn settle_prompt(&mut self, result: &PromptResult) -> Result<()> {
        let Some(status) = result.terminal_status()? else {
            return Ok(());
        };
        // A terminal response alone suffices: no intermediate update is
        // required, matching the old stream-json rule.
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let (command, turn) = (pending.command.clone(), pending.turn.clone());
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
        let text = self.policy.redact_text(&self.text_accum);
        let truncated = self.text_truncated;
        let mut data = json!({"command_id": command, "turn_id": turn,
            "status": status, "outcome": outcome, "native_status": result.stop_reason});
        self.policy.redact_values(&mut data);
        if !text.is_empty() && !truncated {
            self.commit(
                "message.completed",
                &turn,
                "response",
                json!({"command_id": command, "turn_id": turn, "text": text}),
            )
            .await?;
        }
        if truncated {
            data["final_text_unavailable"] = json!("event_budget");
        }
        if failed && data.get("final_text").is_none() && !text.is_empty() && !truncated {
            attach_final_text(&mut data, &text, true, &self.policy);
        }
        self.commit("turn.completed", &turn, "completed", data.clone())
            .await?;
        // Session facts already committed when each update was accepted, so
        // the terminal carries turn scope only.
        self.reject_pending_steer(
            "Antigravity turn ended before its next PreInvocation steer point",
        );
        let final_text = match data.get("final_text").and_then(Value::as_str) {
            Some(final_text) => final_text.to_owned(),
            None if !failed && !truncated => text,
            None => String::new(),
        };
        self.last_turn = Some(TurnFacts {
            command_id: command,
            turn_id: turn.clone(),
            status: status.to_owned(),
            final_text,
            terminal: true,
        });
        self.settlement = Some((turn, status.to_owned()));
        self.clear_fenced();
        self.pending = None;
        Ok(())
    }

    /// Settle a failed `session/prompt` call. An unassociated dispatch
    /// failure (e.g. rejected before admission) withdraws the pending turn
    /// and reports that exact error; anything after association loses the
    /// stream honestly instead of inventing a terminal fact.
    pub async fn fail_prompt(&mut self, error: Error) {
        if self.failure.is_some() {
            return;
        }
        self.clear_fenced();
        let unassociated = self
            .pending
            .as_ref()
            .is_some_and(|pending| !pending.associated);
        self.fail_pending_steer(Error::new(
            if unassociated {
                "stale_turn"
            } else {
                "unknown"
            },
            "Antigravity prompt ended before PreInvocation confirmed steer",
        ));
        if unassociated {
            if let Some(mut pending) = self.pending.take()
                && let Some(reply) = pending.reply.take()
            {
                self.settlement = Some((pending.turn.clone(), "unknown".to_owned()));
                let _ = reply.send(Err(error));
            }
            return;
        }
        self.lose(&error.message).await;
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

    pub async fn lose(&mut self, reason: &str) {
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
        if let Some(pending) = self.pending.as_ref() {
            self.settlement = Some((pending.turn.clone(), "unknown".to_owned()));
        }
        let health_deadline = Instant::now() + EVENT_DEADLINE;
        if !self.gap_reported && !self.writes_blocked {
            let mut source = self.source();
            let _ = source.record_gap_until(reason, health_deadline).await;
        }
        self.mark_source_stale(health_deadline).await;
        self.failure.get_or_insert_with(|| reason.to_owned());
        if let Some(reply) = self.pending.as_mut().and_then(|p| p.reply.take()) {
            let _ = reply.send(Err(Error::new(
                "unknown",
                "Antigravity input outcome is unknown",
            )));
        }
    }

    async fn mark_source_stale(&mut self, deadline: Instant) {
        if self.source_health_attempted {
            return;
        }
        self.source_health_attempted = true;
        let update =
            SourceHealthUpdate::new(self.agent.clone(), self.generation, "stream", "stale");
        let Ok(mut request) = serde_json::to_value(&update) else {
            self.source_health_unknown = true;
            return;
        };
        if update.validate().is_err() {
            self.source_health_unknown = true;
            return;
        }
        request["credential"] = json!(self.credential);
        let acknowledgement = self.host.call("source.health", request, deadline).await;
        if acknowledgement.as_ref().is_ok_and(|value| {
            serde_json::from_value::<SourceHealthUpdate>(value.clone())
                .is_ok_and(|ack| ack == update)
        }) {
            self.source_health_stale = true;
            return;
        }

        // A lost health acknowledgement is settled by readback, never by replay.
        let readback = self
            .host
            .call(
                "source.state",
                json!({
                    "credential": self.credential,
                    "agent_id": self.agent,
                    "generation": self.generation,
                    "source": "stream",
                }),
                deadline,
            )
            .await;
        let confirmed = readback.as_ref().is_ok_and(|state| {
            state.get("agent_id").and_then(Value::as_str) == Some(self.agent.as_str())
                && state.get("generation").and_then(Value::as_i64) == Some(self.generation)
                && state.get("source").and_then(Value::as_str) == Some("stream")
                && state.get("state").and_then(Value::as_str) == Some("open")
                && state.get("health").and_then(Value::as_str) == Some("stale")
        });
        self.source_health_stale = confirmed;
        self.source_health_unknown = !confirmed;
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
                    if let Err(error) =
                        handle_hook_callback(callback.callback, callback.binding, &state).await
                    {
                        state.lock().await.lose(&error.message).await;
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
                        state_guard.lose(&error.message).await;
                    }
                    drop(state_guard);
                    let current = *stream.borrow();
                    if frames.is_empty() {
                        note_stream_fault(&state, current).await;
                    }
                }
            }
        }
        hook_accept_task.abort();
        let _ = hook_accept_task.await;
        // All queued stdout is consumed before concluding loss or planned EOF.
        let mut state = state.lock().await;
        if matches!(
            *stream.borrow(),
            StreamState::Overflow | StreamState::Malformed(_)
        ) {
            state.lose("Antigravity output was incomplete").await;
        } else if !state.stopping {
            state
                .lose("Antigravity stream closed outside a planned stop")
                .await;
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
    binding: Option<HookTurnBinding>,
    state: &Shared,
) -> Result<()> {
    let Some(binding) = binding else {
        let _ = callback.respond(HookOutput::empty()).await;
        return Ok(());
    };
    let origin_is_live =
        process_birth_identity(binding.origin.pid).is_ok_and(|observed| observed == binding.origin);
    let live_binding = {
        let mut state = state.lock().await;
        if origin_is_live && state.hook_binding_is_live(&binding) {
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
