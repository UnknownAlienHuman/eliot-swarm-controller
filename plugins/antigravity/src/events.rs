use std::{collections::HashMap, sync::Arc};

use eliot_core::{ContextItem, Error, Result, SecretPolicy, SourceHealthUpdate};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    native_transport::StreamState,
    session::{EVENT_DEADLINE, StreamSource, attach_final_text, event_id, next_input_outcomes},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    time::Instant,
};

use crate::native::{AcpUpdate, AgyClient, AgyFrame, FencedGuard, PromptResult, classify_update};

pub(crate) const FAMILIES: &[&str] = &["session", "turn", "message", "tool"];
pub(crate) type Shared = Arc<Mutex<State>>;

/// Upper bound on accumulated assistant text per turn. Beyond this the
/// tail is dropped and the turn carries `final_text_unavailable` instead
/// of an unbounded payload.
const MAX_TURN_TEXT_BYTES: usize = 256 * 1024;

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
    context: Vec<ContextItem>,
    associated: bool,
    reply: Option<oneshot::Sender<Result<()>>>,
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
    /// True from fenced `session/load` dispatch until its ordered reply
    /// frame is consumed. Matching-session updates in this window are
    /// validated load history and are discarded, never republished.
    loading: bool,
    /// RAII registration of the outstanding fenced load call.
    load_guard: Option<FencedGuard>,
    /// Ordered load outcome for `Session::start`, published exactly once
    /// by the event task when the fenced load reply is consumed.
    load_tx: watch::Sender<Option<Result<Value>>>,
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
            tool_emitted: std::collections::HashSet::new(),
            tools: HashMap::new(),
            fenced_guard: None,
            loading: false,
            load_guard: None,
            load_tx,
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
        }
    }

    pub fn settlement_for(&self, turn: &str) -> Option<&str> {
        self.settlement
            .as_ref()
            .filter(|(settled, _)| settled == turn)
            .map(|(_, status)| status.as_str())
    }

    /// A fresh receiver for the ordered load outcome published by the
    /// event task. `Session::start` waits on this, never on the RPC
    /// return alone.
    pub fn load_receiver(&self) -> watch::Receiver<Option<Result<Value>>> {
        self.load_tx.subscribe()
    }

    /// Arm the load gate before fenced dispatch: validated history for
    /// the retained session is discarded until the ordered load reply
    /// is consumed.
    pub fn begin_load(&mut self, guard: FencedGuard) {
        self.load_guard = Some(guard);
        self.loading = true;
    }

    /// Retire a load that never reached the ordered reply (dispatch
    /// failure or start timeout): loading mode ends without history
    /// having been validated, so startup fails instead of going live.
    pub fn abort_load(&mut self) {
        self.loading = false;
        self.load_guard = None;
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
        let (reply, result) = oneshot::channel();
        self.pending = Some(Pending {
            command,
            turn,
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
            AgyFrame::Update(params) => self.update(&params).await,
            // The ordered load boundary: consumed after every history
            // update ahead of it on the wire. Publish the outcome for
            // `Session::start` and leave loading mode; nothing here
            // republishes history or creates a turn.
            AgyFrame::LoadResponse(result) => {
                self.loading = false;
                self.load_guard = None;
                let _ = self.load_tx.send_replace(Some(result));
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
        self.check_conversation(&session_id)?;
        // Validated load history: matching-session updates before the
        // ordered load reply are history, not live output. Discard them
        // without association, republication, or turn creation. Live
        // updates after the reply take the normal path below.
        if self.loading {
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
        let text = self.policy.redact_text(&self.text_accum);
        let truncated = self.text_truncated;
        let mut data = json!({"command_id": command, "turn_id": turn,
            "status": status, "native_status": result.stop_reason});
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
        let final_text = data
            .get("final_text")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.last_turn = Some(TurnFacts {
            command_id: command,
            turn_id: turn.clone(),
            status: status.to_owned(),
            final_text: final_text.unwrap_or_default(),
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
        self.loading = false;
        self.load_guard = None;
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
        self.clear_fenced();
        self.loading = false;
        self.load_guard = None;
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
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Observe watch faults while active, including with no incoming
        // frames: a record-local Malformed keeps the reader open, so an
        // EOF-only check would let it vanish while later facts commit.
        // No model-turn timer lives here: stream silence is not staleness
        // and a wait timeout never cancels admitted work; the fenced
        // terminal reply, an error, a stream fault, stop, or drop are the
        // only settlements.
        let mut stream_open = true;
        loop {
            let frame = tokio::select! {
                frame = frames.recv() => frame,
                changed = stream.changed(), if stream_open => {
                    if changed.is_err() {
                        stream_open = false;
                    }
                    let current = *stream.borrow();
                    let mut state = state.lock().await;
                    match current {
                        StreamState::Overflow => {
                            state.lose("Antigravity native queue overflowed").await;
                        }
                        StreamState::Malformed(fault) => {
                            state.lose(&format!(
                                "Antigravity stream contained an invalid frame ({})",
                                fault.label()
                            )).await;
                        }
                        _ => {}
                    }
                    continue;
                }
            };
            let Some(frame) = frame else { break };
            let current = *stream.borrow();
            let mut state = state.lock().await;
            match current {
                StreamState::Overflow => {
                    state.lose("Antigravity native queue overflowed").await;
                }
                StreamState::Malformed(fault) => {
                    state
                        .lose(&format!(
                            "Antigravity stream contained an invalid frame ({})",
                            fault.label()
                        ))
                        .await;
                }
                _ => {}
            }
            if let Err(error) = state.observe(frame, &client).await {
                state.lose(&error.message).await;
            }
        }
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
