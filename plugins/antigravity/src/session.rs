use std::{sync::Arc, time::Duration};

use eliot_core::{Error, InjectPoint, Result, canonical_json_sha256};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, MAX_TEXT_BYTES,
    PeerClient,
<    session::{
        native_no_write, register_native_process, unknown_snapshot, with_claimed_context,
        with_context_points,
    },
};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{
    events::{self, Shared},
    host::TIMEOUT,
    native::{
        InitializeResult, NativeClient, NativeFrame, NativeProcess, PromptResult, SessionResult,
        SetConfigOptionResult, StreamState, prompt_params,
    },
    profile::Plan,
};

/// Bounded deadline for the terminal `session/prompt` response. Unlike the
/// acceptance-only RPCs elsewhere (30 s), a prompt response is the turn's
/// terminal fact, so it gets a long but finite bound; process termination
/// settles it earlier via the transport's pending-call failure.
const PROMPT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Best-effort deadline for the graceful `session/close` on terminate.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

struct RetainedCommand {
    seq: i64,
    command_id: String,
    payload_sha256: [u8; 32],
    turn: String,
    outcome: Result<Value>,
}

#[derive(Debug, PartialEq, Eq)]
enum HistoryDecision<'a> {
    RetainedOutcome(&'a Result<Value>),
    Conflict,
    EvictedHistory,
    Advance,
}

fn check_history<'a>(
    retained: &'a Option<RetainedCommand>,
    highwater_seq: i64,
    seq: i64,
    command_id: &str,
    payload_sha256: [u8; 32],
) -> HistoryDecision<'a> {
    if let Some(retained) = retained {
        if retained.seq == seq
            && retained.command_id == command_id
            && retained.payload_sha256 == payload_sha256
        {
            return HistoryDecision::RetainedOutcome(&retained.outcome);
        }
        if retained.seq == seq || retained.command_id == command_id {
            return HistoryDecision::Conflict;
        }
    }
    if seq <= highwater_seq {
        return HistoryDecision::EvictedHistory;
    }
    HistoryDecision::Advance
}

pub(crate) struct Session {
    pub agent: String,
    pub generation: i64,
    start_command: String,
    scope: String,
    conversation: String,
    native: NativeProcess,
    client: NativeClient,
    state: Shared,
    frames: Option<mpsc::Receiver<NativeFrame>>,
    stream: watch::Receiver<StreamState>,
    task: Option<JoinHandle<()>>,
    prompt_task: Option<JoinHandle<()>>,
    ready: bool,
    highwater_seq: i64,
    retained: Option<RetainedCommand>,
    interrupt_retained: Option<RetainedCommand>,
    stop: Option<(String, bool, Result<Value>)>,
}

impl Session {
    pub async fn spawn(
        params: &AgentStartParams,
        plan: &Plan,
        peer: &PeerClient,
        credential: &str,
    ) -> Result<Self> {
        let launch = eliot_plugin::native_stdio::NativeLaunch {
            program: plan.launch.program.clone(),
            args: plan.launch.args.clone(),
            cwd: plan.launch.cwd.clone(),
            environment: plan.launch.environment.clone(),
        };
        let mut native = NativeProcess::spawn(launch, plan.policy.clone()).await?;
        let client = native.client();
        let frames = native.take_frames()?;
        let stream = native.stream_state();
        let state = Arc::new(Mutex::new(events::State::new(
            peer.clone(),
            credential.into(),
            params.agent_id.clone(),
            params.generation,
            plan.policy.clone(),
            String::new(),
        )));
        Ok(Self {
            agent: params.agent_id.clone(),
            generation: params.generation,
            start_command: params.command_id.clone(),
            scope: plan.scope.clone(),
            conversation: String::new(),
            native,
            client,
            state,
            frames: Some(frames),
            stream,
            task: None,
            prompt_task: None,
            ready: false,
            highwater_seq: 0,
            retained: None,
            interrupt_retained: None,
            stop: None,
        })
    }

    pub async fn start(
        &mut self,
        plan: &Plan,
        peer: &PeerClient,
        credential: &str,
    ) -> Result<Value> {
        register_native_process(
            peer,
            credential,
            &self.agent,
            self.generation,
            self.native.pid(),
            "Antigravity",
            "ACP",
            "conversation",
            TIMEOUT,
        )
        .await?;
        let deadline = Instant::now() + TIMEOUT;
        let init_value = self
            .client
            .call(
                "initialize",
                json!({
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": false, "writeTextFile": false},
                        "terminal": false,
                        "auth": {"terminal": false},
                    },
                    "clientInfo": {"name": "eliot-antigravity", "version": env!("CARGO_PKG_VERSION")},
                }),
                deadline,
            )
            .await?;
        let init: InitializeResult = serde_json::from_value(init_value).map_err(|_| {
            Error::new(
                "unknown",
                "Antigravity ACP initialize response did not match its envelope",
            )
        })?;
        let session_value = if let Some(resume) = plan.resume() {
            self.client
                .call(
                    "session/load",
                    json!({"cwd": plan.launch.cwd, "mcpServers": [], "sessionId": resume}),
                    deadline,
                )
                .await?
        } else {
            self.client
                .call(
                    "session/new",
                    json!({"cwd": plan.launch.cwd, "mcpServers": []}),
                    deadline,
                )
                .await?
        };
        let session: SessionResult = serde_json::from_value(session_value).map_err(|_| {
            Error::new(
                "unknown",
                "Antigravity ACP session response did not match its envelope",
            )
        })?;
        let effective = plan.check_session(&init, &session)?;
        self.conversation = session.session_id.clone();
        // Verify each required config write by its echoed current value.
        for (config_id, value) in plan.config_writes() {
            let echoed = self
                .client
                .call(
                    "session/set_config_option",
                    json!({"sessionId": self.conversation, "configId": config_id, "value": value}),
                    deadline,
                )
                .await?;
            let echoed: SetConfigOptionResult = serde_json::from_value(echoed).map_err(|_| {
                Error::new(
                    "unknown",
                    "Antigravity ACP set_config_option response did not match its envelope",
                )
            })?;
<            let verified = echoed
                .config_options
                .iter()
                .find(|option| option.id == config_id)
                .and_then(|option| option.current_value_str())
                .is_some_and(|current| current == value);
            if !verified {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP config write was not echoed by the server",
                ));
            }
        }
        // Session mode is a launch-profile fact only when it names an ACP
        // mode the server actually advertised; anything else (e.g. CLI
        // `auto`) leaves the server's current mode and records it as-is.
        if let Some(mode) = plan.mode().filter(|mode| *mode != "auto")
            && session
                .modes
                .as_ref()
                .is_some_and(|modes| modes.available_modes.iter().any(|m| m.id == *mode))
        {
            self.client
                .call(
                    "session/set_mode",
                    json!({"sessionId": self.conversation, "modeId": mode}),
                    deadline,
                )
                .await?;
        }
        let effective = with_context_points(effective, &[InjectPoint::NextInput]);
        if *self.stream.borrow() != StreamState::Open {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP stream is already incomplete",
            ));
        }
        {
            let mut state = self.state.lock().await;
            state.conversation = self.conversation.clone();
            state.ready(&self.scope, effective.clone()).await?;
        }
        let frames = self
            .frames
            .take()
            .ok_or_else(|| Error::new("internal", "Antigravity reader disappeared"))?;
        self.task = Some(events::spawn(
            frames,
            self.stream.clone(),
            self.state.clone(),
            self.client.clone(),
        ));
        self.ready = true;
        Ok(
            json!({"agent_id": self.agent, "generation": self.generation,
            "command_id": self.start_command, "state": "ready", "native_scope": self.scope,
            "native_ref": self.conversation, "requested": plan.requested(), "effective": effective}),
        )
    }

    pub async fn command(&mut self, params: AgentCommandParams) -> Result<Value> {
        match params.verb.as_str() {
            "send" => self.send(params).await,
            "interrupt" => self.interrupt(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Antigravity supports only send and interrupt; no native steer, inject or configure channel is connected",
            )),
        }
    }

    pub async fn send(&mut self, params: AgentCommandParams) -> Result<Value> {
        if params.turn_id.is_some()
            || params.request_id.is_some()
            || params.args.as_object().is_none_or(|args| args.len() != 1)
        {
            return Err(Error::new(
                "invalid",
                "Antigravity send accepts only args.text, without a turn or request target",
            ));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Antigravity send requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity input exceeds the text bound",
            ));
        }
        if params.command_id == self.start_command
            || self.stop.as_ref().is_some_and(|s| s.0 == params.command_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        let seq = match params.dispatch_seq {
            Some(seq) if seq > 0 => seq,
            _ => {
                return Err(Error::new(
                    "invalid",
                    "Antigravity send requires a positive dispatch sequence",
                ));
            }
        };
        let payload_sha256 = canonical_json_sha256(&params.args).map_err(|_| {
            Error::new(
                "invalid",
                "Antigravity command arguments cannot be canonicalized",
            )
        })?;
        match check_history(
            &self.retained,
            self.highwater_seq,
            seq,
            &params.command_id,
            payload_sha256,
        ) {
            HistoryDecision::RetainedOutcome(outcome) => return outcome.clone(),
            HistoryDecision::Conflict => {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity command binding conflicts with retained record",
                ));
            }
            HistoryDecision::EvictedHistory => {
                return Err(Error::new(
                    "unknown",
                    "Antigravity command private history is unavailable",
                ));
            }
            HistoryDecision::Advance => {}
        }
        if self.native.transport.process_exit_observed() {
            return Err(Error::new(
                "unknown",
                "Antigravity process exited; new input was not dispatched",
            ));
        }
        if !self.ready {
            return Err(Error::new(
                "unknown",
                "Antigravity session readiness is unresolved",
            ));
        }
        let combined = with_claimed_context(text, &params.context, "Antigravity")?;
        let turn = format!("agy-{}", ulid::Ulid::new());
        let associated = {
            let mut state = self.state.lock().await;
            let associated = state.prepare(params.command_id.clone(), turn.clone())?;
            state.attach_context(&params.command_id, params.context.clone())?;
            associated
        };
        self.highwater_seq = seq;
        let unknown = Err(Error::new(
            "unknown",
            "Antigravity input may have applied and will not be resent",
        ));
        self.retained = Some(RetainedCommand {
            seq,
            command_id: params.command_id.clone(),
            payload_sha256,
            turn: turn.clone(),
            outcome: unknown.clone(),
        });
        // The terminal `session/prompt` response settles the turn from a
        // owned task; this send only waits for the first association, as
        // before. A slow first update keeps the retained `unknown` outcome
        // while the task still completes the turn through the event state.
        let client = self.client.clone();
        let state = self.state.clone();
        let session_id = self.conversation.clone();
        let prompt = prompt_params(&session_id, &combined);
        self.prompt_task = Some(tokio::spawn(async move {
            let result = client
                .call("session/prompt", prompt, Instant::now() + PROMPT_TIMEOUT)
                .await;
            let mut state = state.lock().await;
            match result {
                Ok(value) => match serde_json::from_value::<PromptResult>(value) {
                    Ok(prompt) => state.complete_prompt(&prompt.stop_reason).await,
                    Err(_) => {
                        state
                            .fail_prompt(Error::new(
                                "unknown",
                                "Antigravity prompt response did not match its envelope",
                            ))
                            .await;
                    }
                },
                Err(error) => state.fail_prompt(error).await,
            }
        }));
        let result = match timeout_at(Instant::now() + TIMEOUT, associated).await {
            Ok(Ok(Ok(()))) => Ok(json!({"state": "applied", "turn_id": turn})),
            Ok(Ok(Err(error))) if native_no_write(&error.code) => {
                self.state
                    .lock()
                    .await
                    .withdraw_unwritten(&params.command_id)?;
                Ok(json!({"state": "rejected", "reason": "Antigravity input was not dispatched"}))
            }
            _ => unknown,
        };
        if let Some(retained) = self.retained.as_mut().filter(|r| r.seq == seq) {
            retained.outcome = result.clone();
        }
        result
    }

    /// Interrupt the exact current Eliot turn via the ACP `session/cancel`
    /// notification. The notification proves dispatch only; this command
    /// then waits (bounded) for the prompt task's actual settlement of
    /// this exact turn, and only `cancelled` proves the interrupt.
    /// Generation/session/turn races (e.g. the turn already ended) surface
    /// through that settlement and snapshots, never through this command's
    /// ack.
    pub async fn interrupt(&mut self, params: AgentCommandParams) -> Result<Value> {
        if params.request_id.is_some()
            || !params.args.as_object().is_none_or(|args| args.is_empty())
        {
            return Err(Error::new(
                "invalid",
                "Antigravity interrupt accepts no request target or args",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new("stale_turn", "turn/interrupt requires a captured turn id")
            })?;
        {
            let state = self.state.lock().await;
            if state.active_turn() != Some(turn_id) {
                return Err(Error::new(
                    "stale_turn",
                    "turn/interrupt does not match the exact active native turn",
                ));
            }
        }
        if params.command_id == self.start_command
            || self.stop.as_ref().is_some_and(|s| s.0 == params.command_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        let seq = match params.dispatch_seq {
            Some(seq) if seq > 0 => seq,
            _ => {
                return Err(Error::new(
                    "invalid",
                    "Antigravity interrupt requires a positive dispatch sequence",
                ));
            }
        };
        let payload_sha256 = canonical_json_sha256(&params.args).map_err(|_| {
            Error::new(
                "invalid",
                "Antigravity command arguments cannot be canonicalized",
            )
        })?;
        match check_history(
            &self.interrupt_retained,
            self.highwater_seq,
            seq,
            &params.command_id,
            payload_sha256,
        ) {
            HistoryDecision::RetainedOutcome(outcome) => return outcome.clone(),
            HistoryDecision::Conflict => {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity interrupt binding conflicts with retained record",
                ));
            }
            HistoryDecision::EvictedHistory => {
                return Err(Error::new(
                    "unknown",
                    "Antigravity interrupt private history is unavailable",
                ));
            }
            HistoryDecision::Advance => {}
        }
        // Re-check the exact turn under the same lock discipline as send:
        // a turn that ended since the first check was never interrupted.
        {
            let state = self.state.lock().await;
            if state.active_turn() != Some(turn_id) {
                return Err(Error::new(
                    "stale_turn",
                    "turn/interrupt does not match the exact active native turn",
                ));
            }
        }
        self.highwater_seq = seq;
        let dispatched = self
            .client
            .notify(
                "session/cancel",
                Some(json!({"sessionId": self.conversation})),
                Instant::now() + TIMEOUT,
            )
            .await;
        let result = match dispatched {
            Ok(()) => {
                // The notification write proves dispatch only, never
                // cancellation. Wait (bounded, lock-free between polls)
                // for the prompt task's actual settlement of this exact
                // turn: only `cancelled` proves the interrupt; natural
                // completion, failure, or no settlement stay unknown.
                let deadline = Instant::now() + TIMEOUT;
                let settled = loop {
                    let status = {
                        let state = self.state.lock().await;
                        state.settlement_for(turn_id).map(str::to_owned)
                    };
                    if status.is_some() {
                        break status;
                    }
                    if Instant::now() >= deadline {
                        break None;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                };
                match settled.as_deref() {
                    Some("cancelled") => Ok(json!({"state": "applied", "turn_id": turn_id})),
                    Some(_) => Err(Error::new(
                        "unknown",
                        "Antigravity interrupt raced natural turn completion",
                    )),
                    None => Err(Error::new(
                        "unknown",
                        "Antigravity interrupt settlement is unknown",
                    )),
                }
            }
            Err(error) if native_no_write(&error.code) => Ok(
                json!({"state": "rejected", "reason": "Antigravity interrupt was not dispatched"}),
            ),
            Err(error) => Err(self.client.diagnostic_error(error.with_code("unknown"))),
        };
        self.interrupt_retained = Some(RetainedCommand {
            seq,
            command_id: params.command_id.clone(),
            payload_sha256,
            turn: turn_id.to_owned(),
            outcome: result.clone(),
        });
        result
    }

    pub async fn snapshot(&self, params: AgentSnapshotParams) -> Result<Value> {
        let state = self.state.lock().await;
        let process_exited = self.native.transport.process_exit_observed();
        let retained = self
            .retained
            .as_ref()
            .filter(|r| r.command_id == params.command_id);
        let interrupt = self
            .interrupt_retained
            .as_ref()
            .filter(|r| r.command_id == params.command_id);
        let stop = self.stop.as_ref().is_some_and(|s| s.0 == params.command_id);
        if params.command_id != self.start_command
            && retained.is_none()
            && interrupt.is_none()
            && !stop
        {
            return unknown_snapshot(&params, "Antigravity command identity is not retained");
        }
        let turn = params
            .turn_id
            .as_deref()
            .or_else(|| retained.map(|r| r.turn.as_str()))
            .or_else(|| interrupt.map(|r| r.turn.as_str()));
        if let Some(turn) = turn {
            let proven = retained.is_some_and(|r| r.turn == turn)
                || interrupt.is_some_and(|r| r.turn == turn);
            if !proven {
                return unknown_snapshot(
                    &params,
                    "Antigravity snapshot cannot prove the exact requested turn",
                );
            }
        }
        let last = state
            .last_turn
            .as_ref()
            .filter(|last| turn.is_none_or(|turn| last.turn_id == turn));
        let result_known = last.is_some_and(|last| last.terminal);
        let status = if state.stopped {
            "stopped"
        } else if state.source_unknown || process_exited || !self.ready {
            "unknown"
        } else if state.active_turn().is_some() {
            "busy"
        } else {
            "idle"
        };
        Ok(
            json!({"agent_id": self.agent, "generation": self.generation, "command_id": params.command_id,
            "state": status, "native_scope": self.scope, "native_ref": state.conversation,
            "active_turn": if state.stopped { None } else { state.active_turn() }, "last_turn": last,
            "source_unknown": state.source_unknown, "batch_unknown": state.batch_unknown,
            "source_health_stale": state.source_health_stale,
            "source_health_unknown": state.source_health_unknown,
            "process_exited": process_exited,
            "result_known": result_known, "usage": Value::Null,
            "diagnostics": self.client.diagnostic_error(Error::new("unknown", "")).diagnostics}),
        )
    }

    pub async fn stop(&mut self, params: AgentStopParams) -> Result<Value> {
        if let Some((previous, force, result)) = &self.stop {
            if previous != &params.command_id || *force != params.force {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity stop inputs changed after dispatch",
                ));
            }
            return result.clone();
        }
        if params.command_id == self.start_command
            || self
                .retained
                .as_ref()
                .is_some_and(|r| r.command_id == params.command_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        self.stop = Some((
            params.command_id.clone(),
            params.force,
            Err(Error::new("unknown", "Antigravity stop is unresolved")),
        ));
        let result = self
            .terminate(params.force)
            .await
            .map(|()| json!({"state": "applied"}));
        self.stop = Some((params.command_id, params.force, result.clone()));
        result
    }

    pub async fn terminate(&mut self, force: bool) -> Result<()> {
        self.state.lock().await.stopping = true;
        let deadline = Instant::now() + TIMEOUT;
        if self.ready && !self.conversation.is_empty() {
            let _ = self
                .client
                .call(
                    "session/close",
                    json!({"sessionId": self.conversation}),
                    Instant::now() + CLOSE_TIMEOUT,
                )
                .await;
        }
        self.native
            .terminate(force, deadline)
            .await
            .map_err(|error| self.client.diagnostic_error(error.with_code("unknown")))?;
        if let Some(task) = self.prompt_task.take() {
            let _ = timeout_at(deadline, task).await;
        }
        if let Some(task) = self.task.as_mut() {
            timeout_at(deadline, task)
                .await
                .map_err(|_| Error::new("unknown", "Antigravity event drain deadline elapsed"))?
                .map_err(|_| Error::new("unknown", "Antigravity event reader failed"))?;
            self.task = None;
        }
        if self.ready {
            let mut state = self.state.lock().await;
            state.ended().await?;
        }
        self.ready = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn command_history_tracking_and_conflicts() {
        let payload = json!({"text": "hello world"});
        let payload_sha256 = canonical_json_sha256(&payload).unwrap();
        let outcome: Result<Value> = Ok(json!({"state": "applied", "turn_id": "turn-1"}));

        let retained = Some(RetainedCommand {
            seq: 10,
            command_id: "cmd-1".to_owned(),
            payload_sha256,
            turn: "turn-1".to_owned(),
            outcome: outcome.clone(),
        });

        // Exact match
        let decision = check_history(&retained, 10, 10, "cmd-1", payload_sha256);
        match decision {
            HistoryDecision::RetainedOutcome(out) => {
                assert_eq!(out.as_ref().unwrap()["state"], "applied");
            }
            _ => panic!("expected exact retained outcome"),
        }

        // Conflict: same seq, different command_id
        let decision = check_history(&retained, 10, 10, "cmd-diff", payload_sha256);
        assert!(matches!(decision, HistoryDecision::Conflict));

        // Conflict: same command_id, different seq
        let decision = check_history(&retained, 10, 15, "cmd-1", payload_sha256);
        assert!(matches!(decision, HistoryDecision::Conflict));

        // Conflict: same seq and command_id, different payload
        let other_payload = json!({"text": "tampered"});
        let other_sha256 = canonical_json_sha256(&other_payload).unwrap();
        let decision = check_history(&retained, 10, 10, "cmd-1", other_sha256);
        assert!(matches!(decision, HistoryDecision::Conflict));

        // Evicted history: seq <= highwater_seq (not retained)
        let decision = check_history(&retained, 10, 5, "cmd-old", other_sha256);
        assert!(matches!(decision, HistoryDecision::EvictedHistory));

        // Advance: higher seq > highwater_seq
        let decision = check_history(&retained, 10, 20, "cmd-new", other_sha256);
        assert!(matches!(decision, HistoryDecision::Advance));
    }

    #[test]
    fn thousands_of_commands_keep_constant_memory_and_evict_old() {
        let mut highwater_seq = 0;
        let mut retained: Option<RetainedCommand> = None;

        for i in 1..=2000 {
            let seq = i * 10;
            let cmd_id = format!("cmd-{seq}");
            let payload = json!({"text": format!("prompt {seq}")});
            let payload_sha256 = canonical_json_sha256(&payload).unwrap();

            let decision = check_history(&retained, highwater_seq, seq, &cmd_id, payload_sha256);
            assert!(matches!(decision, HistoryDecision::Advance));

            highwater_seq = seq;
            retained = Some(RetainedCommand {
                seq,
                command_id: cmd_id,
                payload_sha256,
                turn: format!("turn-{seq}"),
                outcome: Ok(json!({"state": "applied"})),
            });
        }

        assert_eq!(retained.as_ref().unwrap().seq, 20_000);
        assert_eq!(retained.as_ref().unwrap().command_id, "cmd-20000");

        let old_payload = json!({"text": "prompt 10"});
        let old_sha256 = canonical_json_sha256(&old_payload).unwrap();
        let decision = check_history(&retained, highwater_seq, 10, "cmd-10", old_sha256);
        assert!(matches!(decision, HistoryDecision::EvictedHistory));
    }

    #[test]
    fn lost_write_unknown_returns_same_outcome_without_retry() {
        let payload = json!({"text": "lost prompt"});
        let payload_sha256 = canonical_json_sha256(&payload).unwrap();
        let unknown_outcome: Result<Value> = Err(Error::new("unknown", "input may have applied"));

        let retained = Some(RetainedCommand {
            seq: 42,
            command_id: "cmd-lost".to_owned(),
            payload_sha256,
            turn: "turn-lost".to_owned(),
            outcome: unknown_outcome,
        });
        let decision = check_history(&retained, 42, 42, "cmd-lost", payload_sha256);
        match decision {
            HistoryDecision::RetainedOutcome(Err(err)) => {
                assert_eq!(err.code, "unknown");
            }
            _ => panic!("expected retained unknown error outcome"),
        }
    }
}
