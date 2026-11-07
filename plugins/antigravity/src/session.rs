use std::sync::Arc;

use eliot_core::{Error, InjectPoint, Result, canonical_json_sha256};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, MAX_TEXT_BYTES,
    PeerClient,
    native_transport::{NativeChild, NativeLaunch, StreamState, encode_frame},
    session::{
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
    native::{AgyFrame, UserInput},
    profile::Plan,
};

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
    native: NativeChild,
    state: Shared,
    frames: Option<mpsc::Receiver<AgyFrame>>,
    stream: watch::Receiver<StreamState>,
    task: Option<JoinHandle<()>>,
    ready: bool,
    highwater_seq: i64,
    retained: Option<RetainedCommand>,
    stop: Option<(String, bool, Result<Value>)>,
}

impl Session {
    pub async fn spawn(
        params: &AgentStartParams,
        plan: &Plan,
        peer: &PeerClient,
        credential: &str,
    ) -> Result<Self> {
        let launch = NativeLaunch {
            program: plan.launch.program.clone(),
            args: plan.launch.args.clone(),
            cwd: plan.launch.cwd.clone(),
            environment: plan.launch.environment.clone(),
        };
        let (native, frames, stream) = crate::native::spawn(launch, plan.policy.clone()).await?;
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
            native,
            state,
            frames: Some(frames),
            stream,
            task: None,
            ready: false,
            highwater_seq: 0,
            retained: None,
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
            "CLI",
            "conversation",
            TIMEOUT,
        )
        .await?;
        let frames = self
            .frames
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Antigravity reader already has an owner"))?;
        let first = timeout_at(Instant::now() + TIMEOUT, frames.recv())
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    "Antigravity init deadline elapsed; no input was sent",
                )
            })?
            .ok_or_else(|| {
                self.native.diagnostic_error(Error::new(
                    "unknown",
                    "Antigravity output closed before init",
                ))
            })?;
        let AgyFrame::Init {
            conversation_id,
            init,
        } = first
        else {
            return Err(Error::new(
                "unknown",
                "Antigravity emitted output before its init",
            ));
        };
        let effective = plan.check_init(&conversation_id, &init)?;
        let effective = with_context_points(effective, &[InjectPoint::NextInput]);
        if *self.stream.borrow() != StreamState::Open {
            return Err(Error::new(
                "unknown",
                "Antigravity init stream is already incomplete",
            ));
        }
        {
            let mut state = self.state.lock().await;
            state.conversation = conversation_id.clone();
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
        ));
        self.ready = true;
        Ok(
            json!({"agent_id": self.agent, "generation": self.generation,
            "command_id": self.start_command, "state": "ready", "native_scope": self.scope,
            "native_ref": conversation_id, "requested": plan.requested(), "effective": effective}),
        )
    }

    pub async fn send(&mut self, params: AgentCommandParams) -> Result<Value> {
        if params.verb != "send" {
            return Err(Error::new(
                "unsupported",
                "Antigravity supports only send; no native approve, answer, interrupt or configure channel is connected",
            ));
        }
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
        if self.native.process_exit_observed() {
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
        let encoded = encode_frame("Antigravity", &UserInput::new(&combined))?;
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
        let result = match self
            .native
            .writer()
            .send(encoded, Instant::now() + TIMEOUT)
            .await
        {
            Ok(()) => match timeout_at(Instant::now() + TIMEOUT, associated).await {
                Ok(Ok(Ok(()))) => Ok(json!({"state": "applied", "turn_id": turn})),
                _ => unknown,
            },
            Err(error) if native_no_write(&error.code) => {
                self.state
                    .lock()
                    .await
                    .withdraw_unwritten(&params.command_id)?;
                Ok(json!({"state": "rejected", "reason": "Antigravity input was not dispatched"}))
            }
            Err(error) => Err(self.native.diagnostic_error(error.with_code("unknown"))),
        };
        if let Some(retained) = self.retained.as_mut().filter(|r| r.seq == seq) {
            retained.outcome = result.clone();
        }
        result
    }

    pub async fn snapshot(&self, params: AgentSnapshotParams) -> Result<Value> {
        let state = self.state.lock().await;
        let process_exited = self.native.process_exit_observed();
        let retained = self
            .retained
            .as_ref()
            .filter(|r| r.command_id == params.command_id);
        let stop = self.stop.as_ref().is_some_and(|s| s.0 == params.command_id);
        if params.command_id != self.start_command && retained.is_none() && !stop {
            return unknown_snapshot(&params, "Antigravity command identity is not retained");
        }
        let turn = params
            .turn_id
            .as_deref()
            .or_else(|| retained.map(|r| r.turn.as_str()));
        if let Some(turn) = turn
            && (retained.is_none_or(|r| r.turn != turn)
                || !(state.active_turn() == Some(turn)
                    || state
                        .last_turn
                        .as_ref()
                        .is_some_and(|last| last.turn_id == turn)))
        {
            return unknown_snapshot(
                &params,
                "Antigravity snapshot cannot prove the exact requested turn",
            );
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
            "result_known": result_known, "usage": state.usage,
            "diagnostics": self.native.diagnostic_error(Error::new("unknown", "")).diagnostics}),
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
        self.native
            .terminate_dropping_input(force, deadline)
            .await
            .map_err(|error| self.native.diagnostic_error(error.with_code("unknown")))?;
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
