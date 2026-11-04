use std::{collections::HashMap, sync::Arc};

use eliot_core::{Error, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, MAX_TEXT_BYTES,
    PeerClient,
    native_transport::{NativeChild, NativeLaunch, StreamState, encode_frame},
    session::{native_no_write, register_native_process, unknown_snapshot},
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

struct Sent {
    text: String,
    turn: String,
    outcome: Result<Value>,
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
    sent: HashMap<String, Sent>,
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
            sent: HashMap::new(),
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
        if let Some(previous) = self.sent.get(&params.command_id) {
            if previous.text != text {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity command id was reused with different input",
                ));
            }
            return previous.outcome.clone();
        }
        if params.command_id == self.start_command
            || self.stop.as_ref().is_some_and(|s| s.0 == params.command_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        if !self.ready {
            return Err(Error::new(
                "unknown",
                "Antigravity session readiness is unresolved",
            ));
        }
        let encoded = encode_frame("Antigravity", &UserInput::new(text))?;
        let turn = format!("agy-{}", ulid::Ulid::new());
        let associated = self
            .state
            .lock()
            .await
            .prepare(params.command_id.clone(), turn.clone())?;
        let unknown = Err(Error::new(
            "unknown",
            "Antigravity input may have applied and will not be resent",
        ));
        self.sent.insert(
            params.command_id.clone(),
            Sent {
                text: text.into(),
                turn: turn.clone(),
                outcome: unknown.clone(),
            },
        );
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
        if let Some(sent) = self.sent.get_mut(&params.command_id) {
            sent.outcome = result.clone();
        }
        result
    }

    pub async fn snapshot(&self, params: AgentSnapshotParams) -> Result<Value> {
        let state = self.state.lock().await;
        let sent = self.sent.get(&params.command_id);
        let stop = self.stop.as_ref().is_some_and(|s| s.0 == params.command_id);
        if params.command_id != self.start_command && sent.is_none() && !stop {
            return unknown_snapshot(&params, "Antigravity command identity is not retained");
        }
        let turn = params
            .turn_id
            .as_deref()
            .or_else(|| sent.map(|s| s.turn.as_str()));
        if let Some(turn) = turn
            && (sent.is_none_or(|sent| sent.turn != turn)
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
        } else if state.source_unknown || !self.ready {
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
        if params.command_id == self.start_command || self.sent.contains_key(&params.command_id) {
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
