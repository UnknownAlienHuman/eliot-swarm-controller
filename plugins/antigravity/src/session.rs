use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use eliot_core::{Error, InjectPoint, Result, canonical_json_sha256};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, MAX_FRAME_BYTES,
    MAX_TEXT_BYTES, PeerClient,
    session::{
        native_no_write, register_native_process, unknown_snapshot, with_claimed_context,
        with_context_points,
    },
};
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, timeout_at},
};

use crate::{
    events::{self, Shared},
    hooks::{self, HookOutput, HookServer},
    host::TIMEOUT,
    native::{
        AgyClient, AgyFrame, AgyProcess, FencedKind, InitializeResult, LoadSessionResult,
        PreparedFencedCall, SessionResult, SetConfigOptionResult, StreamState, prompt_params,
    },
    profile::Plan,
};

/// Best-effort deadline for the graceful `session/close` on terminate.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

struct RetainedCommand {
    seq: i64,
    command_id: String,
    payload_sha256: [u8; 32],
    turn: String,
    outcome: Result<Value>,
}

struct OwnedHookConfig {
    path: PathBuf,
    remove_agents_dir: bool,
}

struct HookConfigGuard(Option<OwnedHookConfig>);

impl Drop for HookConfigGuard {
    fn drop(&mut self) {
        if let Some(config) = &self.0 {
            remove_hook_config(config);
        }
    }
}

pub(crate) struct DeferredSteer {
    pub seq: i64,
    pub generation: i64,
    pub command_id: String,
    pub turn_id: String,
    pub receiver: oneshot::Receiver<Result<Value>>,
}

pub(crate) enum SteerStart {
    Immediate(Result<Value>),
    Deferred(DeferredSteer),
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
    native: AgyProcess,
    client: AgyClient,
    state: Shared,
    frames: Option<mpsc::Receiver<AgyFrame>>,
    stream: watch::Receiver<StreamState>,
    task: Option<JoinHandle<()>>,
    hooks: Option<HookServer>,
    hook_config: Option<OwnedHookConfig>,
    ready: bool,
    highwater_seq: i64,
    retained: Option<RetainedCommand>,
    interrupt_retained: Option<RetainedCommand>,
    steer_retained: Option<RetainedCommand>,
    steer_inflight: Option<i64>,
    stop: Option<(String, bool, Result<Value>)>,
}

impl Session {
    pub async fn spawn(
        params: &AgentStartParams,
        plan: &Plan,
        peer: &PeerClient,
        credential: &str,
        instance_dir: &Path,
        hooks: HookServer,
    ) -> Result<Self> {
        let reserved = [
            hooks::ENV_HOOK_HELPER,
            hooks::ENV_HOOK_ENDPOINT,
            hooks::ENV_HOOK_TOKEN,
        ];
        if plan.launch.environment.iter().any(|(name, _)| {
            reserved
                .iter()
                .any(|reserved| name == std::ffi::OsStr::new(reserved))
        }) {
            return Err(Error::new(
                "invalid",
                "Antigravity hook environment names are reserved for this generation",
            ));
        }
        let helper = std::env::current_exe()
            .map_err(|_| Error::new("io_error", "Could not locate the Eliot hook helper"))?;
        if !helper.is_absolute() || !helper.is_file() {
            return Err(Error::new(
                "invalid",
                "Eliot hook helper path is not an existing absolute executable",
            ));
        }
        let mut hook_config = HookConfigGuard(Some(install_hook_config(
            instance_dir,
            &plan.launch.cwd,
            &helper,
        )?));
        let mut environment = plan.launch.environment.clone();
        environment.extend(hooks.endpoint_env());
        environment.push((hooks::ENV_HOOK_HELPER.into(), helper.into_os_string()));
        let launch = eliot_plugin::native_stdio::NativeLaunch {
            program: plan.launch.program.clone(),
            args: plan.launch.args.clone(),
            cwd: plan.launch.cwd.clone(),
            environment,
        };
        let mut native = AgyProcess::spawn(launch, plan.policy.clone()).await?;
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
            hooks: Some(hooks),
            hook_config: hook_config.0.take(),
            ready: false,
            highwater_seq: 0,
            retained: None,
            interrupt_retained: None,
            steer_retained: None,
            steer_inflight: None,
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
        // The continuous ordered event processor must own the frame
        // channel before any fenced dispatch: the load reply (and its
        // preceding history) is consumed here, in wire order. For a
        // fresh session the conversation id binds after creation; for
        // resume it binds before load dispatch below.
        let frames = self
            .frames
            .take()
            .ok_or_else(|| Error::new("internal", "Antigravity reader disappeared"))?;
        let hooks = self
            .hooks
            .take()
            .ok_or_else(|| Error::new("internal", "Antigravity hook listener disappeared"))?;
        self.task = Some(events::spawn(
            frames,
            self.stream.clone(),
            self.state.clone(),
            self.client.clone(),
            hooks,
        ));
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
        let session = if let Some(resume) = plan.resume() {
            // The load reply is fenced in wire order behind the history
            // replay it precedes: the event task discards validated
            // matching-session history until that reply is consumed, and
            // only then publishes the outcome here. Never gate on the
            // bare RPC return, which can overtake queued history.
            let prepared: PreparedFencedCall = self.client.prepare_fenced(
                "session/load",
                json!({"cwd": plan.launch.cwd, "mcpServers": [], "sessionId": resume}),
                FencedKind::Load,
            )?;
            let request_id = prepared.request_id().to_owned();
            let mut load_rx = {
                let mut state = self.state.lock().await;
                state.arm_load(request_id.clone(), resume).await?
            };
            let dispatched = self.client.dispatch_fenced(prepared, deadline).await;
            let registration = match dispatched {
                Ok((dispatched_id, registration)) if dispatched_id == request_id => registration,
                Ok((_, registration)) => {
                    drop(registration);
                    self.state.lock().await.abort_load(&request_id);
                    return Err(Error::new(
                        "source_integrity",
                        "Antigravity load registration changed request identity",
                    ));
                }
                Err(error) => {
                    self.state.lock().await.abort_load(&request_id);
                    return Err(error);
                }
            };
            self.state
                .lock()
                .await
                .attach_load_guard(&request_id, registration);
            let load_wait = timeout_at(deadline, load_rx.changed())
                .await
                .map_err(|_| Error::new("unknown", "Antigravity ACP load reply deadline elapsed"))
                .and_then(|changed| {
                    changed.map_err(|_| {
                        Error::new("unknown", "Antigravity ACP load outcome is unknown")
                    })
                });
            if let Err(error) = load_wait {
                self.state.lock().await.abort_load(&request_id);
                return Err(error);
            }
            let (outcome_id, loaded) = load_rx
                .borrow()
                .clone()
                .ok_or_else(|| Error::new("unknown", "Antigravity ACP load outcome is unknown"))?;
            if outcome_id != request_id {
                self.state.lock().await.abort_load(&request_id);
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity load outcome changed request identity",
                ));
            }
            let loaded = loaded?;
            // `session/load` returns no session id by design: the exact
            // retained id is the one from this correlated request.
            let loaded: LoadSessionResult = serde_json::from_value(loaded).map_err(|_| {
                Error::new(
                    "unknown",
                    "Antigravity ACP load response did not match its envelope",
                )
            })?;
            SessionResult {
                session_id: resume.to_owned(),
                modes: loaded.modes,
                config_options: loaded.config_options,
            }
        } else {
            let created = self
                .client
                .call(
                    "session/new",
                    json!({"cwd": plan.launch.cwd, "mcpServers": []}),
                    deadline,
                )
                .await?;
            serde_json::from_value(created).map_err(|_| {
                Error::new(
                    "unknown",
                    "Antigravity ACP session response did not match its envelope",
                )
            })?
        };
        let mut effective = plan.check_session(&init, &session)?;
        self.conversation = session.session_id.clone();
        {
            let mut state = self.state.lock().await;
            state.bind_conversation(&self.conversation).await?;
        }
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
            let verified = echoed
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
        // A requested session mode must be one the server actually
        // advertised for this session; anything else (including CLI
        // `auto`, which has no ACP mode id) fails startup clearly
        // instead of being silently ignored.
        if let Some(mode) = plan.mode() {
            let advertised = session
                .modes
                .as_ref()
                .is_some_and(|modes| modes.available_modes.iter().any(|m| m.id == mode));
            if !advertised {
                return Err(Error::new(
                    "unsupported",
                    "Antigravity requested session mode is not advertised by the ACP session",
                ));
            }
            self.client
                .call(
                    "session/set_mode",
                    json!({"sessionId": self.conversation, "modeId": mode}),
                    deadline,
                )
                .await?;
            // Report the actually applied mode, never the pre-set value.
            if let Some(object) = effective.as_object_mut() {
                object.insert("mode".to_owned(), json!(mode));
            }
        }
        let effective = with_context_points(
            effective,
            &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
        );
        if *self.stream.borrow() != StreamState::Open {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP stream is already incomplete",
            ));
        }
        {
            let mut state = self.state.lock().await;
            state.ready(&self.scope, effective.clone()).await?;
        }
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
            "steer" => Err(Error::new(
                "internal",
                "Antigravity steer must use the deferred host response path",
            )),
            "interrupt" => self.interrupt(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Antigravity supports send, steer and interrupt; inject/configure are unsupported",
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
            || self
                .interrupt_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
            || self
                .steer_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
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
        // The terminal reply is fenced into the ordered frame channel, so
        // the event task settles this exact turn behind every preceding
        // update. This send only waits for the first association, as
        // before; a slow first update keeps the retained `unknown`
        // outcome while the fenced reply still settles the turn in order.
        let prompt = prompt_params(&self.conversation, &combined);
        let dispatched = self
            .client
            .call_fenced(
                "session/prompt",
                prompt,
                Instant::now() + TIMEOUT,
                FencedKind::Prompt,
            )
            .await;
        let registration = match dispatched {
            Ok((_, registration)) => registration,
            Err(error) => {
                if native_no_write(&error.code) {
                    self.state
                        .lock()
                        .await
                        .withdraw_unwritten(&params.command_id)?;
                    let result = Ok(
                        json!({"state": "rejected", "reason": "Antigravity input was not dispatched"}),
                    );
                    if let Some(retained) = self.retained.as_mut().filter(|r| r.seq == seq) {
                        retained.outcome = result.clone();
                    }
                    return result;
                }
                let mut state = self.state.lock().await;
                state.fail_prompt(error).await;
                let result = unknown;
                if let Some(retained) = self.retained.as_mut().filter(|r| r.seq == seq) {
                    retained.outcome = result.clone();
                }
                return result;
            }
        };
        // Exactly one owner for the registration from here on: the event
        // state, which retires it on settlement, expiry, loss, or drop —
        // unless this exact turn already settled while the write was in
        // flight, in which case `arm_fenced` retires it immediately.
        self.state.lock().await.arm_fenced(registration, &turn);
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

    pub async fn begin_steer(&mut self, params: AgentCommandParams) -> Result<SteerStart> {
        if params.request_id.is_some() || params.args.as_object().is_none_or(|args| args.len() != 1)
        {
            return Err(Error::new(
                "invalid",
                "Antigravity steer accepts only args.text and an exact turn",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|turn| !turn.trim().is_empty())
            .ok_or_else(|| {
                Error::new("stale_turn", "Antigravity steer requires a captured turn")
            })?;
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Antigravity steer requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity steer exceeds the text bound",
            ));
        }
        if text.trim().is_empty() {
            return Err(Error::new("invalid", "Antigravity steer text is empty"));
        }
        if self.state.lock().await.contains_secret(text) {
            return Err(Error::new(
                "secret_in_payload",
                "Antigravity steer contains a known credential",
            ));
        }
        let output = HookOutput::with_system_message(text.to_owned());
        if serde_json::to_vec(&output).map_or(true, |bytes| {
            bytes.len().saturating_add(1) > MAX_FRAME_BYTES
        }) {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity steer does not fit one PreInvocation response",
            ));
        }
        if params.command_id == self.start_command
            || self
                .stop
                .as_ref()
                .is_some_and(|stop| stop.0 == params.command_id)
            || self
                .retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
            || self
                .interrupt_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        if !self.ready || self.native.transport.process_exit_observed() {
            return Err(Error::new(
                "unknown",
                "Antigravity steer session readiness is unresolved",
            ));
        }
        let seq = match params.dispatch_seq {
            Some(seq) if seq > 0 => seq,
            _ => {
                return Err(Error::new(
                    "invalid",
                    "Antigravity steer requires a positive dispatch sequence",
                ));
            }
        };
        let payload_sha256 = canonical_json_sha256(&params.args).map_err(|_| {
            Error::new(
                "invalid",
                "Antigravity steer arguments cannot be canonicalized",
            )
        })?;
        match check_history(
            &self.steer_retained,
            self.highwater_seq,
            seq,
            &params.command_id,
            payload_sha256,
        ) {
            HistoryDecision::RetainedOutcome(outcome) => {
                if self
                    .steer_retained
                    .as_ref()
                    .is_some_and(|retained| retained.turn == turn_id)
                {
                    return Ok(SteerStart::Immediate(outcome.clone()));
                }
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity steer replay changed its captured turn",
                ));
            }
            HistoryDecision::Conflict => {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity steer command binding conflicts with retained history",
                ));
            }
            HistoryDecision::EvictedHistory => {
                return Err(Error::new(
                    "unknown",
                    "Antigravity steer private history is unavailable",
                ));
            }
            HistoryDecision::Advance => {}
        }
        if self.steer_inflight.is_some() {
            return Err(Error::new(
                "busy",
                "Antigravity already has a steer awaiting its native receipt",
            ));
        }

        let receiver = self.state.lock().await.arm_steer(
            params.command_id.clone(),
            turn_id.to_owned(),
            text.to_owned(),
        )?;
        self.highwater_seq = seq;
        self.steer_retained = Some(RetainedCommand {
            seq,
            command_id: params.command_id.clone(),
            payload_sha256,
            turn: turn_id.to_owned(),
            outcome: Ok(unknown_steer(
                &params.command_id,
                turn_id,
                "PreInvocation has not confirmed application",
            )),
        });
        self.steer_inflight = Some(seq);
        Ok(SteerStart::Deferred(DeferredSteer {
            seq,
            generation: self.generation,
            command_id: params.command_id,
            turn_id: turn_id.to_owned(),
            receiver,
        }))
    }

    pub fn settle_steer(
        &mut self,
        seq: i64,
        generation: i64,
        command_id: &str,
        turn_id: &str,
        outcome: Result<Value>,
    ) -> Result<Value> {
        if generation != self.generation {
            return Err(Error::new(
                "source_integrity",
                "Antigravity deferred steer receipt crossed generations",
            ));
        }
        let Some(retained) = self.steer_retained.as_mut().filter(|retained| {
            retained.seq == seq && retained.command_id == command_id && retained.turn == turn_id
        }) else {
            return Err(Error::new(
                "source_integrity",
                "Antigravity deferred steer receipt lost its retained identity",
            ));
        };
        retained.outcome = outcome.clone();
        if self.steer_inflight == Some(seq) {
            self.steer_inflight = None;
        }
        outcome
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
            || self
                .retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
            || self
                .steer_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
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
                    Some("cancelled") => Ok(json!({
                        "state": "applied",
                        "command_id": params.command_id.clone(),
                        "turn_id": turn_id,
                    })),
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
        let steer = self
            .steer_retained
            .as_ref()
            .filter(|r| r.command_id == params.command_id);
        let stop = self.stop.as_ref().is_some_and(|s| s.0 == params.command_id);
        if params.command_id != self.start_command
            && retained.is_none()
            && interrupt.is_none()
            && steer.is_none()
            && !stop
        {
            return unknown_snapshot(&params, "Antigravity command identity is not retained");
        }
        let turn = params
            .turn_id
            .as_deref()
            .or_else(|| retained.map(|r| r.turn.as_str()))
            .or_else(|| interrupt.map(|r| r.turn.as_str()))
            .or_else(|| steer.map(|r| r.turn.as_str()));
        if let Some(turn) = turn {
            let proven = retained.is_some_and(|r| r.turn == turn)
                || interrupt.is_some_and(|r| r.turn == turn)
                || steer.is_some_and(|r| r.turn == turn);
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
                .interrupt_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
            || self
                .retained
                .as_ref()
                .is_some_and(|r| r.command_id == params.command_id)
            || self
                .steer_retained
                .as_ref()
                .is_some_and(|retained| retained.command_id == params.command_id)
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
        {
            let mut state = self.state.lock().await;
            state.stopping = true;
            state.cancel_pending_steer();
        }
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
        if let Some(task) = self.task.as_mut() {
            timeout_at(deadline, task)
                .await
                .map_err(|_| Error::new("unknown", "Antigravity event drain deadline elapsed"))?
                .map_err(|_| Error::new("unknown", "Antigravity event reader failed"))?;
            self.task = None;
        }
        if let Some(config) = self.hook_config.take() {
            remove_hook_config(&config);
        }
        if self.ready {
            let mut state = self.state.lock().await;
            state.ended().await?;
        }
        self.ready = false;
        Ok(())
    }
}

pub(crate) fn unknown_steer(command_id: &str, turn_id: &str, reason: &str) -> Value {
    json!({
        "state": "unknown",
        "command_id": command_id,
        "turn_id": turn_id,
        "mechanism": events::STEER_MECHANISM,
        "native_hook": "PreInvocation",
        "reason": reason,
    })
}

fn install_hook_config(
    instance_dir: &Path,
    workspace: &Path,
    helper: &Path,
) -> Result<OwnedHookConfig> {
    let command = hook_command(helper)?;
    let private_root = fs::canonicalize(instance_dir).map_err(|_| {
        Error::new(
            "invalid",
            "Antigravity private instance directory is unavailable",
        )
    })?;
    let workspace = fs::canonicalize(workspace)
        .map_err(|_| Error::new("profile_error", "Antigravity workspace is unavailable"))?;
    if private_root.starts_with(&workspace) || workspace.starts_with(&private_root) {
        return Err(Error::new(
            "invalid",
            "Antigravity private instance directory overlaps its workspace",
        ));
    }

    let agents_dir = private_root.join(".agents");
    let remove_agents_dir = match fs::create_dir(&agents_dir) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(_) => {
            return Err(Error::new(
                "io_error",
                "Could not create the private Antigravity hooks directory",
            ));
        }
    };
    let agents_dir = match fs::canonicalize(&agents_dir) {
        Ok(path) if path.is_dir() && path.starts_with(&private_root) => path,
        _ => {
            if remove_agents_dir {
                let _ = fs::remove_dir(&agents_dir);
            }
            return Err(Error::new(
                "invalid",
                "Antigravity hooks directory is not contained by its private instance",
            ));
        }
    };

    let config = hooks::hooks_json(hooks::DEFAULT_HOOK_NAME, &command);
    let mut bytes = match serde_json::to_vec(&config) {
        Ok(bytes) => bytes,
        Err(_) => {
            if remove_agents_dir {
                let _ = fs::remove_dir(&agents_dir);
            }
            return Err(Error::new(
                "internal",
                "Could not encode Antigravity hook config",
            ));
        }
    };
    bytes.push(b'\n');
    let path = agents_dir.join("hooks.json");
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(_) => {
            if remove_agents_dir {
                let _ = fs::remove_dir(&agents_dir);
            }
            return Err(Error::new(
                "io_error",
                "Could not create the owned Antigravity hook config",
            ));
        }
    };
    if file.write_all(&bytes).and_then(|()| file.flush()).is_err() {
        drop(file);
        let _ = fs::remove_file(&path);
        if remove_agents_dir {
            let _ = fs::remove_dir(&agents_dir);
        }
        return Err(Error::new(
            "io_error",
            "Could not write the owned Antigravity hook config",
        ));
    }
    Ok(OwnedHookConfig {
        path,
        remove_agents_dir,
    })
}

fn hook_command(helper: &Path) -> Result<String> {
    let executable = helper
        .to_str()
        .ok_or_else(|| Error::new("invalid", "Eliot hook helper path is not UTF-8"))?;
    // The pinned AGY Windows log shows literal `\"...\"` tokens are not
    // recognized by its command executor. Emit an unquoted bare path only
    // when it has no whitespace or shell metacharacters; other paths stay
    // explicitly unsupported until the vendor's quoting rules are proven.
    let unsafe_token = executable.chars().any(|character| {
        character.is_whitespace()
            || if cfg!(windows) {
                matches!(
                    character,
                    '&' | '|' | '<' | '>' | '^' | '%' | '!' | '(' | ')' | '"'
                )
            } else {
                matches!(
                    character,
                    '&' | '|'
                        | '<'
                        | '>'
                        | ';'
                        | '$'
                        | '`'
                        | '\''
                        | '"'
                        | '*'
                        | '?'
                        | '['
                        | ']'
                        | '#'
                        | '('
                        | ')'
                )
            }
    });
    if unsafe_token {
        return Err(Error::new(
            "unsupported",
            "Antigravity hook executable path contains unqualified command characters",
        ));
    }
    Ok(format!("{executable} plugin antigravity-hook"))
}

fn remove_hook_config(config: &OwnedHookConfig) {
    let _ = fs::remove_file(&config.path);
    if config.remove_agents_dir
        && let Some(parent) = config.path.parent()
    {
        let _ = fs::remove_dir(parent);
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
