use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use eliot_core::{Error, InjectPoint, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, MAX_FRAME_BYTES,
    MAX_TEXT_BYTES, PeerClient,
    session::{
        native_no_write, register_native_process, unknown_snapshot, with_claimed_context,
        with_context_points,
    },
    skill,
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
        AgyClient, AgyFrame, AgyProcess, ConfigOption, FencedKind, InitializeResult,
        LoadSessionResult, PreparedFencedCall, SessionResult, SetConfigOptionResult, StreamState,
        prompt_params,
    },
    profile::Plan,
};

/// Best-effort deadline for the graceful `session/close` on terminate.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Bound for one send's native write plus first-association wait. The daemon
/// settles `plugin_rpc_timeout` at 30 s, so the plugin must settle first
/// with its own cause instead of racing to a joint timeout that reports
/// an empty diagnostic snapshot.
const SEND_TIMEOUT: Duration = Duration::from_secs(25);

fn admit_cancel(state: &events::State, turn_id: &str) -> Result<()> {
    if state.active_turn() != Some(turn_id) || state.source_unknown || state.batch_unknown {
        return Err(Error::new(
            "stale_turn",
            "cancel does not match a healthy exact native turn",
        ));
    }
    Ok(())
}

async fn wait_for_association(
    association: oneshot::Receiver<Result<()>>,
    deadline: Instant,
    turn: &str,
) -> Result<Value> {
    match timeout_at(deadline, association).await {
        Ok(Ok(Ok(()))) => Ok(json!({"state": "applied", "turn_id": turn})),
        Ok(Ok(Err(error))) if error.code == "native_rejected" => Ok(json!({
            "state": "rejected",
            "code": error.code,
            "message": "Antigravity rejected the prompt before admission",
            "where": error.failure_where,
            "origin": "harness",
        })),
        _ => Err(Error::new(
            "unknown",
            "Antigravity input association is unconfirmed; input will not be resent",
        )),
    }
}

fn send_preparation_error(error: Error) -> Error {
    error
        .with_code("native_not_dispatched")
        .with_where("plugin.antigravity.send.prepare")
}

pub(crate) struct OwnedHookConfig {
    path: PathBuf,
    root: PathBuf,
    remove_agents_dir: bool,
    remove_root: bool,
    /// Created skill files, removed first on cleanup.
    skill_files: Vec<PathBuf>,
    /// Created skill directories, innermost first.
    skill_dirs: Vec<PathBuf>,
}

impl OwnedHookConfig {
    pub fn root(&self) -> &Path {
        &self.root
    }
}

pub(crate) struct HookConfigGuard(pub(crate) Option<OwnedHookConfig>);

impl HookConfigGuard {
    pub(crate) fn root(&self) -> &Path {
        self.0
            .as_ref()
            .expect("armed Antigravity hook config guard")
            .root()
    }
}

impl Drop for HookConfigGuard {
    fn drop(&mut self) {
        if let Some(config) = &self.0 {
            remove_hook_config(config);
        }
    }
}

pub(crate) struct DeferredSteer {
    pub generation: i64,
    pub command_id: String,
    pub turn_id: String,
    pub receiver: oneshot::Receiver<Result<Value>>,
}

pub(crate) enum SteerStart {
    Deferred(DeferredSteer),
}

/// Advertised values for one config option id from a session open
/// (`session/new` or `session/load`) result. Surprises decode as no
/// values, so a required write rejects instead of guessing.
fn advertised_option_values(options: Option<&Vec<ConfigOption>>, id: &str) -> Vec<String> {
    options
        .and_then(|options| options.iter().find(|option| option.id == id))
        .map(|option| {
            option
                .option_values()
                .into_iter()
                .map(|(value, _)| value)
                .collect()
        })
        .unwrap_or_default()
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
    mcp_config: Option<crate::mcp::OwnedConfig>,
    ready: bool,
    steer_inflight: Option<i64>,
    stop: Option<(String, Result<Value>)>,
    /// Advertised `model` option values from the latest session open
    /// (`session/new` or `session/load` result), refreshed on resume.
    /// Required writes must name one of these; anything else rejects
    /// before any native write.
    advertised_models: Vec<String>,
    /// Advertised `thought_level` option values, refreshed like the models.
    advertised_levels: Vec<String>,
    /// Tracked native selectors: the base model id and thought level this
    /// session runs. Initialized from the effective start target (frozen
    /// profile or inherited overlay) and advanced only by verified
    /// configures, so an effort-only configure keeps the current base.
    current_base: String,
    current_level: Option<String>,
}

impl Session {
    pub async fn spawn(
        params: &AgentStartParams,
        plan: &Plan,
        peer: &PeerClient,
        credential: &str,
        mut hook_config: HookConfigGuard,
        hooks: HookServer,
        mcp_config: Option<crate::mcp::OwnedConfig>,
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
        if hook_config.root() != plan.hook_discovery {
            return Err(Error::new(
                "invalid",
                "Antigravity hook discovery root changed after staging",
            ));
        }
        let helper = hook_helper_path()?;
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
        // Tracked selectors start at the effective start target (frozen
        // profile or inherited overlay); the advertised sets fill in once
        // the session opens and refresh on every resume.
        let (effective_base, effective_level) = plan.effective_target();
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
            mcp_config,
            ready: false,
            steer_inflight: None,
            stop: None,
            advertised_models: Vec::new(),
            advertised_levels: Vec::new(),
            current_base: effective_base,
            current_level: effective_level,
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
                json!({"cwd": plan.launch.cwd, "mcpServers": [plan.mcp_server_entry()?], "sessionId": resume}),
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
            // The Agent-scoped swarm MCP server rides the documented ACP
            // session-setup surface, per run and per Agent: no owner-global
            // config is touched, and the Agent authenticates with its own
            // inherited identity, never the owner token.
            let created = self
                .client
                .call(
                    "session/new",
                    json!({"cwd": plan.launch.cwd, "mcpServers": [plan.mcp_server_entry()?]}),
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
        self.current_base = effective
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| Error::new("unknown", "Antigravity effective model was not read back"))?
            .to_owned();
        self.current_level = effective
            .get("thought_level")
            .and_then(Value::as_str)
            .map(str::to_owned);
        // Refresh the advertised sets from this open (fresh or resumed):
        // every required write must name one of these values.
        self.advertised_models = advertised_option_values(session.config_options.as_ref(), "model");
        self.advertised_levels =
            advertised_option_values(session.config_options.as_ref(), "thought_level");
        self.conversation = session.session_id.clone();
        {
            let mut state = self.state.lock().await;
            state.bind_conversation(&self.conversation).await?;
        }
        // Verify each required config write by its echoed current value.
        for (config_id, value) in plan.config_writes() {
            self.write_config_option(&config_id, &value, deadline)
                .await?;
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
            "configure" => self.configure(params).await,
            "steer" => Err(Error::new(
                "internal",
                "Antigravity steer must use the deferred host response path",
            )),
            "interrupt" => self.interrupt(params).await,
            "approve" | "deny" => self.reply_request(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Antigravity supports send, configure, steer, interrupt, approve and deny; inject and answer are unsupported",
            )),
        }
    }

    /// Answer one open native permission ask with the explicit
    /// owner/manager decision. Replays the retained outcome for the exact
    /// command id; a reused command id bound to a different reply is a
    /// source-integrity error, never a second native write.
    async fn reply_request(&mut self, params: AgentCommandParams) -> Result<Value> {
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "stale_turn",
                    "Antigravity request reply needs its captured turn id",
                )
            })?;
        let request_id = params
            .request_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    "Antigravity request reply needs its captured request id",
                )
            })?;
        // The event task owns no transport client: the native decision
        // travels on this call, serialized behind queued frames. A dead
        // native process latches unknown without attempting a write.
        if self.native.transport.process_exit_observed() {
            let mut state = self.state.lock().await;
            state.retain_unknown_reply(request_id, &params.command_id);
            return Ok(eliot_plugin::session::unknown_control(
                "Antigravity request reply destination is unavailable; it will not be repeated",
            ));
        }
        let native = self.client.clone();
        let result = match self
            .state
            .lock()
            .await
            .reply_request(
                &params.command_id,
                request_id,
                turn_id,
                &params.verb,
                &params.args,
                &native,
            )
            .await
        {
            Ok(result) => result,
            Err(_) => eliot_plugin::session::unknown_control(
                "Antigravity request reply outcome is unknown",
            ),
        };
        Ok(result)
    }

    /// One verified `session/set_config_option` write: the echoed option
    /// set must carry the written value back. A missing or mismatched echo
    /// is unknown: the write may have applied.
    async fn write_config_option(
        &self,
        config_id: &str,
        value: &str,
        deadline: Instant,
    ) -> Result<()> {
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
        Ok(())
    }

    /// Native `configure`: switch the live session's `model` and
    /// `thought_level` selectors through `session/set_config_option` and
    /// prove each write with its echoed current value. A requested value
    /// the session did not advertise rejects before any write, never
    /// guessing; anything ambiguous after a write is unknown, and nothing
    /// is stored by this layer (the Router stores the overlay only after
    /// this proof). Always writes the resolved selectors, so a retried
    /// configure after an uncertain outcome is idempotent.
    pub async fn configure(&mut self, params: AgentCommandParams) -> Result<Value> {
        if params.turn_id.is_some() || params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "Antigravity configure targets the session, without a turn or request target",
            ));
        }
        if self.native.transport.process_exit_observed() {
            return Err(Error::new(
                "unknown",
                "Antigravity process exited; the configuration was not dispatched",
            ));
        }
        if !self.ready {
            return Err(Error::new(
                "unknown",
                "Antigravity session readiness is unresolved",
            ));
        }
        // Pre-write validation failures reject with proof of no effect
        // (the resolver is pure and nothing has been written yet), so the
        // settings lane stays free. Only an ambiguous outcome at or after
        // the first write is unknown.
        let target = match crate::profile::resolve_configure_target(
            &params.args,
            &self.current_base,
            self.current_level.as_deref(),
        ) {
            Ok(target) => target,
            Err(error) if matches!(error.code.as_str(), "invalid" | "unsupported") => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": error.message,
                }));
            }
            Err(error) => return Err(error),
        };
        if !self
            .advertised_models
            .iter()
            .any(|model| model == &target.base)
        {
            return Ok(json!({
                "state": "rejected",
                "reason": format!("Antigravity model {} is not advertised by the ACP session", target.base),
            }));
        }
        if target.level.as_deref().is_some_and(|level| {
            !self
                .advertised_levels
                .iter()
                .any(|advertised| advertised == level)
        }) {
            return Ok(json!({
                "state": "rejected",
                "reason": "Antigravity thought level is not advertised by the ACP session",
            }));
        }
        let deadline = Instant::now() + TIMEOUT;
        self.write_config_option("model", &target.base, deadline)
            .await?;
        if let Some(level) = target.level.as_deref() {
            self.write_config_option("thought_level", level, deadline)
                .await?;
        }
        self.current_base = target.base.clone();
        self.current_level = target.level.clone();
        Ok(json!({
            "state": "applied",
            "model": target.effective_model,
            "effort": target.level.clone(),
            "model_verified": true,
            "effort_verified": target.level.is_some(),
        }))
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
        if self.native.transport.process_exit_observed() {
            return Err(Error::new(
                "native_not_dispatched",
                "Antigravity process exited; new input was not dispatched",
            )
            .with_where("plugin.antigravity.send.prepare"));
        }
        if !self.ready {
            return Err(Error::new(
                "native_not_dispatched",
                "Antigravity session readiness is unresolved",
            )
            .with_where("plugin.antigravity.send.prepare"));
        }
        let combined = with_claimed_context(text, &params.context, "Antigravity")
            .map_err(send_preparation_error)?;
        let turn = format!("agy-{}", ulid::Ulid::new());
        let deadline = Instant::now() + SEND_TIMEOUT;
        let prepared = self
            .client
            .prepare_fenced(
                "session/prompt",
                prompt_params(&self.conversation, &combined),
                FencedKind::Prompt,
            )
            .map_err(send_preparation_error)?;
        let (write, registration) = prepared.split();
        let association = {
            let mut state = timeout_at(deadline, self.state.lock()).await.map_err(|_| {
                Error::new(
                    "native_not_dispatched",
                    "Antigravity input preparation timed out",
                )
                .with_where("plugin.antigravity.send.prepare")
            })?;
            let association = state
                .prepare(params.command_id.clone(), turn.clone())
                .map_err(send_preparation_error)?;
            state.attach_context(&params.command_id, params.context.clone())?;
            // Transfer the fence before I/O. A slow event commit must not
            // hold the send RPC open or drop a still-live native call.
            state.arm_fenced(registration, &turn);
            association
        };
        let unknown = Err(Error::new(
            "unknown",
            "Antigravity input may have applied and will not be resent",
        ));
        // Native write acceptance is transport delivery only. The event
        // owner acknowledges after committing correlated native evidence.
        let dispatched = self.client.dispatch_fenced_write(write, deadline).await;
        match dispatched {
            Ok(_) => wait_for_association(association, deadline, &turn).await,
            Err(error) if native_no_write(&error.code) => {
                match timeout_at(deadline, self.state.lock()).await {
                    Ok(mut state) => match state.withdraw_unwritten(&params.command_id) {
                        Ok(()) => Err(error.with_where("plugin.antigravity.send.dispatch")),
                        Err(_) => unknown,
                    },
                    _ => unknown,
                }
            }
            // Retain the pending input and its fence: dispatch may have
            // happened, and neither this error nor the wait permits replay.
            Err(_) => unknown,
        }
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
        self.steer_inflight = Some(0);
        Ok(SteerStart::Deferred(DeferredSteer {
            generation: self.generation,
            command_id: params.command_id,
            turn_id: turn_id.to_owned(),
            receiver,
        }))
    }

    pub fn settle_steer(
        &mut self,
        generation: i64,
        _command_id: &str,
        _turn_id: &str,
        outcome: Result<Value>,
    ) -> Result<Value> {
        if generation != self.generation {
            return Err(Error::new(
                "source_integrity",
                "Antigravity deferred steer receipt crossed generations",
            ));
        }
        if self.steer_inflight.is_none() {
            return Err(Error::new(
                "source_integrity",
                "Antigravity steer receipt is not live",
            ));
        }
        self.steer_inflight = None;
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
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        // Re-check the exact turn under the same lock discipline as send:
        // a turn that ended since the first check was never interrupted.
        let dispatched = self
            .cancel_turn(&params.command_id, turn_id, Instant::now() + TIMEOUT, None)
            .await;
        match dispatched {
            Ok(()) => {
                // Per the ACP cancellation contract, answer every pending
                // permission ask of this exact turn with `cancelled`
                // before waiting: the native cannot reach its cancelled
                // terminal while an ask is still open. Best-effort per
                // ask; the terminal below still decides the interrupt.
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
            // The locked re-check refused before native I/O. Preserve that
            // refusal and leave private history available for the next command.
            Err(error) if error.code == "stale_turn" => Err(error),
            Err(error) if native_no_write(&error.code) => Ok(
                json!({"state": "rejected", "reason": "Antigravity interrupt was not dispatched"}),
            ),
            Err(error) => Err(self.client.diagnostic_error(error.with_code("unknown"))),
        }
    }

    async fn cancel_turn(
        &mut self,
        command_id: &str,
        turn_id: &str,
        until: Instant,
        _seq: Option<i64>,
    ) -> Result<()> {
        let mut state = self.state.lock().await;
        admit_cancel(&state, turn_id)?;
        // Hold the event lock across write and latch so late asks observe intent.
        let dispatched = self
            .client
            .notify(
                "session/cancel",
                Some(json!({"sessionId": self.conversation})),
                until,
            )
            .await;
        if !matches!(&dispatched, Err(error) if native_no_write(&error.code)) {
            state.latch_cancellation(command_id, Some(turn_id));
            state
                .answer_pending_cancelled(&self.client, command_id, Some(turn_id))
                .await;
        }
        dispatched
    }

    pub async fn snapshot(&self, params: AgentSnapshotParams) -> Result<Value> {
        let state = self.state.lock().await;
        let process_exited = self.native.transport.process_exit_observed();
        let stop = self.stop.as_ref().is_some_and(|s| s.0 == params.command_id);
        let live_turn = state
            .live_turn_for_command(&params.command_id)
            .or_else(|| state.latest_terminal_turn_for_command(&params.command_id));
        if params.command_id != self.start_command && !stop && live_turn.is_none() {
            return unknown_snapshot(&params, "Antigravity command identity is not live");
        }
        let turn = params.turn_id.as_deref().or(live_turn);
        if let (Some(requested), Some(bound)) = (params.turn_id.as_deref(), live_turn)
            && requested != bound
        {
            return unknown_snapshot(&params, "Antigravity snapshot turn binding is not live");
        }
        let last = state
            .last_turn
            .as_ref()
            .filter(|last| turn.is_none_or(|turn| last.turn_id == turn));
        let result_known = last.is_some_and(|last| last.terminal);
        let mut status = if state.stopped {
            "stopped"
        } else if state.source_unknown || process_exited || !self.ready {
            "unknown"
        } else if state.active_turn().is_some() {
            "busy"
        } else {
            "idle"
        };
        let mut active_turn = if state.stopped {
            None
        } else {
            state.active_turn()
        };
        let mut last_turn = serde_json::to_value(last).map_err(|_| {
            Error::new("internal", "Antigravity turn snapshot could not be encoded")
        })?;
        // The universal status vocabulary names ACP `cancelled` `interrupted`.
        if last_turn["status"] == "cancelled" {
            last_turn["status"] = json!("interrupted");
            last_turn["native_status"] = json!("cancelled");
        }
        // Exact-turn facts from the ACP stream itself: ACP has no turn-status
        // read, so the fenced `session/prompt` call and its reply are the
        // native evidence.
        let evidence = params.turn_id.as_deref().and_then(|turn_id| {
            if let Some(last) = last.filter(|last| last.turn_id == turn_id) {
                return (last.terminal && last.command_id == params.command_id)
                    .then_some(TurnEvidence::Terminal);
            }
            if state.prompt_outstanding(&params.command_id, turn_id)
                && self.ready
                && !state.stopped
                && !process_exited
            {
                return Some(TurnEvidence::Running);
            }
            // All stdout was consumed after the observed exit and no
            // `session/prompt` reply settled the prepared turn.
            (process_exited
                && self.task.as_ref().is_none_or(JoinHandle::is_finished)
                && !state.stopped
                && state.pending_is(&params.command_id, turn_id))
            .then_some(TurnEvidence::Exited)
        });
        match evidence {
            Some(TurnEvidence::Running) => {
                last_turn = json!({"command_id": params.command_id, "turn_id": params.turn_id,
                    "status": "running", "terminal": false});
            }
            Some(TurnEvidence::Exited) => {
                status = "lost";
                active_turn = None;
                last_turn = json!({"command_id": params.command_id, "turn_id": params.turn_id,
                    "status": "lost", "terminal": false});
            }
            Some(TurnEvidence::Terminal) | None => {}
        }
        Ok(
            json!({"agent_id": self.agent, "generation": self.generation, "command_id": params.command_id,
            "state": status, "native_scope": self.scope, "native_ref": state.conversation,
            "active_turn": active_turn, "last_turn": last_turn,
            "source_unknown": state.source_unknown, "batch_unknown": state.batch_unknown,
            "process_exited": process_exited,
            "result_known": result_known, "usage": Value::Null,
            "turn_evidence": evidence.map(TurnEvidence::label),
            "diagnostics": self.client.diagnostic_error(Error::new("unknown", "")).diagnostics}),
        )
    }

    pub async fn stop(&mut self, params: AgentStopParams) -> Result<Value> {
        if let Some((previous, result)) = &self.stop {
            if previous != &params.command_id {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity stop inputs changed after dispatch",
                ));
            }
            return result.clone();
        }
        if params.command_id == self.start_command {
            return Err(Error::new(
                "source_integrity",
                "Antigravity command id belongs to another operation",
            ));
        }
        self.stop = Some((
            params.command_id.clone(),
            Err(Error::new("unknown", "Antigravity stop is unresolved")),
        ));
        // The router owns interrupt/wait and daemon force custody. This
        // adapter only closes the native session gracefully and verifies
        // source drain; force custody remains in the daemon.
        let closure = {
            let mut state = self.state.lock().await;
            state.stopping = true;
            state.close_pending_requests(None).await
        };
        if let Err(error) = closure {
            let error = self.client.diagnostic_error(error);
            self.stop = Some((params.command_id.clone(), Err(error.clone())));
            return Err(error);
        }
        let result = self
            .close_gracefully()
            .await
            .map(|()| json!({"state": "applied"}));
        self.stop = Some((params.command_id, result.clone()));
        result
    }

    pub async fn wait_for_native_exit(&mut self) {
        eliot_plugin::native_lifecycle::wait_for_native_exit(Some(&mut self.native.transport))
            .await;
    }

    /// Native EOF already settles fenced prompt/load and hook waiters. Drain
    /// that task before retiring; `ended` is reserved for actual native End.
    pub async fn retire_native_exit(&mut self) -> Result<()> {
        let drained =
            eliot_plugin::native_lifecycle::drain_native_exit(&mut self.native.transport, async {
                if let Some(task) = self.task.as_mut() {
                    task.await.map_err(|_| {
                        Error::new("unknown", "Antigravity native event task failed")
                    })?;
                }
                Ok(())
            })
            .await;
        if let Some(task) = self.task.take()
            && !task.is_finished()
        {
            task.abort();
            let _ = task.await;
        }
        self.state.lock().await.cancel_pending_steer();
        self.mcp_config.take();
        if let Some(config) = self.hook_config.take() {
            remove_hook_config(&config);
        }
        self.ready = false;
        drained
    }

    pub async fn terminate(&mut self, force: bool) -> Result<()> {
        self.terminate_inner(force, false).await
    }

    async fn close_gracefully(&mut self) -> Result<()> {
        self.terminate_inner(false, true).await
    }

    async fn terminate_inner(&mut self, force: bool, graceful: bool) -> Result<()> {
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
        let process_exit = if graceful {
            self.native.close_gracefully(deadline).await
        } else {
            self.native.terminate(force, deadline).await
        };
        process_exit.map_err(|error| self.client.diagnostic_error(error.with_code("unknown")))?;
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
        self.mcp_config.take();
        if self.ready {
            let mut state = self.state.lock().await;
            state.ended().await?;
        }
        self.ready = false;
        Ok(())
    }
}

/// What the ACP stream and process waiter proved about the exact turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TurnEvidence {
    /// The fenced `session/prompt` awaits its reply on a healthy stream.
    Running,
    /// The exact turn's `session/prompt` reply settled it.
    Terminal,
    /// The process exited and the drained stream held no reply.
    Exited,
}

impl TurnEvidence {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "acp session/prompt pending",
            Self::Terminal => "acp session/prompt stopReason",
            Self::Exited => "process exit with drained acp stream and no session/prompt reply",
        }
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

pub(crate) fn hook_helper_path() -> Result<PathBuf> {
    let helper = std::env::current_exe()
        .map_err(|_| Error::new("io_error", "Could not locate the Eliot hook helper"))?;
    if !helper.is_absolute() || !helper.is_file() {
        return Err(Error::new(
            "invalid",
            "Eliot hook helper path is not an existing absolute executable",
        ));
    }
    Ok(helper)
}

/// Stage the dedicated hook-discovery root for one generation: a sibling of
/// the private instance directory holding nothing but `.agents/hooks.json`
/// (no tokens, logs, state, or sockets). The native CLI discovers hooks
/// through `--add-dir`, so this root — never the instance directory — is
/// the only `--add-dir` value. Both fresh starts and resumed sessions stage
/// a new root on this same path; hook delivery itself is unchanged.
///
/// The stored root is a plain absolute path: on Windows, canonicalization
/// adds the verbatim `\\?\` prefix, which the native CLI turns into an
/// invalid `file://` URL escape and rejects, silently dropping the whole
/// discovery root (and its hooks). Stripping the prefix names the identical
/// directory, so staging, containment and cleanup are unaffected.
pub(crate) fn stage_hook_config(
    instance_dir: &Path,
    workspace: &Path,
    helper: &Path,
    data_dir: &Path,
) -> Result<HookConfigGuard> {
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
    let parent = private_root.parent().ok_or_else(|| {
        Error::new(
            "invalid",
            "Antigravity private instance directory has no parent",
        )
    })?;
    let basename = private_root.file_name().ok_or_else(|| {
        Error::new(
            "invalid",
            "Antigravity private instance directory has no name",
        )
    })?;
    let mut root_name = basename.to_owned();
    root_name.push("-hooks");
    let root = parent.join(root_name);
    // Fail closed on a pre-existing directory: never reuse or merge into an
    // unowned root.
    if fs::create_dir(&root).is_err() {
        return Err(Error::new(
            "io_error",
            "Could not create the owned Antigravity hook discovery directory",
        ));
    }
    let root = match fs::canonicalize(&root) {
        Ok(path) if path.is_dir() && path.starts_with(parent) => path,
        _ => {
            let _ = fs::remove_dir(&root);
            return Err(Error::new(
                "invalid",
                "Antigravity hook discovery directory is not contained by its parent",
            ));
        }
    };
    if root.starts_with(&workspace) || workspace.starts_with(&root) {
        let _ = fs::remove_dir(&root);
        return Err(Error::new(
            "invalid",
            "Antigravity hook discovery directory overlaps its workspace",
        ));
    }

    let agents_dir = root.join(".agents");
    let remove_agents_dir = match fs::create_dir(&agents_dir) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(_) => {
            let _ = fs::remove_dir(&root);
            return Err(Error::new(
                "io_error",
                "Could not create the private Antigravity hooks directory",
            ));
        }
    };
    let agents_dir = match fs::canonicalize(&agents_dir) {
        Ok(path) if path.is_dir() && path.starts_with(&root) => path,
        _ => {
            if remove_agents_dir {
                let _ = fs::remove_dir(&agents_dir);
            }
            let _ = fs::remove_dir(&root);
            return Err(Error::new(
                "invalid",
                "Antigravity hooks directory is not contained by its hook discovery root",
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
            let _ = fs::remove_dir(&root);
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
            let _ = fs::remove_dir(&root);
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
        let _ = fs::remove_dir(&root);
        return Err(Error::new(
            "io_error",
            "Could not write the owned Antigravity hook config",
        ));
    }
    // The bundled coordination skill rides the same discovery root:
    // `.agents/skills/eliot-swarm/` beside `hooks.json`, both
    // daemon-authored static files, never instance state. A CLI that
    // discovers skills from its `--add-dir` roots loads it natively; the
    // checkout and the global skill locations stay untouched. A missing
    // canonical copy fails the start rather than starting an Agent that
    // believes it has no skill.
    let skill_source = skill::skill_dir(data_dir);
    let skills_dir = agents_dir.join("skills");
    let skill_dir = skills_dir.join(skill::SKILL_NAME);
    let mut skill_dirs = Vec::new();
    let mut skill_files = Vec::new();
    let skill_result = (|| {
        for dir in [&skills_dir, &skill_dir] {
            if fs::create_dir(dir).is_err() {
                return Err(Error::new(
                    "io_error",
                    "Could not create the owned Antigravity skill directory",
                ));
            }
            skill_dirs.push(dir.clone());
        }
        skill_dirs.reverse();
        for file in [skill::SKILL_FILE, skill::SKILL_REFERENCE_FILE] {
            let text = skill::read_skill_file(&skill_source.join(file)).map_err(|_| {
                Error::new(
                    "skill_unavailable",
                    "Antigravity skill source is unavailable; the daemon did not materialize it",
                )
            })?;
            let target = skill_dir.join(file);
            let mut handle = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)
                .map_err(|_| {
                    Error::new(
                        "io_error",
                        "Could not create the owned Antigravity skill file",
                    )
                })?;
            if handle
                .write_all(text.as_bytes())
                .and_then(|()| handle.flush())
                .is_err()
            {
                let _ = fs::remove_file(&target);
                return Err(Error::new(
                    "io_error",
                    "Could not write the owned Antigravity skill file",
                ));
            }
            skill_files.push(target);
        }
        Ok(())
    })();
    if let Err(error) = skill_result {
        for file in &skill_files {
            let _ = fs::remove_file(file);
        }
        for dir in &skill_dirs {
            let _ = fs::remove_dir(dir);
        }
        let _ = fs::remove_file(&path);
        if remove_agents_dir {
            let _ = fs::remove_dir(&agents_dir);
        }
        let _ = fs::remove_dir(&root);
        return Err(error);
    }
    // The filesystem work above ran on the canonical (possibly verbatim)
    // paths naming these same files; store the plain spelling so `--add-dir`
    // and diagnostics carry a path the native CLI can load. Deriving both
    // from the stripped root keeps the guard consistent by construction.
    let root = without_verbatim_prefix(&root);
    let agents_dir = root.join(".agents");
    let path = agents_dir.join("hooks.json");
    let skills_dir = agents_dir.join("skills");
    let skill_dir = skills_dir.join(skill::SKILL_NAME);
    let skill_files = [skill::SKILL_FILE, skill::SKILL_REFERENCE_FILE]
        .into_iter()
        .map(|file| skill_dir.join(file))
        .collect();
    Ok(HookConfigGuard(Some(OwnedHookConfig {
        path,
        root,
        remove_agents_dir,
        remove_root: true,
        skill_files,
        skill_dirs: vec![skill_dir, skills_dir],
    })))
}

/// Strip the Windows verbatim path prefix (`\\?\`, including the long-UNC
/// `\\?\UNC\` form) so a canonicalized directory can be handed to native
/// tooling that builds `file://` URLs from its roots: AGY 1.5.3 rejects the
/// escaped `?` and drops the whole root, with its hooks, without firing.
/// The stripped path names the identical directory on Windows; elsewhere,
/// and for non-UTF-8 paths, this is the identity.
pub(crate) fn without_verbatim_prefix(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        let Some(text) = path.as_os_str().to_str() else {
            return path.to_owned();
        };
        if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{unc}"));
        }
        if let Some(plain) = text.strip_prefix(r"\\?\") {
            return PathBuf::from(plain);
        }
        path.to_owned()
    }
    #[cfg(not(windows))]
    {
        path.to_owned()
    }
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
    for file in &config.skill_files {
        let _ = fs::remove_file(file);
    }
    // `remove_dir` only removes an empty directory, so unexpected content
    // is left in place rather than deleted.
    for dir in &config.skill_dirs {
        let _ = fs::remove_dir(dir);
    }
    let _ = fs::remove_file(&config.path);
    if config.remove_agents_dir
        && let Some(parent) = config.path.parent()
    {
        let _ = fs::remove_dir(parent);
    }
    // `remove_dir` only removes an empty directory, so an unexpected
    // non-empty root is left in place rather than deleted.
    if config.remove_root {
        let _ = fs::remove_dir(&config.root);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn send_wait_requires_committed_native_association() {
        let (reply, association) = oneshot::channel();
        let wait = wait_for_association(
            association,
            Instant::now() + Duration::from_secs(1),
            "turn-1",
        );
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut wait)
                .await
                .is_err()
        );
        reply.send(Ok(())).unwrap();
        assert_eq!(
            wait.await.unwrap(),
            json!({"state": "applied", "turn_id": "turn-1"})
        );
    }

    #[tokio::test]
    async fn send_admission_deadline_returns_unknown_before_host_deadline() {
        assert!(SEND_TIMEOUT < TIMEOUT);
        let (reply, association) = oneshot::channel();
        let result = wait_for_association(association, Instant::now(), "turn-1").await;
        assert_eq!(result.unwrap_err().code, "unknown");
        assert!(reply.is_closed());
    }

    #[tokio::test]
    async fn validation_refusal_returns_a_typed_send_rejection() {
        let (reply, association) = oneshot::channel();
        reply
            .send(Err(Error::new("native_rejected", "private native detail")
                .with_where("plugin.antigravity.native.response")))
            .unwrap();
        let result = wait_for_association(association, Instant::now() + SEND_TIMEOUT, "turn-1")
            .await
            .unwrap();
        assert_eq!(result["state"], "rejected");
        assert_eq!(result["code"], "native_rejected");
        assert_eq!(result["where"], "plugin.antigravity.native.response");
        assert!(result.get("turn_id").is_none());
        assert!(!result.to_string().contains("private native detail"));
    }

    #[tokio::test]
    async fn native_exit_without_association_never_acknowledges_applied() {
        let (reply, association) = oneshot::channel();
        drop(reply);
        assert_eq!(
            wait_for_association(association, Instant::now() + SEND_TIMEOUT, "turn-1")
                .await
                .unwrap_err()
                .code,
            "unknown"
        );
    }

    fn stage_dirs(tag: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let base =
            std::env::temp_dir().join(format!("eliot-agy-stage-{tag}-{}", ulid::Ulid::new()));
        let instance_dir = base.join("instance-1");
        let workspace = base.join("workspace");
        let data_dir = base.join("data");
        fs::create_dir_all(&instance_dir).unwrap();
        fs::create_dir_all(&workspace).unwrap();
        // Fixture canonical copy standing in for the daemon-materialized
        // skill tree; staging mechanics are identical for real bytes.
        let skill_dir = data_dir.join("skills").join("skills").join("eliot-swarm");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: eliot-swarm\n---\ntest skill\n",
        )
        .unwrap();
        fs::write(skill_dir.join("reference.md"), "test reference\n").unwrap();
        (base, instance_dir, workspace, data_dir)
    }

    fn hook_command_for(helper: &str) -> String {
        let expected = hooks::hooks_json(
            hooks::DEFAULT_HOOK_NAME,
            &format!("{helper} plugin antigravity-hook"),
        );
        serde_json::to_string(&expected).unwrap()
    }

    fn has_verbatim_prefix(path: &Path) -> bool {
        path.as_os_str()
            .to_str()
            .is_some_and(|text| text.starts_with(r"\\?\"))
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_prefix_is_stripped_for_native_roots() {
        assert_eq!(
            without_verbatim_prefix(Path::new(r"\\?\C:\a\b")),
            PathBuf::from(r"C:\a\b")
        );
        assert_eq!(
            without_verbatim_prefix(Path::new(r"\\?\UNC\srv\sh\root")),
            PathBuf::from(r"\\srv\sh\root")
        );
        assert_eq!(
            without_verbatim_prefix(Path::new(r"C:\a\b")),
            PathBuf::from(r"C:\a\b")
        );
        assert_eq!(
            without_verbatim_prefix(Path::new(r"\\srv\sh\root")),
            PathBuf::from(r"\\srv\sh\root")
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn verbatim_prefix_is_identity_off_windows() {
        assert_eq!(
            without_verbatim_prefix(Path::new("/tmp/eliot-hooks")),
            PathBuf::from("/tmp/eliot-hooks")
        );
    }

    // AGY 1.5.3 turns `--add-dir` into a `file://` URL and rejects the
    // escaped `?` of a verbatim path, dropping the discovery root and its
    // hooks without firing. The staged root must never carry the prefix.
    #[test]
    fn staged_hook_root_has_no_verbatim_prefix() {
        let (base, instance_dir, workspace, data_dir) = stage_dirs("plain");
        let _temp_1 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        let guard = stage_hook_config(&instance_dir, &workspace, helper, &data_dir).unwrap();
        let inner = guard.0.as_ref().unwrap();
        assert!(!has_verbatim_prefix(inner.root()));
        assert!(!has_verbatim_prefix(&inner.path));
        assert_eq!(inner.path.parent().unwrap().join("hooks.json"), inner.path);
        drop(guard);
    }

    #[test]
    fn staged_hook_root_holds_hooks_and_skill() {
        let (base, instance_dir, workspace, data_dir) = stage_dirs("only");
        let _temp_2 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        let guard = stage_hook_config(&instance_dir, &workspace, helper, &data_dir).unwrap();
        // The discovery root is a sibling of the instance directory, never
        // the instance directory itself.
        assert_ne!(guard.root(), instance_dir);
        assert_eq!(
            guard.root().parent().unwrap(),
            without_verbatim_prefix(&fs::canonicalize(&base).unwrap())
        );
        let mut root_entries: Vec<_> = fs::read_dir(guard.root())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        root_entries.sort();
        assert_eq!(root_entries, [std::ffi::OsString::from(".agents")]);
        let mut agents_entries: Vec<_> = fs::read_dir(guard.root().join(".agents"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        agents_entries.sort();
        assert_eq!(
            agents_entries,
            [
                std::ffi::OsString::from("hooks.json"),
                std::ffi::OsString::from("skills"),
            ]
        );
        // The bundled skill stages beside the hook config with its exact
        // canonical files.
        let mut skill_entries: Vec<_> = fs::read_dir(
            guard
                .root()
                .join(".agents")
                .join("skills")
                .join("eliot-swarm"),
        )
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
        skill_entries.sort();
        assert_eq!(
            skill_entries,
            [
                std::ffi::OsString::from("SKILL.md"),
                std::ffi::OsString::from("reference.md"),
            ]
        );
        assert_eq!(
            fs::read_to_string(
                guard
                    .root()
                    .join(".agents")
                    .join("skills")
                    .join("eliot-swarm")
                    .join("SKILL.md")
            )
            .unwrap(),
            "---\nname: eliot-swarm\n---\ntest skill\n"
        );
        let (root, path) = {
            let inner = guard.0.as_ref().unwrap();
            (inner.root().to_owned(), inner.path.clone())
        };
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            hook_command_for("eliot-hook-helper") + "\n"
        );
        // The private instance directory gains no hook material.
        assert!(fs::read_dir(&instance_dir).unwrap().next().is_none());
        drop(guard);
        assert!(!path.exists());
        assert!(!root.exists());
        assert!(instance_dir.is_dir());
    }

    #[test]
    fn dropped_guard_cleans_root_so_restaging_succeeds() {
        let (base, instance_dir, workspace, data_dir) = stage_dirs("retry");
        let _temp_3 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        let guard = stage_hook_config(&instance_dir, &workspace, helper, &data_dir).unwrap();
        let root = guard.root().to_owned();
        assert!(root.is_dir());
        // Simulate a planning error after staging: the armed guard drops
        // without transfer to `Session`.
        drop(guard);
        assert!(!root.exists());
        // A corrected start with the same instance stages cleanly again.
        let guard = stage_hook_config(&instance_dir, &workspace, helper, &data_dir).unwrap();
        assert_eq!(guard.root(), root);
        assert!(guard.root().is_dir());
        drop(guard);
        assert!(!root.exists());
        assert!(instance_dir.is_dir());
    }

    #[test]
    fn staging_fails_closed_on_an_existing_root() {
        let (base, instance_dir, workspace, data_dir) = stage_dirs("exists");
        let _temp_4 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        let squat = base.join("instance-1-hooks");
        fs::create_dir_all(&squat).unwrap();
        let error = match stage_hook_config(&instance_dir, &workspace, helper, &data_dir) {
            Ok(_) => panic!("expected staging to fail on an existing root"),
            Err(error) => error,
        };
        assert_eq!(error.code, "io_error");
        // The unowned directory is left untouched.
        assert!(squat.is_dir());
    }

    #[test]
    fn staging_fails_closed_without_a_skill_source() {
        let (base, instance_dir, workspace, _) = stage_dirs("noskill");
        let _temp_5 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        let missing = base.join("no-data");
        let error = match stage_hook_config(&instance_dir, &workspace, helper, &missing) {
            Ok(_) => panic!("expected staging to fail without a skill source"),
            Err(error) => error,
        };
        assert_eq!(error.code, "skill_unavailable");
        // The withdrawn root leaves no hook config behind either.
        assert!(!base.join("instance-1-hooks").exists());
        assert!(fs::read_dir(&instance_dir).unwrap().next().is_none());
    }

    #[test]
    fn staging_rejects_an_overlapping_workspace() {
        let (base, instance_dir, _, data_dir) = stage_dirs("overlap");
        let _temp_6 = crate::test_temp::TempDir::own(base.clone());
        let helper = Path::new("eliot-hook-helper");
        // A workspace covering the instance tree is rejected before any
        // hook root is staged.
        let error = match stage_hook_config(&instance_dir, &base, helper, &data_dir) {
            Ok(_) => panic!("expected staging to fail on an overlapping workspace"),
            Err(error) => error,
        };
        assert_eq!(error.code, "invalid");
        assert!(!base.join("instance-1-hooks").exists());
        assert!(base.is_dir());
    }
}
