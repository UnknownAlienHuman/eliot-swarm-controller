use crate::{
    events::{self, EventConfig, EventHandle, TurnOutcome},
    native::{
        ClaudeClient, ClaudeProcess, native_no_write,
        types::{InitializeReadback, OutboundUserInput, OutboundUserMessage},
    },
};
use eliot_core::{Error, ProcessRecord, ProcessState, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    native_transport::NativeLaunch,
    protocol::{MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    path::PathBuf,
};
use tokio::{sync::mpsc, time::Instant};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_COMMAND_HISTORY: usize = 32;

#[derive(Default)]
struct ClaudePlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
}

struct Initialized {
    credential: String,
    instance_dir: String,
}

struct FailedStart {
    agent_id: String,
    generation: i64,
    command_id: String,
    error: Error,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    Ready,
    Unknown,
    Stopped,
}

struct AgentSession {
    agent_id: String,
    generation: i64,
    start_command_id: String,
    start_result: Option<Value>,
    phase: SessionPhase,
    session_id: Option<String>,
    native_scope: String,
    native: ClaudeProcess,
    native_client: ClaudeClient,
    events: EventHandle,
    command_inputs: HashMap<String, String>,
    command_turns: HashMap<String, String>,
    unknown_commands: HashSet<String>,
    rejected_commands: HashSet<String>,
    interrupt_applied: HashMap<String, String>,
    interrupted_turns: HashSet<String>,
    unknown_interrupt_turns: HashSet<String>,
    stop_command_id: Option<String>,
}

struct TargetEnvironment {
    variables: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    config_dir: Option<String>,
}

pub(crate) async fn run() -> Result<()> {
    let mut peer = Peer::stdio();
    let client = peer.client();
    let (request_tx, mut requests) = mpsc::channel(16);
    let reader = tokio::spawn(async move {
        while let Some(request) = peer.next_request().await {
            if request_tx.send(request).await.is_err() {
                break;
            }
        }
    });

    let mut plugin = ClaudePlugin::default();
    let mut completed_shutdown = false;
    loop {
        let Some(request) = requests.recv().await else {
            break;
        };
        let is_shutdown = request.method == "shutdown";
        let result = plugin.dispatch(&request, &client).await;
        let response = client
            .respond(request.id, result, Instant::now() + RPC_TIMEOUT)
            .await;
        if is_shutdown {
            completed_shutdown = response.is_ok();
            break;
        }
        if response.is_err() {
            break;
        }
    }

    if !completed_shutdown {
        plugin.shutdown_owned().await;
    }
    reader.abort();
    let _ = reader.await;
    if completed_shutdown {
        Ok(())
    } else {
        Err(Error::new(
            "transport_closed",
            "Claude plugin input closed before shutdown completed",
        ))
    }
}

impl ClaudePlugin {
    async fn dispatch(&mut self, request: &RpcRequest, client: &PeerClient) -> Result<Value> {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => self.command(&request.params).await,
            "agent.snapshot" => self.snapshot(&request.params).await,
            "agent.stop" => self.stop(&request.params).await,
            "shutdown" => self.shutdown().await,
            _ => Err(Error::new(
                "unsupported",
                "Claude plugin method is not implemented",
            )),
        };
        let Some(session) = self.session.as_ref() else {
            return result;
        };
        match result {
            Err(error) => Err(session.native_client.diagnostic_error(error)),
            Ok(mut value) => {
                if request.method == "agent.snapshot"
                    || value
                        .get("state")
                        .and_then(Value::as_str)
                        .is_some_and(|state| matches!(state, "unknown" | "rejected" | "failed"))
                {
                    let diagnostics = serde_json::to_value(
                        session
                            .native_client
                            .diagnostic_error(Error::new("unknown", "")),
                    )
                    .map_err(|_| {
                        Error::new("internal", "Claude diagnostics could not be encoded")
                    })?;
                    if let Some(tails) = diagnostics
                        .get("diagnostics")
                        .filter(|tails| !tails.is_null())
                    {
                        value["diagnostics"] = tails.clone();
                    }
                }
                Ok(value)
            }
        }
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Claude plugin initialize params are invalid")?;
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > MAX_CREDENTIAL_BYTES
        {
            return Err(Error::new(
                "invalid",
                "Claude plugin initialize requires protocol, private instance directory, and credential",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.credential != params.credential
                || existing.instance_dir != params.instance_dir
            {
                return Err(Error::new(
                    "bad_state",
                    "Claude plugin is already initialized for another instance",
                ));
            }
        } else {
            self.initialized = Some(Initialized {
                credential: params.credential,
                instance_dir: params.instance_dir,
            });
        }
        serde_json::to_value(claude_manifest())
            .map_err(|_| Error::new("internal", "Claude plugin manifest could not be encoded"))
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "Claude agent.start params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return match session.phase {
                    SessionPhase::Ready => session.start_result.clone().ok_or_else(|| {
                        Error::new("unknown", "Claude start result was not retained")
                    }),
                    SessionPhase::Unknown => Err(Error::new(
                        "unknown",
                        "Claude session outcome is unresolved; it will not be repeated",
                    )),
                    SessionPhase::Stopped => Err(Error::new(
                        "bad_state",
                        "Claude Agent generation has already stopped",
                    )),
                };
            }
            return Err(Error::new(
                "stale_generation",
                "Claude plugin is already bound to another Agent generation",
            ));
        }
        if let Some(failed) = &self.failed_start {
            if failed.agent_id == params.agent_id
                && failed.generation == params.generation
                && failed.command_id == params.command_id
            {
                return Err(failed.error.clone());
            }
            return Err(Error::new(
                "bad_state",
                "Claude plugin already settled a different start command",
            ));
        }
        let initialized = self.initialized.as_ref().ok_or_else(not_initialized)?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Claude start does not accept input; send it as a separate command",
            ));
        }
        if params.resume_from.is_some() {
            return Err(Error::new(
                "unsupported",
                "Claude resume is not connected in this slice",
            ));
        }
        validate_source_plan(params.source_plan.as_ref())?;
        let frozen_profile = params.profile.as_ref().ok_or_else(|| {
            Error::new("profile_error", "Claude requires a frozen profile object")
        })?;
        let profile = frozen_profile.as_object().ok_or_else(|| {
            Error::new("profile_error", "Claude requires a frozen profile object")
        })?;
        if profile.get("harness").and_then(Value::as_str) != Some("claude") {
            return Err(Error::new(
                "profile_error",
                "Claude plugin received a profile for another harness",
            ));
        }
        let requested_model = profile
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new("profile_error", "Claude profile requires an exact model id")
            })?
            .to_owned();
        // Requested permission posture next to the CLI's effective mode: PR1
        // passes no permission-mode flag, so the readback is the CLI default
        // until PR2 configures it.
        let requested_permission_mode = profile.get("permissions").cloned().unwrap_or(Value::Null);
        let workspace = params
            .workspace
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| Error::new("profile_error", "Claude requires an existing workspace"))?;
        if !workspace.is_dir() {
            return Err(Error::new(
                "profile_error",
                "Claude workspace must be an existing directory",
            ));
        }
        // No PATH default: a resolved wrapper would orphan the real CLI on
        // stop, when only the wrapper PID is owned. The frozen profile must
        // name the native executable.
        let program = profile
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|program| !program.trim().is_empty())
            .map(OsString::from)
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "Claude profile requires an explicit cmd naming one executable path",
                )
            })?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
            config_dir,
        } = target_environment(params.target_environment.as_ref(), profile.get("pass_env"))?;
        secrets.push(initialized.credential.clone());
        // Eliot mints the session id and pre-assigns it: SessionStart hook
        // lines carry it before any input, so no `start_requires_input`.
        let session_id = new_uuid();
        let launch = NativeLaunch {
            program,
            args: vec![
                OsString::from("-p"),
                OsString::from("--input-format"),
                OsString::from("stream-json"),
                OsString::from("--output-format"),
                OsString::from("stream-json"),
                OsString::from("--verbose"),
                OsString::from("--session-id"),
                OsString::from(&session_id),
                OsString::from("--model"),
                OsString::from(&requested_model),
            ],
            cwd: workspace.clone(),
            environment,
        };
        let mut native =
            ClaudeProcess::spawn(launch, SecretPolicy::new(secrets.iter().cloned())).await?;
        let frames = native.take_frames()?;
        let stream = native.stream_state();
        let native_client = native.client();
        let events = events::spawn(
            frames,
            stream,
            peer.clone(),
            EventConfig {
                credential: initialized.credential.clone(),
                agent_id: params.agent_id.clone(),
                generation: params.generation,
                secrets: secrets.clone(),
            },
        );
        let mut session = AgentSession {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            start_command_id: params.command_id.clone(),
            start_result: None,
            phase: SessionPhase::Unknown,
            session_id: None,
            native_scope: claude_scope(config_dir.as_deref()),
            native,
            native_client,
            events,
            command_inputs: HashMap::new(),
            command_turns: HashMap::new(),
            unknown_commands: HashSet::new(),
            rejected_commands: HashSet::new(),
            interrupt_applied: HashMap::new(),
            interrupted_turns: HashSet::new(),
            unknown_interrupt_turns: HashSet::new(),
            stop_command_id: None,
        };

        if let Err(error) = register_native_process(
            peer,
            &initialized.credential,
            &session.agent_id,
            session.generation,
            &session.native,
        )
        .await
        {
            session.events.close().await;
            if session
                .native
                .terminate(true, Instant::now() + RPC_TIMEOUT)
                .await
                .is_err()
            {
                self.session = Some(session);
                let message = format!(
                    "{}; owned CLI cleanup is unconfirmed; no session or input was sent",
                    error.message
                );
                return Err(contextual_error(error, "unknown", message));
            }
            let error = session.native_client.diagnostic_error(error);
            self.failed_start = Some(FailedStart {
                agent_id: params.agent_id,
                generation: params.generation,
                command_id: params.command_id,
                error: error.clone(),
            });
            return Err(error);
        }

        let readback = match session
            .native_client
            .control(
                "initialize",
                json!({"hooks": null}),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                let error = discard_failed_start(
                    &mut session,
                    contextual_error(error, "profile_error", "Claude initialize failed"),
                )
                .await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
        };
        let readback: InitializeReadback = match serde_json::from_value(readback) {
            Ok(readback) => readback,
            Err(_) => {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Claude initialize readback did not match its envelope",
                    ),
                )
                .await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
        };
        // The effective settings are checked BEFORE readiness: on any
        // mismatch no `session.ready` is published and no session is kept.
        let model_available = readback.models.iter().any(|model| {
            model.value.as_deref() == Some(requested_model.as_str())
                || model.resolved_model.as_deref() == Some(requested_model.as_str())
        });
        if !model_available || readback.session_state.as_deref() != Some("idle") {
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    "Claude effective model or session state did not match the frozen profile",
                ),
            )
            .await;
            self.failed_start = Some(FailedStart {
                agent_id: params.agent_id,
                generation: params.generation,
                command_id: params.command_id,
                error: error.clone(),
            });
            return Err(error);
        }
        let effective = json!({
            "model": requested_model,
            "permission_mode": readback.current_permission_mode,
            "session_state": readback.session_state,
            "pid": readback.pid,
        });
        session.session_id = Some(session_id.clone());
        if let Err(error) = session
            .events
            .ready(
                session.native_scope.clone(),
                session_id.clone(),
                effective.clone(),
            )
            .await
        {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            return Err(Error::new(
                "unknown",
                format!(
                    "Claude session is known but session.ready was not acknowledged: {}",
                    error.message
                ),
            ));
        }
        session.phase = SessionPhase::Ready;
        let result = json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": session_id,
            "requested": {
                "model": requested_model,
                "permission_mode": requested_permission_mode,
                "cwd": workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "Claude agent.command params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Claude plugin command verb is not implemented",
            )),
        }
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Claude Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Claude session or event source outcome is unresolved",
            ));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Claude send requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "Claude input exceeds 512 KiB",
            ));
        }
        if let Some(previous) = session.command_inputs.get(&params.command_id) {
            if previous != text {
                return Err(Error::new(
                    "source_integrity",
                    "Claude command id was reused with different input",
                ));
            }
            if let Some(turn_id) = session.command_turns.get(&params.command_id) {
                return Ok(json!({"state": "applied", "turn_id": turn_id}));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "Claude input outcome is unresolved and will not be repeated",
                ));
            }
            if session.rejected_commands.contains(&params.command_id) {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Claude input was known not to apply",
                }));
            }
            return Err(Error::new(
                "unknown",
                "Claude command was already dispatched without a retained exact outcome",
            ));
        }
        let event_status = session.events.status();
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "Claude already has an active turn"));
        }
        let command_id = params.command_id.clone();
        if session.command_inputs.len() >= MAX_COMMAND_HISTORY {
            return Err(Error::new(
                "unsupported",
                "Claude L6.1 per-generation command history bound has been reached",
            ));
        }
        session
            .command_inputs
            .insert(command_id.clone(), text.to_owned());
        // Mint the exact turn association before the write and register it
        // with the event task first: an early lifecycle can never miss its
        // waiter, and a lost association latches unknown without a rewrite.
        let turn_id = new_uuid();
        let outcome = match session
            .events
            .expect_turn(command_id.clone(), turn_id.clone())
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if error.code == "busy" => {
                session.command_inputs.remove(&command_id);
                return Err(Error::new("busy", "Claude already has an active turn"));
            }
            Err(error) => {
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                return Err(Error::new("unknown", error.message));
            }
        };
        let input = OutboundUserInput {
            kind: "user",
            uuid: turn_id.clone(),
            session_id: session
                .session_id
                .clone()
                .ok_or_else(|| Error::new("unknown", "Claude session identity is unresolved"))?,
            parent_tool_use_id: None,
            message: OutboundUserMessage {
                role: "user",
                content: text.to_owned(),
            },
        };
        if let Err(error) = session
            .native_client
            .send_user(input, Instant::now() + RPC_TIMEOUT)
            .await
        {
            if native_no_write(&error.code) {
                // Proven never dispatched: withdraw the pending turn before
                // answering, or every later send wedges `busy` behind it.
                session.events.cancel_turn(command_id.clone()).await;
                session.rejected_commands.insert(command_id);
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Claude input was not dispatched",
                }));
            }
            session.unknown_commands.insert(command_id);
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "Claude input may have applied; it will not be repeated",
            ));
        }
        let outcome = tokio::time::timeout(RPC_TIMEOUT, outcome)
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    "Claude turn association was not observed; the input will not be repeated",
                )
            })?;
        let outcome = outcome.map_err(|_| {
            Error::new(
                "unknown",
                "Claude turn association was lost; the input will not be repeated",
            )
        })?;
        match outcome {
            TurnOutcome::Associated => {
                session.command_turns.insert(command_id, turn_id.clone());
                Ok(json!({"state": "applied", "turn_id": turn_id}))
            }
            TurnOutcome::Rejected(reason) => {
                session.rejected_commands.insert(command_id);
                Ok(json!({"state": "rejected", "reason": reason}))
            }
            TurnOutcome::Unknown => {
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                Err(Error::new(
                    "unknown",
                    "Claude input outcome is unresolved and will not be repeated",
                ))
            }
        }
    }

    async fn interrupt_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Claude Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Claude session or event source outcome is unresolved",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|turn_id| !turn_id.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "Claude interrupt requires the exact turn id"))?;
        if params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "Claude interrupt does not address a native request",
            ));
        }
        let command_id = params.command_id.clone();
        if let Some(applied_turn) = session.interrupt_applied.get(&command_id) {
            if applied_turn != turn_id {
                return Err(Error::new(
                    "source_integrity",
                    "Claude command id was reused for another turn",
                ));
            }
            return Ok(json!({
                "state": "applied",
                "command_id": command_id,
                "turn_id": turn_id,
            }));
        }
        if session.unknown_interrupt_turns.contains(turn_id) {
            return Err(Error::new(
                "unknown",
                "Claude interrupt outcome is unresolved and will not be repeated",
            ));
        }
        if let Some(reason) = session.events.status().failure {
            return Err(Error::new("unknown", reason));
        }
        // The event task answers behind any queued terminal frame, so an
        // interrupt racing completion refuses instead of writing twice.
        if !session.events.check_turn(turn_id).await {
            return Err(Error::new(
                "stale_turn",
                "Claude turn is not the active turn",
            ));
        }
        if session.interrupted_turns.contains(turn_id) {
            session
                .interrupt_applied
                .insert(command_id.clone(), turn_id.to_owned());
            return Ok(json!({
                "state": "applied",
                "command_id": command_id,
                "turn_id": turn_id,
            }));
        }
        let response = match session
            .native_client
            .control("interrupt", json!({}), Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(response) => response,
            Err(error) if native_no_write(&error.code) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Claude interrupt was not dispatched",
                }));
            }
            Err(_) => {
                session.unknown_interrupt_turns.insert(turn_id.to_owned());
                return Err(Error::new(
                    "unknown",
                    "Claude interrupt may have applied; it will not be repeated",
                ));
            }
        };
        session.interrupted_turns.insert(turn_id.to_owned());
        session
            .interrupt_applied
            .insert(command_id.clone(), turn_id.to_owned());
        let mut applied = json!({
            "state": "applied",
            "command_id": command_id,
            "turn_id": turn_id,
        });
        if let Some(still_queued) = response.get("still_queued").cloned() {
            applied["still_queued"] = still_queued;
        }
        Ok(applied)
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentSnapshotParams = decode(raw, "Claude agent.snapshot params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Claude Agent session is not retained"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        // PR1 has no live native session poll: the snapshot is the committed
        // stream position (session/turn bindings plus the latest terminal
        // facts), never a fresh native query.
        if session.stop_command_id.as_deref() == Some(params.command_id.as_str())
            && session.phase == SessionPhase::Stopped
            && params.turn_id.is_none()
        {
            let event_status = session.events.status();
            let native_ref = session
                .session_id
                .clone()
                .or_else(|| event_status.session_id.clone());
            return Ok(json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "state": "stopped",
                "native_scope": session.native_scope,
                "native_ref": native_ref,
                "active_turn": null,
                "last_turn": event_status.last_turn,
                "batch_unknown": event_status.batch_unknown,
                "source_unknown": event_status.source_unknown,
            }));
        }
        let event_status = session.events.status();
        let turn_id = match params.turn_id.as_deref() {
            Some(turn_id) => Some(turn_id.to_owned()),
            None => session.command_turns.get(&params.command_id).cloned(),
        };
        if let Some(requested_turn) = turn_id.as_deref()
            && session
                .command_turns
                .get(&params.command_id)
                .map(String::as_str)
                != Some(requested_turn)
        {
            return unknown_snapshot(
                &params,
                "Claude command has no proof for the requested native turn",
            );
        }
        if !known_command(session, &params.command_id) {
            return unknown_snapshot(&params, "Claude command identity is not retained");
        }
        if session.phase == SessionPhase::Stopped {
            return unknown_snapshot(
                &params,
                "Claude native process is stopped; native readback is unavailable",
            );
        }
        let Some(session_id) = session
            .session_id
            .as_deref()
            .or(event_status.session_id.as_deref())
        else {
            return unknown_snapshot(&params, "Claude session identity was not acknowledged");
        };
        if session.command_inputs.contains_key(&params.command_id) && turn_id.is_none() {
            return unknown_snapshot(
                &params,
                "Claude command has no exact native turn association",
            );
        }
        let state = if event_status.batch_unknown || event_status.source_unknown {
            "unknown"
        } else if event_status.active_turn_id.is_some() {
            "busy"
        } else if event_status.ready {
            "idle"
        } else {
            "unknown"
        };
        let last_turn = match turn_id.as_deref() {
            Some(turn_id) => {
                let active = event_status.active_turn_id.as_deref() == Some(turn_id);
                let retained = event_status
                    .last_turn
                    .as_ref()
                    .filter(|last| last.turn_id == turn_id)
                    .cloned();
                if !active && retained.is_none() {
                    return unknown_snapshot(
                        &params,
                        "Claude readback did not prove the exact requested turn",
                    );
                }
                retained
            }
            None => event_status.last_turn.clone(),
        };
        let result_known = last_turn
            .as_ref()
            .is_some_and(|turn| turn.final_text.is_some());
        Ok(json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": state,
            "native_scope": session.native_scope,
            "native_ref": session_id,
            "active_turn": event_status.active_turn_id,
            "last_turn": last_turn,
            "batch_unknown": event_status.batch_unknown,
            "source_unknown": event_status.source_unknown,
            "result_known": result_known,
        }))
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentStopParams = decode(raw, "Claude agent.stop params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Claude Agent session is not retained"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.phase == SessionPhase::Stopped
            && session.stop_command_id.as_deref() == Some(params.command_id.as_str())
        {
            return Ok(json!({"state": "applied"}));
        }
        if let Some(existing) = &session.stop_command_id {
            if existing != &params.command_id {
                return Err(Error::new(
                    "source_integrity",
                    "Claude stop command id changed after dispatch",
                ));
            }
            return Err(Error::new(
                "unknown",
                "Claude stop outcome is unresolved and will not be repeated",
            ));
        }
        session.stop_command_id = Some(params.command_id);
        terminate_session(session, params.force).await?;
        Ok(json!({"state": "applied"}))
    }

    async fn shutdown(&mut self) -> Result<Value> {
        if let Some(session) = self.session.as_mut() {
            if session.phase != SessionPhase::Stopped {
                terminate_session(session, true).await?;
            }
            session.events.close().await;
        }
        Ok(json!({"state": "shutdown"}))
    }

    async fn shutdown_owned(&mut self) {
        if let Some(session) = self.session.as_mut() {
            if session.phase != SessionPhase::Stopped {
                let _ = terminate_session(session, true).await;
            }
            session.events.close().await;
        }
    }
}

async fn terminate_session(session: &mut AgentSession, force: bool) -> Result<()> {
    let begin = session.events.begin_stop().await;
    let process_exit = session
        .native
        .terminate(force, Instant::now() + RPC_TIMEOUT)
        .await;
    let drained = session
        .native
        .await_output_drained(Instant::now() + RPC_TIMEOUT)
        .await;
    let ended = session.events.end_stop().await;
    if begin.is_err() || process_exit.is_err() || drained.is_err() || ended.is_err() {
        session.phase = SessionPhase::Unknown;
        return Err(Error::new(
            "unknown",
            "Claude process/output drain or session.ended outcome is unknown",
        ));
    }
    session.phase = SessionPhase::Stopped;
    Ok(())
}

async fn register_native_process(
    peer: &PeerClient,
    credential: &str,
    agent_id: &str,
    generation: i64,
    native: &ClaudeProcess,
) -> Result<()> {
    let pid = native.pid().filter(|pid| *pid > 0).ok_or_else(|| {
        Error::new(
            "process_registration",
            "Claude CLI PID is unavailable; no session or input was sent",
        )
    })?;
    let value = peer
        .call(
            "process.put",
            json!({
                "credential": credential,
                "agent_id": agent_id,
                "generation": generation,
                "pid": pid,
                "role": "harness",
            }),
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        .map_err(|error| {
            let message = format!("Claude CLI process registration failed ({})", error.code);
            contextual_error(error, "process_registration", message)
        })?;
    let record: ProcessRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "process_registration",
            "Claude host returned an invalid process.put acknowledgement",
        )
    })?;
    if record.identity.pid != pid
        || record.identity.host_boot_id.trim().is_empty()
        || record.identity.start_time == 0
        || record.owner != format!("agent:{agent_id}#{generation}")
        || record.role != "harness"
        || record.state != ProcessState::Running
        || record.exit_code.is_some()
    {
        return Err(Error::new(
            "process_registration",
            "Claude host did not verify the running CLI under this Agent generation",
        ));
    }
    Ok(())
}

async fn discard_unstarted(session: &mut AgentSession) {
    session.events.close().await;
    let _ = session
        .native
        .terminate(true, Instant::now() + RPC_TIMEOUT)
        .await;
    session.phase = SessionPhase::Stopped;
}

async fn discard_failed_start(session: &mut AgentSession, error: Error) -> Error {
    discard_unstarted(session).await;
    session.native_client.diagnostic_error(error)
}

fn claude_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("turn".to_owned(), "stream".to_owned()),
    ]);
    PluginManifest {
        name: "claude".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: "eliot-plugin/1".to_owned(),
        harness_version: "2.1.278".to_owned(),
        available: true,
        mode: "per_agent".to_owned(),
        verbs: vec![
            "launch".to_owned(),
            "send".to_owned(),
            "stop".to_owned(),
            "interrupt".to_owned(),
        ],
        child_verbs: Vec::new(),
        features: eliot_plugin::protocol::ManifestFeatures {
            start_requires_input: false,
            native_children: false,
            discover: false,
            observe: false,
        },
        sources: BTreeMap::from([("controlled".to_owned(), controlled)]),
        proxy: "none".to_owned(),
    }
}

/// Session and turn uuids: the 128-bit ULID rendered as 8-4-4-4-12 hex for
/// the CLI's `--session-id <uuid>` / `user.uuid` surface.
fn new_uuid() -> String {
    let bits = ulid::Ulid::new().0;
    let hex = format!("{bits:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Native scope names the CLI config identity: the canonical
/// `CLAUDE_CONFIG_DIR` when the target environment sets one, else the CLI
/// default resolution, which the plugin must not guess at.
fn claude_scope(config_dir: Option<&str>) -> String {
    match config_dir.filter(|dir| !dir.trim().is_empty()) {
        Some(dir) => {
            let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| PathBuf::from(dir));
            format!("claude-config:{}", canonical.to_string_lossy())
        }
        None => "claude-config:default".to_owned(),
    }
}

fn unknown_snapshot(params: &AgentSnapshotParams, reason: &str) -> Result<Value> {
    Ok(json!({
        "agent_id": params.agent_id,
        "generation": params.generation,
        "command_id": params.command_id,
        "state": "unknown",
        "active_turn": null,
        "last_turn": null,
        "source_unknown": true,
        "result_known": false,
        "reason": reason,
    }))
}

fn known_command(session: &AgentSession, command_id: &str) -> bool {
    session.start_command_id == command_id
        || session.command_inputs.contains_key(command_id)
        || session.interrupt_applied.contains_key(command_id)
        || session.stop_command_id.as_deref() == Some(command_id)
}

fn validate_binding(session: &AgentSession, agent_id: &str, generation: i64) -> Result<()> {
    if session.agent_id != agent_id || session.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "Claude request does not match the bound Agent generation",
        ));
    }
    Ok(())
}

fn validate_identity(agent_id: &str, generation: i64, command_id: &str) -> Result<()> {
    if agent_id.trim().is_empty()
        || generation <= 0
        || command_id.trim().is_empty()
        || command_id.len() > MAX_COMMAND_ID_BYTES
    {
        return Err(Error::new(
            "invalid",
            "Claude request requires a bounded Agent, generation, and command identity",
        ));
    }
    Ok(())
}

fn validate_source_plan(source_plan: Option<&Value>) -> Result<()> {
    let Some(plan) = source_plan.and_then(Value::as_object) else {
        return Err(Error::new(
            "unsupported",
            "Claude start requires an installed stream source plan",
        ));
    };
    if ["session", "turn", "message"]
        .iter()
        .any(|family| plan.get(*family).and_then(Value::as_str) != Some("stream"))
    {
        return Err(Error::new(
            "unsupported",
            "Claude start requires stream ownership for session, turn, and message",
        ));
    }
    Ok(())
}

fn contextual_error(error: Error, code: impl Into<String>, message: impl Into<String>) -> Error {
    error.with_code(code).with_message(message)
}

fn decode<T: serde::de::DeserializeOwned>(raw: &Value, message: &str) -> Result<T> {
    serde_json::from_value(raw.clone()).map_err(|_| Error::new("invalid", message))
}

fn not_initialized() -> Error {
    Error::new("bad_state", "Claude plugin has not been initialized")
}

fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
) -> Result<TargetEnvironment> {
    let Some(environment) = environment else {
        return Ok(TargetEnvironment {
            variables: Vec::new(),
            secrets: Vec::new(),
            config_dir: None,
        });
    };
    let object = environment.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            "Claude target_environment must be an object",
        )
    })?;
    let pass_env = pass_env.and_then(Value::as_array);
    let mut vars = Vec::with_capacity(object.len());
    let mut secrets = Vec::new();
    let mut config_dir = None;
    for (name, value) in object {
        if name.is_empty() || name.contains('=') || name.contains('\0') {
            return Err(Error::new(
                "profile_error",
                "Claude environment contains an invalid variable name",
            ));
        }
        let value = value.as_str().ok_or_else(|| {
            Error::new("profile_error", "Claude environment values must be strings")
        })?;
        let explicitly_passed = pass_env.is_some_and(|names| {
            names.iter().filter_map(Value::as_str).any(|passed| {
                if cfg!(windows) {
                    name.eq_ignore_ascii_case(passed)
                } else {
                    name == passed
                }
            })
        });
        if (is_secret_name(name) || explicitly_passed) && !value.is_empty() {
            secrets.push(value.to_owned());
        }
        if name.eq_ignore_ascii_case("CLAUDE_CONFIG_DIR") && config_dir.is_none() {
            config_dir = Some(value.to_owned());
        }
        vars.push((OsString::from(name), OsString::from(value)));
    }
    Ok(TargetEnvironment {
        variables: vars,
        secrets,
        config_dir,
    })
}

fn is_secret_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("API_KEY")
        || upper.contains("AUTH")
}

#[cfg(test)]
mod tests {
    use super::{claude_manifest, claude_scope, new_uuid, validate_source_plan};
    use serde_json::json;
    use std::collections::{BTreeMap, HashSet};

    #[test]
    fn uuid_mints_unique_hyphenated_ids() {
        let ids: HashSet<String> = (0..64).map(|_| new_uuid()).collect();
        assert_eq!(ids.len(), 64);
        for id in &ids {
            assert_eq!(id.len(), 36);
            assert_eq!(
                id.chars()
                    .enumerate()
                    .filter(|(_, c)| *c == '-')
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>(),
                vec![8, 13, 18, 23]
            );
            assert!(id.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
        }
    }

    #[test]
    fn manifest_advertises_the_pr1_slice() {
        let manifest = claude_manifest();
        assert_eq!(manifest.name, "claude");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(
            manifest.verbs,
            vec![
                "launch".to_owned(),
                "send".to_owned(),
                "stop".to_owned(),
                "interrupt".to_owned()
            ]
        );
        assert!(!manifest.features.start_requires_input);
        assert_eq!(
            manifest.sources["controlled"],
            BTreeMap::from([
                ("message".to_owned(), "stream".to_owned()),
                ("session".to_owned(), "stream".to_owned()),
                ("turn".to_owned(), "stream".to_owned()),
            ])
        );
    }

    #[test]
    fn scope_names_the_config_identity() {
        assert_eq!(claude_scope(None), "claude-config:default");
        assert_eq!(claude_scope(Some("  ")), "claude-config:default");
        let dir = std::env::temp_dir();
        let scope = claude_scope(Some(&dir.to_string_lossy()));
        assert!(scope.starts_with("claude-config:"), "{scope}");
        assert!(scope.len() > "claude-config:".len());
    }

    #[test]
    fn source_plan_requires_the_stream_families() {
        assert!(validate_source_plan(None).is_err());
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream",
                "turn": "stream",
                "message": "stream",
            })))
            .is_ok()
        );
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream",
                "turn": "stream",
            })))
            .is_err()
        );
    }
}
