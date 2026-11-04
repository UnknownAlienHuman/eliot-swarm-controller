use crate::{
    events::{EventConfig, EventHandle, TurnFacts},
    native::{HttpOutcome, OwnedServer, ServerLaunch, native_no_write, types::ModelRef},
};
use eliot_core::{Error, ProcessRecord, ProcessState, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest, protocol::MAX_COMMAND_ID_BYTES,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{sync::mpsc, time::Instant};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_COMMANDS_PER_GENERATION: usize = 32;

#[derive(Default)]
struct OpenCodePlugin {
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
    server: OwnedServer,
    http: crate::native::OpenCodeHttp,
    events: EventHandle,
    command_inputs: HashMap<String, String>,
    command_turns: HashMap<String, String>,
    unknown_commands: HashSet<String>,
    rejected_commands: HashSet<String>,
    stop_command_id: Option<String>,
}

struct TargetEnvironment {
    variables: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
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

    let mut plugin = OpenCodePlugin::default();
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
            "OpenCode plugin input closed before shutdown completed",
        ))
    }
}

impl OpenCodePlugin {
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
                "OpenCode plugin method is not implemented",
            )),
        };
        let Some(session) = self.session.as_ref() else {
            return result;
        };
        match result {
            Err(error) => Err(session.server.diagnostic_error(error)),
            Ok(mut value) => {
                if request.method == "agent.snapshot"
                    || value
                        .get("state")
                        .and_then(Value::as_str)
                        .is_some_and(|state| matches!(state, "unknown" | "rejected" | "failed"))
                {
                    let diagnostics = serde_json::to_value(
                        session.server.diagnostic_error(Error::new("unknown", "")),
                    )
                    .map_err(|_| {
                        Error::new("internal", "OpenCode diagnostics could not be encoded")
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
        let params: InitializeParams =
            decode(raw, "OpenCode plugin initialize params are invalid")?;
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > eliot_plugin::protocol::MAX_CREDENTIAL_BYTES
        {
            return Err(Error::new(
                "invalid",
                "OpenCode plugin initialize requires protocol, private instance directory, and credential",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.credential != params.credential
                || existing.instance_dir != params.instance_dir
            {
                return Err(Error::new(
                    "bad_state",
                    "OpenCode plugin is already initialized for another instance",
                ));
            }
        } else {
            self.initialized = Some(Initialized {
                credential: params.credential,
                instance_dir: params.instance_dir,
            });
        }
        serde_json::to_value(opencode_manifest())
            .map_err(|_| Error::new("internal", "OpenCode plugin manifest could not be encoded"))
    }

    /// Retain a failed start exactly once: the same start command replays the
    /// stored error without new native I/O, and any other start is refused.
    fn retain_failed_start(&mut self, params: &AgentStartParams, error: Error) -> Error {
        self.failed_start = Some(FailedStart {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            command_id: params.command_id.clone(),
            error: error.clone(),
        });
        error
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "OpenCode agent.start params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return match session.phase {
                    SessionPhase::Ready => session.start_result.clone().ok_or_else(|| {
                        Error::new("unknown", "OpenCode start result was not retained")
                    }),
                    SessionPhase::Unknown => Err(Error::new(
                        "unknown",
                        "OpenCode session/start outcome is unresolved; it will not be repeated",
                    )),
                    SessionPhase::Stopped => Err(Error::new(
                        "bad_state",
                        "OpenCode Agent generation has already stopped",
                    )),
                };
            }
            return Err(Error::new(
                "stale_generation",
                "OpenCode plugin is already bound to another Agent generation",
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
                "OpenCode plugin already settled a different start command",
            ));
        }
        let initialized = self.initialized.as_ref().ok_or_else(not_initialized)?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "OpenCode start does not accept input; send it as a separate command",
            ));
        }
        if params.resume_from.is_some() {
            return Err(Error::new(
                "unsupported",
                "OpenCode resume is not connected in this PR",
            ));
        }
        validate_source_plan(params.source_plan.as_ref())?;
        let frozen_profile = params.profile.as_ref().ok_or_else(|| {
            Error::new("profile_error", "OpenCode requires a frozen profile object")
        })?;
        let profile = frozen_profile.as_object().ok_or_else(|| {
            Error::new("profile_error", "OpenCode requires a frozen profile object")
        })?;
        if profile.get("harness").and_then(Value::as_str) != Some("opencode") {
            return Err(Error::new(
                "profile_error",
                "OpenCode plugin received a profile for another harness",
            ));
        }
        let requested_model = profile
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode profile requires an exact model id",
                )
            })?
            .to_owned();
        let model_ref = split_model(&requested_model).ok_or_else(|| {
            Error::new(
                "profile_error",
                "OpenCode profile model must be provider/model",
            )
        })?;
        let workspace = params
            .workspace
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                Error::new("profile_error", "OpenCode requires an existing workspace")
            })?;
        if !workspace.is_dir() {
            return Err(Error::new(
                "profile_error",
                "OpenCode workspace must be an existing directory",
            ));
        }
        // No PATH default and no script wrapper: the frozen profile must name
        // the native `opencode` executable (the Muse treatment — stop and
        // process registration address the exact spawned PID, and a `.cmd`
        // chain would interpose `cmd.exe` between Eliot and the server).
        let program = profile
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|program| !program.trim().is_empty())
            .map(OsString::from)
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode profile requires an explicit cmd naming one executable path",
                )
            })?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
        } = target_environment(params.target_environment.as_ref(), profile.get("pass_env"))?;
        secrets.push(initialized.credential.clone());
        let policy = SecretPolicy::new(secrets.iter().cloned());

        let mut server = match OwnedServer::spawn(
            ServerLaunch {
                program,
                workspace: workspace.clone(),
                instance_dir: PathBuf::from(&initialized.instance_dir),
                environment,
                model: requested_model.clone(),
            },
            &policy,
        )
        .await
        {
            Ok(server) => server,
            // The spawn cleans up its own child on failure; no diagnostics
            // exist yet because no server was retained.
            Err(error) => {
                return Err(self.retain_failed_start(&params, error));
            }
        };
        let http = server.http().clone();
        // Subscribe before session create/input: the stream must observe the
        // session's whole durable sequence starting at `session.created`.
        let (frames, stream) = match http.subscribe(Instant::now() + RPC_TIMEOUT).await {
            Ok(subscribed) => subscribed,
            Err(error) => {
                let _ = server.terminate(true, Instant::now() + RPC_TIMEOUT).await;
                let error = server.diagnostic_error(error);
                return Err(self.retain_failed_start(&params, error));
            }
        };
        let events = crate::events::spawn(
            frames,
            stream,
            http.clone(),
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
            native_scope: String::new(),
            server,
            http,
            events,
            command_inputs: HashMap::new(),
            command_turns: HashMap::new(),
            unknown_commands: HashSet::new(),
            rejected_commands: HashSet::new(),
            stop_command_id: None,
        };

        if let Err(error) = register_native_process(
            peer,
            &initialized.credential,
            &session.agent_id,
            session.generation,
            &session.server,
        )
        .await
        {
            session.events.close().await;
            if session
                .server
                .terminate(true, Instant::now() + RPC_TIMEOUT)
                .await
                .is_err()
            {
                self.session = Some(session);
                let message = format!(
                    "{}; owned server cleanup is unconfirmed; no session or input was sent",
                    error.message
                );
                return Err(contextual_error(error, "unknown", message));
            }
            let error = session.server.diagnostic_error(error);
            return Err(self.retain_failed_start(&params, error));
        }

        session.native_scope = opencode_scope(session.server.data_dir());
        let title = format!("eliot-{}-g{}", params.agent_id, params.generation);
        let directory = workspace.to_string_lossy().into_owned();
        let created = match session
            .http
            .create_session(&title, &model_ref, &directory, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(created) => created,
            Err(HttpOutcome::Rejected { reason, .. }) => {
                let error = discard_failed_start(
                    &mut session,
                    contextual_error(
                        Error::new("rejected", reason),
                        "profile_error",
                        "OpenCode rejected session/create for the selected profile",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(&params, error));
            }
            Err(HttpOutcome::NotDispatched { reason }) => {
                let error = discard_failed_start(
                    &mut session,
                    contextual_error(
                        Error::new("native_not_dispatched", reason),
                        "native_not_dispatched",
                        "OpenCode session/create was not dispatched",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(&params, error));
            }
            Err(HttpOutcome::Unknown { reason }) => {
                self.session = Some(session);
                return Err(Error::new(
                    "unknown",
                    format!(
                        "OpenCode session/create may have changed native state; it will not be repeated ({reason})"
                    ),
                ));
            }
        };
        session.session_id = Some(created.id.clone());
        // Effective readback before readiness: the create response states the
        // exact model and location; on any mismatch no `session.ready` is
        // published and no session is kept.
        let effective_model = created
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()));
        let model_verified = effective_model.as_deref() == Some(requested_model.as_str());
        let location_ok = created
            .location
            .as_ref()
            .and_then(|location| {
                if location.directory.trim().is_empty() {
                    None
                } else {
                    Some(paths_equal(Path::new(&location.directory), &workspace))
                }
            })
            .unwrap_or(false);
        let effective = json!({
            "model": effective_model.clone(),
            "model_verified": model_verified,
            "cwd": created.location.as_ref().map(|location| location.directory.clone()),
        });
        if !model_verified || !location_ok {
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    "OpenCode effective model or session directory did not match the frozen profile",
                ),
            )
            .await;
            return Err(self.retain_failed_start(&params, error));
        }
        if let Err(error) = session
            .events
            .ready(
                session.native_scope.clone(),
                created.id.clone(),
                effective.clone(),
            )
            .await
        {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            return Err(Error::new(
                "unknown",
                format!(
                    "OpenCode session is known but session.ready was not acknowledged: {}",
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
            "native_ref": created.id,
            "requested": {
                "model": requested_model,
                "cwd": workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "OpenCode agent.command params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            _ => Err(Error::new(
                "unsupported",
                "OpenCode plugin command verb is not implemented",
            )),
        }
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "OpenCode Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "OpenCode session or event source outcome is unresolved",
            ));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "OpenCode send requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "OpenCode input exceeds 512 KiB",
            ));
        }
        if let Some(previous) = session.command_inputs.get(&params.command_id) {
            if previous != text {
                return Err(Error::new(
                    "source_integrity",
                    "OpenCode command id was reused with different input",
                ));
            }
            if let Some(turn_id) = session.command_turns.get(&params.command_id) {
                return Ok(json!({"state": "applied", "turn_id": turn_id}));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt outcome is unresolved and will not be repeated",
                ));
            }
            if session.rejected_commands.contains(&params.command_id) {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "OpenCode prompt was known not to apply",
                }));
            }
            return Err(Error::new(
                "unknown",
                "OpenCode command was already dispatched without a retained exact outcome",
            ));
        }
        let event_status = session.events.status();
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "OpenCode already has an active turn"));
        }
        let command_id = params.command_id.clone();
        if session.command_inputs.len() >= MAX_COMMANDS_PER_GENERATION {
            return Err(Error::new(
                "unsupported",
                "OpenCode L6.2 per-generation command history bound has been reached",
            ));
        }
        session
            .command_inputs
            .insert(command_id.clone(), text.to_owned());
        // One deadline for admission plus association: the HTTP 200 only
        // proves inbox admission; the send applies at the exact
        // input-to-execution association on the event stream.
        let deadline = Instant::now() + RPC_TIMEOUT;
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let inbox = match session.http.prompt(&session_id, text, deadline).await {
            Ok(inbox) => inbox,
            Err(HttpOutcome::Rejected { reason, .. }) => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({"state": "rejected", "reason": reason}));
            }
            Err(HttpOutcome::NotDispatched { reason }) => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({"state": "rejected", "reason": reason}));
            }
            Err(HttpOutcome::Unknown { .. }) => {
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt may have applied; the input will not be repeated",
                ));
            }
        };
        if inbox.session_id != session_id || inbox.id.trim().is_empty() {
            session.unknown_commands.insert(command_id.clone());
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "OpenCode prompt answered for another session",
            ));
        }
        if let Err(error) = session
            .events
            .associate_inbox(command_id.clone(), inbox.id.clone())
            .await
        {
            if native_no_write(&error.code) || error.code == "busy" {
                // The input was admitted, so a lost association is unknown
                // even when the registration itself was refused.
                session.events.abandon_association(command_id.clone()).await;
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt was admitted but its association is unknown",
                ));
            }
            session.events.abandon_association(command_id.clone()).await;
            session.unknown_commands.insert(command_id);
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                format!(
                    "OpenCode prompt was admitted but its association failed: {}",
                    error.message
                ),
            ));
        }
        match session
            .events
            .await_association(&command_id, deadline)
            .await
        {
            Ok(turn_id) => {
                session
                    .command_turns
                    .insert(command_id.clone(), turn_id.clone());
                Ok(json!({"state": "applied", "turn_id": turn_id}))
            }
            Err(_) => {
                session.events.abandon_association(command_id.clone()).await;
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                Err(Error::new(
                    "unknown",
                    "OpenCode prompt was admitted but its execution association is unknown",
                ))
            }
        }
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentSnapshotParams =
            decode(raw, "OpenCode agent.snapshot params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "OpenCode Agent session is not retained"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
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
                "OpenCode command has no proof for the requested native turn",
            );
        }
        if !known_command(session, &params.command_id) {
            return unknown_snapshot(&params, "OpenCode command identity is not retained");
        }
        if session.phase == SessionPhase::Stopped {
            return unknown_snapshot(
                &params,
                "OpenCode native server is stopped; native readback is unavailable",
            );
        }
        let Some(session_id) = session
            .session_id
            .as_deref()
            .or(event_status.session_id.as_deref())
        else {
            return unknown_snapshot(&params, "OpenCode session identity was not acknowledged");
        };
        // The session readback proves the exact owned session is alive; turn
        // facts come from the committed stream fold.
        if session
            .http
            .get_session(session_id, Instant::now() + RPC_TIMEOUT)
            .await
            .is_err()
        {
            return unknown_snapshot(
                &params,
                "OpenCode session readback did not prove the exact owned session",
            );
        }
        if session.command_inputs.contains_key(&params.command_id) && turn_id.is_none() {
            return unknown_snapshot(
                &params,
                "OpenCode command has no exact native turn association",
            );
        }
        let last_turn: Option<TurnFacts> = match turn_id.as_deref() {
            Some(requested) => {
                if event_status.active_turn_id.as_deref() == Some(requested) {
                    Some(TurnFacts {
                        command_id: params.command_id.clone(),
                        turn_id: requested.to_owned(),
                        status: None,
                        final_text: None,
                        terminal: false,
                    })
                } else if event_status
                    .last_turn
                    .as_ref()
                    .is_some_and(|turn| turn.turn_id == requested)
                {
                    event_status.last_turn.clone()
                } else {
                    return unknown_snapshot(
                        &params,
                        "OpenCode readback did not prove the exact requested turn",
                    );
                }
            }
            None => event_status.last_turn.clone(),
        };
        let state = if event_status.active_turn_id.is_some() {
            "busy"
        } else {
            "idle"
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
        let params: AgentStopParams = decode(raw, "OpenCode agent.stop params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "OpenCode Agent session is not retained"))?;
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
                    "OpenCode stop command id changed after dispatch",
                ));
            }
            return Err(Error::new(
                "unknown",
                "OpenCode stop outcome is unresolved and will not be repeated",
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

/// Stop proves the native session ended, then ends the owned server:
/// `DELETE /api/session/{id}` (204 removed, 404 already gone) followed by
/// a `GET` that must 404. A refused write re-checks the session: only a
/// proven-absent session applies, otherwise the outcome stays unknown.
async fn terminate_session(session: &mut AgentSession, force: bool) -> Result<()> {
    let deadline = Instant::now() + RPC_TIMEOUT;
    let begin = session.events.begin_stop().await;
    let mut removed = begin.is_ok();
    if removed && let Some(session_id) = session.session_id.clone() {
        // Only a proven end state ends the generation. Proven absent
        // always applies; proven present never does; an ambiguous
        // readback falls back to the authoritative close write (204
        // removed, 404 was already gone), never to a refused write.
        let delete = session.http.delete_session(&session_id, deadline).await;
        let absent = session.http.session_absent(&session_id, deadline).await;
        removed = match absent {
            Ok(true) => true,
            Ok(false) => false,
            Err(_) => delete.is_ok(),
        };
    }
    let process_exit = session.server.terminate(force, deadline).await;
    let ended = session.events.end_stop().await;
    if !removed || process_exit.is_err() || ended.is_err() {
        session.phase = SessionPhase::Unknown;
        return Err(Error::new(
            "unknown",
            "OpenCode session close, server shutdown, or session.ended outcome is unknown",
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
    server: &OwnedServer,
) -> Result<()> {
    let pid = server.pid().filter(|pid| *pid > 0).ok_or_else(|| {
        Error::new(
            "process_registration",
            "OpenCode server PID is unavailable; no session or input was sent",
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
            let message = format!(
                "OpenCode server process registration failed ({})",
                error.code
            );
            contextual_error(error, "process_registration", message)
        })?;
    let record: ProcessRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "process_registration",
            "OpenCode host returned an invalid process.put acknowledgement",
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
            "OpenCode host did not verify the running server under this Agent generation",
        ));
    }
    Ok(())
}

async fn discard_unstarted(session: &mut AgentSession) {
    session.events.close().await;
    let _ = session
        .server
        .terminate(true, Instant::now() + RPC_TIMEOUT)
        .await;
    session.phase = SessionPhase::Stopped;
}

async fn discard_failed_start(session: &mut AgentSession, error: Error) -> Error {
    discard_unstarted(session).await;
    session.server.diagnostic_error(error)
}

fn validate_binding(session: &AgentSession, agent_id: &str, generation: i64) -> Result<()> {
    if session.agent_id != agent_id || session.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "OpenCode request does not match the bound Agent generation",
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
            "OpenCode request requires a bounded Agent, generation, and command identity",
        ));
    }
    Ok(())
}

fn validate_source_plan(source_plan: Option<&Value>) -> Result<()> {
    let Some(plan) = source_plan.and_then(Value::as_object) else {
        return Err(Error::new(
            "unsupported",
            "OpenCode start requires an installed stream source plan",
        ));
    };
    if ["session", "turn", "message"]
        .iter()
        .any(|family| plan.get(*family).and_then(Value::as_str) != Some("stream"))
    {
        return Err(Error::new(
            "unsupported",
            "OpenCode start requires stream ownership for session, turn, and message",
        ));
    }
    Ok(())
}

fn known_command(session: &AgentSession, command_id: &str) -> bool {
    session.start_command_id == command_id
        || session.command_inputs.contains_key(command_id)
        || session.stop_command_id.as_deref() == Some(command_id)
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

pub(crate) fn contextual_error(
    error: Error,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Error {
    error.with_code(code).with_message(message)
}

fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
) -> Result<TargetEnvironment> {
    let Some(environment) = environment else {
        return Ok(TargetEnvironment {
            variables: Vec::new(),
            secrets: Vec::new(),
        });
    };
    let object = environment.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            "OpenCode target_environment must be an object",
        )
    })?;
    let pass_env = pass_env.and_then(Value::as_array);
    let mut vars = Vec::with_capacity(object.len());
    let mut secrets = Vec::new();
    for (name, value) in object {
        if name.is_empty() || name.contains('=') || name.contains('\0') {
            return Err(Error::new(
                "profile_error",
                "OpenCode environment contains an invalid variable name",
            ));
        }
        let value = value.as_str().ok_or_else(|| {
            Error::new(
                "profile_error",
                "OpenCode environment values must be strings",
            )
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
        vars.push((OsString::from(name), OsString::from(value)));
    }
    Ok(TargetEnvironment {
        variables: vars,
        secrets,
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

/// Installed model-id syntax is `providerID/id`, slashes included.
fn split_model(model: &str) -> Option<ModelRef> {
    let (provider_id, id) = model.split_once('/')?;
    if provider_id.trim().is_empty() || id.trim().is_empty() || id.contains('/') {
        return None;
    }
    Some(ModelRef {
        id: id.to_owned(),
        provider_id: provider_id.to_owned(),
        variant: None,
    })
}

fn paths_equal(left: &Path, right: &Path) -> bool {
    let left = std::fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = std::fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    if cfg!(windows) {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    } else {
        left == right
    }
}

pub(crate) fn opencode_scope(data_dir: &Path) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    let path = data_dir.to_string_lossy();
    let canonical = if cfg!(windows) {
        path.to_lowercase()
    } else {
        path.into_owned()
    };
    for byte in canonical.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("opencode-store:{hash:016x}")
}

fn opencode_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("turn".to_owned(), "stream".to_owned()),
    ]);
    PluginManifest {
        name: "opencode".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: "eliot-plugin/1".to_owned(),
        harness_version: "2.0.32".to_owned(),
        available: true,
        mode: "per_agent".to_owned(),
        verbs: vec!["launch".to_owned(), "send".to_owned(), "stop".to_owned()],
        child_verbs: Vec::new(),
        features: eliot_plugin::protocol::ManifestFeatures {
            discover: false,
            observe: false,
            start_requires_input: false,
            native_children: false,
        },
        sources: BTreeMap::from([("controlled".to_owned(), controlled)]),
        proxy: "none".to_owned(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(raw: &Value, message: &str) -> Result<T> {
    serde_json::from_value(raw.clone()).map_err(|_| Error::new("invalid", message))
}

fn not_initialized() -> Error {
    Error::new("bad_state", "OpenCode plugin has not been initialized")
}

#[cfg(test)]
mod tests {
    use super::{opencode_manifest, opencode_scope, split_model, validate_source_plan};
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn manifest_advertises_the_connected_pr1_verbs() {
        let manifest = opencode_manifest();
        assert_eq!(manifest.name, "opencode");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(manifest.harness_version, "2.0.32");
        assert!(manifest.available);
        assert_eq!(manifest.mode, "per_agent");
        assert_eq!(manifest.verbs, ["launch", "send", "stop"]);
        assert!(manifest.child_verbs.is_empty());
        assert!(!manifest.features.start_requires_input);
        assert!(!manifest.features.native_children);
        assert!(!manifest.features.discover);
        assert!(!manifest.features.observe);
        assert_eq!(manifest.sources.len(), 1);
        assert_eq!(manifest.proxy, "none");
    }

    #[test]
    fn source_plan_requires_the_connected_stream_families() {
        assert!(validate_source_plan(None).is_err());
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream", "message": "stream",
            })))
            .is_ok()
        );
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream", "message": "stream",
                "request": "stream",
            })))
            .is_ok(),
            "extra families are tolerated; the host installs the connected plan"
        );
    }

    #[test]
    fn model_split_requires_provider_slash_id() {
        let model = split_model("opencode/big-pickle").expect("slash form");
        assert_eq!(model.provider_id, "opencode");
        assert_eq!(model.id, "big-pickle");
        for bad in ["", "noprovider", "/noid", "provider/", "a/b/c", "/"] {
            assert!(split_model(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn native_scope_is_stable_per_store() {
        let scope = opencode_scope(&PathBuf::from("/eliot/instance/data"));
        assert_eq!(
            scope,
            opencode_scope(&PathBuf::from("/eliot/instance/data"))
        );
        assert_ne!(scope, opencode_scope(&PathBuf::from("/eliot/other/data")));
        assert!(scope.starts_with("opencode-store:"));
    }
}
