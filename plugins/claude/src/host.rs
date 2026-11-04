use crate::{
    events::{self, EventConfig, EventHandle, EventStatus, TurnOutcome},
    native::{
        ClaudeClient, ClaudeProcess,
        types::{InitializeReadback, OutboundUserInput, OutboundUserMessage},
    },
};
use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    native_transport::NativeLaunch,
    protocol::MAX_CREDENTIAL_BYTES,
    session::{
        self, contextual_error, decode, native_no_write, not_initialized, optional_profile_text,
        parse_resume_subject, register_native_process, unknown_control, unknown_snapshot,
        validate_binding, validate_identity, validate_source_plan,
    },
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
    reply_outcomes: HashMap<String, ReplyOutcome>,
    stop_command_id: Option<String>,
}

/// Validated start inputs: the frozen profile plus the minted session id.
/// Built before any spawn; every later start phase reads from it.
struct StartPlan {
    workspace: PathBuf,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    config_dir: Option<String>,
    credential: String,
    session_id: String,
    requested_model: String,
    requested_permission_mode: Option<String>,
    resume_ref: Option<String>,
}

/// Retained approve/deny outcome for exact command-id replay. The stored
/// value is returned verbatim; a reused id with another binding is an
/// integrity error.
struct ReplyOutcome {
    verb: String,
    request_id: String,
    turn_id: String,
    result: Value,
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

    /// Replay a retained start outcome, or refuse a conflicting generation.
    /// `None` means this is a fresh start that proceeds to spawn.
    fn replay_start(&self, params: &AgentStartParams) -> Option<Result<Value>> {
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return Some(match session.phase {
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
                });
            }
            return Some(Err(Error::new(
                "stale_generation",
                "Claude plugin is already bound to another Agent generation",
            )));
        }
        if let Some(failed) = &self.failed_start {
            if failed.agent_id == params.agent_id
                && failed.generation == params.generation
                && failed.command_id == params.command_id
            {
                return Some(Err(failed.error.clone()));
            }
            return Some(Err(Error::new(
                "bad_state",
                "Claude plugin already settled a different start command",
            )));
        }
        None
    }

    /// Validate the frozen profile and freeze the start inputs. No spawn yet:
    /// every `profile_error` returns before any native process exists.
    fn plan_start(&self, params: &AgentStartParams) -> Result<StartPlan> {
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Claude"))?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Claude start does not accept input; send it as a separate command",
            ));
        }
        let resume_subject = params
            .resume_from
            .as_ref()
            .map(|resume| {
                parse_resume_subject(
                    resume,
                    params.source_state.as_ref(),
                    &params.agent_id,
                    params.generation,
                    "Claude",
                )
            })
            .transpose()?;
        validate_source_plan(
            "Claude",
            &["session", "turn", "message", "request"],
            params.source_plan.as_ref(),
        )?;
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
        // The daemon freezes the closed CLI permission-mode vocabulary here;
        // unset means the CLI-default posture.
        let requested_permission_mode =
            optional_profile_text(profile, "permission_mode", "Claude")?;
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
        if let Some(subject) = resume_subject.as_ref()
            && subject.native_scope != claude_scope(config_dir.as_deref())
        {
            return Err(Error::new(
                "profile_error",
                "Claude resume subject is outside the authenticated native scope",
            ));
        }
        // Fresh: Eliot mints the session id and pre-assigns it; SessionStart
        // hook lines carry it before any input, so no `start_requires_input`.
        // Resume: the CLI continues the exact retained session id; the old
        // prompt is never resent.
        let session_id = match resume_subject.as_ref() {
            Some(subject) => subject.native_ref.clone(),
            None => new_uuid(),
        };
        let resume_ref = resume_subject.map(|subject| subject.native_ref);
        Ok(StartPlan {
            workspace,
            program,
            environment,
            secrets,
            config_dir,
            credential: initialized.credential.clone(),
            session_id,
            requested_model,
            requested_permission_mode,
            resume_ref,
        })
    }

    /// Settle a start whose native process the host would not register. No
    /// session or input was sent; if the owned CLI's cleanup is unconfirmed
    /// the session is kept unknown instead of dropped.
    async fn fail_unregistered_start(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        error: Error,
    ) -> Error {
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
            return contextual_error(error, "unknown", message);
        }
        let error = session.native_client.diagnostic_error(error);
        self.failed_start = Some(FailedStart {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            command_id: params.command_id.clone(),
            error: error.clone(),
        });
        error
    }

    /// Discard a failed start and retain its error for replays. A start
    /// whose cleanup is unconfirmed is `unknown`: the session is kept
    /// instead of dropped (like `fail_unregistered_start`).
    async fn reject_start(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        error: Error,
    ) -> Error {
        session.events.close().await;
        if session
            .native
            .terminate(true, Instant::now() + RPC_TIMEOUT)
            .await
            .is_err()
        {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            let message = format!(
                "{}; owned CLI cleanup is unconfirmed; session.ready was never committed",
                error.message
            );
            return contextual_error(error, "unknown", message);
        }
        let error = session.native_client.diagnostic_error(error);
        self.failed_start = Some(FailedStart {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            command_id: params.command_id.clone(),
            error: error.clone(),
        });
        error
    }

    /// Run `initialize` and check the effective profile BEFORE readiness: on
    /// any mismatch no `session.ready` is published. A resume realigns the
    /// retained session with the frozen profile after the idle preflight. The
    /// session moves through: on success it comes back with the effective
    /// profile, on failure it is discarded (or kept unknown when its cleanup
    /// is unconfirmed).
    async fn read_effective_profile(
        &mut self,
        session: AgentSession,
        params: &AgentStartParams,
        plan: &StartPlan,
    ) -> Result<(AgentSession, Value)> {
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
            // A lost initialize reply leaves the CLI state uncertain: never
            // recode unknown as rejected.
            Err(error) => {
                return Err(self
                    .reject_start(session, params, setter_error(error, "initialize"))
                    .await);
            }
        };
        let readback: InitializeReadback = match serde_json::from_value(readback) {
            Ok(readback) => readback,
            Err(_) => {
                return Err(self
                    .reject_start(
                        session,
                        params,
                        Error::new(
                            "profile_error",
                            "Claude initialize readback did not match its envelope",
                        ),
                    )
                    .await);
            }
        };
        let model_available = readback.models.iter().any(|model| {
            model.value.as_deref() == Some(plan.requested_model.as_str())
                || model.resolved_model.as_deref() == Some(plan.requested_model.as_str())
        });
        if !model_available || readback.session_state.as_deref() != Some("idle") {
            return Err(self
                .reject_start(
                    session,
                    params,
                    Error::new(
                        "profile_error",
                        "Claude effective model or session state did not match the frozen profile",
                    ),
                )
                .await);
        }
        // Resume realigns the retained session with the frozen profile. The
        // initialize idle readback above is only the preflight: the old
        // generation is terminal by Eliot's own Store gates (admission refuses
        // resume on a live generation and dispatch rechecks it), never by
        // this readback alone.
        let mut effective_mode = readback.current_permission_mode.clone();
        if plan.resume_ref.is_some() {
            match realign_resumed_settings(
                &session.native_client,
                &plan.requested_model,
                plan.requested_permission_mode.as_deref(),
                effective_mode.clone(),
            )
            .await
            {
                Ok(mode) => effective_mode = mode,
                Err(error) => {
                    return Err(self.reject_start(session, params, error).await);
                }
            }
        }
        // The CLI never reads the active model back (note 47): membership in
        // `models[]` above is the only proof, so `effective.model` stays null
        // and the requested id travels under `requested`. Requested vs
        // effective permission mode is reported, never forced: launching with
        // `manual` reads back `default` on the installed CLI.
        Ok((
            session,
            json!({
                "model": null,
                "permission_mode": effective_mode,
                "session_state": readback.session_state,
                "pid": readback.pid,
            }),
        ))
    }

    /// Commit `session.ready` and retain the ready session. If readiness is
    /// not acknowledged the known session is kept unknown, never retried.
    async fn commit_readiness(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        plan: &StartPlan,
        effective: Value,
    ) -> Result<Value> {
        session.session_id = Some(plan.session_id.clone());
        if let Err(error) = session
            .events
            .ready(
                session.native_scope.clone(),
                plan.session_id.clone(),
                effective.clone(),
                session.native_client.clone(),
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
            "agent_id": params.agent_id.clone(),
            "generation": params.generation,
            "command_id": params.command_id.clone(),
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": plan.session_id.clone(),
            "requested": {
                "model": plan.requested_model.clone(),
                "permission_mode": plan.requested_permission_mode.clone(),
                "cwd": plan.workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "Claude agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        if let Some(replay) = self.replay_start(&params) {
            return replay;
        }
        let plan = self.plan_start(&params)?;
        let session = launch_session(&plan, &params, peer).await?;
        if let Err(error) = register_native_process(
            peer,
            &plan.credential,
            &session.agent_id,
            session.generation,
            session.native.pid(),
            "Claude",
            "CLI",
            "session",
            RPC_TIMEOUT,
        )
        .await
        {
            return Err(self.fail_unregistered_start(session, &params, error).await);
        }

        let (session, effective) = self.read_effective_profile(session, &params, &plan).await?;
        self.commit_readiness(session, &params, &plan, effective)
            .await
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "Claude agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" => self.reply_request(params).await,
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
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Claude session or event source outcome is unresolved",
            ));
        }
        let text = send_text(&params)?;
        if let Some(replay) = replay_send(session, &params.command_id, text) {
            return replay;
        }
        check_send_ready(session)?;
        session
            .command_inputs
            .insert(params.command_id.clone(), text.to_owned());
        dispatch_send(session, &params.command_id, text).await
    }

    async fn interrupt_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Claude Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
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

    async fn reply_request(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Claude Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Claude session or event source outcome is unresolved",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "stale_turn",
                    "Claude request reply needs its captured turn id",
                )
            })?;
        let request_id = params
            .request_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    "Claude request reply needs its captured request id",
                )
            })?;
        if let Some(previous) = session.reply_outcomes.get(&params.command_id) {
            if previous.verb != params.verb
                || previous.request_id != request_id
                || previous.turn_id != turn_id
            {
                return Err(Error::new(
                    "source_integrity",
                    "Claude command id was reused for another request reply",
                ));
            }
            return Ok(previous.result.clone());
        }
        // The event task owns no transport client: the native decision
        // travels on this call, serialized behind queued frames.
        let native = session.native_client.clone();
        let result = match session
            .events
            .reply_request(
                params.command_id.clone(),
                request_id.to_owned(),
                turn_id.to_owned(),
                params.verb.clone(),
                params.args.clone(),
                native,
            )
            .await
        {
            Ok(result) => result,
            Err(_) => unknown_control("Claude request reply outcome is unknown"),
        };
        session.reply_outcomes.insert(
            params.command_id.clone(),
            ReplyOutcome {
                verb: params.verb,
                request_id: request_id.to_owned(),
                turn_id: turn_id.to_owned(),
                result: result.clone(),
            },
        );
        Ok(result)
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentSnapshotParams = decode(raw, "Claude agent.snapshot params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Claude Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
        // PR1 has no live native session poll: the snapshot is the committed
        // stream position (session/turn bindings plus the latest terminal
        // facts), never a fresh native query.
        if let Some(stopped) = stopped_stop_snapshot(session, &params) {
            return Ok(stopped);
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
        live_session_snapshot(session, &params, &event_status, turn_id)
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentStopParams = decode(raw, "Claude agent.stop params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Claude Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
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

/// The stopped view for the exact stop command: the terminal bindings
/// plus the latest committed facts, with no active turn.
fn stopped_stop_snapshot(session: &AgentSession, params: &AgentSnapshotParams) -> Option<Value> {
    if session.stop_command_id.as_deref() != Some(params.command_id.as_str())
        || session.phase != SessionPhase::Stopped
        || params.turn_id.is_some()
    {
        return None;
    }
    let event_status = session.events.status();
    let native_ref = session
        .session_id
        .clone()
        .or_else(|| event_status.session_id.clone());
    Some(json!({
        "agent_id": params.agent_id.clone(),
        "generation": params.generation,
        "command_id": params.command_id.clone(),
        "state": "stopped",
        "native_scope": session.native_scope.clone(),
        "native_ref": native_ref,
        "active_turn": null,
        "last_turn": event_status.last_turn,
        "batch_unknown": event_status.batch_unknown,
        "source_unknown": event_status.source_unknown,
    }))
}

/// The live-session view: the command binding must be retained, the native
/// process must still run, and the requested turn must be the exact active
/// or last turn.
fn live_session_snapshot(
    session: &AgentSession,
    params: &AgentSnapshotParams,
    event_status: &EventStatus,
    turn_id: Option<String>,
) -> Result<Value> {
    if !known_command(session, &params.command_id) {
        return unknown_snapshot(params, "Claude command identity is not retained");
    }
    if session.phase == SessionPhase::Stopped {
        return unknown_snapshot(
            params,
            "Claude native process is stopped; native readback is unavailable",
        );
    }
    let Some(session_id) = session
        .session_id
        .as_deref()
        .or(event_status.session_id.as_deref())
    else {
        return unknown_snapshot(params, "Claude session identity was not acknowledged");
    };
    if session.command_inputs.contains_key(&params.command_id) && turn_id.is_none() {
        return unknown_snapshot(
            params,
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
                    params,
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
        "agent_id": params.agent_id.clone(),
        "generation": params.generation,
        "command_id": params.command_id.clone(),
        "state": state,
        "native_scope": session.native_scope.clone(),
        "native_ref": session_id,
        "active_turn": event_status.active_turn_id.clone(),
        "last_turn": last_turn,
        "batch_unknown": event_status.batch_unknown,
        "source_unknown": event_status.source_unknown,
        "result_known": result_known,
    }))
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

/// The send text plus its bound. Runs before any registration, so an
/// oversized input never wedges the session.
fn send_text(params: &AgentCommandParams) -> Result<&str> {
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
    Ok(text)
}

/// Replay a known send, or refuse a reused command id with different input.
/// `None` means this is a fresh send that proceeds to dispatch.
fn replay_send(session: &AgentSession, command_id: &str, text: &str) -> Option<Result<Value>> {
    let previous = session.command_inputs.get(command_id)?;
    if previous != text {
        return Some(Err(Error::new(
            "source_integrity",
            "Claude command id was reused with different input",
        )));
    }
    if let Some(turn_id) = session.command_turns.get(command_id) {
        return Some(Ok(json!({"state": "applied", "turn_id": turn_id})));
    }
    if session.unknown_commands.contains(command_id) {
        return Some(Err(Error::new(
            "unknown",
            "Claude input outcome is unresolved and will not be repeated",
        )));
    }
    if session.rejected_commands.contains(command_id) {
        return Some(Ok(json!({
            "state": "rejected",
            "reason": "Claude input was known not to apply",
        })));
    }
    Some(Err(Error::new(
        "unknown",
        "Claude command was already dispatched without a retained exact outcome",
    )))
}

/// Pre-dispatch gates for a fresh send: a failed event source, an already
/// active turn, and the per-generation command history bound.
fn check_send_ready(session: &AgentSession) -> Result<()> {
    let event_status = session.events.status();
    if let Some(reason) = event_status.failure {
        return Err(Error::new("unknown", reason));
    }
    if event_status.active_turn_id.is_some() {
        return Err(Error::new("busy", "Claude already has an active turn"));
    }
    if session.command_inputs.len() >= MAX_COMMAND_HISTORY {
        return Err(Error::new(
            "unsupported",
            "Claude L6.1 per-generation command history bound has been reached",
        ));
    }
    Ok(())
}

/// Mint the exact turn association before the write and register it with
/// the event task first: an early lifecycle can never miss its waiter, and
/// a lost association latches unknown without a rewrite.
async fn dispatch_send(session: &mut AgentSession, command_id: &str, text: &str) -> Result<Value> {
    let turn_id = new_uuid();
    let outcome = match session
        .events
        .expect_turn(command_id.to_owned(), turn_id.clone())
        .await
    {
        Ok(outcome) => outcome,
        Err(error) if error.code == "busy" => {
            session.command_inputs.remove(command_id);
            return Err(Error::new("busy", "Claude already has an active turn"));
        }
        Err(error) => {
            session.unknown_commands.insert(command_id.to_owned());
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
            session.events.cancel_turn(command_id.to_owned()).await;
            session.rejected_commands.insert(command_id.to_owned());
            return Ok(json!({
                "state": "rejected",
                "reason": "Claude input was not dispatched",
            }));
        }
        session.unknown_commands.insert(command_id.to_owned());
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
            session
                .command_turns
                .insert(command_id.to_owned(), turn_id.clone());
            Ok(json!({"state": "applied", "turn_id": turn_id}))
        }
        TurnOutcome::Rejected(reason) => {
            session.rejected_commands.insert(command_id.to_owned());
            Ok(json!({"state": "rejected", "reason": reason}))
        }
        TurnOutcome::Unknown => {
            session.unknown_commands.insert(command_id.to_owned());
            session.phase = SessionPhase::Unknown;
            Err(Error::new(
                "unknown",
                "Claude input outcome is unresolved and will not be repeated",
            ))
        }
    }
}

/// Spawn the owned CLI and bind the event task: the only start phase that
/// creates native resources.
async fn launch_session(
    plan: &StartPlan,
    params: &AgentStartParams,
    peer: &PeerClient,
) -> Result<AgentSession> {
    // Approvals always travel over stdio so every `can_use_tool` ask
    // reaches Eliot. Resume continues the exact retained session id; the
    // old prompt is never resent.
    let mut argv = vec![
        OsString::from("-p"),
        OsString::from("--input-format"),
        OsString::from("stream-json"),
        OsString::from("--output-format"),
        OsString::from("stream-json"),
        OsString::from("--verbose"),
        OsString::from("--permission-prompt-tool"),
        OsString::from("stdio"),
    ];
    match plan.resume_ref.as_ref() {
        Some(native_ref) => {
            argv.push(OsString::from("--resume"));
            argv.push(OsString::from(native_ref));
        }
        None => {
            argv.push(OsString::from("--session-id"));
            argv.push(OsString::from(&plan.session_id));
        }
    }
    argv.push(OsString::from("--model"));
    argv.push(OsString::from(&plan.requested_model));
    if let Some(mode) = plan.requested_permission_mode.as_deref() {
        argv.push(OsString::from("--permission-mode"));
        argv.push(OsString::from(mode));
    }
    let launch = NativeLaunch {
        program: plan.program.clone(),
        args: argv,
        cwd: plan.workspace.clone(),
        environment: plan.environment.clone(),
    };
    let mut native =
        ClaudeProcess::spawn(launch, SecretPolicy::new(plan.secrets.iter().cloned())).await?;
    let frames = native.take_frames()?;
    let stream = native.stream_state();
    let native_client = native.client();
    let events = events::spawn(
        frames,
        stream,
        peer.clone(),
        EventConfig {
            credential: plan.credential.clone(),
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            secrets: plan.secrets.clone(),
        },
    );
    Ok(AgentSession {
        agent_id: params.agent_id.clone(),
        generation: params.generation,
        start_command_id: params.command_id.clone(),
        start_result: None,
        phase: SessionPhase::Unknown,
        session_id: None,
        native_scope: claude_scope(plan.config_dir.as_deref()),
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
        reply_outcomes: HashMap::new(),
        stop_command_id: None,
    })
}

/// Realign a resumed session with the frozen profile before readiness. The
/// model setter has no readback payload, so its success is the application
/// proof; the permission setter echoes its mode, which must match. A lost or
/// timed-out setter reply is `unknown`, never a silent keep, and so is an
/// unexpected echo: the CLI applied its own mode instead.
async fn realign_resumed_settings(
    client: &ClaudeClient,
    requested_model: &str,
    requested_mode: Option<&str>,
    current_mode: Option<String>,
) -> Result<Option<String>> {
    client
        .control(
            "set_model",
            json!({"model": requested_model}),
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        .map_err(|error| setter_error(error, "set_model"))?;
    let mut effective_mode = current_mode;
    if let Some(requested) = requested_mode
        && effective_mode.as_deref() != Some(requested)
    {
        let echo = client
            .control(
                "set_permission_mode",
                json!({"mode": requested}),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
            .map_err(|error| setter_error(error, "set_permission_mode"))?;
        if echo.get("mode").and_then(Value::as_str) != Some(requested) {
            return Err(Error::new(
                "unknown",
                "Claude set_permission_mode echoed its own mode instead of the requested one",
            ));
        }
        effective_mode = Some(requested.to_owned());
    }
    Ok(effective_mode)
}

fn setter_error(error: Error, subtype: &str) -> Error {
    if native_no_write(&error.code) {
        contextual_error(
            error,
            "profile_error",
            format!("Claude {subtype} was not dispatched"),
        )
    } else {
        contextual_error(
            error,
            "unknown",
            format!("Claude {subtype} may have changed native state; it will not be repeated"),
        )
    }
}

fn claude_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("request".to_owned(), "stream".to_owned()),
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
            "resume".to_owned(),
            "send".to_owned(),
            "interrupt".to_owned(),
            "approve".to_owned(),
            "deny".to_owned(),
            "stop".to_owned(),
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

/// The shared start/input/stop commands plus the Claude-only interrupt and
/// request-reply commands.
fn known_command(session: &AgentSession, command_id: &str) -> bool {
    session::known_command(
        &session.start_command_id,
        &session.command_inputs,
        &session.stop_command_id,
        command_id,
    ) || session.interrupt_applied.contains_key(command_id)
        || session.reply_outcomes.contains_key(command_id)
}

/// The shared environment mapping plus the Claude-only config-dir identity:
/// the first `CLAUDE_CONFIG_DIR` value names the native scope.
fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
) -> Result<TargetEnvironment> {
    let mapped = session::target_environment(environment, pass_env, "Claude")?;
    let config_dir = environment.and_then(Value::as_object).and_then(|object| {
        object.iter().find_map(|(name, value)| {
            if name.eq_ignore_ascii_case("CLAUDE_CONFIG_DIR") {
                value.as_str().map(str::to_owned)
            } else {
                None
            }
        })
    });
    Ok(TargetEnvironment {
        variables: mapped.variables,
        secrets: mapped.secrets,
        config_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::{claude_manifest, claude_scope, new_uuid};
    use eliot_plugin::session::{parse_resume_subject, validate_source_plan};
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, HashSet};

    const FAMILIES: &[&str] = &["session", "turn", "message", "request"];

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
    fn manifest_advertises_the_pr2_slice() {
        let manifest = claude_manifest();
        assert_eq!(manifest.name, "claude");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(
            manifest.verbs,
            vec![
                "launch".to_owned(),
                "resume".to_owned(),
                "send".to_owned(),
                "interrupt".to_owned(),
                "approve".to_owned(),
                "deny".to_owned(),
                "stop".to_owned(),
            ]
        );
        assert!(!manifest.features.start_requires_input);
        assert_eq!(
            manifest.sources["controlled"],
            BTreeMap::from([
                ("message".to_owned(), "stream".to_owned()),
                ("request".to_owned(), "stream".to_owned()),
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
        assert!(validate_source_plan("Claude", FAMILIES, None).is_err());
        assert!(
            validate_source_plan(
                "Claude",
                FAMILIES,
                Some(&json!({
                    "session": "stream",
                    "turn": "stream",
                    "message": "stream",
                    "request": "stream",
                }))
            )
            .is_ok()
        );
        assert!(
            validate_source_plan(
                "Claude",
                FAMILIES,
                Some(&json!({
                    "session": "stream",
                    "turn": "stream",
                    "message": "stream",
                }))
            )
            .is_err()
        );
    }

    fn resume_source_state(agent_id: &str) -> serde_json::Value {
        json!({
            "retained_generation": 1,
            "retained_source": {
                "agent_id": agent_id,
                "generation": 1,
                "source": "stream",
                "families": ["session", "turn", "message"],
            },
            "current_turn": null,
        })
    }

    #[test]
    fn resume_subject_requires_exact_scope_ref_and_continuity() {
        let agent_id = "agent-1";
        let state = resume_source_state(agent_id);
        let subject = parse_resume_subject(
            &json!({"native_scope": "scope-1", "native_ref": "ref-1"}),
            Some(&state),
            agent_id,
            2,
            "Claude",
        )
        .expect("valid resume subject");
        assert_eq!(subject.native_scope, "scope-1");
        assert_eq!(subject.native_ref, "ref-1");
        assert_eq!(subject.retained_turn, None);

        // Shape violations are invalid; continuity violations are stale or
        // unknown, never silently adopted.
        assert!(
            parse_resume_subject(
                &json!({"native_scope": "scope-1"}),
                Some(&state),
                agent_id,
                2,
                "Claude",
            )
            .is_err()
        );
        assert!(
            parse_resume_subject(
                &json!({"native_scope": "scope-1", "native_ref": "  "}),
                Some(&state),
                agent_id,
                2,
                "Claude",
            )
            .is_err()
        );
        assert_eq!(
            parse_resume_subject(
                &json!({"native_scope": "scope-1", "native_ref": "ref-1"}),
                Some(&state),
                agent_id,
                3,
                "Claude",
            )
            .unwrap_err()
            .code,
            "stale_generation"
        );
        assert_eq!(
            parse_resume_subject(
                &json!({"native_scope": "scope-1", "native_ref": "ref-1"}),
                None,
                agent_id,
                2,
                "Claude",
            )
            .unwrap_err()
            .code,
            "unknown"
        );
        assert_eq!(
            parse_resume_subject(
                &json!({"native_scope": "scope-1", "native_ref": "ref-1"}),
                Some(&json!({
                    "retained_generation": 1,
                    "retained_source": {
                        "agent_id": "another-agent",
                        "generation": 1,
                        "source": "stream",
                        "families": ["session", "turn", "message"],
                    },
                    "current_turn": null,
                })),
                agent_id,
                2,
                "Claude",
            )
            .unwrap_err()
            .code,
            "stale_generation"
        );
    }

    /// The shared validator (not the old Claude-only presence check) proves
    /// the retained turn summary: a complete summary binds its exact turn id,
    /// an incomplete one is `unknown`.
    #[test]
    fn resume_subject_proves_the_retained_turn_summary() {
        let agent_id = "agent-1";
        let validate = |current_turn: Value| {
            parse_resume_subject(
                &json!({"native_scope": "scope-1", "native_ref": "ref-1"}),
                Some(&json!({
                    "retained_generation": 1,
                    "retained_source": {
                        "agent_id": agent_id,
                        "generation": 1,
                        "source": "stream",
                        "families": ["session", "turn", "message"],
                    },
                    "current_turn": current_turn,
                })),
                agent_id,
                2,
                "Claude",
            )
        };
        let subject = validate(json!({
            "turn_id": "turn-1",
            "state": "completed",
            "status": "completed",
            "started_at": 1,
            "completed_at": 2,
        }))
        .expect("complete turn summary");
        assert_eq!(subject.retained_turn.as_deref(), Some("turn-1"));
        // A summary missing its shape (here: no turn id, bad state) is
        // unknown, never silently adopted.
        for current_turn in [
            json!({
                "state": "completed",
                "status": null,
                "started_at": null,
                "completed_at": 2,
            }),
            json!({
                "turn_id": "turn-1",
                "state": "running wild",
                "status": null,
                "started_at": null,
                "completed_at": null,
            }),
            json!("turn-1"),
        ] {
            assert_eq!(validate(current_turn).unwrap_err().code, "unknown");
        }
    }
}
