use crate::{
    events::{EventConfig, EventHandle, EventStatus, TurnFacts},
    native::{
        AuthSetup, HttpOutcome, IntegrationSelector, OpenCodeHttp, OwnedServer, ServerLaunch,
        types::{InboxUser, ModelRef, SessionInfo},
    },
};
use eliot_core::{ContextItem, Error, InjectPoint, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    session::{
        ResumeSubject, TargetEnvironment, contextual_error, decode, known_command, native_no_write,
        not_initialized, optional_profile_text, parse_resume_subject, paths_equal,
        register_native_process, split_provider_model, target_environment, unknown_snapshot,
        validate_binding, validate_identity, validate_source_plan, with_claimed_context,
        with_context_points,
    },
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{sync::mpsc, time::Instant};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

const RESUME_DRAIN_ROUNDS: usize = 20;
const RESUME_DRAIN_BEAT: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Default)]
struct OpenCodePlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
}

struct Initialized {
    credential: String,
    instance_dir: String,
    store_root: String,
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

/// Static stop-stage class for one unknown error message. Safe by
/// construction: a fixed literal, never a payload, native error, or secret.
#[derive(Clone, Copy)]
enum StopStage {
    Begin,
    Quiesce,
    Retention,
    Process,
    EndAck,
}

impl StopStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Quiesce => "quiesce",
            Self::Retention => "retention",
            Self::Process => "process",
            Self::EndAck => "end_ack",
        }
    }
}

fn note_stage(stage: &mut StopStage, next: StopStage) -> StopStage {
    // This is a function call, so the compiler treats the stage value
    // as read here; sequential semantic stages advance the same
    // variable without a dead-assignment lint. The observable value at
    // the final stage is the furthest semantic step reached.
    *stage = next;
    *stage
}

fn native_io_blocked(session: &AgentSession) -> bool {
    session.resources_proved_closed || session.stop_end_ack_proved
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
    resources_proved_closed: bool,
    stop_end_ack_proved: bool,
    control_bindings: HashMap<String, ControlBinding>,
    control_results: HashMap<String, Value>,
}

/// One retained control (interrupt/reply) command: its exact binding for
/// replay integrity. Applied and rejected results replay byte-identical;
/// unknown replays unknown without rewriting; a rebinding is a
/// source_integrity error, never a second write.
struct ControlBinding {
    verb: String,
    turn_id: Option<String>,
    request_id: Option<String>,
    args: Value,
}

/// A start request that passed admission: decoded params plus the validated
/// frozen profile, workspace, child environment, selected native auth
/// integration, and redaction secrets.
struct ParsedStart {
    params: AgentStartParams,
    requested_model: String,
    model_ref: ModelRef,
    requested_agent: Option<String>,
    workspace: PathBuf,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    auth_selector: IntegrationSelector,
    secrets: Vec<String>,
    credential: String,
    instance_dir: String,
    store_root: String,
    resume: Option<ResumeSubject>,
}

/// Outcome of start admission: a replayed start response, or a parsed
/// request ready to launch.
enum StartAdmission {
    Replay(Result<Value>),
    New(Box<ParsedStart>),
}

/// Outcome of send admission: a replayed response, or a new send ready for
/// its native write.
enum SendAdmission {
    Replay(Value),
    New { command_id: String, text: String },
}

/// Outcome of control (interrupt/reply) admission: a replayed response, or
/// a new control ready for its native write.
enum ControlAdmission {
    Replay(Value),
    New {
        command_id: String,
        turn_id: Option<String>,
        request_id: Option<String>,
        args: Value,
    },
}

/// Outcome of the native prompt write: a known rejection, or an admitted
/// inbox entry awaiting its execution association.
enum PromptAdmission {
    Rejected(Value),
    Admitted(InboxUser),
}

/// A snapshot request that passed its guards: the resolved turn (if any)
/// and the proven session id.
struct SnapshotRequest {
    turn_id: Option<String>,
    session_id: String,
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
        let store_root = params.store_root.clone().unwrap_or_default();
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > eliot_plugin::protocol::MAX_CREDENTIAL_BYTES
            || params.store_root.is_none()
            || !Path::new(&store_root).is_absolute()
            || store_root.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "OpenCode plugin initialize requires protocol, private instance directory, absolute store root, and credential",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.credential != params.credential
                || existing.instance_dir != params.instance_dir
                || existing.store_root != store_root
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
                store_root,
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
        let parsed = match self.parse_start_request(raw)? {
            StartAdmission::Replay(outcome) => return outcome,
            StartAdmission::New(parsed) => parsed,
        };
        let session = match Self::launch_start_session(peer, &parsed).await {
            Ok(session) => session,
            Err(error) => return Err(self.retain_failed_start(&parsed.params, error)),
        };
        let session = self.register_start_session(peer, &parsed, session).await?;
        if parsed.resume.is_some() {
            return self.resume_start_session(*parsed, session).await;
        }
        let (session, created, effective) = self.create_start_session(&parsed, session).await?;
        self.ready_start_session(*parsed, session, created, effective)
            .await
    }

    /// Start phase 1, admission: decode, replay guards, and frozen-profile
    /// validation. Failures here retain nothing: a corrected start may retry.
    fn parse_start_request(&self, raw: &Value) -> Result<StartAdmission> {
        let params: AgentStartParams = decode(raw, "OpenCode agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "OpenCode",
        )?;
        if let Some(replay) = self.start_replay_admission(&params)? {
            return Ok(replay);
        }
        let (initialized, profile) = self.start_prereqs(&params)?;
        // The retained subject validates through the shared Muse/Claude
        // helper: same continuity rules and codes, no local copy.
        let resume = params
            .resume_from
            .as_ref()
            .map(|resume| {
                parse_resume_subject(
                    resume,
                    params.source_state.as_ref(),
                    &params.agent_id,
                    params.generation,
                    "OpenCode",
                )
            })
            .transpose()?;
        let (requested_model, model_ref) = start_model_ref(profile)?;
        let requested_agent = optional_profile_text(profile, "agent", "OpenCode")?;
        let auth_selector = start_auth_selector(profile)?;
        let (workspace, program) = start_paths(profile, &params)?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
        } = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            "OpenCode",
        )?;
        secrets.push(initialized.credential.clone());
        Ok(StartAdmission::New(Box::new(ParsedStart {
            params,
            requested_model,
            model_ref,
            requested_agent,
            workspace,
            program,
            environment,
            auth_selector,
            secrets,
            credential: initialized.credential.clone(),
            instance_dir: initialized.instance_dir.clone(),
            store_root: initialized.store_root.clone(),
            resume,
        })))
    }

    /// Start admission replays: the bound generation answers its own start
    /// command without new native I/O; anything else is refused.
    fn start_replay_admission(&self, params: &AgentStartParams) -> Result<Option<StartAdmission>> {
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return Ok(Some(StartAdmission::Replay(match session.phase {
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
                })));
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
        Ok(None)
    }

    /// Start prerequisites: the plugin is initialized, the start carries no
    /// input, the source plan is the installed stream plan, and the frozen
    /// profile names this harness.
    fn start_prereqs<'a, 'b>(
        &'a self,
        params: &'b AgentStartParams,
    ) -> Result<(&'a Initialized, &'b serde_json::Map<String, Value>)> {
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("OpenCode"))?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "OpenCode start does not accept input; send it as a separate command",
            ));
        }
        validate_source_plan(
            "OpenCode",
            &["session", "turn", "message", "request"],
            params.source_plan.as_ref(),
        )?;
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
        Ok((initialized, profile))
    }

    /// Start phase 2, launch: spawn the owned server, subscribe the event
    /// stream, and assemble the session. The caller retains any error.
    async fn launch_start_session(peer: &PeerClient, parsed: &ParsedStart) -> Result<AgentSession> {
        let mut server = match OwnedServer::spawn(
            ServerLaunch {
                program: parsed.program.clone(),
                workspace: parsed.workspace.clone(),
                instance_dir: PathBuf::from(&parsed.instance_dir),
                store_root: PathBuf::from(&parsed.store_root),
                agent_id: parsed.params.agent_id.clone(),
                environment: parsed.environment.clone(),
                model: parsed.requested_model.clone(),
                auth: AuthSetup {
                    selector: parsed.auth_selector.clone(),
                    reuse_existing: parsed.resume.is_some(),
                },
            },
            &parsed.secrets,
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        {
            Ok(server) => server,
            // The spawn cleans up its own child on failure; no diagnostics
            // exist yet because no server was retained.
            Err(error) => return Err(error),
        };
        let http = server.http().clone();
        // Subscribe before session create/input: the stream must observe the
        // session's whole durable sequence starting at `session.created`.
        let (frames, stream) = match http.subscribe(Instant::now() + RPC_TIMEOUT).await {
            Ok(subscribed) => subscribed,
            Err(error) => {
                let _ = server.terminate(true, Instant::now() + RPC_TIMEOUT).await;
                return Err(server.diagnostic_error(error));
            }
        };
        let events = crate::events::spawn(
            frames,
            stream,
            http.clone(),
            peer.clone(),
            EventConfig {
                credential: parsed.credential.clone(),
                agent_id: parsed.params.agent_id.clone(),
                generation: parsed.params.generation,
                secrets: parsed.secrets.clone(),
            },
        );
        let session = AgentSession {
            agent_id: parsed.params.agent_id.clone(),
            generation: parsed.params.generation,
            start_command_id: parsed.params.command_id.clone(),
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
            resources_proved_closed: false,
            stop_end_ack_proved: false,
            control_bindings: HashMap::new(),
            control_results: HashMap::new(),
        };

        Ok(session)
    }

    /// Start phase 3, registration: prove the owned server runs under this
    /// Agent generation. An unconfirmed cleanup stashes the session as
    /// unknown; a clean one retains the failure for replay.
    async fn register_start_session(
        &mut self,
        peer: &PeerClient,
        parsed: &ParsedStart,
        mut session: AgentSession,
    ) -> Result<AgentSession> {
        if let Err(error) = register_native_process(
            peer,
            &parsed.credential,
            &session.agent_id,
            session.generation,
            session.server.pid(),
            "OpenCode",
            "server",
            "session",
            RPC_TIMEOUT,
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
            return Err(self.retain_failed_start(&parsed.params, error));
        }
        Ok(session)
    }

    /// Start phase 4, create: open the native session and verify its
    /// effective model and directory against the frozen profile. A readback
    /// mismatch best-effort deletes the created session and surfaces the
    /// kill result; no session is kept.
    async fn create_start_session(
        &mut self,
        parsed: &ParsedStart,
        mut session: AgentSession,
    ) -> Result<(AgentSession, SessionInfo, Value)> {
        session.native_scope = opencode_scope(session.server.data_dir());
        let title = format!(
            "eliot-{}-g{}",
            parsed.params.agent_id, parsed.params.generation
        );
        let directory = parsed.workspace.to_string_lossy().into_owned();
        let created = match session
            .http
            .create_session(
                &title,
                &parsed.model_ref,
                &directory,
                Instant::now() + RPC_TIMEOUT,
            )
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
                return Err(self.retain_failed_start(&parsed.params, error));
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
                return Err(self.retain_failed_start(&parsed.params, error));
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
        let model_verified = effective_model.as_deref() == Some(parsed.requested_model.as_str());
        let location_ok = created
            .location
            .as_ref()
            .and_then(|location| {
                if location.directory.trim().is_empty() {
                    None
                } else {
                    Some(paths_equal(
                        Path::new(&location.directory),
                        &parsed.workspace,
                    ))
                }
            })
            .unwrap_or(false);
        let effective = json!({
            "model": effective_model.clone(),
            "model_verified": model_verified,
            "cwd": created.location.as_ref().map(|location| location.directory.clone()),
        });
        let effective = with_context_points(effective, &[InjectPoint::NextInput]);
        if !model_verified || !location_ok {
            let kill = session
                .http
                .delete_session(&created.id, Instant::now() + RPC_TIMEOUT)
                .await;
            let kill_note = match kill {
                Ok(true) => "the created native session was removed".to_owned(),
                Ok(false) => "the created native session was already gone".to_owned(),
                Err(HttpOutcome::Rejected { reason, .. }) => {
                    format!("the created native session kill was refused ({reason})")
                }
                Err(HttpOutcome::Unknown { reason })
                | Err(HttpOutcome::NotDispatched { reason }) => {
                    format!("the created native session kill is unconfirmed ({reason})")
                }
            };
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    format!(
                        "OpenCode effective model or session directory did not match the frozen profile; {kill_note}"
                    ),
                ),
            )
            .await;
            return Err(self.retain_failed_start(&parsed.params, error));
        }
        Ok((session, created, effective))
    }

    /// Resume: reattach to the exact retained native session in this new
    /// generation. No prompt is ever replayed: the new server serves the
    /// same per-Agent store, the preflight proves the retained session
    /// exists and is idle, the model/agent realign to the frozen profile
    /// with an effective readback, and readiness adopts the live edge.
    async fn resume_start_session(
        &mut self,
        parsed: ParsedStart,
        mut session: AgentSession,
    ) -> Result<Value> {
        let resume = parsed.resume.as_ref().expect("resume subject admitted");
        session.native_scope = opencode_scope(session.server.data_dir());
        if session.native_scope != resume.native_scope {
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    "OpenCode resume subject is outside the authenticated native scope",
                ),
            )
            .await;
            return Err(self.retain_failed_start(&parsed.params, error));
        }
        let deadline = Instant::now() + RPC_TIMEOUT;
        let retained =
            match Self::resume_read_retained(&session, &resume.native_ref, deadline).await {
                Ok(retained) => retained,
                Err(error) => return Err(self.fail_resume(&parsed, session, error).await),
            };
        session.session_id = Some(retained.id.clone());
        if let Err(error) = Self::resume_quiesce(&session, &retained.id, deadline).await {
            return Err(self.stash_resume_unknown(session, error.message).await);
        }
        if let Err(error) = Self::resume_verify_idle(&session, &retained.id, deadline).await {
            return Err(self.stash_resume_unknown(session, error.message).await);
        }
        let effective =
            match Self::resume_configure(&parsed, &session, &retained.id, deadline).await {
                Ok(effective) => effective,
                Err(error) if error.code == "unknown" => {
                    return Err(self.stash_resume_unknown(session, error.message).await);
                }
                Err(error) => return Err(self.fail_resume(&parsed, session, error).await),
            };
        let effective = with_context_points(effective, &[InjectPoint::NextInput]);
        if let Err(error) = session
            .events
            .resume_ready(
                session.native_scope.clone(),
                retained.id.clone(),
                effective.clone(),
            )
            .await
        {
            return Err(self.stash_resume_unknown(session, error.message).await);
        }
        session.phase = SessionPhase::Ready;
        let result = json!({
            "agent_id": parsed.params.agent_id,
            "generation": parsed.params.generation,
            "command_id": parsed.params.command_id,
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": retained.id,
            "requested": {
                "model": parsed.requested_model,
                "cwd": parsed.workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    /// Resume read: the retained session must still exist under its exact
    /// id. Proven absence is `not_found` (the retained session no longer exists
    /// in the store — there is nothing to resume); anything ambiguous is `unknown`.
    async fn resume_read_retained(
        session: &AgentSession,
        native_ref: &str,
        deadline: Instant,
    ) -> Result<SessionInfo> {
        let absent = session
            .http
            .session_absent(native_ref, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode retained session read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        if absent {
            return Err(Error::new(
                "not_found",
                "OpenCode retained session no longer exists",
            ));
        }
        let retained = session
            .http
            .get_session(native_ref, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode retained session read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        if retained.id != native_ref {
            return Err(Error::new(
                "source_integrity",
                "OpenCode retained session answered with another identity",
            ));
        }
        Ok(retained)
    }

    /// Resume quiesce: one `session.interrupt` must report no running
    /// execution. A running retained execution is aborted (refusing would
    /// wedge crash-recovery resume) and its settle is awaited on a bounded
    /// leg poll; anything else refuses the resume as unknown. The native
    /// idle proof is this interrupt plus the empty inbox/permission/form
    /// lists, not served history.
    async fn resume_quiesce(
        session: &AgentSession,
        session_id: &str,
        deadline: Instant,
    ) -> Result<()> {
        let interrupted = match session.http.interrupt(session_id, deadline).await {
            Ok(interrupted) => interrupted,
            Err(outcome) => {
                let reason = match outcome {
                    HttpOutcome::Rejected { reason, .. } => reason,
                    HttpOutcome::Unknown { reason } | HttpOutcome::NotDispatched { reason } => {
                        reason
                    }
                };
                return Err(Error::new(
                    "unknown",
                    format!("OpenCode resume quiesce is unresolved ({reason})"),
                ));
            }
        };
        if !interrupted {
            return Ok(());
        }
        for _ in 0..RESUME_DRAIN_ROUNDS {
            tokio::time::sleep(RESUME_DRAIN_BEAT).await;
            if !Self::resume_legs_quiet(session, session_id, deadline).await? {
                continue;
            }
            match session.http.interrupt(session_id, deadline).await {
                Ok(false) => return Ok(()),
                Ok(true) => continue,
                Err(outcome) => {
                    let reason = match outcome {
                        HttpOutcome::Rejected { reason, .. } => reason,
                        HttpOutcome::Unknown { reason } | HttpOutcome::NotDispatched { reason } => {
                            reason
                        }
                    };
                    return Err(Error::new(
                        "unknown",
                        format!("OpenCode resume quiesce is unresolved ({reason})"),
                    ));
                }
            }
        }
        Err(Error::new(
            "unknown",
            "OpenCode retained session did not settle after its interrupt",
        ))
    }

    /// Resume idle legs: the inbox, permission and form lists must all be
    /// empty. Anything pending means a retained execution that this
    /// generation must not adopt.
    async fn resume_verify_idle(
        session: &AgentSession,
        session_id: &str,
        deadline: Instant,
    ) -> Result<()> {
        let (inbox, permissions, forms) =
            Self::resume_leg_counts(session, session_id, deadline).await?;
        if inbox > 0 {
            return Err(Error::new(
                "unknown",
                "OpenCode retained session still has queued input",
            ));
        }
        if permissions > 0 {
            return Err(Error::new(
                "unknown",
                "OpenCode retained session still has pending permission requests",
            ));
        }
        if forms > 0 {
            return Err(Error::new(
                "unknown",
                "OpenCode retained session still has pending forms",
            ));
        }
        Ok(())
    }

    /// Await-helper for the quiesce drain: all three legs quiet.
    async fn resume_legs_quiet(
        session: &AgentSession,
        session_id: &str,
        deadline: Instant,
    ) -> Result<bool> {
        let (inbox, permissions, forms) =
            Self::resume_leg_counts(session, session_id, deadline).await?;
        Ok(inbox == 0 && permissions == 0 && forms == 0)
    }

    /// Read the three idle legs as pending counts. Any read failure is
    /// unknown: the preflight cannot prove the retained session idle.
    async fn resume_leg_counts(
        session: &AgentSession,
        session_id: &str,
        deadline: Instant,
    ) -> Result<(usize, usize, usize)> {
        let inbox = session
            .http
            .list_inbox(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume inbox read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        let permissions = session
            .http
            .list_permissions(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume permission read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        let forms = session
            .http
            .list_forms(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume form read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        Ok((inbox.len(), permissions.len(), forms.len()))
    }

    /// Resume configure: realign the retained session's model and agent
    /// to the frozen profile, then prove the effective values with a fresh
    /// `session.get`. The location must be the workspace; the requested
    /// agent must be installed (the switch validates nothing itself).
    async fn resume_configure(
        parsed: &ParsedStart,
        session: &AgentSession,
        session_id: &str,
        deadline: Instant,
    ) -> Result<Value> {
        let current = session
            .http
            .get_session(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume configure read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        let location_ok = current
            .location
            .as_ref()
            .and_then(|location| {
                if location.directory.trim().is_empty() {
                    None
                } else {
                    Some(paths_equal(
                        Path::new(&location.directory),
                        &parsed.workspace,
                    ))
                }
            })
            .unwrap_or(false);
        if !location_ok {
            return Err(Error::new(
                "profile_error",
                "OpenCode retained session directory is not the frozen workspace",
            ));
        }
        let model_needed = current
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()))
            != Some(parsed.requested_model.clone());
        let agent_needed = parsed
            .requested_agent
            .as_ref()
            .is_some_and(|agent| current.agent.as_ref() != Some(agent));
        if agent_needed {
            let wanted = parsed.requested_agent.clone().expect("agent needed");
            let installed = session.http.list_agents(deadline).await.map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume agent read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
            if !installed.iter().any(|agent| agent == &wanted) {
                return Err(Error::new(
                    "profile_error",
                    format!("OpenCode profile agent {wanted} is not installed"),
                ));
            }
            Self::resume_switch(
                session
                    .http
                    .switch_agent(session_id, &wanted, deadline)
                    .await,
                "agent",
            )?;
        }
        if model_needed {
            Self::resume_switch(
                session
                    .http
                    .switch_model(session_id, &parsed.model_ref, deadline)
                    .await,
                "model",
            )?;
        }
        let effective = session
            .http
            .get_session(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode resume configure readback is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        let model_verified = effective
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()))
            == Some(parsed.requested_model.clone());
        let agent_verified = parsed
            .requested_agent
            .as_ref()
            .is_none_or(|agent| effective.agent.as_ref() == Some(agent));
        if !model_verified || !agent_verified {
            return Err(Error::new(
                "unknown",
                "OpenCode effective model or agent contradicts the applied configuration",
            ));
        }
        Ok(json!({
            "model": effective.model.as_ref().map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim())),
            "model_verified": model_verified,
            "agent": effective.agent.clone(),
            "agent_verified": agent_verified,
            "cwd": effective.location.as_ref().map(|location| location.directory.clone()),
        }))
    }

    /// One resume setter: only its 204 applies. The session existed
    /// moments ago, so any refusal or loss is unknown, never a profile
    /// verdict — the readback decides.
    fn resume_switch(outcome: std::result::Result<(), HttpOutcome>, what: &str) -> Result<()> {
        match outcome {
            Ok(()) => Ok(()),
            Err(HttpOutcome::Rejected { reason, .. })
            | Err(HttpOutcome::Unknown { reason })
            | Err(HttpOutcome::NotDispatched { reason }) => Err(Error::new(
                "unknown",
                format!("OpenCode resume {what} switch is unresolved ({reason})"),
            )),
        }
    }

    /// Discard the server after a resume failure that wrote nothing and
    /// retain the failure for replay.
    async fn fail_resume(
        &mut self,
        parsed: &ParsedStart,
        mut session: AgentSession,
        error: Error,
    ) -> Error {
        let error = discard_failed_start(&mut session, error).await;
        self.retain_failed_start(&parsed.params, error)
    }

    /// Stash a resume-ambiguous session as unknown: the server and stream
    /// stay for the snapshot, and the resume is never repeated.
    async fn stash_resume_unknown(&mut self, session: AgentSession, message: String) -> Error {
        self.session = Some(session);
        Error::new(
            "unknown",
            format!("OpenCode resume is unresolved ({message})"),
        )
    }

    /// Start phase 5, readiness: publish `session.ready` and retain the
    /// result. A lost acknowledgement stashes the known session as unknown.
    async fn ready_start_session(
        &mut self,
        parsed: ParsedStart,
        mut session: AgentSession,
        created: SessionInfo,
        effective: Value,
    ) -> Result<Value> {
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
            "agent_id": parsed.params.agent_id,
            "generation": parsed.params.generation,
            "command_id": parsed.params.command_id,
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": created.id,
            "requested": {
                "model": parsed.requested_model,
                "cwd": parsed.workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "OpenCode agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "OpenCode",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" | "answer" => self.reply_command(params).await,
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
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        if session.phase != SessionPhase::Ready || native_io_blocked(session) {
            return Err(Error::new(
                "unknown",
                "OpenCode session or event source outcome is unresolved",
            ));
        }
        let (command_id, text) = match Self::admit_send_command(session, &params).await? {
            SendAdmission::Replay(value) => return Ok(value),
            SendAdmission::New { command_id, text } => (command_id, text),
        };
        // One deadline for admission plus association: the HTTP 200 only
        // proves inbox admission; the send applies at the exact
        // input-to-execution association on the event stream.
        let deadline = Instant::now() + RPC_TIMEOUT;
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let inbox =
            match Self::prompt_session(session, &command_id, &session_id, &text, deadline).await? {
                PromptAdmission::Rejected(value) => return Ok(value),
                PromptAdmission::Admitted(inbox) => inbox,
            };
        Self::associate_prompt_turn(
            session,
            command_id,
            &inbox.id,
            params.context.clone(),
            deadline,
        )
        .await
    }

    /// Send phase 1, admission: validate the input, replay a retained
    /// outcome without new native I/O, or record the new command. The
    /// idle check reads the event task's current status through the
    /// control queue, serialized behind the terminal commit: the watch
    /// snapshot still shows the finished turn when a send follows its
    /// `turn.completed` immediately.
    async fn admit_send_command(
        session: &mut AgentSession,
        params: &AgentCommandParams,
    ) -> Result<SendAdmission> {
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
                return Ok(SendAdmission::Replay(
                    json!({"state": "applied", "turn_id": turn_id}),
                ));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt outcome is unresolved and will not be repeated",
                ));
            }
            if session.rejected_commands.contains(&params.command_id) {
                return Ok(SendAdmission::Replay(json!({
                    "state": "rejected",
                    "reason": "OpenCode prompt was known not to apply",
                })));
            }
            return Err(Error::new(
                "unknown",
                "OpenCode command was already dispatched without a retained exact outcome",
            ));
        }
        let event_status = session.events.current_status().await.map_err(|_| {
            Error::new(
                "unknown",
                "OpenCode event status is unavailable; the send cannot prove the turn idle",
            )
        })?;
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "OpenCode already has an active turn"));
        }
        let combined = with_claimed_context(text, &params.context, "OpenCode")?;
        let command_id = params.command_id.clone();
        session
            .command_inputs
            .insert(command_id.clone(), text.to_owned());
        Ok(SendAdmission::New {
            command_id,
            text: combined,
        })
    }

    /// Send phase 2, prompt write: admit one input natively. A known rejection
    /// settles `rejected`; anything uncertain latches the session unknown.
    async fn prompt_session(
        session: &mut AgentSession,
        command_id: &str,
        session_id: &str,
        text: &str,
        deadline: Instant,
    ) -> Result<PromptAdmission> {
        let inbox = match session.http.prompt(session_id, text, deadline).await {
            Ok(inbox) => inbox,
            Err(HttpOutcome::Rejected { reason, .. })
            | Err(HttpOutcome::NotDispatched { reason }) => {
                session.rejected_commands.insert(command_id.to_owned());
                return Ok(PromptAdmission::Rejected(
                    json!({"state": "rejected", "reason": reason}),
                ));
            }
            Err(HttpOutcome::Unknown { .. }) => {
                session.unknown_commands.insert(command_id.to_owned());
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt may have applied; the input will not be repeated",
                ));
            }
        };
        if inbox.session_id != session_id || inbox.id.trim().is_empty() {
            session.unknown_commands.insert(command_id.to_owned());
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "OpenCode prompt answered for another session",
            ));
        }
        Ok(PromptAdmission::Admitted(inbox))
    }

    /// Send phase 3, association: bind the admitted inbox entry to its exact
    /// native execution on the event stream. Any failure is unknown: the input
    /// was admitted and is never repeated.
    async fn associate_prompt_turn(
        session: &mut AgentSession,
        command_id: String,
        inbox_id: &str,
        context: Vec<ContextItem>,
        deadline: Instant,
    ) -> Result<Value> {
        if let Err(error) = session
            .events
            .associate_inbox(command_id.clone(), inbox_id.to_owned(), context)
            .await
        {
            // The input was admitted, so a lost association is unknown even
            // when the registration itself was refused.
            let message = if native_no_write(&error.code) {
                "OpenCode prompt was admitted but its association is unknown".to_owned()
            } else {
                format!(
                    "OpenCode prompt was admitted but its association failed: {}",
                    error.message
                )
            };
            session.events.abandon_association(command_id.clone()).await;
            session.unknown_commands.insert(command_id);
            session.phase = SessionPhase::Unknown;
            return Err(Error::new("unknown", message));
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

    /// Interrupt the exact active turn: the authoritative event-task gate
    /// binds the command first, then one `session.interrupt` write runs.
    /// `interrupted: true` applies (the `interrupted` terminal settles the
    /// turn on the stream); `false` re-checks the gate — a finished turn is
    /// stale, a still-active one contradicts the server and stays unknown.
    async fn interrupt_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "OpenCode Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        if session.phase != SessionPhase::Ready || native_io_blocked(session) {
            return Err(Error::new(
                "unknown",
                "OpenCode session or event source outcome is unresolved",
            ));
        }
        let admitted = match Self::admit_control_command(session, &params, false)? {
            ControlAdmission::Replay(value) => return Ok(value),
            ControlAdmission::New {
                command_id,
                turn_id,
                ..
            } => (command_id, turn_id),
        };
        let (command_id, turn_id) = admitted;
        let Some(turn_id) = turn_id else {
            Self::abandon_control(session, &command_id);
            return Err(Error::new(
                "stale_turn",
                "OpenCode interrupt requires the exact turn id",
            ));
        };
        let live = match session.events.check_turn(&turn_id).await {
            Ok(live) => live,
            // The gate itself is unresolved (the event task is gone): the
            // turn may still be live, so this is unknown, never stale.
            Err(_) => {
                Self::abandon_control(session, &command_id);
                return Err(Error::new(
                    "unknown",
                    "OpenCode interrupt turn check is unresolved",
                ));
            }
        };
        if !live {
            Self::abandon_control(session, &command_id);
            return Err(Error::new(
                "stale_turn",
                "OpenCode interrupt named a finished turn",
            ));
        }
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let outcome = session
            .http
            .interrupt(&session_id, Instant::now() + RPC_TIMEOUT)
            .await;
        Self::finish_interrupt(session, &command_id, &turn_id, outcome).await
    }

    /// Settle an interrupt after its one write: applied on
    /// `interrupted: true`, rejected when the session is provably gone,
    /// stale when the turn finished underneath, unknown otherwise. Unknown
    /// never latches the session: the turn lifecycle stays on the stream,
    /// where the snapshot reveals the truth.
    async fn finish_interrupt(
        session: &mut AgentSession,
        command_id: &str,
        turn_id: &str,
        outcome: std::result::Result<bool, HttpOutcome>,
    ) -> Result<Value> {
        match outcome {
            Ok(true) => {
                let result = json!({
                    "state": "applied",
                    "command_id": command_id,
                    "turn_id": turn_id,
                });
                Self::settle_control(session, command_id, result.clone());
                Ok(result)
            }
            Ok(false) => {
                let live = match session.events.check_turn(turn_id).await {
                    Ok(live) => live,
                    // The write went out but the gate is unresolved (the
                    // event task is gone): unknown, never stale.
                    Err(_) => {
                        session.unknown_commands.insert(command_id.to_owned());
                        Self::abandon_control(session, command_id);
                        return Err(Error::new(
                            "unknown",
                            "OpenCode interrupt turn check is unresolved",
                        ));
                    }
                };
                if live {
                    session.unknown_commands.insert(command_id.to_owned());
                    Self::abandon_control(session, command_id);
                    Err(Error::new(
                        "unknown",
                        "OpenCode server reports no running execution but the turn is still active",
                    ))
                } else {
                    Self::abandon_control(session, command_id);
                    Err(Error::new(
                        "stale_turn",
                        "OpenCode interrupt named a finished turn",
                    ))
                }
            }
            Err(HttpOutcome::Rejected { reason, .. }) => {
                let result = json!({
                    "state": "rejected",
                    "command_id": command_id,
                    "turn_id": turn_id,
                    "reason": reason,
                });
                Self::settle_control(session, command_id, result.clone());
                Ok(result)
            }
            Err(HttpOutcome::Unknown { reason } | HttpOutcome::NotDispatched { reason }) => {
                // The binding stays: an exact retry is answered `unknown` and
                // never re-sends `/interrupt` (decision D-14).
                session.unknown_commands.insert(command_id.to_owned());
                Err(Error::new(
                    "unknown",
                    format!("OpenCode interrupt outcome is unresolved ({reason})"),
                ))
            }
        }
    }

    /// Reply to one exact retained native request. The event task stages
    /// the choice against the current offer and performs the one write;
    /// this side only retains the outcome for replay.
    async fn reply_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "OpenCode Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        if session.phase != SessionPhase::Ready || native_io_blocked(session) {
            return Err(Error::new(
                "unknown",
                "OpenCode session or event source outcome is unresolved",
            ));
        }
        let admitted = match Self::admit_control_command(session, &params, true)? {
            ControlAdmission::Replay(value) => return Ok(value),
            ControlAdmission::New {
                command_id,
                turn_id,
                request_id,
                args,
            } => (command_id, turn_id, request_id, args),
        };
        let (command_id, turn_id, request_id, args) = admitted;
        let (Some(turn_id), Some(request_id)) = (turn_id, request_id) else {
            Self::abandon_control(session, &command_id);
            return Err(Error::new(
                "not_found",
                "OpenCode reply requires the exact turn and request ids",
            ));
        };
        let verb = params.verb.clone();
        match session
            .events
            .reply_request(command_id.clone(), request_id, turn_id, verb, args)
            .await
        {
            Ok(result) => {
                if result.get("state").and_then(Value::as_str) == Some("unknown") {
                    session.unknown_commands.insert(command_id.clone());
                }
                Self::settle_control(session, &command_id, result.clone());
                Ok(result)
            }
            Err(error) => {
                // Guard failures (stale, missing, already decided, busy)
                // ran no write and pass through; only a lost control is
                // unknown.
                if matches!(
                    error.code.as_str(),
                    "not_found" | "stale_turn" | "rejected" | "busy"
                ) || native_no_write(&error.code)
                {
                    Self::abandon_control(session, &command_id);
                    return Err(error);
                }
                // The binding stays so an exact retry is answered `unknown`.
                session.unknown_commands.insert(command_id.clone());
                Err(Error::new(
                    "unknown",
                    format!("OpenCode reply outcome is unresolved ({})", error.message),
                ))
            }
        }
    }

    /// Control admission: replay a retained outcome without new native I/O,
    /// refuse a rebinding, or record the new control. Replies additionally
    /// require their request id here; the exact offer binding happens on
    /// the event task.
    fn admit_control_command(
        session: &mut AgentSession,
        params: &AgentCommandParams,
        needs_request: bool,
    ) -> Result<ControlAdmission> {
        if session.unknown_commands.contains(&params.command_id)
            && !session.control_bindings.contains_key(&params.command_id)
        {
            return Err(Error::new(
                "unknown",
                "OpenCode control outcome is unresolved and will not be repeated",
            ));
        }
        if let Some(binding) = session.control_bindings.get(&params.command_id) {
            if binding.verb != params.verb
                || binding.turn_id != params.turn_id
                || binding.request_id != params.request_id
                || binding.args != params.args
            {
                return Err(Error::new(
                    "source_integrity",
                    "OpenCode command id was reused with a different control",
                ));
            }
            if let Some(result) = session.control_results.get(&params.command_id) {
                return Ok(ControlAdmission::Replay(result.clone()));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode control outcome is unresolved and will not be repeated",
                ));
            }
            return Err(Error::new(
                "unknown",
                "OpenCode command was already dispatched without a retained exact outcome",
            ));
        }
        if needs_request && params.request_id.as_deref().is_none_or(str::is_empty) {
            return Err(Error::new(
                "not_found",
                "OpenCode reply requires the exact request id",
            ));
        }
        session.control_bindings.insert(
            params.command_id.clone(),
            ControlBinding {
                verb: params.verb.clone(),
                turn_id: params.turn_id.clone(),
                request_id: params.request_id.clone(),
                args: params.args.clone(),
            },
        );
        Ok(ControlAdmission::New {
            command_id: params.command_id.clone(),
            turn_id: params.turn_id.clone(),
            request_id: params.request_id.clone(),
            args: params.args.clone(),
        })
    }

    /// Retain an applied or rejected control result for byte-identical
    /// replay.
    fn settle_control(session: &mut AgentSession, command_id: &str, result: Value) {
        session
            .control_results
            .insert(command_id.to_owned(), result);
    }

    /// Forget a control binding whose outcome left no result (stale,
    /// unknown): nothing may replay from it.
    fn abandon_control(session: &mut AgentSession, command_id: &str) {
        session.control_bindings.remove(command_id);
        session.control_results.remove(command_id);
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentSnapshotParams =
            decode(raw, "OpenCode agent.snapshot params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "OpenCode",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "OpenCode Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        let event_status = match session.events.current_status().await {
            Ok(status) => status,
            Err(_) => {
                return unknown_snapshot(&params, "OpenCode event status is unavailable");
            }
        };
        if let Some(receipt) = Self::snapshot_stop_receipt(session, &params, &event_status) {
            return Ok(receipt);
        }
        if native_io_blocked(session) {
            // Closed resources block new native I/O regardless of the last
            // semantic phase; an Unknown-phase session with a proved exit
            // never gets a readback/reply/interrupt write after closure.
            return unknown_snapshot(
                &params,
                "OpenCode native resources are closed; native readback is unavailable",
            );
        }
        let request = match Self::resolve_snapshot_request(session, &params, &event_status) {
            Ok(request) => request,
            Err(reason) => return unknown_snapshot(&params, reason),
        };
        // The session readback proves the exact owned session is alive; turn
        // facts come from the committed stream fold.
        if !Self::session_readback_proven(&session.http, &request.session_id).await {
            return unknown_snapshot(
                &params,
                "OpenCode session readback did not prove the exact owned session",
            );
        }
        if session.command_inputs.contains_key(&params.command_id) && request.turn_id.is_none() {
            return unknown_snapshot(
                &params,
                "OpenCode command has no exact native turn association",
            );
        }
        let last_turn =
            match Self::snapshot_turn_facts(&event_status, &params, request.turn_id.as_deref()) {
                Ok(last_turn) => last_turn,
                Err(reason) => return unknown_snapshot(&params, reason),
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
            "native_ref": request.session_id,
            "active_turn": event_status.active_turn_id,
            "last_turn": last_turn,
            "batch_unknown": event_status.batch_unknown,
            "source_unknown": event_status.source_unknown,
            "result_known": result_known,
        }))
    }

    /// Snapshot phase 1, stop receipt: the stop command's own snapshot after
    /// its stop applied.
    fn snapshot_stop_receipt(
        session: &AgentSession,
        params: &AgentSnapshotParams,
        event_status: &EventStatus,
    ) -> Option<Value> {
        if session.stop_command_id.as_deref() == Some(params.command_id.as_str())
            && session.phase == SessionPhase::Stopped
            && params.turn_id.is_none()
        {
            let native_ref = session
                .session_id
                .clone()
                .or_else(|| event_status.session_id.clone());
            return Some(json!({
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
        None
    }

    /// Snapshot phase 2, guards: resolve the requested turn and prove the
    /// read. `Err(reason)` renders as an unknown snapshot with that reason.
    fn resolve_snapshot_request(
        session: &AgentSession,
        params: &AgentSnapshotParams,
        event_status: &EventStatus,
    ) -> std::result::Result<SnapshotRequest, &'static str> {
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
            return Err("OpenCode command has no proof for the requested native turn");
        }
        if !known_command(
            &session.start_command_id,
            &session.command_inputs,
            &session.stop_command_id,
            &params.command_id,
        ) && !session.control_bindings.contains_key(&params.command_id)
        {
            return Err("OpenCode command identity is not retained");
        }
        if session.phase == SessionPhase::Stopped {
            return Err("OpenCode native server is stopped; native readback is unavailable");
        }
        let Some(session_id) = session
            .session_id
            .as_deref()
            .or(event_status.session_id.as_deref())
        else {
            return Err("OpenCode session identity was not acknowledged");
        };
        Ok(SnapshotRequest {
            turn_id,
            session_id: session_id.to_owned(),
        })
    }

    /// Snapshot phase 3, liveness: the exact owned session answers readback.
    async fn session_readback_proven(http: &OpenCodeHttp, session_id: &str) -> bool {
        http.get_session(session_id, Instant::now() + RPC_TIMEOUT)
            .await
            .is_ok()
    }

    /// Snapshot phase 4, facts: the requested turn from the committed stream
    /// fold. `Err(reason)` renders as an unknown snapshot with that reason.
    fn snapshot_turn_facts(
        event_status: &EventStatus,
        params: &AgentSnapshotParams,
        turn_id: Option<&str>,
    ) -> std::result::Result<Option<TurnFacts>, &'static str> {
        match turn_id {
            Some(requested) => {
                if event_status.active_turn_id.as_deref() == Some(requested) {
                    Ok(Some(TurnFacts {
                        command_id: params.command_id.clone(),
                        turn_id: requested.to_owned(),
                        status: None,
                        final_text: None,
                        terminal: false,
                    }))
                } else if event_status
                    .last_turn
                    .as_ref()
                    .is_some_and(|turn| turn.turn_id == requested)
                {
                    Ok(event_status.last_turn.clone())
                } else {
                    Err("OpenCode readback did not prove the exact requested turn")
                }
            }
            None => Ok(event_status.last_turn.clone()),
        }
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentStopParams = decode(raw, "OpenCode agent.stop params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "OpenCode",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "OpenCode Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
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
            // Shutdown with proved resource closure ends the owned path
            // without replaying native interrupt/GET or a blind End. An
            // already-acked End is likewise never repeated.
            if session.phase != SessionPhase::Stopped && !session.resources_proved_closed {
                terminate_session(session, true).await?;
            }
            session.events.close().await;
        }
        Ok(json!({"state": "shutdown"}))
    }

    async fn shutdown_owned(&mut self) {
        if let Some(session) = self.session.as_mut() {
            if session.phase != SessionPhase::Stopped && !session.resources_proved_closed {
                let _ = terminate_session(session, true).await;
            }
            session.events.close().await;
        }
    }
}

/// The frozen profile auth selector: the one explicitly selected native
/// integration (ID or name) whose canonical credential the pre-serve
/// transfer prepares. Required and nonsecret; the capacity-only account
/// field is never consulted for authentication.
fn start_auth_selector(profile: &serde_json::Map<String, Value>) -> Result<IntegrationSelector> {
    let raw = profile
        .get("auth_integration")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "OpenCode profile requires an explicit auth_integration selector",
            )
        })?;
    IntegrationSelector::new(raw).map_err(|error| error.with_code("profile_error"))
}

/// The frozen profile model: an exact `provider/id` selection plus its
/// native reference.
fn start_model_ref(profile: &serde_json::Map<String, Value>) -> Result<(String, ModelRef)> {
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
            "OpenCode profile model must be provider/model with non-empty slash segments and no variant suffix",
        )
    })?;
    Ok((requested_model, model_ref))
}

/// The frozen start paths: an existing workspace directory and the explicit
/// native executable.
fn start_paths(
    profile: &serde_json::Map<String, Value>,
    params: &AgentStartParams,
) -> Result<(PathBuf, OsString)> {
    let workspace = params
        .workspace
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("profile_error", "OpenCode requires an existing workspace"))?;
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
    Ok((workspace, program))
}

/// Stop quiesces the native session and ends the owned server while
/// retaining the native session for resume. Ordinary stop never deletes
/// the session; it quiesces any running execution with the same native
/// interrupt the resume preflight uses, proves the exact session still
/// exists by readback, then ends the owned server through generation
/// process custody. Proven retained applies; proven absent or any
/// ambiguous readback is unknown. Force ends the owned server without
/// the quiesce wait and never touches another Agent.
async fn terminate_session(session: &mut AgentSession, force: bool) -> Result<()> {
    let deadline = Instant::now() + RPC_TIMEOUT;
    // A semantic quiesce/retention failure never skips the bounded
    // owned-resource cleanup below. The semantic error is preserved
    // separately, then processed closure and End run without replaying
    // the unknown effects above. Any error leaves phase Unknown.
    let mut stage = StopStage::Begin;
    let _ = note_stage(&mut stage, StopStage::Begin);
    let mut semantic_error = false;
    if session.events.begin_stop().await.is_err() {
        semantic_error = true;
    } else {
        let _ = note_stage(&mut stage, StopStage::Quiesce);
        if !force && let Some(session_id) = session.session_id.clone() {
            if OpenCodePlugin::resume_quiesce(session, &session_id, deadline)
                .await
                .is_err()
            {
                semantic_error = true;
            } else {
                let _ = note_stage(&mut stage, StopStage::Retention);
                let retained = match session.http.get_session(&session_id, deadline).await {
                    Ok(info) => info.id == session_id,
                    Err(_) => false,
                };
                if !retained {
                    semantic_error = true;
                }
            }
        }
    }
    let _ = note_stage(&mut stage, StopStage::Process);
    let process_exit = session.server.terminate_leader(force, deadline).await;
    if process_exit.is_ok() {
        session.resources_proved_closed = true;
    }
    // End is attempted only when the process-exit proof binds closure.
    let _ = note_stage(&mut stage, StopStage::EndAck);
    let ended = if process_exit.is_ok() {
        Some(session.events.end_stop().await)
    } else {
        None
    };
    if ended.as_ref().is_some_and(|result| result.is_ok()) {
        session.stop_end_ack_proved = true;
    }
    if process_exit.is_ok() {
        session.server.finish_diagnostics(deadline).await;
    }
    if semantic_error
        || process_exit.is_err()
        || ended.as_ref().is_none_or(|result| result.is_err())
    {
        session.phase = SessionPhase::Unknown;
        return Err(Error::new(
            "unknown",
            format!(
                "OpenCode stop outcome is unknown at stage {}",
                stage.as_str()
            ),
        ));
    }
    session.phase = SessionPhase::Stopped;
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

/// The first slash separates `providerID`; the full remaining id stays intact.
fn split_model(model: &str) -> Option<ModelRef> {
    let (provider_id, id) = split_provider_model(model)?;
    Some(ModelRef {
        id: id.to_owned(),
        provider_id: provider_id.to_owned(),
        variant: None,
    })
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
        ("request".to_owned(), "stream".to_owned()),
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
        verbs: vec![
            "launch".to_owned(),
            "resume".to_owned(),
            "send".to_owned(),
            "interrupt".to_owned(),
            "approve".to_owned(),
            "deny".to_owned(),
            "answer".to_owned(),
            "stop".to_owned(),
        ],
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

#[cfg(test)]
mod tests {
    use super::{opencode_manifest, opencode_scope, split_model, start_model_ref};
    use eliot_core::{AgentStartParams, Result};
    use eliot_plugin::session::{
        ResumeSubject, optional_profile_text, parse_resume_subject, validate_source_plan,
    };
    use serde_json::json;
    use std::path::PathBuf;

    const FAMILIES: &[&str] = &["session", "turn", "message", "request"];

    #[test]
    fn manifest_advertises_the_connected_pr2_verbs() {
        let manifest = opencode_manifest();
        assert_eq!(manifest.name, "opencode");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(manifest.harness_version, "2.0.32");
        assert!(manifest.available);
        assert_eq!(manifest.mode, "per_agent");
        assert_eq!(
            manifest.verbs,
            [
                "launch",
                "resume",
                "send",
                "interrupt",
                "approve",
                "deny",
                "answer",
                "stop"
            ]
        );
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
        assert!(validate_source_plan("OpenCode", FAMILIES, None).is_err());
        assert!(
            validate_source_plan(
                "OpenCode",
                FAMILIES,
                Some(&json!({
                    "session": "stream", "turn": "stream", "message": "stream",
                }))
            )
            .is_err(),
            "PR2 connects the request family, so PR1 plans no longer admit"
        );
        assert!(
            validate_source_plan(
                "OpenCode",
                FAMILIES,
                Some(&json!({
                    "session": "stream", "turn": "stream", "message": "stream",
                    "request": "stream",
                }))
            )
            .is_ok()
        );
    }

    #[test]
    fn model_split_requires_provider_slash_id() {
        let model = split_model("opencode/big-pickle").expect("slash form");
        assert_eq!(model.provider_id, "opencode");
        assert_eq!(model.id, "big-pickle");
        for bad in ["", "noprovider", "/noid", "provider/", "a//b", "/"] {
            assert!(split_model(bad).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn profile_model_preserves_nested_native_id_and_unmapped_variant_selection() {
        let requested = "openrouter/stealth/crouton-alpha";
        let profile = json!({"model":requested});
        let (frozen, model) = start_model_ref(profile.as_object().unwrap()).unwrap();
        assert_eq!(frozen, requested);
        assert_eq!(model.provider_id, "openrouter");
        assert_eq!(model.id, "stealth/crouton-alpha");
        assert_eq!(model.variant, None);
        assert_eq!(
            serde_json::to_value(&model).unwrap(),
            json!({"providerID":"openrouter", "id":"stealth/crouton-alpha"})
        );
        assert_eq!(format!("{}/{}", model.provider_id, model.id), requested);
        for malformed in [
            "",
            "noprovider",
            "/model",
            "openrouter/",
            "openrouter//model",
            "openrouter/stealth/",
            "openrouter/stealth//crouton-alpha",
            "openrouter\\stealth\\crouton-alpha",
            "openrouter/stealth\\crouton-alpha",
            "open#router/model",
            "openrouter/model#",
            "openrouter/model#default",
            "openrouter/model##default",
            "openrouter/model\n",
            "\topenrouter/model",
            "openrouter/stealth/ model",
            "openrouter/stealth/\0model",
        ] {
            let profile = json!({"model":malformed});
            assert_eq!(
                start_model_ref(profile.as_object().unwrap())
                    .unwrap_err()
                    .code,
                "profile_error",
                "{malformed:?}"
            );
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

    fn resume_params() -> AgentStartParams {
        let mut params = AgentStartParams::new("a7", 2, "resume-1");
        params.resume_from = Some(json!({"native_scope": "scope-1", "native_ref": "ses_1"}));
        params.source_state = Some(json!({
            "retained_generation": 1,
            "retained_source": {
                "agent_id": "a7", "generation": 1, "source": "stream",
                "families": ["session", "turn", "message"],
            },
            "current_turn": serde_json::Value::Null,
        }));
        params
    }

    fn validate_resume(params: &AgentStartParams) -> Result<ResumeSubject> {
        parse_resume_subject(
            params.resume_from.as_ref().expect("resume fixture"),
            params.source_state.as_ref(),
            &params.agent_id,
            params.generation,
            "OpenCode",
        )
    }

    #[test]
    fn resume_subject_binds_scope_ref_and_continuity() {
        let subject = validate_resume(&resume_params()).expect("valid subject");
        assert_eq!(subject.native_scope, "scope-1");
        assert_eq!(subject.native_ref, "ses_1");
        assert_eq!(subject.retained_turn, None);
        // A PR2 retained source (owning the request family) binds too:
        // only session/turn/message ownership is required.
        let mut params = resume_params();
        params.source_state = Some(json!({
            "retained_generation": 1,
            "retained_source": {
                "agent_id": "a7", "generation": 1, "source": "stream",
                "families": ["session", "turn", "message", "request"],
            },
            "current_turn": {
                "turn_id": "t", "state": "completed", "status": "completed",
                "started_at": 1, "completed_at": 2,
            },
        }));
        let subject = validate_resume(&params).expect("valid");
        assert_eq!(subject.retained_turn.as_deref(), Some("t"));
    }

    #[test]
    fn resume_subject_rejects_broken_continuity() {
        let mut params = resume_params();
        params.generation = 3;
        assert_eq!(
            validate_resume(&params).expect_err("skip").code,
            "stale_generation"
        );
        let mut params = resume_params();
        params.resume_from = Some(json!({"native_scope": "scope-1"}));
        assert_eq!(
            validate_resume(&params).expect_err("no ref").code,
            "invalid"
        );
        let mut params = resume_params();
        params.source_state = Some(json!({
            "retained_generation": 1,
            "retained_source": {
                "agent_id": "a7", "generation": 1, "source": "stream",
                "families": ["session"],
            },
            "current_turn": serde_json::Value::Null,
        }));
        assert_eq!(
            validate_resume(&params).expect_err("families").code,
            "unknown"
        );
        // The daemon names the retained stream source "stream".
        let mut params = resume_params();
        params.source_state = Some(json!({
            "retained_generation": 1,
            "retained_source": {
                "agent_id": "a7", "generation": 1, "source": "controlled",
                "families": ["session", "turn", "message"],
            },
            "current_turn": serde_json::Value::Null,
        }));
        assert_eq!(
            validate_resume(&params).expect_err("source name").code,
            "stale_generation"
        );
    }

    #[test]
    fn profile_agent_is_optional_but_typed() {
        let profile = serde_json::Map::new();
        assert_eq!(
            optional_profile_text(&profile, "agent", "OpenCode").expect("absent"),
            None
        );
        let profile = json!({"agent": "general"})
            .as_object()
            .expect("object")
            .clone();
        assert_eq!(
            optional_profile_text(&profile, "agent", "OpenCode").expect("present"),
            Some("general".to_owned())
        );
        let profile = json!({"agent": null}).as_object().expect("object").clone();
        assert_eq!(
            optional_profile_text(&profile, "agent", "OpenCode").expect("null"),
            None
        );
        let profile = json!({"agent": 7}).as_object().expect("object").clone();
        assert_eq!(
            optional_profile_text(&profile, "agent", "OpenCode")
                .expect_err("typed")
                .code,
            "profile_error"
        );
    }
}
