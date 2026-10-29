use crate::{
    events::{self, EventConfig, EventHandle, TurnFacts, rejected_control, unknown_control},
    native::{
        NativeClient, NativeLaunch, NativeProcess, native_no_write,
        types::{
            NativeClientInfo, NativeCollaborationMode, NativeCollaborationModeKind,
            NativeCollaborationSettings, NativeInitializeCapabilities, NativeInitializeParams,
            NativeInitializeResponse, NativeModelEntry, NativeModelListParams,
            NativeModelListResponse, NativeSandboxMode, NativeTextUserInput,
            NativeThreadConfigOverrides, NativeThreadResumeParams, NativeThreadStartParams,
            NativeTurnInterruptParams, NativeTurnStartParams, NativeTurnStartResponse,
            NativeTurnSteerParams, NativeTurnSteerResponse, NativeUserInput, ThreadReadback,
            ThreadStartReadback,
        },
    },
    observe::ObserverSession,
    route::CodexRoute,
};
use eliot_core::{Error, NativeStoreBinding, ProcessRecord, ProcessState, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, DiscoverParams,
    InitializeParams, MAX_EVENT_BYTES, MAX_TEXT_BYTES, ObserverAttachParams, ObserverDetachParams,
    ObserverSnapshotParams, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, ProfileStoreParams},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, interval, timeout_at},
};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_POLL_DEADLINE: Duration = Duration::from_secs(2);
const MODEL_PAGE_SIZE: u32 = 100;
const MAX_MODEL_PAGES: usize = 100;
const TURN_PAGE_SIZE: u32 = 1000;
const MAX_TURN_PAGES: usize = 16;
const ITEM_PAGE_SIZE: u32 = 1000;
const MAX_ITEM_PAGES: usize = 16;
const MAX_CONTROL_COMMANDS: usize = 32;

#[derive(Default)]
struct CodexPlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
    observer: Option<ObserverSession>,
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
    thread_id: Option<String>,
    native_scope: String,
    requested_model: String,
    selected_effort: String,
    collaboration_mode: Option<NativeCollaborationModeKind>,
    secrets: Vec<String>,
    native: NativeProcess,
    native_client: NativeClient,
    events: EventHandle,
    command_inputs: HashMap<String, String>,
    command_turns: HashMap<String, String>,
    unknown_commands: HashSet<String>,
    rejected_commands: HashSet<String>,
    control_commands: HashMap<String, ControlCommand>,
    stop_command_id: Option<String>,
}

struct ControlCommand {
    verb: String,
    turn_id: Option<String>,
    request_id: Option<String>,
    args: Value,
    outcome: ControlOutcome,
}

enum ControlOutcome {
    Pending,
    Applied(Value),
    Rejected(Value),
    Unknown,
}

struct TargetEnvironment {
    variables: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    metadata_secrets: Vec<String>,
}

struct NativeStartContext<'a> {
    route: &'a CodexRoute,
    store: &'a NativeStoreBinding,
    metadata_secrets: &'a [String],
}

struct ResumeSubject {
    native_scope: String,
    native_ref: String,
    retained_turn: Option<String>,
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

    let mut plugin = CodexPlugin::default();
    let mut completed_shutdown = false;
    let mut live_ticks = interval(LIVE_POLL_INTERVAL);
    live_ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let request = tokio::select! {
            biased;
            request = requests.recv() => request,
            _ = live_ticks.tick(), if plugin.observer.as_ref().is_some_and(ObserverSession::live_poll_enabled) => {
                let initialized = plugin.initialized.as_ref().ok_or_else(not_initialized)?;
                let observer = plugin.observer.as_mut().ok_or_else(not_initialized)?;
                let until = Instant::now() + LIVE_POLL_DEADLINE;
                // Cancelling this future leaves owned file I/O and pending
                // publication in the observer/publisher for settlement or close.
                tokio::select! {
                    biased;
                    request = requests.recv() => request,
                    result = timeout_at(until, observer.poll_live(&client, &initialized.credential, until)) => {
                        if result.is_err() {
                            observer.live_poll_timed_out();
                        }
                        continue;
                    }
                }
            }
        };
        let Some(request) = request else { break };
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
            "Codex plugin input closed before shutdown completed",
        ))
    }
}

impl CodexPlugin {
    async fn dispatch(&mut self, request: &RpcRequest, client: &PeerClient) -> Result<Value> {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "discover" => self.discover(&request.params).await,
            "profile.store" => self.profile_store(&request.params).await,
            "agent.attach" => self.attach(&request.params).await,
            "agent.detach" => self.detach(&request.params).await,
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => self.command(&request.params).await,
            "agent.snapshot" => self.snapshot(&request.params).await,
            "agent.stop" => self.stop(&request.params).await,
            "shutdown" => self.shutdown().await,
            _ => Err(Error::new(
                "unsupported",
                "Codex plugin method is not implemented",
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
                        Error::new("internal", "Codex diagnostics could not be encoded")
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
        let params: InitializeParams = decode(raw, "Codex plugin initialize params are invalid")?;
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > MAX_CREDENTIAL_BYTES
        {
            return Err(Error::new(
                "invalid",
                "Codex plugin initialize requires protocol, private instance directory, and credential",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.credential != params.credential
                || existing.instance_dir != params.instance_dir
            {
                return Err(Error::new(
                    "bad_state",
                    "Codex plugin is already initialized for another instance",
                ));
            }
        } else {
            self.initialized = Some(Initialized {
                credential: params.credential,
                instance_dir: params.instance_dir,
            });
        }
        serde_json::to_value(codex_manifest())
            .map_err(|_| Error::new("internal", "Codex plugin manifest could not be encoded"))
    }

    async fn discover(&self, raw: &Value) -> Result<Value> {
        if self.session.is_some() || self.failed_start.is_some() || self.observer.is_some() {
            return Err(Error::new(
                "bad_state",
                "Codex discovery requires a dedicated reader plugin instance",
            ));
        }
        let params: DiscoverParams = decode(raw, "Codex discover params are invalid")?;
        let (launch, secrets, _) =
            self.reader_launch(&params.profile, &params.target_environment)?;
        crate::discovery::discover(params, launch, secrets).await
    }

    fn reader_launch(
        &self,
        profile: &Value,
        environment: &Value,
    ) -> Result<(NativeLaunch, Vec<String>, Vec<String>)> {
        let initialized = self.initialized.as_ref().ok_or_else(not_initialized)?;
        let route = CodexRoute::from_profile(profile)?;
        let profile = profile.as_object().ok_or_else(|| {
            Error::new(
                "profile_error",
                "Codex discovery requires a frozen profile object",
            )
        })?;
        if profile.get("harness").and_then(Value::as_str) != Some("codex") {
            return Err(Error::new(
                "profile_error",
                "Codex discovery received another harness",
            ));
        }
        let program = match profile.get("cmd") {
            Some(Value::String(program)) if !program.trim().is_empty() => OsString::from(program),
            None => OsString::from("codex"),
            _ => {
                return Err(Error::new(
                    "profile_error",
                    "Codex discovery cmd must name one executable",
                ));
            }
        };
        let TargetEnvironment {
            mut variables,
            mut secrets,
            mut metadata_secrets,
        } = target_environment(Some(environment), profile.get("pass_env"))?;
        route.capture_client_secret(&variables, &mut secrets, &mut metadata_secrets, false)?;
        route.strip_client_key(&mut variables);
        if variables.iter().any(|(name, _)| {
            name.to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("ELIOT_AGENT_")
        }) {
            return Err(Error::new(
                "not_allowed",
                "discovery cannot receive Agent identity or tokens",
            ));
        }
        secrets.push(initialized.credential.clone());
        metadata_secrets.push(initialized.credential.clone());
        let launch = NativeLaunch {
            program,
            args: route.args()?,
            cwd: PathBuf::from(&initialized.instance_dir),
            environment: variables,
        };
        Ok((launch, secrets, metadata_secrets))
    }

    async fn profile_store(&mut self, raw: &Value) -> Result<Value> {
        if self.session.is_some() || self.failed_start.is_some() || self.observer.is_some() {
            return Err(Error::new(
                "bad_state",
                "Codex profile.store requires a dedicated metadata instance",
            ));
        }
        if !raw.is_object() {
            return Err(Error::new(
                "invalid",
                "Codex profile.store params are invalid",
            ));
        }
        let params: ProfileStoreParams = decode(raw, "Codex profile.store params are invalid")?;
        let workspace = Path::new(&params.workspace);
        if params.workspace.trim().is_empty() || !workspace.is_dir() {
            return Err(Error::new(
                "profile_error",
                "Codex profile.store requires an existing workspace",
            ));
        }
        let (mut launch, diagnostic_secrets, metadata_secrets) =
            self.reader_launch(&params.profile, &params.target_environment)?;
        launch.cwd = workspace.to_path_buf();
        let binding = crate::discovery::profile_store(
            &launch,
            &params.target_environment,
            &metadata_secrets,
            &diagnostic_secrets,
            Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await?;
        serde_json::to_value(binding).map_err(|_| {
            Error::new(
                "native_scope_unverified",
                "Codex native store binding could not be encoded",
            )
        })
    }

    async fn attach(&mut self, raw: &Value) -> Result<Value> {
        let params: ObserverAttachParams = decode(raw, "Codex observer attach params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "Codex observation requires a dedicated plugin instance",
            ));
        }
        if let Some(observer) = &self.observer {
            return observer.repeat_attach(&params);
        }
        let (launch, secrets, _) =
            self.reader_launch(&params.profile, &params.target_environment)?;
        let (observer, result) = ObserverSession::attach(
            params,
            launch,
            secrets,
            Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await?;
        self.observer = Some(observer);
        Ok(result)
    }

    async fn detach(&mut self, raw: &Value) -> Result<Value> {
        let params: ObserverDetachParams = decode(raw, "Codex observer detach params are invalid")?;
        self.observer
            .as_mut()
            .ok_or_else(|| Error::new("unsupported", "Codex plugin has no observation attachment"))?
            .detach(&params, Instant::now() + crate::discovery::READ_TIMEOUT)
            .await
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        if self.observer.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot start a controlled session",
            ));
        }
        let params: AgentStartParams = decode(raw, "Codex agent.start params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return match session.phase {
                    SessionPhase::Ready => session.start_result.clone().ok_or_else(|| {
                        Error::new("unknown", "Codex start result was not retained")
                    }),
                    SessionPhase::Unknown => Err(Error::new(
                        "unknown",
                        "Codex thread/start outcome is unresolved; it will not be repeated",
                    )),
                    SessionPhase::Stopped => Err(Error::new(
                        "bad_state",
                        "Codex Agent generation has already stopped",
                    )),
                };
            }
            return Err(Error::new(
                "stale_generation",
                "Codex plugin is already bound to another Agent generation",
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
                "Codex plugin already settled a different start command",
            ));
        }
        let initialized = self.initialized.as_ref().ok_or_else(not_initialized)?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Codex start does not accept input; send it as a separate command",
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
                )
            })
            .transpose()
            .map_err(|mut error| {
                if error.code == "unknown" {
                    error.code = "native_readback".to_owned();
                }
                error
            })?;
        validate_source_plan(params.source_plan.as_ref())?;
        let frozen_profile = params
            .profile
            .as_ref()
            .ok_or_else(|| Error::new("profile_error", "Codex requires a frozen profile object"))?;
        let profile = frozen_profile
            .as_object()
            .ok_or_else(|| Error::new("profile_error", "Codex requires a frozen profile object"))?;
        if profile.get("harness").and_then(Value::as_str) != Some("codex") {
            return Err(Error::new(
                "profile_error",
                "Codex plugin received a profile for another harness",
            ));
        }
        let route = CodexRoute::from_profile(frozen_profile)?;
        let frozen_store = frozen_store_binding(profile)?;
        let requested_model = profile
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Error::new("profile_error", "Codex profile requires an exact model id"))?
            .to_owned();
        let requested_effort = optional_profile_text(profile, "effort")?;
        let collaboration_mode =
            match optional_profile_text(profile, "collaboration_mode")?.as_deref() {
                None => None,
                Some("default") => Some(NativeCollaborationModeKind::Default),
                Some("plan") => Some(NativeCollaborationModeKind::Plan),
                Some(_) => {
                    return Err(Error::new(
                        "profile_error",
                        "Codex collaboration_mode must be default or plan",
                    ));
                }
            };
        let permissions = profile
            .get("permissions")
            .and_then(Value::as_str)
            .unwrap_or("workspace-write")
            .to_owned();
        if !matches!(
            permissions.as_str(),
            "read-only" | "workspace-write" | "danger-full-access"
        ) {
            return Err(Error::new(
                "profile_error",
                "Codex profile permissions must be read-only, workspace-write, or danger-full-access",
            ));
        }
        let sandbox_mode = native_sandbox_mode(&permissions)?;
        let approval_policy = optional_profile_text(profile, "approval_policy")?
            .unwrap_or_else(|| "never".to_owned());
        if !matches!(approval_policy.as_str(), "never" | "on-request") {
            return Err(Error::new(
                "profile_error",
                "Codex approval_policy must be never or on-request",
            ));
        }
        let workspace = params
            .workspace
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| Error::new("profile_error", "Codex requires an existing workspace"))?;
        if !workspace.is_dir() {
            return Err(Error::new(
                "profile_error",
                "Codex workspace must be an existing directory",
            ));
        }
        let program = profile
            .get("cmd")
            .map(|value| {
                value
                    .as_str()
                    .filter(|program| !program.trim().is_empty())
                    .map(OsString::from)
                    .ok_or_else(|| {
                        Error::new(
                            "profile_error",
                            "Codex profile cmd must be one executable path",
                        )
                    })
            })
            .transpose()?
            .unwrap_or_else(|| OsString::from("codex"));
        let TargetEnvironment {
            variables: environment,
            mut secrets,
            mut metadata_secrets,
        } = target_environment(params.target_environment.as_ref(), profile.get("pass_env"))?;
        route.capture_client_secret(&environment, &mut secrets, &mut metadata_secrets, true)?;
        secrets.push(initialized.credential.clone());
        metadata_secrets.push(initialized.credential.clone());
        let launch = NativeLaunch {
            program,
            args: route.args()?,
            cwd: workspace.clone(),
            environment,
        };
        let launch = crate::discovery::pinned_launch_for_binding(&launch, &frozen_store)?;
        let mut native =
            NativeProcess::spawn(launch, SecretPolicy::new(secrets.iter().cloned())).await?;
        let frames = native.take_frames()?;
        let stream = native.stream_state();
        let native_client = native.client();
        let events = events::spawn(
            frames,
            stream,
            native_client.clone(),
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
            thread_id: None,
            native_scope: String::new(),
            requested_model: requested_model.clone(),
            selected_effort: String::new(),
            collaboration_mode,
            secrets,
            native,
            native_client,
            events,
            command_inputs: HashMap::new(),
            command_turns: HashMap::new(),
            unknown_commands: HashSet::new(),
            rejected_commands: HashSet::new(),
            control_commands: HashMap::new(),
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
                    "{}; owned app-server cleanup is unconfirmed; no thread or input was sent",
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

        let (selected_effort, native_scope) = match Self::initialize_native(
            &session.native_client,
            &workspace,
            &requested_model,
            requested_effort.as_deref(),
            collaboration_mode.is_some(),
            NativeStartContext {
                route: &route,
                store: &frozen_store,
                metadata_secrets: &metadata_secrets,
            },
        )
        .await
        {
            Ok((effort, scope)) => (effort, scope),
            Err(error) => {
                let error = discard_failed_start(&mut session, error).await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
        };
        session.selected_effort = selected_effort.clone();
        session.native_scope = native_scope;

        if let Some(subject) = &resume_subject {
            if subject.native_scope != session.native_scope {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Codex resume subject is outside the authenticated native scope",
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
            session.thread_id = Some(subject.native_ref.clone());
            if let Err(error) = verify_resume_preflight(
                &session.native_client,
                &subject.native_ref,
                subject.retained_turn.as_deref(),
            )
            .await
            {
                let message = format!(
                    "Codex resume preflight failed before thread/resume dispatch: {}",
                    error.message
                );
                let error = contextual_error(error, "native_readback", message);
                let error = discard_failed_start(&mut session, error).await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
        }

        let (thread_method, params_value) = if let Some(subject) = &resume_subject {
            let params = NativeThreadResumeParams {
                thread_id: subject.native_ref.clone(),
                exclude_turns: true,
                model: Some(requested_model.clone()),
                model_provider: Some(route.provider().to_owned()),
                cwd: Some(workspace.to_string_lossy().into_owned()),
                sandbox: Some(sandbox_mode),
                approval_policy: Some(json!(approval_policy.clone())),
                config: Some(NativeThreadConfigOverrides {
                    model_reasoning_effort: selected_effort.clone(),
                }),
            };
            (
                "thread/resume",
                serde_json::to_value(params).map_err(|_| {
                    Error::new(
                        "internal",
                        "Codex thread/resume params could not be encoded",
                    )
                })?,
            )
        } else {
            let params = NativeThreadStartParams {
                cwd: Some(workspace.to_string_lossy().into_owned()),
                model: Some(requested_model.clone()),
                model_provider: Some(route.provider().to_owned()),
                sandbox: Some(sandbox_mode),
                approval_policy: Some(json!(approval_policy.clone())),
                config: Some(NativeThreadConfigOverrides {
                    model_reasoning_effort: selected_effort.clone(),
                }),
                service_tier: None,
                ephemeral: None,
            };
            (
                "thread/start",
                serde_json::to_value(params).map_err(|_| {
                    Error::new("internal", "Codex thread/start params could not be encoded")
                })?,
            )
        };
        let response_value = match session
            .native_client
            .call(thread_method, params_value, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(value) => value,
            Err(error) if error.code == "native_rejected" => {
                let (code, message) = if resume_subject.is_some() {
                    (
                        "not_found",
                        "Codex rejected resume of the exact retained thread",
                    )
                } else {
                    (
                        "profile_error",
                        "Codex rejected thread/start for the selected profile",
                    )
                };
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
            Err(error) if native_no_write(&error.code) => {
                let message = format!("Codex {thread_method} was not dispatched");
                let code = error.code.clone();
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id,
                    generation: params.generation,
                    command_id: params.command_id,
                    error: error.clone(),
                });
                return Err(error);
            }
            Err(error) => {
                self.session = Some(session);
                return Err(contextual_error(
                    error,
                    "unknown",
                    format!(
                        "Codex {thread_method} may have changed native state; it will not be repeated"
                    ),
                ));
            }
        };
        let readback: ThreadStartReadback = match serde_json::from_value::<ThreadStartReadback>(
            response_value.clone(),
        ) {
            Ok(readback)
                if !readback.thread.id.trim().is_empty()
                    && resume_subject
                        .as_ref()
                        .is_none_or(|subject| subject.native_ref == readback.thread.id) =>
            {
                readback
            }
            _ => {
                if let Some(thread_id) = response_value
                    .get("thread")
                    .and_then(|thread| thread.get("id"))
                    .and_then(Value::as_str)
                {
                    session.thread_id = Some(thread_id.to_owned());
                }
                self.session = Some(session);
                return Err(Error::new(
                    "unknown",
                    format!(
                        "Codex {thread_method} returned without the exact expected thread identity"
                    ),
                ));
            }
        };
        session.thread_id = Some(readback.thread.id.clone());
        if resume_subject.is_some()
            && response_value
                .get("thread")
                .is_none_or(|thread| thread_state(thread) != "idle")
        {
            self.session = Some(session);
            return Err(Error::new(
                "unknown",
                "Codex thread/resume did not prove the resumed thread idle; no input was sent",
            ));
        }
        let effort_verified = route.requires_chatgpt()
            && readback
                .reasoning_effort
                .as_deref()
                .is_some_and(|effort| effort == selected_effort);
        let effective = json!({
            "model": readback.model.clone(),
            "model_provider": readback.model_provider.clone(),
            "cwd": readback.cwd.clone(),
            "permissions": readback.sandbox.clone(),
            "approval_policy": readback.approval_policy.clone(),
            "effort": readback.reasoning_effort.clone(),
            "effort_verified": effort_verified,
        });
        let settings_match = readback.model == requested_model
            && readback.model_provider == route.provider()
            && paths_equal(Path::new(&readback.cwd), &workspace)
            && sandbox_matches(&readback.sandbox, &permissions, &workspace)
            && readback_approval_matches(&readback.approval_policy, &approval_policy)
            && readback
                .reasoning_effort
                .as_deref()
                .is_none_or(|effort| effort == selected_effort);
        if let Err(error) = session
            .events
            .ready(
                session.native_scope.clone(),
                readback.thread.id.clone(),
                effective.clone(),
            )
            .await
        {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            return Err(Error::new(
                "unknown",
                format!(
                    "Codex thread is known but session.ready was not acknowledged: {}",
                    error.message
                ),
            ));
        }
        if !settings_match {
            session.phase = SessionPhase::Ready;
            let stopped = terminate_session(&mut session, true).await.is_ok();
            if stopped {
                session.phase = SessionPhase::Stopped;
            } else {
                session.phase = SessionPhase::Unknown;
            }
            self.session = Some(session);
            return Err(Error::new(
                "profile_error",
                "Codex effective model, provider, cwd, permissions, approval policy, or reasoning effort did not match the frozen profile",
            ));
        }
        session.phase = SessionPhase::Ready;
        let mut result = json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": readback.thread.id,
            "requested": {
                "model": requested_model,
                "effort": selected_effort,
                "cwd": workspace.to_string_lossy(),
                "permissions": permissions,
                "approval_policy": approval_policy,
            },
            "effective": effective,
        });
        if let Some(mode) = session.collaboration_mode {
            result["requested"]["collaboration_mode"] = json!(mode);
        }
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn initialize_native(
        client: &NativeClient,
        workspace: &Path,
        requested_model: &str,
        requested_effort: Option<&str>,
        experimental_api: bool,
        context: NativeStartContext<'_>,
    ) -> Result<(String, String)> {
        let init_params = NativeInitializeParams {
            client_info: NativeClientInfo {
                name: "eliot".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: Some("Eliot".to_owned()),
            },
            capabilities: experimental_api.then_some(NativeInitializeCapabilities {
                experimental_api: true,
            }),
        };
        let init_value = serde_json::to_value(init_params)
            .map_err(|_| Error::new("internal", "Codex initialize params could not be encoded"))?;
        let initialized: NativeInitializeResponse = serde_json::from_value(
            client
                .call("initialize", init_value, Instant::now() + RPC_TIMEOUT)
                .await
                .map_err(|error| {
                    contextual_error(error, "profile_error", "Codex app-server initialize failed")
                })?,
        )
        .map_err(|_| {
            Error::new(
                "profile_error",
                "Codex app-server initialize response was invalid",
            )
        })?;
        if initialized.codex_home.trim().is_empty() {
            return Err(Error::new(
                "profile_error",
                "Codex app-server did not identify its data scope",
            ));
        }
        if initialized.platform_family.trim().is_empty()
            || initialized.platform_os.trim().is_empty()
            || initialized.user_agent.trim().is_empty()
        {
            return Err(Error::new(
                "profile_error",
                "Codex app-server initialize omitted runtime identity fields",
            ));
        }
        client
            .notify("initialized", None, Instant::now() + RPC_TIMEOUT)
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "profile_error",
                    "Codex app-server did not accept initialized",
                )
            })?;

        crate::discovery::verify_initialized_store(
            client,
            &initialized,
            context.store,
            workspace,
            context.metadata_secrets,
            Instant::now() + RPC_TIMEOUT,
        )
        .await?;
        let native_scope = context.store.native_scope.clone();

        if context.route.requires_chatgpt() {
            let account = client
                .call(
                    "account/read",
                    json!({"refreshToken": false}),
                    Instant::now() + RPC_TIMEOUT,
                )
                .await
                .map_err(|error| {
                    contextual_error(error, "profile_error", "Codex account/read failed")
                })?;
            if account
                .get("account")
                .and_then(|value| value.get("type"))
                .and_then(Value::as_str)
                != Some("chatgpt")
            {
                return Err(Error::new(
                    "profile_error",
                    "Codex profile requires an authenticated ChatGPT account",
                ));
            }
        }

        let config = client
            .call(
                "config/read",
                json!({
                    "cwd": workspace.to_string_lossy(),
                    "includeLayers": false,
                }),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "profile_error",
                    "Codex config/read could not verify the per-process route",
                )
            })?;
        let effective_config = config.get("config").ok_or_else(|| {
            Error::new(
                "profile_error",
                "Codex config/read omitted effective config",
            )
        })?;
        context
            .route
            .verify_config(effective_config, context.metadata_secrets)?;

        if !context.route.requires_chatgpt() {
            return Ok((
                context.route.explicit_effort(requested_effort)?,
                native_scope,
            ));
        }

        let model = discover_model(client, requested_model).await?;
        let effort = match requested_effort {
            Some(effort)
                if model
                    .supported_reasoning_efforts
                    .iter()
                    .any(|supported| supported.reasoning_effort == effort) =>
            {
                effort.to_owned()
            }
            Some(_) => {
                return Err(Error::new(
                    "profile_error",
                    "Codex profile effort is not supported by the exact selected model",
                ));
            }
            None => {
                if !model
                    .supported_reasoning_efforts
                    .iter()
                    .any(|supported| supported.reasoning_effort == model.default_reasoning_effort)
                {
                    return Err(Error::new(
                        "profile_error",
                        "Codex model catalogue default effort is not in its supported-effort list",
                    ));
                }
                model.default_reasoning_effort
            }
        };
        Ok((effort, native_scope))
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        if self.observer.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot issue native control commands",
            ));
        }
        let params: AgentCommandParams = decode(raw, "Codex agent.command params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "steer" => self.steer(params).await,
            "interrupt" => self.interrupt(params).await,
            "approve" | "deny" | "answer" => self.reply_request(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Codex plugin command verb is not implemented",
            )),
        }
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Codex session or event source outcome is unresolved",
            ));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Codex send requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new("frame_too_large", "Codex input exceeds 512 KiB"));
        }
        if let Some(previous) = session.command_inputs.get(&params.command_id) {
            if previous != text {
                return Err(Error::new(
                    "source_integrity",
                    "Codex command id was reused with different input",
                ));
            }
            if let Some(turn_id) = session.command_turns.get(&params.command_id) {
                return Ok(json!({"state": "applied", "turn_id": turn_id}));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "Codex turn/start outcome is unresolved and will not be repeated",
                ));
            }
            if session.rejected_commands.contains(&params.command_id) {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Codex turn/start was known not to apply",
                }));
            }
            return Err(Error::new(
                "unknown",
                "Codex command was already dispatched without a retained exact outcome",
            ));
        }
        let event_status = session.events.status();
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "Codex already has an active turn"));
        }
        let command_id = params.command_id.clone();
        if session.command_inputs.len() >= 32 {
            return Err(Error::new(
                "unsupported",
                "Codex L2.1 per-generation command history bound has been reached",
            ));
        }
        session
            .command_inputs
            .insert(command_id.clone(), text.to_owned());
        let turn_params = NativeTurnStartParams {
            thread_id: session
                .thread_id
                .clone()
                .ok_or_else(|| Error::new("unknown", "Codex thread identity is unresolved"))?,
            input: vec![NativeUserInput::Text(NativeTextUserInput::text(
                text.to_owned(),
            ))],
            cwd: None,
            model: Some(session.requested_model.clone()),
            effort: Some(session.selected_effort.clone()),
            collaboration_mode: session
                .collaboration_mode
                .map(|mode| NativeCollaborationMode {
                    mode,
                    settings: NativeCollaborationSettings {
                        model: session.requested_model.clone(),
                        reasoning_effort: Some(session.selected_effort.clone()),
                        developer_instructions: None,
                    },
                }),
            sandbox_policy: None,
            client_user_message_id: None,
        };
        let encoded = serde_json::to_value(turn_params)
            .map_err(|_| Error::new("internal", "Codex turn/start params could not be encoded"))?;
        let response = session
            .native_client
            .call("turn/start", encoded, Instant::now() + RPC_TIMEOUT)
            .await;
        let response = match response {
            Ok(value) => value,
            Err(error) if native_no_write(&error.code) => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Codex turn/start was not dispatched",
                }));
            }
            Err(error) if error.code == "native_rejected" => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({"state": "rejected", "reason": "Codex rejected turn/start"}));
            }
            Err(error) if error.code == "frame_too_large" => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Codex turn/start frame exceeded the native transport bound before dispatch",
                }));
            }
            Err(_) => {
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "Codex turn/start may have applied; the input will not be repeated",
                ));
            }
        };
        let started: NativeTurnStartResponse =
            match serde_json::from_value::<NativeTurnStartResponse>(response) {
                Ok(started) if !started.turn.id.trim().is_empty() => started,
                _ => {
                    session.unknown_commands.insert(command_id);
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Codex turn/start returned no exact turn identity",
                    ));
                }
            };
        let turn_id = started.turn.id;
        session
            .command_turns
            .insert(command_id.clone(), turn_id.clone());
        if session
            .events
            .associate_turn(command_id.clone(), turn_id.clone())
            .await
            .is_err()
        {
            session.unknown_commands.insert(command_id);
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "Codex turn was accepted but turn.started was not acknowledged",
            ));
        }
        Ok(json!({"state": "applied", "turn_id": turn_id}))
    }

    async fn steer(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "turn/steer does not accept request_id",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Error::new("stale_turn", "turn/steer requires a captured turn id"))?;
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Codex steer requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Ok(rejected_control("Codex steer input exceeds 512 KiB"));
        }
        if session.secrets.iter().any(|secret| text.contains(secret)) {
            return Err(Error::new(
                "secret_in_payload",
                "Codex steer contains a known credential",
            ));
        }
        if let Some(result) = control_replay(session, &params)? {
            return Ok(result);
        }
        if session.phase != SessionPhase::Ready
            || session.events.status().active_turn_id.as_deref() != Some(turn_id)
            || session.events.status().source_unknown
        {
            let result = rejected_control("Codex steer does not match a healthy active turn");
            settle_control(
                session,
                &params.command_id,
                ControlOutcome::Rejected(result.clone()),
            );
            return Ok(result);
        }
        let Some(thread_id) = session.thread_id.clone() else {
            let result = unknown_control("Codex thread identity is unresolved");
            settle_control(session, &params.command_id, ControlOutcome::Unknown);
            return Ok(result);
        };
        let turn_params = NativeTurnSteerParams {
            expected_turn_id: turn_id.to_owned(),
            input: vec![NativeUserInput::Text(NativeTextUserInput::text(
                text.to_owned(),
            ))],
            thread_id,
        };
        let encoded = serde_json::to_value(turn_params)
            .map_err(|_| Error::new("internal", "Codex turn/steer params could not be encoded"))?;
        match session
            .native_client
            .call("turn/steer", encoded, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "native_not_dispatched" | "native_rejected" | "frame_too_large" | "busy"
                ) =>
            {
                let result = rejected_control("Codex turn/steer was not applied");
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Codex turn/steer outcome is unknown");
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(result)
            }
            Ok(response) => {
                let steer: NativeTurnSteerResponse =
                    match serde_json::from_value::<NativeTurnSteerResponse>(response) {
                        Ok(steer) if steer.turn_id == turn_id => steer,
                        _ => {
                            let result = unknown_control(
                                "Codex turn/steer did not confirm the captured turn id",
                            );
                            settle_control(session, &params.command_id, ControlOutcome::Unknown);
                            return Ok(result);
                        }
                    };
                let result = json!({
                    "state": "applied",
                    "command_id": params.command_id,
                    "turn_id": steer.turn_id,
                });
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Applied(result.clone()),
                );
                Ok(result)
            }
        }
    }

    async fn interrupt(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "turn/interrupt does not accept request_id",
            ));
        }
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new("stale_turn", "turn/interrupt requires a captured turn id")
            })?;
        if session.phase == SessionPhase::Stopped
            || session.events.status().active_turn_id.as_deref() != Some(turn_id)
        {
            return Err(Error::new(
                "stale_turn",
                "turn/interrupt does not match the exact active native turn",
            ));
        }
        if let Some(result) = control_replay(session, &params)? {
            return Ok(result);
        }
        let Some(thread_id) = session.thread_id.clone() else {
            let result = unknown_control("Codex thread identity is unresolved");
            settle_control(session, &params.command_id, ControlOutcome::Unknown);
            return Ok(result);
        };
        let interrupt_params = NativeTurnInterruptParams {
            thread_id,
            turn_id: turn_id.to_owned(),
        };
        let encoded = serde_json::to_value(interrupt_params).map_err(|_| {
            Error::new(
                "internal",
                "Codex turn/interrupt params could not be encoded",
            )
        })?;
        match session
            .native_client
            .call("turn/interrupt", encoded, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "native_not_dispatched" | "native_rejected" | "frame_too_large" | "busy"
                ) =>
            {
                let result = rejected_control("Codex turn/interrupt was not applied");
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Codex turn/interrupt outcome is unknown");
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(result)
            }
            Ok(_) => {
                let result = json!({
                    "state": "applied",
                    "command_id": params.command_id,
                    "turn_id": turn_id,
                });
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Applied(result.clone()),
                );
                Ok(result)
            }
        }
    }

    async fn reply_request(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "stale_turn",
                    "Codex request reply needs its captured turn id",
                )
            })?;
        let request_id = params
            .request_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    "Codex request reply needs its captured request id",
                )
            })?;
        if let Some(result) = control_replay(session, &params)? {
            return Ok(result);
        }
        match session
            .events
            .reply_request(
                params.command_id.clone(),
                request_id.to_owned(),
                turn_id.to_owned(),
                params.verb.clone(),
                params.args.clone(),
            )
            .await
        {
            Ok(result) => {
                let outcome = match result.get("state").and_then(Value::as_str) {
                    Some("applied") => ControlOutcome::Applied(result.clone()),
                    Some("rejected") => ControlOutcome::Rejected(result.clone()),
                    Some("unknown") => ControlOutcome::Unknown,
                    _ => {
                        let result = unknown_control("Codex request reply omitted its outcome");
                        settle_control(session, &params.command_id, ControlOutcome::Unknown);
                        return Ok(result);
                    }
                };
                settle_control(session, &params.command_id, outcome);
                Ok(result)
            }
            Err(error) if native_no_write(&error.code) => {
                let result = rejected_control("Codex request reply was not dispatched");
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(error) if error.code == "unknown" => {
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(unknown_control("Codex request reply outcome is unknown"))
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "invalid" | "unsupported" | "secret_in_payload" | "stale_turn" | "not_found"
                ) =>
            {
                let result = rejected_control(&error.message);
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(error) => Err(error),
        }
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        if let Some(observer) = self.observer.as_mut() {
            let params: ObserverSnapshotParams =
                decode(raw, "Codex observer snapshot params are invalid")?;
            return observer
                .snapshot(&params, Instant::now() + crate::discovery::READ_TIMEOUT)
                .await;
        }
        if raw.get("observation").is_some() {
            return Err(Error::new(
                "not_allowed",
                "an observation snapshot requires the exact retained observer attachment",
            ));
        }
        let params: AgentSnapshotParams = decode(raw, "Codex agent.snapshot params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Codex Agent session is not retained"))?;
        validate_binding(session, &params.agent_id, params.generation)?;
        if session.stop_command_id.as_deref() == Some(params.command_id.as_str())
            && session.phase == SessionPhase::Stopped
            && params.turn_id.is_none()
        {
            let event_status = session.events.status();
            let native_ref = session
                .thread_id
                .clone()
                .or_else(|| event_status.thread_id.clone());
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
                "Codex command has no proof for the requested native turn",
            );
        }
        if !known_command(session, &params.command_id) {
            return unknown_snapshot(&params, "Codex command identity is not retained");
        }
        if session.phase == SessionPhase::Stopped {
            return unknown_snapshot(
                &params,
                "Codex native process is stopped; native readback is unavailable",
            );
        }
        let Some(thread_id) = session
            .thread_id
            .as_deref()
            .or(event_status.thread_id.as_deref())
        else {
            return unknown_snapshot(&params, "Codex thread identity was not acknowledged");
        };
        let thread = match read_owned_thread(&session.native_client, thread_id).await {
            Ok(thread) => thread,
            Err(_) => {
                return unknown_snapshot(
                    &params,
                    "Codex thread/read did not prove the exact owned thread",
                );
            }
        };
        let native_state = thread_state(&thread);
        if session.command_inputs.contains_key(&params.command_id) && turn_id.is_none() {
            return unknown_snapshot(
                &params,
                "Codex command has no exact native turn association",
            );
        }
        let last_turn = if let Some(turn_id) = turn_id.as_deref() {
            match read_owned_turn(
                &session.native_client,
                thread_id,
                turn_id,
                &params.command_id,
                &session.secrets,
            )
            .await
            {
                Ok(Some(facts)) => Some(facts),
                _ => {
                    return unknown_snapshot(
                        &params,
                        "Codex readback did not prove the exact requested turn",
                    );
                }
            }
        } else {
            None
        };
        let result_known = last_turn
            .as_ref()
            .is_some_and(|turn| turn.final_text.is_some());
        Ok(json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": native_state,
            "native_scope": session.native_scope,
            "native_ref": thread_id,
            "active_turn": last_turn.as_ref().filter(|turn| !turn.terminal).map(|turn| turn.turn_id.clone()),
            "last_turn": last_turn,
            "batch_unknown": event_status.batch_unknown,
            "source_unknown": event_status.source_unknown,
            "result_known": result_known,
        }))
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        if self.observer.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot stop the native subject",
            ));
        }
        let params: AgentStopParams = decode(raw, "Codex agent.stop params are invalid")?;
        validate_identity(&params.agent_id, params.generation, &params.command_id)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Codex Agent session is not retained"))?;
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
                    "Codex stop command id changed after dispatch",
                ));
            }
            return Err(Error::new(
                "unknown",
                "Codex stop outcome is unresolved and will not be repeated",
            ));
        }
        session.stop_command_id = Some(params.command_id);
        terminate_session(session, params.force).await?;
        Ok(json!({"state": "applied"}))
    }

    async fn shutdown(&mut self) -> Result<Value> {
        if let Some(observer) = self.observer.as_mut() {
            observer
                .shutdown(Instant::now() + crate::discovery::READ_TIMEOUT)
                .await?;
        }
        self.observer = None;
        if let Some(session) = self.session.as_mut() {
            if session.phase != SessionPhase::Stopped {
                terminate_session(session, true).await?;
            }
            session.events.close().await;
        }
        Ok(json!({"state": "shutdown"}))
    }

    async fn shutdown_owned(&mut self) {
        if let Some(observer) = self.observer.as_mut() {
            let _ = observer
                .shutdown(Instant::now() + crate::discovery::READ_TIMEOUT)
                .await;
        }
        self.observer = None;
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
            "Codex process/output drain or session.ended outcome is unknown",
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
    native: &NativeProcess,
) -> Result<()> {
    let pid = native.pid().filter(|pid| *pid > 0).ok_or_else(|| {
        Error::new(
            "process_registration",
            "Codex app-server PID is unavailable; no thread or input was sent",
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
                "Codex app-server process registration failed ({})",
                error.code
            );
            contextual_error(error, "process_registration", message)
        })?;
    let record: ProcessRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "process_registration",
            "Codex host returned an invalid process.put acknowledgement",
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
            "Codex host did not verify the running app-server under this Agent generation",
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

async fn discover_model(client: &NativeClient, requested_model: &str) -> Result<NativeModelEntry> {
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    for _ in 0..MAX_MODEL_PAGES {
        let response = client
            .call(
                "model/list",
                serde_json::to_value(NativeModelListParams {
                    cursor: cursor.clone(),
                    include_hidden: Some(true),
                    limit: Some(MODEL_PAGE_SIZE),
                })
                .map_err(|_| {
                    Error::new("internal", "Codex model/list params could not be encoded")
                })?,
                Instant::now() + RPC_TIMEOUT,
            )
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "profile_error",
                    "Codex model/list did not return a catalogue page",
                )
            })?;
        let page: NativeModelListResponse = serde_json::from_value(response).map_err(|_| {
            Error::new(
                "profile_error",
                "Codex model/list page did not match the installed schema",
            )
        })?;
        if let Some(model) = page
            .data
            .into_iter()
            .find(|model| model.id == requested_model && model.model == requested_model)
        {
            return Ok(model);
        }
        match page.next_cursor {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                cursor = Some(next)
            }
            Some(_) => {
                return Err(Error::new(
                    "profile_error",
                    "Codex model/list returned a repeated or invalid cursor",
                ));
            }
            None => {
                return Err(Error::new(
                    "profile_error",
                    "The exact requested Codex model is unavailable in this account catalogue",
                ));
            }
        }
    }
    Err(Error::new(
        "profile_error",
        "Codex model/list exceeded the bounded pagination limit",
    ))
}

async fn read_owned_thread(client: &NativeClient, thread_id: &str) -> Result<Value> {
    let response: ThreadReadback = serde_json::from_value(
        client
            .call(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": false}),
                Instant::now() + RPC_TIMEOUT,
            )
            .await?,
    )
    .map_err(|_| Error::new("native_readback", "Codex thread/read response was invalid"))?;
    if response.thread.get("id").and_then(Value::as_str) != Some(thread_id) {
        return Err(Error::new(
            "source_integrity",
            "Codex thread/read returned another native thread",
        ));
    }
    Ok(response.thread)
}

async fn read_owned_turn(
    client: &NativeClient,
    thread_id: &str,
    turn_id: &str,
    command_id: &str,
    secrets: &[String],
) -> Result<Option<TurnFacts>> {
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut found: Option<Value> = None;
    for _ in 0..MAX_TURN_PAGES {
        let mut params = json!({
            "threadId": thread_id,
            "limit": TURN_PAGE_SIZE,
            "sortDirection": "desc",
            "itemsView": "notLoaded",
        });
        if let Some(cursor) = &cursor {
            params["cursor"] = Value::String(cursor.clone());
        }
        let response = client
            .call("thread/turns/list", params, Instant::now() + RPC_TIMEOUT)
            .await?;
        let data = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("native_readback", "Codex turn page omitted data"))?;
        if let Some(turn) = data
            .iter()
            .find(|turn| turn.get("id").and_then(Value::as_str) == Some(turn_id))
        {
            found = Some(turn.clone());
            break;
        }
        match response.get("nextCursor").and_then(Value::as_str) {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.to_owned()) => {
                cursor = Some(next.to_owned());
            }
            Some(_) => return Err(Error::new("native_readback", "Codex turn cursor repeated")),
            None => break,
        }
    }
    let Some(turn) = found else { return Ok(None) };
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("native_readback", "Codex turn omitted native status"))?;
    let terminal = matches!(status, "completed" | "interrupted" | "failed");
    if !terminal && status != "inProgress" {
        return Ok(None);
    }
    let final_text = if terminal {
        read_final_text(client, thread_id, turn_id, secrets).await?
    } else {
        None
    };
    Ok(Some(TurnFacts {
        command_id: command_id.to_owned(),
        turn_id: turn_id.to_owned(),
        status: Some(status.to_owned()),
        final_text,
        terminal,
    }))
}

async fn read_final_text(
    client: &NativeClient,
    thread_id: &str,
    turn_id: &str,
    secrets: &[String],
) -> Result<Option<String>> {
    let policy = SecretPolicy::new(secrets.iter().cloned());
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut final_text = None;
    let mut phase_unknown_text = None;
    for _ in 0..MAX_ITEM_PAGES {
        let mut params = json!({
            "threadId": thread_id,
            "turnId": turn_id,
            "limit": ITEM_PAGE_SIZE,
            "sortDirection": "desc",
        });
        if let Some(cursor) = &cursor {
            params["cursor"] = Value::String(cursor.clone());
        }
        let response = client
            .call("thread/items/list", params, Instant::now() + RPC_TIMEOUT)
            .await?;
        let data = response
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::new("native_readback", "Codex item page omitted data"))?;
        for entry in data {
            let item = entry.get("item").unwrap_or(entry);
            if item.get("type").and_then(Value::as_str) == Some("agentMessage") {
                let text = item.get("text").and_then(Value::as_str).ok_or_else(|| {
                    Error::new("native_readback", "Codex final message omitted its text")
                })?;
                match item.get("phase").and_then(Value::as_str) {
                    Some("final_answer") => {
                        if text.len() <= MAX_EVENT_BYTES {
                            final_text = Some(policy.redact_text(text));
                        }
                        return Ok(final_text);
                    }
                    None if phase_unknown_text.is_none() && text.len() <= MAX_EVENT_BYTES => {
                        phase_unknown_text = Some(policy.redact_text(text));
                    }
                    _ => {}
                }
            }
        }
        match response.get("nextCursor").and_then(Value::as_str) {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.to_owned()) => {
                cursor = Some(next.to_owned());
            }
            Some(_) => return Err(Error::new("native_readback", "Codex item cursor repeated")),
            None => break,
        }
    }
    Ok(final_text.or(phase_unknown_text))
}

fn thread_state(thread: &Value) -> &'static str {
    match thread
        .get("status")
        .and_then(|status| status.get("type"))
        .and_then(Value::as_str)
    {
        Some("idle") => "idle",
        Some("active") => {
            let waiting = thread
                .get("status")
                .and_then(|status| status.get("activeFlags"))
                .and_then(Value::as_array)
                .is_some_and(|flags| {
                    flags.iter().any(|flag| {
                        matches!(
                            flag.as_str(),
                            Some("waitingOnApproval" | "waitingOnUserInput")
                        )
                    })
                });
            if waiting { "waiting" } else { "busy" }
        }
        _ => "unknown",
    }
}

fn native_sandbox_mode(permission: &str) -> Result<NativeSandboxMode> {
    match permission {
        "read-only" => Ok(NativeSandboxMode::ReadOnly),
        "workspace-write" => Ok(NativeSandboxMode::WorkspaceWrite),
        "danger-full-access" => Ok(NativeSandboxMode::DangerFullAccess),
        _ => Err(Error::new(
            "profile_error",
            "Codex profile permissions are not a supported sandbox mode",
        )),
    }
}

fn sandbox_matches(sandbox: &Value, requested: &str, workspace: &Path) -> bool {
    let sandbox_type = sandbox.get("type").and_then(Value::as_str);
    let no_network = match sandbox.get("networkAccess") {
        None => true,
        Some(value) => value.as_bool() == Some(false),
    };
    match requested {
        "read-only" => sandbox_type == Some("readOnly") && no_network,
        "danger-full-access" => sandbox_type == Some("dangerFullAccess"),
        "workspace-write" => {
            if sandbox_type != Some("workspaceWrite") || !no_network {
                return false;
            }
            match sandbox.get("writableRoots") {
                None => true,
                Some(Value::Array(roots)) if roots.is_empty() => true,
                Some(Value::Array(roots)) => {
                    roots.len() == 1
                        && roots[0]
                            .as_str()
                            .is_some_and(|root| paths_equal(Path::new(root), workspace))
                }
                Some(_) => false,
            }
        }
        _ => false,
    }
}

fn readback_approval_matches(actual: &Value, requested: &str) -> bool {
    actual.as_str() == Some(requested)
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
        || session.stop_command_id.as_deref() == Some(command_id)
}

fn control_replay(
    session: &mut AgentSession,
    params: &AgentCommandParams,
) -> Result<Option<Value>> {
    if let Some(previous) = session.control_commands.get(&params.command_id) {
        if previous.verb != params.verb
            || previous.turn_id != params.turn_id
            || previous.request_id != params.request_id
            || previous.args != params.args
        {
            return Err(Error::new(
                "source_integrity",
                "Codex control command id was reused with different binding",
            ));
        }
        return Ok(Some(match &previous.outcome {
            ControlOutcome::Applied(result) | ControlOutcome::Rejected(result) => result.clone(),
            ControlOutcome::Pending => {
                unknown_control("Codex control command is already in flight")
            }
            ControlOutcome::Unknown => {
                unknown_control("Codex control command outcome is unresolved")
            }
        }));
    }
    if session.control_commands.len() >= MAX_CONTROL_COMMANDS {
        return Ok(Some(rejected_control(
            "Codex L2.2 per-generation control history bound has been reached",
        )));
    }
    session.control_commands.insert(
        params.command_id.clone(),
        ControlCommand {
            verb: params.verb.clone(),
            turn_id: params.turn_id.clone(),
            request_id: params.request_id.clone(),
            args: params.args.clone(),
            outcome: ControlOutcome::Pending,
        },
    );
    Ok(None)
}

fn settle_control(session: &mut AgentSession, command_id: &str, outcome: ControlOutcome) {
    if let Some(command) = session.control_commands.get_mut(command_id) {
        command.outcome = outcome;
    }
}

fn validate_binding(session: &AgentSession, agent_id: &str, generation: i64) -> Result<()> {
    if session.agent_id != agent_id || session.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "Codex request does not match the bound Agent generation",
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
            "Codex request requires a bounded Agent, generation, and command identity",
        ));
    }
    Ok(())
}

fn validate_source_plan(source_plan: Option<&Value>) -> Result<()> {
    let Some(plan) = source_plan.and_then(Value::as_object) else {
        return Err(Error::new(
            "unsupported",
            "Codex start requires an installed stream source plan",
        ));
    };
    if ["session", "turn", "message", "tool", "request"]
        .iter()
        .any(|family| plan.get(*family).and_then(Value::as_str) != Some("stream"))
    {
        return Err(Error::new(
            "unsupported",
            "Codex start requires stream ownership for session, turn, message, tool, and request",
        ));
    }
    Ok(())
}

fn parse_resume_subject(
    resume: &Value,
    source_state: Option<&Value>,
    agent_id: &str,
    generation: i64,
) -> Result<ResumeSubject> {
    let resume = resume
        .as_object()
        .ok_or_else(|| Error::new("invalid", "Codex resume_from must be an object"))?;
    if resume.len() != 2
        || !resume.contains_key("native_scope")
        || !resume.contains_key("native_ref")
    {
        return Err(Error::new(
            "invalid",
            "Codex resume_from must contain only native_scope and native_ref",
        ));
    }
    let native_scope = resume
        .get("native_scope")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "Codex resume_from requires native_scope"))?
        .to_owned();
    let native_ref = resume
        .get("native_ref")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "Codex resume_from requires native_ref"))?
        .to_owned();
    let source_state = source_state
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("unknown", "Codex resume requires retained native readback"))?;
    let retained_generation = source_state
        .get("retained_generation")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            Error::new(
                "unknown",
                "Codex resume readback omitted its retained generation",
            )
        })?;
    if retained_generation.checked_add(1) != Some(generation) {
        return Err(Error::new(
            "stale_generation",
            "Codex resume generation does not follow the retained source generation",
        ));
    }
    let retained_source = source_state
        .get("retained_source")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("unknown", "Codex resume omitted retained source state"))?;
    if retained_source.get("agent_id").and_then(Value::as_str) != Some(agent_id)
        || retained_source.get("generation").and_then(Value::as_i64) != Some(retained_generation)
        || retained_source.get("source").and_then(Value::as_str) != Some("stream")
    {
        return Err(Error::new(
            "stale_generation",
            "Codex resume source state does not match the retained Agent generation",
        ));
    }
    let families = retained_source
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("unknown", "Codex resume source state omitted its families"))?;
    if ["session", "turn", "message", "tool", "request"]
        .iter()
        .any(|family| !families.iter().any(|value| value.as_str() == Some(family)))
    {
        return Err(Error::new(
            "unknown",
            "Codex retained source did not own the required event families",
        ));
    }
    let current_turn = source_state
        .get("current_turn")
        .ok_or_else(|| Error::new("unknown", "Codex resume readback omitted current_turn"))?;
    let retained_turn = match current_turn {
        Value::Null => None,
        Value::Object(turn) => {
            if !["state", "status", "started_at", "completed_at"]
                .iter()
                .all(|field| turn.contains_key(*field))
                || !matches!(
                    turn.get("state").and_then(Value::as_str),
                    Some("running" | "completed" | "unknown")
                )
                || !matches!(
                    turn.get("status"),
                    Some(value) if value.is_null() || value.as_str().is_some()
                )
                || !matches!(
                    turn.get("started_at"),
                    Some(value) if value.is_null() || value.as_i64().is_some()
                )
                || !matches!(
                    turn.get("completed_at"),
                    Some(value) if value.is_null() || value.as_i64().is_some()
                )
            {
                return Err(Error::new(
                    "unknown",
                    "Codex resume current_turn summary is incomplete",
                ));
            }
            Some(
                turn.get("turn_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "unknown",
                            "Codex resume current_turn omitted its exact turn id",
                        )
                    })?
                    .to_owned(),
            )
        }
        _ => {
            return Err(Error::new(
                "unknown",
                "Codex resume current_turn readback is malformed",
            ));
        }
    };
    Ok(ResumeSubject {
        native_scope,
        native_ref,
        retained_turn,
    })
}

async fn verify_resume_preflight(
    client: &NativeClient,
    thread_id: &str,
    retained_turn: Option<&str>,
) -> Result<()> {
    let thread = read_owned_thread(client, thread_id).await?;
    let newest_turn = read_latest_native_turn_status(client, thread_id).await?;
    validate_resume_preflight(
        thread
            .get("status")
            .and_then(|status| status.get("type"))
            .and_then(Value::as_str),
        retained_turn,
        newest_turn
            .as_ref()
            .map(|(id, status)| (id.as_str(), status.as_str())),
    )
}

fn validate_resume_preflight(
    native_status: Option<&str>,
    retained_turn: Option<&str>,
    newest_turn: Option<(&str, &str)>,
) -> Result<()> {
    if !matches!(native_status, Some("idle" | "notLoaded")) {
        return Err(Error::new(
            "native_readback",
            "Codex retained thread is active or has no resumable runtime status",
        ));
    }
    match (retained_turn, newest_turn) {
        (Some(expected), Some((actual, status)))
            if actual == expected && terminal_turn_status(status) =>
        {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(Error::new(
            "native_readback",
            "Codex newest native turn does not match the exact retained terminal turn",
        )),
    }
}

async fn read_latest_native_turn_status(
    client: &NativeClient,
    thread_id: &str,
) -> Result<Option<(String, String)>> {
    let response = client
        .call(
            "thread/turns/list",
            json!({
                "threadId": thread_id,
                "limit": 1,
                "sortDirection": "desc",
                "itemsView": "notLoaded",
            }),
            Instant::now() + RPC_TIMEOUT,
        )
        .await?;
    let turns = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("native_readback", "Codex newest-turn page omitted data"))?;
    let Some(turn) = turns.first() else {
        return match response.get("nextCursor") {
            None | Some(Value::Null) => Ok(None),
            Some(_) => Err(Error::new(
                "native_readback",
                "Codex empty newest-turn page has an unresolved continuation",
            )),
        };
    };
    let id = turn
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("native_readback", "Codex newest turn omitted its exact id"))?;
    let status = turn
        .get("status")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("native_readback", "Codex newest turn omitted its status"))?;
    Ok(Some((id.to_owned(), status.to_owned())))
}

fn terminal_turn_status(status: &str) -> bool {
    matches!(status, "completed" | "interrupted" | "failed")
}

pub(crate) fn contextual_error(
    error: Error,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Error {
    error.with_code(code).with_message(message)
}

fn optional_profile_text(
    profile: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>> {
    match profile.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "profile_error",
            format!("Codex profile field {key} must be a non-empty string"),
        )),
    }
}

fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
) -> Result<TargetEnvironment> {
    let Some(environment) = environment else {
        return Ok(TargetEnvironment {
            variables: Vec::new(),
            secrets: Vec::new(),
            metadata_secrets: Vec::new(),
        });
    };
    let object = environment.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            "Codex target_environment must be an object",
        )
    })?;
    let pass_env = pass_env.and_then(Value::as_array);
    let mut vars = Vec::with_capacity(object.len());
    let mut secrets = Vec::new();
    let mut metadata_secrets = Vec::new();
    for (name, value) in object {
        if name.is_empty() || name.contains('=') || name.contains('\0') {
            return Err(Error::new(
                "profile_error",
                "Codex environment contains an invalid variable name",
            ));
        }
        let value = value.as_str().ok_or_else(|| {
            Error::new("profile_error", "Codex environment values must be strings")
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
            // Only selector contributions are public store metadata. Never
            // remove by value: a credential may have the same bytes as a path.
            let selector = ["CODEX_HOME", "CODEX_SQLITE_HOME"]
                .into_iter()
                .any(|selector| {
                    if cfg!(windows) {
                        name.eq_ignore_ascii_case(selector)
                    } else {
                        name == selector
                    }
                });
            if !selector {
                metadata_secrets.push(value.to_owned());
            }
        }
        vars.push((OsString::from(name), OsString::from(value)));
    }
    Ok(TargetEnvironment {
        variables: vars,
        secrets,
        metadata_secrets,
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

fn frozen_store_binding(profile: &serde_json::Map<String, Value>) -> Result<NativeStoreBinding> {
    let binding = profile
        .get("native_store_binding")
        .filter(|value| value.is_object())
        .ok_or_else(|| {
            Error::new(
                "native_scope_unverified",
                "Codex controlled start requires its admitted native store binding",
            )
        })?;
    serde_json::from_value(binding.clone()).map_err(|_| {
        Error::new(
            "native_scope_unverified",
            "Codex admitted native store binding is malformed",
        )
    })
}

pub(crate) fn native_scope(codex_home: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    let canonical_home = if cfg!(windows) {
        codex_home.to_lowercase()
    } else {
        codex_home.to_owned()
    };
    for byte in canonical_home.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("codex-home:{hash:016x}")
}

fn codex_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("request".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("tool".to_owned(), "stream".to_owned()),
        ("turn".to_owned(), "stream".to_owned()),
    ]);
    let observed = BTreeMap::from([
        ("message".to_owned(), "native_read".to_owned()),
        ("session".to_owned(), "native_read".to_owned()),
        ("tool".to_owned(), "native_read".to_owned()),
        ("turn".to_owned(), "native_read".to_owned()),
    ]);
    PluginManifest {
        name: "codex".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: "eliot-plugin/1".to_owned(),
        harness_version: "0.161.0".to_owned(),
        available: true,
        mode: "per_agent".to_owned(),
        verbs: vec![
            "launch".to_owned(),
            "resume".to_owned(),
            "send".to_owned(),
            "steer".to_owned(),
            "interrupt".to_owned(),
            "approve".to_owned(),
            "deny".to_owned(),
            "answer".to_owned(),
            "stop".to_owned(),
        ],
        child_verbs: Vec::new(),
        features: eliot_plugin::protocol::ManifestFeatures {
            discover: true,
            observe: true,
            start_requires_input: false,
            native_children: false,
        },
        sources: BTreeMap::from([
            ("controlled".to_owned(), controlled),
            ("observed".to_owned(), observed),
        ]),
        proxy: "responses".to_owned(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(raw: &Value, message: &str) -> Result<T> {
    serde_json::from_value(raw.clone()).map_err(|_| Error::new("invalid", message))
}

fn not_initialized() -> Error {
    Error::new("bad_state", "Codex plugin has not been initialized")
}

#[cfg(test)]
mod tests {
    use super::{
        CodexPlugin, CodexRoute, Initialized, NativeLaunch, codex_manifest, frozen_store_binding,
        target_environment, validate_resume_preflight,
    };
    use eliot_core::SecretPolicy;
    use serde_json::json;
    use std::{ffi::OsString, path::PathBuf};

    #[test]
    fn store_metadata_policy_excludes_selector_names_not_colliding_credentials() {
        let profile = json!({
            "harness": "codex",
            "pass_env": ["CODEX_HOME", "CODEX_SQLITE_HOME", "DIAGNOSTIC_VALUE"],
        });
        for credential_name in [None, Some("DIAGNOSTIC_VALUE"), Some("ELIOT_OWNER_TOKEN")] {
            let mut environment = json!({
                "CODEX_HOME": "dummy-home-value",
                "CODEX_SQLITE_HOME": "dummy-root-value",
            });
            if let Some(name) = credential_name {
                environment[name] = json!("dummy-home-value");
            }
            let plugin = CodexPlugin {
                initialized: Some(Initialized {
                    // An instance credential colliding with a selector must
                    // remain protected too, independently of environment names.
                    credential: "dummy-root-value".to_owned(),
                    instance_dir: "dummy-private-instance".to_owned(),
                }),
                ..CodexPlugin::default()
            };
            let (_, diagnostics, metadata) = plugin.reader_launch(&profile, &environment).unwrap();
            let diagnostics = SecretPolicy::new(diagnostics);
            let metadata = SecretPolicy::new(metadata);
            assert!(diagnostics.contains_text("dummy-home-value"));
            assert!(diagnostics.contains_text("dummy-root-value"));
            assert_eq!(
                metadata.contains_text("dummy-home-value"),
                credential_name.is_some()
            );
            assert!(metadata.contains_text("dummy-root-value"));
        }

        let captured = target_environment(
            Some(&json!({"CODEX_HOME": "dummy-shared-value", "ELIOT_AGENT_TOKEN": "dummy-shared-value"})),
            profile.get("pass_env"),
        ).unwrap();
        assert!(SecretPolicy::new(captured.metadata_secrets).contains_text("dummy-shared-value"));

        for name in ["CODEX_HOME", "codex_home"] {
            let names = json!([name]);
            let captured =
                target_environment(Some(&json!({(name): "dummy-selector-value"})), Some(&names))
                    .unwrap();
            assert!(SecretPolicy::new(captured.secrets).contains_text("dummy-selector-value"));
            assert_eq!(
                SecretPolicy::new(captured.metadata_secrets).contains_text("dummy-selector-value"),
                !cfg!(windows) && name != "CODEX_HOME",
            );
        }
    }

    #[test]
    fn reader_launch_routes_named_provider_and_captures_opaque_client_key() {
        let secret = "opaque-client-value";
        let profile = json!({
            "harness":"codex",
            "pass_env":[],
            "route_binding":{
                "name":"corp_gateway",
                "endpoint":"http://127.0.0.1:10300",
                "readiness_path":"/readyz",
                "provider_base_url":"http://127.0.0.1:10300/v1",
                "client_key_env_name":"LOCAL_CLIENT"
            }
        });
        let plugin = CodexPlugin {
            initialized: Some(Initialized {
                credential: "instance-credential".to_owned(),
                instance_dir: "private-instance".to_owned(),
            }),
            ..CodexPlugin::default()
        };

        let (launch, diagnostic_secrets, metadata_secrets) = plugin
            .reader_launch(&profile, &json!({"LOCAL_CLIENT":secret}))
            .unwrap();
        assert!(SecretPolicy::new(&diagnostic_secrets).contains_text(secret));
        assert!(SecretPolicy::new(&metadata_secrets).contains_text(secret));
        assert!(
            launch
                .environment
                .iter()
                .all(|(name, _)| name != "LOCAL_CLIENT")
        );
        assert!(launch.args.iter().any(|arg| {
            arg.to_string_lossy()
                .contains("model_provider=\"corp_gateway\"")
        }));
        assert!(
            launch
                .args
                .iter()
                .all(|arg| { !arg.to_string_lossy().contains(secret) })
        );

        let (missing_key_launch, _, _) = plugin.reader_launch(&profile, &json!({})).unwrap();
        assert!(
            missing_key_launch
                .environment
                .iter()
                .all(|(name, _)| name != "LOCAL_CLIENT")
        );
    }

    #[test]
    fn codex_manifest_advertises_responses_proxy_capability() {
        assert_eq!(codex_manifest().proxy, "responses");
    }

    #[test]
    fn controlled_start_requires_an_admitted_binding_without_legacy_reselection() {
        for profile in [
            json!({"pass_env_values": {"CODEX_HOME": null, "CODEX_SQLITE_HOME": null}}),
            json!({"native_store_binding": null}),
            json!({"native_store_binding": {"home": "fixture", "root": "fixture"}}),
            json!({"native_store_binding": ["fixture", "fixture", "fixture"]}),
        ] {
            let error = frozen_store_binding(profile.as_object().unwrap()).unwrap_err();
            assert_eq!(error.code, "native_scope_unverified");
        }
    }

    #[test]
    fn controlled_launch_pins_binding_despite_changed_selector_inputs() {
        let home = if cfg!(windows) {
            r"C:\fixture\frozen-home"
        } else {
            "/fixture/frozen-home"
        };
        let root = if cfg!(windows) {
            r"C:\fixture\frozen-root"
        } else {
            "/fixture/frozen-root"
        };
        let profile = json!({"native_store_binding": {
            "home": home, "root": root, "native_scope": "fixture-retained-scope",
        }});
        let binding = frozen_store_binding(profile.as_object().unwrap()).unwrap();
        let launch = NativeLaunch {
            program: OsString::from("codex-not-executed"),
            args: CodexRoute::from_profile(&json!({"harness":"codex"}))
                .unwrap()
                .args()
                .unwrap(),
            cwd: PathBuf::from(home),
            environment: vec![
                (
                    OsString::from("CODEX_HOME"),
                    OsString::from("changed-home-input"),
                ),
                (
                    OsString::from("CODEX_SQLITE_HOME"),
                    OsString::from("changed-root-input"),
                ),
            ],
        };
        let pinned = crate::discovery::pinned_launch_for_binding(&launch, &binding).unwrap();
        let homes: Vec<_> = pinned
            .environment
            .iter()
            .filter(|(name, _)| name.to_str() == Some("CODEX_HOME"))
            .collect();
        assert_eq!(homes.len(), 1);
        assert_eq!(homes[0].1.to_str(), Some(home));
        let sqlite_pin = format!("sqlite_home={}", serde_json::to_string(root).unwrap());
        assert!(
            pinned
                .args
                .iter()
                .any(|arg| arg.to_str() == Some(sqlite_pin.as_str()))
        );
        assert_eq!(binding.native_scope, "fixture-retained-scope");
    }

    #[test]
    fn producer_policy_includes_explicit_pass_env_values_from_target_environment() {
        let profile = json!({"pass_env": ["DIAGNOSTIC_VALUE", "MISSING_VALUE"]});
        for name in ["DIAGNOSTIC_VALUE", "diagnostic_value"] {
            for value in ["native_error", "private diagnostic message"] {
                let environment = json!({
                    (name): value,
                    "SERVICE_TOKEN": "dummy heuristic credential",
                    "ORDINARY_VALUE": "ordinary target value",
                });
                let captured =
                    target_environment(Some(&environment), profile.get("pass_env")).unwrap();
                let policy = SecretPolicy::new(captured.secrets);
                let matches = cfg!(windows) || name == "DIAGNOSTIC_VALUE";
                assert_eq!(policy.contains_text(value), matches);
                if matches {
                    assert_eq!(policy.redact_text(value), "[REDACTED]");
                }
                assert!(policy.contains_text("dummy heuristic credential"));
                assert!(!policy.contains_text("ordinary target value"));
            }
        }
    }

    #[test]
    fn unloaded_retained_thread_requires_latest_terminal_turn() {
        for status in ["completed", "interrupted", "failed"] {
            assert!(
                validate_resume_preflight(
                    Some("notLoaded"),
                    Some("retained"),
                    Some(("retained", status)),
                )
                .is_ok()
            );
        }
        for native_status in [Some("active"), Some("systemError"), Some("unknown"), None] {
            let error = validate_resume_preflight(
                native_status,
                Some("retained"),
                Some(("retained", "completed")),
            )
            .expect_err("only idle or unloaded native threads can pass preflight");
            assert_eq!(error.code, "native_readback");
        }
        for newest in [
            None,
            Some(("newer", "completed")),
            Some(("retained", "inProgress")),
            Some(("retained", "unknown")),
        ] {
            assert!(
                validate_resume_preflight(Some("notLoaded"), Some("retained"), newest).is_err()
            );
        }
        let unretained_newest = Some(("unretained", "completed"));
        assert!(validate_resume_preflight(Some("notLoaded"), None, unretained_newest).is_err());
    }
}
